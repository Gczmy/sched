from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from dataclasses import asdict
from unittest import mock

from gsched import cli, recovery, state
from gsched.dispatcher import Dispatcher, _inbox_task_spec
from gsched.fingerprint import compute_fingerprint
from gsched.executor import Executor, process_start_token
from gsched.execution import BackendUnavailable, LinuxFdBackend
from gsched.execution_policy import normalize_execution
from gsched.schema import SchemaError, validate_batch
from test_review_cli_state import TempStateCase


class RecoveryProtocolTests(TempStateCase):
    def setUp(self):
        super().setUp()
        self.root = Path(self.tmp.name)
        self.worker = self.root / "worker.py"
        self.worker.write_bytes((Path(__file__).parent / "fixtures" / "recovery_worker.py").read_bytes())
        (self.root / "settings.json").write_text(json.dumps({"total": 5, "oom_at": [2]}))
        self.cfg["projects"]["p"]["root"] = str(self.root)

    def task(self, mode="smoke", smoke="smoke-task-v1"):
        def sha(name):
            return hashlib.sha256((self.root / name).read_bytes()).hexdigest()
        declaration = {"protocol": recovery.PROTOCOL, "mode": mode,
            "code": {"worker.py": sha("worker.py")}, "config": {"settings.json": sha("settings.json")}, "inputs": {}}
        if mode == "run":
            declaration["smoke_job_id"] = smoke
        return {"id": "task", "cmd": [sys.executable, "worker.py"], "git": False,
                "resources": {"gpu": 0}, "max_retry": 0, "recovery": declaration}

    def spec(self, mode="smoke", smoke="smoke-task-v1"):
        raw = {"name": "fixture", "project": "p", "cwd": str(self.root), "tasks": [self.task(mode, smoke)]}
        spec = validate_batch(raw, self.cfg)["tasks"][0]
        fp, _, _ = compute_fingerprint(spec["cmd"], None, spec["cwd_abs"], False, {}, artifacts=spec["artifacts"])
        recovery.freeze(spec, fp)
        return spec

    def seed(self, batch, spec, status="pending", version=1):
        job_id = f"{batch}-task-v{version}"
        with state.connect() as conn:
            if state.get_batch(conn, batch) is None:
                state.insert_batch(conn, batch, batch, "mix", [], None, str(self.root), {}, project="p")
                conn.execute("UPDATE batches SET status='active' WHERE id=?", (batch,))
            state.insert_task(conn, batch, "task", version, spec, 0, "p")
            state.insert_job(conn, job_id, batch, "task", version, spec["_recovery_fingerprint"], project="p")
            state.update_job(conn, job_id, status=status, rc=0 if status == "done" else None)
        with state.connect() as conn:
            return dict(state.get_job(conn, job_id))

    def run_worker(self, job, spec):
        env = dict(os.environ)
        env[recovery.ENVIRONMENT] = recovery.context(state.host_dir(), job, spec)
        env["PYTHONPATH"] = str(Path(__file__).parent.parent)
        return subprocess.run(spec["cmd"], cwd=self.root, env=env, capture_output=True, text=True, timeout=10)

    def test_actual_worker_smoke_oom_and_resume_keep_completed_progress(self):
        smoke_spec = self.spec()
        smoke_job = self.seed("smoke", smoke_spec, "running")
        completed = self.run_worker(smoke_job, smoke_spec)
        self.assertEqual(0, completed.returncode, completed.stderr)
        with state.connect() as conn:
            state.update_job(conn, smoke_job["id"], status="done", rc=0)
        spec = self.spec("run")
        job = self.seed("formal", spec)
        with state.connect() as conn:
            self.assertTrue(recovery.smoke_gate(conn, state.host_dir(), job, spec))
        first = self.run_worker(job, spec)
        self.assertEqual(42, first.returncode, first.stderr)
        self.assertEqual("oom", recovery.report(state.host_dir(), job, spec)["outcome"])
        store = recovery.CheckpointStore(json.loads(recovery.context(state.host_dir(), job, spec)))
        self.assertEqual({"next": 2, "results": [0, 1]}, store.load())
        next_job = self.seed("formal", spec, version=2)
        resumed = self.run_worker(next_job, spec)
        self.assertEqual(0, resumed.returncode, resumed.stderr)
        self.assertEqual({"next": 5, "results": list(range(5))}, json.loads((self.root / "result.json").read_text()))

    def test_pending_failed_skipped_missing_or_wrong_binding_smoke_never_opens_gate(self):
        smoke_spec = self.spec()
        smoke_job = self.seed("smoke", smoke_spec)
        formal_spec = self.spec("run")
        formal_job = self.seed("formal", formal_spec)
        self.assertEqual(0, self.run_worker(smoke_job, smoke_spec).returncode)
        with state.connect() as conn:
            for status, rc in (("pending", None), ("running", None), ("failed", 42), ("skip", None), ("done", None)):
                state.update_job(conn, smoke_job["id"], status=status, rc=rc)
                self.assertFalse(recovery.smoke_gate(conn, state.host_dir(), formal_job, formal_spec))
            state.update_job(conn, smoke_job["id"], status="done", rc=0)
            other = {**formal_spec, "_recovery_binding": "0" * 64}
            self.assertFalse(recovery.smoke_gate(conn, state.host_dir(), formal_job, other))
            self.assertTrue(recovery.smoke_gate(conn, state.host_dir(), formal_job, formal_spec))

    def test_successful_exit_without_smoke_receipt_is_insufficient(self):
        self.seed("smoke", self.spec(), "done")
        formal = self.spec("run")
        job = self.seed("formal", formal)
        with state.connect() as conn:
            self.assertFalse(recovery.smoke_gate(conn, state.host_dir(), job, formal))

    def test_input_changes_symlinks_and_configuration_changes_are_denied(self):
        spec = self.spec()
        with self.assertRaises(recovery.RecoveryError):
            recovery.verify_binding(spec, "0" * 64)
        (self.root / "settings.json").write_text("changed")
        with self.assertRaises(recovery.RecoveryError):
            recovery.verify_binding(spec, spec["_recovery_fingerprint"])
        (self.root / "settings.json").unlink()
        (self.root / "settings.json").symlink_to(self.worker)
        with self.assertRaises(recovery.RecoveryError):
            recovery.verify_inputs(spec)

    def test_bad_schema_internal_fields_and_spoofed_transport_are_rejected(self):
        for patch in ({"max_retry": 1}, {"max_retry": False}, {"_recovery_binding": "0" * 64},
                      {"_recovery_context": "spoof"}, {"env": {recovery.ENVIRONMENT: "spoof"}}):
            task = {**self.task(), **patch}
            with self.assertRaises((SchemaError, recovery.RecoveryError)):
                validate_batch({"name": "x", "project": "p", "tasks": [task]}, self.cfg)
        raw = self.task()
        for patch in ({"mode": "other"}, {"code": {}}, {"inputs": {"../outside": "0" * 64}}, {"mode": "run"}, {"smoke_job_id": "bad/ref"}):
            task = copy.deepcopy(raw)
            task["recovery"].update(patch)
            with self.assertRaises(SchemaError):
                validate_batch({"name": "x", "project": "p", "tasks": [task]}, self.cfg)

    def test_atomic_save_preserves_old_checkpoint_on_failure_and_rejects_corruption(self):
        spec = self.spec()
        job = self.seed("smoke", spec)
        value = json.loads(recovery.context(state.host_dir(), job, spec))
        store = recovery.CheckpointStore(value)
        store.save({"step": 1})
        with mock.patch.object(recovery.os, "replace", side_effect=OSError("injected")):
            with self.assertRaises(OSError):
                store.save({"step": 2})
        self.assertEqual({"step": 1}, store.load())
        self.assertFalse(list(Path(value["checkpoint_dir"]).glob(".pending-*")))
        path = Path(value["checkpoint_dir"]) / "checkpoint.json"
        record = json.loads(path.read_text())
        record["payload"] = {"step": 999}
        path.write_text(json.dumps(record))
        with self.assertRaises(recovery.RecoveryError):
            store.load()
        path.unlink()
        path.symlink_to(self.worker)
        with self.assertRaises(OSError):
            store.load()

    def test_reports_need_checkpoint_and_do_not_supply_wait_authority(self):
        spec = self.spec()
        job = self.seed("smoke", spec)
        store = recovery.CheckpointStore(json.loads(recovery.context(state.host_dir(), job, spec)))
        with self.assertRaises(recovery.RecoveryError):
            store.report("smoke_ok")
        store.save({"step": 1})
        store.report("smoke_ok")
        with state.connect() as conn:
            self.assertEqual("pending", state.get_job(conn, job["id"])["status"])
            formal = self.spec("run")
            self.assertFalse(recovery.smoke_gate(conn, state.host_dir(), job, formal))

    def test_inbox_fields_match_direct_submission_and_query_is_read_only(self):
        spec = self.spec()
        persisted = _inbox_task_spec(spec, spec["cmd"], None)
        for key in ("recovery", *recovery.INTERNAL_FIELDS):
            self.assertEqual(spec[key], persisted[key])
        job = self.seed("smoke", spec)
        code, output, error = self.capture(cli.cmd_recovery, argparse.Namespace(task="smoke:task", json=True))
        self.assertEqual(0, code, error)
        self.assertEqual("absent", json.loads(output)["checkpoint"]["state"])
        self.assertFalse((Path(state.host_dir()) / "recovery").exists())

    def dispatcher(self):
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher.host_dir = state.host_dir()
        dispatcher.executor = mock.Mock()
        dispatcher.executor.launch.return_value = 43210
        dispatcher.log_line = mock.Mock()
        dispatcher._prepare_launch_marker = mock.Mock(return_value=False)
        dispatcher._snapshot_fingerprint = mock.Mock(return_value=(self.spec()["_recovery_fingerprint"], {}, None))
        dispatcher._should_skip = mock.Mock(return_value=False)
        dispatcher._drop_job_rc = mock.Mock()
        dispatcher._clean_stale_artifacts = mock.Mock()
        return dispatcher

    def test_direct_launch_is_gated_before_claim_cleanup_and_child_creation(self):
        spec = self.spec("run")
        job = self.seed("formal", spec)
        dispatcher = self.dispatcher()
        with state.connect() as conn:
            self.assertFalse(dispatcher._launch_job(conn, state.get_job(conn, job["id"]), None))
            self.assertEqual("pending", state.get_job(conn, job["id"])["status"])
        dispatcher.executor.launch.assert_not_called()
        dispatcher._clean_stale_artifacts.assert_not_called()

    def test_successful_gate_transmits_scheduler_context_and_binding_drift_blocks_launch(self):
        smoke_spec = self.spec()
        smoke = self.seed("smoke", smoke_spec, "done")
        self.assertEqual(0, self.run_worker(smoke, smoke_spec).returncode)
        spec = self.spec("run")
        job = self.seed("formal", spec)
        dispatcher = self.dispatcher()
        with state.connect() as conn:
            self.assertTrue(dispatcher._launch_job(conn, state.get_job(conn, job["id"]), None))
        value = json.loads(dispatcher.executor.launch.call_args.kwargs["env"][recovery.ENVIRONMENT])
        self.assertEqual(job["id"], value["job_id"])
        self.assertEqual(spec["_recovery_binding"], value["binding_sha256"])
        next_job = self.seed("formal", spec, version=2)
        dispatcher.executor.launch.reset_mock()
        (self.root / "settings.json").write_text("changed")
        with state.connect() as conn:
            with self.assertRaises(recovery.RecoveryError):
                dispatcher._launch_job(conn, state.get_job(conn, next_job["id"]), None)
        dispatcher.executor.launch.assert_not_called()

    def test_manual_retry_clean_and_restart_do_not_reuse_recovery_version(self):
        spec = self.spec()
        job = self.seed("smoke", spec, "failed")
        code, _, error = self.capture(cli.cmd_retry, argparse.Namespace(task="smoke:task"))
        self.assertEqual(1, code, error)
        self.assertIn("resubmit", error)
        dispatcher = self.dispatcher()
        dispatcher._release_in_tx = mock.Mock()
        with state.connect() as conn:
            state.update_job(conn, job["id"], status="interrupted")
            dispatcher._requeue_for_retry(conn, state.get_job(conn, job["id"]))
            self.assertEqual("interrupted", state.get_job(conn, job["id"])["status"])
            state.update_job(conn, job["id"], status="skip")
            conn.execute("UPDATE batches SET status='done' WHERE id='smoke'")
        code, _, error = self.capture(cli.cmd_clean, argparse.Namespace(batch="smoke", yes=True))
        self.assertEqual(1, code, error)
        self.assertIn("recovery", error)

    def test_native_backends_receive_context_without_changing_fd4_identity(self):
        try:
            LinuxFdBackend()
        except BackendUnavailable:
            if os.environ.get("SCHED_REQUIRE_NATIVE") == "1":
                raise
            self.skipTest("native backend was not explicitly built")
        executable = os.path.realpath(sys.executable)
        for kind in ("linux_fd", "linux_fd_owner"):
            profile = {"kind": kind, "executable": executable,
                "sha256": hashlib.sha256(Path(executable).read_bytes()).hexdigest(),
                "argv": [executable, "worker.py"], "env": {"PYTHONPATH": str(Path(__file__).parent.parent), "CHECK_IDENTITY_FD": "1"},
                "projects": ["p"], "input_slots": {}}
            self.cfg["execution_backends"] = {"worker": profile}
            spec = self.spec()
            spec["cmd"] = profile["argv"]
            spec["execution"] = {"backend": "worker", "inputs": {}}
            spec["_execution_binding"] = normalize_execution(spec, self.cfg, "p", {})
            fp, _, _ = compute_fingerprint(spec["cmd"], None, spec["cwd_abs"], False, {},
                                          artifacts=spec["artifacts"], execution_binding_sha256=recovery.digest(spec["_execution_binding"]))
            recovery.freeze(spec, fp)
            job = self.seed(kind, spec, "running")
            spec["_recovery_context"] = recovery.context(state.host_dir(), job, spec)
            from gsched.execution_state import reserve
            token = int(process_start_token(os.getpid())[5:])
            with state.connect() as conn:
                identity = reserve(conn, job["id"], spec["_execution_binding"], profile, "review-node", token)
            executor = Executor()
            prepared = executor.prepare_configured_execution(job["id"], spec, profile, identity, None, str(self.root / f"{kind}.log"))
            try:
                owner = prepared.launch()
                deadline = time.monotonic() + 10
                observation = owner.poll()
                while observation.status not in ("exited", "not_started") and time.monotonic() < deadline:
                    time.sleep(.02)
                    observation = owner.poll()
                self.assertEqual("exited", observation.status, asdict(observation))
                self.assertEqual(0, observation.returncode, (self.root / f"{kind}.log").read_text())
                self.assertTrue(observation.group_clean)
                self.assertEqual("smoke_ok", recovery.report(state.host_dir(), job, spec)["outcome"])
            finally:
                executor.retire_configured_execution(job["id"])
