from __future__ import annotations

import argparse
import copy
import hashlib
import sys
import json
import os
from pathlib import Path
import sqlite3
import time
from unittest import mock

from gsched import cli, execution_state, recovery, recovery_state, state
from gsched.dispatcher import Dispatcher
from gsched.fingerprint import compute_fingerprint
from gsched.schema import SchemaError, validate_batch
import test_recovery_protocol as protocol_fixture
from test_review_cli_state import TempStateCase


class RecoveryQueueTests(TempStateCase):
    def setUp(self):
        super().setUp()
        self.root = Path(self.tmp.name)
        self.worker = self.root / "worker.py"
        self.worker.write_bytes((Path(__file__).parent / "fixtures" / "recovery_worker.py").read_bytes())
        (self.root / "settings.json").write_text(json.dumps({"total": 5, "oom_at": [2]}))
        self.cfg["projects"]["p"]["root"] = str(self.root)

    spec = protocol_fixture.RecoveryProtocolTests.spec
    seed = protocol_fixture.RecoveryProtocolTests.seed
    run_worker = protocol_fixture.RecoveryProtocolTests.run_worker

    def task(self, mode="smoke", smoke="smoke-task-v1"):
        task = protocol_fixture.RecoveryProtocolTests.task(self, mode, smoke)
        task["recovery"]["retry"] = {"cooldown_sec": 0}
        return task

    def store(self, job, spec):
        return recovery.CheckpointStore(json.loads(recovery.context(state.host_dir(), job, spec)))

    def oom(self, job, spec, *, payload=None):
        store = self.store(job, spec)
        store.save({"next": 2, "results": [0, 1]} if payload is None else payload)
        store.report("oom")
        with state.connect() as conn:
            state.update_job(conn, job["id"], status="failed", rc=42, failure="oom")
            recovery_state.record_clean(conn, job, spec, "oom", "wait")
            return recovery_state.create_next(conn, state.host_dir(), job, spec)

    def latest(self, batch="formal", task="task"):
        with state.connect() as conn:
            return dict(conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id=? ORDER BY version DESC LIMIT 1", (batch, task)).fetchone())

    def test_atomic_new_version_preserves_failed_history_and_is_idempotent(self):
        spec = self.spec("run")
        job = self.seed("formal", spec, "running")
        self.assertTrue(self.oom(job, spec))
        successor = self.latest()
        self.assertEqual(2, successor["version"])
        self.assertEqual("pending", successor["status"])
        with state.connect() as conn:
            old = state.get_job(conn, job["id"])
            self.assertEqual(("failed", 42, "oom"), (old["status"], old["rc"], old["failure"]))
            self.assertTrue(recovery_state.create_next(conn, state.host_dir(), job, spec))
            self.assertEqual(2, conn.execute("SELECT COUNT(*) FROM jobs WHERE batch_id='formal'").fetchone()[0])
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM recovery_queue").fetchone()[0])
            self.assertEqual("queued", recovery_state.public(conn, job["id"])["settlement"]["decision"])
            self.assertIsNone(execution_state.get(conn, successor["id"]))
            self.assertTrue(json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id='formal' AND version=2").fetchone()[0])["_force_rerun"])

    def test_publication_failure_rolls_back_all_new_version_writes(self):
        spec = self.spec("run")
        job = self.seed("formal", spec, "failed")
        store = self.store(job, spec)
        store.save({"next": 1})
        with state.connect() as conn:
            state.update_job(conn, job["id"], rc=42, failure="oom")
            recovery_state.record_clean(conn, job, spec, "oom", "wait")
            with mock.patch.object(state, "insert_job", side_effect=RuntimeError("injected publication failure")):
                with self.assertRaises(RuntimeError):
                    recovery_state.create_next(conn, state.host_dir(), job, spec)
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM recovery_queue").fetchone()[0])
            self.assertEqual("pending", recovery_state.public(conn, job["id"])["settlement"]["decision"])
            self.assertTrue(recovery_state.create_next(conn, state.host_dir(), job, spec))

    def test_normal_pass_barrier_and_repeated_oom_move_to_fifo_tail_across_restart(self):
        spec = self.spec("run")
        a = self.seed("formal", spec, "running")
        with state.connect() as conn:
            for name in ("b", "c"):
                other = dict(spec, id=name)
                state.insert_task(conn, "formal", name, 1, other, 1, "p")
                state.insert_job(conn, f"formal-{name}-v1", "formal", name, 1, spec["_recovery_fingerprint"], project="p")
        self.assertTrue(self.oom(a, spec))
        a2 = self.latest()
        with state.connect() as conn:
            self.assertFalse(recovery_state.eligible(conn, a2))
            state.update_job(conn, "formal-b-v1", status="done", rc=0)
            state.update_job(conn, "formal-c-v1", status="running")
            self.assertFalse(recovery_state.eligible(conn, a2))
            state.update_job(conn, "formal-c-v1", status="done", rc=0)
            self.assertTrue(recovery_state.eligible(conn, a2))
            state.insert_task(conn, "formal", "b", 2, dict(spec, id="b"), 1, "p")
            state.insert_job(conn, "formal-b-v2", "formal", "b", 2, spec["_recovery_fingerprint"], project="p")
            # Another completed ordinary group may independently defer.
            bspec = dict(spec, id="b")
            recovery.freeze(bspec, spec["_recovery_fingerprint"])
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id='formal' AND id='b' AND version=2", (json.dumps(bspec),))
            b = dict(state.get_job(conn, "formal-b-v2"))
        self.assertTrue(self.oom(b, bspec))
        b3 = self.latest(task="b")
        with state.connect() as conn:
            self.assertTrue(recovery_state.eligible(conn, a2))
            self.assertFalse(recovery_state.eligible(conn, b3))
            state.update_job(conn, a2["id"], status="running")
            a2spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id='formal' AND id='task' AND version=2").fetchone()[0])
        self.assertTrue(self.oom(a2, a2spec, payload={"next": 3}))
        a3 = self.latest()
        # A fresh database connection models restart; order comes only from SQLite.
        with state.connect() as conn:
            self.assertTrue(recovery_state.eligible(conn, b3))
            self.assertFalse(recovery_state.eligible(conn, a3))
            entries = conn.execute("SELECT * FROM recovery_queue ORDER BY seq").fetchall()
            self.assertEqual([a2["id"], b3["id"], a3["id"]], [row["job_id"] for row in entries])
            self.assertEqual(2, entries[-1]["round"])
            self.assertEqual(a["id"], entries[-1]["root_job_id"])

    def test_cooldown_attempt_limit_and_corrupt_or_missing_checkpoint(self):
        for name, patch, checkpoint, expected in (
            ("missing", {}, None, "checkpoint_invalid_or_absent"),
            ("limit", {"max_attempts": 1}, {"step": 1}, "attempt_limit"),
        ):
            spec = self.spec("run")
            spec["recovery"]["retry"].update(patch)
            recovery.freeze(spec, spec["_recovery_fingerprint"])
            job = self.seed(name, spec, "failed")
            if checkpoint is not None:
                self.store(job, spec).save(checkpoint)
            with state.connect() as conn:
                state.update_job(conn, job["id"], rc=42, failure="oom")
                recovery_state.record_clean(conn, job, spec, "oom", "wait")
                self.assertFalse(recovery_state.create_next(conn, state.host_dir(), job, spec))
                self.assertEqual(expected, recovery_state.public(conn, job["id"])["settlement"]["reason"])
        spec = self.spec("run")
        spec["recovery"]["retry"]["cooldown_sec"] = 60
        recovery.freeze(spec, spec["_recovery_fingerprint"])
        job = self.seed("cool", spec)
        self.assertTrue(self.oom(job, spec))
        with state.connect() as conn:
            next_job = self.latest("cool")
            q = recovery_state.public(conn, next_job["id"])["queue"]
            self.assertFalse(recovery_state.eligible(conn, next_job, timestamp=q["not_before"] - .01))
            self.assertTrue(recovery_state.eligible(conn, next_job, timestamp=q["not_before"]))

    def test_pending_cancel_or_newer_manual_version_prevents_automatic_successor(self):
        spec = self.spec("run")
        for name in ("cancel", "newer"):
            job = self.seed(name, spec, "failed")
            self.store(job, spec).save({"step": 1})
            with state.connect() as conn:
                state.update_job(conn, job["id"], rc=42, failure="oom")
                recovery_state.record_clean(conn, job, spec, "oom", "wait")
                if name == "cancel":
                    conn.execute("INSERT INTO control_requests(job_id,created_at) VALUES(?,?)", (job["id"], state.now()))
                else:
                    state.insert_task(conn, name, "task", 2, spec, 0, "p")
                    state.insert_job(conn, f"{name}-task-v2", name, "task", 2, spec["_recovery_fingerprint"], project="p")
                self.assertFalse(recovery_state.create_next(conn, state.host_dir(), job, spec))
                self.assertEqual("cancelled", recovery_state.public(conn, job["id"])["settlement"]["decision"])

    def test_native_unknown_wait_or_unclean_group_cannot_authorize_retry(self):
        spec = self.spec("run")
        job = self.seed("formal", spec, "running")
        with state.connect() as conn:
            identity = execution_state.reserve(conn, job["id"], {"backend_id": "worker", "backend_config_sha256": "1" * 64}, {"sha256": "2" * 64}, "review-node", 123)
            execution_state.launch_intent(conn, job["id"])
            state.update_job(conn, job["id"], status="failed", rc=42, failure="oom")
            for observed in (
                {"status": "authority_lost", "pid": 12345, "returncode": None, "rusage": None, "group_clean": None},
                {"status": "cleanup_pending", "pid": 12345, "returncode": 42, "rusage": {}, "group_clean": False},
            ):
                execution_state.observe(conn, job["id"], observed)
                with self.assertRaises(state.StateError):
                    recovery_state.record_clean(conn, job, spec, "oom", "wait")
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM recovery_settlements").fetchone()[0])
            execution_state.observe(conn, job["id"], {"status": "exited", "pid": 12345, "returncode": 42, "rusage": {}, "group_clean": True})
            recovery_state.record_clean(conn, job, spec, "oom", "wait")
            with self.assertRaises(state.StateError):
                execution_state.launch_intent(conn, job["id"])
            self.assertEqual(identity["attempt_id"], json.loads(execution_state.get(conn, job["id"])["identity"])["attempt_id"])

    def test_group_gone_creates_new_version_without_inventing_original_wait(self):
        spec = self.spec("run")
        job = self.seed("formal", spec, "running")
        self.store(job, spec).save({"step": 1})
        with state.connect() as conn:
            execution_state.reserve(conn, job["id"], {"backend_id": "worker", "backend_config_sha256": "1" * 64}, {"sha256": "2" * 64}, "review-node", 123)
            execution_state.launch_intent(conn, job["id"])
            execution_state.observe(conn, job["id"], {"status": "authority_lost", "pid": 12345, "returncode": None, "rusage": None, "group_clean": True})
            state.update_job(conn, job["id"], status="interrupted", rc=None, failure="execution_authority_lost")
            recovery_state.record_clean(conn, job, spec, "interrupted", "group_gone")
            self.assertTrue(recovery_state.create_next(conn, state.host_dir(), job, spec))
            self.assertIsNone(state.get_job(conn, job["id"])["rc"])
            self.assertEqual("unresolved", execution_state.get(conn, job["id"])["phase"])

    def test_queue_and_settlement_lineage_are_immutable(self):
        spec = self.spec("run")
        job = self.seed("formal", spec)
        self.assertTrue(self.oom(job, spec))
        with state.connect() as conn:
            for sql in ("UPDATE recovery_queue SET round=99", "DELETE FROM recovery_queue", "DELETE FROM recovery_settlements", "UPDATE recovery_settlements SET authority='group_gone'", "UPDATE recovery_settlements SET successor_job_id=NULL"):
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(sql)

    def test_retry_policy_is_explicit_strict_and_bound_to_smoke(self):
        for patch in ({"oom": 1}, {"interrupted": "yes"}, {"cooldown_sec": True}, {"cooldown_sec": float("nan")}, {"cooldown_sec": 10**400}, {"max_attempts": False}, {"max_attempts": -1}, {"oom": False, "interrupted": False}, {"unknown": 1}):
            task = self.task()
            task["recovery"]["retry"] = patch
            with self.assertRaises(SchemaError):
                validate_batch({"name": "x", "project": "p", "tasks": [task]}, self.cfg)
        first = self.spec()
        second = copy.deepcopy(first)
        second["recovery"]["retry"]["cooldown_sec"] = 60
        recovery.freeze(second, second["_recovery_fingerprint"])
        self.assertNotEqual(first["_recovery_binding"], second["_recovery_binding"])

    def test_schema7_query_is_read_only_and_schema8_migration_is_complete(self):
        with state.connect() as conn:
            conn.execute("DROP TRIGGER recovery_queue_retained")
            conn.execute("DROP TRIGGER recovery_settlement_retained")
            conn.execute("DROP TABLE recovery_queue")
            conn.execute("DROP TABLE recovery_settlements")
            conn.execute("PRAGMA user_version=7")
            self.assertTrue(state._schema_is_complete(conn, 7))
            self.assertEqual({"queue": None, "settlement": None}, recovery_state.public(conn, "missing"))
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(state.DB_SCHEMA_VERSION, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertTrue(state._schema_is_complete(conn, state.DB_SCHEMA_VERSION))

    def dispatch_scenario(self, kind=None, restart=True):
        settings = {"total": 5, "oom_at": [], "oom_log": False, "groups": {"a": {"oom_at": [2]}}}
        (self.root / "settings.json").write_text(json.dumps(settings))
        self.cfg["max_cpu_jobs"] = 1
        repo = str(Path(__file__).parent.parent)
        if kind is not None:
            from gsched.execution import BackendUnavailable, LinuxFdBackend
            try:
                LinuxFdBackend()
            except BackendUnavailable:
                if os.environ.get("SCHED_REQUIRE_NATIVE") == "1":
                    raise
                self.skipTest("native backend was not explicitly built")
            executable = os.path.realpath(sys.executable)
            executable_sha = hashlib.sha256(Path(executable).read_bytes()).hexdigest()
            self.cfg["execution_backends"] = {name: {"kind": kind, "executable": executable,
                "sha256": executable_sha, "argv": [executable, "worker.py", name],
                "env": {"PYTHONPATH": repo, "CHECK_IDENTITY_FD": "1"}, "projects": ["p"], "input_slots": {}}
                for name in ("a", "b", "c")}
        Path(self.config_path).write_text(json.dumps(self.cfg))
        with mock.patch.dict(os.environ, {"PYTHONPATH": repo}):
            dispatcher = Dispatcher(self.cfg, fake=True)
            self.addCleanup(dispatcher.log.close)
            groups = []
            for name in ("a", "b", "c"):
                spec = self.spec()
                spec["id"] = name
                spec["cmd"].append(name)
                fingerprint_options = {}
                if kind is not None:
                    from gsched.execution_policy import normalize_execution
                    spec["cmd"] = self.cfg["execution_backends"][name]["argv"]
                    spec["execution"] = {"backend": name, "inputs": {}}
                    spec["_execution_binding"] = normalize_execution(spec, self.cfg, "p", {})
                    fingerprint_options["execution_binding_sha256"] = recovery.digest(spec["_execution_binding"])
                fp, _, _ = compute_fingerprint(spec["cmd"], None, spec["cwd_abs"], False, {}, artifacts=spec["artifacts"], **fingerprint_options)
                recovery.freeze(spec, fp)
                groups.append(spec)
                with state.connect() as conn:
                    if state.get_batch(conn, "smoke") is None:
                        state.insert_batch(conn, "smoke", "smoke", "mix", [], None, str(self.root), {}, project="p")
                        conn.execute("UPDATE batches SET status='active' WHERE id='smoke'")
                    state.insert_task(conn, "smoke", name, 1, spec, len(groups), "p")
                    state.insert_job(conn, f"smoke-{name}-v1", "smoke", name, 1, fp, project="p")
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                dispatcher._tick()
                with state.connect() as conn:
                    if state.get_batch(conn, "smoke")["status"] == "done":
                        break
                time.sleep(.03)
            else:
                self.fail((Path(state.host_dir()) / "scheduler.log").read_text())
            with state.connect() as conn:
                state.insert_batch(conn, "formal", "formal", "mix", [], None, str(self.root), {}, project="p")
                conn.execute("UPDATE batches SET status='active' WHERE id='formal'")
                for index, smoke_spec in enumerate(groups):
                    name = smoke_spec["id"]
                    spec = copy.deepcopy(smoke_spec)
                    spec["recovery"].update(mode="run", smoke_job_id=f"smoke-{name}-v1")
                    state.insert_task(conn, "formal", name, 1, spec, index, "p")
                    state.insert_job(conn, f"formal-{name}-v1", "formal", name, 1, spec["_recovery_fingerprint"], project="p")
            restarted = False
            retired_executors = []
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                # The test remains the OS parent after object reconstruction.
                # Reap zombies as the host reaper would after a real daemon exit;
                # do not transfer these wait results to the new dispatcher.
                for retired in retired_executors:
                    for process in retired._procs.values():
                        process.poll()
                dispatcher._tick()
                with state.connect() as conn:
                    queued = conn.execute("SELECT * FROM recovery_queue").fetchone()
                    if queued and restart and not restarted:
                        restarted = True
                        retired_executors.append(dispatcher.executor)
                        dispatcher.log.close()
                        dispatcher = Dispatcher(self.cfg, fake=True)
                        self.addCleanup(dispatcher.log.close)
                        dispatcher._adopt_running()
                    if state.get_batch(conn, "formal")["status"] == "done":
                        rows = conn.execute("SELECT * FROM jobs WHERE batch_id='formal' ORDER BY started_at,rowid").fetchall()
                        break
                time.sleep(.03)
            else:
                self.fail((Path(state.host_dir()) / "scheduler.log").read_text())
            self.assertEqual(restart, restarted)
            self.assertEqual(["formal-a-v1", "formal-b-v1", "formal-c-v1", "formal-a-v2"], [row["id"] for row in rows])
            self.assertEqual(["failed", "done", "done", "done"], [row["status"] for row in rows])
            self.assertEqual(42, rows[0]["rc"])
            self.assertEqual("oom", rows[0]["failure"])
            self.assertEqual(0, rows[-1]["rc"])

            if kind is not None:
                with state.connect() as conn:
                    old_attempt = execution_state.get(conn, "formal-a-v1")
                    new_attempt = execution_state.get(conn, "formal-a-v2")
                    self.assertNotEqual(old_attempt["attempt_id"], new_attempt["attempt_id"])
                    self.assertEqual("exited", old_attempt["phase"])
                    self.assertEqual(42, json.loads(old_attempt["observation"])["returncode"])
                    self.assertEqual(0, json.loads(new_attempt["observation"])["returncode"])

    def test_actual_dispatcher_runs_a_oom_then_b_c_then_a_resume_after_restart(self):
        self.dispatch_scenario()

    def test_linux_fd_oom_starts_fresh_attempt_preserving_original_wait(self):
        self.dispatch_scenario("linux_fd", restart=False)

    def test_persistent_owner_queue_survives_dispatcher_restart_with_original_wait(self):
        self.dispatch_scenario("linux_fd_owner")

    def test_interruption_before_first_checkpoint_can_restart_initial_state_but_not_lose_old_progress(self):
        spec = self.spec("run")
        job = self.seed("formal", spec, "interrupted")
        with state.connect() as conn:
            recovery_state.record_clean(conn, job, spec, "interrupted", "group_gone")
            self.assertTrue(recovery_state.create_next(conn, state.host_dir(), job, spec))
        next_job = self.latest()
        with state.connect() as conn:
            next_spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id='formal' AND version=2").fetchone()[0])
        self.assertTrue(self.oom(next_job, next_spec))
        next_job = self.latest()
        with state.connect() as conn:
            next_spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id='formal' AND version=3").fetchone()[0])
        value = json.loads(recovery.context(state.host_dir(), next_job, next_spec))
        (Path(value["checkpoint_dir"]) / "checkpoint.json").unlink()
        with state.connect() as conn:
            state.update_job(conn, next_job["id"], status="interrupted", rc=None)
            recovery_state.record_clean(conn, next_job, next_spec, "interrupted", "group_gone")
            self.assertFalse(recovery_state.create_next(conn, state.host_dir(), next_job, next_spec))

    def test_corrupt_checkpoint_denies_successor_and_history_query_exposes_decision(self):
        spec = self.spec("run")
        job = self.seed("formal", spec, "failed")
        store = self.store(job, spec)
        store.save({"step": 1})
        value = json.loads(recovery.context(state.host_dir(), job, spec))
        checkpoint = Path(value["checkpoint_dir"]) / "checkpoint.json"
        payload = json.loads(checkpoint.read_text())
        payload["payload_sha256"] = "0" * 64
        checkpoint.write_text(json.dumps(payload))
        with state.connect() as conn:
            state.update_job(conn, job["id"], rc=42, failure="oom")
            recovery_state.record_clean(conn, job, spec, "oom", "wait")
            self.assertFalse(recovery_state.create_next(conn, state.host_dir(), job, spec))
        code, output, error = self.capture(cli.cmd_recovery, argparse.Namespace(task="formal:task", json=True, version=1))
        self.assertEqual(0, code, error)
        output = json.loads(output)
        self.assertEqual("denied", output["settlement"]["decision"])
        self.assertEqual("invalid", output["checkpoint"]["state"])
        self.assertNotIn(str(self.root), json.dumps(output))

    def test_consumed_preparation_can_authorize_new_version_without_starting_old_attempt(self):
        spec = self.spec("run")
        job = self.seed("formal", spec, "running")
        with state.connect() as conn:
            identity = execution_state.reserve(conn, job["id"], {"backend_id": "worker", "backend_config_sha256": "1" * 64}, {"sha256": "2" * 64}, "review-node", 123)
            execution_state.observe(conn, job["id"], {"status": "not_started", "pid": None, "returncode": None, "rusage": None, "group_clean": True})
            state.update_job(conn, job["id"], status="interrupted", rc=None, failure="execution_not_started")
            recovery_state.record_clean(conn, job, spec, "interrupted", "not_started")
            self.assertTrue(recovery_state.create_next(conn, state.host_dir(), job, spec))
            old = execution_state.get(conn, job["id"])
            self.assertEqual("not_started", old["phase"])
            self.assertIsNone(old["launch_intent_at"])
            self.assertEqual(identity, json.loads(old["identity"]))
            with self.assertRaises(state.StateError):
                execution_state.launch_intent(conn, job["id"])

    def test_smoke_binding_includes_resources_limits_and_probes(self):
        spec = self.spec("run")
        for key, changed in (("resources", {"gpus": 1, "vram_gib": 12}), ("duration_min", 10), ("probes", [{"cmd": "false"}]), ("max_parallel", 2)):
            with self.subTest(key=key):
                altered = copy.deepcopy(spec)
                altered[key] = changed
                with self.assertRaises(recovery.RecoveryError):
                    recovery.verify_binding(altered, spec["_recovery_fingerprint"])
        successor = dict(spec, _force_rerun=True)
        recovery.verify_binding(successor, spec["_recovery_fingerprint"])
