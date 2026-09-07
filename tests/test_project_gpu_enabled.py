from __future__ import annotations

import argparse
import concurrent.futures
import copy
import json
import os
import threading
from unittest import mock

from gsched import cli, config, state
from gsched.dispatcher import Dispatcher
from gsched.schema import SchemaError, validate_batch
from test_review_cli_state import TempStateCase
from test_review_dispatcher_gpu import DispatcherStateCase


class ProjectGpuAdmissionTests(TempStateCase):
    def write_config(self):
        with open(self.config_path, "w") as stream:
            json.dump(self.cfg, stream)

    def set_enabled(self, enabled):
        patch = os.path.join(self.tmp.name, "gpu-policy.json")
        with open(patch, "w") as stream:
            json.dump({"projects": {"p": {"gpu_enabled": enabled}}}, stream)
        code, _, error = self.capture(cli.cmd_config_set, argparse.Namespace(file=patch, yes=True))
        self.assertEqual(0, code, error)

    def batch_spec(self, gpu=1):
        return {"name": "access", "project": "p", "tasks": [
            {"id": "task", "cmd": ["/bin/true"], "resources": {"gpu": gpu}}
        ]}

    def submit(self, spec, *, dry_run=False):
        path = os.path.join(self.tmp.name, "batch.json")
        with open(path, "w") as stream:
            json.dump(spec, stream)
        return self.capture(cli.cmd_submit, argparse.Namespace(batch=path, dry_run=dry_run, json=dry_run))

    def counts(self):
        with state.connect() as conn:
            return tuple(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                         for table in ("batches", "tasks", "jobs"))

    def seed_gpu_job(self, **kwargs):
        job_id = self.seed_batch(**kwargs)
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            row = conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
                               (job["batch_id"], job["task_id"], job["version"])).fetchone()
            spec = json.loads(row["spec"])
            spec["resources"]["gpu"] = 1
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id=? AND id=? AND version=?",
                         (json.dumps(spec), job["batch_id"], job["task_id"], job["version"]))
        return job_id

    def test_config_switch_is_boolean_and_omission_preserves_unlimited_quota(self):
        for value in (None, 0, 1, "false", [], {}):
            with self.subTest(value=value):
                self.cfg["projects"]["p"]["gpu_enabled"] = value
                self.write_config()
                with self.assertRaisesRegex(config.ConfigError, "gpu_enabled"):
                    config.load_config()
        for value in (True, False):
            self.cfg["projects"]["p"]["gpu_enabled"] = value
            self.write_config()
            self.assertEqual(value, config.project_gpu_enabled(config.load_config(), "p"))
        del self.cfg["projects"]["p"]["gpu_enabled"]
        self.cfg["projects"]["p"]["gpu_quota"] = 0
        self.write_config()
        self.assertTrue(config.project_gpu_enabled(config.load_config(), "p"))
        validate_batch(self.batch_spec(), config.load_config())

    def test_schema_rejects_implicit_gpu_stages_and_sweeps_but_allows_cpu(self):
        self.cfg["projects"]["p"]["gpu_enabled"] = False
        implicit = self.batch_spec()
        del implicit["tasks"][0]["resources"]
        staged = self.batch_spec()
        staged["tasks"][0].pop("cmd")
        staged["tasks"][0]["stages"] = [{"cmd": ["/bin/true"]}]
        sweep = self.batch_spec()
        sweep["sweep"] = {"matrix": {"seed": [1, 2]}}
        for spec in (self.batch_spec(), implicit, staged, sweep):
            with self.subTest(spec=spec), self.assertRaisesRegex(SchemaError, "gpu_enabled=false"):
                validate_batch(spec, self.cfg)
        self.assertEqual(0, validate_batch(self.batch_spec(0), self.cfg)["tasks"][0]["resources"]["gpu"])

    def test_submit_rejects_gpu_without_publishing_any_rows_and_cpu_succeeds(self):
        self.set_enabled(False)
        for dry_run in (False, True):
            code, _, error = self.submit(self.batch_spec(), dry_run=dry_run)
            self.assertEqual(1, code)
            self.assertIn("gpu_enabled", error)
            self.assertEqual((0, 0, 0), self.counts())
        with mock.patch.object(cli, "_ensure_running_locked", return_value="test daemon"):
            self.assertEqual(0, self.submit(self.batch_spec(0))[0])
        self.assertEqual((1, 1, 1), self.counts())

    def test_submit_rechecks_disable_after_fingerprint_preparation(self):
        def disable(*args, **kwargs):
            self.set_enabled(False)
            return "fp", None, None
        with mock.patch("gsched.fingerprint.compute_fingerprint", side_effect=disable):
            code, _, error = self.submit(self.batch_spec())
        self.assertEqual(1, code)
        self.assertIn("gpu_enabled", error)
        self.assertEqual((0, 0, 0), self.counts())

    def gateway_delivery(self, gpu=1):
        with mock.patch.dict(os.environ, {"SCHED_ALLOW_FOREIGN_WRITE": ""}), mock.patch.object(
            cli, "_daemon_health", return_value={}
        ):
            code, output, error = self.submit(self.batch_spec(gpu))
        self.assertEqual(0, code, error)
        bid = output.split("已投递: ", 1)[1].split()[0]
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = copy.deepcopy(self.cfg)
        dispatcher._config_path = self.config_path
        dispatcher.log_line = mock.Mock()
        dispatcher._reload_config_now = mock.Mock(return_value=True)
        dispatcher._drain_submit_inbox()
        return bid, dispatcher

    def test_gateway_gpu_payload_rejected_after_disable_with_cli_receipt(self):
        bid, dispatcher = self.gateway_delivery()
        self.set_enabled(False)
        dispatcher._process_control_requests()
        self.assertEqual((0, 0, 0), self.counts())
        code, output, error = self.capture(cli.cmd_verify, argparse.Namespace(batch=bid))
        self.assertEqual(1, code, error)
        self.assertIn("投递被拒绝", output)
        self.assertIn("gpu_enabled", output)
        with state.connect() as conn:
            row = conn.execute("SELECT status, result, job_id FROM control_requests WHERE op='batch_submit'").fetchone()
        self.assertEqual("done", row["status"])
        self.assertFalse(os.path.exists(row["job_id"]))

    def test_gateway_cpu_survives_disable_and_policy_read_error_retries(self):
        bid, dispatcher = self.gateway_delivery(0)
        self.set_enabled(False)
        with mock.patch.object(dispatcher, "_read_gpu_policy", side_effect=RuntimeError("temporary I/O")):
            dispatcher._process_control_requests()
        with state.connect() as conn:
            row = conn.execute("SELECT status, job_id FROM control_requests WHERE op='batch_submit'").fetchone()
        self.assertEqual("pending", row["status"])
        self.assertTrue(os.path.isfile(row["job_id"]))
        dispatcher._process_control_requests()
        self.assertEqual((1, 1, 1), self.counts())
        self.assertEqual(0, self.capture(cli.cmd_verify, argparse.Namespace(batch=bid))[0])

    def test_duplicate_gateway_receipt_replays_after_gpu_disable(self):
        _, dispatcher = self.gateway_delivery()
        with state.connect() as conn:
            payload = conn.execute("SELECT job_id FROM control_requests WHERE op='batch_submit'").fetchone()[0]
        with open(payload) as stream:
            envelope = json.load(stream)
        dispatcher._process_control_requests()
        self.set_enabled(False)
        replacement = os.path.join(state.submission_inbox_dir(), "duplicate.json")
        with state.open_private_text(replacement, "w") as stream:
            json.dump(envelope, stream)
        with state.connect() as conn:
            request_id = state.insert_control_request(conn, replacement, op="batch_submit")
        dispatcher._process_control_requests()
        self.assertEqual((1, 1, 1), self.counts())
        with state.connect() as conn:
            result = conn.execute("SELECT result FROM control_requests WHERE id=?", (request_id,)).fetchone()[0]
        self.assertIn("重复投递", result)

    def run_args(self, cpu=False, dry_run=False):
        self.cfg["venvs"] = {"test": "/usr/bin/python3"}
        self.write_config()
        return argparse.Namespace(cmd=["--", "echo", "ok"], venv="test", cwd=self.tmp.name,
                                  gpus=None, cpu_only=cpu, cpus=None, duration=None,
                                  out=None, project="p", dry_run=dry_run)

    def test_run_checks_preview_and_final_gate_and_allows_cpu(self):
        args = self.run_args()
        self.set_enabled(False)
        for dry_run in (False, True):
            args.dry_run = dry_run
            self.assertEqual(1, self.capture(cli.cmd_run, args)[0])
        args.cpu_only = True
        args.dry_run = False
        with mock.patch.object(cli, "_ensure_running_locked", return_value="test daemon"):
            self.assertEqual(0, self.capture(cli.cmd_run, args)[0])
        self.assertEqual((1, 1, 1), self.counts())

    def test_run_rechecks_disable_before_insert(self):
        args = self.run_args()
        def disable(*args, **kwargs):
            self.set_enabled(False)
            return "fp", None, None
        with mock.patch("gsched.fingerprint.compute_fingerprint", side_effect=disable):
            self.assertEqual(1, self.capture(cli.cmd_run, args)[0])
        self.assertEqual((0, 0, 0), self.counts())

    def test_retry_and_resubmit_reject_gpu_without_reopening_or_new_version(self):
        self.seed_gpu_job(batch_status="blocked")
        self.set_enabled(False)
        revision = self.batch_revision()
        for reference in ("batch", "batch:task"):
            self.assertEqual(1, self.capture(cli.cmd_retry, argparse.Namespace(task=reference))[0])
            for dry_run in (False, True):
                args = argparse.Namespace(task=reference, failed=":" not in reference,
                                          resubmit_all=False, dry_run=dry_run)
                self.assertEqual(1, self.capture(cli.cmd_resubmit, args)[0])
        self.assertEqual(revision, self.batch_revision())
        self.assertEqual((1, 1, 1), self.counts())

    def test_cpu_retry_is_allowed_in_disabled_project(self):
        self.seed_batch(batch_status="blocked")
        self.set_enabled(False)
        with mock.patch.object(cli, "_ensure_running_locked", return_value="test daemon"):
            self.assertEqual(0, self.capture(cli.cmd_retry, argparse.Namespace(task="batch:task"))[0])
        self.assertEqual("pending", self.status_json()["jobs"][0]["status"])

    def test_cpu_resubmit_is_allowed_in_disabled_project(self):
        self.seed_batch(batch_status="blocked")
        self.set_enabled(False)
        args = argparse.Namespace(task="batch:task", failed=False, resubmit_all=False, dry_run=False)
        with mock.patch.object(cli, "_ensure_running_locked", return_value="test daemon"):
            self.assertEqual(0, self.capture(cli.cmd_resubmit, args)[0])
        self.assertEqual((1, 2, 2), self.counts())

    def test_mixed_batch_retry_and_resubmit_are_atomic_when_gpu_disabled(self):
        self.seed_gpu_job(batch_status="blocked", task_id="gpu")
        batch = "batch-20260829-000000"
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks").fetchone()[0])
            spec.update(id="cpu", resources={"gpu": 0})
            state.insert_task(conn, batch, "cpu", 1, spec, 1, "p")
            state.insert_job(conn, f"{batch}-cpu-v1", batch, "cpu", 1, None, None, "p")
            state.update_job(conn, f"{batch}-cpu-v1", status="failed")
        self.set_enabled(False)
        revision = self.batch_revision()
        self.assertEqual(1, self.capture(cli.cmd_retry, argparse.Namespace(task="batch"))[0])
        args = argparse.Namespace(task="batch", failed=True, resubmit_all=False, dry_run=False)
        self.assertEqual(1, self.capture(cli.cmd_resubmit, args)[0])
        self.assertEqual(revision, self.batch_revision())
        self.assertEqual((1, 2, 2), self.counts())
        with state.connect() as conn:
            self.assertEqual(["failed", "failed"], [row[0] for row in conn.execute("SELECT status FROM jobs")])

    def test_resubmit_rechecks_after_fingerprint_preparation(self):
        self.seed_gpu_job(batch_status="blocked")
        def disable(*args, **kwargs):
            self.set_enabled(False)
            return "fp", None, None
        args = argparse.Namespace(task="batch:task", failed=False, resubmit_all=False, dry_run=False)
        with mock.patch("gsched.fingerprint.compute_fingerprint", side_effect=disable):
            self.assertEqual(1, self.capture(cli.cmd_resubmit, args)[0])
        self.assertEqual((1, 1, 1), self.counts())

    def test_project_json_and_status_distinguish_disabled_unlimited_and_limited(self):
        self.cfg["projects"]["p"]["gpu_enabled"] = False
        self.cfg["projects"]["unlimited"] = {"root": self.tmp.name, "gpu_quota": 0}
        self.cfg["projects"]["limited"] = {"root": self.tmp.name, "gpu_quota": 2}
        self.write_config()
        self.seed_gpu_job(job_status="pending")
        code, output, error = self.capture(cli.main, ["project", "list", "--json"])
        self.assertEqual(0, code, error)
        rows = {row["name"]: row for row in json.loads(output)["projects"]}
        self.assertEqual(["disabled", "unlimited", "limited"],
                         [rows[name]["gpu_access"] for name in ("p", "unlimited", "limited")])
        self.assertFalse(rows["p"]["gpu_enabled"])
        self.assertEqual("project_gpu_disabled", self.status_json()["jobs"][0]["wait_reason"])
        self.assertIn("禁用", self.capture(cli.cmd_project_list, argparse.Namespace(json=False))[1])


class ProjectGpuDispatchTests(DispatcherStateCase):
    def policy(self, enabled):
        cfg = copy.deepcopy(self.cfg)
        cfg["projects"]["p"]["gpu_enabled"] = enabled
        with open(self.config_path, "w") as stream:
            json.dump(cfg, stream)

    def fake_dispatcher(self):
        dispatcher = self.dispatcher()
        dispatcher._assign_in_tx = mock.Mock(return_value=1)
        launched = []
        def launch(conn, job, gpu):
            launched.append(job["id"])
            state.update_job(conn, job["id"], status="running", gpu=gpu)
            return True
        dispatcher._launch_job = mock.Mock(side_effect=launch)
        return dispatcher, launched

    def test_disabled_queue_holds_cpu_continues_running_survives_and_enable_resumes(self):
        self.cfg["projects"]["p"]["gpu_quota"] = 0
        self.seed_jobs([("live", {"gpu": 1}, "running"),
                        ("held", {"gpu": 1}, "pending"), ("cpu", {"gpu": 0}, "pending")])
        self.policy(False)  # The daemon's cached self.cfg still says enabled.
        dispatcher, launched = self.fake_dispatcher()
        dispatcher._dispatch_ready_jobs()
        self.assertEqual(["cpu"], launched)
        dispatcher._assign_in_tx.assert_not_called()
        with state.connect() as conn:
            self.assertEqual("running", state.get_job(conn, "live")["status"])
            self.assertEqual("pending", state.get_job(conn, "held")["status"])
        self.policy(True)
        dispatcher._dispatch_ready_jobs()
        self.assertEqual(["cpu", "held"], launched)

    def test_unreadable_policy_pauses_gpu_but_not_cpu(self):
        self.seed_jobs([("gpu", {"gpu": 1}, "pending"), ("cpu", {"gpu": 0}, "pending")])
        with open(self.config_path, "w") as stream:
            stream.write("{")
        dispatcher, launched = self.fake_dispatcher()
        dispatcher._dispatch_ready_jobs()
        self.assertEqual(["cpu"], launched)
        dispatcher._assign_in_tx.assert_not_called()

    def test_recovered_and_auto_retry_jobs_remain_held(self):
        self.seed_jobs([("recovered", {"gpu": 1}, "interrupted"), ("retry", {"gpu": 1}, "failed")])
        self.policy(False)
        dispatcher, launched = self.fake_dispatcher()
        with state.connect() as conn:
            dispatcher._requeue_for_retry(conn, state.get_job(conn, "recovered"))
            Dispatcher._maybe_retry(dispatcher, conn, state.get_job(conn, "retry"))
        dispatcher._dispatch_ready_jobs()
        self.assertEqual([], launched)
        with state.connect() as conn:
            self.assertEqual(["pending", "pending"], [row[0] for row in conn.execute("SELECT status FROM jobs")])

    def test_config_set_waits_for_an_inflight_dispatch_decision(self):
        self.seed_jobs([("gpu", {"gpu": 1}, "pending")])
        dispatcher, launched = self.fake_dispatcher()
        entered, release = threading.Event(), threading.Event()
        def assign(*args):
            entered.set()
            self.assertTrue(release.wait(5))
            return 0
        dispatcher._assign_in_tx.side_effect = assign
        patch = os.path.join(self.tmp.name, "disable.json")
        with open(patch, "w") as stream:
            json.dump({"projects": {"p": {"gpu_enabled": False}}}, stream)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            dispatch = pool.submit(dispatcher._dispatch_ready_jobs)
            self.assertTrue(entered.wait(5))
            update = pool.submit(cli.cmd_config_set, argparse.Namespace(file=patch, yes=True))
            try:
                with self.assertRaises(concurrent.futures.TimeoutError):
                    update.result(timeout=0.1)
            finally:
                release.set()
            dispatch.result(timeout=5)
            self.assertEqual(0, update.result(timeout=5))
        self.assertEqual(["gpu"], launched)
        self.assertFalse(config.project_gpu_enabled(config.load_config(), "p"))
