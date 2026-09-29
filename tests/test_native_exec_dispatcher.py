from __future__ import annotations

import copy
import io
import json
import os
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing
from types import SimpleNamespace
from unittest import mock

from gsched import cli, state
from gsched.dispatcher import (
    CONFIG_COLD_KEYS,
    Dispatcher,
    _native_root_identities,
)
from gsched.fingerprint import compute_fingerprint
from gsched.native_exec import (
    NATIVE_EXEC_PROFILE_V2_SCHEMA,
    NativeExecProfileError,
    native_exec_project_roots,
)
from gsched.native_launch import NativeLaunchUnavailable
from gsched.schema import validate_batch


class NativeExecDispatcherCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state_root = os.path.join(self.tmp.name, "state")
        self.config_path = os.path.join(self.tmp.name, "config.json")
        self.argv = ["/usr/bin/python3", "-I", "-S", "job.py"]
        self.cfg = {
            "schema_version": 1,
            "user": "test",
            "node": "native-dispatcher-node",
            "state_dir": self.state_root,
            "gpus": [],
            "default_project": "p",
            "projects": {
                "p": {
                    "root": self.tmp.name,
                    "git": False,
                    "gpu_quota": 0,
                }
            },
            "venvs": {},
            "task_default_env": {"PYTHONPATH": "/default-poison"},
            "native_exec_profiles": {
                "formal-v1": {
                    "mode": "strict",
                    "project": "p",
                    "batch_name": "native-batch",
                    "task_id": "native-task",
                    "submitted_argv": list(self.argv),
                }
            },
        }
        with open(self.config_path, "w", encoding="utf-8") as stream:
            json.dump(self.cfg, stream)
        self.env = mock.patch.dict(
            os.environ,
            {
                "SCHED_STATE": self.state_root,
                "SCHED_CONFIG": self.config_path,
            },
            clear=False,
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        self._clear_state_caches()
        self.addCleanup(self._clear_state_caches)
        state.init_db()
        self.raw_batch = {
            "name": "native-batch",
            "mode": "strict",
            "project": "p",
            "cwd": self.tmp.name,
            "tasks": [
                {
                    "id": "native-task",
                    "cmd": list(self.argv),
                    "git": False,
                    "resources": {"gpu": 0, "cpus": 1},
                    "max_retry": 0,
                }
            ],
        }

    @staticmethod
    def _clear_state_caches() -> None:
        state._hostname_cache.clear()
        state._hostname_last_good.clear()
        state._pinned_host.clear()

    def _normalized_task(self) -> dict:
        return validate_batch(copy.deepcopy(self.raw_batch), self.cfg)["tasks"][0]

    def _persist_task_spec(self, task: dict) -> dict:
        return {
            "id": task["id"],
            "cmd": task["cmd"],
            "stages": task["stages"],
            "cwd_abs": task["cwd_abs"],
            "git": task["git"],
            "env": task["env"],
            "resources": task["resources"],
            "duration_min": task["duration_min"],
            "max_retry": task["max_retry"],
            "artifacts": task["artifacts"],
            "paths_escape": task.get("paths_escape", False),
            "probes": task.get("probes"),
            "max_parallel": task.get("max_parallel"),
            "_force_rerun": task.get("_force_rerun"),
            "progress_regex": task.get("progress_regex"),
            "runtime": task.get("runtime"),
            "runtime_prefix": task.get("runtime_prefix"),
            "_native_exec_profile_id": task["_native_exec_profile_id"],
            "_native_exec_profile_sha256": task[
                "_native_exec_profile_sha256"
            ],
            "_native_exec_project_root_identity_sha256": task[
                "_native_exec_project_root_identity_sha256"
            ],
            "_native_exec_submitted_argv": list(
                task["_native_exec_submitted_argv"]
            ),
        }

    def _seed_native_job(self) -> tuple[str, dict]:
        task = self._normalized_task()
        spec = self._persist_task_spec(task)
        job_id = "native-job-v1"
        with state.connect() as conn:
            state.insert_batch(
                conn,
                "native-batch-id",
                "native-batch",
                "strict",
                [],
                None,
                self.tmp.name,
                {},
                project="p",
            )
            conn.execute(
                "UPDATE batches SET status='active' WHERE id='native-batch-id'"
            )
            state.insert_task(
                conn,
                "native-batch-id",
                "native-task",
                1,
                spec,
                0,
                "p",
            )
            state.insert_job(
                conn,
                job_id,
                "native-batch-id",
                "native-task",
                1,
                "submission-fingerprint",
                None,
                "p",
            )
        return job_id, spec

    def _seed_v2_candidate(self) -> tuple[str, dict]:
        job_id, spec = self._seed_native_job()
        spec["_native_exec_contract_v2"] = {}
        with state.connect() as conn:
            conn.execute(
                "UPDATE tasks SET spec=? WHERE batch_id=? AND id=? AND version=1",
                (json.dumps(spec), "native-batch-id", "native-task"),
            )
        return job_id, spec

    def _claim_isolated_native_session(self, conn, job_id: str, spec: dict) -> None:
        state.claim_native_session_candidate(
            conn,
            job_id=job_id,
            job_version=1,
            session_id="a" * 32,
            profile_id=spec["_native_exec_profile_id"],
            profile_sha256=spec["_native_exec_profile_sha256"],
            project_root_path=spec["cwd_abs"],
            project_root_identity_sha256=spec[
                "_native_exec_project_root_identity_sha256"
            ],
        )

    def _seed_isolated_native_session(self) -> str:
        job_id, spec = self._seed_v2_candidate()
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._claim_isolated_native_session(conn, job_id, spec)
        return job_id

    def _dispatcher(self) -> Dispatcher:
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher._native_exec_project_roots = native_exec_project_roots(
            self.cfg
        )
        dispatcher._native_exec_project_root_identities = (
            _native_root_identities(dispatcher._native_exec_project_roots)
        )
        dispatcher.host_dir = state.host_dir()
        dispatcher.venv_paths = {}
        dispatcher.executor = mock.Mock()
        dispatcher.executor.launch.return_value = 4242
        dispatcher.executor.failed_classify.return_value = ("other", None)
        dispatcher._launch_inflight = {}
        dispatcher._prepare_launch_marker = mock.Mock(return_value=False)
        dispatcher._snapshot_fingerprint = mock.Mock(
            return_value=("launch-fingerprint", {}, None)
        )
        dispatcher._should_skip = mock.Mock(return_value=False)
        dispatcher._drop_job_rc = mock.Mock()
        dispatcher._clean_stale_artifacts = mock.Mock()
        dispatcher._drop_launch_marker = mock.Mock()
        dispatcher.log_line = mock.Mock()
        return dispatcher


class NativeExecDispatcherLaunchTests(NativeExecDispatcherCase):
    def test_v2_cannot_reach_running_claim_without_formal_backend(self) -> None:
        job_id, _spec = self._seed_native_job()
        dispatcher = self._dispatcher()
        dispatcher._reattest_native_launch = mock.Mock(
            return_value={"schema": NATIVE_EXEC_PROFILE_V2_SCHEMA}
        )
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            with self.assertRaises(NativeLaunchUnavailable):
                dispatcher._launch_job(conn, job, None)
        dispatcher._prepare_launch_marker.assert_not_called()
        dispatcher.executor.launch.assert_not_called()
        with state.connect() as conn:
            self.assertEqual("pending", state.get_job(conn, job_id)["status"])

    def test_clean_rejects_terminal_strict_batch_without_requeue(self) -> None:
        job_id, _spec = self._seed_native_job()
        with state.connect() as conn:
            conn.execute(
                "UPDATE batches SET status='done' WHERE id='native-batch-id'"
            )
            conn.execute(
                "UPDATE jobs SET status='skip' WHERE id=?", (job_id,)
            )
        with mock.patch("sys.stderr", new_callable=io.StringIO) as stderr:
            result = cli.cmd_clean(
                SimpleNamespace(batch="native-batch-id", yes=True)
            )
        self.assertEqual(1, result)
        self.assertIn("strict native", stderr.getvalue())
        with state.connect() as conn:
            self.assertEqual("skip", state.get_job(conn, job_id)["status"])
            self.assertEqual(
                "submission-fingerprint", state.get_job(conn, job_id)["fingerprint"]
            )

    def test_native_profile_registry_is_a_daemon_cold_key(self) -> None:
        self.assertIn("native_exec_profiles", CONFIG_COLD_KEYS)

    def test_daemon_reload_rejects_referenced_root_but_allows_quota(self) -> None:
        dispatcher = self._dispatcher()
        dispatcher._config_path = self.config_path

        changed_root = copy.deepcopy(self.cfg)
        new_root = os.path.join(self.tmp.name, "new-root")
        os.mkdir(new_root)
        changed_root["projects"]["p"]["root"] = new_root
        with open(self.config_path, "w", encoding="utf-8") as stream:
            json.dump(changed_root, stream)
        self.assertFalse(dispatcher._reload_config_now())
        self.assertEqual(self.tmp.name, dispatcher.cfg["projects"]["p"]["root"])

        changed_quota = copy.deepcopy(self.cfg)
        changed_quota["projects"]["p"]["gpu_quota"] = 1
        with open(self.config_path, "w", encoding="utf-8") as stream:
            json.dump(changed_quota, stream)
        self.assertTrue(dispatcher._reload_config_now())
        self.assertEqual(1, dispatcher.cfg["projects"]["p"]["gpu_quota"])

    def test_launch_reattests_persisted_binding_and_passes_trusted_tuple(self) -> None:
        job_id, spec = self._seed_native_job()
        dispatcher = self._dispatcher()
        dispatcher._ready_task_specs = {job_id: copy.deepcopy(spec)}
        dispatcher._ready_fingerprint_snapshots = {
            job_id: ("launch-fingerprint", {}, None)
        }

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            self.assertTrue(dispatcher._launch_job(conn, job, None))

        kwargs = dispatcher.executor.launch.call_args.kwargs
        self.assertEqual("formal-v1", kwargs["native_exec_profile_id"])
        self.assertEqual(
            spec["_native_exec_profile_sha256"],
            kwargs["native_exec_profile_sha256"],
        )
        self.assertEqual(self.argv, kwargs["native_exec_submitted_argv"])
        self.assertNotIn("PYTHONPATH", kwargs["env"])
        with state.connect() as conn:
            self.assertEqual("running", state.get_job(conn, job_id)["status"])

    def test_every_persisted_native_tuple_tamper_fails_before_claim(self) -> None:
        mutations = {
            "missing_digest": lambda spec: spec.pop(
                "_native_exec_profile_sha256"
            ),
            "missing_root_identity": lambda spec: spec.pop(
                "_native_exec_project_root_identity_sha256"
            ),
            "profile_id": lambda spec: spec.__setitem__(
                "_native_exec_profile_id", "other-v1"
            ),
            "profile_digest": lambda spec: spec.__setitem__(
                "_native_exec_profile_sha256", "0" * 64
            ),
            "root_identity_digest": lambda spec: spec.__setitem__(
                "_native_exec_project_root_identity_sha256", "0" * 64
            ),
            "submitted_argv": lambda spec: (
                spec.__setitem__(
                    "cmd", self.argv + ["--changed"]
                ),
                spec.__setitem__(
                    "_native_exec_submitted_argv",
                    self.argv + ["--changed"],
                ),
            ),
            "cwd": lambda spec: spec.__setitem__(
                "cwd_abs", os.path.join(self.tmp.name, "other")
            ),
            "runtime": lambda spec: (
                spec.__setitem__("runtime", {"prefix": self.tmp.name}),
                spec.__setitem__("runtime_prefix", self.tmp.name),
            ),
            "git_true": lambda spec: spec.__setitem__("git", True),
            "git_missing": lambda spec: spec.pop("git"),
            "artifacts": lambda spec: spec.__setitem__(
                "artifacts",
                {"out": {"path": "would-be-cleaned.json"}},
            ),
            "max_retry": lambda spec: spec.__setitem__("max_retry", 1),
            "probes": lambda spec: spec.__setitem__(
                "probes", {"ready_on_log": "READY"}
            ),
            "task_env": lambda spec: spec.__setitem__(
                "env", {"PYTHONPATH": "/poison"}
            ),
            "resources": lambda spec: spec.__setitem__(
                "resources",
                {"gpu": 1, "cpus": 1, "gpu_share": False},
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                job_id, spec = self._seed_native_job()
                mutate(spec)
                with state.connect() as conn:
                    conn.execute(
                        "UPDATE tasks SET spec=?"
                        " WHERE batch_id='native-batch-id'"
                        " AND id='native-task' AND version=1",
                        (json.dumps(spec),),
                    )
                dispatcher = self._dispatcher()
                with state.connect() as conn:
                    job = state.get_job(conn, job_id)
                    with self.assertRaises(NativeExecProfileError):
                        dispatcher._launch_job(conn, job, None)
                dispatcher.executor.launch.assert_not_called()
                with state.connect() as conn:
                    self.assertEqual(
                        "pending", state.get_job(conn, job_id)["status"]
                    )
                with state.connect() as conn:
                    conn.execute("DELETE FROM jobs")
                    conn.execute("DELETE FROM tasks")
                    conn.execute("DELETE FROM batches")

    def test_persisted_native_batch_env_fails_before_claim(self) -> None:
        job_id, _spec = self._seed_native_job()
        with state.connect() as conn:
            conn.execute(
                "UPDATE batches SET env=? WHERE id='native-batch-id'",
                (json.dumps({"PATH": "/poison"}),),
            )
        dispatcher = self._dispatcher()

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            with self.assertRaises(NativeExecProfileError):
                dispatcher._launch_job(conn, job, None)

        dispatcher.executor.launch.assert_not_called()
        with state.connect() as conn:
            self.assertEqual("pending", state.get_job(conn, job_id)["status"])

    def test_project_root_identity_drift_fails_before_claim(self) -> None:
        job_id, _spec = self._seed_native_job()
        dispatcher = self._dispatcher()
        dispatcher._native_exec_project_root_identities["p"] = (-1, -1)

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            with self.assertRaises(NativeExecProfileError):
                dispatcher._launch_job(conn, job, None)

        dispatcher.executor.launch.assert_not_called()
        with state.connect() as conn:
            self.assertEqual("pending", state.get_job(conn, job_id)["status"])

    def test_config_and_cached_spec_drift_fail_before_claim(self) -> None:
        for label in ("config", "cache"):
            with self.subTest(label=label):
                job_id, spec = self._seed_native_job()
                dispatcher = self._dispatcher()
                if label == "config":
                    changed_cfg = copy.deepcopy(self.cfg)
                    changed_cfg["native_exec_profiles"]["formal-v1"][
                        "submitted_argv"
                    ].append("--admin-drift")
                    dispatcher.cfg = changed_cfg
                else:
                    cached = copy.deepcopy(spec)
                    cached["resources"]["cpus"] += 1
                    dispatcher._ready_task_specs = {job_id: cached}
                with state.connect() as conn:
                    job = state.get_job(conn, job_id)
                    with self.assertRaises(NativeExecProfileError):
                        dispatcher._launch_job(conn, job, None)
                dispatcher.executor.launch.assert_not_called()
                with state.connect() as conn:
                    self.assertEqual(
                        "pending", state.get_job(conn, job_id)["status"]
                    )
                    conn.execute("DELETE FROM jobs")
                    conn.execute("DELETE FROM tasks")
                    conn.execute("DELETE FROM batches")

    def test_strict_task_without_tuple_fails_before_claim(self) -> None:
        job_id, spec = self._seed_native_job()
        for key in (
            "_native_exec_profile_id",
            "_native_exec_profile_sha256",
            "_native_exec_project_root_identity_sha256",
            "_native_exec_submitted_argv",
        ):
            spec.pop(key)
        with state.connect() as conn:
            conn.execute(
                "UPDATE tasks SET spec=? WHERE batch_id='native-batch-id'",
                (json.dumps(spec),),
            )
        dispatcher = self._dispatcher()

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            with self.assertRaises(NativeExecProfileError):
                dispatcher._launch_job(conn, job, None)

        dispatcher.executor.launch.assert_not_called()
        with state.connect() as conn:
            self.assertEqual("pending", state.get_job(conn, job_id)["status"])

    def test_normal_launch_does_not_pass_empty_native_parameters(self) -> None:
        spec = {
            "id": "normal-task",
            "cmd": ["/bin/true"],
            "stages": None,
            "cwd_abs": self.tmp.name,
            "git": False,
            "env": {},
            "resources": {"gpu": 0, "cpus": 1},
            "artifacts": {},
        }
        with state.connect() as conn:
            state.insert_batch(
                conn,
                "normal-batch-id",
                "normal-batch",
                "mix",
                [],
                None,
                self.tmp.name,
                {},
                project="p",
            )
            conn.execute("UPDATE batches SET status='active' WHERE id='normal-batch-id'")
            state.insert_task(
                conn, "normal-batch-id", "normal-task", 1, spec, 0, "p"
            )
            state.insert_job(
                conn,
                "normal-job-v1",
                "normal-batch-id",
                "normal-task",
                1,
                None,
                None,
                "p",
            )
        dispatcher = self._dispatcher()
        with state.connect() as conn:
            job = state.get_job(conn, "normal-job-v1")
            self.assertTrue(dispatcher._launch_job(conn, job, None))

        kwargs = dispatcher.executor.launch.call_args.kwargs
        for key in (
            "native_exec_profile_id",
            "native_exec_profile_sha256",
            "native_exec_submitted_argv",
        ):
            self.assertNotIn(key, kwargs)

    def test_snapshot_binds_only_nonempty_native_digest(self) -> None:
        dispatcher = self._dispatcher()
        dispatcher._snapshot_fingerprint = Dispatcher._snapshot_fingerprint.__get__(
            dispatcher, Dispatcher
        )
        base = {
            "cmd": ["/bin/true"],
            "stages": None,
            "git": False,
        }
        with mock.patch(
            "gsched.dispatcher.compute_fingerprint",
            return_value=("fp", None, None),
        ) as fingerprint:
            dispatcher._snapshot_fingerprint(base, self.tmp.name, "normal")
            self.assertNotIn(
                "native_exec_profile_sha256",
                fingerprint.call_args.kwargs,
            )
            with_empty = dict(base, _native_exec_profile_sha256="")
            dispatcher._snapshot_fingerprint(
                with_empty, self.tmp.name, "empty"
            )
            self.assertNotIn(
                "native_exec_profile_sha256",
                fingerprint.call_args.kwargs,
            )
            with_digest = dict(base, _native_exec_profile_sha256="a" * 64)
            with_digest[
                "_native_exec_project_root_identity_sha256"
            ] = "b" * 64
            dispatcher._snapshot_fingerprint(
                with_digest, self.tmp.name, "native"
            )
            self.assertEqual(
                "a" * 64,
                fingerprint.call_args.kwargs[
                    "native_exec_profile_sha256"
                ],
            )
            self.assertEqual(
                "b" * 64,
                fingerprint.call_args.kwargs[
                    "native_exec_project_root_identity_sha256"
                ],
            )

    def test_native_snapshot_never_probes_git(self) -> None:
        dispatcher = self._dispatcher()
        dispatcher._snapshot_fingerprint = Dispatcher._snapshot_fingerprint.__get__(
            dispatcher, Dispatcher
        )
        spec = self._normalized_task()

        with mock.patch(
            "gsched.fingerprint._git_worktree_state",
            side_effect=AssertionError("native launch must not probe git"),
        ) as git_probe:
            fingerprint, stage_fingerprints, revision = (
                dispatcher._snapshot_fingerprint(
                    spec,
                    self.tmp.name,
                    "native-no-git",
                )
            )

        self.assertIsNotNone(fingerprint)
        self.assertEqual({}, stage_fingerprints)
        self.assertIsNone(revision)
        git_probe.assert_not_called()

    def test_node_restart_interruption_cannot_requeue_native_job(self) -> None:
        job_id, _spec = self._seed_native_job()
        dispatcher = self._dispatcher()
        with state.connect() as conn:
            state.update_job(
                conn,
                job_id,
                status="interrupted",
                pgid=4242,
                started_at=state.now(),
            )
            job = state.get_job(conn, job_id)
            dispatcher._requeue_for_retry(conn, job)

        with state.connect() as conn:
            settled = state.get_job(conn, job_id)
        self.assertEqual("blocked", settled["status"])
        self.assertEqual(
            "interrupted_native_no_retry", settled["failure"]
        )
        self.assertIsNone(settled["pgid"])

    def test_failure_retry_path_ignores_tampered_native_max_retry(self) -> None:
        job_id, spec = self._seed_native_job()
        spec["max_retry"] = 99
        dispatcher = self._dispatcher()
        with state.connect() as conn:
            conn.execute(
                "UPDATE tasks SET spec=? WHERE batch_id='native-batch-id'",
                (json.dumps(spec),),
            )
            state.update_job(
                conn,
                job_id,
                status="failed",
                failure="launch",
                retries=0,
            )
            job = state.get_job(conn, job_id)
            dispatcher._maybe_retry(conn, job)

        with state.connect() as conn:
            settled = state.get_job(conn, job_id)
        self.assertEqual("blocked", settled["status"])
        self.assertEqual(0, settled["retries"])

    def test_native_reap_ignores_forged_zero_rc_while_popen_is_owned(self) -> None:
        job_id, _spec = self._seed_native_job()
        dispatcher = self._dispatcher()
        dispatcher.executor.has_process.return_value = True
        dispatcher.executor.poll_rc.return_value = 7
        with state.connect() as conn:
            state.update_job(
                conn,
                job_id,
                status="running",
                pgid=4242,
                started_at=state.now(),
            )
            job = state.get_job(conn, job_id)
        rc_path = dispatcher._job_rc_path(job)
        assert rc_path is not None
        state.ensure_private_directory(os.path.dirname(rc_path))
        with open(rc_path, "w", encoding="utf-8") as stream:
            stream.write("0\n")

        dispatcher._reap_finished_jobs()

        with state.connect() as conn:
            settled = state.get_job(conn, job_id)
        self.assertEqual("blocked", settled["status"])
        self.assertEqual(7, settled["rc"])
        self.assertFalse(os.path.exists(rc_path))

    def test_native_adoption_rejects_forged_zero_rc_without_popen(self) -> None:
        job_id, _spec = self._seed_native_job()
        dispatcher = self._dispatcher()
        dispatcher._job_process_state = mock.Mock(return_value="dead")
        with state.connect() as conn:
            state.update_job(
                conn,
                job_id,
                status="running",
                pgid=4242,
                started_at=state.now(),
            )
            job = state.get_job(conn, job_id)
        rc_path = dispatcher._job_rc_path(job)
        assert rc_path is not None
        state.ensure_private_directory(os.path.dirname(rc_path))
        with open(rc_path, "w", encoding="utf-8") as stream:
            stream.write("0\n")

        dispatcher._adopt_running()

        with state.connect() as conn:
            settled = state.get_job(conn, job_id)
        self.assertEqual("blocked", settled["status"])
        self.assertEqual(137, settled["rc"])
        self.assertEqual("native_rc_authority_lost", settled["failure"])
        self.assertFalse(os.path.exists(rc_path))


class NativeExecDispatcherRecoveryTests(NativeExecDispatcherCase):
    def test_startup_and_tick_adoption_preserve_unowned_session(self) -> None:
        job_id = self._seed_isolated_native_session()
        dispatcher = self._dispatcher()
        dispatcher._read_job_rc = mock.Mock(return_value=0)
        dispatcher._should_skip.return_value = True
        dispatcher._maybe_retry = mock.Mock()
        dispatcher._job_process_state = mock.Mock(return_value="dead")

        dispatcher._adopt_running()
        dispatcher._adopt_running(unidentified_only=True)
        dispatcher._reap_finished_jobs()

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            batch = state.get_batch(conn, "native-batch-id")
            job_count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        self.assertEqual("running", job["status"])
        self.assertIsNone(job["pgid"])
        self.assertIsNone(job["rc"])
        self.assertIsNone(job["finished_at"])
        self.assertEqual(0, job["retries"])
        self.assertEqual("active", batch["status"])
        self.assertEqual(1, job_count)
        dispatcher._prepare_launch_marker.assert_not_called()
        dispatcher._job_process_state.assert_not_called()
        dispatcher._read_job_rc.assert_not_called()
        dispatcher._should_skip.assert_not_called()
        dispatcher._maybe_retry.assert_not_called()
        dispatcher.executor.has_process.assert_not_called()
        dispatcher._drop_launch_marker.assert_not_called()

    def test_pending_cancel_survives_unidentified_tick_adoption(self) -> None:
        job_id = self._seed_isolated_native_session()
        dispatcher = self._dispatcher()
        with state.connect() as conn:
            request_id = state.insert_control_request(conn, job_id)

        # Tick processes control requests before adopting pgid-less jobs.
        original_get_job = state.get_job

        def read_job_under_writer(conn, current_id):
            self.assertEqual(1, state._submission_lock_depth.get())
            self.assertTrue(conn.in_transaction)
            with closing(sqlite3.connect(state.db_path(), timeout=0.0)) as other:
                with self.assertRaises(sqlite3.OperationalError):
                    other.execute("BEGIN IMMEDIATE")
            return original_get_job(conn, current_id)

        with mock.patch.object(state, "get_job", side_effect=read_job_under_writer):
            dispatcher._process_control_requests()
        dispatcher._adopt_running(unidentified_only=True)
        dispatcher._reap_finished_jobs()

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            request = conn.execute(
                "SELECT status FROM control_requests WHERE id=?", (request_id,)
            ).fetchone()
        self.assertEqual("running", job["status"])
        self.assertIsNone(job["pgid"])
        self.assertEqual("cancelled", job["kill_reason"])
        self.assertIsNone(job["rc"])
        self.assertIsNone(job["finished_at"])
        self.assertEqual("pending", request["status"])
        dispatcher._prepare_launch_marker.assert_not_called()
        dispatcher._drop_launch_marker.assert_not_called()

    def test_native_cancel_releases_previous_writer_before_submission_gate(self) -> None:
        job_id = self._seed_isolated_native_session()
        dispatcher = self._dispatcher()
        with state.connect() as conn:
            missing_request = state.insert_control_request(conn, "missing-job")
            native_request = state.insert_control_request(conn, job_id)

        original_lock = state.submission_lock
        observed_gate_entries = []

        def assert_writer_released_before_gate():
            with closing(sqlite3.connect(state.db_path(), timeout=0.0)) as other:
                other.execute("BEGIN IMMEDIATE")
                other.rollback()
            observed_gate_entries.append(True)
            return original_lock()

        with mock.patch.object(
            state, "submission_lock", side_effect=assert_writer_released_before_gate
        ):
            dispatcher._process_control_requests()

        self.assertEqual(2, len(observed_gate_entries))
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            requests = conn.execute(
                "SELECT id, status FROM control_requests WHERE id IN (?,?) ORDER BY id",
                (missing_request, native_request),
            ).fetchall()
        self.assertEqual("cancelled", job["kill_reason"])
        self.assertEqual(["done", "pending"], [r["status"] for r in requests])

    def test_native_session_appearing_after_gate_defers_legacy_cancel(self) -> None:
        job_id, spec = self._seed_v2_candidate()
        dispatcher = self._dispatcher()
        dispatcher._signal_job_result = mock.Mock()
        with state.connect() as conn:
            request_id = state.insert_control_request(conn, job_id)

        original_commit = dispatcher._commit_native_cancel_under_gate

        def claim_after_gate(current_request_id):
            committed = original_commit(current_request_id)
            self.assertFalse(committed)
            with state.submission_lock():
                with state.connect() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    self._claim_isolated_native_session(conn, job_id, spec)
            return committed

        with mock.patch.object(
            dispatcher, "_commit_native_cancel_under_gate", side_effect=claim_after_gate
        ):
            dispatcher._process_control_requests()

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            request = conn.execute(
                "SELECT status FROM control_requests WHERE id=?", (request_id,)
            ).fetchone()
        self.assertEqual("running", job["status"])
        self.assertIsNone(job["kill_reason"])
        self.assertEqual("pending", request["status"])
        dispatcher._signal_job_result.assert_not_called()

    def test_cancel_requests_survive_tick_adoption_even_with_stale_pgid(self) -> None:
        job_id = self._seed_isolated_native_session()
        dispatcher = self._dispatcher()
        dispatcher._signal_job_result = mock.Mock()
        with state.connect() as conn:
            # Model a PGID incorrectly recovered from a legacy marker before
            # the native-session fence was installed.
            state.update_job(conn, job_id, pgid=4242)
            first = state.insert_control_request(conn, job_id)
            second = state.insert_control_request(conn, job_id)

        dispatcher._process_control_requests()
        dispatcher._adopt_running()
        dispatcher._reap_finished_jobs()

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            requests = conn.execute(
                "SELECT id, status FROM control_requests WHERE id IN (?,?) ORDER BY id",
                (first, second),
            ).fetchall()
        self.assertEqual("running", job["status"])
        self.assertEqual("cancelled", job["kill_reason"])
        self.assertIsNone(job["rc"])
        self.assertIsNone(job["finished_at"])
        self.assertEqual(["pending", "pending"], [r["status"] for r in requests])
        dispatcher._signal_job_result.assert_not_called()
        dispatcher.executor.has_process.assert_not_called()

    def test_native_timeout_is_durable_without_owner_or_process_group(self) -> None:
        self.raw_batch["tasks"][0]["duration_min"] = 1
        job_id = self._seed_isolated_native_session()
        dispatcher = self._dispatcher()
        dispatcher._signal_job_result = mock.Mock()
        with state.connect() as conn:
            state.update_job(conn, job_id, started_at="2000-01-01 00:00:00")

        dispatcher._check_timeouts()
        dispatcher._check_timeouts()

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            session = state.get_native_session(conn, "a" * 32)
            conn.execute("BEGIN IMMEDIATE")
            with self.assertRaisesRegex(state.StateError, "no longer an active reservation"):
                state.mark_native_session_log_attempted(conn, "a" * 32)
        self.assertEqual("running", job["status"])
        self.assertEqual("timed_out", job["kill_reason"])
        self.assertIsNone(job["pgid"])
        self.assertIsNone(job["rc"])
        self.assertIsNone(job["finished_at"])
        self.assertEqual("reserved", session["phase"])
        self.assertIsNone(session["log_attempted_at"])
        self.assertEqual(1, dispatcher.log_line.call_count)
        dispatcher._signal_job_result.assert_not_called()
        dispatcher.executor.has_process.assert_not_called()

    def test_native_timeout_claims_submission_gate_before_writer(self) -> None:
        self.raw_batch["tasks"][0]["duration_min"] = 1
        job_id = self._seed_isolated_native_session()
        dispatcher = self._dispatcher()
        with state.connect() as conn:
            state.update_job(conn, job_id, started_at="2000-01-01 00:00:00")

        original_mark = state.mark_native_session_timed_out

        def mark_under_gate(conn, **kwargs):
            self.assertEqual(1, state._submission_lock_depth.get())
            self.assertTrue(conn.in_transaction)
            return original_mark(conn, **kwargs)

        with mock.patch.object(
            state, "mark_native_session_timed_out", side_effect=mark_under_gate
        ) as mark:
            dispatcher._check_native_timeouts()

        mark.assert_called_once()
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
        self.assertEqual("timed_out", job["kill_reason"])

    def test_native_timeout_snapshot_loses_to_cancel_before_gate(self) -> None:
        self.raw_batch["tasks"][0]["duration_min"] = 1
        job_id = self._seed_isolated_native_session()
        dispatcher = self._dispatcher()
        with state.connect() as conn:
            state.update_job(conn, job_id, started_at="2000-01-01 00:00:00")

        original_lock = state.submission_lock
        request_ids = []

        def cancel_before_timeout_lock():
            with state.connect() as conn:
                request_ids.append(state.insert_control_request(conn, job_id))
            return original_lock()

        with mock.patch.object(
            state, "submission_lock", side_effect=cancel_before_timeout_lock
        ):
            dispatcher._check_native_timeouts()

        self.assertEqual(1, len(request_ids))
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            request = conn.execute(
                "SELECT status FROM control_requests WHERE id=?", (request_ids[0],)
            ).fetchone()
        self.assertIsNone(job["kill_reason"])
        self.assertEqual("pending", request["status"])

    def test_pending_native_cancel_wins_over_timeout(self) -> None:
        self.raw_batch["tasks"][0]["duration_min"] = 1
        job_id = self._seed_isolated_native_session()
        dispatcher = self._dispatcher()
        with state.connect() as conn:
            state.update_job(conn, job_id, started_at="2000-01-01 00:00:00")
            request_id = state.insert_control_request(conn, job_id)

        dispatcher._check_timeouts()

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            request = conn.execute(
                "SELECT status FROM control_requests WHERE id=?", (request_id,)
            ).fetchone()
            conn.execute("BEGIN IMMEDIATE")
            self.assertFalse(
                state.mark_native_session_timed_out(
                    conn,
                    session_id="a" * 32,
                    job_id=job_id,
                    job_version=1,
                    started_at=job["started_at"],
                )
            )
        self.assertIsNone(job["kill_reason"])
        self.assertEqual("pending", request["status"])
        dispatcher.log_line.assert_not_called()

    def test_node_restart_and_stop_preserve_unowned_session(self) -> None:
        job_id = self._seed_isolated_native_session()
        dispatcher = self._dispatcher()
        dispatcher._prev_hb_ts = 1
        dispatcher._recover_launch_markers = mock.Mock(return_value=False)
        dispatcher._wait_for_job_states = mock.Mock(return_value={})
        dispatcher._unresolved_launch_markers = mock.Mock(return_value=False)
        dispatcher._cleanup_lock = mock.Mock()
        dispatcher._signal_job = mock.Mock()

        with mock.patch(
            "gsched.dispatcher.open", create=True, return_value=io.StringIO("1 1")
        ):
            dispatcher._check_node_restart()
        self.assertFalse(dispatcher._stop_locked())

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
        self.assertEqual("running", job["status"])
        self.assertIsNone(job["rc"])
        self.assertIsNone(job["finished_at"])
        dispatcher._signal_job.assert_not_called()
        dispatcher._cleanup_lock.assert_not_called()

    def test_legacy_marker_cannot_assign_native_session_pgid(self) -> None:
        job_id = self._seed_isolated_native_session()
        dispatcher = self._dispatcher()
        marker_path = dispatcher._launch_marker_path({"id": job_id})
        os.makedirs(os.path.dirname(marker_path), exist_ok=True)
        with open(marker_path, "w", encoding="utf-8") as stream:
            stream.write("stale legacy marker")
        dispatcher._read_launch_marker_identity = mock.Mock(return_value=(4242, "1"))

        with mock.patch("gsched.dispatcher._claim_abandoned_launch_intent") as claim:
            dispatcher._recover_launch_markers()

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
        self.assertEqual("running", job["status"])
        self.assertIsNone(job["pgid"])
        self.assertTrue(os.path.exists(marker_path))
        claim.assert_not_called()
        dispatcher._read_launch_marker_identity.assert_not_called()

    def test_nonrunning_native_session_marker_is_not_cleaned_or_signalled(self) -> None:
        job_id = self._seed_isolated_native_session()
        dispatcher = self._dispatcher()
        with state.connect() as conn:
            state.update_job(conn, job_id, status="blocked")
        marker_path = dispatcher._launch_marker_path({"id": job_id})
        os.makedirs(os.path.dirname(marker_path), exist_ok=True)
        with open(marker_path, "w", encoding="utf-8") as stream:
            stream.write("stale legacy marker")

        real_prepare = Dispatcher._prepare_launch_marker.__get__(dispatcher, Dispatcher)
        dispatcher._prepare_launch_marker = mock.Mock(wraps=real_prepare)
        dispatcher._read_launch_marker_identity = mock.Mock(return_value=(4242, "1"))
        dispatcher._launch_identity_state = mock.Mock(return_value="alive")
        dispatcher._signal_launch_identity = mock.Mock(return_value=True)

        with mock.patch(
            "gsched.dispatcher._claim_abandoned_launch_intent", return_value=None
        ) as claim:
            dispatcher._recover_launch_markers()

        self.assertTrue(os.path.exists(marker_path))
        dispatcher._prepare_launch_marker.assert_not_called()
        dispatcher._signal_launch_identity.assert_not_called()
        claim.assert_not_called()

    def test_pending_v2_legacy_marker_blocks_claim_and_recovery(self) -> None:
        job_id, spec = self._seed_v2_candidate()
        dispatcher = self._dispatcher()
        marker_path = dispatcher._launch_marker_path({"id": job_id})
        os.makedirs(os.path.dirname(marker_path), exist_ok=True)
        with open(marker_path, "w", encoding="utf-8") as stream:
            stream.write("stale legacy marker")

        dispatcher._recover_launch_markers()

        self.assertTrue(os.path.exists(marker_path))
        dispatcher._prepare_launch_marker.assert_not_called()
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            with self.assertRaisesRegex(state.StateError, "legacy launch marker"):
                self._claim_isolated_native_session(conn, job_id, spec)
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            session_count = conn.execute(
                "SELECT COUNT(*) FROM native_sessions WHERE job_id=?", (job_id,)
            ).fetchone()[0]
        self.assertEqual("pending", job["status"])
        self.assertIsNone(job["pgid"])
        self.assertIsNone(job["started_at"])
        self.assertEqual(0, session_count)
        self.assertTrue(os.path.exists(marker_path))

    def test_shutdown_marker_rolls_back_deferred_native_claim(self) -> None:
        job_id, spec = self._seed_v2_candidate()
        state.mark_idle_shutdown()
        self.addCleanup(state.clear_idle_shutdown)
        observed_statuses = []
        original_marker = state.submission_shutdown_marker
        with state.connect() as conn:
            conn.execute("BEGIN DEFERRED")

            def marker_after_writer_claim() -> str:
                observed_statuses.append(state.get_job(conn, job_id)["status"])
                return original_marker()

            with mock.patch.object(
                state, "submission_shutdown_marker", side_effect=marker_after_writer_claim
            ):
                with self.assertRaisesRegex(state.StateError, "daemon shutdown"):
                    self._claim_isolated_native_session(conn, job_id, spec)

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            session_count = conn.execute(
                "SELECT COUNT(*) FROM native_sessions WHERE job_id=?", (job_id,)
            ).fetchone()[0]
        self.assertEqual(["running"], observed_statuses)
        self.assertEqual("pending", job["status"])
        self.assertIsNone(job["started_at"])
        self.assertEqual(0, session_count)

    def test_native_claim_rejects_uncertain_shutdown_marker_inspection(self) -> None:
        job_id, spec = self._seed_v2_candidate()
        with state.connect() as conn:
            conn.execute("BEGIN DEFERRED")
            with mock.patch.object(
                state.os, "lstat", side_effect=PermissionError("inspection denied")
            ):
                with self.assertRaisesRegex(state.StateError, "inspect shutdown marker"):
                    self._claim_isolated_native_session(conn, job_id, spec)

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            session_count = conn.execute(
                "SELECT COUNT(*) FROM native_sessions WHERE job_id=?", (job_id,)
            ).fetchone()[0]
        self.assertEqual("pending", job["status"])
        self.assertIsNone(job["started_at"])
        self.assertEqual(0, session_count)

    def test_native_claim_rejects_uncertain_legacy_marker_inspection(self) -> None:
        job_id, spec = self._seed_v2_candidate()
        marker_path = state.launch_marker_path(job_id)
        original_lstat = os.lstat

        def uncertain_marker(path):
            if path == marker_path:
                raise PermissionError("marker inspection denied")
            return original_lstat(path)

        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            with mock.patch.object(state.os, "lstat", side_effect=uncertain_marker):
                with self.assertRaisesRegex(
                    state.StateError, "inspect legacy launch marker"
                ):
                    self._claim_isolated_native_session(conn, job_id, spec)

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            session_count = conn.execute(
                "SELECT COUNT(*) FROM native_sessions WHERE job_id=?", (job_id,)
            ).fetchone()[0]
        self.assertEqual("pending", job["status"])
        self.assertIsNone(job["started_at"])
        self.assertEqual(0, session_count)

    def test_stop_rechecks_a_session_claimed_after_its_running_snapshot(self) -> None:
        job_id, spec = self._seed_v2_candidate()
        dispatcher = self._dispatcher()
        dispatcher._recover_launch_markers = mock.Mock(return_value=False)
        dispatcher._unresolved_launch_markers = mock.Mock(return_value=False)
        dispatcher._cleanup_lock = mock.Mock()
        claimed = False

        def claim_during_stop_wait(_rows, _seconds):
            nonlocal claimed
            if not claimed:
                claimed = True
                with state.connect() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    self._claim_isolated_native_session(conn, job_id, spec)
            return {}

        dispatcher._wait_for_job_states = mock.Mock(side_effect=claim_during_stop_wait)

        self.assertFalse(dispatcher._stop_locked())

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
        self.assertEqual("running", job["status"])
        dispatcher._cleanup_lock.assert_not_called()

    def test_drain_publishes_marker_while_holding_writer(self) -> None:
        job_id, spec = self._seed_v2_candidate()
        dispatcher = self._dispatcher()
        dispatcher._unresolved_launch_markers = mock.Mock(return_value=False)
        original_mark = state.mark_idle_shutdown

        def mark_under_writer():
            with closing(sqlite3.connect(state.db_path(), timeout=0.0)) as other:
                with self.assertRaises(sqlite3.OperationalError):
                    other.execute("BEGIN IMMEDIATE")
            return original_mark()

        with (
            mock.patch("gsched.resources.drain_state", return_value={"stop": True}),
            mock.patch.object(state, "mark_idle_shutdown", side_effect=mark_under_writer),
        ):
            self.assertTrue(dispatcher._drain_complete())
        self.addCleanup(state.clear_idle_shutdown)

        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            with self.assertRaisesRegex(state.StateError, "daemon shutdown"):
                self._claim_isolated_native_session(conn, job_id, spec)
        with state.connect() as conn:
            self.assertEqual("pending", state.get_job(conn, job_id)["status"])

    def test_idle_rechecks_activity_after_native_claim(self) -> None:
        job_id, spec = self._seed_v2_candidate()
        with state.connect() as conn:
            conn.execute(
                "UPDATE batches SET status='blocked' WHERE id='native-batch-id'"
            )
        dispatcher = self._dispatcher()
        dispatcher.idle_timeout_min = 1
        dispatcher.last_activity = time.time() - 120
        dispatcher._unresolved_launch_markers = mock.Mock(return_value=False)

        def activate_and_claim():
            with state.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "UPDATE batches SET status='active' WHERE id='native-batch-id'"
                )
                self._claim_isolated_native_session(conn, job_id, spec)
            return False

        dispatcher._submit_inbox_pending = mock.Mock(side_effect=activate_and_claim)
        with mock.patch("gsched.resources.drain_state", return_value=None):
            self.assertFalse(dispatcher._idle_check())

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
        self.assertEqual("running", job["status"])
        self.assertFalse(state.idle_shutdown_pending())

    def test_dispatch_failure_does_not_settle_a_concurrent_session_claim(self) -> None:
        job_id, spec = self._seed_v2_candidate()
        dispatcher = self._dispatcher()
        dispatcher._read_gpu_policy = mock.Mock(return_value={})
        dispatcher._project_priority = mock.Mock(return_value=0)
        dispatcher._task_has_unresolved_launch_marker = mock.Mock(return_value=False)
        dispatcher._reconcile_ready_batch_markers = mock.Mock()
        dispatcher._update_project_quota_used = mock.Mock()
        dispatcher._cpu_in_use = mock.Mock(return_value=0)
        dispatcher._launch_marker_alive = mock.Mock(return_value=False)
        dispatcher._release_in_tx = mock.Mock()
        dispatcher._maybe_retry = mock.Mock()
        dispatcher._projects = self.cfg["projects"]
        dispatcher._project_quota_used = {}
        dispatcher.allocator = mock.Mock()
        marker_path = dispatcher._launch_marker_path({"id": job_id})
        marker_bytes = b"concurrent native marker"

        def competing_claim(_conn, _job, _gpu):
            with state.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                self._claim_isolated_native_session(conn, job_id, spec)
            os.makedirs(os.path.dirname(marker_path), exist_ok=True)
            with open(marker_path, "wb") as stream:
                stream.write(marker_bytes)
            raise NativeLaunchUnavailable("formal V2 launcher is not connected")

        dispatcher._launch_job = mock.Mock(side_effect=competing_claim)

        dispatcher._dispatch_ready_jobs_serialized()

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            session = conn.execute(
                "SELECT session_id FROM native_sessions WHERE job_id=?", (job_id,)
            ).fetchone()
        with open(marker_path, "rb") as stream:
            self.assertEqual(marker_bytes, stream.read())
        self.assertEqual("running", job["status"])
        self.assertIsNone(job["rc"])
        self.assertIsNone(job["failure"])
        self.assertIsNotNone(session)
        dispatcher._prepare_launch_marker.assert_not_called()
        dispatcher._release_in_tx.assert_not_called()
        dispatcher._maybe_retry.assert_not_called()


class NativeExecDispatcherInboxTests(NativeExecDispatcherCase):
    def _queue_payload(self, *, bid: str, path_name: str) -> str:
        payload_path = os.path.join(self.tmp.name, path_name)
        with open(payload_path, "w", encoding="utf-8") as stream:
            json.dump({"spec": self.raw_batch, "bid": bid}, stream)
        with state.connect() as conn:
            conn.execute(
                "INSERT INTO control_requests"
                " (job_id, op, status, created_at)"
                " VALUES (?, 'batch_submit', 'pending', ?)",
                (payload_path, state.now()),
            )
        return payload_path

    def test_inbox_v2_is_validation_only_before_fingerprint_or_batch_write(self) -> None:
        batch_env = {
            "PYTHONNOUSERSITE": "1",
            "OMP_NUM_THREADS": "1",
        }
        self.cfg["native_exec_profiles"] = {
            "frozen-v2": {
                "schema": "sched_native_exec_profile_v2",
                "mode": "strict",
                "project": "p",
                "batch_name": "frozen-v2",
                "task_id": "probe",
                "submitted_argv": list(self.argv),
                "cwd": "{PROJECT:p}",
                "depends_on": [],
                "_protocol": "scheduler_probe_runtime_end_attestation_v1",
                "batch_env": dict(batch_env),
                "task_env": {},
                "runtime": {"prefix": self.tmp.name},
                "duration_min": 1440,
                "max_retry": 0,
                "resources": {"gpu": 0, "cpus": 1},
                "artifacts": {},
            }
        }
        self.raw_batch = {
            "name": "frozen-v2",
            "project": "p",
            "mode": "strict",
            "cwd": "{PROJECT:p}",
            "depends_on": [],
            "_protocol": "scheduler_probe_runtime_end_attestation_v1",
            "env": dict(batch_env),
            "tasks": [
                {
                    "id": "probe",
                    "cmd": list(self.argv),
                    "env": {},
                    "runtime": {"prefix": self.tmp.name},
                    "duration_min": 1440,
                    "max_retry": 0,
                    "artifacts": {},
                    "resources": {"gpu": 0, "cpus": 1},
                }
            ],
        }
        self._queue_payload(bid="frozen-v2-inbox-id", path_name="frozen-v2.json")
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher.log_line = mock.Mock()

        with (
            mock.patch("gsched.dispatcher.compute_fingerprint") as fingerprint,
            mock.patch("gsched.dispatcher._validate_inbox_dependencies") as dependencies,
            mock.patch("gsched.dispatcher.state.submission_lock") as submission_lock,
            mock.patch("gsched.dispatcher.state.insert_batch") as insert_batch,
            mock.patch("gsched.dispatcher.state.insert_task") as insert_task,
            mock.patch("gsched.dispatcher.state.insert_job") as insert_job,
            mock.patch("subprocess.Popen") as popen,
        ):
            dispatcher._process_control_requests()

        dependencies.assert_not_called()
        fingerprint.assert_not_called()
        submission_lock.assert_not_called()
        insert_batch.assert_not_called()
        insert_task.assert_not_called()
        insert_job.assert_not_called()
        popen.assert_not_called()
        with state.connect() as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0])
            request = conn.execute(
                "SELECT status, result FROM control_requests WHERE op='batch_submit'"
            ).fetchone()
        self.assertEqual("done", request["status"])
        self.assertIn("validation-only", request["result"])

    def test_inbox_persists_same_metadata_and_fingerprint_binding(self) -> None:
        payload_path = os.path.join(self.tmp.name, "submit-native.json")
        with open(payload_path, "w", encoding="utf-8") as stream:
            json.dump(
                {
                    "spec": self.raw_batch,
                    "bid": "native-batch-inbox-id",
                },
                stream,
            )
        with state.connect() as conn:
            conn.execute(
                "INSERT INTO control_requests"
                " (job_id, op, status, created_at)"
                " VALUES (?, 'batch_submit', 'pending', ?)",
                (payload_path, state.now()),
            )
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher.log_line = mock.Mock()

        dispatcher._process_control_requests()

        normalized = self._normalized_task()
        expected_digest = normalized["_native_exec_profile_sha256"]
        expected_fingerprint, _, _ = compute_fingerprint(
            self.argv,
            None,
            normalized["cwd_abs"],
            normalized["git"],
            {},
            runtime_prefix=normalized.get("runtime_prefix"),
            execution_env=self.cfg["task_default_env"],
            artifacts=normalized["artifacts"],
            native_exec_profile_sha256=expected_digest,
            native_exec_project_root_identity_sha256=normalized[
                "_native_exec_project_root_identity_sha256"
            ],
        )
        with state.connect() as conn:
            task = conn.execute(
                "SELECT spec FROM tasks "
                "WHERE batch_id='native-batch-inbox-id'"
            ).fetchone()
            job = conn.execute(
                "SELECT fingerprint FROM jobs"
                " WHERE batch_id='native-batch-inbox-id'"
            ).fetchone()
            request = conn.execute(
                "SELECT status FROM control_requests"
                " WHERE op='batch_submit'"
            ).fetchone()
        self.assertIsNotNone(task)
        self.assertIsNotNone(job)
        persisted = json.loads(task["spec"])
        self.assertEqual("formal-v1", persisted["_native_exec_profile_id"])
        self.assertEqual(
            expected_digest, persisted["_native_exec_profile_sha256"]
        )
        self.assertEqual(
            self.argv, persisted["_native_exec_submitted_argv"]
        )
        self.assertEqual(self.argv, persisted["cmd"])
        self.assertEqual(expected_fingerprint, job["fingerprint"])
        self.assertEqual("done", request["status"])

    def test_inbox_new_bid_cannot_reuse_terminal_strict_name(self) -> None:
        with state.connect() as conn:
            state.insert_batch(
                conn,
                "historical-native-id",
                "native-batch",
                "strict",
                [],
                None,
                self.tmp.name,
                {},
                project="p",
            )
            conn.execute(
                "UPDATE batches SET status='done' "
                "WHERE id='historical-native-id'"
            )
        self._queue_payload(
            bid="native-batch-new-native-id",
            path_name="submit-replay.json",
        )
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher.log_line = mock.Mock()

        dispatcher._process_control_requests()

        with state.connect() as conn:
            batches = conn.execute(
                "SELECT id FROM batches ORDER BY id"
            ).fetchall()
            request = conn.execute(
                "SELECT status, result FROM control_requests"
            ).fetchone()
        self.assertEqual(["historical-native-id"], [row["id"] for row in batches])
        self.assertEqual("done", request["status"])
        self.assertIn("已消费", request["result"])

    def test_inbox_identical_bid_remains_idempotent_before_name_check(self) -> None:
        self._queue_payload(
            bid="native-batch-inbox-id",
            path_name="submit-original.json",
        )
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher.log_line = mock.Mock()
        dispatcher._process_control_requests()

        with state.connect() as conn:
            conn.execute(
                "UPDATE batches SET status='done' "
                "WHERE id='native-batch-inbox-id'"
            )
        self._queue_payload(
            bid="native-batch-inbox-id",
            path_name="submit-duplicate.json",
        )
        dispatcher._process_control_requests()

        with state.connect() as conn:
            requests = conn.execute(
                "SELECT status, result FROM control_requests ORDER BY id"
            ).fetchall()
            count = conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]
        self.assertEqual(1, count)
        self.assertEqual("done", requests[-1]["status"])
        self.assertIn("重复投递", requests[-1]["result"])

    def test_inbox_strict_duplicate_rejects_mix_preclaim(self) -> None:
        with state.connect() as conn:
            state.insert_batch(
                conn,
                "native-batch-inbox-id",
                "native-batch",
                "mix",
                [],
                None,
                self.tmp.name,
                {},
                project="p",
            )
        self._queue_payload(
            bid="native-batch-inbox-id",
            path_name="submit-preclaimed.json",
        )
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher.log_line = mock.Mock()

        dispatcher._process_control_requests()

        with state.connect() as conn:
            batch = state.get_batch(conn, "native-batch-inbox-id")
            request = conn.execute(
                "SELECT status, result FROM control_requests ORDER BY id DESC"
            ).fetchone()
            task_count = conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE batch_id='native-batch-inbox-id'"
            ).fetchone()[0]
            job_count = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE batch_id='native-batch-inbox-id'"
            ).fetchone()[0]
        self.assertEqual("mix", batch["mode"])
        self.assertEqual(0, task_count)
        self.assertEqual(0, job_count)
        self.assertEqual("done", request["status"])
        self.assertIn("exact delivery 不一致", request["result"])

    def test_inbox_strict_duplicate_rejects_incomplete_or_drifted_state(
        self,
    ) -> None:
        cases = (
            "missing_task",
            "missing_job",
            "extra_task",
            "extra_job",
            "task_spec_drift",
        )
        for index, case in enumerate(cases):
            with self.subTest(case=case):
                self._queue_payload(
                    bid="native-batch-inbox-id",
                    path_name=f"submit-original-{index}.json",
                )
                dispatcher = Dispatcher.__new__(Dispatcher)
                dispatcher.cfg = self.cfg
                dispatcher.log_line = mock.Mock()
                dispatcher._process_control_requests()

                with state.connect() as conn:
                    if case == "missing_task":
                        conn.execute(
                            "DELETE FROM tasks "
                            "WHERE batch_id='native-batch-inbox-id'"
                        )
                    elif case == "missing_job":
                        conn.execute(
                            "DELETE FROM jobs "
                            "WHERE batch_id='native-batch-inbox-id'"
                        )
                    elif case == "extra_task":
                        state.insert_task(
                            conn,
                            "native-batch-inbox-id",
                            "extra-task",
                            1,
                            {"id": "extra-task"},
                            1,
                            "p",
                        )
                    elif case == "extra_job":
                        state.insert_job(
                            conn,
                            "native-batch-inbox-id-extra-task-v1",
                            "native-batch-inbox-id",
                            "extra-task",
                            1,
                            None,
                            None,
                            "p",
                        )
                    else:
                        row = conn.execute(
                            "SELECT spec FROM tasks "
                            "WHERE batch_id='native-batch-inbox-id'"
                        ).fetchone()
                        drifted = json.loads(row["spec"])
                        drifted["duration_min"] = 999
                        conn.execute(
                            "UPDATE tasks SET spec=? "
                            "WHERE batch_id='native-batch-inbox-id'",
                            (json.dumps(drifted),),
                        )

                self._queue_payload(
                    bid="native-batch-inbox-id",
                    path_name=f"submit-duplicate-{index}.json",
                )
                dispatcher._process_control_requests()

                with state.connect() as conn:
                    request = conn.execute(
                        "SELECT status, result FROM control_requests "
                        "ORDER BY id DESC"
                    ).fetchone()
                    self.assertEqual("done", request["status"])
                    self.assertIn(
                        "exact delivery 不一致",
                        request["result"],
                    )
                    conn.execute("DELETE FROM jobs")
                    conn.execute("DELETE FROM tasks")
                    conn.execute("DELETE FROM batches")
                    conn.execute("DELETE FROM control_requests")

    def test_inbox_mix_cannot_claim_reserved_native_batch_name(self) -> None:
        mix_spec = copy.deepcopy(self.raw_batch)
        mix_spec["mode"] = "mix"
        payload_path = os.path.join(self.tmp.name, "submit-reserved-mix.json")
        with open(payload_path, "w", encoding="utf-8") as stream:
            json.dump(
                {
                    "spec": mix_spec,
                    "bid": "native-batch-reserved-mix-id",
                },
                stream,
            )
        with state.connect() as conn:
            conn.execute(
                "INSERT INTO control_requests"
                " (job_id, op, status, created_at)"
                " VALUES (?, 'batch_submit', 'pending', ?)",
                (payload_path, state.now()),
            )
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher.log_line = mock.Mock()

        dispatcher._process_control_requests()

        with state.connect() as conn:
            request = conn.execute(
                "SELECT status, result FROM control_requests"
            ).fetchone()
            count = conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]
        self.assertEqual(0, count)
        self.assertEqual("done", request["status"])
        self.assertIn("保留", request["result"])

    def test_inbox_rejects_path_like_or_wrong_prefix_bid_before_write(self) -> None:
        bad_bids = (
            "/absolute/escape",
            "../relative-escape",
            "native-batch-../escape",
            "other-batch-safe-looking",
            "native-batch-line\nbreak",
        )
        for index, bid in enumerate(bad_bids):
            with self.subTest(bid=bid):
                payload_path = os.path.join(
                    self.tmp.name, f"submit-bad-bid-{index}.json"
                )
                with open(payload_path, "w", encoding="utf-8") as stream:
                    json.dump(
                        {"spec": self.raw_batch, "bid": bid},
                        stream,
                    )
                with state.connect() as conn:
                    conn.execute(
                        "INSERT INTO control_requests"
                        " (job_id, op, status, created_at)"
                        " VALUES (?, 'batch_submit', 'pending', ?)",
                        (payload_path, state.now()),
                    )
                dispatcher = Dispatcher.__new__(Dispatcher)
                dispatcher.cfg = self.cfg
                dispatcher.log_line = mock.Mock()

                with mock.patch.object(state, "insert_batch") as insert_batch:
                    dispatcher._process_control_requests()

                insert_batch.assert_not_called()
                with state.connect() as conn:
                    request = conn.execute(
                        "SELECT status, result FROM control_requests "
                        "ORDER BY id DESC LIMIT 1"
                    ).fetchone()
                    self.assertEqual(
                        0,
                        conn.execute(
                            "SELECT COUNT(*) FROM batches"
                        ).fetchone()[0],
                    )
                self.assertEqual("done", request["status"])
                self.assertIn("inbox bid", request["result"])


if __name__ == "__main__":
    unittest.main()
