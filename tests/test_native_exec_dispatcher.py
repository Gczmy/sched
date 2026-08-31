from __future__ import annotations

import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from gsched import state
from gsched.dispatcher import (
    CONFIG_COLD_KEYS,
    Dispatcher,
    _native_root_identities,
)
from gsched.fingerprint import compute_fingerprint
from gsched.native_exec import (
    NativeExecProfileError,
    native_exec_project_roots,
)
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
