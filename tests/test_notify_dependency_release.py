from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from gsched import cli, state
from gsched.dispatcher import Dispatcher
from gsched.schema import SchemaError, validate_persisted_dependencies


class NotifyDependencyReleaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state_root = os.path.join(self.tmp.name, "state")
        self.config_path = os.path.join(self.tmp.name, "config.json")
        self.cfg = {
            "schema_version": 1,
            "user": "test",
            "node": "notify-dependency-release",
            "state_dir": self.state_root,
            "gpus": [],
            "default_project": "p",
            "projects": {
                "p": {
                    "root": self.tmp.name,
                    "git": False,
                    "gpu_quota": 1,
                }
            },
            "venvs": {},
            "task_default_env": {"PYTHONNOUSERSITE": "1"},
            "notify": {"file": {"enabled": True}},
        }
        with open(self.config_path, "w", encoding="utf-8") as stream:
            json.dump(self.cfg, stream)
        self.environment = mock.patch.dict(
            os.environ,
            {
                "SCHED_STATE": self.state_root,
                "SCHED_CONFIG": self.config_path,
                "SCHED_ALLOW_FOREIGN_WRITE": "1",
            },
            clear=False,
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.previous_read_only = state.read_only()
        state.set_read_only(False)
        self.addCleanup(state.set_read_only, self.previous_read_only)
        self._clear_state_caches()
        self.addCleanup(self._clear_state_caches)
        state.init_db()

    @staticmethod
    def _clear_state_caches() -> None:
        state._hostname_cache.clear()
        state._hostname_last_good.clear()
        state._pinned_host.clear()

    def _insert_batch(
        self,
        batch_id: str,
        name: str,
        *,
        status: str,
        depends_on: list[str] | None = None,
        job_status: str | None = None,
    ) -> None:
        with state.connect() as conn:
            state.insert_batch(
                conn,
                batch_id,
                name,
                "mix",
                depends_on or [],
                None,
                self.tmp.name,
                {},
                project="p",
            )
            conn.execute(
                "UPDATE batches SET status=? WHERE id=?",
                (status, batch_id),
            )
            if job_status is None:
                return
            task_id = "task"
            task_spec = {
                "id": task_id,
                "cmd": ["/bin/true"],
                "stages": None,
                "cwd_abs": self.tmp.name,
                "git": False,
                "env": {},
                "resources": {"gpu": 0, "cpus": 1},
                "duration_min": 1,
                "max_retry": 0,
                "artifacts": {},
                "paths_escape": False,
                "probes": None,
                "project": "p",
            }
            state.insert_task(conn, batch_id, task_id, 1, task_spec, 0, "p")
            job_id = f"{batch_id}-{task_id}-v1"
            state.insert_job(
                conn,
                job_id,
                batch_id,
                task_id,
                1,
                f"{batch_id}-fingerprint",
                None,
                "p",
            )
            terminal = job_status not in (
                "pending",
                "running",
                "waiting_quota",
                "waiting_dep",
            )
            state.update_job(
                conn,
                job_id,
                status=job_status,
                rc=1 if job_status == "failed" else (0 if terminal else None),
                failure="release regression" if job_status == "failed" else None,
                started_at=state.now() if terminal else None,
                finished_at=state.now() if terminal else None,
            )

    def _dispatcher(self) -> Dispatcher:
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher.host_dir = state.host_dir()
        dispatcher._notify_threads = []
        dispatcher.log_line = mock.Mock()
        dispatcher._batch_has_unresolved_launch_marker = mock.Mock(
            return_value=False
        )
        return dispatcher

    @staticmethod
    def _capture(function, *args):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = function(*args)
        return result, stdout.getvalue(), stderr.getvalue()

    def test_older_terminal_generation_that_loses_marker_ownership_does_not_notify(
        self,
    ) -> None:
        self._insert_batch(
            "shared-old",
            "shared",
            status="active",
            job_status="done",
        )
        self._insert_batch("shared-new", "shared", status="done")
        dispatcher = self._dispatcher()
        dispatcher._write_marker = mock.Mock()
        dispatcher._notify_batch = mock.Mock()

        dispatcher._settle_batch_status()

        with state.connect() as conn:
            old = state.get_batch(conn, "shared-old")
        self.assertEqual("done", old["status"])
        dispatcher._write_marker.assert_not_called()
        dispatcher._notify_batch.assert_not_called()

    def test_reopened_active_batch_is_not_encoded_as_blocked_notification(
        self,
    ) -> None:
        self._insert_batch(
            "reopened",
            "reopened",
            status="active",
            job_status="pending",
        )
        dispatcher = self._dispatcher()

        with mock.patch(
            "gsched.dispatcher.notify.build_event"
        ) as build_event, mock.patch(
            "gsched.dispatcher.notify.send"
        ) as send:
            notified = dispatcher._notify_batch(
                "reopened",
                "reopened",
                "blocked",
            )

        self.assertFalse(notified)
        build_event.assert_not_called()
        send.assert_not_called()
        self.assertEqual([], dispatcher._notify_threads)

    def test_latest_done_and_blocked_notify_once_with_exact_event_kind(
        self,
    ) -> None:
        self._insert_batch(
            "terminal-done",
            "terminal-done",
            status="active",
            job_status="done",
        )
        self._insert_batch(
            "terminal-blocked",
            "terminal-blocked",
            status="active",
            job_status="failed",
        )
        dispatcher = self._dispatcher()
        events: list[dict] = []

        def capture_event(event, _cfg):
            events.append(event)
            return []

        with mock.patch(
            "gsched.dispatcher.notify.send",
            side_effect=capture_event,
        ):
            dispatcher._settle_batch_status()
            for thread in list(dispatcher._notify_threads):
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())
            dispatcher._settle_batch_status()
            for thread in list(dispatcher._notify_threads):
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())

        by_batch = {event["batch"]: event for event in events}
        self.assertEqual(2, len(events))
        self.assertEqual(
            "batch_done",
            by_batch["terminal-done"]["event"],
        )
        self.assertEqual(
            "batch_blocked",
            by_batch["terminal-blocked"]["event"],
        )
        self.assertEqual("terminal-done", by_batch["terminal-done"]["batch_id"])
        self.assertEqual(
            "terminal-blocked",
            by_batch["terminal-blocked"]["batch_id"],
        )

    def test_terminal_settlement_lock_failure_propagates_and_is_fail_closed(
        self,
    ) -> None:
        self._insert_batch(
            "lock-failure",
            "lock-failure",
            status="active",
            job_status="done",
        )
        dispatcher = self._dispatcher()
        dispatcher._write_marker = mock.Mock()
        dispatcher._notify_batch = mock.Mock()

        with mock.patch.object(
            state,
            "submission_lock",
            side_effect=state.StateError("injected submission lock failure"),
        ), self.assertRaisesRegex(
            state.StateError,
            "injected submission lock failure",
        ):
            dispatcher._settle_batch_status()

        with state.connect() as conn:
            batch = state.get_batch(conn, "lock-failure")
        self.assertEqual("active", batch["status"])
        dispatcher._write_marker.assert_not_called()
        dispatcher._notify_batch.assert_not_called()

    def test_dependency_validation_uses_only_latest_same_name_payload(
        self,
    ) -> None:
        self._insert_batch("a-old", "A", status="done")
        with state.connect() as conn:
            conn.execute(
                "UPDATE batches SET depends_on=? WHERE id=?",
                ("not-json", "a-old"),
            )
        self._insert_batch("a-new", "A", status="done")

        with state.connect() as conn:
            validate_persisted_dependencies(conn, "B", ["A"])

        self._insert_batch("a-latest-corrupt", "A", status="done")
        with state.connect() as conn:
            conn.execute(
                "UPDATE batches SET depends_on=? WHERE id=?",
                ("not-json", "a-latest-corrupt"),
            )
            with self.assertRaisesRegex(SchemaError, "depends_on 状态无效"):
                validate_persisted_dependencies(conn, "B", ["A"])

    def test_cmd_submit_rechecks_cycle_after_fingerprint_window(self) -> None:
        self._insert_batch("a-old", "A", status="done")
        self._insert_batch("b-old", "B", status="done")
        batch_path = os.path.join(self.tmp.name, "candidate-b.json")
        with open(batch_path, "w", encoding="utf-8") as stream:
            json.dump(
                {
                    "name": "B",
                    "project": "p",
                    "cwd": self.tmp.name,
                    "depends_on": ["A"],
                    "tasks": [
                        {
                            "id": "task",
                            "cmd": ["/bin/true"],
                            "git": False,
                            "resources": {"gpu": 0, "cpus": 1},
                        }
                    ],
                },
                stream,
            )
        injected: list[str] = []

        def fingerprint_with_competing_submit(*_args, **_kwargs):
            self.assertEqual([], injected)
            with state.submission_connect() as conn:
                state.insert_batch(
                    conn,
                    "a-racing",
                    "A",
                    "mix",
                    ["B"],
                    None,
                    self.tmp.name,
                    {},
                    project="p",
                )
            injected.append("a-racing")
            return "candidate-fingerprint", None, None

        args = argparse.Namespace(batch=batch_path, dry_run=False, json=False)
        with mock.patch.object(
            cli,
            "_load_cfg",
            return_value=self.cfg,
        ), mock.patch(
            "gsched.fingerprint.compute_fingerprint",
            side_effect=fingerprint_with_competing_submit,
        ) as compute_fingerprint, mock.patch.object(
            cli,
            "_ensure_running_locked",
        ) as ensure_running:
            rc, stdout, stderr = self._capture(cli.cmd_submit, args)

        self.assertEqual(1, rc)
        self.assertEqual(["a-racing"], injected)
        compute_fingerprint.assert_called_once()
        ensure_running.assert_not_called()
        self.assertIn("依赖成环", stderr)
        self.assertNotIn("已入队", stdout + stderr)
        with state.connect() as conn:
            b_rows = conn.execute(
                "SELECT id FROM batches WHERE name='B' ORDER BY rowid"
            ).fetchall()
            candidate_jobs = conn.execute(
                "SELECT COUNT(*) AS n FROM jobs WHERE fingerprint=?",
                ("candidate-fingerprint",),
            ).fetchone()["n"]
        self.assertEqual(["b-old"], [row["id"] for row in b_rows])
        self.assertEqual(0, candidate_jobs)

    def test_clean_deletion_window_blocks_dependency_unlock(self) -> None:
        self._insert_batch(
            "upstream-id",
            "upstream",
            status="done",
            job_status="skip",
        )
        self._insert_batch(
            "downstream-id",
            "downstream",
            status="queued",
            depends_on=["upstream"],
        )
        artifact_path = os.path.join(self.tmp.name, "clean-window.out")
        with open(artifact_path, "w", encoding="utf-8") as stream:
            stream.write("producer output\n")
        with state.connect() as conn:
            row = conn.execute(
                "SELECT spec FROM tasks"
                " WHERE batch_id='upstream-id' AND id='task' AND version=1"
            ).fetchone()
            spec = json.loads(row["spec"])
            spec["artifacts"] = {
                "output": {"path": os.path.basename(artifact_path)}
            }
            conn.execute(
                "UPDATE tasks SET spec=?"
                " WHERE batch_id='upstream-id' AND id='task' AND version=1",
                (json.dumps(spec),),
            )

        ready_path = os.path.join(self.tmp.name, "unlock-contender.ready")
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        program = (
            "import os,sys\n"
            "sys.path.insert(0,sys.argv[1])\n"
            "from gsched import state\n"
            "from gsched.dispatcher import Dispatcher\n"
            "with open(sys.argv[2],'w',encoding='utf-8') as f: f.write('ready')\n"
            "d=Dispatcher.__new__(Dispatcher)\n"
            "d.host_dir=state.host_dir()\n"
            "d.log_line=lambda _message: None\n"
            "d._batch_has_unresolved_launch_marker=lambda _conn,_bid: False\n"
            "d._unlock_dependent_batches()\n"
        )
        contender: subprocess.Popen | None = None
        real_unlink = cli.artifacts.unlink_artifact

        def unlink_with_unlock_contender(*args, **kwargs):
            nonlocal contender
            contender = subprocess.Popen(
                [sys.executable, "-I", "-c", program, repo_root, ready_path],
                cwd=repo_root,
                env=dict(os.environ),
            )
            deadline = time.monotonic() + 5
            while not os.path.exists(ready_path) and time.monotonic() < deadline:
                if contender.poll() is not None:
                    break
                time.sleep(0.01)
            self.assertTrue(os.path.exists(ready_path))
            time.sleep(0.1)
            self.assertIsNone(
                contender.poll(),
                "dependency unlock crossed clean's artifact-deletion gate",
            )
            return real_unlink(*args, **kwargs)

        args = argparse.Namespace(batch="upstream", yes=True)
        try:
            with mock.patch.object(
                cli.artifacts,
                "unlink_artifact",
                side_effect=unlink_with_unlock_contender,
            ), mock.patch.object(
                cli,
                "_ensure_running_locked",
                return_value="daemon wake suppressed in test",
            ):
                rc, _stdout, stderr = self._capture(cli.cmd_clean, args)
            self.assertEqual(0, rc, stderr)
        finally:
            if contender is not None and contender.poll() is None:
                contender.wait(timeout=5)

        assert contender is not None
        self.assertEqual(0, contender.returncode)
        self.assertFalse(os.path.exists(artifact_path))
        with state.connect() as conn:
            upstream = state.get_batch(conn, "upstream-id")
            upstream_job = conn.execute(
                "SELECT status FROM jobs WHERE batch_id='upstream-id'"
            ).fetchone()
            downstream = state.get_batch(conn, "downstream-id")
        self.assertEqual("active", upstream["status"])
        self.assertEqual("pending", upstream_job["status"])
        self.assertEqual("queued", downstream["status"])

    def test_dispatch_final_phase_waits_for_clean_submission_gate(self) -> None:
        ready_path = os.path.join(self.tmp.name, "dispatch-contender.ready")
        completed_path = os.path.join(
            self.tmp.name,
            "dispatch-contender.completed",
        )
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        program = (
            "import os,sys\n"
            "sys.path.insert(0,sys.argv[1])\n"
            "from gsched.dispatcher import Dispatcher\n"
            "with open(sys.argv[2],'w',encoding='utf-8') as f: f.write('ready')\n"
            "d=Dispatcher.__new__(Dispatcher)\n"
            "def serialized():\n"
            "  with open(sys.argv[3],'w',encoding='utf-8') as f: f.write('done')\n"
            "d._dispatch_ready_jobs_serialized=serialized\n"
            "d._dispatch_ready_jobs()\n"
        )
        contender: subprocess.Popen | None = None
        try:
            with state.submission_lock():
                contender = subprocess.Popen(
                    [
                        sys.executable,
                        "-I",
                        "-c",
                        program,
                        repo_root,
                        ready_path,
                        completed_path,
                    ],
                    cwd=repo_root,
                    env=dict(os.environ),
                )
                deadline = time.monotonic() + 5
                while (
                    not os.path.exists(ready_path)
                    and time.monotonic() < deadline
                ):
                    if contender.poll() is not None:
                        break
                    time.sleep(0.01)
                self.assertTrue(os.path.exists(ready_path))
                time.sleep(0.1)
                self.assertIsNone(contender.poll())
                self.assertFalse(os.path.exists(completed_path))
            contender.wait(timeout=5)
        finally:
            if contender is not None and contender.poll() is None:
                contender.kill()
                contender.wait(timeout=5)

        self.assertEqual(0, contender.returncode)
        self.assertTrue(os.path.isfile(completed_path))

    def test_missing_skip_artifact_is_not_terminal_success(self) -> None:
        self._insert_batch(
            "skip-upstream",
            "skip-upstream",
            status="blocked",
            job_status="skip",
        )
        self._insert_batch(
            "skip-downstream",
            "skip-downstream",
            status="queued",
            depends_on=["skip-upstream"],
        )
        with state.connect() as conn:
            task = conn.execute(
                "SELECT spec FROM tasks WHERE batch_id='skip-upstream'"
            ).fetchone()
            spec = json.loads(task["spec"])
            spec["artifacts"] = {
                "missing": {"path": "cleaned-shared-output.bin"}
            }
            conn.execute(
                "UPDATE tasks SET spec=? WHERE batch_id='skip-upstream'",
                (json.dumps(spec),),
            )

        dispatcher = self._dispatcher()
        dispatcher._write_marker = mock.Mock()
        dispatcher._notify_batch = mock.Mock()
        dispatcher._unlock_dependent_batches()
        dispatcher._settle_batch_status()

        with state.connect() as conn:
            upstream = state.get_batch(conn, "skip-upstream")
            downstream = state.get_batch(conn, "skip-downstream")
        self.assertEqual("blocked", upstream["status"])
        self.assertEqual("queued", downstream["status"])
        dispatcher._write_marker.assert_not_called()
        dispatcher._notify_batch.assert_not_called()

    def test_cleaned_skip_alias_does_not_unlock_done_producer_dependency(
        self,
    ) -> None:
        self._insert_batch(
            "producer-id",
            "producer",
            status="done",
            job_status="done",
        )
        self._insert_batch(
            "consumer-id",
            "consumer",
            status="done",
            job_status="skip",
        )
        self._insert_batch(
            "dependent-id",
            "dependent",
            status="queued",
            depends_on=["producer"],
        )
        shared_path = os.path.join(self.tmp.name, "shared-output.bin")
        with open(shared_path, "wb") as stream:
            stream.write(b"shared producer output")
        with state.connect() as conn:
            for batch_id in ("producer-id", "consumer-id"):
                row = conn.execute(
                    "SELECT spec FROM tasks WHERE batch_id=?",
                    (batch_id,),
                ).fetchone()
                spec = json.loads(row["spec"])
                spec["artifacts"] = {
                    "shared": {"path": os.path.basename(shared_path)}
                }
                conn.execute(
                    "UPDATE tasks SET spec=? WHERE batch_id=?",
                    (json.dumps(spec), batch_id),
                )
                conn.execute(
                    "UPDATE jobs SET fingerprint='shared-fingerprint'"
                    " WHERE batch_id=?",
                    (batch_id,),
                )

        with mock.patch.object(
            cli,
            "_ensure_running_locked",
            return_value="daemon wake suppressed in test",
        ):
            rc, _stdout, stderr = self._capture(
                cli.cmd_clean,
                argparse.Namespace(batch="consumer", yes=True),
            )
        self.assertEqual(0, rc, stderr)
        self.assertFalse(os.path.exists(shared_path))

        dispatcher = self._dispatcher()
        dispatcher._unlock_dependent_batches()
        with state.connect() as conn:
            dependent = state.get_batch(conn, "dependent-id")
        self.assertEqual("queued", dependent["status"])

    def test_clean_rejects_other_active_batch_before_shared_deletion(self) -> None:
        self._insert_batch(
            "clean-target",
            "clean-target",
            status="done",
            job_status="skip",
        )
        self._insert_batch(
            "already-active-dependent",
            "already-active-dependent",
            status="active",
            depends_on=["clean-target"],
            job_status="pending",
        )
        artifact_path = os.path.join(self.tmp.name, "active-shared.out")
        with open(artifact_path, "w", encoding="utf-8") as stream:
            stream.write("must remain")
        with state.connect() as conn:
            row = conn.execute(
                "SELECT spec FROM tasks WHERE batch_id='clean-target'"
            ).fetchone()
            spec = json.loads(row["spec"])
            spec["artifacts"] = {
                "shared": {"path": os.path.basename(artifact_path)}
            }
            conn.execute(
                "UPDATE tasks SET spec=? WHERE batch_id='clean-target'",
                (json.dumps(spec),),
            )

        rc, _stdout, stderr = self._capture(
            cli.cmd_clean,
            argparse.Namespace(batch="clean-target", yes=True),
        )
        self.assertEqual(1, rc)
        self.assertIn("其他 active 批次", stderr)
        self.assertTrue(os.path.isfile(artifact_path))
        with state.connect() as conn:
            target_job = conn.execute(
                "SELECT status, fingerprint FROM jobs"
                " WHERE batch_id='clean-target'"
            ).fetchone()
        self.assertEqual("skip", target_job["status"])
        self.assertIsNotNone(target_job["fingerprint"])

    def test_active_done_with_missing_artifact_becomes_blocked(self) -> None:
        self._insert_batch(
            "invalid-done",
            "invalid-done",
            status="active",
            job_status="done",
        )
        with state.connect() as conn:
            row = conn.execute(
                "SELECT spec FROM tasks WHERE batch_id='invalid-done'"
            ).fetchone()
            spec = json.loads(row["spec"])
            spec["artifacts"] = {
                "missing": {"path": "deleted-before-settlement.out"}
            }
            conn.execute(
                "UPDATE tasks SET spec=? WHERE batch_id='invalid-done'",
                (json.dumps(spec),),
            )
        dispatcher = self._dispatcher()
        dispatcher._write_marker = mock.Mock()
        dispatcher._notify_batch = mock.Mock()

        dispatcher._settle_batch_status()

        with state.connect() as conn:
            batch = state.get_batch(conn, "invalid-done")
        self.assertEqual("blocked", batch["status"])
        dispatcher._write_marker.assert_called_once()
        self.assertEqual(
            "blocked",
            dispatcher._write_marker.call_args.args[1],
        )
        dispatcher._notify_batch.assert_called_once_with(
            "invalid-done",
            "invalid-done",
            "blocked",
        )

    def test_failed_status_short_circuits_terminal_artifact_io(self) -> None:
        self._insert_batch(
            "failed-upstream",
            "failed-upstream",
            status="active",
            job_status="failed",
        )
        self._insert_batch(
            "failed-dependent",
            "failed-dependent",
            status="queued",
            depends_on=["failed-upstream"],
        )
        dispatcher = self._dispatcher()
        dispatcher._terminal_job_successful = mock.Mock(
            side_effect=AssertionError(
                "artifact I/O must not run for nominally failed jobs"
            )
        )
        dispatcher._write_marker = mock.Mock()
        dispatcher._notify_batch = mock.Mock()

        dispatcher._unlock_dependent_batches()
        dispatcher._settle_batch_status()

        with state.connect() as conn:
            upstream = state.get_batch(conn, "failed-upstream")
            dependent = state.get_batch(conn, "failed-dependent")
        self.assertEqual("blocked", upstream["status"])
        self.assertEqual("queued", dependent["status"])
        dispatcher._terminal_job_successful.assert_not_called()


if __name__ == "__main__":
    unittest.main()
