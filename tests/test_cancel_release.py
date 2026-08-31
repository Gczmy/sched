from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import gsched.dispatcher as dispatcher_module
from gsched import cli, state
from gsched.dispatcher import Dispatcher
from gsched.executor import _create_launch_intent


class CancelReleaseRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config_path = os.path.join(self.tmp.name, "config.json")
        self.cfg = {
            "schema_version": 1,
            "user": "test",
            "node": "cancel-review-node",
            "state_dir": self.tmp.name,
            "gpus": [{"idx": 0, "mem_gib": 24}],
            "default_project": "p",
            "projects": {
                "p": {
                    "root": self.tmp.name,
                    "git": False,
                    "gpu_quota": 1,
                }
            },
            "venvs": {},
            "cpus_total": 8,
            "gpu_job_cpus": 1,
        }
        with open(self.config_path, "w", encoding="utf-8") as stream:
            json.dump(self.cfg, stream)
        self.env = mock.patch.dict(
            os.environ,
            {
                "SCHED_STATE": self.tmp.name,
                "SCHED_CONFIG": self.config_path,
            },
            clear=False,
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        state.set_read_only(False)
        state._hostname_cache.clear()
        state._hostname_last_good.clear()
        state._pinned_host.clear()
        self.addCleanup(state._hostname_cache.clear)
        self.addCleanup(state._hostname_last_good.clear)
        self.addCleanup(state._pinned_host.clear)
        state.init_db()
        with state.connect() as conn:
            state.init_gpus(conn, [0])

    def seed_job(
        self,
        status: str,
        *,
        pgid: int | None = None,
        gpu: int | None = None,
        kill_reason: str | None = None,
    ) -> str:
        job_id = "cancel-job"
        spec = {
            "id": "task",
            "cmd": ["/bin/true"],
            "stages": None,
            "cwd_abs": self.tmp.name,
            "git": False,
            "env": {},
            "resources": {"gpu": 1 if gpu is not None else 0, "cpus": 1},
            "duration_min": None,
            "max_retry": 2,
            "artifacts": {},
            "paths_escape": False,
            "probes": None,
        }
        with state.connect() as conn:
            state.insert_batch(
                conn,
                "batch",
                "batch",
                "mix",
                [],
                None,
                self.tmp.name,
                {},
                project="p",
            )
            conn.execute("UPDATE batches SET status='active' WHERE id='batch'")
            state.insert_task(conn, "batch", "task", 1, spec, 0, "p")
            state.insert_job(
                conn,
                job_id,
                "batch",
                "task",
                1,
                "cancel-fingerprint",
                None,
                "p",
            )
            state.update_job(
                conn,
                job_id,
                status=status,
                pgid=pgid,
                gpu=gpu,
                kill_reason=kill_reason,
            )
            if gpu is not None:
                conn.execute(
                    "UPDATE gpus SET status='assigned', job_id=? WHERE idx=?",
                    (job_id, gpu),
                )
                conn.execute(
                    "INSERT INTO gpu_jobs (gpu_id, job_id, vram_gib, updated_at)"
                    " VALUES (?, ?, ?, ?)",
                    (gpu, job_id, None, state.now()),
                )
        return job_id

    def dispatcher(self) -> Dispatcher:
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher.host_dir = state.host_dir()
        dispatcher.executor = mock.Mock()
        dispatcher.log_line = mock.Mock()
        dispatcher._release_in_tx = Dispatcher._release_in_tx.__get__(dispatcher)
        return dispatcher

    def test_a_cancel_claims_writer_before_dispatch_can_promote_pending(self) -> None:
        job_id = self.seed_job("pending")
        args = argparse.Namespace(
            batch="batch",
            yes=True,
            project=None,
            bulk_project=None,
        )
        real_resolve = cli._resolve_batch_ref
        contender_results: list[str] = []

        def resolve_with_competing_dispatch(ref, conn):
            resolved = real_resolve(ref, conn)
            contender = sqlite3.connect(state.db_path(), timeout=0)
            try:
                contender.execute(
                    "UPDATE jobs SET status='running', pgid=4242"
                    " WHERE id=? AND status='pending'",
                    (job_id,),
                )
                contender.commit()
                contender_results.append("committed")
            except sqlite3.OperationalError as error:
                self.assertIn("locked", str(error).lower())
                contender_results.append("locked")
            finally:
                contender.rollback()
                contender.close()
            return resolved

        output = io.StringIO()
        errors = io.StringIO()
        with mock.patch.object(
            cli,
            "_resolve_batch_ref",
            side_effect=resolve_with_competing_dispatch,
        ), contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            result = cli.cmd_cancel(args)

        self.assertEqual(0, result, errors.getvalue())
        self.assertEqual(["locked"], contender_results)
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            requests = conn.execute(
                "SELECT id FROM control_requests WHERE job_id=?",
                (job_id,),
            ).fetchall()
        self.assertEqual("cancelled", job["status"])
        self.assertEqual([], requests)

    def test_b_restart_claims_writer_before_cancel_request_can_cross(self) -> None:
        job_id = self.seed_job("running", pgid=4242)
        dispatcher = self.dispatcher()
        dispatcher._prev_hb_ts = 1.0
        dispatcher._drop_launch_marker = mock.Mock()
        real_update_job = state.update_job
        contender_results: list[str] = []
        attempted = False

        def update_with_competing_cancel(conn, target_job_id, **fields):
            nonlocal attempted
            if not attempted:
                attempted = True
                contender = sqlite3.connect(state.db_path(), timeout=0)
                try:
                    contender.execute(
                        "INSERT INTO control_requests"
                        " (job_id, op, status, created_at)"
                        " VALUES (?, 'cancel', 'pending', ?)",
                        (job_id, state.now()),
                    )
                    contender.commit()
                    contender_results.append("committed")
                except sqlite3.OperationalError as error:
                    self.assertIn("locked", str(error).lower())
                    contender_results.append("locked")
                finally:
                    contender.rollback()
                    contender.close()
            return real_update_job(conn, target_job_id, **fields)

        with mock.patch(
            "builtins.open",
            mock.mock_open(read_data="1\n"),
        ), mock.patch.object(
            dispatcher_module.time,
            "time",
            return_value=100.0,
        ), mock.patch.object(
            state,
            "update_job",
            side_effect=update_with_competing_cancel,
        ):
            dispatcher._check_node_restart()

        self.assertEqual(["locked"], contender_results)
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            requests = conn.execute(
                "SELECT id FROM control_requests WHERE job_id=?",
                (job_id,),
            ).fetchall()
        self.assertEqual("pending", job["status"])
        self.assertEqual([], requests)

    def test_b_restart_preserves_recorded_cancel_and_releases_gpu_marker(self) -> None:
        job_id = self.seed_job(
            "running",
            pgid=4242,
            gpu=0,
            kill_reason="cancelled",
        )
        with state.connect() as conn:
            request_id = state.insert_control_request(conn, job_id)
            state.finish_control_request(conn, request_id, "signal confirmed")
        dispatcher = self.dispatcher()
        dispatcher._prev_hb_ts = 1.0
        marker = dispatcher._launch_marker_path({"id": job_id})
        state.ensure_private_directory(os.path.dirname(marker))
        with state.open_private_text(marker, "w") as stream:
            stream.write("4242 proc:1\n")

        with mock.patch(
            "builtins.open",
            mock.mock_open(read_data="1\n"),
        ), mock.patch.object(
            dispatcher_module.time,
            "time",
            return_value=100.0,
        ):
            dispatcher._check_node_restart()

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            gpu = state.get_gpu(conn, 0)
            assignments = conn.execute(
                "SELECT job_id FROM gpu_jobs WHERE job_id=?",
                (job_id,),
            ).fetchall()
        self.assertEqual("cancelled", job["status"])
        self.assertEqual("cancelled", job["kill_reason"])
        self.assertIsNone(job["pgid"])
        self.assertIsNone(job["gpu"])
        self.assertEqual("releasing", gpu["status"])
        self.assertEqual([], assignments)
        self.assertFalse(os.path.exists(marker))

    def test_c_reap_success_cannot_cross_a_pending_cancel(self) -> None:
        job_id = self.seed_job("running", pgid=4242)
        with state.connect() as conn:
            request_id = state.insert_control_request(conn, job_id)
        dispatcher = self.dispatcher()
        dispatcher._job_process_state = mock.Mock(return_value="dead")
        dispatcher.executor.has_process.return_value = True
        dispatcher.executor.poll_rc.return_value = 0
        dispatcher._read_job_rc = mock.Mock(return_value=None)
        dispatcher._consume_profile = mock.Mock()
        dispatcher._maybe_retry = mock.Mock()
        dispatcher._drop_launch_marker = mock.Mock()
        dispatcher._drop_profile = mock.Mock()
        dispatcher._drop_rc_path = mock.Mock()

        dispatcher._reap_finished_jobs()

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            request = conn.execute(
                "SELECT status FROM control_requests WHERE id=?",
                (request_id,),
            ).fetchone()
        self.assertEqual("cancelled", job["status"])
        self.assertEqual("done", request["status"])
        dispatcher._consume_profile.assert_not_called()
        dispatcher._maybe_retry.assert_not_called()

        dispatcher._notify_threads = []
        dispatcher._apply_terminal_marker_effect = mock.Mock(return_value=True)
        dispatcher._notify_batch = mock.Mock()
        dispatcher._settle_batch_status()
        with state.connect() as conn:
            batch = state.get_batch(conn, "batch")
        self.assertEqual("blocked", batch["status"])
        self.assertFalse(
            any(
                call.args[2] == "done"
                for call in dispatcher._notify_batch.call_args_list
            )
        )

    def test_d_adoption_success_cannot_cross_a_pending_cancel(self) -> None:
        job_id = self.seed_job("running", pgid=4242)
        with state.connect() as conn:
            request_id = state.insert_control_request(conn, job_id)
        dispatcher = self.dispatcher()
        dispatcher._prepare_launch_marker = mock.Mock(return_value=False)
        dispatcher._job_process_state = mock.Mock(return_value="dead")
        dispatcher._read_job_rc = mock.Mock(return_value=None)
        dispatcher._should_skip = mock.Mock(return_value=True)
        dispatcher._consume_profile = mock.Mock()
        dispatcher._drop_launch_marker = mock.Mock()
        dispatcher._drop_profile = mock.Mock()
        dispatcher._drop_rc_path = mock.Mock()

        dispatcher._adopt_running()

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            request = conn.execute(
                "SELECT status FROM control_requests WHERE id=?",
                (request_id,),
            ).fetchone()
        self.assertEqual("cancelled", job["status"])
        self.assertEqual("done", request["status"])
        dispatcher._read_job_rc.assert_not_called()
        dispatcher._should_skip.assert_not_called()
        dispatcher._consume_profile.assert_not_called()

    def test_e_dead_control_target_records_intent_before_settlement(self) -> None:
        job_id = self.seed_job("running", pgid=4242)
        with state.connect() as conn:
            request_id = state.insert_control_request(conn, job_id)
        dispatcher = self.dispatcher()
        dispatcher._job_process_state = mock.Mock(return_value="dead")
        dispatcher._maybe_retry = mock.Mock()

        dispatcher._process_control_requests()

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            request = conn.execute(
                "SELECT status FROM control_requests WHERE id=?",
                (request_id,),
            ).fetchone()
        self.assertEqual("running", job["status"])
        self.assertEqual("cancelled", job["kill_reason"])
        self.assertEqual("done", request["status"])

        with state.connect() as conn:
            current = state.get_job(conn, job_id)
            cleanup = dispatcher._handle_job_done(conn, current, 1)
        with state.connect() as conn:
            settled = state.get_job(conn, job_id)
        self.assertEqual("cancelled", settled["status"])
        self.assertIsNone(settled["pgid"])
        self.assertEqual(2, len(cleanup))
        dispatcher._maybe_retry.assert_not_called()

    def test_f_unlocked_intent_and_missing_marker_converge_on_later_ticks(
        self,
    ) -> None:
        job_id = self.seed_job("running", pgid=None, gpu=0)
        dispatcher = self.dispatcher()
        marker = dispatcher._launch_marker_path({"id": job_id})
        intent = _create_launch_intent(marker)

        self.assertFalse(dispatcher._recover_launch_markers())
        dispatcher._adopt_running(unidentified_only=True)
        with state.connect() as conn:
            locked_job = state.get_job(conn, job_id)
            locked_gpu = state.get_gpu(conn, 0)
        self.assertEqual("running", locked_job["status"])
        self.assertIsNone(locked_job["pgid"])
        self.assertEqual(0, locked_job["gpu"])
        self.assertEqual("assigned", locked_gpu["status"])

        os.close(intent.fd)

        self.assertTrue(dispatcher._recover_launch_markers())
        self.assertFalse(os.path.lexists(marker))

        # Model a tick failure immediately after the filesystem claim: no
        # marker remains, but the next periodic unidentified adoption must
        # still settle/requeue and release the durable lease.
        dispatcher._adopt_running(unidentified_only=True)

        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            gpu = state.get_gpu(conn, 0)
            assignments = conn.execute(
                "SELECT job_id FROM gpu_jobs WHERE job_id=?",
                (job_id,),
            ).fetchall()
        self.assertEqual("pending", job["status"])
        self.assertIsNone(job["pgid"])
        self.assertIsNone(job["gpu"])
        self.assertEqual("releasing", gpu["status"])
        self.assertEqual([], assignments)


if __name__ == "__main__":
    unittest.main()
