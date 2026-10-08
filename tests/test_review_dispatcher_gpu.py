from __future__ import annotations

import contextlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import tempfile
import time
import unittest
from unittest import mock

import gsched.dispatcher as dispatcher_module
from gsched import daemon, state
from gsched.allocator import Allocator
from gsched.dispatcher import Dispatcher
from gsched.executor import Executor


class DispatcherStateCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config_path = os.path.join(self.tmp.name, "config.json")
        self.cfg = {
            "schema_version": 1,
            "user": "test",
            "node": "review-node",
            "state_dir": self.tmp.name,
            "gpus": [{"idx": 0, "mem_gib": 24}, {"idx": 1, "mem_gib": 8}],
            "default_project": "p",
            "projects": {"p": {"root": self.tmp.name, "git": False, "gpu_quota": 1}},
            "venvs": {},
            "cpus_total": 32,
            "gpu_job_cpus": 1,
        }
        with open(self.config_path, "w", encoding="utf-8") as stream:
            json.dump(self.cfg, stream)
        self.env = mock.patch.dict(
            os.environ,
            {"SCHED_STATE": self.tmp.name, "SCHED_CONFIG": self.config_path},
            clear=False,
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        state._hostname_cache.clear()
        state._hostname_last_good.clear()
        state._pinned_host.clear()
        self.addCleanup(state._hostname_cache.clear)
        self.addCleanup(state._hostname_last_good.clear)
        self.addCleanup(state._pinned_host.clear)
        state.init_db()
        with state.connect() as conn:
            state.init_gpus(conn, [0, 1])

    def seed_jobs(self, jobs):
        """jobs: iterable of (task_id, resources, status)."""
        with state.connect() as conn:
            state.insert_batch(
                conn, "batch", "batch", "mix", [], None, self.tmp.name, {}, project="p"
            )
            conn.execute("UPDATE batches SET status='active' WHERE id='batch'")
            for order, (task_id, resources, status) in enumerate(jobs):
                spec = {
                    "id": task_id,
                    "cmd": ["/bin/true"],
                    "stages": None,
                    "cwd_abs": self.tmp.name,
                    "git": False,
                    "env": {},
                    "resources": resources,
                    "artifacts": {},
                    "project": "p",
                }
                state.insert_task(conn, "batch", task_id, 1, spec, order, "p")
                state.insert_job(conn, task_id, "batch", task_id, 1, task_id + "-fp", None, "p")
                state.update_job(
                    conn,
                    task_id,
                    status=status,
                    gpu=0 if status == "running" and resources.get("gpu", 1) else None,
                )

    def dispatcher(self) -> Dispatcher:
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher._projects = self.cfg["projects"]
        dispatcher._project_quota_used = {}
        dispatcher._launch_inflight = {}
        dispatcher.host_dir = state.host_dir()
        dispatcher.executor = mock.Mock()
        dispatcher.executor.configured_owner.return_value = None
        dispatcher._launch_marker_alive = mock.Mock(return_value=False)
        dispatcher.log_line = mock.Mock()
        dispatcher._maybe_retry = mock.Mock()
        dispatcher._release_in_tx = mock.Mock()
        dispatcher._cpu_in_use = mock.Mock(return_value=0)
        return dispatcher


class ReviewProjectQuotaTests(DispatcherStateCase):
    def test_s_h06_two_gpu_jobs_cannot_oversell_one_slot_in_same_tick(self) -> None:
        self.seed_jobs([
            ("gpu-a", {"gpu": 1}, "pending"),
            ("gpu-b", {"gpu": 1}, "pending"),
        ])
        dispatcher = self.dispatcher()
        cards = iter([0, 1])
        dispatcher._assign_in_tx = mock.Mock(side_effect=lambda *_args: next(cards))
        launched = []
        dispatcher._launch_job = mock.Mock(
            side_effect=lambda _conn, job, _gpu: launched.append(job["id"]) or True
        )

        dispatcher._dispatch_ready_jobs()

        self.assertEqual(["gpu-a"], launched)

    def test_s_h06_full_gpu_quota_does_not_block_cpu_only_job(self) -> None:
        self.seed_jobs([
            ("running-gpu", {"gpu": 1}, "running"),
            ("cpu", {"gpu": 0, "cpus": 1}, "pending"),
        ])
        dispatcher = self.dispatcher()
        dispatcher._assign_in_tx = mock.Mock()
        launched = []
        dispatcher._launch_job = mock.Mock(
            side_effect=lambda _conn, job, gpu: launched.append((job["id"], gpu)) or True
        )

        dispatcher._dispatch_ready_jobs()

        self.assertEqual([("cpu", None)], launched)
        dispatcher._assign_in_tx.assert_not_called()

    def test_s_h06_skipped_launch_does_not_consume_project_gpu_quota(self) -> None:
        self.seed_jobs([
            ("skip", {"gpu": 1}, "pending"),
            ("launch", {"gpu": 1}, "pending"),
            ("blocked", {"gpu": 1}, "pending"),
        ])
        dispatcher = self.dispatcher()
        dispatcher._assign_in_tx = mock.Mock(return_value=0)
        attempts = []

        def launch(_conn, job, _gpu):
            attempts.append(job["id"])
            return job["id"] != "skip"

        dispatcher._launch_job = mock.Mock(side_effect=launch)
        dispatcher._dispatch_ready_jobs()
        self.assertEqual(["skip", "launch"], attempts)

    def test_s_h06_failed_launch_does_not_consume_project_gpu_quota(self) -> None:
        self.seed_jobs([
            ("fail", {"gpu": 1}, "pending"),
            ("launch", {"gpu": 1}, "pending"),
            ("blocked", {"gpu": 1}, "pending"),
        ])
        dispatcher = self.dispatcher()
        dispatcher._assign_in_tx = mock.Mock(return_value=0)
        attempts = []

        def launch(_conn, job, _gpu):
            attempts.append(job["id"])
            if job["id"] == "fail":
                raise OSError("launch failed")
            return True

        dispatcher._launch_job = mock.Mock(side_effect=launch)
        dispatcher._dispatch_ready_jobs()
        self.assertEqual(["fail", "launch"], attempts)


class ReviewHeadOfLineTests(DispatcherStateCase):
    def _assert_small_launches_after_oversized(self, big_resources, small_resources):
        self.cfg["projects"]["p"]["gpu_quota"] = 0
        self.seed_jobs([
            ("oversized", big_resources, "pending"),
            ("small", small_resources, "pending"),
        ])
        dispatcher = self.dispatcher()
        launched = []

        def assign(_conn, job_id, _spec, _project):
            dispatcher._assign_reject_scope = "all"
            return None if job_id == "oversized" else 1

        dispatcher._assign_in_tx = mock.Mock(side_effect=assign)
        dispatcher._launch_job = mock.Mock(
            side_effect=lambda _conn, job, _gpu: launched.append(job["id"]) or True
        )
        dispatcher._dispatch_ready_jobs()
        self.assertEqual(["small"], launched)

    def test_s_h07_oversized_exclusive_job_does_not_starve_small_job(self) -> None:
        self._assert_small_launches_after_oversized(
            {"gpu": 1, "vram_gib": 80}, {"gpu": 1, "vram_gib": 4}
        )

    def test_s_h07_oversized_shared_job_does_not_starve_small_shared_job(self) -> None:
        self.cfg["co_locate"] = True
        self._assert_small_launches_after_oversized(
            {"gpu": 1, "gpu_share": True, "vram_gib": 80},
            {"gpu": 1, "gpu_share": True, "vram_gib": 2},
        )

    def test_s_h07_heterogeneous_cards_still_allow_a_fitting_later_job(self) -> None:
        self._assert_small_launches_after_oversized(
            {"gpu": 1, "vram_gib": 32}, {"gpu": 1, "vram_gib": 8}
        )


class ReviewDispatchGenerationTests(DispatcherStateCase):
    def _capture_cpu_launches(self) -> list[str]:
        dispatcher = self.dispatcher()
        launched: list[str] = []
        dispatcher._assign_in_tx = mock.Mock()
        dispatcher._launch_job = mock.Mock(
            side_effect=lambda _conn, job, _gpu: launched.append(job["id"]) or True
        )
        dispatcher._dispatch_ready_jobs()
        dispatcher._assign_in_tx.assert_not_called()
        return launched

    def _insert_second_version(self, status: str) -> None:
        with state.connect() as conn:
            row = conn.execute(
                "SELECT spec, order_idx FROM tasks"
                " WHERE batch_id='batch' AND id='task' AND version=1"
            ).fetchone()
            state.insert_task(
                conn,
                "batch",
                "task",
                2,
                json.loads(row["spec"]),
                row["order_idx"],
                "p",
            )
            state.insert_job(
                conn,
                "task-v2",
                "batch",
                "task",
                2,
                "task-v2-fp",
                None,
                "p",
            )
            state.update_job(conn, "task-v2", status=status)

    def _insert_queued_downstream(self) -> None:
        with state.connect() as conn:
            state.insert_batch(
                conn,
                "downstream",
                "downstream",
                "mix",
                ["batch"],
                None,
                self.tmp.name,
                {},
                project="p",
            )

    def test_done_batch_pending_job_is_never_dispatched(self) -> None:
        self.seed_jobs([("task", {"gpu": 0, "cpus": 1}, "pending")])
        with state.connect() as conn:
            conn.execute("UPDATE batches SET status='done' WHERE id='batch'")

        self.assertEqual([], self._capture_cpu_launches())

    def test_blocked_batch_pending_history_is_not_reopened_or_dispatched(
        self,
    ) -> None:
        self.seed_jobs([("task", {"gpu": 0, "cpus": 1}, "pending")])
        with state.connect() as conn:
            conn.execute("UPDATE batches SET status='blocked' WHERE id='batch'")

        dispatcher = self.dispatcher()
        dispatcher._notify_threads = []
        dispatcher._write_marker = mock.Mock()
        dispatcher._notify_batch = mock.Mock()
        dispatcher._settle_batch_status()
        dispatcher._dispatch_ready_jobs()

        with state.connect() as conn:
            batch = state.get_batch(conn, "batch")
            job = state.get_job(conn, "task")
        self.assertEqual("blocked", batch["status"])
        self.assertEqual("pending", job["status"])
        self.assertIsNone(job["started_at"])
        dispatcher.executor.launch.assert_not_called()

    def test_settle_claims_writer_before_retry_can_invalidate_failure_snapshot(
        self,
    ) -> None:
        self.seed_jobs([("task", {"gpu": 0, "cpus": 1}, "failed")])
        dispatcher = self.dispatcher()
        dispatcher._notify_threads = []
        dispatcher._write_marker = mock.Mock()
        dispatcher._remove_marker = mock.Mock()
        dispatcher._notify_batch = mock.Mock()
        original_connect = state.connect
        contender_results: list[str] = []

        @contextlib.contextmanager
        def traced_connect():
            with original_connect() as conn:
                attempted = False

                def trace(statement: str) -> None:
                    nonlocal attempted
                    if attempted or "SET status='blocked'" not in statement:
                        return
                    attempted = True
                    contender = sqlite3.connect(state.db_path(), timeout=0)
                    try:
                        contender.execute(
                            "UPDATE jobs SET status='pending' WHERE id='task'"
                        )
                        contender.commit()
                        contender_results.append("committed")
                    except sqlite3.OperationalError as error:
                        self.assertIn("locked", str(error).lower())
                        contender_results.append("locked")
                    finally:
                        contender.rollback()
                        contender.close()

                conn.set_trace_callback(trace)
                try:
                    yield conn
                finally:
                    conn.set_trace_callback(None)

        with mock.patch.object(state, "connect", side_effect=traced_connect):
            dispatcher._settle_batch_status()

        self.assertEqual(["locked"], contender_results)
        with original_connect() as conn:
            batch = state.get_batch(conn, "batch")
            job = state.get_job(conn, "task")
        self.assertEqual("blocked", batch["status"])
        self.assertEqual("failed", job["status"])

    def test_settle_terminal_cas_does_not_revive_discarded_batch(self) -> None:
        self.seed_jobs([("task", {"gpu": 0, "cpus": 1}, "done")])
        dispatcher = self.dispatcher()
        dispatcher._notify_threads = []
        dispatcher._write_marker = mock.Mock()
        dispatcher._remove_marker = mock.Mock()
        dispatcher._notify_batch = mock.Mock()

        def discard_before_terminal_publish(conn, _batch_id):
            conn.execute(
                "UPDATE batches SET status='discarded' WHERE id='batch'"
            )
            return False

        dispatcher._batch_has_unresolved_launch_marker = mock.Mock(
            side_effect=discard_before_terminal_publish
        )
        dispatcher._settle_batch_status()

        with state.connect() as conn:
            batch = state.get_batch(conn, "batch")
        self.assertEqual("discarded", batch["status"])
        dispatcher._write_marker.assert_not_called()
        dispatcher._notify_batch.assert_not_called()

    def test_active_batch_dispatches_only_latest_pending_version(self) -> None:
        self.seed_jobs([("task", {"gpu": 0, "cpus": 1}, "pending")])
        self._insert_second_version("pending")

        self.assertEqual(["task-v2"], self._capture_cpu_launches())

    def test_active_batch_ignores_old_pending_when_latest_is_done(self) -> None:
        self.seed_jobs([("task", {"gpu": 0, "cpus": 1}, "pending")])
        self._insert_second_version("done")

        self.assertEqual([], self._capture_cpu_launches())

    def _dispatch_across_claim_race(self, mutate) -> Dispatcher:
        dispatcher = self.dispatcher()
        dispatcher._snapshot_fingerprint = mock.Mock(
            return_value=("current-fp", {}, None)
        )
        dispatcher._prepare_launch_marker = mock.Mock(return_value=False)
        dispatcher._launch_marker_alive = mock.Mock(
            side_effect=lambda _job: mutate() or False
        )
        dispatcher.executor.launch.side_effect = AssertionError(
            "non-dispatchable stale candidate reached executor"
        )

        dispatcher._dispatch_ready_jobs()
        return dispatcher

    def test_final_claim_rechecks_batch_active_after_candidate_snapshot(self) -> None:
        self.seed_jobs([("task", {"gpu": 0, "cpus": 1}, "pending")])

        def finish_batch() -> None:
            with state.connect() as conn:
                conn.execute("UPDATE batches SET status='done' WHERE id='batch'")

        dispatcher = self._dispatch_across_claim_race(finish_batch)

        with state.connect() as conn:
            job = state.get_job(conn, "task")
        self.assertEqual("pending", job["status"])
        self.assertIsNone(job["started_at"])
        dispatcher.executor.launch.assert_not_called()

    def test_final_claim_rechecks_latest_version_after_candidate_snapshot(self) -> None:
        self.seed_jobs([("task", {"gpu": 0, "cpus": 1}, "pending")])

        dispatcher = self._dispatch_across_claim_race(
            lambda: self._insert_second_version("pending")
        )

        with state.connect() as conn:
            jobs = conn.execute(
                "SELECT id, status, started_at FROM jobs"
                " WHERE batch_id='batch' ORDER BY version"
            ).fetchall()
        self.assertEqual(
            [("task", "pending", None), ("task-v2", "pending", None)],
            [
                (job["id"], job["status"], job["started_at"])
                for job in jobs
            ],
        )
        dispatcher.executor.launch.assert_not_called()

    def test_final_claim_blocks_old_generation_marker_created_after_scan(self) -> None:
        self.seed_jobs([("task", {"gpu": 0, "cpus": 1}, "pending")])
        self._insert_second_version("pending")

        def publish_old_marker() -> None:
            marker = state.launch_marker_path("task")
            state.ensure_private_directory(os.path.dirname(marker))
            with state.open_private_text(marker, "w") as stream:
                stream.write("unknown identity")

        dispatcher = self._dispatch_across_claim_race(publish_old_marker)

        with state.connect() as conn:
            jobs = conn.execute(
                "SELECT id, status, started_at FROM jobs"
                " WHERE batch_id='batch' ORDER BY version"
            ).fetchall()
        self.assertEqual(
            [("task", "pending", None), ("task-v2", "pending", None)],
            [
                (job["id"], job["status"], job["started_at"])
                for job in jobs
            ],
        )
        dispatcher.executor.launch.assert_not_called()

    def test_launch_log_failure_is_nonfatal_after_pgid_writeback(self) -> None:
        self.seed_jobs([("task", {"gpu": 1}, "pending")])
        dispatcher = self.dispatcher()
        dispatcher._snapshot_fingerprint = mock.Mock(
            return_value=("current-fp", {}, None)
        )
        dispatcher._should_skip = mock.Mock(return_value=False)
        dispatcher._clean_stale_artifacts = mock.Mock()
        dispatcher.executor.launch.return_value = 4242

        def fail_only_launch_record(message: str) -> None:
            if message.startswith("LAUNCH job"):
                raise OSError("injected log sink failure")

        dispatcher.log_line.side_effect = fail_only_launch_record
        with state.connect() as conn:
            conn.execute(
                "UPDATE gpus SET status='assigned', job_id='task' WHERE idx=0"
            )
            conn.execute(
                "INSERT INTO gpu_jobs (gpu_id, job_id, vram_gib, updated_at)"
                " VALUES (0, 'task', NULL, ?)",
                (state.now(),),
            )
            job = state.get_job(conn, "task")
            self.assertTrue(dispatcher._launch_job(conn, job, 0))

        with state.connect() as conn:
            current = state.get_job(conn, "task")
            gpu = state.get_gpu(conn, 0)
            assignment = conn.execute(
                "SELECT job_id FROM gpu_jobs WHERE gpu_id=0"
            ).fetchone()
        self.assertEqual("running", current["status"])
        self.assertEqual(0, current["gpu"])
        self.assertEqual(4242, current["pgid"])
        self.assertEqual("assigned", gpu["status"])
        self.assertEqual("task", assignment["job_id"])
        dispatcher.executor.kill_pgid.assert_not_called()

    def test_obsolete_pending_does_not_prevent_idle_shutdown(self) -> None:
        self.seed_jobs([("task", {"gpu": 0, "cpus": 1}, "pending")])
        self._insert_second_version("done")
        with state.connect() as conn:
            conn.execute("UPDATE batches SET status='done' WHERE id='batch'")
        dispatcher = self.dispatcher()
        dispatcher.idle_timeout_min = 1
        dispatcher.last_activity = time.time() - 61
        dispatcher._submit_inbox_pending = mock.Mock(return_value=False)
        dispatcher.log_line = mock.Mock()

        with mock.patch.object(state, "mark_idle_shutdown") as mark_shutdown:
            self.assertTrue(dispatcher._idle_check())

        mark_shutdown.assert_called_once_with()

    def test_obsolete_pending_launch_marker_blocks_settle_dependency_and_idle(
        self,
    ) -> None:
        self.seed_jobs([("task", {"gpu": 0, "cpus": 1}, "pending")])
        self._insert_second_version("done")
        dispatcher = self.dispatcher()
        marker = dispatcher._launch_marker_path({"id": "task"})
        state.ensure_private_directory(os.path.dirname(marker))
        with state.open_private_text(marker, "w") as stream:
            stream.write("unknown identity")

        with state.connect() as conn:
            self.assertFalse(dispatcher._batch_successful(conn, "batch"))
        dispatcher._write_marker = mock.Mock()
        dispatcher._notify_batch = mock.Mock()
        dispatcher._settle_batch_status()
        with state.connect() as conn:
            batch = state.get_batch(conn, "batch")
        self.assertEqual("active", batch["status"])
        dispatcher._write_marker.assert_not_called()

        dispatcher.idle_timeout_min = 1
        dispatcher.last_activity = time.time() - 61
        dispatcher._submit_inbox_pending = mock.Mock(return_value=False)
        with mock.patch.object(state, "mark_idle_shutdown") as mark_shutdown:
            self.assertFalse(dispatcher._idle_check())
        mark_shutdown.assert_not_called()

    def test_dependency_unlock_claims_writer_before_upstream_can_change(
        self,
    ) -> None:
        self.seed_jobs([("task", {"gpu": 0, "cpus": 1}, "done")])
        self._insert_queued_downstream()
        dispatcher = self.dispatcher()
        real_successful = dispatcher._batch_successful
        contender_results: list[str] = []

        def successful_then_competing_resubmit(conn, batch_name):
            successful = real_successful(conn, batch_name)
            contender = sqlite3.connect(state.db_path(), timeout=0)
            try:
                contender.execute(
                    "UPDATE jobs SET status='pending' WHERE id='task'"
                )
                contender.commit()
                contender_results.append("committed")
            except sqlite3.OperationalError as error:
                self.assertIn("locked", str(error).lower())
                contender_results.append("locked")
            finally:
                contender.rollback()
                contender.close()
            return successful

        dispatcher._batch_successful = mock.Mock(
            side_effect=successful_then_competing_resubmit
        )
        dispatcher._unlock_dependent_batches()

        self.assertEqual(["locked"], contender_results)
        with state.connect() as conn:
            upstream = state.get_job(conn, "task")
            downstream = state.get_batch(conn, "downstream")
        self.assertEqual("done", upstream["status"])
        self.assertEqual("active", downstream["status"])

    def test_dependency_unlock_cas_does_not_revive_discarded_batch(self) -> None:
        self.seed_jobs([("task", {"gpu": 0, "cpus": 1}, "done")])
        self._insert_queued_downstream()
        dispatcher = self.dispatcher()

        def discard_before_activation(conn, _batch_name):
            conn.execute(
                "UPDATE batches SET status='discarded' WHERE id='downstream'"
            )
            return True

        dispatcher._batch_successful = mock.Mock(
            side_effect=discard_before_activation
        )
        dispatcher._unlock_dependent_batches()

        with state.connect() as conn:
            downstream = state.get_batch(conn, "downstream")
        self.assertEqual("discarded", downstream["status"])
        self.assertFalse(
            any("依赖解锁" in str(call) for call in dispatcher.log_line.call_args_list)
        )


class ReviewExternalOccupancyTests(DispatcherStateCase):
    def test_s_h08_external_pid_during_release_never_creates_dispatchable_free_window(self) -> None:
        self.cfg["projects"]["p"]["gpu_quota"] = 0
        self.seed_jobs([("queued", {"gpu": 1}, "pending")])
        with state.connect() as conn:
            conn.execute("UPDATE gpus SET status='releasing', job_id=NULL WHERE idx=0")
            conn.execute("UPDATE gpus SET status='unmanaged' WHERE idx=1")
        allocator = Allocator.__new__(Allocator)
        allocator.fake = False
        allocator.gpu_list = [0]
        allocator._compute_pids_by_card = mock.Mock(return_value={0: [999]})
        allocator._known_job_pgids = mock.Mock(return_value=set())
        allocator._pgid_of = mock.Mock(return_value=999)
        allocator._confirm_release = mock.Mock(return_value=True)
        allocator._reset_release_confirm = mock.Mock()
        allocator._util_opt = mock.Mock(return_value=0)
        allocator._confirm_occupied = mock.Mock(return_value=False)
        allocator._reset_occupied = mock.Mock()
        allocator.mem_total = mock.Mock(return_value=24.0)

        allocator.settle_releasing()
        allocator.probe_free()
        dispatcher = self.dispatcher()
        dispatcher.allocator = allocator
        dispatcher._gpu_max_jobs = {}
        dispatcher._assign_in_tx = Dispatcher._assign_in_tx.__get__(dispatcher, Dispatcher)
        launched = []
        dispatcher._launch_job = mock.Mock(
            side_effect=lambda _conn, job, _gpu: launched.append(job["id"]) or True
        )
        dispatcher._dispatch_ready_jobs()

        with state.connect() as conn:
            gpu = state.get_gpu(conn, 0)
        self.assertEqual("unmanaged", gpu["status"])
        self.assertEqual([], launched)


class ReviewLaunchFingerprintTests(DispatcherStateCase):
    def test_s_h10_launch_fingerprints_outside_write_tx_and_reuses_snapshot(self) -> None:
        artifact_paths = [
            os.path.join(self.tmp.name, "stale-a.bin"),
            os.path.join(self.tmp.name, "stale-b.bin"),
        ]
        for path in artifact_paths:
            with open(path, "wb") as stream:
                stream.write(b"stale")
        spec = {
            "id": "cleanup",
            "cmd": ["/bin/true"],
            "stages": None,
            "cwd_abs": self.tmp.name,
            "git": False,
            "env": {},
            "resources": {"gpu": 0, "cpus": 1},
            "artifacts": {
                "a": {"path": artifact_paths[0], "min_bytes": 1},
                "b": {"path": artifact_paths[1], "min_bytes": 1},
            },
            "project": "p",
        }
        with state.connect() as conn:
            state.insert_batch(
                conn,
                "fingerprint-batch",
                "fingerprint-batch",
                "mix",
                [],
                None,
                self.tmp.name,
                {},
                project="p",
            )
            conn.execute(
                "UPDATE batches SET status='active' WHERE id='fingerprint-batch'"
            )
            state.insert_task(
                conn, "fingerprint-batch", "cleanup", 1, spec, 0, "p"
            )
            state.insert_job(
                conn,
                "fingerprint-job",
                "fingerprint-batch",
                "cleanup",
                1,
                "submission-fp",
                None,
                "p",
            )

        dispatcher = self.dispatcher()
        dispatcher.host_dir = os.path.join(self.tmp.name, "review-node")
        dispatcher.venv_paths = {}
        dispatcher.executor.launch.return_value = 4242
        dispatcher._drop_job_rc = mock.Mock()
        dispatcher._should_skip = mock.Mock(return_value=False)
        dispatcher._job_log_path = mock.Mock(
            return_value=os.path.join(self.tmp.name, "fingerprint-job.log")
        )
        dispatcher._job_rc_prefix = mock.Mock(return_value="fingerprint-job")
        dispatcher._launch_marker_path = mock.Mock(
            return_value=os.path.join(self.tmp.name, "launch", "fingerprint-job")
        )
        dispatcher._drop_launch_marker = mock.Mock()
        transaction_states = []

        with state.connect() as conn:
            job = state.get_job(conn, "fingerprint-job")

            def fingerprint(*_args, **_kwargs):
                transaction_states.append(conn.in_transaction)
                return "launch-fp", {}, None

            with mock.patch(
                "gsched.dispatcher.compute_fingerprint", side_effect=fingerprint
            ):
                launched = dispatcher._launch_job(conn, job, None)

        self.assertTrue(launched)
        self.assertEqual([False], transaction_states)
        self.assertFalse(any(os.path.exists(path) for path in artifact_paths))
        dispatcher.executor.launch.assert_called_once()

    def test_stale_cleanup_requires_a_committed_launch_claim(self) -> None:
        self.seed_jobs(
            [("commit-guard", {"gpu": 0, "cpus": 1}, "pending")]
        )
        artifact = os.path.join(self.tmp.name, "commit-guard.out")
        with open(artifact, "wb") as stream:
            stream.write(b"stale")
        stage_artifact = os.path.join(
            self.tmp.name, "commit-guard-stage.out"
        )
        with open(stage_artifact, "wb") as stream:
            stream.write(b"stale stage")
        with state.connect() as conn:
            row = conn.execute(
                "SELECT spec FROM tasks"
                " WHERE batch_id='batch' AND id='commit-guard'"
            ).fetchone()
            spec = json.loads(row["spec"])
            spec["artifacts"] = {
                "out": {"path": artifact, "min_bytes": 1}
            }
            spec["stages"] = [
                {
                    "cmd": ["/bin/true"],
                    "artifacts": {
                        "stage": {
                            "path": stage_artifact,
                            "min_bytes": 1,
                        }
                    },
                }
            ]
            conn.execute(
                "UPDATE tasks SET spec=?"
                " WHERE batch_id='batch' AND id='commit-guard'",
                (json.dumps(spec),),
            )
            state.update_job(conn, "commit-guard", pgid=3131)

        dispatcher = self.dispatcher()
        checkpoint_dir = os.path.join(
            dispatcher.host_dir, "stage_checkpoints", "commit-guard"
        )
        os.makedirs(checkpoint_dir, mode=0o700, exist_ok=True)
        checkpoint = os.path.join(checkpoint_dir, "stage-0.json")
        with open(checkpoint, "w", encoding="utf-8") as stream:
            json.dump(
                {"schema_version": 1, "fingerprint": "stale"},
                stream,
            )
        with state.connect() as conn:
            stale_job = dict(state.get_job(conn, "commit-guard"))
        stale_rc = dispatcher._job_rc_path(stale_job)
        assert stale_rc is not None
        os.makedirs(os.path.dirname(stale_rc), mode=0o700, exist_ok=True)
        with open(stale_rc, "w", encoding="utf-8") as stream:
            stream.write("1\n")
        dispatcher._ready_task_specs = {"commit-guard": spec}
        dispatcher._ready_fingerprint_snapshots = {
            "commit-guard": ("launch-fp", {"0": "new-stage-fp"}, None)
        }
        dispatcher._should_skip = mock.Mock(return_value=False)

        class CommitFailingConnection:
            def __init__(self, wrapped):
                self.wrapped = wrapped

            def __getattr__(self, name):
                return getattr(self.wrapped, name)

            def commit(self):
                raise sqlite3.OperationalError("pre-delete commit failure")

        with self.assertRaisesRegex(
            sqlite3.OperationalError, "pre-delete commit failure"
        ):
            with state.connect() as conn:
                job = state.get_job(conn, "commit-guard")
                dispatcher._launch_job(
                    CommitFailingConnection(conn), job, None
                )

        self.assertTrue(
            all(
                os.path.exists(path)
                for path in (artifact, stage_artifact, checkpoint, stale_rc)
            )
        )
        dispatcher.executor.launch.assert_not_called()
        with state.connect() as conn:
            self.assertEqual(
                "pending", state.get_job(conn, "commit-guard")["status"]
            )

    def test_launch_failure_after_cleanup_leaves_durable_recovery_claim(
        self,
    ) -> None:
        self.seed_jobs(
            [("launch-crash", {"gpu": 1, "cpus": 1}, "pending")]
        )
        artifact = os.path.join(self.tmp.name, "launch-crash.out")
        with open(artifact, "wb") as stream:
            stream.write(b"stale")
        with state.connect() as conn:
            row = conn.execute(
                "SELECT spec FROM tasks"
                " WHERE batch_id='batch' AND id='launch-crash'"
            ).fetchone()
            spec = json.loads(row["spec"])
            spec["artifacts"] = {
                "out": {"path": artifact, "min_bytes": 1}
            }
            conn.execute(
                "UPDATE tasks SET spec=?"
                " WHERE batch_id='batch' AND id='launch-crash'",
                (json.dumps(spec),),
            )

        dispatcher = self.dispatcher()
        dispatcher._ready_task_specs = {"launch-crash": spec}
        dispatcher._ready_fingerprint_snapshots = {
            "launch-crash": ("launch-fp", {}, "git-rev")
        }
        dispatcher._should_skip = mock.Mock(return_value=False)
        dispatcher.executor.launch.side_effect = RuntimeError("launch crashed")
        clean_stale = dispatcher._clean_stale_artifacts
        observed_claims = []

        def assert_durable_claim(*args, **kwargs):
            with state.connect() as observer:
                current = state.get_job(observer, "launch-crash")
                gpu_claim = observer.execute(
                    "SELECT gpu_id FROM gpu_jobs WHERE job_id='launch-crash'"
                ).fetchone()
                observed_claims.append(
                    (
                        current["status"],
                        current["pgid"],
                        current["gpu"],
                        current["fingerprint"],
                        gpu_claim["gpu_id"] if gpu_claim else None,
                    )
                )
            return clean_stale(*args, **kwargs)

        dispatcher._clean_stale_artifacts = mock.Mock(
            side_effect=assert_durable_claim
        )
        with self.assertRaisesRegex(RuntimeError, "launch crashed"):
            with state.connect() as conn:
                job = state.get_job(conn, "launch-crash")
                conn.execute(
                    "UPDATE gpus SET status='assigned', job_id=? WHERE idx=0",
                    (job["id"],),
                )
                conn.execute(
                    "INSERT INTO gpu_jobs"
                    " (gpu_id, job_id, vram_gib, updated_at)"
                    " VALUES (0, ?, NULL, ?)",
                    (job["id"], state.now()),
                )
                dispatcher._launch_job(conn, job, 0)

        self.assertEqual(
            [("running", None, 0, "launch-fp", 0)], observed_claims
        )
        self.assertFalse(os.path.exists(artifact))
        with state.connect() as conn:
            current = state.get_job(conn, "launch-crash")
            self.assertEqual("running", current["status"])
            self.assertIsNone(current["pgid"])
            self.assertEqual(0, current["gpu"])
            self.assertIsNotNone(
                conn.execute(
                    "SELECT 1 FROM gpu_jobs WHERE job_id='launch-crash'"
                ).fetchone()
            )

    def test_skip_does_not_precommit_or_delete_valid_artifacts(self) -> None:
        self.seed_jobs([("skip-guard", {"gpu": 0, "cpus": 1}, "pending")])
        artifact = os.path.join(self.tmp.name, "skip-guard.out")
        with open(artifact, "wb") as stream:
            stream.write(b"valid")
        with state.connect() as conn:
            row = conn.execute(
                "SELECT spec FROM tasks"
                " WHERE batch_id='batch' AND id='skip-guard'"
            ).fetchone()
            spec = json.loads(row["spec"])
            spec["artifacts"] = {
                "out": {"path": artifact, "min_bytes": 1}
            }
            conn.execute(
                "UPDATE tasks SET spec=?"
                " WHERE batch_id='batch' AND id='skip-guard'",
                (json.dumps(spec),),
            )

        dispatcher = self.dispatcher()
        dispatcher._ready_task_specs = {"skip-guard": spec}
        dispatcher._ready_fingerprint_snapshots = {
            "skip-guard": ("launch-fp", {}, None)
        }
        dispatcher._should_skip = mock.Mock(return_value=True)
        commits = []

        class TrackingConnection:
            def __init__(self, wrapped):
                self.wrapped = wrapped

            def __getattr__(self, name):
                return getattr(self.wrapped, name)

            def commit(self):
                commits.append(True)
                return self.wrapped.commit()

        with state.connect() as conn:
            job = state.get_job(conn, "skip-guard")
            launched = dispatcher._launch_job(
                TrackingConnection(conn), job, None
            )

        self.assertFalse(launched)
        self.assertEqual([], commits)
        self.assertTrue(os.path.isfile(artifact))
        dispatcher.executor.launch.assert_not_called()

    def test_stage_retry_preserves_only_valid_upstream_checkpoint_artifacts(
        self,
    ) -> None:
        dispatcher = self.dispatcher()
        dispatcher.host_dir = os.path.join(self.tmp.name, "review-node")
        checkpoint_dir = os.path.join(
            dispatcher.host_dir, "stage_checkpoints", "stage-job"
        )
        os.makedirs(checkpoint_dir, mode=0o700)
        stages = []
        for index in range(2):
            artifact = os.path.join(self.tmp.name, f"retry-stage-{index}.out")
            with open(artifact, "w", encoding="utf-8") as stream:
                stream.write("valid")
            with open(
                os.path.join(checkpoint_dir, f"stage-{index}.json"),
                "w",
                encoding="utf-8",
            ) as stream:
                json.dump(
                    {
                        "schema_version": 1,
                        "fingerprint": (
                            f"stage-fp-{index}" if index == 0 else "stale"
                        ),
                    },
                    stream,
                )
            stages.append(
                {
                    "cmd": ["/bin/true"],
                    "artifacts": {
                        f"stage-{index}": {
                            "path": artifact,
                            "min_bytes": 1,
                        }
                    },
                }
            )
        spec = {
            "cwd_abs": self.tmp.name,
            "artifacts": {},
            "stages": stages,
        }
        job = {"id": "stage-job", "project": "p", "task_id": "task"}
        with state.connect() as conn, mock.patch.object(
            dispatcher, "_fingerprint_matches", return_value=False
        ):
            dispatcher._clean_stale_artifacts(
                conn,
                spec,
                job,
                current_fingerprint="task-fp",
                stage_fingerprints={"0": "stage-fp-0", "1": "stage-fp-1"},
            )

        self.assertTrue(os.path.exists(stages[0]["artifacts"]["stage-0"]["path"]))
        self.assertTrue(os.path.exists(os.path.join(checkpoint_dir, "stage-0.json")))
        self.assertFalse(os.path.exists(stages[1]["artifacts"]["stage-1"]["path"]))
        self.assertFalse(os.path.exists(os.path.join(checkpoint_dir, "stage-1.json")))

    def test_force_rerun_unconditionally_removes_prior_artifacts(self) -> None:
        artifact = os.path.join(self.tmp.name, "force-rerun.out")
        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write("stale but valid")
        dispatcher = self.dispatcher()
        dispatcher.host_dir = os.path.join(self.tmp.name, "review-node")
        spec = {
            "cwd_abs": self.tmp.name,
            "_force_rerun": True,
            "artifacts": {"out": {"path": artifact, "min_bytes": 1}},
            "stages": None,
        }
        job = {"id": "force-job", "project": "p", "task_id": "task"}
        with state.connect() as conn, mock.patch.object(
            dispatcher, "_fingerprint_matches", return_value=True
        ):
            dispatcher._clean_stale_artifacts(
                conn,
                spec,
                job,
                current_fingerprint="same-fingerprint",
                stage_fingerprints={},
            )

        self.assertFalse(os.path.exists(artifact))

    def test_stale_cleanup_never_unlinks_through_intermediate_symlink(
        self,
    ) -> None:
        outside = tempfile.TemporaryDirectory()
        self.addCleanup(outside.cleanup)
        artifact = os.path.join(outside.name, "outside.out")
        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write("must survive")
        os.symlink(outside.name, os.path.join(self.tmp.name, "redirect"))
        dispatcher = self.dispatcher()
        dispatcher.host_dir = os.path.join(self.tmp.name, "review-node")
        spec = {
            "cwd_abs": self.tmp.name,
            "_force_rerun": True,
            "paths_escape": False,
            "artifacts": {
                "out": {
                    "path": os.path.join("redirect", "outside.out"),
                    "min_bytes": 1,
                }
            },
            "stages": None,
        }
        job = {"id": "escape-job", "project": "p", "task_id": "task"}

        with state.connect() as conn:
            dispatcher._clean_stale_artifacts(
                conn,
                spec,
                job,
                current_fingerprint="fingerprint",
                stage_fingerprints={},
            )

        self.assertTrue(os.path.isfile(artifact))


class ReviewProbeSettlementTests(DispatcherStateCase):
    def test_ready_probe_remains_running_until_process_group_is_dead(self) -> None:
        self.seed_jobs([("ready", {"gpu": 1}, "running")])
        with state.connect() as conn:
            row = conn.execute(
                "SELECT spec FROM tasks WHERE batch_id='batch' AND id='ready'"
            ).fetchone()
            spec = json.loads(row["spec"])
            spec["probes"] = {"ready_on_log": "READY"}
            conn.execute(
                "UPDATE tasks SET spec=? WHERE batch_id='batch' AND id='ready'",
                (json.dumps(spec),),
            )
            state.update_job(conn, "ready", pgid=4242)
        log_path = os.path.join(self.tmp.name, "ready.log")
        with open(log_path, "w", encoding="utf-8") as stream:
            stream.write("READY\n")
        dispatcher = self.dispatcher()
        dispatcher.host_dir = self.tmp.name
        dispatcher._probe_offsets = {}
        dispatcher._job_log_path = mock.Mock(return_value=log_path)
        dispatcher._job_rc_path = mock.Mock(return_value=None)
        dispatcher.executor.alive.return_value = True
        dispatcher._signal_job_result = mock.Mock(
            return_value=dispatcher_module._SIGNAL_SENT
        )
        dispatcher.executor.kill_pgid.return_value = True

        dispatcher._check_probes()

        with state.connect() as conn:
            running = state.get_job(conn, "ready")
        self.assertEqual("running", running["status"])
        self.assertEqual("probe_ready", running["kill_reason"])
        self.assertIsNone(running["finished_at"])
        dispatcher._release_in_tx.assert_not_called()

        dispatcher.executor.alive.return_value = False
        with state.connect() as conn:
            state.update_job(conn, "ready", rc=137)
            dispatcher._handle_job_done(conn, state.get_job(conn, "ready"), 137)
            settled = state.get_job(conn, "ready")
        self.assertEqual("done", settled["status"])
        dispatcher._release_in_tx.assert_called_once_with(
            mock.ANY, "ready"
        )


class ReviewRcSidecarTests(DispatcherStateCase):
    def _read_rc_in_subprocess(
        self,
        host_dir: str,
        job: dict,
    ) -> tuple[int | None, int]:
        code = (
            "import json,sys,tracemalloc\n"
            "from gsched.dispatcher import Dispatcher\n"
            "dispatcher=Dispatcher.__new__(Dispatcher)\n"
            "dispatcher.host_dir=sys.argv[1]\n"
            "job={'id':sys.argv[2],'pgid':int(sys.argv[3])}\n"
            "tracemalloc.start()\n"
            "value=dispatcher._read_job_rc(job)\n"
            "_,peak=tracemalloc.get_traced_memory()\n"
            "print(json.dumps([value,peak]))\n"
        )
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                code,
                host_dir,
                str(job["id"]),
                str(job["pgid"]),
            ],
            cwd=os.path.dirname(os.path.dirname(__file__)),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            stdout, stderr = process.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate(timeout=2)
            self.fail("RC sidecar read exceeded its two-second bound")
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=2)

        self.assertEqual(0, process.returncode, stderr)

        value, peak_bytes = json.loads(stdout)
        return value, int(peak_bytes)

    def test_rc_sidecar_fifo_symlink_and_oversize_are_bounded_unknown(
        self,
    ) -> None:
        dispatcher = self.dispatcher()
        dispatcher.host_dir = self.tmp.name
        rc_dir = os.path.join(self.tmp.name, "rc")
        os.makedirs(rc_dir, mode=0o700, exist_ok=True)

        for case in ("fifo", "symlink", "oversize"):
            with self.subTest(case=case):
                job = {"id": f"untrusted-rc-{case}", "pgid": 9191}
                path = dispatcher._job_rc_path(job)
                assert path is not None
                if case == "fifo":
                    os.mkfifo(path, mode=0o600)
                elif case == "symlink":
                    target = os.path.join(rc_dir, f"{case}-target")
                    with open(target, "w", encoding="utf-8") as stream:
                        stream.write("19\n")
                    os.symlink(target, path)
                else:
                    with open(path, "wb") as stream:
                        stream.write(b"19\n")
                        stream.truncate(8 * 1024 * 1024)

                value, peak_bytes = self._read_rc_in_subprocess(
                    dispatcher.host_dir,
                    job,
                )
                self.assertIsNone(value)
                if case == "oversize":
                    self.assertLess(peak_bytes, 1024 * 1024)



class ReviewSignalIdentityTests(DispatcherStateCase):
    def job_dispatcher(self, token: str | None) -> tuple[Dispatcher, dict]:
        dispatcher = self.dispatcher()
        dispatcher.host_dir = self.tmp.name
        job = {"id": "owned-job", "pgid": 4242}
        marker_path = dispatcher._launch_marker_path(job)
        os.makedirs(os.path.dirname(marker_path), mode=0o700, exist_ok=True)
        with open(marker_path, "w", encoding="utf-8") as stream:
            suffix = f" {token}" if token is not None else ""
            stream.write(f"4242{suffix}\n")
        dispatcher.executor.alive.return_value = True
        dispatcher.executor.kill_pgid.return_value = True
        return dispatcher, job

    def test_reused_or_tokenless_job_pgid_never_authorizes_signal(self) -> None:
        for marker_token, current_token in (
            ("proc:100", "proc:200"),
            (None, "proc:200"),
            ("proc:200", None),
        ):
            with self.subTest(
                marker_token=marker_token,
                current_token=current_token,
            ):
                dispatcher, job = self.job_dispatcher(marker_token)
                with mock.patch.object(
                    dispatcher,
                    "_proc_start_time",
                    return_value=current_token,
                ):
                    sent = dispatcher._signal_job(job, signal.SIGTERM)
                self.assertFalse(sent)
                dispatcher.executor.kill_pgid.assert_not_called()

    def test_exact_job_start_token_authorizes_signal(self) -> None:
        dispatcher, job = self.job_dispatcher("proc:200")
        with mock.patch.object(
            dispatcher,
            "_proc_start_time",
            return_value="proc:200",
        ):
            sent = dispatcher._signal_job(job, signal.SIGTERM)

        self.assertTrue(sent)
        dispatcher.executor.kill_pgid.assert_called_once_with(
            4242, signal.SIGTERM
        )

    def test_exact_darwin_start_token_authorizes_signal(self) -> None:
        token = "darwin:1788100000:123456"
        dispatcher, job = self.job_dispatcher(token)
        with mock.patch.object(
            dispatcher,
            "_proc_start_time",
            return_value=token,
        ):
            sent = dispatcher._signal_job(job, signal.SIGTERM)

        self.assertTrue(sent)
        dispatcher.executor.kill_pgid.assert_called_once_with(
            4242,
            signal.SIGTERM,
        )


    def test_second_resolution_ps_token_never_authorizes_signal(self) -> None:
        weak_token = "ps:Sun Aug 30 12:34:56 2026"
        dispatcher, job = self.job_dispatcher(weak_token)
        with mock.patch.object(
            dispatcher,
            "_proc_start_time",
            return_value=weak_token,
        ):
            sent = dispatcher._signal_job(job, signal.SIGTERM)

        self.assertFalse(sent)
        dispatcher.executor.kill_pgid.assert_not_called()


    def test_signal_result_distinguishes_transient_error_from_owned_exit(
        self,
    ) -> None:
        dispatcher, job = self.job_dispatcher("proc:200")
        dispatcher._job_process_state = mock.Mock(
            side_effect=["alive", "alive", "alive"]
        )
        dispatcher.executor.kill_pgid.return_value = False
        self.assertEqual(
            dispatcher_module._SIGNAL_UNKNOWN,
            dispatcher._signal_job_result(job, signal.SIGTERM),
        )
        dispatcher._job_process_state = mock.Mock(
            side_effect=["alive", "alive"]
        )
        dispatcher.executor.kill_pgid.side_effect = PermissionError(
            "transient permission probe failure"
        )
        self.assertEqual(
            dispatcher_module._SIGNAL_UNKNOWN,
            dispatcher._signal_job_result(job, signal.SIGTERM),
        )
        dispatcher.executor.kill_pgid.side_effect = None
        dispatcher.executor.kill_pgid.return_value = False

        dispatcher._job_process_state = mock.Mock(
            side_effect=["alive", "alive", "mismatch"]
        )
        self.assertEqual(
            dispatcher_module._SIGNAL_DEAD,
            dispatcher._signal_job_result(job, signal.SIGKILL),
        )

    def test_signal_rechecks_identity_immediately_before_delivery(self) -> None:
        dispatcher, job = self.job_dispatcher("proc:200")
        with mock.patch.object(
            dispatcher,
            "_proc_start_time",
            side_effect=["proc:200", "proc:300"],
        ):
            sent = dispatcher._signal_job(job, signal.SIGTERM)

        self.assertFalse(sent)
        dispatcher.executor.kill_pgid.assert_not_called()

    def test_orphaned_owned_group_rejects_even_explicit_escalation(self) -> None:
        dispatcher, job = self.job_dispatcher("proc:200")
        with mock.patch.object(
            dispatcher,
            "_proc_start_time",
            return_value=None,
        ):
            initial = dispatcher._signal_job(job, signal.SIGTERM)
            escalated = dispatcher._signal_job(
                job,
                signal.SIGKILL,
            )

        self.assertFalse(initial)
        self.assertFalse(escalated)
        dispatcher.executor.kill_pgid.assert_not_called()

    def test_precommit_marker_kills_exact_orphan_and_blocks_relaunch(self) -> None:
        self.seed_jobs([("precommit", {"gpu": 0, "cpus": 1}, "pending")])
        dispatcher = self.dispatcher()
        dispatcher.host_dir = self.tmp.name
        with state.connect() as conn:
            job = dict(state.get_job(conn, "precommit"))
        marker_path = dispatcher._launch_marker_path(job)
        os.makedirs(os.path.dirname(marker_path), mode=0o700, exist_ok=True)
        with open(marker_path, "w", encoding="utf-8") as stream:
            stream.write("5151 proc:100\n")
        dispatcher._proc_start_time = mock.Mock(return_value="proc:100")
        dispatcher.executor.alive.return_value = True
        dispatcher.executor.kill_pgid.return_value = True

        self.assertTrue(dispatcher._prepare_launch_marker(job))
        dispatcher.executor.kill_pgid.assert_called_once_with(5151, signal.SIGKILL)
        self.assertTrue(os.path.exists(marker_path))
        with state.connect() as conn:
            self.assertFalse(dispatcher._launch_job(conn, job, None))
            current = state.get_job(conn, "precommit")
        self.assertEqual("pending", current["status"])
        dispatcher.executor.launch.assert_not_called()

    def test_unknown_precommit_marker_blocks_without_signalling(self) -> None:
        self.seed_jobs([("unknown-marker", {"gpu": 0, "cpus": 1}, "pending")])
        dispatcher = self.dispatcher()
        dispatcher.host_dir = self.tmp.name
        with state.connect() as conn:
            job = dict(state.get_job(conn, "unknown-marker"))
        marker_path = dispatcher._launch_marker_path(job)
        os.makedirs(os.path.dirname(marker_path), mode=0o700, exist_ok=True)
        with open(marker_path, "w", encoding="utf-8") as stream:
            stream.write("6161 proc:200\n")
        dispatcher._proc_start_time = mock.Mock(return_value=None)
        dispatcher.executor.alive.side_effect = OSError("probe unavailable")

        self.assertTrue(dispatcher._prepare_launch_marker(job))
        self.assertTrue(os.path.exists(marker_path))
        dispatcher.executor.kill_pgid.assert_not_called()

    def test_reaper_settles_owned_exit_after_pgid_reuse_without_polling_group(
        self,
    ) -> None:
        self.seed_jobs([("reused-group", {"gpu": 0, "cpus": 1}, "running")])
        dispatcher = self.dispatcher()
        dispatcher.host_dir = self.tmp.name
        dispatcher._release_gpu_for_job = mock.Mock()
        dispatcher._consume_profile = mock.Mock()
        with state.connect() as conn:
            state.update_job(conn, "reused-group", pgid=7171)
            job = dict(state.get_job(conn, "reused-group"))
        marker_path = dispatcher._launch_marker_path(job)
        os.makedirs(os.path.dirname(marker_path), mode=0o700, exist_ok=True)
        with open(marker_path, "w", encoding="utf-8") as stream:
            stream.write("7171 proc:100\n")
        rc_path = dispatcher._job_rc_path(job)
        assert rc_path is not None
        os.makedirs(os.path.dirname(rc_path), mode=0o700, exist_ok=True)
        with open(rc_path, "w", encoding="utf-8") as stream:
            stream.write("0\n")
        dispatcher._proc_start_time = mock.Mock(return_value="proc:200")

        dispatcher._reap_finished_jobs()

        with state.connect() as conn:
            current = state.get_job(conn, "reused-group")
        self.assertEqual("done", current["status"])
        self.assertEqual(0, current["rc"])
        dispatcher.executor.poll_rc.assert_not_called()
        dispatcher.executor.kill_pgid.assert_not_called()


    def test_real_wrapper_marker_matches_live_process_identity(self) -> None:
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.host_dir = self.tmp.name
        dispatcher.executor = Executor()
        dispatcher.log_line = mock.Mock()
        job = {"id": "real-wrapper", "pgid": None}
        marker = dispatcher._launch_marker_path(job)
        pgid = dispatcher.executor.launch(
            cmd=["/bin/sleep", "30"],
            stages=None,
            cwd=self.tmp.name,
            env={"SCHED_LAUNCH_MARKER": marker},
            gpu=None,
            log_path=os.path.join(self.tmp.name, "wrapper.log"),
        )
        job["pgid"] = pgid
        try:
            states = []
            for _ in range(20):
                states.append(dispatcher._job_process_state(job))
                if states[-1] != "alive":
                    break
                time.sleep(0.01)
            self.assertEqual(["alive"] * len(states), states)
        finally:
            dispatcher.executor.kill_pgid(pgid, signal.SIGKILL)


class ReviewSubmitInboxBoundsTests(DispatcherStateCase):
    def queue_valid_payload(
        self,
        *,
        name: str = "gated-submit",
        bid: str = "gated-submit-1",
    ) -> tuple[Dispatcher, str]:
        dispatcher = self.dispatcher()
        dispatcher.cfg = self.cfg
        inbox_dir = os.path.join(self.tmp.name, "review-node", "submit_inbox")
        os.makedirs(inbox_dir, mode=0o700, exist_ok=True)
        payload_path = os.path.join(inbox_dir, f"submit-{bid}.json")
        spec = {
            "schema_version": 1,
            "name": name,
            "project": "p",
            "cwd": self.tmp.name,
            "tasks": [
                {
                    "id": "task",
                    "cmd": ["/bin/true"],
                    "resources": {"gpu": 0, "cpus": 1},
                }
            ],
        }
        with open(payload_path, "w", encoding="utf-8") as stream:
            json.dump({"spec": spec, "bid": bid}, stream)
        dispatcher._drain_submit_inbox()
        return dispatcher, payload_path

    def consume_payload(self, content: bytes) -> tuple[str, str]:
        dispatcher = self.dispatcher()
        dispatcher.cfg = self.cfg
        inbox_dir = os.path.join(self.tmp.name, "review-node", "submit_inbox")
        os.makedirs(inbox_dir, mode=0o700, exist_ok=True)
        payload_path = os.path.join(inbox_dir, "submit-bounded.json")
        with open(payload_path, "wb") as stream:
            stream.write(content)

        dispatcher._drain_submit_inbox()
        dispatcher._process_control_requests()

        with state.connect() as conn:
            request = conn.execute(
                "SELECT status, result FROM control_requests"
                " WHERE op='batch_submit' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertIsNotNone(request)
        self.assertFalse(os.path.exists(payload_path))
        return request["status"], request["result"]

    def test_oversized_submit_payload_is_deterministically_rejected(self) -> None:
        content = b"{" + b"x" * dispatcher_module.INBOX_MAX_BYTES

        status, result = self.consume_payload(content)

        self.assertEqual("done", status)
        self.assertIn("payload 过大", result)

    def test_deep_submit_payload_is_deterministically_rejected(self) -> None:
        nested: object = 0
        for _ in range(dispatcher_module.INBOX_MAX_DEPTH + 1):
            nested = {"nested": nested}
        content = json.dumps(
            {"spec": {"name": "deep"}, "padding": nested}
        ).encode("utf-8")

        status, result = self.consume_payload(content)

        self.assertEqual("done", status)
        self.assertIn("嵌套过深", result)

    def test_many_node_submit_payload_is_deterministically_rejected(self) -> None:
        content = json.dumps(
            {
                "spec": {"name": "many-nodes"},
                "padding": [0] * dispatcher_module.INBOX_MAX_NODES,
            }
        ).encode("utf-8")

        status, result = self.consume_payload(content)

        self.assertEqual("done", status)
        self.assertIn("节点过多", result)

    def test_valid_submit_rechecks_and_commits_inside_submission_gate(self) -> None:
        dispatcher, payload_path = self.queue_valid_payload()
        observed: list[tuple[str, int]] = []
        real_dependencies = dispatcher_module._validate_inbox_dependencies
        real_insert_batch = state.insert_batch

        def checked_dependencies(conn, norm):
            observed.append(("dependencies", state._submission_lock_depth.get()))
            return real_dependencies(conn, norm)

        def checked_insert(*args, **kwargs):
            observed.append(("insert", state._submission_lock_depth.get()))
            return real_insert_batch(*args, **kwargs)

        with mock.patch.object(
            dispatcher_module,
            "_validate_inbox_dependencies",
            side_effect=checked_dependencies,
        ), mock.patch.object(
            state,
            "insert_batch",
            side_effect=checked_insert,
        ):
            dispatcher._process_control_requests()

        with state.connect() as conn:
            batch = state.get_batch(conn, "gated-submit-1")
            request = conn.execute(
                "SELECT status FROM control_requests WHERE job_id=?",
                (payload_path,),
            ).fetchone()
        self.assertIsNotNone(batch)
        self.assertEqual("done", request["status"])
        self.assertFalse(os.path.exists(payload_path))
        self.assertEqual(
            [("dependencies", 1), ("insert", 1)],
            observed,
        )

    def test_shutdown_fence_keeps_prepared_submit_pending(self) -> None:
        dispatcher, payload_path = self.queue_valid_payload(
            name="shutdown-gated-submit",
            bid="shutdown-gated-submit-1",
        )
        observed_depths: list[int] = []

        def shutdown_active() -> bool:
            observed_depths.append(state._submission_lock_depth.get())
            return True

        with mock.patch.object(
            state,
            "submission_shutdown_active",
            side_effect=shutdown_active,
        ):
            dispatcher._process_control_requests()

        with state.connect() as conn:
            batch = state.get_batch(conn, "shutdown-gated-submit-1")
            request = conn.execute(
                "SELECT status FROM control_requests WHERE job_id=?",
                (payload_path,),
            ).fetchone()
        self.assertIsNone(batch)
        self.assertEqual("pending", request["status"])
        self.assertTrue(os.path.exists(payload_path))
        self.assertEqual([1], observed_depths)

    def test_duplicate_bid_is_finalized_inside_submission_gate(self) -> None:
        with state.connect() as conn:
            state.insert_batch(
                conn,
                "duplicate-submit-1",
                "duplicate-submit",
                "mix",
                [],
                None,
                self.tmp.name,
                {},
                project="p",
            )
            conn.execute(
                "UPDATE batches SET status='done' WHERE id='duplicate-submit-1'"
            )
        dispatcher, payload_path = self.queue_valid_payload(
            name="duplicate-submit",
            bid="duplicate-submit-1",
        )
        observed_depths: list[int] = []
        real_finish = state.finish_control_request

        def checked_finish(conn, request_id, result):
            if "重复投递" in result:
                observed_depths.append(state._submission_lock_depth.get())
            return real_finish(conn, request_id, result)

        with mock.patch.object(
            state,
            "finish_control_request",
            side_effect=checked_finish,
        ):
            dispatcher._process_control_requests()

        with state.connect() as conn:
            batches = conn.execute(
                "SELECT id FROM batches WHERE name='duplicate-submit'"
            ).fetchall()
            request = conn.execute(
                "SELECT status FROM control_requests WHERE job_id=?",
                (payload_path,),
            ).fetchone()
        self.assertEqual(["duplicate-submit-1"], [row["id"] for row in batches])
        self.assertEqual("done", request["status"])
        self.assertFalse(os.path.exists(payload_path))
        self.assertEqual([1], observed_depths)


class ReviewProgressRegexTests(DispatcherStateCase):
    def test_progress_scan_delegates_matching_to_the_bounded_worker(self) -> None:
        self.seed_jobs([("progress", {"gpu": 0, "cpus": 1}, "running")])
        with state.connect() as conn:
            row = conn.execute(
                "SELECT spec FROM tasks WHERE batch_id='batch' AND id='progress'"
            ).fetchone()
            spec = json.loads(row["spec"])
            spec["progress_regex"] = r"Epoch \d+/\d+"
            conn.execute(
                "UPDATE tasks SET spec=? WHERE batch_id='batch' AND id='progress'",
                (json.dumps(spec),),
            )
        log_dir = os.path.join(self.tmp.name, "logs", "batch")
        os.makedirs(log_dir)
        with open(
            os.path.join(log_dir, "progress-v1.log"),
            "w",
            encoding="utf-8",
        ) as stream:
            stream.write("Epoch 3/4\n")
        dispatcher = self.dispatcher()
        dispatcher.host_dir = self.tmp.name
        dispatcher._last_progress_scan = 0

        with mock.patch(
            "gsched.dispatcher.bounded_regex_last_match",
            return_value="Epoch 3/4",
        ) as bounded_match:
            dispatcher._scan_progress()

        bounded_match.assert_called_once()
        self.assertEqual(r"Epoch \d+/\d+", bounded_match.call_args.args[0])
        with state.connect() as conn:
            progress = conn.execute(
                "SELECT progress FROM jobs WHERE id='progress'"
            ).fetchone()["progress"]
        self.assertEqual("Epoch 3/4", progress)


class ReviewDispatcherLockTests(DispatcherStateCase):
    def lock_dispatcher(self, name: str) -> Dispatcher:
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.lock_dir = os.path.join(self.tmp.name, f"{name}.lock")
        dispatcher.pid_file = os.path.join(self.tmp.name, f"{name}.pid")
        dispatcher.heartbeat_file = os.path.join(self.tmp.name, f"{name}.heartbeat")
        dispatcher._notify_threads = []
        dispatcher.log_line = mock.Mock()
        return dispatcher

    def test_running_dispatcher_loses_lease_when_owner_file_is_replaced(self) -> None:
        dispatcher = self.lock_dispatcher("lease-loss")
        dispatcher._lease_owner = {
            "schema_version": 1,
            "lease_id": "mine",
            "pid": 123,
            "start_token": "proc:1",
            "physical_host": dispatcher_module.socket.gethostname().strip(),
        }
        replacement = {
            **dispatcher._lease_owner,
            "lease_id": "replacement",
        }
        with mock.patch.object(
            dispatcher,
            "_read_lock_owner",
            return_value=replacement,
        ):
            self.assertFalse(dispatcher._owns_current_lease())

    def write_expired_owner(
        self, dispatcher: Dispatcher, pid: int, start_token: str | None = None
    ) -> None:
        os.makedirs(dispatcher.lock_dir)
        owner = {
            "schema_version": 1,
            "lease_id": "expired",
            "pid": pid,
            "physical_host": dispatcher_module.socket.gethostname().strip(),
        }
        if start_token is not None:
            owner["start_token"] = start_token
        with open(dispatcher._lock_owner_file(), "w", encoding="utf-8") as stream:
            json.dump(owner, stream)
        with open(dispatcher.pid_file, "w", encoding="utf-8") as stream:
            stream.write(str(pid))
        os.utime(dispatcher.lock_dir, (1, 1))

    def test_s_h09_fresh_ownerless_lock_is_startup_in_progress_not_stale(self) -> None:
        dispatcher = self.lock_dispatcher("startup")
        os.makedirs(dispatcher.lock_dir)
        dispatcher._is_running = mock.Mock(return_value=False)
        dispatcher._cleanup_lock = mock.Mock()
        dispatcher._touch_heartbeat = mock.Mock()

        acquired = dispatcher.acquire_lock()

        self.assertFalse(acquired)
        self.assertTrue(os.path.isdir(dispatcher.lock_dir))
        dispatcher._cleanup_lock.assert_not_called()
        dispatcher._touch_heartbeat.assert_not_called()

    def test_s_h09_invalid_owner_is_never_treated_as_ownerless_stale_lock(self) -> None:
        dispatcher = self.lock_dispatcher("invalid-owner")
        os.makedirs(dispatcher.lock_dir)
        with open(dispatcher._lock_owner_file(), "w", encoding="utf-8") as stream:
            stream.write("{not valid json")
        os.utime(dispatcher.lock_dir, (1, 1))

        acquired = dispatcher.acquire_lock()

        self.assertFalse(acquired)
        self.assertTrue(os.path.isdir(dispatcher.lock_dir))
        self.assertTrue(os.path.lexists(dispatcher._lock_owner_file()))

    def test_s_h09_symlink_owner_is_invalid_and_never_reclaimed(self) -> None:
        dispatcher = self.lock_dispatcher("symlink-owner")
        os.makedirs(dispatcher.lock_dir)
        target = os.path.join(self.tmp.name, "foreign-owner.json")
        with open(target, "w", encoding="utf-8") as stream:
            json.dump({"pid": 9000, "start_token": "proc:old"}, stream)
        os.symlink(target, dispatcher._lock_owner_file())
        os.utime(dispatcher.lock_dir, (1, 1))
        dispatcher._pid_exists = mock.Mock(return_value=False)

        acquired = dispatcher.acquire_lock()

        self.assertFalse(acquired)
        self.assertTrue(os.path.islink(dispatcher._lock_owner_file()))

    def test_s_h09_racing_reclaimers_cannot_delete_a_replacement_lease(self) -> None:
        first = self.lock_dispatcher("race")
        second = self.lock_dispatcher("race")
        self.write_expired_owner(first, 9000, "proc:900")
        first._is_running = mock.Mock(return_value=False)
        second._is_running = mock.Mock(return_value=False)
        observed_expired = threading.Barrier(2)
        replacement_ready = threading.Event()
        original_reads = [first._read_lock_owner, second._read_lock_owner]

        def synchronized_read(index):
            calls = 0

            def read_owner():
                nonlocal calls
                owner = original_reads[index]()
                calls += 1
                if calls == 1:
                    observed_expired.wait(timeout=2)
                return owner

            return read_owner

        first._read_lock_owner = synchronized_read(0)
        second._read_lock_owner = synchronized_read(1)
        first._pid_exists = mock.Mock(return_value=False)

        def wait_for_replacement(_pid):
            self.assertTrue(replacement_ready.wait(timeout=2))
            return False

        second._pid_exists = mock.Mock(side_effect=wait_for_replacement)
        original_touch = first._touch_heartbeat

        def publish_replacement():
            original_touch()
            replacement_ready.set()

        first._touch_heartbeat = publish_replacement
        results = {}
        errors = []

        def reclaim(label, dispatcher):
            try:
                results[label] = dispatcher.acquire_lock()
            except BaseException as exc:
                errors.append(exc)

        def current_pid():
            return {"first-reclaimer": 101, "second-reclaimer": 202}.get(
                threading.current_thread().name, 303
            )

        with mock.patch("gsched.dispatcher.os.getpid", side_effect=current_pid):
            threads = [
                threading.Thread(
                    target=reclaim,
                    args=("first", first),
                    name="first-reclaimer",
                ),
                threading.Thread(
                    target=reclaim,
                    args=("second", second),
                    name="second-reclaimer",
                ),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=3)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual([], errors)
        self.assertEqual({"first": True, "second": False}, results)
        with open(first._lock_owner_file(), encoding="utf-8") as stream:
            final_owner = json.load(stream)
        self.assertEqual(101, final_owner["pid"])

    def test_s_h09_live_stale_owner_is_never_signalled_or_reclaimed(self) -> None:
        dispatcher = self.lock_dispatcher("live-owner")
        self.write_expired_owner(dispatcher, 5151, "proc:515100")
        dispatcher._is_running = mock.Mock(return_value=False)
        dispatcher._pid_exists = mock.Mock(return_value=True)
        dispatcher._heartbeat_fresh = mock.Mock(return_value=False)
        dispatcher._proc_start_time = mock.Mock(return_value="proc:515100")

        with mock.patch(
            "gsched.dispatcher.pid_cmdline_matches", return_value=True
        ), mock.patch("gsched.dispatcher.os.kill") as kill, mock.patch(
            "gsched.dispatcher.time.sleep"
        ):
            acquired = dispatcher.acquire_lock()

        self.assertFalse(acquired)
        kill.assert_not_called()
        self.assertTrue(os.path.isdir(dispatcher.lock_dir))

    def test_s_h09_pid_reuse_token_mismatch_reclaims_without_signalling(self) -> None:
        dispatcher = self.lock_dispatcher("reused-pid")
        self.write_expired_owner(dispatcher, 6161, "proc:616100")
        dispatcher._is_running = mock.Mock(return_value=False)
        dispatcher._pid_exists = mock.Mock(return_value=True)
        dispatcher._heartbeat_fresh = mock.Mock(return_value=False)
        dispatcher._proc_start_time = mock.Mock(return_value="proc:616200")

        with mock.patch(
            "gsched.dispatcher.pid_cmdline_matches", return_value=True
        ), mock.patch("gsched.dispatcher.os.kill") as kill, mock.patch(
            "gsched.dispatcher.time.sleep"
        ):
            acquired = dispatcher.acquire_lock()

        self.assertTrue(acquired)
        kill.assert_not_called()


class ReviewDispatcherGracefulStopTests(DispatcherStateCase):
    def test_term_ignoring_job_is_killed_exactly_before_state_release(self) -> None:
        self.seed_jobs([("term-ignore", {"gpu": 1}, "running")])
        with state.connect() as conn:
            state.update_job(conn, "term-ignore", pgid=4242)
        dispatcher = self.dispatcher()
        dispatcher.host_dir = self.tmp.name
        dispatcher._cleanup_lock = mock.Mock()
        dispatcher._signal_job = mock.Mock(return_value=True)
        dispatcher._job_process_state = mock.Mock(
            side_effect=["alive", "dead"]
        )
        dispatcher._wait_for_job_states = mock.Mock(
            side_effect=[
                {"term-ignore": "alive"},
                {"term-ignore": "dead"},
            ]
        )

        completed = dispatcher._stop_locked()

        self.assertTrue(completed)
        sent = [
            (call.args[1], call.kwargs)
            for call in dispatcher._signal_job.call_args_list
        ]
        self.assertEqual(
            [
                (signal.SIGTERM, {}),
                (signal.SIGKILL, {}),
            ],
            sent,
        )
        with state.connect() as conn:
            current = state.get_job(conn, "term-ignore")
        self.assertEqual("cancelled", current["status"])
        dispatcher._release_in_tx.assert_called_once_with(
            mock.ANY, "term-ignore"
        )
        dispatcher._cleanup_lock.assert_called_once_with()

    def test_unknown_job_identity_keeps_running_resources_and_lease(self) -> None:
        self.seed_jobs([("unknown-stop", {"gpu": 1}, "running")])
        with state.connect() as conn:
            state.update_job(conn, "unknown-stop", pgid=5252)
        dispatcher = self.dispatcher()
        dispatcher.host_dir = self.tmp.name
        dispatcher._cleanup_lock = mock.Mock()
        dispatcher._job_process_state = mock.Mock(return_value="unknown")
        dispatcher._signal_job = mock.Mock()
        dispatcher._wait_for_job_states = mock.Mock(
            side_effect=[
                {"unknown-stop": "unknown"},
                {"unknown-stop": "unknown"},
            ]
        )

        completed = dispatcher._stop_locked()

        self.assertFalse(completed)
        with state.connect() as conn:
            current = state.get_job(conn, "unknown-stop")
        self.assertEqual("running", current["status"])
        self.assertEqual(0, current["gpu"])
        dispatcher._signal_job.assert_not_called()
        dispatcher._release_in_tx.assert_not_called()
        dispatcher._cleanup_lock.assert_not_called()

    def test_exited_local_supervisor_proves_a_reused_group_is_not_our_job(
        self,
    ) -> None:
        self.seed_jobs([("reused-group", {"gpu": 1}, "running")])
        with state.connect() as conn:
            state.update_job(conn, "reused-group", pgid=6262)

        class ReusedGroupExecutor:
            @staticmethod
            def alive(_pgid):
                return True

            @staticmethod
            def local_supervisor_completed(_pgid):
                return True

        dispatcher = self.dispatcher()
        dispatcher.executor = ReusedGroupExecutor()
        dispatcher._read_launch_identity = mock.Mock(
            return_value=(6262, "darwin:100:200")
        )
        dispatcher._proc_start_time = mock.Mock(return_value=None)
        dispatcher._recover_launch_markers = mock.Mock()
        dispatcher._unresolved_launch_markers = mock.Mock(return_value=False)
        dispatcher._drop_job_rc = mock.Mock()
        dispatcher._drop_profile = mock.Mock()
        dispatcher._drop_launch_marker = mock.Mock()
        dispatcher._cleanup_lock = mock.Mock()
        dispatcher._signal_job = mock.Mock()

        with state.connect() as conn:
            job = state.get_job(conn, "reused-group")
        self.assertEqual("mismatch", dispatcher._job_process_state(job))
        self.assertTrue(dispatcher._stop_locked())

        with state.connect() as conn:
            current = state.get_job(conn, "reused-group")
        self.assertEqual("cancelled", current["status"])
        dispatcher._signal_job.assert_not_called()
        dispatcher._cleanup_lock.assert_called_once_with()

    def test_unknown_launch_marker_blocks_adoption_and_stop_settlement(
        self,
    ) -> None:
        self.seed_jobs([("unknown-launch", {"gpu": 1}, "running")])
        dispatcher = self.dispatcher()
        dispatcher.host_dir = self.tmp.name
        dispatcher._release_gpu_for_job = mock.Mock()
        dispatcher._consume_profile = mock.Mock()
        dispatcher._cleanup_lock = mock.Mock()
        with state.connect() as conn:
            state.update_job(conn, "unknown-launch", pgid=None)
            job = dict(state.get_job(conn, "unknown-launch"))
        marker_path = dispatcher._launch_marker_path(job)
        os.makedirs(os.path.dirname(marker_path), mode=0o700, exist_ok=True)
        with open(marker_path, "w", encoding="utf-8") as stream:
            stream.write("{invalid launch identity")

        dispatcher._adopt_running()
        with state.connect() as conn:
            after_adopt = state.get_job(conn, "unknown-launch")
        self.assertEqual("running", after_adopt["status"])
        self.assertIsNone(after_adopt["pgid"])
        self.assertTrue(os.path.isfile(marker_path))
        dispatcher._release_gpu_for_job.assert_not_called()

        self.assertFalse(dispatcher._stop_locked())
        with state.connect() as conn:
            after_stop = state.get_job(conn, "unknown-launch")
        self.assertEqual("running", after_stop["status"])
        self.assertIsNone(after_stop["pgid"])
        self.assertTrue(os.path.isfile(marker_path))
        dispatcher._release_in_tx.assert_not_called()
        dispatcher._cleanup_lock.assert_not_called()

    def test_run_keeps_lease_loop_alive_until_stop_completes(self) -> None:
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.fake = True
        dispatcher.allocator = mock.Mock(gpu_list=[])
        dispatcher.log_line = mock.Mock()
        dispatcher._recover_launch_markers = mock.Mock()
        dispatcher._adopt_running = mock.Mock()
        dispatcher._owns_current_lease = mock.Mock(return_value=True)
        dispatcher._heartbeat = mock.Mock()
        dispatcher._stop_requested = True
        dispatcher.stop = mock.Mock(side_effect=[False, True])
        dispatcher._tick = mock.Mock()
        dispatcher._cleanup_lock = mock.Mock()
        with mock.patch.object(
            state, "clear_idle_shutdown"
        ), mock.patch.object(
            state,
            "connect",
            return_value=contextlib.nullcontext(mock.Mock()),
        ), mock.patch.object(
            state, "init_gpus"
        ):
            dispatcher.run(once=True)

        self.assertEqual(2, dispatcher.stop.call_count)
        dispatcher._tick.assert_not_called()
        dispatcher._cleanup_lock.assert_called_once_with()



class ReviewDaemonShutdownTests(DispatcherStateCase):
    def test_s_h11_stop_waits_past_ten_seconds_for_legitimate_graceful_tick(
        self,
    ) -> None:
        owner = {
            "schema_version": 1,
            "lease_id": "lease",
            "pid": 123,
            "start_token": "proc:100",
            "physical_host": daemon.socket.gethostname().strip(),
        }
        token_calls = {"count": 0}

        def current_start(_pid):
            token_calls["count"] += 1
            return "proc:100" if token_calls["count"] < 25 else None

        sent = []
        with mock.patch.object(
            daemon, "_read_lease_owner", return_value=owner
        ), mock.patch.object(
            daemon,
            "_pid_alive",
            side_effect=lambda _pid: token_calls["count"] < 25,
        ), mock.patch.object(
            daemon, "process_start_token", side_effect=current_start
        ), mock.patch.object(
            daemon.time, "sleep", return_value=None
        ), mock.patch.object(
            daemon.os,
            "kill",
            side_effect=lambda pid, sig: sent.append((pid, sig)),
        ), mock.patch.object(
            state, "submission_lock", return_value=contextlib.nullcontext()
        ), mock.patch.object(
            state, "mark_idle_shutdown"
        ), mock.patch.object(
            daemon, "_cleanup"
        ) as cleanup:
            text = daemon.stop()

        self.assertIn("已停止", text)
        self.assertGreater(token_calls["count"], 22)
        self.assertIn((123, signal.SIGTERM), sent)
        self.assertNotIn((123, signal.SIGKILL), sent)
        cleanup.assert_called_once_with(owner)

    def test_s_h11_stop_rejects_reused_daemon_pid_by_exact_start_token(
        self,
    ) -> None:
        owner = {
            "schema_version": 1,
            "lease_id": "lease",
            "pid": 123,
            "start_token": "proc:100",
            "physical_host": daemon.socket.gethostname().strip(),
        }
        with mock.patch.object(
            daemon, "_read_lease_owner", return_value=owner
        ), mock.patch.object(
            daemon, "_pid_alive", return_value=True
        ), mock.patch.object(
            daemon, "process_start_token", return_value="proc:200"
        ), mock.patch.object(
            daemon, "_heartbeat_fresh", return_value=True
        ), mock.patch.object(
            daemon.os, "kill"
        ) as kill:
            text = daemon.stop()

        self.assertIn("身份", text)
        kill.assert_not_called()

    def test_stop_preserves_ownership_when_identity_becomes_unknown(self) -> None:
        owner = {
            "schema_version": 1,
            "lease_id": "lease",
            "pid": 123,
            "start_token": "proc:100",
            "physical_host": daemon.socket.gethostname().strip(),
        }
        tokens = iter(["proc:100", "proc:100", None, None, None])
        with mock.patch.object(
            daemon, "_read_lease_owner", return_value=owner
        ), mock.patch.object(
            daemon, "_pid_alive", return_value=True
        ), mock.patch.object(
            daemon, "process_start_token", side_effect=lambda _pid: next(tokens, None)
        ), mock.patch.object(
            daemon, "STOP_TIMEOUT_SEC", 1
        ), mock.patch.object(
            daemon.time, "sleep", return_value=None
        ), mock.patch.object(
            daemon.os, "kill"
        ) as kill, mock.patch.object(
            state, "submission_lock", return_value=contextlib.nullcontext()
        ), mock.patch.object(
            state, "mark_idle_shutdown", return_value="stop-token"
        ), mock.patch.object(
            state, "clear_idle_shutdown"
        ) as clear_idle, mock.patch.object(
            daemon, "_cleanup"
        ) as cleanup:
            text = daemon.stop()

        self.assertIn("超时", text)
        kill.assert_called_once_with(123, signal.SIGTERM)
        clear_idle.assert_not_called()
        cleanup.assert_not_called()

    def test_start_fails_when_child_exits_before_readiness(self) -> None:
        proc = mock.Mock(pid=321)
        proc.poll.return_value = 9
        with mock.patch.object(
            daemon, "check", return_value=[]
        ), mock.patch.object(
            daemon, "is_running", return_value=False
        ), mock.patch.object(
            daemon.subprocess, "Popen", return_value=proc
        ), mock.patch.object(
            daemon, "process_start_token", return_value="proc:300"
        ), mock.patch.object(
            daemon.time, "sleep", return_value=None
        ):
            text = daemon.start(fake=True, force=True)

        self.assertIn("启动失败", text)
        self.assertIn("rc=9", text)

    def test_start_fails_and_exactly_terminates_child_that_never_readies(
        self,
    ) -> None:
        proc = mock.Mock(pid=654)
        proc.poll.return_value = None
        proc.wait.side_effect = [
            subprocess.TimeoutExpired("dispatcher", 2),
            0,
        ]
        with mock.patch.object(
            daemon, "check", return_value=[]
        ), mock.patch.object(
            daemon, "is_running", return_value=False
        ), mock.patch.object(
            daemon.subprocess, "Popen", return_value=proc
        ), mock.patch.object(
            daemon, "process_start_token", return_value="proc:600"
        ), mock.patch.object(
            daemon, "START_TIMEOUT_SEC", 0
        ), mock.patch.object(
            daemon.time, "sleep", return_value=None
        ), mock.patch.object(
            daemon.os, "kill"
        ) as kill:
            text = daemon.start(fake=True, force=True)

        self.assertIn("启动失败", text)
        self.assertIn("未就绪", text)
        self.assertEqual(
            [
                mock.call(654, signal.SIGTERM),
                mock.call(654, signal.SIGKILL),
            ],
            kill.call_args_list,
        )


class ReviewAcceptanceCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.helper = os.path.join(
            os.path.dirname(__file__), "acceptance_cleanup.sh"
        )
        self.calls = os.path.join(self.tmp.name, "cleanup.calls")
        self.fake_python = os.path.join(self.tmp.name, "python")
        with open(self.fake_python, "w", encoding="utf-8") as stream:
            stream.write(
                "#!/bin/sh\n"
                "printf '%s|%s\\n' \"$SCHED_STATE\" \"$*\" >> \"$CLEANUP_LOG\"\n"
                "case \"$*\" in\n"
                "  *\"status --json\") "
                "printf '%s\\n' "
                "'{\"schema_version\":1,\"limit\":100,\"batches\":[],"
                "\"jobs\":[],\"gpus\":[],\"truncated\":{\"batches\":false,"
                "\"jobs\":false},\"next_cursor\":null,\"next_job_cursor\":null,"
                "\"daemon_health\":{},\"cpu\":{\"used\":0,\"total\":1}}' ;;\n"
                "esac\n"
            )
        os.chmod(self.fake_python, 0o700)

    def run_cleanup_shell(self, ending: str) -> subprocess.CompletedProcess:
        script = (
            f"source {self.helper!r}; "
            "sched_accept_make_root STATE review-cleanup; "
            "touch \"$STATE/config.json\"; "
            # Force the runtime-present branch: runtime-absent roots now skip
            # needless CLI calls, while this legacy contract specifically
            # verifies stop/status are the only scheduler operations used.
            "mkdir -p \"$STATE/testnode\"; "
            "touch \"$STATE/testnode/daemon.pid\"; "
            f"{ending}"
        )
        return subprocess.run(
            ["/bin/bash", "-c", script],
            env={
                **os.environ,
                "PY": self.fake_python,
                "CLEANUP_LOG": self.calls,
            },
            capture_output=True,
            text=True,
            check=False,
        )

    def test_cleanup_is_cli_only_and_idempotent(self) -> None:
        completed = self.run_cleanup_shell(
            "sched_accept_cleanup; sched_accept_cleanup"
        )

        self.assertEqual(0, completed.returncode, completed.stderr)
        with open(self.calls, encoding="utf-8") as stream:
            calls = stream.read().splitlines()
        self.assertEqual(2, len(calls))
        self.assertTrue(calls[0].endswith("|-m gsched.cli daemon stop"), calls)
        self.assertTrue(calls[1].endswith("|-m gsched.cli status --json"), calls)
        root = calls[0].split("|", 1)[0]
        self.assertEqual(root, calls[1].split("|", 1)[0])
        self.assertFalse(os.path.exists(root))

    def test_interrupt_and_term_run_registered_cleanup(self) -> None:
        for sig, expected_rc in (("INT", 130), ("TERM", 143)):
            with self.subTest(sig=sig):
                try:
                    os.unlink(self.calls)
                except FileNotFoundError:
                    pass
                completed = self.run_cleanup_shell(f"kill -{sig} $$")
                self.assertEqual(expected_rc, completed.returncode)
                with open(self.calls, encoding="utf-8") as stream:
                    calls = stream.read().splitlines()
                self.assertEqual(2, len(calls))
                self.assertTrue(
                    calls[0].endswith("|-m gsched.cli daemon stop"), calls
                )
                self.assertTrue(
                    calls[1].endswith("|-m gsched.cli status --json"), calls
                )
                root = calls[0].split("|", 1)[0]
                self.assertEqual(root, calls[1].split("|", 1)[0])
                self.assertFalse(os.path.exists(root))




class ReviewUuidMappingTests(unittest.TestCase):
    @staticmethod
    def completed(argv, rc=0, stdout=""):
        return subprocess.CompletedProcess(argv, rc, stdout=stdout, stderr="")

    def allocator(self):
        allocator = Allocator.__new__(Allocator)
        allocator.fake = False
        allocator._uuid_map = None
        return allocator

    def test_s_h12_transient_uuid_mapping_failure_is_not_cached_and_recovers(self) -> None:
        allocator = self.allocator()
        mapping_attempt = {"count": 0}

        def run(argv, **_kwargs):
            if any("query-compute-apps" in arg for arg in argv):
                return self.completed(argv, stdout="77, GPU-a\n")
            mapping_attempt["count"] += 1
            if mapping_attempt["count"] == 1:
                return self.completed(argv, rc=1)
            return self.completed(argv, stdout="0, GPU-a\n")

        with mock.patch("gsched.allocator.subprocess.run", side_effect=run):
            first = allocator._compute_pids_by_card()
            cache_after_failure = allocator._uuid_map
            second = allocator._compute_pids_by_card()

        self.assertIsNone(first)
        self.assertIsNone(cache_after_failure)
        self.assertEqual({0: [77]}, second)
        self.assertEqual(3, mapping_attempt["count"])

    def test_s_h12_unknown_compute_uuid_is_indeterminate_not_empty(self) -> None:
        allocator = self.allocator()

        def run(argv, **_kwargs):
            if any("query-compute-apps" in arg for arg in argv):
                return self.completed(argv, stdout="77, GPU-unknown\n")
            return self.completed(argv, stdout="0, GPU-known\n")

        with mock.patch("gsched.allocator.subprocess.run", side_effect=run):
            result = allocator._compute_pids_by_card()
        self.assertIsNone(result)

    def test_s_h12_cached_uuid_map_is_revalidated_before_pid_attribution(self) -> None:
        allocator = self.allocator()
        allocator.gpu_list = [0, 1]
        topology_attempts = {"count": 0}

        def run(argv, **_kwargs):
            if any("query-compute-apps" in arg for arg in argv):
                return self.completed(argv, stdout="77, GPU-a\n")
            topology_attempts["count"] += 1
            if topology_attempts["count"] <= 3:
                return self.completed(argv, stdout="0, GPU-a\n1, GPU-b\n")
            return self.completed(argv, stdout="0, GPU-b\n1, GPU-a\n")

        with mock.patch("gsched.allocator.subprocess.run", side_effect=run):
            first = allocator._compute_pids_by_card()
            reordered = allocator._compute_pids_by_card()

        self.assertEqual({0: [77]}, first)
        self.assertIsNone(reordered)
        self.assertEqual(4, topology_attempts["count"])

    def test_s_h12_first_pid_sample_is_bracketed_by_stable_topology(self) -> None:
        allocator = self.allocator()
        allocator.gpu_list = [0, 1]
        topology_attempts = {"count": 0}

        def run(argv, **_kwargs):
            if any("query-compute-apps" in arg for arg in argv):
                return self.completed(argv, stdout="77, GPU-a\n")
            topology_attempts["count"] += 1
            if topology_attempts["count"] == 1:
                return self.completed(argv, stdout="0, GPU-a\n1, GPU-b\n")
            return self.completed(argv, stdout="0, GPU-b\n1, GPU-a\n")

        with mock.patch("gsched.allocator.subprocess.run", side_effect=run):
            result = allocator._compute_pids_by_card()

        self.assertIsNone(result)
        self.assertEqual(2, topology_attempts["count"])


class ReviewProfileConsumptionTests(DispatcherStateCase):
    def profile_dispatcher(self) -> Dispatcher:
        dispatcher = self.dispatcher()
        dispatcher.host_dir = self.tmp.name
        os.makedirs(
            os.path.join(dispatcher.host_dir, "profiles"),
            mode=0o700,
            exist_ok=True,
        )
        return dispatcher

    def test_untrusted_profile_shapes_and_file_types_are_ignored(self) -> None:
        dispatcher = self.profile_dispatcher()
        target = os.path.join(self.tmp.name, "profile-target.json")
        with open(target, "w", encoding="utf-8") as stream:
            json.dump({"peak_gib": 2.5}, stream)

        cases = (
            "list",
            "extra-key",
            "infinity",
            "negative",
            "excessive",
            "oversize",
            "directory",
            "symlink",
            "hardlink",
        )
        for case in cases:
            with self.subTest(case=case):
                profile_key = f"unsafe-profile-{case}"
                spec = {"resources": {"profile_key": profile_key}}
                job = {
                    "id": f"profile-{case}",
                    "project": "p",
                    "git_rev": None,
                }
                path = dispatcher._profile_path(job)
                if case == "list":
                    with open(path, "w", encoding="utf-8") as stream:
                        json.dump([{"peak_gib": 1.0}], stream)
                elif case == "extra-key":
                    with open(path, "w", encoding="utf-8") as stream:
                        json.dump({"peak_gib": 1.0, "unexpected": True}, stream)
                elif case == "infinity":
                    with open(path, "w", encoding="utf-8") as stream:
                        stream.write('{"peak_gib": Infinity}')
                elif case in {"negative", "excessive"}:
                    peak = -0.1 if case == "negative" else 1024.1
                    with open(path, "w", encoding="utf-8") as stream:
                        json.dump({"peak_gib": peak}, stream)
                elif case == "oversize":
                    with open(path, "wb") as stream:
                        stream.write(b" " * (64 * 1024 + 1))
                elif case == "directory":
                    os.mkdir(path)
                elif case == "symlink":
                    os.symlink(target, path)
                else:
                    os.link(target, path)

                with state.connect() as conn:
                    dispatcher._consume_profile(conn, job, spec)
                    cached_count = conn.execute(
                        "SELECT COUNT(*) AS n FROM profile_cache"
                    ).fetchone()["n"]
                self.assertEqual(0, cached_count)

    def test_valid_profile_is_upserted_without_precommit_unlink(self) -> None:
        dispatcher = self.profile_dispatcher()
        job = {
            "id": "profile-valid",
            "project": "p",
            "git_rev": "abc123",
        }
        path = dispatcher._profile_path(job)
        with open(path, "w", encoding="utf-8") as stream:
            json.dump({"peak_gib": 3.25}, stream)

        with state.connect() as conn:
            dispatcher._consume_profile(
                conn,
                job,
                {"resources": {"profile_key": "valid-profile"}},
            )
            cached = conn.execute(
                "SELECT peak_gib, git_rev FROM profile_cache"
            ).fetchone()

        self.assertEqual(3.25, cached["peak_gib"])
        self.assertEqual("abc123", cached["git_rev"])
        self.assertTrue(os.path.isfile(path))

    def test_profile_recursion_error_does_not_block_job_settlement(self) -> None:
        self.seed_jobs(
            [
                (
                    "profile-recursion",
                    {"gpu": 1, "profile_key": "recursive-profile"},
                    "running",
                )
            ]
        )
        dispatcher = self.profile_dispatcher()
        profile_path = dispatcher._profile_path({"id": "profile-recursion"})
        depth = sys.getrecursionlimit() + 100
        with open(profile_path, "w", encoding="utf-8") as stream:
            stream.write("[" * depth + "0" + "]" * depth)

        with state.connect() as conn:
            state.update_job(conn, "profile-recursion", rc=0)
            job = dict(state.get_job(conn, "profile-recursion"))
            cleanup = dispatcher._handle_job_done(conn, job, 0)
            settled = state.get_job(conn, "profile-recursion")

        self.assertEqual("done", settled["status"])
        self.assertEqual(0, settled["rc"])
        self.assertEqual(["launch", "profile"], [kind for kind, _ in cleanup])
        dispatcher._release_in_tx.assert_called_once_with(
            mock.ANY,
            "profile-recursion",
        )

    def test_sched_profile_out_is_forced_to_the_dispatcher_owned_path(
        self,
    ) -> None:
        self.seed_jobs([("profile-env", {"gpu": 0, "cpus": 1}, "pending")])
        task_selected_dir = os.path.join(
            self.tmp.name,
            "task-selected",
            "profiles",
        )
        task_selected_path = os.path.join(task_selected_dir, "stolen.json")
        batch_selected_dir = os.path.join(
            self.tmp.name,
            "batch-selected",
            "profiles",
        )
        batch_selected_path = os.path.join(batch_selected_dir, "stolen.json")
        with state.connect() as conn:
            row = conn.execute(
                "SELECT spec FROM tasks"
                " WHERE batch_id='batch' AND id='profile-env'"
            ).fetchone()
            spec = json.loads(row["spec"])
            spec["env"] = {"SCHED_PROFILE_OUT": task_selected_path}
            conn.execute(
                "UPDATE tasks SET spec=?"
                " WHERE batch_id='batch' AND id='profile-env'",
                (json.dumps(spec),),
            )
            conn.execute(
                "UPDATE batches SET env=? WHERE id='batch'",
                (
                    json.dumps(
                        {"SCHED_PROFILE_OUT": batch_selected_path}
                    ),
                ),
            )
        config_selected_dir = os.path.join(
            self.tmp.name,
            "config-selected",
            "profiles",
        )
        config_selected_path = os.path.join(config_selected_dir, "stolen.json")

        dispatcher = self.profile_dispatcher()
        dispatcher.cfg["task_default_env"] = {
            "SCHED_PROFILE_OUT": config_selected_path
        }
        dispatcher._snapshot_fingerprint = mock.Mock(
            return_value=(None, {}, None)
        )
        dispatcher._should_skip = mock.Mock(return_value=False)
        dispatcher._clean_stale_artifacts = mock.Mock()
        dispatcher.executor.launch.return_value = 5252
        with state.connect() as conn:
            job = dict(state.get_job(conn, "profile-env"))
            self.assertTrue(dispatcher._launch_job(conn, job, None))

        launch_env = dispatcher.executor.launch.call_args.kwargs["env"]
        self.assertEqual(
            dispatcher._profile_path(job),
            launch_env["SCHED_PROFILE_OUT"],
        )
        self.assertFalse(os.path.exists(task_selected_dir))
        self.assertFalse(os.path.exists(batch_selected_dir))

        self.assertFalse(os.path.exists(config_selected_dir))

    def test_profile_cache_key_is_collision_free_and_packing_is_project_isolated(
        self,
    ) -> None:
        dispatcher = self.profile_dispatcher()
        dispatcher.cfg["projects"]["q"] = {
            "root": self.tmp.name,
            "git": False,
            "gpu_quota": 1,
        }
        for project in ("tenant:a", "tenant"):
            dispatcher.cfg["projects"][project] = {
                "root": self.tmp.name,
                "git": False,
                "gpu_quota": 1,
            }
        dispatcher.cfg["co_locate"] = True
        dispatcher.cfg["co_locate_safety"] = 1.0
        dispatcher.allocator = mock.Mock(gpu_list=[0])
        dispatcher.allocator.mem_total.return_value = 8.0
        dispatcher._gpu_max_jobs = {}
        dispatcher._frozen_gpus = set()
        dispatcher._cap_warned = set()

        with state.connect() as conn:
            profiles = (
                ("p", "shared-model", 2.0),
                ("q", "shared-model", 9.0),
                ("tenant:a", "model", 3.0),
                ("tenant", "a:model", 7.0),
            )
            for index, (project, profile_key, peak) in enumerate(profiles):
                job = {
                    "id": f"profile-collision-{index}",
                    "project": project,
                    "git_rev": f"{project}-rev",
                }
                with open(
                    dispatcher._profile_path(job),
                    "w",
                    encoding="utf-8",
                ) as stream:
                    json.dump({"peak_gib": peak}, stream)
                dispatcher._consume_profile(
                    conn,
                    job,
                    {"resources": {"profile_key": profile_key}},
                )

            cached_count = conn.execute(
                "SELECT COUNT(*) AS n FROM profile_cache"
            ).fetchone()["n"]
            packing_spec = {
                "resources": {
                    "gpu_share": True,
                    "vram_gib": 1.0,
                    "profile_key": "shared-model",
                }
            }
            q_gpu = dispatcher._assign_in_tx(
                conn,
                "packing-q",
                packing_spec,
                project="q",
            )
            p_gpu = dispatcher._assign_in_tx(
                conn,
                "packing-p",
                packing_spec,
                project="p",
            )
            p_vram = conn.execute(
                "SELECT vram_gib FROM gpu_jobs WHERE job_id='packing-p'"
            ).fetchone()["vram_gib"]
            conn.execute("DELETE FROM gpu_jobs")
            conn.execute(
                "UPDATE gpus SET status='free', job_id=NULL WHERE idx=0"
            )

            first_collision_gpu = dispatcher._assign_in_tx(
                conn,
                "packing-collision-0",
                {
                    "resources": {
                        "gpu_share": True,
                        "vram_gib": 1.0,
                        "profile_key": "model",
                    }
                },
                project="tenant:a",
            )
            first_collision_vram = conn.execute(
                "SELECT vram_gib FROM gpu_jobs"
                " WHERE job_id='packing-collision-0'"
            ).fetchone()["vram_gib"]
            conn.execute("DELETE FROM gpu_jobs")
            conn.execute(
                "UPDATE gpus SET status='free', job_id=NULL WHERE idx=0"
            )

            second_collision_gpu = dispatcher._assign_in_tx(
                conn,
                "packing-collision-1",
                {
                    "resources": {
                        "gpu_share": True,
                        "vram_gib": 1.0,
                        "profile_key": "a:model",
                    }
                },
                project="tenant",
            )
            second_collision_vram = conn.execute(
                "SELECT vram_gib FROM gpu_jobs"
                " WHERE job_id='packing-collision-1'"
            ).fetchone()["vram_gib"]

        self.assertEqual(4, cached_count)
        self.assertIsNone(q_gpu)
        self.assertEqual(0, p_gpu)
        self.assertEqual(2.0, p_vram)
        self.assertEqual(0, first_collision_gpu)
        self.assertEqual(3.0, first_collision_vram)
        self.assertEqual(0, second_collision_gpu)
        self.assertEqual(7.0, second_collision_vram)


class ReviewDurableDispatcherEffectsTests(DispatcherStateCase):
    @staticmethod
    @contextlib.contextmanager
    def failing_connect(original_connect):
        with original_connect() as conn:
            yield conn
            raise sqlite3.OperationalError("injected commit failure")

    def test_batch_markers_and_notifications_wait_for_commit(self) -> None:
        self.seed_jobs([("terminal", {"gpu": 0, "cpus": 1}, "done")])
        dispatcher = self.dispatcher()
        dispatcher._notify_threads = []
        dispatcher._write_marker = mock.Mock()
        dispatcher._remove_marker = mock.Mock()
        dispatcher._notify_batch = mock.Mock()
        original_connect = state.connect

        with mock.patch.object(
            state,
            "connect",
            side_effect=lambda: self.failing_connect(original_connect),
        ):
            with self.assertRaisesRegex(sqlite3.OperationalError, "commit failure"):
                dispatcher._settle_batch_status()

        dispatcher._write_marker.assert_not_called()
        dispatcher._remove_marker.assert_not_called()
        dispatcher._notify_batch.assert_not_called()

        with original_connect() as conn:
            conn.execute("UPDATE batches SET status='blocked' WHERE id='batch'")
            state.update_job(conn, "terminal", status="pending")
        with mock.patch.object(
            state,
            "connect",
            side_effect=lambda: self.failing_connect(original_connect),
        ):
            with self.assertRaisesRegex(sqlite3.OperationalError, "commit failure"):
                dispatcher._settle_batch_status()

        dispatcher._write_marker.assert_not_called()
        dispatcher._remove_marker.assert_not_called()
        dispatcher._notify_batch.assert_not_called()

    def test_reaper_cleanup_waits_for_commit(self) -> None:
        self.seed_jobs([("cleanup", {"gpu": 0, "cpus": 1}, "running")])
        dispatcher = self.dispatcher()
        dispatcher.host_dir = state.host_dir()
        dispatcher._job_process_state = mock.Mock(return_value="dead")
        dispatcher.executor.has_process.return_value = True
        dispatcher.executor.poll_rc.return_value = 0
        with state.connect() as conn:
            state.update_job(conn, "cleanup", pgid=4242)
            job = dict(state.get_job(conn, "cleanup"))

        paths = (
            dispatcher._launch_marker_path(job),
            dispatcher._job_rc_path(job),
            dispatcher._profile_path(job),
        )
        assert paths[1] is not None
        for path in paths:
            os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
        with open(paths[0], "w", encoding="utf-8") as stream:
            stream.write("4242 proc:1\n")
        with open(paths[1], "w", encoding="utf-8") as stream:
            stream.write("0\n")
        with open(paths[2], "w", encoding="utf-8") as stream:
            json.dump({"peak_gib": 1.25}, stream)

        original_connect = state.connect
        with mock.patch.object(
            state,
            "connect",
            side_effect=lambda: self.failing_connect(original_connect),
        ):
            with self.assertRaisesRegex(sqlite3.OperationalError, "commit failure"):
                dispatcher._reap_finished_jobs()

        self.assertTrue(all(os.path.exists(path) for path in paths))

    def test_timeout_signal_observes_durable_kill_reason(self) -> None:
        self.seed_jobs([("timeout", {"gpu": 0, "cpus": 1}, "running")])
        with state.connect() as conn:
            row = conn.execute(
                "SELECT spec FROM tasks WHERE batch_id='batch' AND id='timeout'"
            ).fetchone()
            spec = json.loads(row["spec"])
            spec["duration_min"] = 0.01
            conn.execute(
                "UPDATE tasks SET spec=? WHERE batch_id='batch' AND id='timeout'",
                (json.dumps(spec),),
            )
            state.update_job(
                conn,
                "timeout",
                pgid=4242,
                started_at="2000-01-01 00:00:00",
            )

        dispatcher = self.dispatcher()
        original_connect = state.connect
        observed_reasons = []

        def signal_spy(_job, _sig=signal.SIGTERM, **_kwargs):
            with original_connect() as observer:
                observed_reasons.append(
                    state.get_job(observer, "timeout")["kill_reason"]
                )
            return dispatcher_module._SIGNAL_SENT

        dispatcher._signal_job_result = mock.Mock(side_effect=signal_spy)
        dispatcher._check_timeouts()

        self.assertEqual(["timed_out"], observed_reasons)

    def test_cancel_transient_term_failure_keeps_request_and_intent(self) -> None:
        self.seed_jobs([("cancel-fail", {"gpu": 0, "cpus": 1}, "running")])
        with state.connect() as conn:
            state.update_job(conn, "cancel-fail", pgid=4242)
            request_id = state.insert_control_request(conn, "cancel-fail")

        dispatcher = self.dispatcher()
        dispatcher._job_process_state = mock.Mock(return_value="alive")
        dispatcher._signal_job_result = mock.Mock(
            return_value=dispatcher_module._SIGNAL_UNKNOWN
        )
        dispatcher._process_control_requests()

        with state.connect() as conn:
            job = state.get_job(conn, "cancel-fail")
            request = conn.execute(
                "SELECT status, result FROM control_requests WHERE id=?",
                (request_id,),
            ).fetchone()
        self.assertEqual("cancelled", job["kill_reason"])
        self.assertEqual("pending", request["status"])
        self.assertIsNone(request["result"])

        dispatcher._signal_job_result.return_value = (
            dispatcher_module._SIGNAL_SENT
        )
        dispatcher._process_control_requests()
        with state.connect() as conn:
            request = conn.execute(
                "SELECT status, result FROM control_requests WHERE id=?",
                (request_id,),
            ).fetchone()
        self.assertEqual("done", request["status"])
        self.assertIn("SIGKILL", request["result"])

    def test_orphan_group_stays_pending_but_identity_mismatch_completes(
        self,
    ) -> None:
        self.seed_jobs(
            [
                ("cancel-orphan", {"gpu": 0, "cpus": 1}, "running"),
                ("cancel-mismatch", {"gpu": 0, "cpus": 1}, "running"),
            ]
        )
        with state.connect() as conn:
            for job_id in ("cancel-orphan", "cancel-mismatch"):
                state.update_job(
                    conn,
                    job_id,
                    pgid=4242,
                    kill_reason="cancelled",
                )
            orphan_request = state.insert_control_request(
                conn, "cancel-orphan"
            )
            mismatch_request = state.insert_control_request(
                conn, "cancel-mismatch"
            )

        dispatcher = self.dispatcher()
        dispatcher._job_process_state = mock.Mock(
            side_effect=["group_alive", "mismatch"]
        )
        dispatcher._signal_job_result = mock.Mock()
        dispatcher._process_control_requests()

        with state.connect() as conn:
            requests = {
                row["id"]: row
                for row in conn.execute(
                    "SELECT id, status, result FROM control_requests"
                    " WHERE id IN (?, ?)",
                    (orphan_request, mismatch_request),
                )
            }
        self.assertEqual("pending", requests[orphan_request]["status"])
        self.assertEqual("done", requests[mismatch_request]["status"])
        self.assertIn("mismatch", requests[mismatch_request]["result"])
        dispatcher._signal_job_result.assert_not_called()

    def test_timeout_and_probe_transient_signal_failure_preserves_intents(
        self,
    ) -> None:
        self.seed_jobs(
            [
                ("timeout-fail", {"gpu": 0, "cpus": 1}, "running"),
                ("probe-fail", {"gpu": 0, "cpus": 1}, "running"),
            ]
        )
        probe_log = os.path.join(self.tmp.name, "probe-fail.log")
        with open(probe_log, "w", encoding="utf-8") as stream:
            stream.write("FAIL-MATCH\n")
        with state.connect() as conn:
            for job_id in ("timeout-fail", "probe-fail"):
                row = conn.execute(
                    "SELECT spec FROM tasks WHERE batch_id='batch' AND id=?",
                    (job_id,),
                ).fetchone()
                spec = json.loads(row["spec"])
                if job_id == "timeout-fail":
                    spec["duration_min"] = 0.01
                else:
                    spec["probes"] = {"fail_on_log": "FAIL-MATCH"}
                conn.execute(
                    "UPDATE tasks SET spec=?"
                    " WHERE batch_id='batch' AND id=?",
                    (json.dumps(spec), job_id),
                )
                state.update_job(
                    conn,
                    job_id,
                    pgid=4242,
                    started_at="2000-01-01 00:00:00",
                )

        dispatcher = self.dispatcher()
        dispatcher._probe_offsets = {}
        dispatcher._job_log_path = mock.Mock(return_value=probe_log)
        dispatcher._signal_job_result = mock.Mock(
            return_value=dispatcher_module._SIGNAL_UNKNOWN
        )
        dispatcher._check_timeouts()
        dispatcher._check_probes()

        with state.connect() as conn:
            reasons = {
                job_id: state.get_job(conn, job_id)["kill_reason"]
                for job_id in ("timeout-fail", "probe-fail")
            }
        self.assertEqual(
            {
                "timeout-fail": "timed_out",
                "probe-fail": "probe_failed",
            },
            reasons,
        )

    def test_failed_kill_escalation_preserves_existing_intents(self) -> None:
        reasons = {
            "cancel-escalate": "cancelled",
            "timeout-escalate": "timed_out",
            "probe-escalate": "probe_failed",
        }
        self.seed_jobs(
            [
                (job_id, {"gpu": 0, "cpus": 1}, "running")
                for job_id in reasons
            ]
        )
        with state.connect() as conn:
            for job_id, reason in reasons.items():
                state.update_job(
                    conn,
                    job_id,
                    pgid=4242,
                    kill_reason=reason,
                )
            state.insert_control_request(conn, "cancel-escalate")

        dispatcher = self.dispatcher()
        dispatcher._probe_offsets = {}
        dispatcher._job_process_state = mock.Mock(return_value="alive")
        dispatcher._signal_job_result = mock.Mock(
            return_value=dispatcher_module._SIGNAL_UNKNOWN
        )
        dispatcher._process_control_requests()
        dispatcher._check_timeouts()
        dispatcher._check_probes()

        with state.connect() as conn:
            observed = {
                job_id: state.get_job(conn, job_id)["kill_reason"]
                for job_id in reasons
            }
        self.assertEqual(reasons, observed)

    def test_node_restart_launch_marker_cleanup_waits_for_commit(self) -> None:
        self.seed_jobs([("restart", {"gpu": 0, "cpus": 1}, "running")])
        dispatcher = self.dispatcher()
        dispatcher.host_dir = state.host_dir()
        dispatcher._prev_hb_ts = 1.0
        with state.connect() as conn:
            state.update_job(conn, "restart", pgid=4242)
            job = dict(state.get_job(conn, "restart"))
        marker_path = dispatcher._launch_marker_path(job)
        os.makedirs(os.path.dirname(marker_path), mode=0o700, exist_ok=True)
        with open(marker_path, "w", encoding="utf-8") as stream:
            stream.write("4242 proc:1\n")

        original_connect = state.connect
        with mock.patch(
            "builtins.open",
            mock.mock_open(read_data="1\n"),
        ), mock.patch.object(
            dispatcher_module.time,
            "time",
            return_value=100.0,
        ), mock.patch.object(
            state,
            "connect",
            side_effect=lambda: self.failing_connect(original_connect),
        ):
            with self.assertRaisesRegex(sqlite3.OperationalError, "commit failure"):
                dispatcher._check_node_restart()

        self.assertTrue(os.path.exists(marker_path))

    def test_probe_increment_is_capped_and_still_matches_tail(self) -> None:
        self.seed_jobs([("large-probe", {"gpu": 0, "cpus": 1}, "running")])
        pattern = "TAIL-PROBE-MATCH"
        with state.connect() as conn:
            row = conn.execute(
                "SELECT spec FROM tasks WHERE batch_id='batch' AND id='large-probe'"
            ).fetchone()
            spec = json.loads(row["spec"])
            spec["probes"] = {"fail_on_log": pattern}
            conn.execute(
                "UPDATE tasks SET spec=?"
                " WHERE batch_id='batch' AND id='large-probe'",
                (json.dumps(spec),),
            )
            state.update_job(conn, "large-probe", pgid=4242)

        cap = dispatcher_module.PROBE_READ_MAX_BYTES
        log_path = os.path.join(self.tmp.name, "large-probe.log")
        with open(log_path, "wb") as stream:
            stream.write(b"x" * (cap * 2))
            stream.write(pattern.encode("utf-8"))
        dispatcher = self.dispatcher()
        dispatcher._probe_offsets = {"large-probe": 1}
        dispatcher._job_log_path = mock.Mock(return_value=log_path)
        dispatcher._signal_job_result = mock.Mock(
            return_value=dispatcher_module._SIGNAL_SENT
        )
        read_sizes = []
        original_open = open

        class TrackingReader:
            def __init__(self, stream):
                self.stream = stream

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return self.stream.__exit__(*args)

            def __getattr__(self, name):
                return getattr(self.stream, name)

            def read(self, size=-1):
                read_sizes.append(size)
                return self.stream.read(size)

        def tracked_open(path, mode="r", *args, **kwargs):
            stream = original_open(path, mode, *args, **kwargs)
            if path == log_path and "b" in mode:
                return TrackingReader(stream)
            return stream

        with mock.patch("builtins.open", side_effect=tracked_open):
            dispatcher._check_probes()

        self.assertTrue(read_sizes)
        self.assertTrue(all(0 <= size <= cap for size in read_sizes))
        with state.connect() as conn:
            job = state.get_job(conn, "large-probe")
        self.assertEqual("probe_failed", job["kill_reason"])
        self.assertEqual(
            os.path.getsize(log_path),
            dispatcher._probe_offsets["large-probe"],
        )

    def test_probe_cap_preserves_overlap_and_truncate_semantics(self) -> None:
        self.seed_jobs(
            [
                ("overlap", {"gpu": 0, "cpus": 1}, "running"),
                ("rotated", {"gpu": 0, "cpus": 1}, "running"),
            ]
        )
        patterns = {
            "overlap": "CROSS-BOUNDARY",
            "rotated": "ROTATED-MATCH",
        }
        with state.connect() as conn:
            for job_id, pattern in patterns.items():
                row = conn.execute(
                    "SELECT spec FROM tasks"
                    " WHERE batch_id='batch' AND id=?",
                    (job_id,),
                ).fetchone()
                spec = json.loads(row["spec"])
                spec["probes"] = {"fail_on_log": pattern}
                conn.execute(
                    "UPDATE tasks SET spec=?"
                    " WHERE batch_id='batch' AND id=?",
                    (json.dumps(spec), job_id),
                )
                state.update_job(conn, job_id, pgid=4242)

        paths = {
            job_id: os.path.join(self.tmp.name, f"{job_id}.log")
            for job_id in patterns
        }
        initial = b"prefix-CROSS"
        with open(paths["overlap"], "wb") as stream:
            stream.write(initial)
        with open(paths["overlap"], "ab") as stream:
            stream.write(b"-BOUNDARY")
        with open(paths["rotated"], "wb") as stream:
            stream.write(b"ROTATED-MATCH")

        dispatcher = self.dispatcher()
        dispatcher._probe_offsets = {
            "overlap": len(initial),
            "rotated": 10_000,
        }
        dispatcher._job_log_path = mock.Mock(
            side_effect=lambda job: paths[job["id"]]
        )
        dispatcher._signal_job_result = mock.Mock(
            return_value=dispatcher_module._SIGNAL_SENT
        )
        dispatcher._check_probes()

        with state.connect() as conn:
            reasons = {
                job_id: state.get_job(conn, job_id)["kill_reason"]
                for job_id in patterns
            }
        self.assertEqual(
            {"overlap": "probe_failed", "rotated": "probe_failed"},
            reasons,
        )
        self.assertEqual(
            {job_id: os.path.getsize(path) for job_id, path in paths.items()},
            dispatcher._probe_offsets,
        )


class ReviewSignalDeliveryTests(unittest.TestCase):
    def test_disappeared_process_group_is_not_reported_as_signalled(self) -> None:
        executor = Executor.__new__(Executor)
        executor._procs = {}
        executor._dead_pgroups = set()
        with mock.patch(
            "gsched.executor.os.killpg",
            side_effect=ProcessLookupError,
        ):
            self.assertFalse(executor.kill_pgid(4242, signal.SIGTERM))
if __name__ == "__main__":
    unittest.main()
