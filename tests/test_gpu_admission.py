from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import time
from unittest import mock

from gsched import config, gpu_admission, recovery, recovery_state, recovery_watch, state, notify
from gsched.allocator import Allocator
from gsched.dispatcher import Dispatcher
from test_review_cli_state import TempStateCase
import test_recovery_queue as queue_fixture


class GpuAdmissionTests(TempStateCase):
    def setUp(self):
        super().setUp()
        self.fake = mock.patch.dict(os.environ, {"SCHED_FAKE_GPUS": "0:64,1:24", "SCHED_FAKE_FREE_GIB": '{"0": 40, "1": 12}', "SCHED_FAKE_COMPUTE_APPS": ""})
        self.fake.start()
        self.addCleanup(self.fake.stop)
        self.cfg.update(gpus=[0, 1], co_locate=True, co_locate_safety=0.7, co_locate_max_jobs=4)
        self.cfg["projects"]["p"].update(gpu_quota=0, gpu_admission={})
        Path(self.config_path).write_text(json.dumps(self.cfg))
        self.dispatcher = Dispatcher(self.cfg, fake=True)
        self.addCleanup(self.dispatcher.log.close)
        with state.connect() as conn:
            state.init_gpus(conn, self.dispatcher.allocator.gpu_list)
        self.dispatcher.allocator.probe_capacity()

    def job(self, batch="b", *, share=False, peak=None):
        job_id = self.seed_batch(batch_id=batch, name=batch, job_status="pending")
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=?", (batch,)).fetchone()[0])
            spec["resources"] = {"gpu": 1, "cpus": 1, "gpu_share": share}
            if peak is not None:
                spec["resources"]["vram_gib"] = peak
            spec["max_retry"] = 0
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id=?", (json.dumps(spec), batch))
        return job_id, spec

    def permits(self, job_id, spec, idx=0, sampled=None):
        with state.connect() as conn:
            return gpu_admission.permits(conn, self.cfg, idx, job_id, spec, "p", gpu_admission.sample(self.dispatcher.allocator) if sampled is None else sampled)

    def test_opt_in_defaults_are_fixed_12_gib_on_different_cards(self):
        job, spec = self.job()
        self.assertTrue(self.permits(job, spec, 0))
        self.assertTrue(self.permits(job, spec, 1))
        with mock.patch.dict(os.environ, {"SCHED_FAKE_FREE_GIB": '{"0": 11.99, "1": 12}'}):
            self.assertFalse(self.permits(job, spec, 0))
        self.cfg["projects"]["p"].pop("gpu_admission")
        self.assertIsNone(gpu_admission.policy(self.cfg, "p"))
        self.assertFalse(self.permits(job, spec))

    def test_strict_policy_and_retry_tier_validation(self):
        for value in (None, [], {"allow_external_occupancy": 1}, {"min_free_gib": True}, {"min_free_gib": float("nan")}, {"min_free_gib": 10**400}, {"bad": True}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                gpu_admission.normalize(value)
        for tiers in ([], [11], [24, 12], [True], [float("inf")], [10**400]):
            with self.subTest(tiers=tiers), self.assertRaises(ValueError):
                recovery_state.normalize_policy({"min_free_gib_by_round": tiers})
        self.assertEqual([12], recovery_state.normalize_policy({})["min_free_gib_by_round"])
        self.cfg["projects"]["p"]["gpu_admission"] = {"min_free_gib": 30}
        config._validate(self.cfg, "test")

    def test_known_external_occupancy_requires_explicit_permission(self):
        job, spec = self.job()
        with state.connect() as conn:
            conn.execute("UPDATE gpus SET status='unmanaged' WHERE idx=0")
        with mock.patch.dict(os.environ, {"SCHED_FAKE_COMPUTE_APPS": "0:90001"}):
            self.assertFalse(self.permits(job, spec))
            self.cfg["projects"]["p"]["gpu_admission"]["allow_external_occupancy"] = True
            self.assertTrue(self.permits(job, spec))
            with mock.patch.object(self.dispatcher.allocator, "_pgid_of", return_value=None):
                self.assertFalse(self.permits(job, spec))
        self.assertFalse(self.permits(job, spec))

    def test_unknown_probes_and_topology_drift_fail_closed(self):
        job, spec = self.job()
        allocator = self.dispatcher.allocator
        for method in ("_compute_pids_by_card", "_uuid_to_idx", "_util_opt"):
            with mock.patch.object(allocator, method, return_value=None):
                self.assertFalse(self.permits(job, spec))
        with mock.patch.object(allocator, "_uuid_to_idx", side_effect=[{"a": 0}, {"b": 0}]):
            self.assertFalse(self.permits(job, spec))
        for bad in ('{"0": null}', '{"0": 100}', '{"0": "40"}', '{"0": NaN}', '{}bad'):
            with mock.patch.dict(os.environ, {"SCHED_FAKE_FREE_GIB": bad}):
                self.assertFalse(self.permits(job, spec))
        with mock.patch.object(allocator, "_util_opt", return_value=1):
            self.assertFalse(self.permits(job, spec))

    def test_quarantine_ignore_releasing_and_orphan_assignments_stay_blocked(self):
        job, spec = self.job()
        for patch in ("quarantined=1", "ignore_until='manual'", "status='releasing'", "status='assigned'"):
            with state.connect() as conn:
                conn.execute("UPDATE gpus SET status='free',quarantined=0,ignore_until=NULL WHERE idx=0")
                conn.execute(f"UPDATE gpus SET {patch} WHERE idx=0")
            self.assertFalse(self.permits(job, spec))

    def test_same_tick_outstanding_peak_is_reserved_and_survives_new_dispatcher(self):
        first, first_spec = self.job("a", share=True, peak=30)
        second, second_spec = self.job("b", share=True, peak=12)
        dispatcher = self.dispatcher
        dispatcher._gpu_admission_samples = gpu_admission.sample(dispatcher.allocator)
        with state.connect() as conn:
            self.assertEqual(0, dispatcher._assign_in_tx(conn, first, first_spec, "p"))
            state.update_job(conn, first, status="running", gpu=0, pgid=90000)
        with mock.patch.dict(os.environ, {"SCHED_FAKE_COMPUTE_APPS": "0:90000"}):
            self.assertFalse(self.permits(second, second_spec, 0))  # 40 - reserved 30 < 12
            with mock.patch.dict(os.environ, {"SCHED_FAKE_FREE_GIB": '{"0": 42}'}):
                self.assertTrue(self.permits(second, second_spec, 0))
                with state.connect() as conn:
                    state.update_job(conn, first, status="failed", rc=42)
                self.assertFalse(self.permits(second, second_spec, 0))  # own residue

    def test_exclusive_launch_keeps_legacy_exclusive_sentinel(self):
        job, spec = self.job()
        self.dispatcher._gpu_admission_samples = gpu_admission.sample(self.dispatcher.allocator)
        with state.connect() as conn:
            self.assertEqual(0, self.dispatcher._assign_in_tx(conn, job, spec, "p"))
            self.assertIsNone(conn.execute("SELECT vram_gib FROM gpu_jobs WHERE job_id=?", (job,)).fetchone()[0])
            self.assertTrue(gpu_admission.permits(conn, self.cfg, 0, job, spec, "p", gpu_admission.sample(self.dispatcher.allocator)))

    def test_launch_recheck_before_claim_and_child_keeps_job_pending(self):
        job, spec = self.job()
        dispatcher = self.dispatcher
        dispatcher._gpu_admission_samples = gpu_admission.sample(dispatcher.allocator)
        dispatcher.executor.launch = mock.Mock()
        dispatcher._clean_stale_artifacts = mock.Mock()
        with state.connect() as conn:
            gpu = dispatcher._assign_in_tx(conn, job, spec, "p")
            with mock.patch.dict(os.environ, {"SCHED_FAKE_FREE_GIB": '{"0": 4}'}):
                self.assertFalse(dispatcher._launch_job(conn, state.get_job(conn, job), gpu))
            self.assertEqual("pending", state.get_job(conn, job)["status"])
        dispatcher.executor.launch.assert_not_called()
        dispatcher._clean_stale_artifacts.assert_not_called()

    def test_launch_recheck_uses_hot_policy_and_stale_sample_cannot_admit(self):
        job, spec = self.job()
        sampled = gpu_admission.sample(self.dispatcher.allocator)
        sampled[0]["sampled_at"] -= 10
        self.assertFalse(self.permits(job, spec, sampled=sampled))
        self.dispatcher._gpu_admission_samples = gpu_admission.sample(self.dispatcher.allocator)
        with state.connect() as conn:
            gpu = self.dispatcher._assign_in_tx(conn, job, spec, "p")
            newer = copy.deepcopy(self.cfg)
            newer["projects"]["p"]["gpu_admission"]["min_free_gib"] = 50
            Path(self.config_path).write_text(json.dumps(newer))
            self.assertFalse(self.dispatcher._launch_job(conn, state.get_job(conn, job), gpu))
            self.assertEqual("pending", state.get_job(conn, job)["status"])

    def test_pending_reservation_before_crash_can_be_reused_once_without_double_count(self):
        job, spec = self.job()
        self.dispatcher._gpu_admission_samples = gpu_admission.sample(self.dispatcher.allocator)
        with state.connect() as conn:
            self.assertEqual(0, self.dispatcher._assign_in_tx(conn, job, spec, "p"))
        restarted = Dispatcher(self.cfg, fake=True)
        self.addCleanup(restarted.log.close)
        restarted._gpu_admission_samples = gpu_admission.sample(restarted.allocator)
        with state.connect() as conn:
            self.assertEqual(0, restarted._assign_in_tx(conn, job, spec, "p"))
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM gpu_jobs WHERE job_id=?", (job,)).fetchone()[0])
            self.assertEqual("pending", state.get_job(conn, job)["status"])

    def test_capacity_override_and_explicit_ordinary_floor_are_honored(self):
        job, spec = self.job(peak=30)
        self.assertTrue(self.permits(job, spec))
        with state.connect() as conn:
            conn.execute("UPDATE gpus SET mem_total_gib=24 WHERE idx=0")
        self.assertFalse(self.permits(job, spec))
        spec["resources"].pop("vram_gib")
        self.cfg["projects"]["p"]["gpu_admission"]["min_free_gib"] = 6
        with mock.patch.dict(os.environ, {"SCHED_FAKE_FREE_GIB": '{"0": 8}'}):
            self.assertTrue(self.permits(job, spec))
        spec["resources"].update(gpu_share=True, vram_gib=18)
        self.assertFalse(self.permits(job, spec))  # 18 > safety 0.7 * override 24

    def test_profile_peak_reservation_is_checked_at_selection_and_launch(self):
        job, spec = self.job(share=True, peak=12)
        spec["resources"]["profile_key"] = "peak"
        from gsched.dispatcher import _profile_cache_key
        with state.connect() as conn:
            conn.execute("INSERT INTO profile_cache(profile_key,peak_gib,updated_at) VALUES(?,?,?)", (_profile_cache_key("p", "peak"), 41, state.now()))
        self.assertFalse(self.permits(job, spec))
        with state.connect() as conn:
            conn.execute("UPDATE profile_cache SET peak_gib=30")
        self.assertTrue(self.permits(job, spec))

    def test_actual_cpu_child_runs_on_fake_known_occupied_gpu_without_reclassification(self):
        job, spec = self.job()
        self.cfg["projects"]["p"]["gpu_admission"]["allow_external_occupancy"] = True
        Path(self.config_path).write_text(json.dumps(self.cfg))
        with state.connect() as conn:
            conn.execute("UPDATE gpus SET status='unmanaged' WHERE idx=0")
        with mock.patch.dict(os.environ, {"SCHED_FAKE_COMPUTE_APPS": "0:90001", "SCHED_FAKE_FREE_GIB": '{"0": 40, "1": 0}'}):
            for _ in range(100):
                self.dispatcher._tick()
                with state.connect() as conn:
                    current = dict(state.get_job(conn, job))
                if current["status"] == "done":
                    break
                time.sleep(.02)
            self.assertEqual(("done", 0, 0), (current["status"], current["rc"], current["gpu"]))


class RecoveryTierWatchTests(TempStateCase):
    spec = queue_fixture.RecoveryQueueTests.spec
    task = queue_fixture.RecoveryQueueTests.task
    seed = queue_fixture.RecoveryQueueTests.seed
    store = queue_fixture.RecoveryQueueTests.store
    oom = queue_fixture.RecoveryQueueTests.oom
    latest = queue_fixture.RecoveryQueueTests.latest

    def setUp(self):
        super().setUp()
        self.root = Path(self.tmp.name)
        self.worker = self.root / "worker.py"
        self.worker.write_bytes((Path(__file__).parent / "fixtures" / "recovery_worker.py").read_bytes())
        (self.root / "settings.json").write_text(json.dumps({"total": 5, "oom_at": [2]}))
        self.cfg["projects"]["p"]["root"] = str(self.root)

    def prepare(self, patch=None):
        spec = self.spec("run")
        if patch:
            spec["recovery"]["retry"].update(patch)
            recovery.freeze(spec, spec["_recovery_fingerprint"])
        job = self.seed("formal", spec, "running")
        self.assertTrue(self.oom(job, spec))
        return spec, self.latest()

    def test_tiers_are_round_bound_keep_12_default_and_persist_across_restart(self):
        spec, job = self.prepare({"min_free_gib_by_round": [12, 24, 30]})
        configured = gpu_admission.normalize({})
        with state.connect() as conn:
            self.assertEqual(24, gpu_admission.minimum(conn, job["id"], spec, configured))
        self.oom(job, dict(spec, _force_rerun=True))
        with state.connect() as conn:
            self.assertEqual(30, gpu_admission.minimum(conn, self.latest()["id"], spec, configured))

    def test_no_progress_notice_is_durable_throttled_and_optional_stop_preserves_old_fact(self):
        spec, job = self.prepare({"no_progress_sec": 10, "max_no_progress_sec": 20})
        self.cfg["notify"] = {"on": ["recovery_no_progress"], "file": {"enabled": True}}
        with state.connect() as conn:
            root = recovery_state.public(conn, job["id"])["queue"]["root_job_id"]
            at = recovery_watch.public(conn, job["id"], recovery_state.public(conn, job["id"])["queue"])["watch"]["last_progress_at"]
            recovery_watch.inspect(conn, state.host_dir(), job, spec, self.cfg, timestamp=at+11)
            recovery_watch.inspect(conn, state.host_dir(), job, spec, self.cfg, timestamp=at+12)
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM recovery_notices").fetchone()[0])
            self.assertEqual("pending", state.get_job(conn, job["id"])["status"])
        recovery_watch.deliver(self.cfg, lambda _: None)
        self.assertEqual(1, len(notify.list_inbox()))
        with state.connect() as conn:
            recovery_watch.inspect(conn, state.host_dir(), state.get_job(conn, job["id"]), spec, self.cfg, timestamp=at+21)
            self.assertEqual("blocked", state.get_job(conn, job["id"])["status"])
            self.assertEqual(("failed", 42), tuple(conn.execute("SELECT status,rc FROM jobs WHERE id=?", (root,)).fetchone()))

    def test_same_progress_across_repeated_oom_does_not_reset_timer_but_new_progress_does(self):
        spec, job = self.prepare()
        with state.connect() as conn:
            original = recovery_state.public(conn, job["id"])["watch"]["last_progress_at"]
        self.oom(job, dict(spec, _force_rerun=True))
        latest = self.latest()
        with state.connect() as conn:
            self.assertEqual(original, recovery_state.public(conn, latest["id"])["watch"]["last_progress_at"])
            self.store(latest, spec).save({"next": 4})
            recovery_watch.inspect(conn, state.host_dir(), latest, spec, self.cfg, timestamp=original+50)
            self.assertEqual(original+50, recovery_state.public(conn, latest["id"])["watch"]["last_progress_at"])

    def test_default_unlimited_does_not_stop_and_corrupt_or_missing_checkpoint_never_counts_progress(self):
        spec, job = self.prepare()
        context = json.loads(recovery.context(state.host_dir(), job, spec, create=False))
        checkpoint = Path(context["checkpoint_dir"]) / "checkpoint.json"
        with state.connect() as conn:
            at = recovery_state.public(conn, job["id"])["watch"]["last_progress_at"]
            checkpoint.write_text("corrupt")
            recovery_watch.inspect(conn, state.host_dir(), job, spec, self.cfg, timestamp=at+100000)
            self.assertEqual(at, recovery_state.public(conn, job["id"])["watch"]["last_progress_at"])
            checkpoint.unlink()
            recovery_watch.inspect(conn, state.host_dir(), job, spec, self.cfg, timestamp=at+200000)
            self.assertEqual(at, recovery_state.public(conn, job["id"])["watch"]["last_progress_at"])
            self.assertEqual("pending", state.get_job(conn, job["id"])["status"])

    def test_schema8_retry_policy_remains_valid_after_upgrade_without_resetting_queue_clock(self):
        spec, job = self.prepare()
        for key in ("min_free_gib_by_round", "no_progress_sec", "max_no_progress_sec"):
            del spec["recovery"]["retry"][key]
        with state.connect() as conn:
            conn.execute("DELETE FROM recovery_watch")
            queue = recovery_state.public(conn, job["id"])["queue"]
            recovery_watch.inspect(conn, state.host_dir(), job, spec, self.cfg, timestamp=queue["first_queued_at"]+4000)
            self.assertEqual(queue["first_queued_at"], recovery_state.public(conn, job["id"])["watch"]["last_progress_at"])
            self.assertEqual("pending", state.get_job(conn, job["id"])["status"])

    def test_schema8_readonly_query_and_atomic_upgrade_to_schema9(self):
        with state.connect() as conn:
            conn.execute("DROP TABLE recovery_notices")
            conn.execute("DROP TABLE recovery_watch")
            conn.execute("PRAGMA user_version=8")
            self.assertTrue(state._schema_is_complete(conn, 8))
            self.assertEqual({"watch": None}, recovery_watch.public(conn, "missing", None))
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(9, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertTrue(state._schema_is_complete(conn, 9))
