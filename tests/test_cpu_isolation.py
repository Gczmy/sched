"""Pure and synthetic state CPU claims; never spawn a worker locally."""
import json
import sqlite3
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from gsched import allocation, cli, cpu_isolation, state
from gsched.dispatcher import Dispatcher
from test_review_cli_state import TempStateCase


class CpuIsolationTests(TempStateCase):
    def setUp(self):
        super().setUp()
        self.cfg["cpu_isolation"] = {"mode": "affinity"}
        self.context = {"pid": 123, "start_token": "proc:456", "physical_host": "review-node",
                        "uid": 1000, "cgroups": [{"hierarchy": "0", "controllers": "", "path": "/fixture"}],
                        "affinity": [0, 1, 2, 3], "cpus_allowed_list": "0-3"}
        origin = {**self.context, "slurm_environment": {}, "policy": {"mode": "auto", "unknown_policy": "pause"}}
        from gsched.integration import instance_id
        with state.connect() as conn:
            origin["instance_id"] = instance_id(conn)
        self.dispatcher = Dispatcher.__new__(Dispatcher)
        self.dispatcher.cfg = self.cfg
        self.dispatcher._task_cpus = lambda spec: spec["resources"]["cpus"]
        self.dispatcher._cluster_lease = SimpleNamespace(origin=origin, sample={"known": False, "observed_at": time.time()},
            frozen_binding=None, decision={"invalid_latched": False, "allocation_state": "valid"}, owner={"lease_id": "fixture-lease"})
        self.probes = mock.patch("gsched.cpu_isolation.cluster_lease.kernel_context", return_value=self.context)
        self.probes.start()
        self.addCleanup(self.probes.stop)
        self.platform = mock.patch("gsched.cpu_isolation.sys.platform", "linux")
        self.platform.start()
        self.addCleanup(self.platform.stop)
        self.memfd = mock.patch("gsched.cpu_isolation.os.memfd_create", create=True)
        self.memfd.start()
        self.addCleanup(self.memfd.stop)

    def job(self, number, cpus=1):
        batch = f"batch-{number}"
        identifier = self.seed_batch(batch_id=batch, job_status="pending")
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=?", (batch,)).fetchone()[0])
            spec["resources"]["cpus"] = cpus
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id=?", (json.dumps(spec), batch))
        return identifier, spec

    def reserve(self, number, count):
        job, spec = self.job(number, count)
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            result = cpu_isolation.select(self.dispatcher, conn, count, job_id=job)
            self.assertTrue(result["allowed"], result)
            state.update_job(conn, job, status="running", pgid=None)
            identifier = allocation.reserve(conn, job, spec, self.dispatcher, cpu_binding=result["binding"])
            return job, identifier, result["binding"]

    def test_policy_explicit_and_default_off(self):
        self.assertEqual({"mode": "off"}, cpu_isolation.policy({}))
        for value in (None, True, {"mode": "cgroup"}, {"mode": True}, {"mode": "affinity", "extra": 1}):
            with self.assertRaises(ValueError):
                cpu_isolation.policy({"cpu_isolation": value})
        with state.connect() as conn, mock.patch.object(cpu_isolation.cluster_lease, "kernel_context", side_effect=AssertionError()):
            self.dispatcher.cfg = {}
            self.assertIsNone(cpu_isolation.select(self.dispatcher, conn, 99999))

    def test_two_jobs_disjoint_and_exhaustion_does_not_allocate(self):
        first, _, binding = self.reserve(1, 2)
        self.assertEqual([0, 1], binding["cpus"])
        second, _, other = self.reserve(2, 2)
        self.assertEqual([2, 3], other["cpus"])
        with state.connect() as conn:
            self.assertFalse(cpu_isolation.select(self.dispatcher, conn, 1)["allowed"])
            self.assertEqual(4, conn.execute("SELECT COUNT(*) FROM cpu_assignments").fetchone()[0])

    def test_high_request_waits_while_small_request_can_fill_hole(self):
        self.reserve(1, 3)
        with state.connect() as conn:
            self.assertEqual("cpu_pool_exhausted", cpu_isolation.select(self.dispatcher, conn, 2)["reason"])
            self.assertEqual([3], cpu_isolation.select(self.dispatcher, conn, 1)["binding"]["cpus"])

    def test_release_preserves_history_and_original_allocation(self):
        job, identifier, binding = self.reserve(1, 2)
        with state.connect() as conn:
            original = state.get_job(conn, job)
            state.update_job(conn, job, status="pending")
            cpu_isolation.release(conn, original, cleanup_source="ordinary_group_gone")
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM cpu_assignments").fetchone()[0])
            value = allocation.query(conn, "batch-1", "task", allocation_id=identifier)["allocations"][0]
            self.assertEqual(binding, value["cpu_binding"])
            self.assertEqual("cpu_affinity_released", value["events"][-1]["data"]["event"])
            self.assertFalse(value["events"][-1]["data"]["wait_authority_granted"])
            self.assertEqual([0, 1], cpu_isolation.select(self.dispatcher, conn, 2)["binding"]["cpus"])

    def test_unknown_old_claim_retained_after_status_change_and_no_same_job_replay(self):
        job, _, _ = self.reserve(1, 2)
        with state.connect() as conn:
            state.update_job(conn, job, status="pending")
            result = cpu_isolation.select(self.dispatcher, conn, 1, job_id=job)
            self.assertEqual("prior_cpu_claim_not_released", result["reason"])
            self.assertEqual([2, 3], cpu_isolation.select(self.dispatcher, conn, 2)["binding"]["cpus"])
            self.assertEqual(2, conn.execute("SELECT COUNT(*) FROM cpu_assignments").fetchone()[0])

    def test_legacy_running_unbound_task_pauses_new_affinity(self):
        job, _ = self.job(1)
        with state.connect() as conn:
            state.update_job(conn, job, status="running")
            self.assertEqual("legacy_running_cpu_binding_unknown", cpu_isolation.select(self.dispatcher, conn, 1)["reason"])

    def test_recorded_pool_explanation_shares_claim_selection_without_probes(self):
        self.reserve(1, 4)
        owner = self.dispatcher._cluster_lease.owner
        item = {"origin": self.dispatcher._cluster_lease.origin, "recorded_exit": None,
                "current_owner_binding": True, "observation_age_s": 1,
                "recorded_check": {"data": {**self.dispatcher._cluster_lease.decision,
                    "current_context": self.context, "slurm_observation": self.dispatcher._cluster_lease.sample,
                    "slurm_binding": None}}}
        with state.connect() as conn, mock.patch("gsched.daemon._read_lease_owner", return_value=owner), mock.patch.object(cpu_isolation.cluster_lease, "query", return_value={"leases": [item]}), mock.patch.object(cpu_isolation.cluster_lease, "kernel_context", side_effect=AssertionError("gateway probe")):
            result = cpu_isolation.recorded_selection(conn, self.cfg, 1)
            self.assertFalse(result["allowed"])
            self.assertEqual("cpu_pool_exhausted", result["reason"])
            self.assertFalse(result["runtime_probed"])
            item["observation_age_s"] = 100
            self.assertIsNone(cpu_isolation.recorded_selection(conn, self.cfg, 1)["allowed"])
            item["observation_age_s"] = 1
            item["current_owner_binding"] = False
            self.assertIsNone(cpu_isolation.recorded_selection(conn, self.cfg, 1)["allowed"])

    def test_claim_selection_and_kernel_resolution_are_separate_pure_decisions(self):
        with state.connect() as conn:
            self.assertEqual({"allowed": True, "cpus": [2]}, cpu_isolation.choose(conn, 1, [2, 3]))
            self.assertEqual("cpu_request_exceeds_affinity_bound", cpu_isolation.choose(conn, 8193, [2, 3])["reason"])
            with self.assertRaises(state.StateError):
                cpu_isolation.choose(conn, 1, [2, 2])

    def test_kernel_drift_or_invalid_latch_never_migrates(self):
        self.reserve(1, 1)
        with state.connect() as conn:
            self.dispatcher._cluster_lease.origin = dict(self.dispatcher._cluster_lease.origin, affinity=[0, 1, 2])
            self.assertEqual("original_cpu_kernel_context_changed_or_unknown", cpu_isolation.select(self.dispatcher, conn, 1)["reason"])
            self.dispatcher._cluster_lease.origin["affinity"] = [0, 1, 2, 3]
            self.dispatcher._cluster_lease.decision["invalid_latched"] = True
            self.assertEqual("original_cpu_capacity_unavailable", cpu_isolation.select(self.dispatcher, conn, 1)["reason"])

    def test_missing_claim_never_falls_back_to_unconstrained_launch(self):
        job, identifier, _ = self.reserve(1, 2)
        with state.connect() as conn:
            self.assertEqual((0, 1), cpu_isolation.launch_constraints(conn, state.get_job(conn, job)).cpu_affinity)
            conn.execute("DELETE FROM cpu_assignments WHERE allocation_id=?", (identifier,))
            with self.assertRaises(state.StateError):
                cpu_isolation.launch_constraints(conn, state.get_job(conn, job))

    def test_rollback_cpu_claims_and_uniqueness_are_transactional(self):
        self.reserve(1, 2)
        job, spec = self.job(2, 2)
        with self.assertRaises(sqlite3.IntegrityError), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            state.update_job(conn, job, status="running")
            binding = {"schema_version": 1, "mode": "affinity", "cpus": [0, 1], "hard_isolation": False}
            allocation.reserve(conn, job, spec, self.dispatcher, cpu_binding=binding)
        with state.connect() as conn:
            self.assertEqual("pending", state.get_job(conn, job)["status"])
            self.assertEqual(2, conn.execute("SELECT COUNT(*) FROM cpu_assignments").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM allocations WHERE job_id=?", (job,)).fetchone()[0])

    def test_query_pagination_passive_and_update_forbidden(self):
        self.reserve(1, 1)
        self.reserve(2, 1)
        with state.connect() as conn:
            first = cpu_isolation.query(conn, self.cfg, limit=1)
            second = cpu_isolation.query(conn, self.cfg, limit=1, cursor=first["next_cursor"])
            self.assertTrue(first["truncated"])
            self.assertFalse(second["truncated"])
            self.assertFalse(first["runtime_probed"])
            self.assertFalse(first["hard_isolation"])
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("UPDATE cpu_assignments SET cpu=3 WHERE cpu=0")

    def test_actual_old_schema17_query_does_not_migrate_or_backfill(self):
        with state.connect() as conn:
            conn.execute("DROP TRIGGER cpu_assignment_immutable")
            conn.execute("DROP TABLE cpu_assignments")
            conn.execute("PRAGMA user_version=17")
        result, output, error = self.capture(cli.main, ["cpu-isolation", "--json"])
        self.assertEqual(0, result, error)
        self.assertEqual("migration_required", json.loads(output)["reason"])
        with state.connect() as conn:
            self.assertEqual(17, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertTrue(state._schema_is_complete(conn, 17))
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(18, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM cpu_assignments").fetchone()[0])
