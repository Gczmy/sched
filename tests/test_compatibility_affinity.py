"""Synthetic joint lease/CPU claims; not real Slurm or hard-isolation proof."""
import copy
import time
from types import SimpleNamespace

from gsched import cluster_lease, cpu_capacity, cpu_isolation, state
from test_cpu_isolation import CpuIsolationFixture
from test_lease_ancestry import compatible


class CompatibilityAffinityTests(CpuIsolationFixture):
    def setUp(self):
        super().setUp()
        self.cfg.update(cpus_total=120,
                        lease_validation={"membership": "launch_ancestry", "unknown_policy": "pause"})
        initial, current, observed = compatible()
        initial["instance_id"] = self.dispatcher._cluster_lease.origin["instance_id"]
        self.context.clear()
        self.context.update(current)
        self.dispatcher._cluster_lease = SimpleNamespace(
            origin=initial, sample=observed, frozen_binding=None,
            decision=cluster_lease.decide(initial, current, observed), owner={"lease_id": "fixture-lease"})

    def decision(self, *, invalid_latched=False):
        monitor = self.dispatcher._cluster_lease
        monitor.decision = cluster_lease.decide(monitor.origin, self.context, monitor.sample,
                                               invalid_latched=invalid_latched)
        return monitor.decision

    def test_fixed_budget_does_not_expand_verified_original_cpu_pool(self):
        monitor = self.dispatcher._cluster_lease
        capacity = cpu_capacity.resolve(self.cfg, origin=monitor.origin, current=self.context,
                                        slurm=monitor.sample, lease_decision=monitor.decision)
        self.assertEqual(120, capacity["effective_total"])
        self.assertEqual(2, capacity["observed_upper_bound"])
        self.assertIn("fixed_cpu_total_exceeds_observed_capacity", capacity["warnings"])
        first, _, left = self.reserve(1, 1)
        second, _, right = self.reserve(2, 1)
        self.assertFalse(set(left["cpus"]) & set(right["cpus"]))
        self.assertEqual([0, 1], sorted(left["cpus"] + right["cpus"]))
        self.assertFalse(left["hard_isolation"])
        with state.connect() as conn:
            self.assertEqual("cpu_pool_exhausted", cpu_isolation.select(self.dispatcher, conn, 1)["reason"])
            self.assertEqual(2, len(cpu_isolation.claims(conn)))
            self.assertNotEqual(first, second)

    def test_unknown_and_expired_tracking_refuse_claims_with_fixed_budget(self):
        monitor = self.dispatcher._cluster_lease
        original = copy.deepcopy(monitor.sample)
        for change in ("unknown", "expired", "tracking"):
            monitor.sample = copy.deepcopy(original)
            if change == "unknown":
                monitor.sample["known"] = False
            elif change == "expired":
                monitor.sample["observed_at"] = time.time() - cluster_lease.MAX_AGE - 1
            else:
                monitor.sample["launch_tracking"] = {"known": False}
            self.assertFalse(self.decision()["dispatch_allowed"])
            with state.connect() as conn:
                self.assertFalse(cpu_isolation.select(self.dispatcher, conn, 1)["allowed"])
                self.assertEqual({}, cpu_isolation.claims(conn))
        monitor.sample = original
        self.assertTrue(self.decision()["dispatch_allowed"])

    def test_invalid_or_mask_drift_keeps_original_claim_and_never_migrates(self):
        monitor = self.dispatcher._cluster_lease
        _, identifier, binding = self.reserve(1, 1)
        original = copy.deepcopy(monitor.origin)
        with state.connect() as conn:
            claims = cpu_isolation.claims(conn)
        self.context["affinity"] = [0]
        self.assertFalse(self.decision()["dispatch_allowed"])
        with state.connect() as conn:
            self.assertFalse(cpu_isolation.select(self.dispatcher, conn, 1)["allowed"])
            self.assertEqual(claims, cpu_isolation.claims(conn))
        self.context["affinity"] = [0, 1]
        self.assertFalse(self.decision(invalid_latched=True)["dispatch_allowed"])
        with state.connect() as conn:
            self.assertFalse(cpu_isolation.select(self.dispatcher, conn, 1)["allowed"])
            self.assertEqual(claims, cpu_isolation.claims(conn))
            self.assertEqual(binding, cpu_isolation.allocation_binding(conn, identifier,
                             next(iter(claims))[1])[1])
        self.assertEqual(original, monitor.origin)
