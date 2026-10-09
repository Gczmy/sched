"""Synthetic original-root observations; never query a real cgroup/BPF here."""
import copy
import json
import time
from dataclasses import asdict
from unittest import mock

from gsched import cli, cluster_lease, cpu_isolation, scope_health as health, state
from gsched.execution_policy import digest
import test_cpu_scope_controller as scopes
import test_device_scope_controller as devices
from test_review_cli_state import TempStateCase


class RootHealthFixture(scopes.CpuScopeFixture):
    def setUp(self):
        super().setUp()
        self.owner = self.dispatcher._cluster_lease.owner
        for target, kwargs in (("gsched.daemon._read_lease_owner", {"return_value": self.owner}),
                              ("gsched.daemon.health_snapshot", {"return_value": {"process_state": "unknown", "query_host": "gateway"}})):
            patch = mock.patch(target, **kwargs)
            patch.start()
            self.addCleanup(patch.stop)

    def read(self, cfg=None):
        with state.connect() as conn:
            return health.query(conn, self.cfg if cfg is None else cfg)

    def facts(self):
        return self.read()["recorded_origin"]["data"]["facts"]


class RootHealthTests(RootHealthFixture):
    def test_exact_original_query_is_passive_and_cannot_grant_capabilities(self):
        with mock.patch.object(cluster_lease, "kernel_context", side_effect=AssertionError("query kernel probe")), \
                mock.patch.object(cluster_lease, "probe", side_effect=AssertionError("query Slurm probe")), \
                mock.patch.object(self.controller.manager, "_verify", side_effect=AssertionError("query root probe")):
            value = self.read()
        self.assertTrue(value["available"])
        self.assertEqual("ready", value["status"])
        self.assertEqual(asdict(self.parent), value["recorded_origin"]["data"]["facts"]["parent"])
        self.assertEqual(self.owner["lease_id"], value["lease_id"])
        for key in ("runtime_probed", "admission_granted", "wait_authority_granted", "physical_boundary_verified"):
            self.assertFalse(value[key])

    def test_shared_claim_decision_explains_fit_without_reserving_or_joining(self):
        self.allocated(count=3)
        with state.connect() as conn, mock.patch.object(cluster_lease, "kernel_context", side_effect=AssertionError("probe")):
            before = conn.total_changes
            one = cpu_isolation.recorded_selection(conn, self.cfg, 1)
            two = cpu_isolation.recorded_selection(conn, self.cfg, 2)
            self.assertTrue(one["allowed"])
            self.assertEqual([3], one["cpus"])
            self.assertFalse(two["allowed"])
            self.assertEqual("cpu_pool_exhausted", two["reason"])
            self.assertFalse(one["admission_granted"])
            self.assertEqual(before, conn.total_changes)

    def test_stale_future_and_replaced_owner_observations_are_unknown(self):
        observed_at = self.read()["recorded_check"]["data"]["observed_at"]
        for now in (observed_at + health.MAX_AGE + 1, observed_at - 1):
            with mock.patch.object(health.time, "time", return_value=now):
                value = self.read()
                self.assertFalse(value["available"])
                self.assertEqual("root_observation_missing_or_stale", value["reason"])
        with mock.patch("gsched.daemon._read_lease_owner", side_effect=[self.owner, self.owner, self.owner, {**self.owner, "lease_id": "f" * 32}]):
            self.assertFalse(self.read()["available"])

    def test_recorded_exit_invalid_lease_and_missing_owner_are_not_root_readiness(self):
        with mock.patch("gsched.daemon._read_lease_owner", return_value=None):
            self.assertEqual("daemon_owner_missing", self.read()["reason"])
        with state.connect() as conn:
            cluster_lease.event(conn, self.owner["lease_id"], "check", {"allocation_state": "invalid"})
        self.assertEqual("original_lease_invalid", self.read()["reason"])
        with state.connect() as conn:
            cluster_lease.event(conn, self.owner["lease_id"], "exit", {"reason": "fixture_exit"})
        self.assertEqual("daemon_owner_not_current", self.read()["reason"])
        with self.assertRaises(state.StateError):
            health.record(self.controller, facts=self.facts(), observed_at=time.time())

    def test_cold_policy_and_disabled_mode_never_rebind_the_original(self):
        cfg = copy.deepcopy(self.cfg)
        cfg["cpu_isolation"]["delegated_root"] = "/sys/fs/cgroup/other"
        self.assertEqual("cold_policy_changed", self.read(cfg)["reason"])
        self.cfg["cpu_isolation"] = {"mode": "off"}
        self.assertFalse(self.controller.refresh_health())
        self.assertIsNone(self.controller.sampled_at)
        self.assertEqual("cold_policy_changed", self.read()["reason"])
        self.cfg["cpu_isolation"] = self.controller.policy
        self.assertTrue(self.controller.refresh_health())
        self.assertEqual("ready", self.read()["status"])

    def test_origin_is_immutable_unknown_never_refreshes_or_replaces_it(self):
        original = self.read()["recorded_origin"]
        self.controller.manager._verify = mock.Mock(side_effect=OSError("root unreadable"))
        self.assertFalse(self.controller.refresh_health())
        value = self.read()
        self.assertEqual("unknown", value["status"])
        self.assertEqual("root_preflight_unavailable", value["reason"])
        self.assertEqual(original, value["recorded_origin"])
        self.assertEqual([], self.calls)

    def test_capacity_or_kernel_drift_refuses_new_launch_and_records_unknown(self):
        self.root_cpus = (1, 2, 3)
        self.assertFalse(self.controller.refresh_health())
        self.assertEqual("unknown", self.read()["status"])
        self.assertFalse(self.controller.admission_current())
        self.assertEqual([0, 1, 2, 3], self.facts()["cpus"])

    def test_sampling_change_in_bracket_never_records_ready_or_modifies_parent(self):
        self.controller.manager._verify = mock.Mock(side_effect=[(self.root_cpus, (0,)), ((1, 2, 3), (0,))])
        self.assertFalse(self.controller.refresh_health())
        self.assertEqual("unknown", self.read()["status"])
        self.assertFalse(self.controller.admission_current())
        self.assertEqual([], self.calls)

    def test_unreadable_config_and_forged_hierarchy_never_reuse_ready_health(self):
        self.dispatcher._read_gpu_policy = mock.Mock(side_effect=RuntimeError("configuration unavailable"))
        self.assertFalse(self.controller.refresh_health())
        self.assertEqual("unknown", self.read()["status"])
        facts = copy.deepcopy(self.facts())
        facts["authority"]["parent_cgroup"] = "/another"
        with self.assertRaises(state.StateError):
            health.record(self.controller, facts=facts, observed_at=time.time())

    def test_clock_moves_while_acquiring_writer_cannot_publish_ready(self):
        now, facts = time.time(), self.facts()
        with mock.patch.object(health.time, "time", side_effect=[now, now + 6]), self.assertRaises(state.StateError):
            health.record(self.controller, facts=facts, observed_at=now)

    def test_durable_publication_does_not_reset_original_launch_freshness(self):
        with mock.patch("gsched.cpu_scope_controller.time.monotonic", side_effect=[0, 0, 6]):
            with self.assertRaises(state.StateError):
                self.controller.preflight()
        self.assertFalse(self.controller.admission_current())
        self.assertEqual("unknown", self.read()["status"])

    def test_unresolved_scope_status_does_not_explain_positive_fit(self):
        self.controller.healthy = False
        self.assertFalse(self.controller.refresh_health())
        self.assertEqual("unavailable", self.read()["status"])
        with state.connect() as conn:
            fit = health.fit(conn, self.cfg, 1)
        self.assertIsNone(fit["allowed"])
        self.assertEqual("active_scopes_unresolved", fit["reason"])

    def test_old_marker_refuses_new_evidence_before_writing(self):
        facts = self.facts()
        with state.connect() as conn:
            conn.execute("PRAGMA user_version=24")  # Synthetic fixture only.
            before = [tuple(r) for r in conn.execute("SELECT * FROM daemon_lease_events ORDER BY seq")]
            self.assertEqual("migration_required", health.query(conn, self.cfg)["reason"])
            conn.commit()
            with self.assertRaises(state.StateError):
                health.record(self.controller, facts=facts, observed_at=time.time())
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(before, [tuple(r) for r in conn.execute("SELECT * FROM daemon_lease_events ORDER BY seq")])
            self.assertEqual(state.DB_SCHEMA_VERSION, conn.execute("PRAGMA user_version").fetchone()[0])

    def test_corrupt_original_digest_or_check_binding_is_not_a_health_result(self):
        row = self.read()["recorded_check"]
        corrupt = {**row["data"], "origin_sha256": "f" * 64}
        with state.connect() as conn:
            cluster_lease.event(conn, self.owner["lease_id"], "scope_check", corrupt)
        with self.assertRaises(state.StateError):
            self.read()

    def test_new_daemon_without_root_observation_does_not_inherit_previous_ready(self):
        original = self.read()["recorded_origin"]
        self.register_lease()
        replacement = self.dispatcher._cluster_lease.owner
        with mock.patch("gsched.daemon._read_lease_owner", return_value=replacement):
            value = self.read()
        self.assertFalse(value["available"])
        self.assertIsNone(value["recorded_origin"])
        with state.connect() as conn:
            row = conn.execute("SELECT * FROM daemon_lease_events WHERE lease_id=? AND kind='scope_origin'", (self.owner["lease_id"],)).fetchone()
            self.assertEqual(original, cluster_lease.decode(row))

    def test_bound_writer_unpersisted_origin_and_expired_probe_refuse_record(self):
        facts = self.facts()
        token = state._bound_connection.set(object())
        try:
            with self.assertRaises(state.StateError):
                health.record(self.controller, facts=facts, observed_at=time.time())
        finally:
            state._bound_connection.reset(token)
        for observed in (True, float("nan"), time.time() - 6, time.time() + 10):
            with self.assertRaises(state.StateError):
                health.record(self.controller, facts=facts, observed_at=observed)
        self.dispatcher._cluster_lease.origin["uid"] = 999
        with self.assertRaises(state.StateError):
            health.record(self.controller, facts=facts, observed_at=time.time())

    def test_cli_private_snapshot_does_not_change_revisions_or_initialize(self):
        self.job(1)
        with state.connect() as conn:
            before = conn.execute("SELECT revision FROM batches").fetchone()[0]
        with mock.patch.object(cli, "load_config", return_value=self.cfg), \
                mock.patch.object(state, "init_db", side_effect=AssertionError("migration")), \
                mock.patch.object(cluster_lease, "kernel_context", side_effect=AssertionError("query-host probe")):
            rc, stdout, stderr = self.capture(cli.main, ["scope-health", "--json"])
        self.assertEqual(0, rc, stderr)
        report = json.loads(stdout)
        self.assertEqual("sched-scope-health-state-v1", report["contract"])
        self.assertEqual("ready", report["status"])
        self.assertEqual("none", report["effect"])
        self.assertFalse(report["physical_boundary_verified"])
        with state.connect() as conn:
            self.assertEqual(before, conn.execute("SELECT revision FROM batches").fetchone()[0])


class DeviceRootHealthTests(devices.DeviceControllerFixture):
    # Only new tests, not duplicated inherited controller cases.
    def test_parent_bpf_changed_or_unknown_does_not_reinstall_or_grant_launch(self):
        with mock.patch("gsched.device_scope_controller.preflight", return_value={"program_ids": [8], "attach_flags": 2}):
            self.assertFalse(self.controller.refresh_health())
        self.assertFalse(self.controller.admission_current())
        with state.connect() as conn:
            latest = conn.execute("SELECT * FROM daemon_lease_events WHERE lease_id=? AND kind='scope_check' ORDER BY seq DESC LIMIT 1", (self.dispatcher._cluster_lease.owner["lease_id"],)).fetchone()
            self.assertEqual("invalid", cluster_lease.decode(latest)["data"]["status"])
        with mock.patch("gsched.device_scope_controller.preflight", side_effect=OSError("BPF query denied")):
            self.assertFalse(self.controller.refresh_health())
        self.assertEqual([], self.installed)
        self.assertEqual({}, self.entries)

    def test_bpf_query_bracket_change_is_unknown_and_not_a_new_original(self):
        with mock.patch("gsched.device_scope_controller.preflight", side_effect=[{"program_ids": [], "attach_flags": 0}, {"program_ids": [8], "attach_flags": 2}]):
            self.assertFalse(self.controller.refresh_health())
        with state.connect() as conn:
            row = conn.execute("SELECT * FROM daemon_lease_events WHERE lease_id=? AND kind='scope_check' ORDER BY seq DESC LIMIT 1", (self.dispatcher._cluster_lease.owner["lease_id"],)).fetchone()
            self.assertEqual("unknown", cluster_lease.decode(row)["data"]["status"])
        self.assertEqual([], self.installed)


class RootHealthMigrationTests(TempStateCase):
    def test_real_old_layout_has_no_root_backfill_and_retains_old_identity_jobs(self):
        self.seed_batch()
        with state.connect() as conn:
            conn.execute("PRAGMA user_version=24")  # Complete old schema, no root events.
            before = {table: [tuple(r) for r in conn.execute("SELECT * FROM " + table)] for table in ("scheduler_identity", "batches", "tasks", "jobs", "daemon_leases", "daemon_lease_events")}
            self.assertEqual("migration_required", health.query(conn, self.cfg)["reason"])
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(before, {table: [tuple(r) for r in conn.execute("SELECT * FROM " + table)] for table in before})
            self.assertEqual(0, conn.execute("SELECT count(*) FROM daemon_lease_events WHERE kind IN ('scope_origin','scope_check')").fetchone()[0])
