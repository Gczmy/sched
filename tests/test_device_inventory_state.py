"""Frozen device/allocation evidence models; no BPF, hardware or worker effects."""
import copy
from dataclasses import asdict
import json
import sqlite3
from types import SimpleNamespace
from unittest import mock

from gsched import allocation, cli, cpu_isolation, cpu_scope_controller, device_inventory_state as ledger, device_scope_state as devices, state
from gsched.execution import DeviceIntent
from gsched.execution.scopes import ScopeParent
from gsched.execution_policy import digest
import test_cpu_isolation as affinity
import test_device_inventory as mapping
import test_device_scope_state as scope_fixture


class DeviceInventoryStateTests(affinity.CpuIsolationFixture):
    configured = scope_fixture.DeviceScopeStateTests.configured
    cpu_observation = staticmethod(scope_fixture.DeviceScopeStateTests.cpu_observation)
    installed_data = staticmethod(scope_fixture.DeviceScopeStateTests.installed_data)
    removed = scope_fixture.DeviceScopeStateTests.removed

    def setUp(self):
        super().setUp()
        self.cfg["cpu_isolation"] = {"mode": "cgroup", "delegated_root": "/sys/fs/cgroup/fixture/scopes"}
        self.parent = ScopeParent(self.cfg["cpu_isolation"]["delegated_root"], 1, 10,
            mapping.CONTEXT["boot_id"], mapping.CONTEXT["mount_namespace"], 1000)
        authority = cpu_scope_controller.authority_from_facts(self.parent.path,
            self.dispatcher._cluster_lease.origin, self.context, self.dispatcher._cluster_lease.decision,
            [{"root": "/", "mountpoint": "/sys/fs/cgroup"}])
        self.dispatcher._cpu_scopes = SimpleNamespace(admission_current=lambda: True, cpus=(0, 1, 2, 3),
            binding_fields=lambda: {"scope_parent": asdict(self.parent), "scope_mems": [0],
                "scope_authority": authority, "scope_pool": [0, 1, 2, 3]})
        self.clock = mock.patch("gsched.device_inventory_state.time.time", return_value=100)
        self.clock_mock = self.clock.start()
        self.addCleanup(self.clock.stop)

    def original(self, number=1, gpu=False):
        if gpu:
            self.dispatcher.allocator = SimpleNamespace(_uuid_map={mapping.SECOND: 1}, _uuid_observed_at=99)
            original_job = self.job
            def with_gpu(n, cpus):
                job, spec = original_job(n, cpus)
                spec["resources"]["gpu"] = 1
                with state.connect() as conn:
                    conn.execute("UPDATE tasks SET spec=? WHERE batch_id=(SELECT batch_id FROM jobs WHERE id=?)", (json.dumps(spec), job))
                    conn.execute("INSERT INTO gpus(idx,mem_total_gib,status) VALUES(?,?,?)", (1, 24, "free"))
                    conn.execute("INSERT INTO gpu_jobs(gpu_id,job_id,vram_gib) VALUES(?,?,?)", (1, job, 1))
                return job, spec
            self.job = with_gpu
        try:
            job, identifier, placeholder = self.configured(number)
        finally:
            if gpu:
                self.job = original_job
        captured = mapping.captured()
        with state.connect() as conn:
            original, _ = ledger.cpu._allocation(conn, identifier, job)
            intent = DeviceIntent(placeholder.scope,
                ledger.inventory.select_policy(captured, original["gpu_reservations"], now=100))
            conn.execute("BEGIN IMMEDIATE")
            devices.reserve(conn, job, intent)
        return job, identifier, intent, captured

    def frozen(self, number=1, gpu=False):
        job, identifier, intent, captured = self.original(number, gpu)
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            binding = ledger.freeze(conn, intent.scope.intent.scope_id, captured, 99)
        return job, identifier, intent, captured, binding

    def test_cpu_binding_freezes_full_evidence_and_original_identities(self):
        job, identifier, intent, captured, binding = self.frozen()
        with state.connect() as conn:
            self.assertEqual(binding, ledger.load(conn, intent.scope.intent.scope_id))
            self.assertEqual(identifier, binding["allocation_id"])
            self.assertEqual(job, binding["job_id"])
            self.assertEqual(captured, binding["inventory"])
            self.assertEqual(digest(captured), binding["inventory_sha256"])
            self.assertEqual(digest(intent.to_dict()), binding["device_intent_sha256"])
            events = allocation.query(conn, "batch-1", "task", allocation_id=identifier)["allocations"][0]["events"]
            self.assertEqual("device_inventory_frozen", events[-1]["data"]["event"])
            self.assertFalse(events[-1]["data"]["physical_boundary_verified"])
        captured["context"]["dev_inode"] += 1
        with state.connect() as conn:
            self.assertNotEqual(captured, ledger.load(conn, intent.scope.intent.scope_id)["inventory"])

    def test_gpu_binding_uses_original_uuid_and_driver_minor_not_index(self):
        _, _, intent, _, _ = self.frozen(gpu=True)
        self.assertIn((195, 7), {(r.major, r.minor) for r in intent.policy.rules})
        self.assertNotIn((195, 1), {(r.major, r.minor) for r in intent.policy.rules})
        with state.connect() as conn:
            self.assertFalse(ledger.verify_current(conn, intent.scope.intent.scope_id, mapping.captured(), 100)["admission_granted"])

    def test_second_freeze_and_update_delete_are_refused(self):
        _, _, intent, captured, _ = self.frozen()
        with self.assertRaises(sqlite3.IntegrityError), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ledger.freeze(conn, intent.scope.intent.scope_id, captured, 99)
        for sql in ("UPDATE device_inventory_bindings SET payload='{}'", "DELETE FROM device_inventory_bindings"):
            with state.connect() as conn, self.assertRaises(sqlite3.IntegrityError):
                conn.execute(sql)

    def test_freeze_requires_writer_and_fresh_finite_observation(self):
        _, _, intent, captured = self.original()
        with state.connect() as conn, self.assertRaises(state.StateError):
            ledger.freeze(conn, intent.scope.intent.scope_id, captured, 99)
        for timestamp in (True, -1, 94, 101, float("nan"), float("inf"), None):
            with self.assertRaises(state.StateError), state.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                ledger.freeze(conn, intent.scope.intent.scope_id, captured, timestamp)
        with state.connect() as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM device_inventory_bindings").fetchone()[0])

    def test_wrong_policy_context_or_changed_original_gpu_claim_refuses_freeze(self):
        job, _, intent, captured = self.original(gpu=True)
        cases = [copy.deepcopy(captured) for _ in range(3)]
        cases[0]["context"]["boot_id"] = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        cases[1]["context"]["mount_namespace"] = "mnt:[456]"
        cases[2]["nodes"]["nvidia7"]["major"] = 196
        for sample in cases:
            with self.assertRaises((state.StateError, ValueError)), state.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                ledger.freeze(conn, intent.scope.intent.scope_id, sample, 99)
        with state.connect() as conn:
            conn.execute("DELETE FROM gpu_jobs WHERE job_id=?", (job,))
        with self.assertRaises(state.StateError), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ledger.freeze(conn, intent.scope.intent.scope_id, captured, 99)

    def test_install_consumption_or_cancel_prevents_late_binding(self):
        job, _, intent, captured = self.original()
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            _, events = devices.load(conn, intent.scope.intent.scope_id)
            devices.advance(conn, intent.scope.intent.scope_id, events[-1]["event_id"], "install_intent")
        with self.assertRaises(state.StateError), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ledger.freeze(conn, intent.scope.intent.scope_id, captured, 99)
        with state.connect() as conn:
            state.update_job(conn, job, status="cancelled")
        with self.assertRaises(state.StateError), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ledger.freeze(conn, intent.scope.intent.scope_id, captured, 99)

    def test_fresh_check_rejects_context_inode_topology_and_minor_drift(self):
        _, _, intent, captured, _ = self.frozen()
        patches = [("context", "boot_id", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
                   ("context", "dev_inode", 42), ("context", "mount_namespace", "mnt:[456]")]
        samples = []
        for key, field, value in patches:
            sample = copy.deepcopy(captured)
            sample[key][field] = value
            samples.append(sample)
        sample = copy.deepcopy(captured)
        sample["nodes"]["nvidia7"]["inode"] += 1
        samples.append(sample)
        sample = copy.deepcopy(captured)
        sample["cards"][1]["index"] = 2
        samples.append(sample)
        for sample in samples:
            with state.connect() as conn, self.assertRaises(state.StateError):
                ledger.verify_current(conn, intent.scope.intent.scope_id, sample, 100)
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            with self.assertRaises(state.StateError):
                ledger.verify_current(conn, intent.scope.intent.scope_id, captured, 100)

    def test_old_gpu_topology_and_new_sample_cannot_refresh_original_reservation(self):
        _, _, intent, captured, _ = self.frozen(gpu=True)
        self.clock_mock.return_value = 106
        with state.connect() as conn, self.assertRaises(ValueError):
            ledger.verify_current(conn, intent.scope.intent.scope_id, captured, 106)
        # Historical query validates at the original freeze time, not now.
        with state.connect() as conn:
            self.assertEqual(captured, ledger.load(conn, intent.scope.intent.scope_id)["inventory"])

    def test_unknown_and_consumed_launch_cannot_use_mapping_for_new_effect(self):
        _, _, intent, captured, _ = self.frozen()
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            _, events = devices.load(conn, intent.scope.intent.scope_id)
            event = devices.advance(conn, intent.scope.intent.scope_id, events[-1]["event_id"], "install_intent")
            devices.advance(conn, intent.scope.intent.scope_id, event["event_id"], "unknown", {"reason": "test_install_unknown"})
        with state.connect() as conn, self.assertRaises(state.StateError):
            ledger.verify_current(conn, intent.scope.intent.scope_id, captured, 100)
        with state.connect() as conn:
            self.assertEqual(captured, ledger.load(conn, intent.scope.intent.scope_id)["inventory"])

    def test_installed_check_grants_no_authority_and_consumed_launch_is_rejected(self):
        _, _, intent, captured, _ = self.frozen()
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            _, events = devices.load(conn, intent.scope.intent.scope_id)
            event = devices.advance(conn, intent.scope.intent.scope_id, events[-1]["event_id"], "install_intent")
            installed = devices.advance(conn, intent.scope.intent.scope_id, event["event_id"], "installed", self.installed_data(intent))
        with state.connect() as conn:
            result = ledger.verify_current(conn, intent.scope.intent.scope_id, captured, 100)
            self.assertFalse(result["admission_granted"])
            self.assertFalse(result["physical_boundary_verified"])
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            devices.advance(conn, intent.scope.intent.scope_id, installed["event_id"], "launch_intent")
        with state.connect() as conn, self.assertRaises(state.StateError):
            ledger.verify_current(conn, intent.scope.intent.scope_id, captured, 100)

    def test_claim_loss_rejects_current_but_retains_history(self):
        job, _, intent, captured, _ = self.frozen()
        with state.connect() as conn:
            conn.execute("DELETE FROM cpu_assignments WHERE job_id=?", (job,))
            self.assertEqual(captured, ledger.load(conn, intent.scope.intent.scope_id)["inventory"])
            with self.assertRaises(state.StateError):
                ledger.verify_current(conn, intent.scope.intent.scope_id, captured, 100)

    def test_new_generation_cannot_rebind_or_use_original_inventory(self):
        job, _, intent, captured, binding = self.frozen()
        with state.connect() as conn:
            current = state.get_job(conn, job)
            conn.execute("INSERT INTO jobs(id,batch_id,task_id,version,status,allocation_id) VALUES(?,?,?,?,?,NULL)",
                ("future-generation", current["batch_id"], current["task_id"], current["version"] + 1, "pending"))
            self.assertEqual(binding, ledger.load(conn, intent.scope.intent.scope_id))
            with self.assertRaises(state.StateError):
                ledger.verify_current(conn, intent.scope.intent.scope_id, captured, 100)

    def test_cancel_rejects_current_with_original_claims_intact(self):
        job, _, intent, captured, _ = self.frozen()
        with state.connect() as conn:
            state.update_job(conn, job, status="cancelled")
            self.assertEqual(captured, ledger.load(conn, intent.scope.intent.scope_id)["inventory"])
            with self.assertRaises(state.StateError):
                ledger.verify_current(conn, intent.scope.intent.scope_id, captured, 100)

    def test_missing_gpu_reservation_is_not_treated_as_cpu_only(self):
        original_job = self.job
        def declared_gpu_without_claim(n, cpus):
            job, spec = original_job(n, cpus)
            spec["resources"]["gpu"] = 1
            with state.connect() as conn:
                conn.execute("UPDATE tasks SET spec=? WHERE batch_id=(SELECT batch_id FROM jobs WHERE id=?)", (json.dumps(spec), job))
            return job, spec
        self.job = declared_gpu_without_claim
        try:
            job, _, placeholder = self.configured()
        finally:
            self.job = original_job
        captured = mapping.captured()
        intent = DeviceIntent(placeholder.scope, ledger.inventory.select_policy(captured, [], now=100))
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            devices.reserve(conn, job, intent)
        with self.assertRaisesRegex(state.StateError, "cardinality"), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ledger.freeze(conn, intent.scope.intent.scope_id, captured, 99)

    def test_wrong_cpu_policy_and_unknown_gpu_mig_reject_frozen_selection(self):
        job, _, placeholder = self.configured()
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            devices.reserve(conn, job, placeholder)
        with self.assertRaises(state.StateError), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ledger.freeze(conn, placeholder.scope.intent.scope_id, mapping.captured(), 99)
        _, _, intent, captured = self.original(2, gpu=True)
        for card in captured["cards"]:
            card["mig_current"] = card["mig_pending"] = "unknown"
        with self.assertRaises(ValueError), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ledger.freeze(conn, intent.scope.intent.scope_id, captured, 99)

    def test_missing_binding_and_invalid_query_parameters_never_guess_inventory(self):
        _, _, intent, captured = self.original()
        with state.connect() as conn, self.assertRaises(state.StateError):
            ledger.verify_current(conn, intent.scope.intent.scope_id, captured, 100)
        for kwargs in ({"limit": True}, {"limit": 101}, {"scope_id": "bad"},
                       {"scope_id": "a" * 32, "cursor": "b" * 32}):
            with state.connect() as conn, self.assertRaises((ValueError, state.StateError)):
                ledger.query(conn, **kwargs)

    def test_rollback_keeps_no_binding_or_frozen_allocation_event(self):
        _, identifier, intent, captured = self.original()
        with self.assertRaises(RuntimeError), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ledger.freeze(conn, intent.scope.intent.scope_id, captured, 99)
            raise RuntimeError("rollback after original binding")
        with state.connect() as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM device_inventory_bindings").fetchone()[0])
            self.assertNotIn("device_inventory_frozen", [e["data"].get("event") for e in
                allocation.query(conn, "batch-1", "task", allocation_id=identifier)["allocations"][0]["events"]])

    def test_bound_and_corrupt_records_fail_closed_before_payload_read(self):
        _, _, intent, _, _ = self.frozen()
        with state.connect() as conn, mock.patch.object(ledger, "MAX_RECORD", 1), self.assertRaises(state.StateError):
            ledger.load(conn, intent.scope.intent.scope_id)
        with state.connect() as conn:
            conn.execute("DROP TRIGGER device_inventory_immutable")
            conn.execute("UPDATE device_inventory_bindings SET payload='{}'")
            with self.assertRaises(state.StateError):
                ledger.load(conn, intent.scope.intent.scope_id)

    def test_passive_query_pagination_full_evidence_budget_and_cli_no_probe(self):
        _, _, first, _, _ = self.frozen(1)
        _, _, second, _, _ = self.frozen(2)
        with state.connect() as conn, mock.patch.object(ledger.inventory, "capture", side_effect=AssertionError("hardware probe")):
            changes = conn.total_changes
            page = ledger.query(conn, limit=1)
            self.assertTrue(page["truncated"])
            self.assertNotIn("inventory", page["bindings"][0])
            rest = ledger.query(conn, cursor=page["next_cursor"])
            self.assertEqual(second.scope.intent.scope_id, rest["bindings"][0]["scope_id"])
            self.assertIn("inventory", ledger.query(conn, scope_id=first.scope.intent.scope_id)["bindings"][0])
            self.assertEqual(changes, conn.total_changes)
            with mock.patch.object(ledger, "MAX_EVIDENCE", 1), self.assertRaises(state.StateError):
                ledger.query(conn)
        with mock.patch.object(state, "init_db", side_effect=AssertionError("CLI migration")), \
                mock.patch.object(ledger.inventory, "capture", side_effect=AssertionError("CLI hardware probe")):
            code, out, err = self.capture(cli.main, ["device-inventory-bindings", "--scope-id", first.scope.intent.scope_id, "--json"])
        self.assertEqual((0, ""), (code, err))
        result = json.loads(out)
        self.assertEqual("sched-device-inventory-binding-v1", result["contract"])
        for key in ("runtime_probed", "admission_granted", "wait_authority_granted", "physical_boundary_verified"):
            self.assertFalse(result[key])

    def test_schema21_query_no_migration_and_upgrade_preserves_all_original_records(self):
        _, _, intent, _ = self.original()
        tables = ("device_scopes", "device_scope_events", "cpu_scopes", "cpu_scope_events",
                  "cpu_assignments", "allocations", "allocation_events", "scheduler_identity")
        with state.connect() as conn:
            before = {name: [tuple(r) for r in conn.execute("SELECT * FROM " + name)] for name in tables}
            conn.execute("DROP TABLE device_inventory_bindings")
            conn.execute("PRAGMA user_version=21")
            self.assertTrue(state._schema_is_complete(conn, 21))
        with mock.patch.object(state, "init_db", side_effect=AssertionError("old query migration")):
            code, out, err = self.capture(cli.main, ["device-inventory-bindings", "--json"])
        self.assertEqual((0, ""), (code, err))
        self.assertEqual("migration_required", json.loads(out)["reason"])
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(22, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertTrue(state._schema_is_complete(conn, 22))
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM device_inventory_bindings").fetchone()[0])
            self.assertEqual(before, {name: [tuple(r) for r in conn.execute("SELECT * FROM " + name)] for name in tables})
            self.assertEqual(intent.to_dict(), devices.load(conn, intent.scope.intent.scope_id)[0]["intent"])
        with state.connect() as conn, mock.patch.object(state, "DB_SCHEMA_VERSION", 21), self.assertRaises(state.StateError):
            state._require_supported_schema(conn)
