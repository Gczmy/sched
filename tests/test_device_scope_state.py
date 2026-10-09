"""Synthetic original device ledger/SQLite guards, no BPF or worker effects."""
import argparse
from dataclasses import asdict
import json
import sqlite3
from types import SimpleNamespace
import unittest
from unittest import mock

from gsched import allocation, cli, cpu_isolation, cpu_scope_controller, cpu_scope_state as cpu, device_scope_state as ledger, state
from gsched.execution import CpuScopeBinding, CpuScopeIntent, DeviceBinding, DeviceIntent, DevicePolicy, DeviceRule
from gsched.execution.scopes import ScopeParent
from gsched.execution_policy import digest
import test_cpu_isolation as affinity


class DeviceScopeStateTests(affinity.CpuIsolationFixture):
    def setUp(self):
        super().setUp()
        self.cfg["cpu_isolation"] = {"mode": "cgroup", "delegated_root": "/sys/fs/cgroup/fixture/scopes"}
        self.parent = ScopeParent(self.cfg["cpu_isolation"]["delegated_root"], 1, 10, "a" * 36, "mnt:[123]", 1000)
        authority = cpu_scope_controller.authority_from_facts(self.parent.path,
            self.dispatcher._cluster_lease.origin, self.context, self.dispatcher._cluster_lease.decision,
            [{"root": "/", "mountpoint": "/sys/fs/cgroup"}])
        self.dispatcher._cpu_scopes = SimpleNamespace(admission_current=lambda: True, cpus=(0, 1, 2, 3),
            binding_fields=lambda: {"scope_parent": asdict(self.parent), "scope_mems": [0],
                "scope_authority": authority, "scope_pool": [0, 1, 2, 3]})

    def configured(self, number=1):
        job, spec = self.job(number, 1)
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            selected = cpu_isolation.select(self.dispatcher, conn, 1, job_id=job)
            state.update_job(conn, job, status="running", pgid=None)
            identifier = allocation.reserve(conn, job, spec, self.dispatcher, cpu_binding=selected["binding"])
            origin, _ = cpu_isolation.allocation_binding(conn, identifier, job)
            intent = CpuScopeIntent(self.parent, f"{number:032x}", tuple(selected["binding"]["cpus"]), (0,), digest(origin))
            event = cpu.reserve(conn, job, intent)
            event = cpu.advance(conn, intent.scope_id, event["event_id"], "create_intent")
            binding = CpuScopeBinding(intent, 1, 20 + number)
            event = cpu.advance(conn, intent.scope_id, event["event_id"], "inode_bound", {"binding": binding.to_dict()})
            event = cpu.advance(conn, intent.scope_id, event["event_id"], "configure_intent")
            cpu.advance(conn, intent.scope_id, event["event_id"], "configured", {"observation": self.cpu_observation(intent)})
        return job, identifier, DeviceIntent(binding, DevicePolicy((DeviceRule("char", 1, 3, 6),)))

    @staticmethod
    def cpu_observation(intent):
        return {"scope_configured": True, "populated": False, "direct_process_count": 0,
            "effective_cpus": list(intent.cpus), "effective_mems": list(intent.mems),
            "admission_granted": False, "wait_authority_granted": False, "device_isolation": "not_configured"}

    def reserved(self, number=1):
        job, identifier, intent = self.configured(number)
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            event = ledger.reserve(conn, job, intent)
        return job, identifier, intent, event

    def step(self, intent, event, kind, data=None):
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return ledger.advance(conn, intent.scope.intent.scope_id, event["event_id"], kind, data)

    @staticmethod
    def installed_data(intent, *, number=71):
        binding = DeviceBinding(intent, number, "d" * 16)
        return {"binding": binding.to_dict(), "observation": {"device_attachment_verified": True,
            "program_id": number, "admission_granted": False, "wait_authority_granted": False}}

    def removed(self, intent, job):
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cpu.request_cleanup(conn, state.get_job(conn, job), cleanup_source="configured_not_started")
            _, events = cpu.load(conn, intent.scope.intent.scope_id)
            event = cpu.advance(conn, intent.scope.intent.scope_id, events[-1]["event_id"], "cleanup_intent", {
                "observation": self.cpu_observation(intent.scope.intent), "cleanup_source": "configured_not_started"})
            return cpu.advance(conn, intent.scope.intent.scope_id, event["event_id"], "removed", {
                "scope_removed": True, "wait_authority_granted": False})

    def test_reserved_record_freezes_original_allocation_scope_and_policy(self):
        job, identifier, intent, event = self.reserved()
        with state.connect() as conn:
            value, events = ledger.load(conn, intent.scope.intent.scope_id)
            self.assertEqual([event], events)
            self.assertEqual(intent.to_dict(), value["intent"])
            self.assertEqual(identifier, value["allocation_id"])
            self.assertEqual(job, value["job_id"])
            self.assertEqual(1, value["version"])
            self.assertEqual("fixture-lease", value["lease_id"])
            self.assertFalse(ledger.release_allowed(conn, identifier, job))
            self.assertEqual("device_scope_reserved", allocation.query(conn, "batch-1", "task", allocation_id=identifier)["allocations"][0]["events"][-1]["data"]["event"])

    def test_reservation_transaction_rollback_and_no_duplicate_policy_for_allocation(self):
        job, identifier, intent = self.configured()
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ledger.reserve(conn, job, intent)
            conn.rollback()
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM device_scopes").fetchone()[0])
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ledger.reserve(conn, job, intent)
        with state.connect() as conn, self.assertRaises(sqlite3.IntegrityError):
            conn.execute("BEGIN IMMEDIATE")
            ledger.reserve(conn, job, intent)

    def test_original_intent_and_event_history_cannot_be_overwritten_or_deleted(self):
        self.reserved()
        for sql in ("UPDATE device_scopes SET payload='{}'", "DELETE FROM device_scopes",
                    "UPDATE device_scope_events SET payload='{}'", "DELETE FROM device_scope_events"):
            with self.subTest(sql=sql), state.connect() as conn, self.assertRaises(sqlite3.IntegrityError):
                conn.execute(sql)

    def test_reservation_rejects_other_inode_policy_digest_and_started_cpu_scope(self):
        job, _, intent = self.configured()
        altered = DeviceIntent(CpuScopeBinding(intent.scope.intent, 1, 999), intent.policy)
        with state.connect() as conn, self.assertRaises(state.StateError):
            conn.execute("BEGIN IMMEDIATE")
            ledger.reserve(conn, job, altered)
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            _, events = cpu.load(conn, intent.scope.intent.scope_id)
            cpu.advance(conn, intent.scope.intent.scope_id, events[-1]["event_id"], "launch_intent")
        with state.connect() as conn, self.assertRaises(state.StateError):
            conn.execute("BEGIN IMMEDIATE")
            ledger.reserve(conn, job, intent)

    def test_cas_consumes_install_before_effect_and_recovery_never_replays(self):
        _, _, intent, reserved = self.reserved()
        with state.connect() as conn, self.assertRaises(state.StateError):
            ledger.advance(conn, intent.scope.intent.scope_id, reserved["event_id"], "install_intent")
        consumed = self.step(intent, reserved, "install_intent")
        for old in (reserved, consumed):
            with self.assertRaises(state.StateError):
                self.step(intent, old, "install_intent")
        unknown = self.step(intent, consumed, "unknown", {"reason": "attachment_result_unknown"})
        for kind, data in (("install_intent", None), ("installed", self.installed_data(intent)),
                           ("launch_intent", None), ("abandoned", None)):
            with self.subTest(kind=kind), self.assertRaises(state.StateError):
                self.step(intent, unknown, kind, data)

    def test_installation_requires_exact_program_intent_and_false_authority(self):
        _, _, intent, event = self.reserved()
        event = self.step(intent, event, "install_intent")
        original = self.installed_data(intent)
        for patch in ({"program_id": 72}, {"program_id": True}, {"device_attachment_verified": 1},
                      {"device_attachment_verified": False}, {"admission_granted": True}, {"wait_authority_granted": True}):
            changed = {**original, "observation": {**original["observation"], **patch}}
            with self.subTest(patch=patch), self.assertRaises(state.StateError):
                self.step(intent, event, "installed", changed)
        other = DeviceIntent(intent.scope, DevicePolicy(()))
        with self.assertRaises(state.StateError):
            self.step(intent, event, "installed", self.installed_data(other))
        installed = self.step(intent, event, "installed", original)
        with self.assertRaises(state.StateError):
            self.step(intent, installed, "installed", original)

    def test_device_intent_blocks_cpu_launch_until_both_intents_are_committed(self):
        job, identifier, intent, event = self.reserved()
        with state.connect() as conn, self.assertRaises(state.StateError):
            conn.execute("BEGIN IMMEDIATE")
            _, events = cpu.load(conn, intent.scope.intent.scope_id)
            cpu.advance(conn, intent.scope.intent.scope_id, events[-1]["event_id"], "launch_intent")
        event = self.step(intent, event, "install_intent")
        event = self.step(intent, event, "installed", self.installed_data(intent))
        event = self.step(intent, event, "launch_intent")
        with self.assertRaises(state.StateError):
            self.step(intent, event, "launch_intent")
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            _, events = cpu.load(conn, intent.scope.intent.scope_id)
            cpu.advance(conn, intent.scope.intent.scope_id, events[-1]["event_id"], "launch_intent")
        with state.connect() as conn:
            self.assertFalse(ledger.release_allowed(conn, identifier, job))

    def test_current_controller_cannot_substitute_cpu_handle_for_recorded_device_policy(self):
        job, _, intent, event = self.reserved()
        event = self.step(intent, event, "install_intent")
        event = self.step(intent, event, "installed", self.installed_data(intent))
        self.step(intent, event, "launch_intent")
        with state.connect() as conn, mock.patch("gsched.execution.devices._native", side_effect=AssertionError("BPF effect")):
            with self.assertRaises(state.StateError):
                cpu_scope_controller.Controller.launch_check(self.dispatcher._cpu_scopes, conn, state.get_job(conn, job))

    def test_cancel_or_nonlatest_generation_refuses_new_device_effect(self):
        job, _, intent, event = self.reserved()
        with state.connect() as conn:
            state.update_job(conn, job, kill_reason="cancelled")
        with self.assertRaises(state.StateError):
            self.step(intent, event, "install_intent")
        with state.connect() as conn:
            state.update_job(conn, job, status="pending", kill_reason=None)
        with self.assertRaises(state.StateError):
            self.step(intent, event, "install_intent")
        with state.connect() as conn:
            state.update_job(conn, job, status="running")
            original = state.get_job(conn, job)
            state.insert_job(conn, job_id="newer-device-generation", batch_id=original["batch_id"],
                task_id=original["task_id"], version=2, fingerprint="fixture-new")
        with self.assertRaises(state.StateError):
            self.step(intent, event, "install_intent")

    def test_installed_readback_after_cancel_retains_unknown_not_launch_permission(self):
        job, _, intent, event = self.reserved()
        consumed = self.step(intent, event, "install_intent")
        with state.connect() as conn:
            state.update_job(conn, job, kill_reason="cancelled")
        with self.assertRaises(state.StateError):
            self.step(intent, consumed, "installed", self.installed_data(intent))
        self.step(intent, consumed, "unknown", {"reason": "cancelled_before_readback_commit"})

    def test_unknown_survives_task_terminal_retry_and_original_claim_release(self):
        job, identifier, intent, event = self.reserved()
        consumed = self.step(intent, event, "install_intent")
        self.step(intent, consumed, "unknown", {"reason": "attachment_result_unknown"})
        with state.connect() as conn:
            original = state.get_job(conn, job)
            state.update_job(conn, job, status="pending")
            cpu_isolation.release(conn, original, cleanup_source="adoption_group_gone")
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM cpu_assignments").fetchone()[0])
            self.assertFalse(ledger.release_allowed(conn, identifier, job))
            self.assertTrue(cpu.unresolved(conn))
        self.dispatcher.cfg = {}
        with state.connect() as conn:
            self.assertFalse(cpu_isolation.select(self.dispatcher, conn, 1)["allowed"])

    def test_empty_or_absent_scope_does_not_release_device_policy(self):
        _, _, intent, event = self.reserved()
        for reference in (event["event_id"], "f" * 64):
            with self.assertRaises(state.StateError):
                self.step(intent, event, "released", {"cpu_removed_event_id": reference,
                    "semantics": "original_scope_removed_not_program_gc_or_wait"})

    def test_real_recorded_original_scope_removal_reconciles_unknown_without_reinstall(self):
        job, identifier, intent, event = self.reserved()
        event = self.step(intent, event, "install_intent")
        self.step(intent, event, "unknown", {"reason": "attachment_result_unknown"})
        removed = self.removed(intent, job)
        with state.connect() as conn:
            self.assertFalse(cpu.release_allowed(conn, identifier, job))
            self.assertTrue(cpu.unresolved(conn))  # CPU removed alone cannot bypass device.
            self.dispatcher.cfg = {}
            self.assertFalse(cpu_isolation.select(self.dispatcher, conn, 1)["allowed"])
            conn.execute("BEGIN IMMEDIATE")
            self.assertTrue(cpu.request_cleanup(conn, state.get_job(conn, job), cleanup_source="configured_not_started"))
            value, events = ledger.load(conn, intent.scope.intent.scope_id)
            self.assertEqual("released", events[-1]["kind"])
            self.assertEqual(removed["event_id"], events[-1]["data"]["cpu_removed_event_id"])
            self.assertFalse(cpu.unresolved(conn))
            cpu_isolation.release(conn, state.get_job(conn, job), cleanup_source="configured_not_started")
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM cpu_assignments").fetchone()[0])
            self.assertEqual("unknown", events[-2]["kind"])

    def test_abandon_only_pristine_reservation_and_no_cpu_fallback(self):
        job, identifier, intent, event = self.reserved()
        terminal = self.step(intent, event, "abandoned")
        with state.connect() as conn:
            self.assertTrue(ledger.release_allowed(conn, identifier, job))
            self.assertFalse(cpu.release_allowed(conn, identifier, job))
            with self.assertRaises(state.StateError):
                ledger.cpu_launch_guard(conn, identifier, job)
        with self.assertRaises(state.StateError):
            self.step(intent, terminal, "install_intent")

    def test_query_is_passive_bounded_and_exact_chain_does_not_grant_health(self):
        _, _, first, _ = self.reserved(1)
        _, _, second, _ = self.reserved(2)
        with state.connect() as conn, mock.patch("gsched.execution.devices._native", side_effect=AssertionError("query BPF probe")):
            before = conn.total_changes
            page = ledger.query(conn, limit=1)
            self.assertTrue(page["truncated"])
            self.assertEqual(first.scope.intent.scope_id, page["next_cursor"])
            rest = ledger.query(conn, cursor=page["next_cursor"])
            self.assertEqual(second.scope.intent.scope_id, rest["scopes"][0]["scope_id"])
            exact = ledger.query(conn, scope_id=first.scope.intent.scope_id)
            self.assertEqual(1, len(exact["scopes"][0]["events"]))
            for key in ("runtime_probed", "admission_granted", "wait_authority_granted", "physical_boundary_verified"):
                self.assertFalse(exact[key])
            self.assertEqual(before, conn.total_changes)
        for kwargs in ({"limit": True}, {"limit": 101}, {"scope_id": "x"}, {"cursor": "a" * 32, "scope_id": "b" * 32}):
            with state.connect() as conn, self.assertRaises((ValueError, state.StateError)):
                ledger.query(conn, **kwargs)

    def test_schema20_is_passive_and_schema21_migration_does_not_backfill_policy(self):
        self.configured()
        retained = ("cpu_scopes", "cpu_scope_events", "cpu_assignments", "allocations", "allocation_events", "scheduler_identity")
        with state.connect() as conn:
            before = {name: [tuple(row) for row in conn.execute("SELECT * FROM " + name)] for name in retained}
            for name in ("device_scope_events", "device_scopes"):
                conn.execute("DROP TABLE " + name)
            conn.execute("PRAGMA user_version=20")
            self.assertTrue(state._schema_is_complete(conn, 20))
        state.set_read_only(True)
        try:
            with state.connect() as conn:
                result = ledger.query(conn)
                self.assertFalse(result["available"])
                self.assertEqual("migration_required", result["reason"])
                self.assertEqual(20, conn.execute("PRAGMA user_version").fetchone()[0])
        finally:
            state.set_read_only(False)
        with mock.patch.object(state, "init_db", side_effect=AssertionError("old CLI query migration")):
            code, out, err = self.capture(cli.main, ["device-scopes", "--json"])
        self.assertEqual((0, ""), (code, err))
        self.assertEqual("migration_required", json.loads(out)["reason"])
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(state.DB_SCHEMA_VERSION, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM device_scopes").fetchone()[0])
            after = {name: [tuple(row) for row in conn.execute("SELECT * FROM " + name)] for name in retained}
            self.assertEqual(before, after)

    def test_oversized_or_corrupt_chain_refused_before_read_and_resource_release(self):
        job, identifier, intent, event = self.reserved()
        with state.connect() as conn:
            conn.execute("DROP TRIGGER device_scope_event_immutable")
            conn.execute("UPDATE device_scope_events SET payload=?", (" " * (ledger.MAX_RECORD + 1),))
            with self.assertRaises(state.StateError):
                ledger.load(conn, intent.scope.intent.scope_id)
            with self.assertRaises(state.StateError):
                ledger.release_allowed(conn, identifier, job)

    def test_linked_evidence_budget_is_checked_before_loading_payloads(self):
        _, _, intent, _ = self.reserved()
        with state.connect() as conn, mock.patch.object(ledger, "MAX_EVIDENCE", 1), \
                mock.patch.object(cpu, "load", side_effect=AssertionError("oversized linked read")):
            with self.assertRaises(state.StateError):
                ledger.query(conn, scope_id=intent.scope.intent.scope_id)
            with self.assertRaises(state.StateError):
                ledger.load(conn, intent.scope.intent.scope_id)

    def test_cli_read_uses_private_snapshot_and_never_initializes_or_probes(self):
        _, _, intent, _ = self.reserved()
        args = argparse.Namespace(json=True, scope_id=intent.scope.intent.scope_id, limit=20, cursor=None)
        with mock.patch.object(cli, "load_config", return_value=self.cfg), \
                mock.patch("gsched.execution.devices._native", side_effect=AssertionError("CLI BPF probe")), \
                mock.patch.object(state, "init_db", side_effect=AssertionError("query migration")), \
                mock.patch("builtins.print") as printed:
            self.assertEqual(0, cli.cmd_device_scopes(args))
        value = json.loads(printed.call_args.args[0])
        self.assertEqual("sched-device-scope-state-v1", value["contract"])
        self.assertEqual("none", value["effect"])

    def test_main_query_and_capability_named_contract_keep_default_shapes(self):
        with mock.patch.object(state, "init_db", side_effect=AssertionError("query migration")):
            code, out, err = self.capture(cli.main, ["device-scopes", "--json"])
        self.assertEqual((0, ""), (code, err))
        value = json.loads(out)
        self.assertEqual([], value["scopes"])
        self.assertEqual("sched-device-scope-state-v1", value["contract"])
        with state.connect() as conn, mock.patch.object(state, "DB_SCHEMA_VERSION", 20), self.assertRaises(state.StateError):
            state._require_supported_schema(conn)
