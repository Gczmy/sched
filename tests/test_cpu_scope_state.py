"""Synthetic private state lifecycle; no daemon, worker or cgroup effects."""
import argparse
import json
import sqlite3
import unittest
from unittest import mock

from gsched import allocation, cli, cpu_isolation, cpu_scope_state as ledger, state
from gsched.execution import CpuScopeBinding, CpuScopeIntent
from gsched.execution.scopes import ScopeParent
from gsched.execution_policy import digest
import test_cpu_isolation as affinity


class CpuScopeStateTests(affinity.CpuIsolationFixture):

    def scope(self, number=1, cpus=1):
        job, spec = self.job(number, cpus)
        parent = ScopeParent("/srv/example/cpu-scopes", 1, 10, "a" * 36, "mnt:[123]", 1000)
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            selection = cpu_isolation.select(self.dispatcher, conn, cpus, job_id=job)
            state.update_job(conn, job, status="running", pgid=None)
            identifier = allocation.reserve(conn, job, spec, self.dispatcher, cpu_binding=selection["binding"])
            value = allocation._allocation(conn.execute("SELECT * FROM allocations WHERE allocation_id=?", (identifier,)).fetchone())
            intent = CpuScopeIntent(parent, f"{number:032x}", tuple(selection["binding"]["cpus"]), (0,), digest(value))
            event = ledger.reserve(conn, job, intent)
        return job, identifier, intent, event

    def advance(self, intent, event, kind, data=None):
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return ledger.advance(conn, intent.scope_id, event["event_id"], kind, data)

    def bound(self, number=1):
        job, identifier, intent, event = self.scope(number)
        event = self.advance(intent, event, "create_intent")
        binding = CpuScopeBinding(intent, 1, 20 + number)
        event = self.advance(intent, event, "inode_bound", {"binding": binding.to_dict()})
        return job, identifier, intent, event, binding

    @staticmethod
    def observation(intent):
        return {"scope_configured": True, "populated": False, "direct_process_count": 0,
                "effective_cpus": list(intent.cpus), "effective_mems": list(intent.mems),
                "admission_granted": False, "wait_authority_granted": False, "device_isolation": "not_configured"}

    def test_reservation_atomic_with_original_allocation_and_claims(self):
        job, identifier, intent, event = self.scope(cpus=2)
        with state.connect() as conn:
            value, events = ledger.load(conn, intent.scope_id)
            self.assertEqual(identifier, value["allocation_id"])
            self.assertEqual(job, value["job_id"])
            self.assertEqual([event], events)
            self.assertFalse(ledger.release_allowed(conn, identifier, job))
            self.assertEqual(2, conn.execute("SELECT COUNT(*) FROM cpu_assignments").fetchone()[0])
            self.assertEqual("cpu_scope_reserved", allocation.query(conn, "batch-1", "task", allocation_id=identifier)["allocations"][0]["events"][-1]["data"]["event"])

    def test_scope_reservation_rollback_does_not_leave_intent_or_event(self):
        job, identifier, intent, event = self.scope()
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ledger.advance(conn, intent.scope_id, event["event_id"], "create_intent")
            conn.rollback()
            self.assertEqual([event], ledger.load(conn, intent.scope_id)[1])
            self.assertFalse(ledger.release_allowed(conn, identifier, job))

    def test_original_binding_cannot_be_changed_or_deleted(self):
        self.scope()
        for sql in ("UPDATE cpu_scopes SET payload='{}'", "DELETE FROM cpu_scopes", "UPDATE cpu_scope_events SET payload='{}'", "DELETE FROM cpu_scope_events"):
            with self.subTest(sql=sql), self.assertRaises(sqlite3.IntegrityError), state.connect() as conn:
                conn.execute(sql)

    def test_duplicate_allocation_and_scope_id_are_not_new_creation_permission(self):
        job, identifier, intent, _ = self.scope()
        with self.assertRaises(sqlite3.IntegrityError), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            ledger.reserve(conn, job, intent)
        with state.connect() as conn:
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM cpu_scopes").fetchone()[0])

    def test_reservation_requires_original_digest_and_cpu_set(self):
        job, identifier, intent, _ = self.scope()
        for changed in (CpuScopeIntent(intent.parent, "b" * 32, intent.cpus, intent.mems, "c" * 64),
                        CpuScopeIntent(intent.parent, "b" * 32, (3,), intent.mems, intent.binding_sha256)):
            with self.subTest(changed=changed), self.assertRaises(state.StateError), state.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                ledger.reserve(conn, job, changed)

    def test_effect_requires_writer_cas_and_is_consumed_before_external_work(self):
        _, _, intent, event = self.scope()
        with self.assertRaises(state.StateError), state.connect() as conn:
            ledger.advance(conn, intent.scope_id, event["event_id"], "create_intent")
        consumed = self.advance(intent, event, "create_intent")
        with self.assertRaises(state.StateError):
            self.advance(intent, event, "create_intent")
        with self.assertRaises(state.StateError):
            self.advance(intent, consumed, "create_intent")
        with state.connect() as conn:
            self.assertIsNone(ledger.query(conn, scope_id=intent.scope_id)["scopes"][0]["original_inode_binding"])

    def test_crash_after_mkdir_before_inode_stays_unknown_not_guessed_by_name(self):
        job, identifier, intent, event = self.scope()
        event = self.advance(intent, event, "create_intent")
        unknown = self.advance(intent, event, "unknown", {"reason": "creation_result_unknown"})
        for kind, data in (("create_intent", None), ("inode_bound", {"binding": CpuScopeBinding(intent, 1, 42).to_dict()}),
                           ("abandoned", None), ("cleanup_intent", {"observation": self.observation(intent), "cleanup_source": "launch_not_started"})):
            with self.subTest(kind=kind), self.assertRaises(state.StateError):
                self.advance(intent, unknown, kind, data)
        with state.connect() as conn:
            original = state.get_job(conn, job)
            state.update_job(conn, job, status="pending")
            cpu_isolation.release(conn, original, cleanup_source="adoption_group_gone")
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM cpu_assignments").fetchone()[0])
            self.assertEqual("prior_cpu_claim_not_released", cpu_isolation.select(self.dispatcher, conn, 1, job_id=job)["reason"])
            self.assertFalse(ledger.release_allowed(conn, identifier, job))

    def test_bound_scope_never_launches_through_affinity_fallback(self):
        job, _, _, _, _ = self.bound()
        with state.connect() as conn, self.assertRaises(state.StateError):
            cpu_isolation.launch_constraints(conn, state.get_job(conn, job))

    def test_inode_must_match_the_original_intent_and_never_migrate(self):
        _, _, intent, event = self.scope()
        event = self.advance(intent, event, "create_intent")
        other = CpuScopeIntent(intent.parent, "c" * 32, intent.cpus, intent.mems, intent.binding_sha256)
        with self.assertRaises(state.StateError):
            self.advance(intent, event, "inode_bound", {"binding": CpuScopeBinding(other, 1, 22).to_dict()})
        event = self.advance(intent, event, "inode_bound", {"binding": CpuScopeBinding(intent, 1, 22).to_dict()})
        with self.assertRaises(state.StateError):
            self.advance(intent, event, "inode_bound", {"binding": CpuScopeBinding(intent, 1, 23).to_dict()})

    def test_configuration_and_launch_intents_are_one_shot_without_wait_authority(self):
        job, identifier, intent, event, _ = self.bound()
        event = self.advance(intent, event, "configure_intent")
        for patch in ({"populated": True}, {"effective_cpus": [3]}, {"scope_configured": False}, {"wait_authority_granted": True}, {"effective_mems": [1]}):
            with self.subTest(patch=patch), self.assertRaises(state.StateError):
                self.advance(intent, event, "configured", {"observation": {**self.observation(intent), **patch}})
        event = self.advance(intent, event, "configured", {"observation": self.observation(intent)})
        event = self.advance(intent, event, "launch_intent")
        with self.assertRaises(state.StateError):
            self.advance(intent, event, "launch_intent")
        with state.connect() as conn:
            self.assertFalse(ledger.release_allowed(conn, identifier, job))
            result = ledger.query(conn, scope_id=intent.scope_id)
            self.assertTrue(result["scopes"][0]["launch_consumed"])
            self.assertFalse(result["wait_authority_granted"])
            self.assertFalse(result["physical_boundary_verified"])

    def test_cancel_or_nonlatest_generation_refuses_new_effect(self):
        job, _, intent, event = self.scope()
        with state.connect() as conn:
            state.update_job(conn, job, kill_reason="cancelled")
        with self.assertRaises(state.StateError):
            self.advance(intent, event, "create_intent")
        with state.connect() as conn:
            state.update_job(conn, job, kill_reason=None, status="pending")
        with self.assertRaises(state.StateError):
            self.advance(intent, event, "create_intent")

    def test_empty_original_removal_only_then_cpu_release_without_wait_fact(self):
        job, identifier, intent, event, _ = self.bound()
        event = self.advance(intent, event, "cleanup_intent", {"observation": self.observation(intent), "cleanup_source": "launch_not_started"})
        with state.connect() as conn:
            cpu_isolation.release(conn, state.get_job(conn, job), cleanup_source="launch_not_started")
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM cpu_assignments").fetchone()[0])
        with self.assertRaises(state.StateError):
            self.advance(intent, event, "removed", {"scope_removed": True, "wait_authority_granted": True})
        event = self.advance(intent, event, "removed", {"scope_removed": True, "wait_authority_granted": False})
        with state.connect() as conn:
            self.assertTrue(ledger.release_allowed(conn, identifier, job))
            cpu_isolation.release(conn, state.get_job(conn, job), cleanup_source="launch_not_started")
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM cpu_assignments").fetchone()[0])
            self.assertTrue(all(e["data"].get("wait_authority_granted") is not True for e in allocation.query(conn, "batch-1", "task", allocation_id=identifier)["allocations"][0]["events"]))
        with self.assertRaises(state.StateError):
            self.advance(intent, event, "unknown", {"reason": "late_observation"})

    def test_busy_scope_or_direct_process_mismatch_retains_claim(self):
        _, _, intent, event, _ = self.bound()
        for patch in ({"populated": True}, {"direct_process_count": 1}, {"direct_process_count": True}, {"admission_granted": True}):
            with self.subTest(patch=patch), self.assertRaises(state.StateError):
                self.advance(intent, event, "cleanup_intent", {"observation": {**self.observation(intent), **patch}, "cleanup_source": "ordinary_group_gone"})

    def test_unknown_configuration_can_only_cleanup_original_inode(self):
        job, identifier, intent, event, _ = self.bound()
        event = self.advance(intent, event, "configure_intent")
        event = self.advance(intent, event, "unknown", {"reason": "configuration_result_unknown"})
        with self.assertRaises(state.StateError):
            self.advance(intent, event, "configure_intent")
        event = self.advance(intent, event, "cleanup_intent", {"observation": self.observation(intent), "cleanup_source": "configured_not_started"})
        event = self.advance(intent, event, "unknown", {"reason": "removal_result_unknown"})
        with self.assertRaises(state.StateError):
            self.advance(intent, event, "cleanup_intent", {"observation": self.observation(intent), "cleanup_source": "configured_not_started"})
        with state.connect() as conn:
            self.assertFalse(ledger.release_allowed(conn, identifier, job))

    def test_unconsumed_reservation_can_be_abandoned_without_filesystem_effect(self):
        job, identifier, intent, event = self.scope()
        event = self.advance(intent, event, "abandoned")
        with state.connect() as conn:
            self.assertTrue(ledger.release_allowed(conn, identifier, job))
        with self.assertRaises(state.StateError):
            self.advance(intent, event, "create_intent")

    def test_cold_off_cannot_bypass_old_scope_or_release_unknown_cpus(self):
        _, _, intent, event = self.scope()
        self.dispatcher.cfg = {**self.cfg, "cpu_isolation": {"mode": "off"}}
        with state.connect() as conn:
            self.assertEqual("unresolved_cpu_scope_retained", cpu_isolation.select(self.dispatcher, conn, 1)["reason"])
        self.advance(intent, event, "abandoned")
        with state.connect() as conn:
            self.assertIsNone(cpu_isolation.select(self.dispatcher, conn, 1))

    def test_cli_private_snapshot_has_no_effect_and_exact_bounded_history(self):
        _, _, intent, _ = self.scope()
        self.scope(2)
        args = argparse.Namespace(scope_id=None, limit=1, cursor=None, json=True)
        with mock.patch.object(ledger, "advance", side_effect=AssertionError("effect")), mock.patch("gsched.execution.scopes._context", side_effect=AssertionError("kernel probe")), mock.patch.object(state, "init_db", side_effect=AssertionError("migration")):
            code, out, err = self.capture(cli.cmd_cpu_scopes, args)
        self.assertEqual((0, ""), (code, err))
        first = json.loads(out)
        self.assertTrue(first["truncated"])
        self.assertNotIn("events", first["scopes"][0])
        args.cursor = first["next_cursor"]
        code, out, err = self.capture(cli.cmd_cpu_scopes, args)
        self.assertEqual(0, code, err)
        self.assertFalse(json.loads(out)["truncated"])
        args.scope_id, args.cursor = intent.scope_id, None
        code, out, err = self.capture(cli.cmd_cpu_scopes, args)
        exact = json.loads(out)
        self.assertEqual(1, len(exact["scopes"][0]["events"]))
        self.assertFalse(exact["runtime_probed"])
        self.assertEqual("none", exact["effect"])

    def test_legacy_reader_does_not_migrate_or_backfill_scopes(self):
        with state.connect() as conn:
            for name in ("cpu_scope_immutable", "cpu_scope_retained", "cpu_scope_event_immutable", "cpu_scope_event_retained"):
                conn.execute("DROP TRIGGER " + name)
            conn.execute("DROP TABLE cpu_scope_events")
            conn.execute("DROP TABLE cpu_scopes")
            conn.execute("PRAGMA user_version=18")
            self.assertTrue(state._schema_is_complete(conn, 18))
            self.assertEqual("migration_required", ledger.query(conn)["reason"])
        with mock.patch.object(state, "init_db", side_effect=AssertionError("query migration")):
            code, out, err = self.capture(cli.main, ["cpu-scopes", "--json"])
        self.assertEqual(0, code, err)
        self.assertEqual("migration_required", json.loads(out)["reason"])
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(state.DB_SCHEMA_VERSION, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertTrue(state._schema_is_complete(conn, 19))
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM cpu_scopes").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM cpu_scope_events").fetchone()[0])

    def test_query_rejects_bad_bounds_missing_identity_and_ambiguous_cursor(self):
        _, _, intent, _ = self.scope()
        for kwargs in ({"limit": 0}, {"limit": True}, {"limit": 101}, {"cursor": "../x"}, {"scope_id": "a" * 32, "cursor": intent.scope_id}, {"scope_id": "a" * 32}):
            with self.subTest(kwargs=kwargs), state.connect() as conn, self.assertRaises((ValueError, state.StateError)):
                ledger.query(conn, **kwargs)


if __name__ == "__main__":
    unittest.main()
