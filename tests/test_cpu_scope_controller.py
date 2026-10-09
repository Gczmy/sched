"""Synthetic controller/SQLite integration; never create a kernel scope here."""
import errno
import copy
import secrets
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from gsched import allocation, cpu_isolation, cpu_scope_controller as effects, cpu_scope_state as ledger, state
from gsched.execution import CpuScopeBinding, ScopeUnavailable
from gsched.execution.scopes import ScopeParent
import test_cpu_isolation as affinity


class ScopeAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.context = {"pid": 1, "start_token": "proc:2", "physical_host": "example", "uid": 1000,
                        "affinity": [0, 1], "cgroups": [{"hierarchy": "0", "controllers": "", "path": "/job_123/step_0/controller"}]}
        self.origin = {**self.context, "slurm_environment": {"SLURM_JOB_ID": "123", "SLURM_STEP_ID": "0"}}
        self.decision = {"allocation_state": "valid", "invalid_latched": False, "dispatch_allowed": True}
        self.mounts = [{"root": "/", "mountpoint": "/sys/fs/cgroup"}]

    def authority(self, path="/sys/fs/cgroup/job_123/step_0/scopes", **changes):
        return effects.authority_from_facts(path, changes.get("origin", self.origin), changes.get("current", self.context), changes.get("decision", self.decision), changes.get("mounts", self.mounts))

    def test_original_slurm_job_and_step_are_required_not_a_config_assertion(self):
        result = self.authority()
        self.assertEqual("/job_123/step_0", result["original_boundary"])
        for path in ("/sys/fs/cgroup/job_123/step_1/scopes", "/sys/fs/cgroup/job_124/step_0/scopes", "/sys/fs/cgroup/job_123", "/sys/fs/cgroup/job_1234/scopes"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                self.authority(path)
        for state_name in ("unknown", "invalid"):
            with self.assertRaises(ValueError):
                self.authority(decision={**self.decision, "allocation_state": state_name})

    def test_kernel_changes_missing_or_ambiguous_mounts_do_not_migrate(self):
        for changes in ({"current": {**self.context, "uid": 1001}}, {"current": {**self.context, "affinity": [1]}},
                        {"mounts": []}, {"mounts": self.mounts * 2}, {"mounts": [{"root": "/subroot", "mountpoint": "/sys/fs/cgroup"}]},
                        {"decision": {**self.decision, "invalid_latched": True}}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.authority(**changes)

    def test_non_slurm_requires_ancestor_or_descendant_of_original_daemon(self):
        origin = {**self.origin, "slurm_environment": {}}
        self.assertEqual("/job_123/step_0/scopes".replace("/scopes", ""), self.authority("/sys/fs/cgroup/job_123/step_0", origin=origin)["parent_cgroup"])
        with self.assertRaises(ValueError):
            self.authority(origin=origin)  # Sibling not explicitly delegated ancestor.
        with self.assertRaises(ValueError):
            self.authority(origin={**origin, "slurm_environment": {"SLURM_STEP_ID": "0"}})

    def test_mount_parser_is_bounded_and_decodes_paths_not_arbitrary_fields(self):
        self.assertEqual(self.mounts, effects.mounts_from_text("30 20 0:30 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n"))
        self.assertEqual("/example path", effects.mounts_from_text("30 20 0:30 / /example\\040path rw - cgroup2 cgroup rw")[0]["mountpoint"])
        for text in ("invalid", "x" * (1024 * 1024 + 1), "30 20 0:30 / /a/../b rw - cgroup2 cgroup rw"):
            with self.assertRaises(ValueError):
                effects.mounts_from_text(text)


class CpuScopeFixture(affinity.CpuIsolationFixture):
    def setUp(self):
        super().setUp()
        self.context["cgroups"] = [{"hierarchy": "0", "controllers": "", "path": "/example/controller"}]
        self.dispatcher._cluster_lease.origin["cgroups"] = self.context["cgroups"]
        self.dispatcher._cluster_lease.update = lambda: True
        self.cfg["cpu_isolation"] = {"mode": "cgroup", "delegated_root": "/sys/fs/cgroup/example"}
        self.dispatcher._read_gpu_policy = lambda: self.cfg
        self.register_lease()
        self.messages = []
        self.dispatcher.log_line = self.messages.append
        self.parent = ScopeParent(self.cfg["cpu_isolation"]["delegated_root"], 1, 10, "a" * 36, "mnt:[123]", 1000)
        self.entries, self.calls = {}, []
        self.root_cpus = (0, 1, 2, 3)
        outer = self

        class FakeScope:
            def __init__(self, entry, *, restored=False):
                self.entry, self.binding, self.restored = entry, entry.binding, restored
            def observe(self):
                e = self.entry
                return {"scope_configured": e.configured, "populated": e.populated, "direct_process_count": e.direct,
                        "effective_cpus": list(self.binding.intent.cpus), "effective_mems": [0],
                        "admission_granted": False, "wait_authority_granted": False, "device_isolation": "not_configured"}
            def configure(self):
                if self.restored:
                    raise AssertionError("restored configuration")
                outer.assert_phase(self.binding, "configure_intent")
                outer.calls.append("configure")
                self.entry.configured = True
                return self.observe()
            def constraints(self):
                if self.restored:
                    raise AssertionError("restored launch capability")
                outer.assert_phase(self.binding, "configured")
                outer.calls.append("constraints")
                return SimpleNamespace(cpu_affinity=self.binding.intent.cpus, cgroup_procs_fd=99)
            def remove(self):
                outer.assert_phase(self.binding, "cleanup_intent")
                if self.entry.populated or self.entry.direct:
                    raise AssertionError("busy removal")
                outer.calls.append("remove")
                del outer.entries[self.binding.intent.scope_id]
                return {"scope_removed": True, "wait_authority_granted": False}
            def close(self):
                outer.calls.append("close_scope_fd")

        class FakeManager:
            def __init__(self, path):
                if path != outer.parent.path:
                    raise ValueError("wrong parent")
                self.parent = outer.parent
            def _verify(self):
                return outer.root_cpus, (0,)
            def create(self, intent):
                outer.assert_phase(CpuScopeBinding(intent, 1, 100), "create_intent")
                outer.calls.append("create")
                if intent.scope_id in outer.entries:
                    raise OSError(errno.EEXIST, "original name exists")
                entry = SimpleNamespace(binding=CpuScopeBinding(intent, 1, 100 + len(outer.entries)), configured=False, populated=False, direct=0)
                outer.entries[intent.scope_id] = entry
                return FakeScope(entry)
            def restore(self, binding):
                outer.calls.append("restore")
                entry = outer.entries.get(binding.intent.scope_id)
                if entry is None or entry.binding != binding:
                    raise ScopeUnavailable("original scope inode missing or replaced", reason="scope_original_inode_binding_changed")
                return FakeScope(entry, restored=True)
            def close(self):
                outer.calls.append("close_parent_fd")

        for target, kwargs in (("gsched.cpu_scope_controller.DelegatedCpuScopes", {"side_effect": FakeManager}),
                              ("gsched.cpu_scope_controller._mounts", {"return_value": [{"root": "/", "mountpoint": "/sys/fs/cgroup"}]})):
            patch = mock.patch(target, **kwargs)
            patch.start()
            self.addCleanup(patch.stop)
        self.controller = effects.Controller(self.dispatcher)
        self.dispatcher._cpu_scopes = self.controller
        self.addCleanup(self.controller.close)

    def register_lease(self):
        """Synthetic new daemon birth, never repurpose the previous lease."""
        from gsched import cluster_lease
        monitor = self.dispatcher._cluster_lease
        owner = {"lease_id": secrets.token_hex(16), **{k: self.context[k] for k in ("pid", "start_token", "physical_host")}}
        monitor.owner = owner
        monitor.origin = {**copy.deepcopy(monitor.origin), "lease_id": owner["lease_id"], "started_at": time.time()}
        with state.connect() as conn:
            conn.execute("INSERT INTO daemon_leases VALUES(?,?,?,?)", (owner["lease_id"], monitor.origin["instance_id"], cluster_lease.encode(monitor.origin), cluster_lease.digest(monitor.origin)))
            cluster_lease.event(conn, owner["lease_id"], "check", {**monitor.decision, "current_context": self.context,
                "slurm_observation": monitor.sample, "slurm_binding": monitor.frozen_binding})

    def assert_phase(self, binding, phase):
        with state.connect() as conn:
            self.assertEqual(phase, ledger.load(conn, binding.intent.scope_id)[1][-1]["kind"])

    def allocated(self, number=1, count=1):
        job, spec = self.job(number, count)
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            selected = cpu_isolation.select(self.dispatcher, conn, count, job_id=job)
            self.assertTrue(selected["allowed"], selected)
            state.update_job(conn, job, status="running", pgid=None)
            identifier = allocation.reserve(conn, job, spec, self.dispatcher, cpu_binding=selected["binding"])
            current = state.get_job(conn, job)
            intent = self.controller.reserve(conn, current)
        return job, identifier, intent

    def prepared(self):
        job, identifier, intent = self.allocated()
        with state.connect() as conn:
            constraints = self.controller.prepare(conn, state.get_job(conn, job))
            checked = self.controller.launch_check(conn, state.get_job(conn, job))
            conn.execute("BEGIN IMMEDIATE")
            self.controller.launch_intent(conn, checked)
        self.controller.finish_launch(job)
        return job, identifier, intent, constraints

    def request(self, job):
        with state.connect() as conn:
            return ledger.request_cleanup(conn, state.get_job(conn, job), cleanup_source="ordinary_group_gone")

class ScopeControllerTests(CpuScopeFixture):
    def test_prepared_effects_follow_committed_intents_and_grant_only_once(self):
        job, identifier, intent, constraints = self.prepared()
        self.assertEqual((0,), constraints.cpu_affinity)
        self.assertEqual(["create", "configure", "constraints"], [c for c in self.calls if c in ("create", "configure", "constraints")])
        with state.connect() as conn, self.assertRaises(state.StateError):
            self.controller.prepare(conn, state.get_job(conn, job))
        self.assertFalse(self.request(job))
        self.assertTrue(effects.maintain(self.dispatcher))
        with state.connect() as conn:
            self.assertEqual("removed", ledger.load(conn, intent.scope_id)[1][-1]["kind"])
            self.assertTrue(ledger.release_allowed(conn, identifier, job))
            cpu_isolation.release(conn, state.get_job(conn, job), cleanup_source="ordinary_group_gone")
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM cpu_assignments").fetchone()[0])

    def test_group_clean_does_not_remove_busy_descendants_or_release_gpu_cpu(self):
        job, identifier, intent, _ = self.prepared()
        self.entries[intent.scope_id].populated = True
        with state.connect() as conn:
            current = state.get_job(conn, job)
            self.assertEqual([], self.dispatcher._handle_job_done(conn, current, 0))
        self.assertTrue(effects.maintain(self.dispatcher))
        self.assertNotIn("remove", self.calls)
        with state.connect() as conn:
            self.assertEqual("running", state.get_job(conn, job)["status"])
            self.assertFalse(ledger.release_allowed(conn, identifier, job))
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM cpu_assignments").fetchone()[0])
        self.entries[intent.scope_id].populated = False
        self.assertTrue(effects.maintain(self.dispatcher))
        self.assertIn("remove", self.calls)

    def test_crash_after_create_before_inode_is_not_recovered_by_name(self):
        job, identifier, intent = self.allocated()
        original = self.controller.manager.create
        def crashed(value):
            original(value)
            raise OSError("creation result unknown")
        with mock.patch.object(self.controller.manager, "create", side_effect=crashed), state.connect() as conn, self.assertRaises(OSError):
            self.controller.prepare(conn, state.get_job(conn, job))
        self.assertFalse(effects.maintain(self.dispatcher))
        self.assertNotIn("restore", self.calls)
        self.assertEqual(1, len(self.entries))
        with state.connect() as conn:
            self.assertFalse(ledger.release_allowed(conn, identifier, job))
            self.assertEqual("unknown", ledger.load(conn, intent.scope_id)[1][-1]["kind"])

    def test_recovery_before_launch_cleans_original_inode_without_start_or_reconfigure(self):
        job, _, intent = self.allocated()
        with state.connect() as conn:
            self.controller.prepare(conn, state.get_job(conn, job))
        self.controller.finish_launch(job)  # Model process FD loss, not directory loss.
        self.assertTrue(effects.maintain(self.dispatcher))
        self.assertEqual(1, self.calls.count("create"))
        self.assertEqual(1, self.calls.count("configure"))
        self.assertEqual(1, self.calls.count("constraints"))
        with state.connect() as conn:
            self.assertEqual("removed", ledger.load(conn, intent.scope_id)[1][-1]["kind"])
            self.assertFalse(ledger.query(conn, scope_id=intent.scope_id)["scopes"][0]["launch_consumed"])

    def test_removal_result_unknown_never_inferred_from_missing_directory(self):
        job, identifier, intent, _ = self.prepared()
        self.request(job)
        original = effects._advance
        def failed(identifier, kind, data=None):
            if kind == "removed":
                raise state.StateError("commit unavailable")
            return original(identifier, kind, data)
        with mock.patch.object(effects, "_advance", side_effect=failed):
            self.assertFalse(effects.maintain(self.dispatcher))
        self.assertFalse(self.entries)
        self.assertFalse(effects.maintain(self.dispatcher))
        with state.connect() as conn:
            self.assertFalse(ledger.release_allowed(conn, identifier, job))
            self.assertEqual("unknown", ledger.load(conn, intent.scope_id)[1][-1]["kind"])

    def test_replaced_inode_or_changed_configuration_pauses_without_recreation(self):
        _, _, intent, _ = self.prepared()
        self.entries[intent.scope_id].configured = False
        self.assertFalse(effects.maintain(self.dispatcher))
        self.assertFalse(self.controller.admission_current())
        self.assertEqual(1, self.calls.count("create"))
        with state.connect() as conn:
            self.assertEqual("scope_configuration_drift", ledger.load(conn, intent.scope_id)[1][-1]["data"]["reason"])

    def test_creation_and_maintenance_never_use_uncommitted_request_writer(self):
        job, _, _ = self.allocated()
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            with self.assertRaises(state.StateError):
                self.controller.prepare(conn, state.get_job(conn, job))
            with state.bind_connection(conn):
                self.assertFalse(effects.maintain(self.dispatcher))
        self.assertNotIn("create", self.calls)

    def test_cancel_and_cold_policy_change_refuse_launch_after_configuration(self):
        job, _, _ = self.allocated()
        with state.connect() as conn:
            self.controller.prepare(conn, state.get_job(conn, job))
            state.update_job(conn, job, kill_reason="cancelled")
            conn.commit()
            checked = self.controller.launch_check(conn, state.get_job(conn, job))
            conn.execute("BEGIN IMMEDIATE")
            with self.assertRaises(state.StateError):
                self.controller.launch_intent(conn, checked)
            conn.rollback()
            self.cfg["cpu_isolation"] = {"mode": "off"}
            with self.assertRaises(state.StateError):
                self.controller.launch_check(conn, state.get_job(conn, job))

    def test_parent_cpu_or_memory_drift_refuses_new_admission(self):
        self.root_cpus = (1, 2, 3)
        with self.assertRaises(ValueError):
            self.controller.preflight()

    def test_original_wait_survives_later_scope_cleanup_ticks_in_same_daemon(self):
        job, identifier, intent, _ = self.prepared()
        fact = {"source": "local_supervisor_wait", "returncode": 0, "group_clean": True,
                "pid": 123, "start_token": "proc:456", "binding_verified": True,
                "subject": "scheduler_supervisor_command_chain"}
        self.entries[intent.scope_id].populated = True
        self.dispatcher._job_process_state = lambda _: "dead"
        with state.connect() as conn:
            state.update_job(conn, job, pgid=123)
            self.dispatcher._handle_job_done(conn, state.get_job(conn, job), 0, ordinary_wait=fact)
        retained = self.dispatcher._cpu_scope_settlements[identifier]
        self.assertEqual(fact, retained["wait"])
        self.assertEqual(0, retained["rc"])
        self.entries[intent.scope_id].populated = False
        self.assertTrue(effects.maintain(self.dispatcher))
        with mock.patch.object(self.dispatcher, "_consume_pending_cancel_before_requeue", return_value=True), state.connect() as conn:
            self.dispatcher._handle_job_done(conn, state.get_job(conn, job), retained["rc"], ordinary_wait=retained["wait"])
        self.assertNotIn(identifier, self.dispatcher._cpu_scope_settlements)

    def test_reaper_consumes_retained_allocation_wait_without_poll_or_rc_file(self):
        job, identifier, _, _ = self.prepared()
        retained = {"rc": 0, "wait": {"source": "local_supervisor_wait"}, "authoritative": False}
        self.dispatcher._cpu_scope_settlements = {identifier: retained}
        with state.connect() as conn:
            state.update_job(conn, job, pgid=123)
        with mock.patch.object(self.dispatcher, "_reap_configured_executions", return_value=[]), mock.patch.object(self.dispatcher, "_has_native_session", return_value=False), mock.patch.object(self.dispatcher, "_handle_job_done", return_value=[]) as settle:
            self.dispatcher._reap_finished_jobs()
        self.assertEqual(0, settle.call_args.args[2])
        self.assertEqual(retained["wait"], settle.call_args.kwargs["ordinary_wait"])

    def test_clean_scope_without_original_wait_interrupts_without_artifact_success(self):
        job, _, intent, _ = self.prepared()
        self.request(job)
        self.assertTrue(effects.maintain(self.dispatcher))
        with mock.patch.object(self.dispatcher, "_release_gpu_for_job"), mock.patch.object(self.dispatcher, "_completion_artifacts_valid", side_effect=AssertionError("no fabricated completion")), state.connect() as conn:
            self.dispatcher._handle_job_done(conn, state.get_job(conn, job), 0)
            current = state.get_job(conn, job)
            self.assertEqual("interrupted", current["status"])
            self.assertEqual("cpu_scope_wait_authority_lost", current["failure"])

    def test_execution_exit_does_not_retire_owner_before_scope_settlement(self):
        job, _, _, _ = self.prepared()
        self.dispatcher.executor = SimpleNamespace(retire_configured_execution=mock.Mock(side_effect=AssertionError("owner retired before settlement")))
        with state.connect() as conn:
            self.dispatcher._ack_configured_owner(conn, job)
        self.dispatcher.executor.retire_configured_execution.assert_not_called()

    def test_missing_scope_intent_never_downgrades_cgroup_launch_or_release(self):
        job, spec = self.job(1)
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            selected = cpu_isolation.select(self.dispatcher, conn, 1, job_id=job)
            state.update_job(conn, job, status="running")
            identifier = allocation.reserve(conn, job, spec, self.dispatcher, cpu_binding=selected["binding"])
            current = state.get_job(conn, job)
            with self.assertRaises(state.StateError):
                cpu_isolation.launch_constraints(conn, current)
            with self.assertRaises(state.StateError):
                ledger.request_cleanup(conn, current, cleanup_source="ordinary_group_gone")
            with self.assertRaises(state.StateError):
                cpu_isolation.release(conn, current, cleanup_source="ordinary_group_gone")
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM cpu_assignments WHERE allocation_id=?", (identifier,)).fetchone()[0])


if __name__ == "__main__":
    unittest.main()
