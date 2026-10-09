"""Synthetic controller/effect integration; no native attach or worker starts."""
import copy
import json
from types import SimpleNamespace
import unittest
from unittest import mock

from gsched import cpu_scope_controller as scopes, cpu_scope_state as cpu, device_scope_controller as effects, device_scope_state as ledger, state
from gsched.execution import DeviceBinding
from gsched.execution.scopes import ScopeParent
import test_cpu_scope_controller as fixture
import test_device_inventory as mapping


class DevicePolicyTests(unittest.TestCase):
    def test_explicit_cold_policy_requires_cpu_cgroup_without_probes(self):
        self.assertEqual({"mode": "off"}, effects.policy({}))
        for raw in (None, True, {"mode": True}, {"mode": "auto"}, {"mode": "off", "extra": 1}):
            with self.assertRaises(ValueError):
                effects.policy({"device_isolation": raw})
        for mode in ("off", "affinity"):
            with self.assertRaises(ValueError):
                effects.policy({"device_isolation": {"mode": "nvidia"}, "cpu_isolation": {"mode": mode}})

    def test_passive_preflight_only_queries_retained_parent_and_rejects_incomplete_results(self):
        native = SimpleNamespace(device_program_query=mock.Mock(return_value={"program_ids": [], "attach_flags": 0}))
        with mock.patch("gsched.execution.devices._native", return_value=native):
            effects.preflight(SimpleNamespace(_fd=99))
            native.device_program_query.assert_called_once_with(99)
            for result in ({}, {"program_ids": [True], "attach_flags": 0}, {"program_ids": [1, 1], "attach_flags": 0},
                           {"program_ids": list(range(1, 66)), "attach_flags": 0}, {"program_ids": [], "attach_flags": True}):
                native.device_program_query.return_value = result
                with self.assertRaises(state.StateError):
                    effects.preflight(SimpleNamespace(_fd=99))


class DeviceControllerFixture(fixture.CpuScopeFixture):
    def setUp(self):
        super().setUp()
        self.controller.close()
        self.parent = ScopeParent(self.parent.path, self.parent.device, self.parent.inode,
                                  mapping.CONTEXT["boot_id"], mapping.CONTEXT["mount_namespace"], self.parent.uid)
        self.cfg["device_isolation"] = {"mode": "nvidia"}
        self.register_lease()  # Cold policy/parent changes require a new birth.
        self.sampled = mapping.captured_mig()
        self.installed, self.query_ok = [], True
        outer = self

        class ModelDevice:
            def __init__(self, scope, intent, *, binding=None):
                self.scope, self.intent, self.binding = scope, intent, binding
                self._phase = "new" if binding is None else "restored"
            def install(self):
                if self._phase != "new":
                    raise AssertionError("reinstall")
                with state.connect() as conn:
                    outer.assertEqual("install_intent", ledger.load(conn, self.intent.scope.intent.scope_id)[1][-1]["kind"])
                self._phase = "installed"
                self.binding = DeviceBinding(self.intent, 71, "d" * 16)
                outer.installed.append(self)
                return self.binding
            def observe(self):
                if not outer.query_ok:
                    raise RuntimeError("original attachment missing")
                return {"device_attachment_verified": True, "program_id": self.binding.program_id,
                        "admission_granted": False, "wait_authority_granted": False}
            def constraints(self):
                if self._phase != "installed":
                    raise AssertionError("restored/consumed launch")
                with state.connect() as conn:
                    outer.assertEqual("installed", ledger.load(conn, self.intent.scope.intent.scope_id)[1][-1]["kind"])
                self._phase = "launch_capability_issued"
                return self.scope.constraints()

        for target, kwargs in (("gsched.device_scope_controller.preflight", {"return_value": {"program_ids": [], "attach_flags": 0}}),
                              ("gsched.device_scope_controller.DeviceScope", {"side_effect": ModelDevice}),
                              ("gsched.device_scope_controller.inventory.capture", {"side_effect": lambda **kwargs: copy.deepcopy(self.sampled)}),
                              ("gsched.device_scope_controller.time.time", {"return_value": 100})):
            patch = mock.patch(target, **kwargs)
            patched = patch.start()
            if target.endswith("time.time"):
                self.clock = patched
            self.addCleanup(patch.stop)
        self.controller = scopes.Controller(self.dispatcher)
        self.dispatcher._cpu_scopes = self.controller
        self.addCleanup(self.controller.close)

    def prepare(self):
        job, identifier, intent = self.allocated()
        with state.connect() as conn:
            self.controller.prepare(conn, state.get_job(conn, job))
        return job, identifier, intent

    def checked(self, job):
        with state.connect() as conn:
            return self.controller.launch_check(conn, state.get_job(conn, job))

    def launch(self, job):
        checked = self.checked(job)
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.controller.launch_intent(conn, checked)
        self.controller.finish_launch(job)

class DeviceControllerTests(DeviceControllerFixture):
    def test_original_requirement_is_frozen_before_effects_and_both_launch_cas_commit(self):
        job, identifier, intent = self.prepare()
        with state.connect() as conn:
            original, binding = cpu._allocation(conn, identifier, job)
            self.assertEqual("nvidia", binding["device_isolation"])
            self.assertEqual("installed", ledger.load(conn, intent.scope_id)[1][-1]["kind"])
        self.assertEqual(1, len(self.installed))
        self.launch(job)
        with state.connect() as conn:
            self.assertEqual("launch_intent", ledger.load(conn, intent.scope_id)[1][-1]["kind"])
            self.assertEqual("launch_intent", cpu.load(conn, intent.scope_id)[1][-1]["kind"])
        self.assertEqual({}, self.controller.device_handles)

    def test_required_device_record_missing_cannot_launch_cpu_only(self):
        job, identifier, intent = self.allocated()
        with state.connect() as conn, self.assertRaises(state.StateError):
            ledger.cpu_launch_guard(conn, identifier, job)
        self.cfg["device_isolation"] = {"mode": "off"}
        with state.connect() as conn, self.assertRaises(state.StateError):
            self.controller.prepare(conn, state.get_job(conn, job))
        self.assertEqual([], self.installed)

    def test_unknown_install_is_consumed_and_not_replayed(self):
        job, identifier, intent = self.allocated()
        with mock.patch.object(effects.DeviceScope, "side_effect", side_effect=RuntimeError("constructor unknown")):
            with state.connect() as conn, self.assertRaises(RuntimeError):
                self.controller.prepare(conn, state.get_job(conn, job))
        with state.connect() as conn:
            self.assertEqual("unknown", ledger.load(conn, intent.scope_id)[1][-1]["kind"])
        with state.connect() as conn, self.assertRaises(state.StateError):
            self.controller.prepare(conn, state.get_job(conn, job))
        self.assertEqual([], self.installed)

    def test_effect_then_lost_result_keeps_consumed_intent_and_never_grants_constraints(self):
        job, identifier, intent = self.allocated()
        factory = effects.DeviceScope.side_effect
        def lost(scope, device_intent):
            device = factory(scope, device_intent)
            install = device.install
            def install_then_raise():
                install()
                raise RuntimeError("attachment result lost")
            device.install = install_then_raise
            return device
        with mock.patch.object(effects.DeviceScope, "side_effect", lost):
            with state.connect() as conn, self.assertRaises(RuntimeError):
                self.controller.prepare(conn, state.get_job(conn, job))
        self.assertEqual(1, len(self.installed))
        self.assertNotIn("constraints", self.calls)
        with state.connect() as conn:
            self.assertEqual("unknown", ledger.load(conn, intent.scope_id)[1][-1]["kind"])
            self.assertFalse(cpu.release_allowed(conn, identifier, job))
        with state.connect() as conn, self.assertRaises(state.StateError):
            self.controller.prepare(conn, state.get_job(conn, job))

    def test_original_gpu_minor_is_selected_and_cas_cannot_refresh_expired_topology(self):
        original_job = self.job
        def gpu_job(number, count):
            job, spec = original_job(number, count)
            spec["resources"]["gpu"] = 1
            with state.connect() as conn:
                conn.execute("UPDATE tasks SET spec=? WHERE batch_id=(SELECT batch_id FROM jobs WHERE id=?)", (json.dumps(spec), job))
                conn.execute("INSERT INTO gpus(idx,mem_total_gib,status) VALUES(1,24,'free')")
                conn.execute("INSERT INTO gpu_jobs(gpu_id,job_id,vram_gib) VALUES(1,?,1)", (job,))
            return job, spec
        self.job = gpu_job
        self.dispatcher.allocator = SimpleNamespace(_uuid_map={mapping.SECOND: 1}, _uuid_observed_at=99)
        job, identifier, intent = self.prepare()
        self.assertIn((195, 7), {(r.major, r.minor) for r in self.installed[0].intent.policy.rules})
        self.assertNotIn((195, 1), {(r.major, r.minor) for r in self.installed[0].intent.policy.rules})
        checked = self.checked(job)
        self.clock.return_value = 104.5  # Probe still fresh, but original topology expired.
        with self.assertRaises(ValueError), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.controller.launch_intent(conn, checked)
        with state.connect() as conn:
            self.assertEqual("installed", ledger.load(conn, intent.scope_id)[1][-1]["kind"])

    def test_current_semantic_guard_refuses_older_writer_without_rewriting_records(self):
        job, identifier, intent = self.prepare()
        with state.connect() as conn:
            original = ledger.load(conn, intent.scope_id)
            self.assertEqual(state.DB_SCHEMA_VERSION, conn.execute("PRAGMA user_version").fetchone()[0])
        with mock.patch.object(state, "DB_SCHEMA_VERSION", state.DB_SCHEMA_VERSION - 1), self.assertRaises(state.StateError):
            state.init_db()
        with state.connect() as conn:
            self.assertEqual(original, ledger.load(conn, intent.scope_id))

    def test_original_mapping_drift_prevents_device_or_cpu_launch(self):
        job, identifier, intent = self.prepare()
        self.sampled["nodes"]["nvidia7"]["inode"] += 1
        with self.assertRaises(state.StateError):
            self.checked(job)
        with state.connect() as conn:
            self.assertEqual("configured", cpu.load(conn, intent.scope_id)[1][-1]["kind"])

    def test_attachment_query_failure_does_not_issue_new_constraints(self):
        job, identifier, intent = self.prepare()
        self.query_ok = False
        with self.assertRaises(RuntimeError):
            self.checked(job)
        self.assertEqual(1, self.calls.count("constraints"))

    def test_lost_retained_handle_or_cold_policy_refuses_launch(self):
        job, identifier, intent = self.prepare()
        original = self.controller.device_handles.pop(identifier)
        with self.assertRaises(state.StateError):
            self.checked(job)
        self.controller.device_handles[identifier] = original
        self.cfg["device_isolation"] = {"mode": "off"}
        with self.assertRaises(state.StateError):
            self.checked(job)

    def test_expired_check_and_cancel_refuse_both_writer_intents(self):
        job, identifier, intent = self.prepare()
        checked = self.checked(job)
        self.clock.return_value = 106
        with self.assertRaises(state.StateError), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.controller.launch_intent(conn, checked)
        self.clock.return_value = 100
        with state.connect() as conn:
            state.update_job(conn, job, status="cancelled")
        with self.assertRaises(state.StateError), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.controller.launch_intent(conn, checked)
        with state.connect() as conn:
            self.assertEqual("installed", ledger.load(conn, intent.scope_id)[1][-1]["kind"])
            self.assertEqual("configured", cpu.load(conn, intent.scope_id)[1][-1]["kind"])

    def test_cpu_cas_conflict_rolls_back_device_launch_intent(self):
        job, identifier, intent = self.prepare()
        checked = self.checked(job)
        checked = (checked[0], "0" * 32, *checked[2:])
        with self.assertRaises(state.StateError), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.controller.launch_intent(conn, checked)
        with state.connect() as conn:
            self.assertEqual("installed", ledger.load(conn, intent.scope_id)[1][-1]["kind"])

    def test_recovery_only_observes_original_binding_and_never_installs_or_launches(self):
        job, identifier, intent = self.prepare()
        self.launch(job)
        self.assertTrue(scopes.maintain(self.dispatcher))
        self.assertEqual(1, len(self.installed))
        with self.assertRaises(state.StateError):
            self.checked(job)

    def test_unknown_attachment_retains_resources_but_empty_original_cleanup_can_finish(self):
        job, identifier, intent = self.prepare()
        self.launch(job)
        self.query_ok = False
        self.assertFalse(scopes.maintain(self.dispatcher))
        with state.connect() as conn:
            self.assertEqual("unknown", ledger.load(conn, intent.scope_id)[1][-1]["kind"])
            self.assertFalse(cpu.release_allowed(conn, identifier, job))
        self.assertFalse(self.request(job))
        self.assertTrue(scopes.maintain(self.dispatcher))
        self.assertTrue(self.request(job))  # Removed record is reconciled by request_cleanup.
        with state.connect() as conn:
            self.assertEqual("released", ledger.load(conn, intent.scope_id)[1][-1]["kind"])
            self.assertTrue(cpu.release_allowed(conn, identifier, job))

    def test_fake_topology_refused_before_parent_open(self):
        self.dispatcher.fake = True
        with mock.patch.object(scopes, "DelegatedCpuScopes", side_effect=AssertionError("parent touched")), self.assertRaises(ValueError):
            scopes.Controller(self.dispatcher)
