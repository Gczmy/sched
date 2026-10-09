"""Pure NVML getter models and original MIG evidence; no vendor calls locally."""
import contextlib
import copy
import ctypes
import io
import json
import subprocess
import sys
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from gsched import cli, device_inventory as inventory, device_inventory_state as frozen, device_scope_controller, mig_capability as mig, state
import test_device_inventory as mapping
import test_device_inventory_state as records


class MigCapabilityTests(unittest.TestCase):
    def test_success_disabled_versus_not_supported_and_unknown_are_distinct(self):
        self.assertEqual("supported", next(iter(mig.reconcile(mapping.capabilities(), inventory.parse_smi(mapping.CSV)).values())))
        csv = mapping.CSV.replace("Disabled, Disabled", "N/A, N/A")
        result = mapping.captured_mig(csv, evidence=mapping.capabilities(csv, code=3))
        self.assertEqual({"not_supported"}, set(mig.reconcile(result["mig_capabilities"], result["cards"]).values()))
        policy = inventory.select_policy(result, [mapping.reservation()], now=100)
        self.assertIn((195, 7), {(r.major, r.minor) for r in policy.rules})
        for code in (1, 2, 4, 9, 10, 15, 17, 18, 999):
            unknown = mapping.captured_mig(csv, evidence=mapping.capabilities(csv, code=code))
            self.assertEqual({"unknown"}, set(mig.reconcile(unknown["mig_capabilities"], unknown["cards"]).values()))
            with self.assertRaises(ValueError):
                inventory.select_policy(unknown, [mapping.reservation()], now=100)
            self.assertEqual(6, len(inventory.select_policy(unknown, [], now=100).rules))

    def test_na_without_capability_and_legacy_inventory_never_grants_full_gpu(self):
        csv = mapping.CSV.replace("Disabled, Disabled", "N/A, N/A")
        with self.assertRaises(ValueError):
            inventory.select_policy(mapping.captured(csv), [mapping.reservation()], now=100)
        old = mapping.captured()
        original = copy.deepcopy(old)
        inventory.select_policy(old, [mapping.reservation()], now=100)
        self.assertEqual(original, old)
        self.assertEqual(inventory.VERSION, old["interface_version"])
        self.assertNotIn("mig_capabilities", old)

    def test_not_supported_only_from_verified_get_mig_mode_never_other_api_codes(self):
        for phase in ("initialization", "handle", "uuid_before", "uuid_after", "version", "library", "api", "shutdown"):
            raw = mapping.capabilities(code=3, phase=phase)
            statuses = mig.reconcile(raw, inventory.parse_smi(mapping.CSV))
            self.assertEqual({"unknown"}, set(statuses.values()))
            result = mapping.captured_mig(evidence=raw)
            with self.assertRaises(ValueError):
                inventory.select_policy(result, [mapping.reservation()], now=100)

    def test_csv_and_nvml_modes_must_agree_no_na_or_enable_fallback(self):
        for csv, raw in ((mapping.CSV, mapping.capabilities(code=3)),
                         (mapping.CSV, mapping.capabilities(current=1)),
                         (mapping.CSV.replace("Disabled, Disabled", "N/A, N/A"), mapping.capabilities())):
            with self.assertRaises(ValueError):
                mapping.captured_mig(csv, evidence=raw)
        for current, pending in ((1, 1), (0, 1), (1, 0)):
            csv = mapping.CSV.replace("Disabled, Disabled", ("Enabled" if current else "Disabled") + ", " + ("Enabled" if pending else "Disabled"))
            result = mapping.captured_mig(csv, evidence=mapping.capabilities(csv, current=current, pending=pending))
            with self.assertRaises(ValueError):
                inventory.select_policy(result, [mapping.reservation()], now=100)

    def test_missing_duplicate_uuid_identity_version_and_mode_corruption_refused(self):
        raw = mapping.capabilities()
        cases = []
        for field, value in (("uuid", mapping.FIRST), ("identity_verified", 1), ("identity_verified", False),
                             ("code", True), ("phase", "set_mig"), ("current", True), ("pending", -1)):
            changed = copy.deepcopy(raw)
            changed["entries"][1][field] = value
            cases.append(changed)
        cases += [{**raw, "extra": True}, {**raw, "entries": []}, {**raw, "entries": raw["entries"][:1]},
                  {**raw, "library_version": None}, {**raw, "driver_version": "x" * 129}]
        for value in cases:
            with self.assertRaises(ValueError):
                mig.reconcile(value, inventory.parse_smi(mapping.CSV))
        result = mapping.captured_mig(evidence=mapping.capabilities(current=2))
        with self.assertRaises(ValueError):
            inventory.select_policy(result, [mapping.reservation()], now=100)

    def test_optional_cli_negotiates_separate_contract_without_db_or_mutation(self):
        csv = mapping.CSV.replace("Disabled, Disabled", "N/A, N/A")
        result = mapping.captured_mig(csv, evidence=mapping.capabilities(csv, code=3))
        with mock.patch.object(cli, "load_config", return_value={"node": "example"}), \
                mock.patch.object(cli, "_is_foreign_host", return_value=False), mock.patch.object(cli.sys, "platform", "linux"), \
                mock.patch.object(state, "connect", side_effect=AssertionError("DB read")), \
                mock.patch.object(inventory, "capture", return_value=result) as capture, mock.patch("builtins.print") as printed:
            self.assertEqual(0, cli.main(["device-inventory", "--with-mig-capability", "--json"]))
        capture.assert_called_once_with(include_mig=True)
        report = json.loads(printed.call_args.args[0])
        self.assertEqual("sched-device-inventory-mig-v1", report["contract"])
        self.assertEqual({"not_supported"}, {r["status"] for r in report["mig_support"]})
        self.assertFalse(report["admission_granted"])

    def test_controller_requires_modern_evidence_not_implicit_upgrade_of_legacy_map(self):
        with mock.patch.object(inventory, "capture", return_value=mapping.captured()) as capture, self.assertRaises(state.StateError):
            device_scope_controller._sample()
        capture.assert_called_once_with(include_mig=True)


class NvmlGetterModels(unittest.TestCase):
    def probe(self, *, mig_code=3, current=0, pending=0, fail=None, changed_uuid=False, missing=None):
        counts = {}
        handles = {}
        def invoke(name, *args):
            counts[name] = counts.get(name, 0) + 1
            if name == fail:
                return 3  # Same code in another API must stay unknown.
            if name == "nvmlDeviceGetHandleByUUID":
                handles[len(handles) + 1] = args[0].decode()
                ctypes.cast(args[1], ctypes.POINTER(ctypes.c_void_p))[0] = len(handles)
            elif name == "nvmlDeviceGetUUID":
                target = handles[args[0].value]
                args[1].value = (mapping.FIRST if changed_uuid and counts[name] % 2 == 0 else target).encode()
            elif name in {"nvmlSystemGetDriverVersion", "nvmlSystemGetNVMLVersion"}:
                args[0].value = b"550.1"
            elif name == "nvmlDeviceGetMigMode":
                if mig_code == 0:
                    ctypes.cast(args[1], ctypes.POINTER(ctypes.c_uint))[0] = current
                    ctypes.cast(args[2], ctypes.POINTER(ctypes.c_uint))[0] = pending
                return mig_code
            return 0
        class Library:
            def __getattr__(self, name):
                if name == missing:
                    raise AttributeError(name)
                return mock.Mock(side_effect=lambda *args: invoke(name, *args))
        stream = io.StringIO()
        with mock.patch.object(ctypes, "CDLL", return_value=Library()), \
                mock.patch("sys.argv", ["probe", json.dumps([mapping.FIRST, mapping.SECOND])]), contextlib.redirect_stdout(stream):
            exec(mig.ENTRY, {})
        return json.loads(stream.getvalue()), counts

    def test_read_only_getters_verify_uuid_twice_and_explicit_not_supported(self):
        result, calls = self.probe()
        self.assertEqual({"not_supported"}, set(mig.reconcile(result, inventory.parse_smi(mapping.CSV.replace("Disabled, Disabled", "N/A, N/A"))).values()))
        self.assertEqual(4, calls["nvmlDeviceGetUUID"])
        self.assertEqual(1, calls["nvmlShutdown"])
        self.assertFalse(any("Set" in name or "Create" in name or "Destroy" in name for name in calls))

    def test_initialization_handle_uuid_and_missing_api_are_unknown_not_unsupported(self):
        for fail in ("nvmlInit_v2", "nvmlDeviceGetHandleByUUID", "nvmlDeviceGetUUID", "nvmlSystemGetDriverVersion", "nvmlShutdown"):
            result, calls = self.probe(fail=fail)
            self.assertEqual({"unknown"}, set(mig.reconcile(result, inventory.parse_smi(mapping.CSV)).values()))
        result, calls = self.probe(missing="nvmlDeviceGetMigMode")
        self.assertEqual({"unknown"}, set(mig.reconcile(result, inventory.parse_smi(mapping.CSV)).values()))
        self.assertNotIn("nvmlInit_v2", calls)

    def test_uuid_change_and_success_with_invalid_modes_do_not_grant_full_gpu(self):
        result, calls = self.probe(changed_uuid=True)
        self.assertEqual("unknown", mig.reconcile(result, inventory.parse_smi(mapping.CSV.replace("Disabled, Disabled", "N/A, N/A")))[mapping.SECOND])
        for current in (0, 1, 2):
            result, calls = self.probe(mig_code=0, current=current)
            self.assertEqual("supported" if current in (0, 1) else "unknown", mig.classification(result["entries"][0]))


class FrozenMigTests(records.DeviceInventoryFixture):
    def original(self, number=1, gpu=False):
        job, identifier, intent, captured = super().original(number, gpu)
        return job, identifier, intent, mapping.captured_mig()

    def test_original_mig_evidence_is_frozen_and_queried_without_reprobe(self):
        job, identifier, intent, captured, binding = self.frozen(gpu=True)
        with state.connect() as conn, mock.patch.object(inventory, "capture", side_effect=AssertionError("probe")):
            self.assertEqual(captured, frozen.load(conn, intent.scope.intent.scope_id)["inventory"])
            self.assertEqual(captured, frozen.query(conn, scope_id=intent.scope.intent.scope_id)["bindings"][0]["inventory"])
        with state.connect() as conn:
            self.assertFalse(frozen.verify_current(conn, intent.scope.intent.scope_id, copy.deepcopy(captured), 100)["admission_granted"])

    def test_changed_mig_driver_status_or_modes_refuse_revalidation(self):
        _, _, intent, captured, _ = self.frozen(gpu=True)
        for field, value in (("driver_version", "551.1"), ("library_version", "13.551.1")):
            changed = copy.deepcopy(captured)
            changed["mig_capabilities"][field] = value
            with state.connect() as conn, self.assertRaises(state.StateError):
                frozen.verify_current(conn, intent.scope.intent.scope_id, changed, 100)
        changed = copy.deepcopy(captured)
        changed["mig_capabilities"]["entries"][1]["code"] = 4
        changed["mig_capabilities"]["entries"][1]["current"] = None
        changed["mig_capabilities"]["entries"][1]["pending"] = None
        with state.connect() as conn, self.assertRaises(state.StateError):
            frozen.verify_current(conn, intent.scope.intent.scope_id, changed, 100)

    def test_schema23_migration_keeps_legacy_frozen_inventory_byte_identical(self):
        # The legacy fixture intentionally retains original v1 facts.
        _, _, intent, captured = records.DeviceInventoryFixture.original(self, gpu=True)
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            frozen.freeze(conn, intent.scope.intent.scope_id, captured, 99)
            original = conn.execute("SELECT payload,payload_sha256 FROM device_inventory_bindings").fetchone()
            conn.execute("PRAGMA user_version=23")  # Synthetic old-schema fixture only.
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(tuple(original), tuple(conn.execute("SELECT payload,payload_sha256 FROM device_inventory_bindings").fetchone()))
            self.assertEqual(inventory.VERSION, frozen.load(conn, intent.scope.intent.scope_id)["inventory"]["interface_version"])

    def test_modern_binding_requires_current_marker_before_writing_any_fact(self):
        _, _, intent, captured = self.original(gpu=True)
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("PRAGMA user_version=23")
            with self.assertRaises(state.StateError):
                frozen.freeze(conn, intent.scope.intent.scope_id, captured, 99)
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM device_inventory_bindings").fetchone()[0])
            conn.rollback()


class MigCaptureModels(mapping.DeviceCaptureFixture):
    def test_complete_capture_brackets_same_original_capability_without_changing_legacy_shape(self):
        capability = mapping.capabilities()
        def query(deadline, **options):
            if options:
                self.assertEqual([mapping.FIRST, mapping.SECOND], options["uuids"])
                return json.dumps(capability)
            return mapping.CSV
        with mock.patch.object(inventory, "_query", side_effect=query) as called:
            result = inventory.capture(include_mig=True)
        self.assertEqual(mapping.captured_mig(), result)
        self.assertEqual(4, called.call_count)
        self.assertEqual({10, *self.descriptors.values()}, set(self.closed))

    def test_mid_capture_capability_change_or_missing_result_refuses_map(self):
        first, last = mapping.capabilities(), mapping.capabilities()
        last["driver_version"] = "551.1"
        with mock.patch.object(inventory, "_query", side_effect=[mapping.CSV, json.dumps(first), mapping.CSV, json.dumps(last)]), self.assertRaises(ValueError):
            inventory.capture(include_mig=True)
        with mock.patch.object(inventory, "_query", side_effect=[mapping.CSV, "null", mapping.CSV, "null"]), self.assertRaises(ValueError):
            inventory.capture(include_mig=True)

    def test_probe_targets_bound_and_unreaped_original_child_never_spawns(self):
        import time
        for targets in ([], [mapping.FIRST, mapping.FIRST], ["MIG-example"], [mapping.FIRST] * 129):
            with mock.patch.object(inventory, "_pending_query", None), mock.patch.object(inventory.subprocess, "Popen") as spawned, self.assertRaises(ValueError):
                inventory._query(time.monotonic() + 2, uuids=targets)
            spawned.assert_not_called()
        previous = mock.Mock()
        previous.poll.return_value = None
        with mock.patch.object(inventory, "_pending_query", previous), mock.patch.object(inventory.subprocess, "Popen") as spawned, self.assertRaises(TimeoutError):
            inventory._query(time.monotonic() + 2, uuids=[mapping.FIRST])
        spawned.assert_not_called()


@unittest.skipUnless(sys.platform == "linux", "bounded helper requires Linux compute")
class BoundedMigQueryTests(unittest.TestCase):
    def helper(self, code, deadline):
        real_popen = subprocess.Popen
        children = []
        def launch(command, **options):
            self.assertEqual([sys.executable, "-I", "-S", "-c", mig.ENTRY, json.dumps([mapping.FIRST])], command)
            process = real_popen([sys.executable, "-I", "-S", "-c", code], **options)
            children.append(process)
            return process
        with mock.patch.object(inventory.subprocess, "Popen", side_effect=launch):
            try:
                return inventory._query(time.monotonic() + deadline, uuids=[mapping.FIRST])
            finally:
                self.assertIsNotNone(children[0].returncode)
                self.assertTrue(children[0].stdout.closed)

    def test_fixed_isolated_helper_reaps_actual_cpu_child_without_nvml_loading(self):
        self.assertEqual("example\n", self.helper("print('example')", 2))

    def test_output_overflow_refuses_partial_evidence_and_reaps_child(self):
        with self.assertRaises(ValueError):
            self.helper("import os; os.write(1,b'x'*100000)", 2)

    def test_observation_deadline_reaps_exact_helper_not_restart(self):
        with self.assertRaises(TimeoutError):
            self.helper("import time; time.sleep(20)", .1)
