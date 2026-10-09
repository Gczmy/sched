"""Exact device mapping models; helper execution only on Linux compute hosts."""
import copy
import json
import os
import stat
import subprocess
import sys
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from gsched import cli, device_inventory as inventory, state


FIRST = "GPU-11111111-1111-1111-1111-111111111111"
SECOND = "GPU-22222222-2222-2222-2222-222222222222"
CSV = f"0, {FIRST}, 00000000:06:00.0, Disabled, Disabled\n1, {SECOND}, 00000000:09:00.0, Disabled, Disabled\n"
MAJORS = "Character devices:\n1 mem\n5 tty\n195 nvidia-frontend\n510 nvidia-uvm\nBlock devices:\n8 sd\n"
CONTEXT = {"boot_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "mount_namespace": "mnt:[123]", "dev_device": 12, "dev_inode": 13}


def facts(csv=CSV):
    cards = inventory.parse_smi(csv)
    cards = [{**c, "minor": {FIRST: 3, SECOND: 7}[c["uuid"]]} for c in cards]
    pairs = {name: (major, minor) for name, major, minor in inventory.CPU_DEVICES}
    pairs.update(nvidiactl=(195, 255), **{"nvidia-uvm": (510, 0)})
    pairs.update({"nvidia" + str(c["minor"]): (195, c["minor"]) for c in cards})
    nodes = {name: inventory.DeviceNode(name, 12, n + 100, major, minor) for n, (name, (major, minor)) in enumerate(pairs.items())}
    information = {c["pci"]: f"Model: example\nGPU UUID: {c['uuid']}\nDevice Minor: {c['minor']}\n" for c in cards}
    return nodes, information


def captured(csv=CSV):
    nodes, information = facts(csv)
    return inventory.from_facts(csv, csv, MAJORS, nodes, information, CONTEXT)


def reservation(index=1, uuid=SECOND):
    return dict(gpu_id=index, gpu_uuid=uuid, simulated=False, topology_status="recorded_sample", topology_observed_at=99)


class DeviceInventoryTests(unittest.TestCase):
    def test_index_is_not_minor_and_only_selected_gpu_is_allowed(self):
        result = captured()
        self.assertEqual(7, result["cards"][1]["minor"])
        policy = inventory.select_policy(result, [reservation()], now=100)
        allowed = {(r.major, r.minor, r.access) for r in policy.rules}
        self.assertIn((195, 7, 6), allowed)
        self.assertNotIn((195, 1, 6), allowed)
        self.assertNotIn((195, 3, 6), allowed)
        self.assertIn((195, 255, 6), allowed)
        self.assertIn((510, 0, 6), allowed)
        self.assertNotIn((510, 1, 6), allowed)  # tools/capabilities never guessed
        self.assertTrue(all(rule.access == 6 for rule in policy.rules))

    def test_cpu_only_excludes_all_gpu_shared_control_and_uvm_nodes(self):
        policy = inventory.select_policy(captured(), [], now=100)
        self.assertEqual({(major, minor) for _, major, minor in inventory.CPU_DEVICES}, {(r.major, r.minor) for r in policy.rules})

    def test_complete_bracket_and_unique_identities_are_required(self):
        nodes, information = facts()
        for after in (CSV.replace(SECOND, FIRST), CSV.replace("1, " + SECOND, "0, " + SECOND),
                      CSV.replace("09:00.0", "06:00.0"), CSV.replace(SECOND, "MIG-example"),
                      CSV.replace("1, " + SECOND, "2, " + SECOND), CSV.replace("Disabled, Disabled", "Enabled, Disabled")):
            with self.subTest(after=after), self.assertRaises(ValueError):
                inventory.from_facts(CSV, after, MAJORS, nodes, information, CONTEXT)

    def test_driver_information_and_device_identity_are_independently_verified(self):
        nodes, information = facts()
        cases = [(MAJORS.replace("510 nvidia-uvm", "511 nvidia-uvm"), nodes, information),
                 (MAJORS + "Character devices:\n196 nvidia\n", nodes, information),
                 (MAJORS, {k: v for k, v in nodes.items() if k != "nvidia7"}, information),
                 (MAJORS, {**nodes, "nvidia7": inventory.DeviceNode("nvidia7", 12, 200, 195, 1)}, information),
                 (MAJORS, nodes, {**information, "0000:09:00.0": "GPU UUID: " + FIRST + "\nDevice Minor: 7\n"}),
                 (MAJORS, nodes, {**information, "0000:09:00.0": information["0000:09:00.0"].replace("Minor: 7", "Minor: 3")}),
                 (MAJORS, nodes, {**information, "0000:09:00.0": information["0000:09:00.0"] + "GPU UUID: " + SECOND + "\n"})]
        for major_text, device_nodes, info in cases:
            with self.assertRaises(ValueError):
                inventory.from_facts(CSV, CSV, major_text, device_nodes, info, CONTEXT)

    def test_missing_unknown_simulated_stale_or_changed_reservation_never_grants_policy(self):
        for patch in ({"gpu_id": True}, {"gpu_id": 9}, {"gpu_uuid": FIRST}, {"simulated": True}, {"simulated": 0},
                      {"topology_status": "unknown"}, {"topology_observed_at": 94}, {"topology_observed_at": 101},
                      {"topology_observed_at": float("nan")}, {"topology_observed_at": None}):
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                inventory.select_policy(captured(), [{**reservation(), **patch}], now=100)
        with self.assertRaises(ValueError):
            inventory.select_policy(captured(), [reservation(), reservation()], now=100)

    def test_mig_enabled_pending_or_unknown_is_not_interpreted_as_full_gpu(self):
        for values in ("Enabled, Enabled", "Disabled, Enabled", "N/A, N/A", "[N/A], [N/A]"):
            result = captured(CSV.replace("Disabled, Disabled", values))
            self.assertEqual(6, len(inventory.select_policy(result, [], now=100).rules))
            with self.assertRaises(ValueError):
                inventory.select_policy(result, [reservation()], now=100)

    def test_boundaries_and_invalid_numbers_or_names_are_rejected(self):
        for text in ("", CSV * 129, "x" * (inventory.MAX_TEXT + 1),
                     CSV.replace("1, " + SECOND, "01, " + SECOND), CSV.replace("09:00.0", "09:20.0")):
            with self.assertRaises(ValueError):
                inventory.parse_smi(text)
        for minor in ("255", "01", "-1", "unknown"):
            with self.assertRaises(ValueError):
                inventory.verify_information("GPU UUID: " + FIRST + "\nDevice Minor: " + minor, {"uuid": FIRST})
        for args in (("../null", 1, 2, 1, 3), ("null", True, 2, 1, 3), ("null", 1, 0, 1, 3), ("null", 1, 2, -1, 3)):
            with self.assertRaises(ValueError):
                inventory.DeviceNode(*args)
        for patch in ({"boot_id": "unknown"}, {"mount_namespace": "mnt:[]"}, {"dev_inode": True}):
            nodes, information = facts()
            with self.assertRaises(ValueError):
                inventory.from_facts(CSV, CSV, MAJORS, nodes, information, {**CONTEXT, **patch})

    def test_serialized_inventory_is_strict_and_original_input_is_not_mutated(self):
        original = captured()
        before = copy.deepcopy(original)
        inventory.select_policy(json.loads(json.dumps(original)), [reservation()], now=100)
        self.assertEqual(before, original)
        for patch in ({"extra": True}, {"interface_version": "unknown"}, {"cards": []}, {"nodes": {}}):
            with self.assertRaises((ValueError, KeyError, TypeError)):
                inventory.select_policy({**original, **patch}, [], now=100)
        for now in (True, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                inventory.select_policy(original, [], now=now)


class DeviceCaptureModels(unittest.TestCase):
    def setUp(self):
        self.nodes, self.information = facts()
        self.descriptors = {n: 20 + i for i, n in enumerate(self.nodes)}
        self.closed = []
        self.infos = {fd: SimpleNamespace(st_mode=stat.S_IFCHR, st_dev=node.device, st_ino=node.inode,
                          st_rdev=(node.major, node.minor)) for n, fd in self.descriptors.items() for node in [self.nodes[n]]}
        self.root = SimpleNamespace(st_mode=stat.S_IFDIR, st_dev=12, st_ino=13)
        real_stat = os.stat
        def node_stat(name, **kwargs):
            if name == "/dev":
                return self.root
            if name in self.descriptors:
                return self.infos[self.descriptors[name]]
            return real_stat(name, **kwargs)
        def read(path):
            if path.endswith("/boot_id"):
                return CONTEXT["boot_id"] + "\n"
            if path == "/proc/devices":
                return MAJORS
            return self.information[path.split("/")[-2]]
        for target, options in (("O_PATH", {"new": getattr(os, "O_PATH", 0x200000), "create": True}),
            ("open", {"side_effect": lambda path, flags, **kw: 10 if path == "/dev" else self.descriptors[path]}),
            ("fstat", {"side_effect": lambda fd: self.root if fd == 10 else self.infos[fd]}),
            ("stat", {"side_effect": node_stat}),
            ("major", {"side_effect": lambda value: value[0]}), ("minor", {"side_effect": lambda value: value[1]}),
            ("readlink", {"return_value": CONTEXT["mount_namespace"]}), ("close", {"side_effect": self.closed.append})):
            patch = mock.patch.object(inventory.os, target, **options)
            patch.start()
            self.addCleanup(patch.stop)
        self.read_patch = mock.patch.object(inventory, "_read", side_effect=read)
        self.read_mock = self.read_patch.start()
        self.addCleanup(self.read_patch.stop)

    def test_capture_uses_only_nonfollowing_path_descriptors_and_closes_all(self):
        with mock.patch.object(inventory, "_query", return_value=CSV) as query:
            self.assertEqual(captured(), inventory.capture())
        self.assertEqual(2, query.call_count)
        self.assertEqual({10, *self.descriptors.values()}, set(self.closed))
        self.assertEqual(len(self.closed), len(set(self.closed)))
        for call in inventory.os.open.call_args_list:
            self.assertTrue(call.args[1] & inventory.os.O_PATH)
            self.assertTrue(call.args[1] & inventory.os.O_NOFOLLOW)
            if call.args[0] != "/dev":
                self.assertEqual(10, call.kwargs["dir_fd"])

    def test_symlink_or_nondevice_refused_and_descriptors_closed(self):
        self.infos[self.descriptors["nvidia7"]].st_mode = stat.S_IFLNK
        with mock.patch.object(inventory, "_query", return_value=CSV), self.assertRaises(ValueError):
            inventory.capture()
        self.assertIn(10, self.closed)
        self.assertIn(self.descriptors["nvidia7"], self.closed)

    def test_node_root_or_namespace_change_refuses_capture(self):
        replaced = SimpleNamespace(st_mode=stat.S_IFDIR, st_dev=12, st_ino=14)
        with mock.patch.object(inventory, "_query", return_value=CSV), \
                mock.patch.object(inventory.os, "stat", return_value=replaced), self.assertRaises(ValueError):
            inventory.capture()
        self.assertIn(10, self.closed)

    def test_partial_failure_never_returns_inventory(self):
        self.read_mock.side_effect = OSError("unreadable")
        with mock.patch.object(inventory, "_query", return_value=CSV), self.assertRaises(OSError):
            inventory.capture()
        self.assertEqual([10], self.closed)

    def test_node_namespace_or_driver_information_drift_cannot_pass_bracket(self):
        node = self.nodes["nvidia7"]
        changed = SimpleNamespace(st_mode=stat.S_IFCHR, st_dev=node.device, st_ino=node.inode + 1, st_rdev=(node.major, node.minor))
        with mock.patch.object(inventory, "_query", return_value=CSV), \
                mock.patch.object(inventory.os, "stat", side_effect=lambda name, **kw:
                    self.root if name == "/dev" else changed if name == "nvidia7" else self.infos[self.descriptors[name]]), \
                self.assertRaises(ValueError):
            inventory.capture()
        with mock.patch.object(inventory, "_query", return_value=CSV), \
                mock.patch.object(inventory.os, "readlink", side_effect=["mnt:[123]", "mnt:[124]"]), self.assertRaises(ValueError):
            inventory.capture()
        read = self.read_mock.side_effect
        seen = []
        def shifted(path):
            seen.append(path)
            text = read(path)
            return text + "\n" if path == "/proc/devices" and seen.count(path) > 1 else text
        self.read_mock.side_effect = shifted
        with mock.patch.object(inventory, "_query", return_value=CSV), self.assertRaises(ValueError):
            inventory.capture()

    def test_expired_or_unreaped_helper_does_not_spawn_another(self):
        with mock.patch.object(inventory.subprocess, "Popen") as spawn, \
                mock.patch.object(inventory, "_pending_query", None), self.assertRaises(TimeoutError):
            inventory._query(time.monotonic() - 1)
        spawn.assert_not_called()
        previous = mock.Mock()
        previous.poll.return_value = None
        with mock.patch.object(inventory.subprocess, "Popen") as spawn, \
                mock.patch.object(inventory, "_pending_query", previous), self.assertRaises(TimeoutError):
            inventory._query(time.monotonic() + 2)
        spawn.assert_not_called()
        previous.wait.assert_not_called()


class DeviceInventoryCliTests(unittest.TestCase):
    def test_compute_only_early_dispatch_does_not_open_or_migrate_database(self):
        with mock.patch.object(cli, "load_config", return_value={"node": "example"}), \
                mock.patch.object(cli, "_is_foreign_host", return_value=False), mock.patch.object(cli.sys, "platform", "linux"), \
                mock.patch.object(state, "connect", side_effect=AssertionError("DB read")), \
                mock.patch.object(state, "init_db", side_effect=AssertionError("migration")), \
                mock.patch.object(inventory, "capture", return_value=captured()), mock.patch("builtins.print") as printed:
            self.assertEqual(0, cli.main(["device-inventory", "--json"]))
        result = json.loads(printed.call_args.args[0])
        self.assertEqual("sched-device-inventory-v1", result["contract"])
        self.assertTrue(result["runtime_probed"])
        for key in ("admission_granted", "wait_authority_granted", "physical_boundary_verified"):
            self.assertFalse(result[key])

    def test_gateway_guard_cannot_be_bypassed_by_foreign_write_override(self):
        with mock.patch.object(cli, "load_config", return_value={"node": "example"}), \
                mock.patch.object(cli, "_is_foreign_host", return_value=True), mock.patch.dict(os.environ, SCHED_ALLOW_FOREIGN_WRITE="1"), \
                mock.patch.object(inventory, "capture") as capture, mock.patch("builtins.print"):
            self.assertEqual(1, cli.main(["device-inventory", "--json"]))
        capture.assert_not_called()

    def test_unreadable_probe_returns_failure_not_empty_or_success(self):
        with mock.patch.object(cli, "load_config", return_value={"node": "example"}), \
                mock.patch.object(cli, "_is_foreign_host", return_value=False), mock.patch.object(cli.sys, "platform", "linux"), \
                mock.patch.object(inventory, "capture", side_effect=subprocess.TimeoutExpired("nvidia-smi", 5)), mock.patch("builtins.print"):
            self.assertEqual(1, cli.main(["device-inventory", "--json"]))


@unittest.skipUnless(sys.platform == "linux", "bounded helper execution requires Linux compute")
class BoundedDeviceQueryTests(unittest.TestCase):
    def helper(self, code, deadline):
        real_popen = subprocess.Popen
        children = []
        def launch(command, **kwargs):
            self.assertEqual("nvidia-smi", command[0])
            process = real_popen([sys.executable, "-I", "-c", code], **kwargs)
            children.append(process)
            return process
        with mock.patch.object(inventory.subprocess, "Popen", side_effect=launch):
            try:
                return inventory._query(time.monotonic() + deadline)
            finally:
                self.assertIsNotNone(children[0].returncode)
                self.assertTrue(children[0].stdout.closed)

    def test_bounded_helper_reads_actual_cpu_child_and_reaps_it(self):
        self.assertEqual("example\n", self.helper("print('example')", 2))

    def test_output_overflow_reaps_child_without_returning_partial_success(self):
        with self.assertRaises(ValueError):
            self.helper("import os; os.write(1,b'x'*100000)", 2)

    def test_deadline_reaps_exact_child_without_retry(self):
        with self.assertRaises(TimeoutError):
            self.helper("import time; time.sleep(5)", .1)


if __name__ == "__main__":
    unittest.main()
