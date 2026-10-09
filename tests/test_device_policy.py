"""Compiler and original-scope fault models, not privileged kernel evidence."""
import errno
import json
import os
from pathlib import Path
import random
import struct
import sys
import unittest
from unittest import mock

from gsched.execution import (BackendUnavailable, DeviceBinding, DeviceIntent,
    DevicePolicy, DeviceRule, DeviceScope)
from gsched.execution import devices
from gsched.execution.scopes import CpuScope, CpuScopeBinding, CpuScopeIntent, ScopeParent


def evaluate(program, kind, major, minor, access):
    instructions = list(struct.iter_unpack("=BBhi", program))
    registers = [0] * 11
    context = {0: (access << 16) | kind, 4: major, 8: minor}
    pc = 0
    for _ in range(len(instructions) + 1):
        code, regs, offset, immediate = instructions[pc]
        dst, src = (regs & 15, regs >> 4) if sys.byteorder == "little" else (regs >> 4, regs & 15)
        if code == 0x61:
            registers[dst] = context[offset]
        elif code == 0xbf:
            registers[dst] = registers[src]
        elif code == 0x77:
            registers[dst] >>= immediate
        elif code == 0x57:
            registers[dst] &= immediate & ((1 << 64) - 1)
        elif code in (0x16, 0x56):
            equal = (registers[dst] & 0xffffffff) == (immediate & 0xffffffff)
            if equal == (code == 0x16):
                pc += offset
        elif code == 0xb7:
            registers[dst] = immediate
        elif code == 0x95:
            return registers[0]
        else:
            raise AssertionError("unexpected opcode")
        pc += 1
        if not 0 <= pc < len(instructions):
            raise AssertionError("jump out of bounds")
    raise AssertionError("program did not terminate")


class DevicePolicyTests(unittest.TestCase):
    def test_native_endian_instruction_and_register_encoding(self):
        for endian, format_code, registers in (("little", "<BBhi", 0x12), ("big", ">BBhi", 0x21)):
            with mock.patch.object(devices.sys, "byteorder", endian):
                program = DevicePolicy(()).compile()
            self.assertEqual((0x61, registers, 0, 0), struct.unpack(format_code, program[:8]))
            self.assertEqual(4, struct.unpack(format_code, program[4 * 8:5 * 8])[2])

    def test_values_and_roundtrip(self):
        policy = DevicePolicy((DeviceRule("char", 1, 3, 6),))
        self.assertEqual(policy, DevicePolicy.from_dict(json.loads(json.dumps(policy.to_dict()))))
        for args in (("gpu", 1, 3, 6), ("char", True, 3, 6), ("char", -1, 3, 6),
                ("char", 1, 2**32, 6), ("char", 1, 3, 0), ("char", 1, 3, True), ("char", 1, 3, 8)):
            with self.subTest(args=args), self.assertRaises(ValueError):
                DeviceRule(*args)
        for rules in ([policy.rules[0]], policy.rules * 2,
                (DeviceRule("char", 2, 0, 7), DeviceRule("char", 1, 0, 7)),
                tuple(DeviceRule("char", 1, i, 7) for i in range(257))):
            with self.assertRaises(ValueError):
                DevicePolicy(rules)
        for patch in ({"default": "allow"}, {"extra": True}, {"rules": [{}]}):
            with self.assertRaises(ValueError):
                DevicePolicy.from_dict({**policy.to_dict(), **patch})

    def test_compiled_program_matches_independent_exact_policy(self):
        rules = (DeviceRule("block", 0xffffffff, 0x80000000, 7),
                 DeviceRule("char", 1, 3, 6), DeviceRule("char", 195, 0, 2))
        policy = DevicePolicy(rules)
        rng = random.Random(9183)
        examples = [(k, r.major, r.minor, a) for r in rules for k in range(4) for a in range(16)]
        examples += [(rng.randrange(4), rng.choice([1, 195, 0xffffffff, 0x80000000]),
            rng.choice([0, 3, 0x80000000]), rng.randrange(0x10000)) for _ in range(1000)]
        for kind, major, minor, access in examples:
            expected = int(any(kind == (1 if r.kind == "block" else 2) and major == r.major
                and minor == r.minor and access != 0 and access & ~r.access == 0 for r in rules))
            self.assertEqual(expected, evaluate(policy.compile(), kind, major, minor, access))
        self.assertEqual(0, evaluate(DevicePolicy(()).compile(), 2, 1, 3, 6))

    def test_maximum_program_is_bounded_and_jumps_terminate(self):
        policy = DevicePolicy(tuple(DeviceRule("char", 1, n, 7) for n in range(256)))
        self.assertEqual(2060 * 8, len(policy.compile()))
        self.assertEqual(1, evaluate(policy.compile(), 2, 1, 255, 7))
        self.assertEqual(0, evaluate(policy.compile(), 2, 1, 256, 7))
        self.assertEqual(0, evaluate(policy.compile(), 2, 1, 255, 0xffff))


class DeviceScopeModelTests(unittest.TestCase):
    def setUp(self):
        parent = ScopeParent("/example/delegate", 1, 2, "a" * 36, "mnt:[1]", 0)
        scope_intent = CpuScopeIntent(parent, "b" * 32, (0,), (0,), "c" * 64)
        self.binding = CpuScopeBinding(scope_intent, 1, 3)
        self.scope = CpuScope(self.binding, 50, 51, "configured")
        self.intent = DeviceIntent(self.binding, DevicePolicy((DeviceRule("char", 1, 3, 6),)))
        self.query = {"program_ids": [], "attach_flags": 0}
        self.native = mock.Mock()
        self.native.device_program_query.side_effect = lambda fd: dict(self.query)
        self.native.device_program_load.return_value = 52
        self.native.device_program_info.return_value = {"program_id": 71, "program_tag": "d" * 16}
        self.native.device_program_fd.return_value = 53
        self.native.device_program_attach.side_effect = self.attach
        for target, options in (
                ("gsched.execution.devices._native", {"return_value": self.native}),
                ("gsched.execution.devices.os.close", {}),
                ("gsched.execution.scopes.CpuScope._identity", {}),
                ("gsched.execution.scopes.CpuScope.observe", {"return_value":
                    {"scope_configured": True, "populated": False, "direct_process_count": 0}})):
            patch = mock.patch(target, **options)
            patch.start()
            self.addCleanup(patch.stop)

    def attach(self, *args):
        self.query = {"program_ids": [71], "attach_flags": 2}

    def test_serialization_binds_original_scope_policy_and_digest(self):
        value = self.intent.to_dict()
        self.assertEqual(self.intent, DeviceIntent.from_dict(json.loads(json.dumps(value))))
        with self.assertRaises(ValueError):
            DeviceIntent.from_dict({**value, "program_sha256": "0" * 64})
        binding = DeviceBinding(self.intent, 71, "d" * 16)
        self.assertEqual(binding, DeviceBinding.from_dict(binding.to_dict()))
        for number in (0, True, 2**32):
            with self.assertRaises(ValueError):
                DeviceBinding(self.intent, number, "d" * 16)

    def test_persisted_intent_digest_does_not_depend_on_query_host_endianness(self):
        original = self.intent.to_dict()
        other = "big" if sys.byteorder == "little" else "little"
        with mock.patch.object(devices.sys, "byteorder", other):
            restored = DeviceIntent.from_dict(original)
            self.assertEqual(original, restored.to_dict())

    def test_foreign_bytecode_cannot_install_or_fall_back_to_cpu(self):
        other = "big" if sys.byteorder == "little" else "little"
        intent = DeviceIntent(self.binding, self.intent.policy, other)
        handle = DeviceScope(self.scope, intent)
        with self.assertRaises(BackendUnavailable) as refused:
            handle.install()
        self.assertEqual("device_intent_foreign_byteorder", refused.exception.reason)
        self.native.device_program_query.assert_not_called()
        self.native.device_program_load.assert_not_called()
        self.native.device_program_attach.assert_not_called()
        with self.assertRaises(RuntimeError):
            handle.install()
        with self.assertRaises(RuntimeError):
            self.scope.constraints()

    def test_install_once_guard_and_original_attachment_observation(self):
        handle = DeviceScope(self.scope, self.intent)
        with self.assertRaises(BackendUnavailable):
            handle.observe()
        binding = handle.install()
        self.assertEqual(71, binding.program_id)
        self.assertTrue(handle.observe()["device_attachment_verified"])
        self.assertFalse(handle.observe()["wait_authority_granted"])
        with self.assertRaises(RuntimeError):
            handle.install()
        self.query["program_ids"] = [72]
        with self.assertRaises(BackendUnavailable):
            handle.observe()
        # Plain CPU launch cannot bypass the device guard; no procs FD opened.
        with mock.patch("gsched.execution.scopes.os.open") as opened, self.assertRaises(BackendUnavailable):
            self.scope.constraints()
        opened.assert_not_called()

    def test_before_install_plain_cpu_launch_is_consumed_not_fallback(self):
        handle = DeviceScope(self.scope, self.intent)
        with self.assertRaises(BackendUnavailable):
            self.scope.constraints()
        with self.assertRaises(RuntimeError):
            handle.install()
        self.native.device_program_attach.assert_not_called()

    def test_verified_device_guard_precedes_one_launch_fd_grant(self):
        handle = DeviceScope(self.scope, self.intent)
        handle.install()
        with mock.patch("gsched.execution.scopes.os.open", return_value=54) as opened, \
                mock.patch("gsched.execution.scopes.LaunchConstraints.validate", side_effect=lambda: None):
            handle.constraints()
        opened.assert_called_once()
        self.assertEqual("launch_capability_issued", self.scope._phase)
        with self.assertRaises(RuntimeError):
            handle.constraints()
        with self.assertRaises(RuntimeError):
            self.scope.constraints()

    def test_load_failure_and_attach_result_unknown_do_not_retry_or_fallback(self):
        for stage in ("load", "attach", "readback"):
            with self.subTest(stage=stage):
                self.scope = CpuScope(self.binding, 50, 51, "configured")
                self.query = {"program_ids": [], "attach_flags": 0}
                self.native.device_program_load.side_effect = OSError(errno.EPERM, "denied") if stage == "load" else None
                self.native.device_program_attach.side_effect = OSError(errno.EIO, "unknown") if stage == "attach" else self.attach
                if stage == "readback":
                    self.native.device_program_attach.side_effect = lambda *args: None
                handle = DeviceScope(self.scope, self.intent)
                with self.assertRaises((OSError, BackendUnavailable)):
                    handle.install()
                with self.assertRaises(RuntimeError):
                    handle.install()
                with self.assertRaises(RuntimeError):
                    self.scope.constraints()

    def test_existing_policy_not_replaced_or_detached(self):
        self.query["program_ids"] = [99]
        handle = DeviceScope(self.scope, self.intent)
        with self.assertRaises(BackendUnavailable):
            handle.install()
        self.native.device_program_load.assert_not_called()
        self.native.device_program_attach.assert_not_called()

    def test_restore_only_observes_same_inode_program_and_tag(self):
        binding = DeviceScope(self.scope, self.intent).install()
        restored = CpuScope(self.binding, 60, 61, "restored")
        handle = DeviceScope(restored, self.intent, binding=binding)
        self.assertTrue(handle.observe()["device_attachment_verified"])
        for action in (handle.install, handle.constraints, restored.constraints):
            with self.assertRaises(RuntimeError):
                action()
        self.native.device_program_info.return_value["program_tag"] = "e" * 16
        with self.assertRaises(BackendUnavailable):
            handle.observe()

    def test_query_failure_never_interpreted_as_no_policy(self):
        handle = DeviceScope(self.scope, self.intent)
        self.native.device_program_query.side_effect = OSError(errno.EPERM, "denied")
        with self.assertRaises(OSError):
            handle.install()
        self.native.device_program_load.assert_not_called()

    def test_attachment_race_is_unknown_and_never_removes_other_policy(self):
        self.native.device_program_attach.side_effect = lambda *args: self.query.update(
            program_ids=[71, 99], attach_flags=2)
        handle = DeviceScope(self.scope, self.intent)
        with self.assertRaises(BackendUnavailable):
            handle.install()
        self.assertEqual([71, 99], self.query["program_ids"])
        with self.assertRaises(RuntimeError):
            self.scope.constraints()

    def test_empty_cpu_proof_required_before_any_device_effect(self):
        handle = DeviceScope(self.scope, self.intent)
        with mock.patch.object(CpuScope, "observe", return_value={
                "scope_configured": True, "populated": True, "direct_process_count": 0}):
            with self.assertRaises(BackendUnavailable):
                handle.install()
        self.native.device_program_load.assert_not_called()

    def test_original_inode_failure_prevents_program_query(self):
        handle = DeviceScope(self.scope, self.intent)
        handle.install()
        self.native.device_program_query.reset_mock()
        with mock.patch.object(CpuScope, "_identity", side_effect=BackendUnavailable("changed")):
            with self.assertRaises(BackendUnavailable):
                handle.observe()
        self.native.device_program_query.assert_not_called()


@unittest.skipUnless(sys.platform == "linux", "native ABI checks require Linux")
class NativeDeviceValidationTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get("SCHED_TEST_DEVICE_LOAD_DENIAL") == "1",
                         "unprivileged syscall denial requires explicit compute opt-in")
    def test_real_unprivileged_load_denial_does_not_attach(self):
        status = Path("/proc/self/status").read_text()
        effective = int(next(line.split()[1] for line in status.splitlines() if line.startswith("CapEff:")), 16)
        if os.geteuid() == 0 or effective & ((1 << 21) | (1 << 39)):
            self.skipTest("refuse privileged policy load without positive-test authorization")
        native = devices._native()
        try:
            descriptor = native.device_program_load(DevicePolicy(()).compile())
        except OSError as error:
            self.assertIn(error.errno, (errno.EPERM, errno.EACCES))
        else:
            os.close(descriptor)
            self.fail("unexpectedly loaded privileged device program; no attach attempted")

    def test_native_input_bounds_and_regular_directory_refusal(self):
        try:
            native = devices._native()
        except BackendUnavailable:
            if os.environ.get("SCHED_REQUIRE_NATIVE") == "1":
                raise
            self.skipTest("explicit native build unavailable")
        for value in (b"", b"0" * 17, b"0" * (4097 * 8)):
            with self.assertRaises(ValueError):
                native.device_program_load(value)
        for value in (True, -1, 2**64):
            with self.assertRaises((TypeError, ValueError, OverflowError)):
                native.device_program_query(value)
        descriptor = os.open("/tmp", os.O_RDONLY | os.O_DIRECTORY)
        try:
            with self.assertRaises(ValueError):
                native.device_program_query(descriptor)
        finally:
            os.close(descriptor)
