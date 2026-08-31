from __future__ import annotations

import fcntl
import hashlib
import inspect
import os
import socket
import struct
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from gsched.executor import Executor
from gsched.native_exec import native_exec_project_root_identity_sha256
from gsched.native_launch import (
    NATIVE_ACTUAL_ARGV,
    NATIVE_ACTUAL_ENV_ITEMS,
    NATIVE_CONTROL_FD,
    NATIVE_LAUNCH_CHANNEL_REQUEST,
    NATIVE_LAUNCH_FLAGS_NONE,
    NATIVE_LAUNCH_MESSAGE_REQUEST,
    NATIVE_LAUNCH_PLAN_SCHEMA,
    NATIVE_LAUNCH_REQUEST_SEQUENCE,
    NATIVE_LAUNCH_WIRE_HEADER_BYTES,
    NATIVE_LAUNCH_WIRE_MAGIC,
    NATIVE_LAUNCH_WIRE_OUTER_HEADER_BYTES,
    NATIVE_LAUNCH_WIRE_VERSION,
    NATIVE_PROJECT_ROOT_FD,
    NATIVE_REQUEST_FD,
    NativeLaunchPlan,
    NativeLaunchPlanError,
    NativeLaunchUnavailable,
    _create_native_launch_plan,
)


SCHED_ROOT = Path(__file__).resolve().parents[1]
SCHED_TMP = SCHED_ROOT / "tmp"


class NativeLaunchPlanContractTests(unittest.TestCase):
    def test_actual_entry_is_fixed_and_cannot_contain_logical_argv(self) -> None:
        logical = ("/untrusted/python", "-I", "-S", "job.py")

        self.assertEqual("sched_native_launch_plan_v1", NATIVE_LAUNCH_PLAN_SCHEMA)
        self.assertEqual(
            ("m2b-exec-monitor[native-entry-v1]", "--native-entry-v1"),
            NATIVE_ACTUAL_ARGV,
        )
        self.assertEqual((), NATIVE_ACTUAL_ENV_ITEMS)
        self.assertTrue(set(logical).isdisjoint(NATIVE_ACTUAL_ARGV))
        self.assertEqual(
            {3, 4, 5},
            {NATIVE_REQUEST_FD, NATIVE_CONTROL_FD, NATIVE_PROJECT_ROOT_FD},
        )
        self.assertEqual(b"M2BNLC01", NATIVE_LAUNCH_WIRE_MAGIC)
        self.assertEqual(1, NATIVE_LAUNCH_WIRE_VERSION)
        self.assertEqual(1, NATIVE_LAUNCH_CHANNEL_REQUEST)
        self.assertEqual(1, NATIVE_LAUNCH_MESSAGE_REQUEST)
        self.assertEqual(0, NATIVE_LAUNCH_FLAGS_NONE)
        self.assertEqual(0, NATIVE_LAUNCH_REQUEST_SEQUENCE)
        self.assertEqual(4, NATIVE_LAUNCH_WIRE_OUTER_HEADER_BYTES)
        self.assertEqual(20, NATIVE_LAUNCH_WIRE_HEADER_BYTES)
        self.assertEqual(["self", "plan"], list(inspect.signature(Executor.launch_native).parameters))

    def test_direct_plan_construction_requires_private_authority(self) -> None:
        with self.assertRaisesRegex(NativeLaunchPlanError, "internal authority"):
            NativeLaunchPlan(
                object(),
                profile_id="profile-v2",
                profile_sha256="a" * 64,
                project_root_identity_sha256="b" * 64,
                project_root_path="/project",
                logical_submitted_argv=("/bin/true",),
                launcher_sha256="c" * 64,
                request_sha256="d" * 64,
                log_relative_path="logs/native.log",
                owned_fds=(10, 11, 12, 13, 14),
            )

    def test_malformed_metadata_and_aliasing_fail_before_duplication(self) -> None:
        base = {
            "profile_id": "profile-v2",
            "profile_sha256": "a" * 64,
            "project_root_identity_sha256": "b" * 64,
            "project_root_path": "/project",
            "logical_submitted_argv": ["/bin/true"],
            "launcher_sha256": "c" * 64,
            "request_sha256": "d" * 64,
            "log_relative_path": "logs/native.log",
            "launcher_fd": 10,
            "request_fd": 11,
            "control_fd": 12,
            "project_root_fd": 13,
            "log_fd": 14,
        }
        mutations = (
            {"profile_id": "bad profile"},
            {"profile_sha256": "A" * 64},
            {"logical_submitted_argv": ["relative-python"]},
            {"request_fd": True},
            {"control_fd": 10},
            {"log_relative_path": "../outside.log"},
        )
        for mutation in mutations:
            values = dict(base)
            values.update(mutation)
            with self.subTest(mutation=mutation), mock.patch(
                "gsched.native_launch.os.dup"
            ) as duplicate:
                with self.assertRaises(NativeLaunchPlanError):
                    _create_native_launch_plan(**values)
                duplicate.assert_not_called()


@unittest.skipUnless(
    sys.platform.startswith("linux")
    and hasattr(os, "memfd_create")
    and hasattr(fcntl, "F_ADD_SEALS"),
    "requires Linux sealed memfd support",
)
class NativeLaunchPlanLinuxTests(unittest.TestCase):
    def setUp(self) -> None:
        SCHED_TMP.mkdir(parents=True, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(dir=SCHED_TMP)
        self.addCleanup(self.tmp.cleanup)
        self.root = os.path.realpath(self.tmp.name)
        launcher_path = os.path.realpath(sys.executable)
        self.launcher_fd = os.open(
            launcher_path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0),
        )
        with open(launcher_path, "rb") as launcher:
            self.launcher_sha256 = hashlib.sha256(launcher.read()).hexdigest()

        memfd_flags = getattr(os, "MFD_CLOEXEC", 0) | getattr(
            os, "MFD_ALLOW_SEALING", 0
        )
        request_body = b'{"schema":"opaque-native-request-fixture-v1"}'
        wire_header = struct.pack(
            "!8sBBBBII",
            NATIVE_LAUNCH_WIRE_MAGIC,
            NATIVE_LAUNCH_WIRE_VERSION,
            NATIVE_LAUNCH_CHANNEL_REQUEST,
            NATIVE_LAUNCH_MESSAGE_REQUEST,
            NATIVE_LAUNCH_FLAGS_NONE,
            NATIVE_LAUNCH_REQUEST_SEQUENCE,
            len(request_body),
        )
        payload = wire_header + request_body
        self.request = struct.pack("!I", len(payload)) + payload
        request_builder_fd = os.memfd_create("native-request-test", memfd_flags)
        os.write(request_builder_fd, self.request)
        required_seals = (
            fcntl.F_SEAL_SEAL
            | fcntl.F_SEAL_SHRINK
            | fcntl.F_SEAL_GROW
            | fcntl.F_SEAL_WRITE
        )
        fcntl.fcntl(request_builder_fd, fcntl.F_ADD_SEALS, required_seals)
        try:
            self.request_fd = os.open(
                f"/proc/self/fd/{request_builder_fd}",
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0),
            )
        finally:
            os.close(request_builder_fd)

        self.scheduler_channel, self.native_channel = socket.socketpair(
            socket.AF_UNIX, socket.SOCK_STREAM
        )
        self.root_fd = os.open(
            self.root,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0),
        )
        self.log_relative_path = "logs/native.log"
        os.mkdir(os.path.join(self.root, "logs"))
        self.log_path = os.path.join(self.root, self.log_relative_path)
        self.log_fd = os.open(
            self.log_path,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_APPEND
            | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        self.original_fds = (
            self.launcher_fd,
            self.request_fd,
            self.native_channel.fileno(),
            self.root_fd,
            self.log_fd,
        )
        self.addCleanup(self._close_originals)

    def _close_originals(self) -> None:
        self.scheduler_channel.close()
        self.native_channel.close()
        for fd in (self.launcher_fd, self.request_fd, self.root_fd, self.log_fd):
            try:
                os.close(fd)
            except OSError:
                pass

    def plan(self, **overrides):
        values = {
            "profile_id": "frozen-profile-v2",
            "profile_sha256": "a" * 64,
            "project_root_identity_sha256": (
                native_exec_project_root_identity_sha256(self.root)
            ),
            "project_root_path": self.root,
            "logical_submitted_argv": [
                "/opt/reviewed/python",
                "-I",
                "-S",
                "logical-bootstrap.py",
            ],
            "launcher_sha256": self.launcher_sha256,
            "request_sha256": hashlib.sha256(self.request).hexdigest(),
            "log_relative_path": self.log_relative_path,
            "launcher_fd": self.launcher_fd,
            "request_fd": self.request_fd,
            "control_fd": self.native_channel.fileno(),
            "project_root_fd": self.root_fd,
            "log_fd": self.log_fd,
        }
        values.update(overrides)
        return _create_native_launch_plan(**values)

    def test_plan_owns_distinct_copies_and_keeps_fixed_actual_contract(self) -> None:
        plan = self.plan()
        self.addCleanup(plan.close)

        self.assertEqual(NATIVE_LAUNCH_PLAN_SCHEMA, plan.schema)
        self.assertEqual(NATIVE_ACTUAL_ARGV, plan.actual_argv)
        self.assertEqual((), plan.actual_env_items)
        self.assertEqual(
            (
                "/opt/reviewed/python",
                "-I",
                "-S",
                "logical-bootstrap.py",
            ),
            plan.logical_submitted_argv,
        )
        self.assertTrue(set(plan.owned_fds).isdisjoint(self.original_fds))
        plan.validate_live_fds()

    def test_request_copy_has_an_independent_zero_offset(self) -> None:
        plan = self.plan()
        self.addCleanup(plan.close)

        os.lseek(self.request_fd, 7, os.SEEK_SET)
        self.assertEqual(os.lseek(plan.request_fd, 0, os.SEEK_CUR), 0)
        plan.validate_live_fds()

    @unittest.skipUnless(hasattr(os, "O_PATH"), "requires Linux O_PATH")
    def test_project_root_o_path_descriptor_fails_closed(self) -> None:
        path_fd = os.open(
            self.root,
            os.O_PATH | getattr(os, "O_CLOEXEC", 0),
        )
        self.addCleanup(os.close, path_fd)
        with self.assertRaisesRegex(NativeLaunchPlanError, "must not use O_PATH"):
            self.plan(project_root_fd=path_fd)

    def test_log_fd_must_match_one_non_symlink_project_local_path(self) -> None:
        outside_tmp = tempfile.TemporaryDirectory(dir=SCHED_TMP)
        self.addCleanup(outside_tmp.cleanup)
        outside_path = os.path.join(outside_tmp.name, "outside.log")
        outside_fd = os.open(
            outside_path,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        self.addCleanup(os.close, outside_fd)
        with self.assertRaisesRegex(NativeLaunchPlanError, "does not match"):
            self.plan(log_fd=outside_fd)

        symlink_path = os.path.join(self.root, "logs", "symlink.log")
        os.symlink(self.log_path, symlink_path)
        with self.assertRaisesRegex(NativeLaunchPlanError, "must not be a symlink"):
            self.plan(log_relative_path="logs/symlink.log")

        hardlink_path = os.path.join(self.root, "logs", "hardlink.log")
        os.link(self.log_path, hardlink_path)
        with self.assertRaisesRegex(NativeLaunchPlanError, "exactly one"):
            self.plan()

    def test_executor_refuses_backend_without_popen_and_closes_owned_fds(self) -> None:
        plan = self.plan()
        owned = plan.owned_fds

        with mock.patch("gsched.executor.subprocess.Popen") as popen:
            with self.assertRaisesRegex(NativeLaunchUnavailable, "no pathname"):
                Executor().launch_native(plan)

        popen.assert_not_called()
        self.assertTrue(plan.closed)
        for fd in owned:
            with self.assertRaises(OSError):
                os.fstat(fd)
        for fd in self.original_fds:
            os.fstat(fd)


if __name__ == "__main__":
    unittest.main()
