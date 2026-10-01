"""Deployment checks detect drift without executing administrator programs."""
import errno
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from gsched.execution import preflight


@unittest.skipUnless(sys.platform == "linux", "Linux administrator file preflight")
class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.executable = self.root / "worker"
        self.executable.write_bytes(b"\x7fELF" + b"reviewed payload")
        self.executable.chmod(0o600)
        self.profile = {"kind": "linux_fd", "executable": str(self.executable),
                        "sha256": hashlib.sha256(self.executable.read_bytes()).hexdigest(),
                        "argv": ["worker"], "env": {}, "projects": ["text"], "input_slots": {}}
        self.cfg = {"projects": {"text": {"root": str(self.root)}},
                    "execution_backends": {"worker": self.profile}}

    def checks(self):
        return preflight.deployment_checks(self.cfg)

    def reason(self):
        return self.checks()[0]["reason"]

    def test_checks_are_passive_identified_and_do_not_require_original_execute_bits(self):
        with mock.patch("subprocess.Popen", side_effect=AssertionError("child born")), mock.patch(
            "gsched.state.connect", side_effect=AssertionError("DB opened")
        ), mock.patch("gsched.execution_policy.sealed_bytes", side_effect=AssertionError("input sealed")):
            checks = self.checks()
        self.assertTrue(all(c["level"] == "ok" and c["performed"] for c in checks))
        self.assertEqual(["worker", "worker"], [c["subject"] for c in checks])
        self.assertEqual([None, "text"], [c["project"] for c in checks])
        self.assertNotIn(str(self.root), json.dumps(checks))

    def test_missing_symlink_and_nonregular_executables_fail_without_blocking(self):
        self.executable.unlink()
        self.assertEqual("executable_missing", self.reason())
        self.executable.symlink_to("absent")
        self.assertEqual("executable_symlink", self.reason())
        self.executable.unlink()
        self.executable.mkdir()
        self.assertEqual("executable_not_regular", self.reason())
        self.executable.rmdir()
        os.mkfifo(self.executable)
        self.assertEqual("executable_not_regular", self.reason())

    def test_digest_and_elf_are_separate_checks(self):
        self.executable.write_bytes(b"\x7fELFdrift")
        self.assertEqual("executable_digest_mismatch", self.reason())
        self.profile["sha256"] = hashlib.sha256(b"not ELF").hexdigest()
        self.executable.write_bytes(b"not ELF")
        self.assertEqual("executable_not_elf", self.reason())

    def test_bounded_read_and_file_change_during_hash_are_rejected(self):
        with mock.patch.object(preflight, "MAX_EXECUTABLE_BYTES", 4):
            self.assertEqual("executable_too_large", self.reason())
        original = os.read
        def read(descriptor, size):
            data = original(descriptor, size)
            if data:
                self.executable.write_bytes(b"changed size")
            return data
        with mock.patch.object(preflight.os, "read", side_effect=read):
            self.assertEqual("executable_changed", self.reason())

    def test_permission_errors_are_stable_and_do_not_leak_exception_paths(self):
        with mock.patch.object(preflight.os, "open", side_effect=PermissionError(errno.EACCES, "private path")):
            checks = self.checks()
        self.assertEqual("executable_unreadable", checks[0]["reason"])
        self.assertEqual("project_root_unreadable", checks[1]["reason"])
        self.assertNotIn("private path", json.dumps(checks))
        with mock.patch.object(preflight.os, "access", return_value=False):
            self.assertEqual("project_root_unsearchable", self.checks()[1]["reason"])

    def test_roots_are_normalized_like_launch_and_missing_or_non_directory_is_reported(self):
        self.cfg["projects"]["text"]["root"] = str(self.root / "alias")
        (self.root / "alias").symlink_to(self.root, target_is_directory=True)
        self.assertIsNone(self.checks()[1]["reason"])
        self.cfg["projects"]["text"]["root"] = str(self.executable)
        self.assertEqual("project_root_not_directory", self.checks()[1]["reason"])
        self.cfg["projects"]["text"]["root"] = str(self.root / "absent")
        self.assertEqual("project_root_missing", self.checks()[1]["reason"])

    def test_each_profile_is_checked_and_descriptors_are_closed(self):
        self.cfg["execution_backends"]["second"] = dict(self.profile, sha256="0" * 64)
        before = set(os.listdir("/proc/self/fd"))
        checks = self.checks()
        self.assertEqual(before, set(os.listdir("/proc/self/fd")))
        executable = {c["subject"]: c["reason"] for c in checks if c["id"] == "execution_executable"}
        self.assertEqual({"second": "executable_digest_mismatch", "worker": None}, executable)

    def test_unsupported_platform_does_not_probe_paths(self):
        with mock.patch.object(preflight.sys, "platform", "win32"), mock.patch.object(
            preflight.os, "open", side_effect=AssertionError("opened file")
        ):
            checks = self.checks()
        self.assertTrue(all(not c["performed"] and c["reason"] == "non_linux" for c in checks))
