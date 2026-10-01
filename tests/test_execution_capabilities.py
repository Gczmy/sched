"""Capability evidence and structured checks never invent execution authority."""
import argparse
import hashlib
import json
import os
import sys
from unittest import mock

from gsched import cli, daemon, state
from gsched.execution import BackendUnavailable
from gsched.execution import capabilities
from pathlib import Path
from test_review_cli_state import TempStateCase


class CapabilityTests(TempStateCase):
    def test_real_linux_owner_primitives(self):
        if sys.platform != "linux":
            self.skipTest("Linux owner primitives")
        capabilities._owner_primitives()

    def test_invalid_process_identity_is_unavailable(self):
        with mock.patch.object(capabilities.LinuxFdBackend, "__init__", return_value=None), mock.patch.object(
            capabilities, "boot_id", return_value="invalid"
        ):
            self.assertEqual("unavailable", capabilities.snapshot()["backends"]["linux_fd_owner"]["status"])

    def test_missing_native_and_unknown_probe_never_verify_declared_features(self):
        for error, status, reason in ((BackendUnavailable("absent", reason="native_unavailable"), "unavailable", "native_unavailable"),
                                      (RuntimeError("private diagnostic"), "unknown", "probe_failed")):
            with mock.patch.object(capabilities.LinuxFdBackend, "__init__", side_effect=error), mock.patch.object(
                capabilities, "_owner_primitives", side_effect=AssertionError("probed unavailable owner")
            ):
                result = capabilities.snapshot()
            for name in ("linux_fd", "linux_fd_owner"):
                backend = result["backends"][name]
                self.assertEqual(status, backend["status"])
                self.assertEqual(reason, backend["reason"])
                self.assertEqual([], backend["verified"])
                self.assertTrue(backend["declared"])
            self.assertNotIn("private diagnostic", json.dumps(result))

    def test_owner_primitives_required_without_birth_or_service_reconnect(self):
        with mock.patch.object(capabilities.LinuxFdBackend, "__init__", return_value=None):
            with mock.patch.object(capabilities, "_owner_primitives", side_effect=OSError("blocked")):
                result = capabilities.snapshot()
            self.assertEqual("available", result["backends"]["linux_fd"]["status"])
            self.assertEqual("unavailable", result["backends"]["linux_fd_owner"]["status"])
            with mock.patch.object(capabilities, "_owner_primitives", return_value=None):
                result = capabilities.snapshot()
            self.assertIn("persistent_owner_v1", result["backends"]["linux_fd_owner"]["verified"])

    def test_capabilities_cli_does_not_read_config_or_database(self):
        with mock.patch("gsched.config.load_config", side_effect=AssertionError("read config")), mock.patch.object(
            state, "connect", side_effect=AssertionError("opened DB")
        ), mock.patch.object(state, "init_db", side_effect=AssertionError("initialized DB")):
            code, text, error = self.capture(cli.main, ["capabilities", "--json"])
        self.assertEqual(0, code, error)
        self.assertEqual("execution_capabilities", json.loads(text)["query"])

    def test_json_check_preserves_failure_exit_and_warning_semantics(self):
        for levels, code in ((["ok", "warn"], 0), (["ok", "fail"], 1)):
            checks = [{"id": "test", "item": "check", "subject": None, "performed": True,
                       "detail": "detail", "level": level} for level in levels]
            with mock.patch.object(daemon, "check", return_value=checks):
                args = argparse.Namespace(action="check", json=True, fake=False)
                result, text, error = self.capture(cli.cmd_daemon, args)
            self.assertEqual(code, result, error)
            value = json.loads(text)
            self.assertEqual("daemon_check", value["query"])
            self.assertEqual(code == 0, value["passed"])
            self.assertEqual(checks, value["checks"])
            self.assertEqual(levels.count("warn"), value["summary"]["warn"])

    def test_foreign_json_check_cannot_bypass_compute_node_guard(self):
        with mock.patch.dict(os.environ, {"SCHED_ALLOW_FOREIGN_WRITE": ""}), mock.patch.object(
            cli, "_is_foreign_host", return_value=True
        ), mock.patch.object(daemon, "check", side_effect=AssertionError("check ran on gateway")):
            code, _, error = self.capture(cli.main, ["daemon", "check", "--json"])
        self.assertEqual(2, code)
        self.assertIn("计算节点", error)

    def test_existing_checks_have_stable_ids_and_simulation_markers(self):
        with mock.patch("getpass.getuser", return_value="test"):
            checks = daemon.check(fake=True)
        self.assertTrue(all(set(c) == {"id", "subject", "performed", "item", "detail", "level"} for c in checks))
        gpu = next(c for c in checks if c["id"] == "gpu_probe")
        self.assertFalse(gpu["performed"])
        self.assertFalse(next(c for c in checks if c["id"] == "project_git")["performed"])

    def test_daemon_checks_only_configured_backend_requirements(self):
        self.cfg["execution_backends"] = {"worker": {"kind": "linux_fd_owner"}}
        probes = {"linux_fd": {"status": "available", "reason": None, "verified": ["fd_exec_v1"]},
                  "linux_fd_owner": {"status": "unavailable", "reason": "owner_primitives_unavailable", "verified": []}}
        with mock.patch.object(daemon, "load_config", return_value=self.cfg), mock.patch.object(
            capabilities, "snapshot", return_value={"backends": probes}
        ), mock.patch("gsched.execution.preflight.deployment_checks", return_value=[]):
            checks = daemon.check(fake=True)
        execution = [c for c in checks if c["id"] == "execution_backend"]
        self.assertEqual(1, len(execution))
        self.assertEqual("linux_fd_owner", execution[0]["subject"])
        self.assertEqual("fail", execution[0]["level"])

    def test_daemon_json_includes_each_registered_executable_and_root(self):
        executable = os.path.join(self.tmp.name, "worker")
        Path(executable).write_bytes(b"\x7fELFpreflight fixture")
        self.cfg["execution_backends"] = {"worker": {
            "kind": "linux_fd", "executable": executable,
            "sha256": hashlib.sha256(Path(executable).read_bytes()).hexdigest(),
            "argv": ["worker"], "env": {}, "projects": ["p"], "input_slots": {},
        }}
        probes = {"linux_fd": {"status": "available", "reason": None, "verified": ["fd_exec_v1"]}}
        with mock.patch.object(daemon, "load_config", return_value=self.cfg), mock.patch.object(
            capabilities, "snapshot", return_value={"backends": probes}
        ), mock.patch("getpass.getuser", return_value="test"):
            code, output, _ = self.capture(cli.cmd_daemon, argparse.Namespace(action="check", json=True, fake=True))
            value = json.loads(output)
            deployment = [c for c in value["checks"] if c["id"] in {"execution_executable", "execution_project_root"}]
            self.assertEqual(0, code)
            self.assertTrue(all(c["reason"] is None and c["performed"] for c in deployment))
            self.assertEqual(2, len(deployment))
            Path(executable).write_bytes(b"\x7fELFdrift")
            code, output, _ = self.capture(cli.cmd_daemon, argparse.Namespace(action="check", json=True, fake=True))
        self.assertEqual(1, code)
        failed = next(c for c in json.loads(output)["checks"] if c["id"] == "execution_executable")
        self.assertEqual("worker", failed["subject"])
        self.assertEqual("executable_digest_mismatch", failed["reason"])
