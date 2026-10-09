"""Pure guard/program checks; never open devices or create/attach cgroups."""
import ast
import errno
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from run_device_positive_accept import DevicePositive, authorize, probe_program, verify_probe


class DevicePositiveSourceTests(unittest.TestCase):
    def environment(self):
        return {"SCHED_TEST_DEVICE_ATTACH_AUTHORIZED": "1",
                "SCHED_TEST_CPU_SCOPE_ROOT": "/sys/fs/cgroup/private-example"}

    def test_authorization_before_any_fixture_or_effect(self):
        env = self.environment()
        self.assertEqual(env["SCHED_TEST_CPU_SCOPE_ROOT"], authorize(["--positive-cpu"], env, "linux"))
        for args, environ, platform in (([], env, "linux"), (["--positive-cpu"], {}, "linux"),
                (["--positive-cpu"], env, "darwin"), (["--positive-cpu", "--fake"], env, "linux")):
            with self.subTest(args=args, environment=environ, platform=platform), self.assertRaises(ValueError):
                authorize(args, environ, platform)

    def test_noncanonical_roots_refused(self):
        for root in ("", "/", "relative", "/sys//fs/cgroup/x", "/sys/fs/cgroup/x/", "/sys/fs/cgroup/./x", "/sys/fs/cgroup/../x"):
            with self.subTest(root=root), self.assertRaises(ValueError):
                authorize(["--positive-cpu"], {**self.environment(), "SCHED_TEST_CPU_SCOPE_ROOT": root}, "linux")

    def test_probe_paths_bounded_exact_and_no_kernel_mutation(self):
        tree = ast.parse(probe_program(["nvidia0", "nvidiactl", "nvidia-uvm"]))
        calls = {ast.unparse(node.func) for node in ast.walk(tree) if isinstance(node, ast.Call)}
        self.assertEqual({"os.open", "os.close", "print", "json.dumps"}, calls)
        for names in ([], ["../nvidia0"], ["nvidia/0"], ["nvidia０"], ["nvidia255"], ["nvidia00"], ["nvidia0", "nvidia0"], ["nvidia0"] * 131):
            with self.subTest(names=names), self.assertRaises(ValueError):
                probe_program(names)

    def test_dac_missing_and_external_denials_cannot_count_as_child_policy(self):
        names = ["nvidia0", "nvidiactl", "nvidia-uvm"]
        opened = {name: {"opened": True} for name in ["null", "zero", *names]}
        denied = {**opened, **{name: {"errno": errno.EPERM} for name in names}}
        verify_probe(opened, names, denied=False)
        verify_probe(denied, names, denied=True)
        for result, expected_denial in ((denied, False), (opened, True),
                ({**denied, "nvidia0": {"errno": errno.EACCES}}, True),
                ({**denied, "nvidia0": {"errno": errno.ENOENT}}, True),
                ({**denied, "null": {"errno": errno.EPERM}}, True)):
            with self.subTest(result=result, denied=expected_denial), self.assertRaises(AssertionError):
                verify_probe(result, names, denied=expected_denial)

    def test_private_positive_uses_no_fake_and_only_cpu_workers(self):
        # Synthetic source rendering only, including on platforms without this
        # Linux function; this does not execute the generated worker.
        with tempfile.TemporaryDirectory() as temporary, mock.patch("os.sched_getaffinity", return_value={0, 1}, create=True):
            acceptance = DevicePositive(Path(temporary))
            self.assertEqual((), acceptance.daemon_flags)
            self.assertNotIn("SCHED_FAKE_GPUS", acceptance.env)
            self.assertNotIn("SCHED_ALLOW_FOREIGN_WRITE", acceptance.env)
            acceptance.probe = probe_program(["nvidia0", "nvidiactl", "nvidia-uvm"])
            for task, kind in (("ordinary", None), ("fd", "linux_fd"), ("owner", "linux_fd_owner")):
                worker = acceptance.scoped_worker(task, kind)
                self.assertEqual({"gpu": 0, "cpus": 1}, worker["resources"])
                tree = ast.parse(worker["cmd"][-1])
                self.assertTrue(any(isinstance(n, ast.Call) and ast.unparse(n.func) == "exec" for n in ast.walk(tree)))
                self.assertIn("devices.json", worker["cmd"][-1])
                if kind:
                    self.assertEqual(worker["cmd"], acceptance.cfg["execution_backends"][task]["argv"])

    def test_authorization_is_not_an_implicit_kernel_probe(self):
        with mock.patch("os.open", side_effect=AssertionError("no probe")):
            authorize(["--positive-cpu"], self.environment(), "linux")

    def test_generated_probe_closes_successful_fds_and_preserves_errno(self):
        names = ["nvidia0", "nvidiactl", "nvidia-uvm"]
        # Execute only the generated Python with synthetic open/close, not a
        # device, process, cgroup, backend or scheduler execution.
        for denied in (False, True):
            def opened(path, flags):
                if denied and path.startswith("/dev/nvidia"):
                    raise PermissionError(errno.EPERM, "synthetic denied")
                return 42
            output = io.StringIO()
            with mock.patch("os.open", side_effect=opened), mock.patch("os.close") as close, contextlib.redirect_stdout(output):
                exec(probe_program(names), {})
            verify_probe(json.loads(output.getvalue()), names, denied=denied)
            self.assertEqual(2 if denied else 5, close.call_count)


if __name__ == "__main__":
    unittest.main()
