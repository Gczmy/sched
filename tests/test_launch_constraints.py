"""Launch boundary tests. Real execution runs on Linux compute/CI only."""
import errno
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from gsched.execution import (BackendUnavailable, CONSTRAINTS_VERSION, ExecutionEnvelope,
    LaunchConstraints, LinuxFdBackend, SubprocessBackend, retained_owners)
from gsched.execution.constraints import MAX_CPU_INDEX


class ConstraintValueTests(unittest.TestCase):
    def test_rejects_ambiguous_empty_unsorted_duplicate_and_unbounded_values(self):
        for cpus in ((), [], (True,), ("0",), (-1,), (2, 1), (1, 1), (MAX_CPU_INDEX + 1,)):
            with self.subTest(cpus=cpus), self.assertRaises(ValueError):
                LaunchConstraints(cpu_affinity=cpus)
        for descriptor in (-1, True, "3"):
            with self.assertRaises(ValueError):
                LaunchConstraints(cgroup_procs_fd=descriptor)
        with self.assertRaises(ValueError):
            LaunchConstraints((0,), interface_version="other/v1")

    def test_regular_file_not_an_acceptable_cgroup_control_capability(self):
        with tempfile.TemporaryFile() as stream:
            with mock.patch("gsched.execution.constraints.sys.platform", "linux"), \
                    mock.patch("os.readlink", return_value="/tmp/not-cgroup"), self.assertRaises(ValueError):
                LaunchConstraints(cgroup_procs_fd=stream.fileno()).validate()

    def test_outside_current_affinity_rejected_before_preparation(self):
        with mock.patch("gsched.execution.constraints.sys.platform", "linux"), \
                mock.patch("os.sched_getaffinity", create=True, return_value={1, 2}), \
                mock.patch("gsched.execution.backend.subprocess.Popen", side_effect=AssertionError("launch")):
            with self.assertRaises(BackendUnavailable) as result:
                LaunchConstraints((0,)).validate()
        self.assertEqual("cpu_affinity_unavailable", result.exception.reason)

    def test_nonlinux_never_silently_falls_back(self):
        with mock.patch("gsched.execution.constraints.sys.platform", "darwin"), self.assertRaises(BackendUnavailable):
            LaunchConstraints((0,)).validate()

    def test_cgroup_named_regular_file_is_not_a_kernel_cgroup(self):
        with tempfile.TemporaryFile() as stream:
            with mock.patch("gsched.execution.constraints.sys.platform", "linux"), \
                    mock.patch("os.readlink", return_value="/tmp/example/cgroup.procs"), \
                    mock.patch("gsched.execution.constraints._filesystem_magic", return_value=0), self.assertRaises(ValueError):
                LaunchConstraints(cgroup_procs_fd=stream.fileno()).validate()

    def test_control_fd_cannot_be_exposed_in_child_descriptor_bindings(self):
        with tempfile.TemporaryFile() as stream:
            constraints = LaunchConstraints(cgroup_procs_fd=stream.fileno())
            with mock.patch.object(LaunchConstraints, "validate", return_value=constraints):
                with self.assertRaisesRegex(ValueError, "cannot be passed"):
                    SubprocessBackend().prepare(ExecutionEnvelope(("worker",)), constraints=constraints,
                                                pass_fds=(stream.fileno(),))
                for name in ("stdout_fd", "stderr_fd"):
                    with self.assertRaisesRegex(ValueError, "cannot be passed"):
                        SubprocessBackend().prepare(ExecutionEnvelope(("worker",)), constraints=constraints,
                                                    **{name: stream.fileno()})
                backend = LinuxFdBackend.__new__(LinuxFdBackend)
                backend._module = mock.Mock()
                backend._module.constraints_interface_version = CONSTRAINTS_VERSION
                with self.assertRaisesRegex(ValueError, "cannot be bound"):
                    backend.prepare(ExecutionEnvelope(("worker",)), executable_fd=3,
                                    fd_bindings={8: stream.fileno()}, constraints=constraints)
                backend._module.prepare.assert_not_called()

    def test_old_native_module_default_unchanged_but_constraints_refused(self):
        backend = LinuxFdBackend.__new__(LinuxFdBackend)
        backend._module = mock.Mock(spec=["prepare"])
        envelope = ExecutionEnvelope(("worker",))
        with mock.patch("os.sched_getaffinity", create=True, return_value={0}), \
                mock.patch("gsched.execution.constraints.sys.platform", "linux"):
            prepared = backend.prepare(envelope, executable_fd=3)
            self.assertEqual(5, len(backend._module.prepare.call_args.args))
            prepared.close()
            with self.assertRaises(BackendUnavailable) as result:
                backend.prepare(envelope, executable_fd=3, constraints=LaunchConstraints((0,)))
        self.assertEqual("native_constraints_unavailable", result.exception.reason)


@unittest.skipUnless(sys.platform == "linux", "Linux compute/CI launch boundary")
class SubprocessConstraintTests(unittest.TestCase):
    def setUp(self):
        self.cpu = min(os.sched_getaffinity(0))
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.before = set(retained_owners())

    def finish(self, prepared):
        owner = prepared.launch()
        try:
            observation = owner.wait(10)
            return observation
        finally:
            owner.close()
            prepared.close()
            self.assertEqual(self.before, set(retained_owners()))

    def test_cpu_and_descendant_affinity_applied_before_program_runs(self):
        output = Path(self.root.name) / "result.json"
        program = "import json,os,subprocess,sys; child=subprocess.check_output([sys.executable,'-I','-c','import os; print(sorted(os.sched_getaffinity(0)))'],text=True); open(sys.argv[1],'w').write(json.dumps([sorted(os.sched_getaffinity(0)),json.loads(child)]))"
        envelope = ExecutionEnvelope((sys.executable, "-I", "-c", program, str(output)), {})
        observation = self.finish(SubprocessBackend().prepare(envelope, constraints=LaunchConstraints((self.cpu,))))
        self.assertEqual(0, observation.returncode)
        self.assertEqual([[self.cpu], [self.cpu]], json.loads(output.read_text()))
        self.assertIsNone(observation.launch_error)

    def test_bad_executable_keeps_actual_bootstrap_wait_and_errno(self):
        envelope = ExecutionEnvelope((str(Path(self.root.name) / "absent-worker"),), {})
        observation = self.finish(SubprocessBackend().prepare(envelope, constraints=LaunchConstraints((self.cpu,))))
        self.assertEqual("exited", observation.status)
        self.assertIsNotNone(observation.pid)
        self.assertEqual(127, observation.returncode)
        self.assertEqual(errno.ENOENT, observation.launch_error)
        self.assertTrue(observation.group_clean)

    def test_affinity_drift_after_prepare_is_rejected_by_child_exact_check(self):
        # Preparation can be followed by CPU offline/cgroup changes. Simulate
        # availability disappearing only at the prepare-side probe; the real
        # kernel in the child refuses this impossible CPU before user code.
        absent = MAX_CPU_INDEX
        output = Path(self.root.name) / "should-not-exist"
        envelope = ExecutionEnvelope((sys.executable, "-I", "-c", "open(__import__('sys').argv[1],'w').write('run')", str(output)))
        with mock.patch("os.sched_getaffinity", return_value={absent}):
            prepared = SubprocessBackend().prepare(envelope, constraints=LaunchConstraints((absent,)))
        observation = self.finish(prepared)
        self.assertEqual(127, observation.returncode)
        self.assertEqual(errno.EINVAL, observation.launch_error)
        self.assertFalse(output.exists())

    def test_prepared_close_does_not_leak_control_or_stdio_descriptors(self):
        with tempfile.TemporaryFile() as output:
            prepared = SubprocessBackend().prepare(ExecutionEnvelope(("/bin/true",)), stdout_fd=output.fileno(),
                constraints=LaunchConstraints((self.cpu,)))
            descriptors = (*prepared._owned_stdio, prepared.owner._constraint_error_fd)
            prepared.close()
            for descriptor in descriptors:
                with self.assertRaises(OSError):
                    os.fstat(descriptor)
            os.fstat(output.fileno())
        self.assertEqual(self.before, set(retained_owners()))

    def test_cancel_retains_original_owner_and_does_not_change_parent_mask(self):
        initial = os.sched_getaffinity(0)
        prepared = SubprocessBackend().prepare(ExecutionEnvelope((sys.executable, "-I", "-c", "import time; time.sleep(30)")),
            constraints=LaunchConstraints((self.cpu,)))
        owner = prepared.launch()
        owner.cancel(.01)
        observation = owner.wait(10)
        self.assertNotEqual(0, observation.returncode)
        owner.close()
        prepared.close()
        self.assertEqual(initial, os.sched_getaffinity(0))
        self.assertEqual(self.before, set(retained_owners()))


@unittest.skipUnless(sys.platform == "linux", "Linux compute/CI native boundary")
class NativeConstraintTests(unittest.TestCase):
    def setUp(self):
        try:
            self.backend = LinuxFdBackend()
        except BackendUnavailable as error:
            self.skipTest(str(error))
        self.cpu = min(os.sched_getaffinity(0))

    def test_real_readonly_cgroup_descriptor_refused_without_join(self):
        try:
            descriptor = os.open("/sys/fs/cgroup/cgroup.procs", os.O_RDONLY | os.O_CLOEXEC)
        except FileNotFoundError:
            self.skipTest("cgroup-v2 is not mounted at the standard test path")
        executable = os.open(sys.executable, os.O_RDONLY | os.O_CLOEXEC)
        try:
            with self.assertRaises(ValueError):
                LaunchConstraints(cgroup_procs_fd=descriptor).validate()
            # Check the native interface independently of Python validation.
            with self.assertRaises(ValueError):
                self.backend._module.prepare(executable, ("worker",), (), -1, (), (), descriptor)
        finally:
            os.close(executable)
            os.close(descriptor)

    def test_native_affinity_drift_never_executes_user_program(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "should-not-exist"
            envelope = ExecutionEnvelope((sys.executable, "-I", "-c",
                "open(__import__('sys').argv[1],'w').write('run')", str(output)))
            executable = os.open(sys.executable, os.O_RDONLY | os.O_CLOEXEC)
            try:
                with mock.patch("os.sched_getaffinity", return_value={MAX_CPU_INDEX}):
                    prepared = self.backend.prepare(envelope, executable_fd=executable,
                                                    constraints=LaunchConstraints((MAX_CPU_INDEX,)))
            finally:
                os.close(executable)
            owner = prepared.launch()
            observation = owner.wait(10)
            self.assertEqual(errno.EINVAL, observation.launch_error)
            self.assertNotEqual(0, observation.returncode)
            self.assertFalse(output.exists())
            owner.close()
            prepared.close()

    def test_retained_elf_fd_receives_mask_without_wrapper_or_extra_control_fds(self):
        program = "import json,os; print(json.dumps(sorted(os.sched_getaffinity(0))))"
        with tempfile.TemporaryFile() as output:
            executable = os.open(sys.executable, os.O_RDONLY | os.O_CLOEXEC)
            try:
                prepared = self.backend.prepare(ExecutionEnvelope((sys.executable, "-I", "-c", program)), executable_fd=executable,
                    fd_bindings={1: output.fileno(), 2: output.fileno()}, constraints=LaunchConstraints((self.cpu,)))
            finally:
                os.close(executable)
            owner = prepared.launch()
            observation = owner.wait(10)
            self.assertEqual(0, observation.returncode)
            output.seek(0)
            self.assertEqual([self.cpu], json.loads(output.read()))
            owner.close()
            prepared.close()

    def test_persistent_owner_keeps_original_mask_after_authenticated_reconnect(self):
        from gsched.execution import PersistentLinuxFdBackend, PersistentOwner
        with tempfile.TemporaryFile() as output:
            executable = os.open(sys.executable, os.O_RDONLY | os.O_CLOEXEC)
            root = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            try:
                prepared = PersistentLinuxFdBackend().prepare(ExecutionEnvelope((sys.executable, "-I", "-c", "import json,os,time; print(json.dumps(sorted(os.sched_getaffinity(0))),flush=True); time.sleep(.1)")),
                    executable_fd=executable, cwd_fd=root, fd_bindings={1: output.fileno(), 2: output.fileno()},
                    identity={"attempt_id": "a" * 32},
                    constraints=LaunchConstraints((self.cpu,)), terminal_retention=1)
            finally:
                os.close(executable)
                os.close(root)
            prepared.launch()
            reconnect = PersistentOwner(prepared.owner.binding)
            observation = reconnect.wait(10)
            self.assertEqual(0, observation.returncode)
            output.seek(0)
            self.assertEqual([self.cpu], json.loads(output.read()))
            reconnect.close()
            # Reconnecting preserves child authority but does not replace the
            # caller's original Popen handle for the service itself.
            prepared.owner._process.wait(timeout=10)
            prepared.close()


if __name__ == "__main__":
    unittest.main()
