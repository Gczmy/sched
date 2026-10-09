"""Pure scope failure/recovery model and opt-in delegated Linux acceptance.

Synthetic cgroup files never prove kernel isolation. Real positive cases require
an explicitly supplied private cpuset subtree; the tests do not delegate one or
modify its parents. Execution cases belong on compute nodes / Linux CI only.
"""
import errno
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

from gsched.execution import (BackendUnavailable, CpuScopeBinding, CpuScopeIntent, DelegatedCpuScopes,
    ExecutionEnvelope, LaunchConstraints, LinuxFdBackend, ScopeUnavailable, SubprocessBackend)
from gsched.execution import scopes


class ScopeValueTests(unittest.TestCase):
    def test_bounded_cpu_and_memory_range_parser(self):
        self.assertEqual((0, 1, 2, 4), scopes._indices("0-2,4"))
        for value in ("", "1,1", "2-1", "1,0", "01", "0-1048575", "-1", "0,", "a", "1048576", "1\n2"):
            with self.subTest(value=value), self.assertRaises(ScopeUnavailable):
                scopes._indices(value)

    def test_nonlinux_does_not_degrade_to_affinity_or_regular_directory(self):
        with mock.patch.object(scopes.sys, "platform", "darwin"), self.assertRaises(ScopeUnavailable) as result:
            DelegatedCpuScopes("/tmp/example")
        self.assertEqual("scope_requires_linux", result.exception.reason)

    def test_explicit_paths_reject_relative_parent_traversal_and_aliases(self):
        for value in ("/", "tmp", "/tmp/../other", "/tmp//other", "/tmp/", "//tmp", "/tmp/./x", "/tmp/\0x", "~/.cgroup"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                scopes._open_directory(value)


class ScopeModelTests(unittest.TestCase):
    """Regular fixture files + mocked cgroup magic, not kernel evidence."""
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.controls(self.root, parent=True)
        self.fds = []
        self.real_mkdir = os.mkdir
        for target, options in (
            ("gsched.execution.scopes._context", {"return_value": ("a" * 36, "mnt:[123]", os.geteuid())}),
            ("gsched.execution.scopes._filesystem_magic", {"return_value": 0x63677270}),
            ("gsched.execution.scopes.os.sched_getaffinity", {"create": True, "return_value": {0, 1, 2, 3}}),
            ("gsched.execution.scopes.os.access", {"return_value": True}),
            ("gsched.execution.scopes.os.readlink", {"side_effect": self.fd_path}),
        ):
            patch = mock.patch(target, **options)
            patch.start()
            self.addCleanup(patch.stop)
        self.manager = DelegatedCpuScopes(str(self.root))
        self.addCleanup(self.manager.close)

    @staticmethod
    def controls(path, *, parent=False):
        values = {"cgroup.type": "domain", "cgroup.procs": "", "cgroup.events": "populated 0\nfrozen 0\n",
                  "cgroup.subtree_control": "cpuset", "cpuset.cpus": "0-3" if parent else "",
                  "cpuset.cpus.effective": "0-3", "cpuset.mems": "0", "cpuset.mems.effective": "0"}
        for name, value in values.items():
            (path / name).write_text(value)

    def fd_path(self, value):
        number = int(value.rsplit("/", 1)[1])
        inode = os.fstat(number).st_ino
        for path in (self.root, *(p for p in self.root.iterdir() if p.is_dir())):
            if path.stat().st_ino == inode:
                return str(path)
        raise OSError(errno.ENOENT, "fixture retained fd not found")

    def make(self, *, count=1):
        intent = self.manager.intent(tuple(range(count)), "d" * 64)
        def mkdir(name, *, mode, dir_fd):
            self.real_mkdir(name, mode, dir_fd=dir_fd)
            self.controls(self.root / name)
        with mock.patch.object(scopes.os, "mkdir", side_effect=mkdir):
            scope = self.manager.create(intent)
        self.addCleanup(scope.close)
        return scope

    def configure(self, scope, *, drift=False):
        original = scopes._write
        def write(descriptor, name, value):
            original(descriptor, name, value)
            path = Path(self.fd_path("/proc/self/fd/" + str(descriptor)))
            if name == "cpuset.cpus":
                (path / "cpuset.cpus.effective").write_text("3" if drift else value)
        with mock.patch.object(scopes, "_write", side_effect=write):
            return scope.configure()

    def test_intent_binding_roundtrip_exact_and_ambiguous_fields_refused(self):
        scope = self.make()
        value = scope.binding.to_dict()
        self.assertEqual(scope.binding, CpuScopeBinding.from_dict(json.loads(json.dumps(value))))
        for patch in ({"inode": True}, {"extra": 1}, {"device": -1}):
            with self.assertRaises((ValueError, TypeError)):
                CpuScopeBinding.from_dict({**value, **patch})
        for cpus in ([], [True], [1, 0], [0, 0], [1048576]):
            with self.assertRaises(ValueError):
                CpuScopeIntent.from_dict({**value["intent"], "cpus": cpus})

    def test_normalized_cpu_ranges_do_not_invalidate_kernel_configuration(self):
        scope = self.make(count=2)
        self.configure(scope)
        (self.root / scope.binding.intent.name / "cpuset.cpus").write_text("0-1")
        self.assertTrue(scope.observe()["scope_configured"])

    def test_requested_memory_drift_cannot_hide_behind_current_effective_subset(self):
        scope = self.make()
        self.configure(scope)
        (self.root / scope.binding.intent.name / "cpuset.mems").write_text("0-1")
        self.assertFalse(scope.observe()["scope_configured"])
        with self.assertRaises(ScopeUnavailable):
            scope.constraints()

    def test_busy_parent_and_missing_delegation_rejected_before_mkdir(self):
        intent = self.manager.intent((0,), "d" * 64)
        for field, content in (("cgroup.procs", "123"), ("cgroup.subtree_control", "cpu"), ("cgroup.type", "threaded")):
            path = self.root / field
            previous = path.read_text()
            path.write_text(content)
            with mock.patch.object(scopes.os, "mkdir", side_effect=AssertionError("created")), self.assertRaises(ScopeUnavailable):
                self.manager.create(intent)
            path.write_text(previous)

    def test_cpu_and_memory_drift_refused_before_creation(self):
        intent = self.manager.intent((0, 1), "d" * 64)
        (self.root / "cpuset.cpus.effective").write_text("2-3")
        with self.assertRaises(ScopeUnavailable):
            self.manager.create(intent)
        (self.root / "cpuset.cpus.effective").write_text("0-3")
        (self.root / "cpuset.mems.effective").write_text("1")
        with self.assertRaises(ScopeUnavailable):
            self.manager.create(intent)

    def test_original_name_never_reused_and_restore_never_configures_or_grants_launch(self):
        scope = self.make()
        self.configure(scope)
        with self.assertRaises(FileExistsError):
            self.manager.create(scope.binding.intent)
        restored = self.manager.restore(scope.binding)
        self.addCleanup(restored.close)
        with mock.patch.object(scopes, "_write", side_effect=AssertionError("reconfigured")):
            self.assertTrue(restored.observe()["scope_configured"])
            with self.assertRaises(RuntimeError):
                restored.configure()
            with self.assertRaises(RuntimeError):
                restored.constraints()

    def test_configure_failure_consumed_and_retained_not_deleted(self):
        scope = self.make()
        with self.assertRaises(ScopeUnavailable):
            self.configure(scope, drift=True)
        with self.assertRaises(RuntimeError):
            scope.configure()
        self.assertTrue((self.root / scope.binding.intent.name).is_dir())
        with self.assertRaises(RuntimeError):
            scope.constraints()

    def test_restoring_replaced_inode_refuses_without_recreate(self):
        scope = self.make()
        path = self.root / scope.binding.intent.name
        path.rename(self.root / "old")
        path.mkdir(mode=0o700)
        self.controls(path)
        with mock.patch.object(scopes.os, "mkdir", side_effect=AssertionError("recreated")), self.assertRaises(ScopeUnavailable):
            self.manager.restore(scope.binding)
        with self.assertRaises(ScopeUnavailable):
            scope.observe()

    def test_busy_descendant_or_inconsistent_direct_members_retained_without_wait(self):
        scope = self.make()
        self.configure(scope)
        root = self.root / scope.binding.intent.name
        for events, processes in (("populated 1", ""), ("populated 0", "42"), ("frozen 0", ""), ("populated 2", "")):
            (root / "cgroup.events").write_text(events)
            (root / "cgroup.procs").write_text(processes)
            with mock.patch.object(scopes.os, "rmdir", side_effect=AssertionError("deleted")), self.assertRaises((ScopeUnavailable, ValueError)):
                scope.remove()
        self.assertTrue(root.is_dir())

    def test_empty_original_cleanup_is_not_wait_and_does_not_remove_other_scope(self):
        scope = self.make()
        self.configure(scope)
        other = self.make()
        with mock.patch.object(scopes.os, "rmdir") as remove:
            result = scope.remove()
        self.assertEqual(scope.binding.intent.name, remove.call_args.args[0])
        self.assertFalse(result["wait_authority_granted"])
        self.assertTrue((self.root / other.binding.intent.name).is_dir())
        self.assertIsNone(scope._fd)

    def test_observation_is_passive_and_launch_capability_only_issued_once(self):
        scope = self.make()
        self.configure(scope)
        with mock.patch.object(scopes, "_write", side_effect=AssertionError("write")), mock.patch.object(LaunchConstraints, "validate", return_value=None):
            result = scope.observe()
            self.assertFalse(result["admission_granted"] or result["wait_authority_granted"])
            scope.constraints()
            with self.assertRaises(RuntimeError):
                scope.constraints()
        descriptor = scope._procs
        scope.close()
        with self.assertRaises(OSError):
            os.fstat(descriptor)

    def test_kernel_boot_namespace_and_owner_changes_refuse(self):
        scope = self.make()
        for context in (("b" * 36, "mnt:[123]", os.geteuid()), ("a" * 36, "mnt:[999]", os.geteuid()), ("a" * 36, "mnt:[123]", os.geteuid() + 1)):
            with mock.patch.object(scopes, "_context", return_value=context), self.assertRaises(ScopeUnavailable):
                scope.observe()

    def test_world_writable_and_symlink_parent_refused(self):
        self.root.chmod(0o777)
        with self.assertRaises(ScopeUnavailable):
            self.manager.intent((0,), "d" * 64)
        self.root.chmod(0o700)
        link = self.root / "link"
        link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(OSError):
            DelegatedCpuScopes(str(link))


@unittest.skipUnless(sys.platform == "linux", "Linux compute / CI only")
class ScopeLinuxDenialTests(unittest.TestCase):
    def test_regular_directory_never_creates_a_fake_scope(self):
        with tempfile.TemporaryDirectory() as root, self.assertRaises(ScopeUnavailable) as result:
            DelegatedCpuScopes(root)
        self.assertEqual("scope_parent_not_cgroup_v2", result.exception.reason)


@unittest.skipUnless(sys.platform == "linux" and os.environ.get("SCHED_TEST_CPU_SCOPE_ROOT"), "explicit dedicated cpuset delegation required; not kernel acceptance")
class ScopeLinuxAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.manager = DelegatedCpuScopes(os.environ["SCHED_TEST_CPU_SCOPE_ROOT"])
        self.addCleanup(self.manager.close)

    def scope(self):
        available, _ = self.manager._verify()
        cpus = sorted(set(available) & os.sched_getaffinity(0))
        if len(cpus) < 2:
            raise RuntimeError("positive scope acceptance requires two authorized CPUs")
        scope = self.manager.create(self.manager.intent((cpus[0],), "c" * 64))
        self.addCleanup(scope.close)
        self.assertTrue(scope.configure()["scope_configured"])
        return scope

    def execute(self, *, native=False):
        scope = self.scope()
        initial = sorted(os.sched_getaffinity(0))
        program = ("import os,json,subprocess,sys; os.sched_setaffinity(0," + repr(initial) + "); "
                   "child=json.loads(subprocess.check_output([sys.executable,'-I','-c','import os,json; print(json.dumps(sorted(os.sched_getaffinity(0))))'])); "
                   "print(json.dumps([sorted(os.sched_getaffinity(0)),child,open('/proc/self/cgroup').read()]))")
        constraints = scope.constraints()
        with tempfile.TemporaryFile() as output:
            envelope = ExecutionEnvelope((sys.executable, "-I", "-c", program), {})
            if native:
                try:
                    backend = LinuxFdBackend()
                except BackendUnavailable:
                    if os.environ.get("SCHED_REQUIRE_NATIVE") == "1":
                        raise
                    scope.remove()
                    self.skipTest("optional native module unavailable")
                descriptor = os.open(sys.executable, os.O_RDONLY | os.O_CLOEXEC)
                try:
                    prepared = backend.prepare(envelope, executable_fd=descriptor, fd_bindings={1: output.fileno(), 2: output.fileno()}, constraints=constraints)
                finally:
                    os.close(descriptor)
            else:
                prepared = SubprocessBackend().prepare(envelope, stdout_fd=output.fileno(), stderr_fd=output.fileno(), constraints=constraints)
            owner = prepared.launch()
            try:
                observation = owner.wait(10)
                self.assertEqual(0, observation.returncode)
                self.assertTrue(observation.group_clean)
                output.seek(0)
                direct, child, membership = json.loads(output.read())
                self.assertEqual(list(scope.binding.intent.cpus), direct)
                self.assertEqual(direct, child)
                self.assertIn("/" + scope.binding.intent.name, membership)
            finally:
                owner.close()
                prepared.close()
        restored = self.manager.restore(scope.binding)
        try:
            self.assertFalse(restored.observe()["populated"])
            with self.assertRaises(RuntimeError):
                restored.constraints()
            self.assertTrue(restored.remove()["scope_removed"])
        finally:
            restored.close()
        self.assertEqual(initial, sorted(os.sched_getaffinity(0)))
        with self.assertRaises(FileNotFoundError):
            self.manager.restore(scope.binding)

    def test_subprocess_scope_join_caps_affinity_and_descendants(self):
        self.execute()

    def test_native_scope_join_caps_affinity_and_descendants(self):
        self.execute(native=True)

    def test_scheduler_scope_cli_join_and_cleanup(self):
        from run_cpu_cgroup_accept import run
        run(positive=True)


if __name__ == "__main__":
    unittest.main()
