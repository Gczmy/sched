"""Native integration tests; explicit native build is required to run this file."""
from __future__ import annotations

import errno
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

if sys.platform == "linux":
    import fcntl

from gsched.execution import BackendUnavailable, ExecutionEnvelope, LinuxFdBackend


@unittest.skipUnless(sys.platform == "linux", "Linux native execution")
class LinuxExecutionBackendTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.backend = LinuxFdBackend()
        except BackendUnavailable:
            if os.environ.get("SCHED_REQUIRE_NATIVE") == "1":
                raise
            raise unittest.SkipTest("native backend was not explicitly built")
        compiler = shutil.which("cc") or shutil.which("gcc")
        if compiler is None:
            raise RuntimeError("native tests require a C compiler")
        cls.directory = tempfile.TemporaryDirectory()
        root = Path(cls.directory.name)
        programs = {
            "copy": r'''
#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
int main(void) {
    const char *unexpected = getenv("FORBIDDEN_PARENT_VALUE");
    if (unexpected) return 41;
    const char *probe = getenv("UNRELATED_FD");
    if (probe && fcntl(atoi(probe), F_GETFD) >= 0) return 42;
    char buffer[1024]; ssize_t count = read(3, buffer, sizeof(buffer));
    if (count < 0) return 43;
    for (ssize_t i = 0; i < count; ++i) buffer[i] = (char)toupper((unsigned char)buffer[i]);
    return write(1, buffer, (size_t)count) == count ? 0 : 44;
}
''',
            "numeric": r'''
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
int main(int argc, char **argv) {
    if (argc != 2) return 45;
    int value = atoi(argv[1]);
    printf("%d %ld\n", value * value, (long)getppid());
    return 7;
}
''',
            "ignore": r'''
#include <signal.h>
#include <unistd.h>
int main(void) {
    signal(SIGTERM, SIG_IGN);
    if (write(1, "ready\n", 6) != 6) return 46;
    for (;;) pause();
}
''',
            "family": r'''
#include <errno.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
static volatile sig_atomic_t requested = 0;
static void stop(int number) { (void)number; requested = 1; }
int main(void) {
    signal(SIGTERM, stop);
    pid_t child = fork();
    if (child < 0) return 47;
    if (child == 0) { signal(SIGTERM, SIG_DFL); for (;;) pause(); }
    if (write(1, "ready\n", 6) != 6) return 48;
    while (!requested) pause();
    while (waitpid(child, 0, 0) < 0) if (errno != EINTR) return 49;
    return 0;
}
''',
            "early_exit": r'''
#include <sys/types.h>
#include <unistd.h>
int main(void) {
    pid_t child = fork();
    if (child < 0) return 50;
    if (child == 0) { sleep(1); _exit(0); }
    return 0;
}
''',
        }
        cls.programs = {}
        for name, source in programs.items():
            source_path = root / (name + ".c")
            source_path.write_text(source)
            target = root / name
            subprocess.run([compiler, "-std=c11", "-Wall", "-Wextra", "-Werror", str(source_path), "-o", str(target)], check=True)
            cls.programs[name] = target

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def sealed_program(self, name):
        descriptor = os.memfd_create("execution-test-program", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
        with self.programs[name].open("rb") as source:
            data = source.read()
        os.write(descriptor, data)
        os.fchmod(descriptor, 0o500)
        fcntl.fcntl(descriptor, fcntl.F_ADD_SEALS,
                    fcntl.F_SEAL_WRITE | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_SEAL)
        self.addCleanup(os.close, descriptor)
        return descriptor

    def prepare(self, program, output, *, argv=None, env=None, bindings=None):
        descriptor = self.sealed_program(program)
        slots = {1: output.fileno(), 2: output.fileno()}
        slots.update(bindings or {})
        return self.backend.prepare(ExecutionEnvelope(argv or (program,), env or {}), executable_fd=descriptor, fd_bindings=slots)

    def test_unrelated_copy_program_retains_inputs_and_closes_unlisted_fds(self):
        with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as unrelated:
            os.set_inheritable(unrelated.fileno(), True)
            input_fd = os.memfd_create("execution-test-input", os.MFD_CLOEXEC)
            os.write(input_fd, b"a small message\n"); os.lseek(input_fd, 0, os.SEEK_SET)
            prepared = self.prepare("copy", output, env={"UNRELATED_FD": str(unrelated.fileno())}, bindings={3: input_fd})
            os.close(input_fd)
            old = os.environ.get("FORBIDDEN_PARENT_VALUE")
            os.environ["FORBIDDEN_PARENT_VALUE"] = "must not leak"
            try:
                owner = prepared.launch()
            finally:
                if old is None: os.environ.pop("FORBIDDEN_PARENT_VALUE")
                else: os.environ["FORBIDDEN_PARENT_VALUE"] = old
            observation = owner.wait(10)
            self.assertEqual(0, observation.returncode)
            self.assertTrue(observation.group_clean)
            output.seek(0); self.assertEqual(b"A SMALL MESSAGE\n", output.read())
            owner.close(); prepared.close()

    def test_unrelated_numeric_program_actual_parent_wait_and_rusage(self):
        with tempfile.TemporaryFile() as output:
            prepared = self.prepare("numeric", output, argv=("numeric", "12"))
            owner = prepared.launch()
            observation = owner.wait(10)
            self.assertEqual(7, observation.returncode)
            self.assertIsNotNone(observation.rusage)
            self.assertGreaterEqual(observation.rusage["user_seconds"], 0)
            output.seek(0); self.assertEqual(f"144 {os.getpid()}\n".encode(), output.read())
            with self.assertRaises(RuntimeError): prepared.launch()
            with self.assertRaises(ChildProcessError): os.waitpid(observation.pid, os.WNOHANG)
            owner.close(); prepared.close()

    def wait_ready(self, output):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            # pread does not race a shared output offset with the child.
            if os.pread(output.fileno(), 6, 0) == b"ready\n": return
            time.sleep(0.005)
        self.fail("native fixture did not become ready")

    def test_cancel_term_escalates_to_kill_and_cleans_owned_fds(self):
        before = len(os.listdir("/proc/self/fd"))
        with tempfile.TemporaryFile() as output:
            prepared = self.prepare("ignore", output)
            owner = prepared.launch()
            self.wait_ready(output)
            with self.assertRaisesRegex(RuntimeError, "active"):
                owner.close()
            owner.cancel(grace_period=0.02)
            self.assertEqual(-9, owner.wait(10).returncode)
            owner.close(); prepared.close()
        # sealed_program's original caller FD remains until test cleanup.
        self.assertEqual(before + 1, len(os.listdir("/proc/self/fd")))

    def test_managed_descendants_receive_cancellation_before_release(self):
        with tempfile.TemporaryFile() as output:
            prepared = self.prepare("family", output)
            owner = prepared.launch()
            self.wait_ready(output)
            owner.cancel(grace_period=2)
            observation = owner.wait(10)
            self.assertTrue(observation.group_clean)
            self.assertEqual(0, observation.returncode)
            with self.assertRaises(ProcessLookupError): os.killpg(observation.pid, 0)
            owner.close(); prepared.close()

    def test_exec_failure_is_observed_not_retried_or_fallback(self):
        descriptor = os.memfd_create("invalid-native-program", os.MFD_CLOEXEC)
        self.addCleanup(os.close, descriptor)
        os.write(descriptor, b"\x7fELF" + b"\0" * 64); os.fchmod(descriptor, 0o500)
        prepared = self.backend.prepare(ExecutionEnvelope(("invalid",)), executable_fd=descriptor)
        owner = prepared.launch()
        observation = owner.wait(10)
        self.assertEqual(127, observation.returncode)
        self.assertEqual(errno.ENOEXEC, observation.launch_error)
        with self.assertRaises(RuntimeError): prepared.launch()
        owner.close(); prepared.close()

    def test_descriptor_exhaustion_before_fork_is_not_started_and_cannot_replay(self):
        import resource

        with tempfile.TemporaryFile() as output:
            prepared = self.prepare("numeric", output, argv=("numeric", "3"))
            old_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (0, old_limit[1]))
                with self.assertRaises(OSError) as raised:
                    prepared.launch()
            finally:
                resource.setrlimit(resource.RLIMIT_NOFILE, old_limit)
            self.assertEqual(errno.EMFILE, raised.exception.errno)
            observation = prepared.owner.poll()
            self.assertEqual("not_started", observation.status)
            self.assertIsNone(observation.pid)
            self.assertIsNone(observation.returncode)
            self.assertIsNone(observation.rusage)
            self.assertTrue(observation.group_clean)
            self.assertEqual(errno.EMFILE, observation.launch_error)
            with self.assertRaises(RuntimeError):
                prepared.launch()
            prepared.owner.close(); prepared.close()

    def test_stolen_wait_authority_cannot_be_reconstructed_or_signalled(self):
        with tempfile.TemporaryFile() as output:
            prepared = self.prepare("numeric", output, argv=("numeric", "3"))
            owner = prepared.launch()
            os.waitpid(owner.pid, 0)
            self.assertEqual("authority_lost", owner.poll().status)
            with self.assertRaises(RuntimeError): owner.cancel()
            with self.assertRaises(RuntimeError): owner.close()
            # This test intentionally consumed the kernel wait externally.
            from gsched.execution import backend
            backend._retained.pop(owner.owner_id)
            prepared.close()

    def test_interrupted_native_return_preserves_owner_and_prevents_relaunch(self):
        class InterruptedReturn:
            def __init__(self, native): self.native = native
            def start(self):
                self.native.start()
                raise KeyboardInterrupt("injected after native child registration")
            def __getattr__(self, name): return getattr(self.native, name)

        with tempfile.TemporaryFile() as output:
            prepared = self.prepare("ignore", output)
            prepared.owner._native = InterruptedReturn(prepared.owner._native)
            with self.assertRaises(KeyboardInterrupt): prepared.launch()
            owner = prepared.owner
            self.assertIsNotNone(owner.pid)
            with self.assertRaises(RuntimeError): prepared.launch()
            owner.cancel(grace_period=0)
            self.assertEqual("exited", owner.wait(10).status)
            owner.close(); prepared.close()

    def test_leader_exit_does_not_prove_group_cleanup(self):
        # Linux permits a process to adopt orphaned grandchildren explicitly.
        # The test uses a separate test process so it cannot change daemon state.
        code = r'''
import ctypes, os, sys, time
from gsched.execution import ExecutionEnvelope, LinuxFdBackend
libc = ctypes.CDLL(None, use_errno=True)
assert libc.prctl(36, 1, 0, 0, 0) == 0  # PR_SET_CHILD_SUBREAPER
fd = os.open(sys.argv[1], os.O_RDONLY)
prepared = LinuxFdBackend().prepare(ExecutionEnvelope(("early-exit",)), executable_fd=fd)
os.close(fd)
owner = prepared.launch()
deadline = time.monotonic() + 5
while time.monotonic() < deadline:
    seen = owner.poll()
    if seen.status == "cleanup_pending": break
    time.sleep(.005)
assert seen.status == "cleanup_pending", seen
assert seen.returncode == 0 and seen.group_clean is False
try: owner.close()
except RuntimeError: pass
else: raise AssertionError("released live process group")
# Explicit adoption gives this isolated test permission to reap the orphan.
os.waitpid(-1, 0)
assert owner.wait(5).group_clean is True
owner.close(); prepared.close()
'''
        subprocess.run([sys.executable, "-c", code, str(self.programs["early_exit"])], check=True, timeout=15)


if __name__ == "__main__":
    unittest.main()
