from __future__ import annotations

import os
import sys
import tempfile
import threading
import unittest
from unittest import mock

from gsched.execution import (
    BackendUnavailable, ExecutionEnvelope, LinuxFdBackend, SubprocessBackend,
    retained_owners,
)


class ExecutionBackendTests(unittest.TestCase):
    def test_explicit_environment_is_copied_and_parent_values_do_not_leak(self):
        environment = {"OUTPUT": "expected"}
        envelope = ExecutionEnvelope((sys.executable, "-c", "import os; print(os.environ.get('OUTPUT')); print(os.environ.get('SCHED_TEST_PARENT_ONLY','absent'))"), environment)
        environment["OUTPUT"] = "mutated"
        with tempfile.TemporaryFile() as output, mock.patch.dict(os.environ, {"SCHED_TEST_PARENT_ONLY": "secret"}):
            prepared = SubprocessBackend().prepare(envelope, stdout_fd=output.fileno(), stderr_fd=output.fileno())
            owner = prepared.launch()
            self.assertEqual("exited", owner.wait(10).status)
            self.assertEqual(0, owner.poll().returncode)
            output.seek(0)
            self.assertEqual(b"expected\nabsent", output.read().replace(b"\r", b"").strip())
            owner.close()
            prepared.close()

    def test_second_unrelated_program_exit_and_single_launch(self):
        prepared = SubprocessBackend().prepare(ExecutionEnvelope((sys.executable, "-c", "raise SystemExit(7)")))
        owner = prepared.launch()
        with self.assertRaisesRegex(RuntimeError, "consumed"):
            prepared.launch()
        self.assertEqual(7, owner.wait(10).returncode)
        self.assertIn(owner, retained_owners())
        owner.close()
        self.assertNotIn(owner, retained_owners())
        prepared.close()

    def test_exec_failure_has_no_child_and_releases_original_owner(self):
        prepared = SubprocessBackend().prepare(ExecutionEnvelope(("/nonexistent/sched-exec-worker",)))
        with self.assertRaises(OSError):
            prepared.launch()
        observation = prepared.owner.poll()
        self.assertEqual("not_started", observation.status)
        self.assertIsNone(observation.pid)
        self.assertTrue(observation.group_clean)
        with self.assertRaises(RuntimeError):
            prepared.launch()
        prepared.owner.close()
        prepared.close()
        self.assertNotIn(prepared.owner, retained_owners())

    @unittest.skipUnless(os.name == "posix", "scheduler process groups require POSIX")
    def test_scheduler_retirement_releases_completed_subprocess_owner(self):
        import time
        from gsched.executor import Executor
        executor = Executor()
        before = set(retained_owners())
        with tempfile.TemporaryDirectory() as root:
            pid = executor.launch(cmd=[sys.executable, "-c", "pass"], stages=None,
                                  cwd=root, env={}, gpu=None, log_path=os.path.join(root, "job.log"))
            deadline = time.monotonic() + 10
            rc = None
            while rc is None and time.monotonic() < deadline:
                rc = executor.poll_rc(pid)
                time.sleep(.01)
            self.assertEqual(0, rc)
        self.assertEqual(before, set(retained_owners()))

    def test_cancel_and_timeout_require_original_owner(self):
        prepared = SubprocessBackend().prepare(ExecutionEnvelope((sys.executable, "-c", "import time; time.sleep(30)")))
        owner = prepared.launch()
        with self.assertRaises(TimeoutError):
            owner.wait(0)
        for invalid in (float("nan"), float("inf"), -1):
            with self.assertRaises(ValueError): owner.wait(invalid)
            with self.assertRaises(ValueError): owner.cancel(invalid)
        with self.assertRaisesRegex(RuntimeError, "active"):
            owner.close()
        errors = []
        thread = threading.Thread(target=lambda: self._other_thread(owner, errors))
        thread.start(); thread.join()
        self.assertTrue(errors)
        owner.cancel(grace_period=0.01)
        self.assertEqual("exited", owner.wait(10).status)
        owner.close(); prepared.close()

    @staticmethod
    def _other_thread(owner, errors):
        try:
            owner.poll()
        except RuntimeError as error:
            errors.append(str(error))

    def test_prepared_close_releases_only_its_duplicates(self):
        with tempfile.TemporaryFile() as output:
            prepared = SubprocessBackend().prepare(ExecutionEnvelope((sys.executable, "-c", "pass")), stdout_fd=output.fileno())
            duplicates = prepared._owned_stdio
            prepared.close()
            for descriptor in duplicates:
                with self.assertRaises(OSError):
                    os.fstat(descriptor)
            os.fstat(output.fileno())
            with self.assertRaises(RuntimeError):
                prepared.launch()

    def test_interrupted_subprocess_return_is_consumed_and_not_reconstructed(self):
        prepared = SubprocessBackend().prepare(ExecutionEnvelope((sys.executable, "-c", "pass")))
        with mock.patch("gsched.execution.backend.subprocess.Popen", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                prepared.launch()
        self.assertEqual("authority_lost", prepared.owner.poll().status)
        with self.assertRaises(RuntimeError):
            prepared.launch()
        with self.assertRaisesRegex(RuntimeError, "unresolved"):
            prepared.owner.close()
        prepared.close()
        # No actual child was created by this injected fault. Test-only cleanup.
        from gsched.execution import backend
        backend._retained.pop(prepared.owner.owner_id)

    def test_rejects_bad_interface_and_environment(self):
        for kwargs in ({"interface_version": "other/v1"}, {"env": {"A=B": "x"}}, {"argv": ("bad\0",)}):
            values = {"argv": (sys.executable,)}
            values.update(kwargs)
            with self.assertRaises(ValueError):
                ExecutionEnvelope(**values)

    @unittest.skipIf(sys.platform == "linux", "absence is covered by explicit native build tests")
    def test_native_backend_never_falls_back_to_subprocess(self):
        with self.assertRaises(BackendUnavailable):
            LinuxFdBackend()


if __name__ == "__main__":
    unittest.main()
