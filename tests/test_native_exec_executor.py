from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock

from gsched.executor import Executor


PROFILE_ID = "m2b-preparation-v1"
PROFILE_SHA256 = "a" * 64
SUBMITTED_ARGV = ["/opt/m2b/verifier", "--phase", "preparation"]


class NativeExecExecutorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.log_path = os.path.join(self.tmp.name, "native.log")

    def _launch(self, **overrides):
        values = {
            "cmd": list(SUBMITTED_ARGV),
            "stages": None,
            "cwd": self.tmp.name,
            "env": {},
            "gpu": None,
            "log_path": self.log_path,
            "native_exec_profile_id": PROFILE_ID,
            "native_exec_profile_sha256": PROFILE_SHA256,
            "native_exec_submitted_argv": list(SUBMITTED_ARGV),
        }
        values.update(overrides)
        return Executor().launch(**values)

    def test_native_exec_metadata_must_be_all_present(self) -> None:
        incomplete = (
            {
                "native_exec_profile_id": None,
            },
            {
                "native_exec_profile_sha256": None,
            },
            {
                "native_exec_submitted_argv": None,
            },
        )
        for overrides in incomplete:
            with self.subTest(overrides=overrides), mock.patch(
                "gsched.executor.subprocess.Popen"
            ) as popen:
                with self.assertRaisesRegex(ValueError, "all present"):
                    self._launch(**overrides)
                popen.assert_not_called()

    def test_native_exec_rejects_invalid_or_drifted_metadata(self) -> None:
        cases = (
            (
                {"native_exec_profile_id": "bad profile"},
                "profile id",
            ),
            (
                {"native_exec_profile_sha256": "A" * 64},
                "profile digest",
            ),
            (
                {"native_exec_submitted_argv": []},
                "submitted argv",
            ),
            (
                {
                    "cmd": ["verifier", "--phase", "preparation"],
                    "native_exec_submitted_argv": [
                        "verifier",
                        "--phase",
                        "preparation",
                    ],
                },
                "absolute path",
            ),
            (
                {"conda_env_dir": "/opt/unbound-runtime"},
                "explicit runtime",
            ),
            (
                {"gpu": 0},
                "CPU-only",
            ),
            (
                {"stages": [{"cmd": ["/bin/true"]}]},
                "forbids stages",
            ),
            (
                {"cmd": ["/opt/m2b/verifier", "--phase", "raw"]},
                "differs from submitted argv",
            ),
            (
                {"env": {"PATH": "/untrusted", "PYTHONPATH": "/poison"}},
                "non-scheduler keys",
            ),
        )
        for overrides, message in cases:
            with self.subTest(overrides=overrides), mock.patch(
                "gsched.executor.subprocess.Popen"
            ) as popen:
                with self.assertRaisesRegex(ValueError, message):
                    self._launch(**overrides)
                popen.assert_not_called()

    def test_native_exec_bypasses_bash_and_rc_supervisor(self) -> None:
        proc = mock.Mock(pid=4242)
        rc_dir = os.path.join(self.tmp.name, "rc")
        with mock.patch.dict(
            os.environ,
            {
                "PATH": "/daemon-path",
                "PYTHONPATH": "/daemon-pythonpath",
                "LD_PRELOAD": "/daemon-preload",
            },
            clear=False,
        ), mock.patch(
            "gsched.executor.subprocess.Popen",
            return_value=proc,
        ) as popen:
            pgid = self._launch(
                env={
                    "SCHED_RC_DIR": rc_dir,
                    "SCHED_RC_PREFIX": "native",
                }
            )

        self.assertEqual(4242, pgid)
        self.assertEqual(SUBMITTED_ARGV, popen.call_args.args[0])
        self.assertTrue(popen.call_args.kwargs["start_new_session"])
        self.assertEqual(
            {
                "SCHED_RC_DIR": rc_dir,
                "SCHED_RC_PREFIX": "native",
                "CUDA_VISIBLE_DEVICES": "",
            },
            popen.call_args.kwargs["env"],
        )


if __name__ == "__main__":
    unittest.main()
