"""One-shot Python wrapper behavior around the pinned native M owner."""

from __future__ import annotations

import json
import sys
import types
import unittest
from unittest import mock

import gsched
from gsched.executor import Executor
from gsched.native_monitor import NativeMonitorLaunch


SESSION_ID = "a" * 32


def _plan() -> NativeMonitorLaunch:
    # The fake native owner inspects only the call boundary, not these FDs.
    return NativeMonitorLaunch(
        monitor_fd=3,
        monitor_path="/monitor",
        monitor_bytes=1,
        monitor_sha256=b"\0" * 32,
        request_fd=4,
        request_bytes=1,
        request_sha256=b"\0" * 32,
        root_fd=5,
        root_path="/root",
        input_fd=6,
        output_fd=7,
        error_fd=8,
        deadline_ns=9,
        evidence_relative_path="results/evidence",
    )


class NativeMonitorExecutorTests(unittest.TestCase):
    def test_replay_does_not_cancel_first_owner(self) -> None:
        owner = mock.Mock()
        executor = Executor()
        executor._native_monitors[SESSION_ID] = owner

        executor.start_native_monitor(SESSION_ID, _plan())
        with self.assertRaisesRegex(ValueError, "already attempted"):
            executor.start_native_monitor(SESSION_ID, _plan())

        owner.start.assert_called_once()
        owner.cancel.assert_not_called()

    def test_first_start_failure_still_cancels_once(self) -> None:
        owner = mock.Mock()
        owner.start.side_effect = RuntimeError("native start failed")
        executor = Executor()
        executor._native_monitors[SESSION_ID] = owner

        with self.assertRaisesRegex(RuntimeError, "native start failed"):
            executor.start_native_monitor(SESSION_ID, _plan())
        with self.assertRaisesRegex(ValueError, "already attempted"):
            executor.start_native_monitor(SESSION_ID, _plan())

        owner.start.assert_called_once()
        owner.cancel.assert_called_once_with()

    def test_recovered_pin_cannot_retry_unknown_start(self) -> None:
        owner = mock.Mock()
        owner.snapshot.return_value = json.dumps(
            {"session_id": SESSION_ID, "started": True}
        ).encode("ascii")
        native_module = types.ModuleType("gsched._m2b_scheduler_native")
        native_module.retained_owners = lambda: (owner,)
        executor = Executor()

        with mock.patch.dict(
            sys.modules, {"gsched._m2b_scheduler_native": native_module}
        ), mock.patch.object(gsched, "_m2b_scheduler_native", native_module, create=True):
            executor.recover_native_monitor(SESSION_ID)
        with self.assertRaisesRegex(ValueError, "already attempted"):
            executor.start_native_monitor(SESSION_ID, _plan())

        owner.start.assert_not_called()
        owner.cancel.assert_not_called()


if __name__ == "__main__":
    unittest.main()
