"""One-shot Python wrapper behavior around the pinned native M owner."""

from __future__ import annotations

import gc
import json
import sys
import threading
import types
import unittest
import weakref
from unittest import mock

import gsched
import gsched.executor as executor_module
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
    def setUp(self) -> None:
        claims_patch = mock.patch.object(executor_module, "_native_monitor_claims", {})
        claims_patch.start()
        self.addCleanup(claims_patch.stop)

    def _bridge(self, owner: mock.Mock) -> types.ModuleType:
        owner.snapshot.return_value = json.dumps(
            {"session_id": SESSION_ID, "started": False}
        ).encode("ascii")
        native_module = types.ModuleType("gsched._m2b_scheduler_native")
        native_module.create_empty = mock.Mock(return_value=owner)
        native_module.retained_owners = lambda: (owner,)
        return native_module

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

    def test_other_executor_cannot_take_or_recover_claimed_pin(self) -> None:
        owner = mock.Mock()
        native_module = self._bridge(owner)
        first = Executor()
        second = Executor()

        with mock.patch.dict(
            sys.modules, {"gsched._m2b_scheduler_native": native_module}
        ), mock.patch.object(gsched, "_m2b_scheduler_native", native_module, create=True):
            first.reserve_native_monitor(SESSION_ID)
            del first._native_monitors[SESSION_ID]
            with self.assertRaisesRegex(ValueError, "another Executor"):
                second.recover_native_monitor(SESSION_ID)
            with self.assertRaisesRegex(ValueError, "already claimed"):
                second.reserve_native_monitor(SESSION_ID)
            native_module.create_empty.assert_called_once_with(SESSION_ID)
            owner.close.assert_not_called()
            owner.cancel.assert_not_called()
            owner.retire.assert_not_called()

            first.recover_native_monitor(SESSION_ID)
            first.discard_empty_native_monitor(SESSION_ID)

        self.assertNotIn(SESSION_ID, executor_module._native_monitor_claims)

    def test_original_executor_recovers_pin_without_replaying_start(self) -> None:
        owner = mock.Mock()
        native_module = self._bridge(owner)
        executor = Executor()

        with mock.patch.dict(
            sys.modules, {"gsched._m2b_scheduler_native": native_module}
        ), mock.patch.object(gsched, "_m2b_scheduler_native", native_module, create=True):
            executor.reserve_native_monitor(SESSION_ID)
            del executor._native_monitors[SESSION_ID]
            executor.recover_native_monitor(SESSION_ID)
            with self.assertRaisesRegex(ValueError, "already attempted"):
                executor.start_native_monitor(SESSION_ID, _plan())
            executor.discard_empty_native_monitor(SESSION_ID)

        owner.start.assert_not_called()
        owner.cancel.assert_not_called()
        self.assertNotIn(SESSION_ID, executor_module._native_monitor_claims)

    def test_new_executor_recovers_when_original_instance_is_gone(self) -> None:
        owner = mock.Mock()
        native_module = self._bridge(owner)
        first = Executor()
        second = Executor()

        with mock.patch.dict(
            sys.modules, {"gsched._m2b_scheduler_native": native_module}
        ), mock.patch.object(gsched, "_m2b_scheduler_native", native_module, create=True):
            first.reserve_native_monitor(SESSION_ID)
            first_ref = weakref.ref(first)
            del first
            gc.collect()
            self.assertIsNone(first_ref())

            second.recover_native_monitor(SESSION_ID)
            with self.assertRaisesRegex(ValueError, "already attempted"):
                second.start_native_monitor(SESSION_ID, _plan())
            second.discard_empty_native_monitor(SESSION_ID)

        self.assertNotIn(SESSION_ID, executor_module._native_monitor_claims)

    def test_create_failure_keeps_claim_only_if_native_pin_remains(self) -> None:
        owner = mock.Mock()
        native_module = self._bridge(owner)
        native_module.create_empty.side_effect = RuntimeError("interrupted native return")
        first = Executor()
        second = Executor()

        with mock.patch.dict(
            sys.modules, {"gsched._m2b_scheduler_native": native_module}
        ), mock.patch.object(gsched, "_m2b_scheduler_native", native_module, create=True):
            with self.assertRaisesRegex(RuntimeError, "interrupted native return"):
                first.reserve_native_monitor(SESSION_ID)
            with self.assertRaisesRegex(ValueError, "another Executor"):
                second.recover_native_monitor(SESSION_ID)
            first.recover_native_monitor(SESSION_ID)
            first.discard_empty_native_monitor(SESSION_ID)

        self.assertNotIn(SESSION_ID, executor_module._native_monitor_claims)

    def test_create_failure_without_pin_releases_claim(self) -> None:
        owner = mock.Mock()
        native_module = self._bridge(owner)
        native_module.create_empty.side_effect = RuntimeError("no native owner")
        native_module.retained_owners = lambda: ()

        with mock.patch.dict(
            sys.modules, {"gsched._m2b_scheduler_native": native_module}
        ), mock.patch.object(gsched, "_m2b_scheduler_native", native_module, create=True):
            with self.assertRaisesRegex(RuntimeError, "no native owner"):
                Executor().reserve_native_monitor(SESSION_ID)

        self.assertNotIn(SESSION_ID, executor_module._native_monitor_claims)

    def test_pin_inspection_failure_keeps_claim(self) -> None:
        owner = mock.Mock()
        native_module = self._bridge(owner)
        native_module.create_empty.side_effect = RuntimeError("unknown native result")
        native_module.retained_owners = mock.Mock(side_effect=OSError("pin query failed"))
        first = Executor()
        second = Executor()

        with mock.patch.dict(
            sys.modules, {"gsched._m2b_scheduler_native": native_module}
        ), mock.patch.object(gsched, "_m2b_scheduler_native", native_module, create=True):
            with self.assertRaisesRegex(RuntimeError, "unknown native result"):
                first.reserve_native_monitor(SESSION_ID)
            with self.assertRaisesRegex(ValueError, "another Executor"):
                second.recover_native_monitor(SESSION_ID)

        self.assertIs(executor_module._native_monitor_claimant(SESSION_ID), first)

    def test_parallel_reserve_cannot_release_other_threads_claim(self) -> None:
        owner = mock.Mock()
        native_module = self._bridge(owner)
        owner_tid = None

        def create_empty(session_id: str) -> mock.Mock:
            nonlocal owner_tid
            owner_tid = threading.get_native_id()
            return owner

        native_module.create_empty = create_empty
        native_module.retained_owners = lambda: (
            (owner,) if threading.get_native_id() == owner_tid else ()
        )
        original_claim = executor_module._claim_native_monitor
        claimed = threading.Event()
        continue_reserve = threading.Event()
        created = threading.Event()
        finish = threading.Event()
        failures: list[BaseException] = []
        executor = Executor()

        def paused_claim(session_id: str, claimant: Executor, *, recover: bool = False) -> bool:
            acquired = original_claim(session_id, claimant, recover=recover)
            if acquired and threading.current_thread().name == "first-reserve":
                claimed.set()
                if not continue_reserve.wait(5):
                    raise TimeoutError("reserve barrier timed out")
            return acquired

        def first_reserve() -> None:
            try:
                executor.reserve_native_monitor(SESSION_ID)
                created.set()
                if not finish.wait(5):
                    raise TimeoutError("cleanup barrier timed out")
                executor.discard_empty_native_monitor(SESSION_ID)
            except BaseException as exc:
                failures.append(exc)
                created.set()

        with mock.patch.dict(
            sys.modules, {"gsched._m2b_scheduler_native": native_module}
        ), mock.patch.object(gsched, "_m2b_scheduler_native", native_module, create=True), \
                mock.patch.object(executor_module, "_claim_native_monitor", paused_claim):
            worker = threading.Thread(target=first_reserve, name="first-reserve")
            worker.start()
            try:
                self.assertTrue(claimed.wait(5))
                with self.assertRaisesRegex(ValueError, "another thread"):
                    executor.reserve_native_monitor(SESSION_ID)
                self.assertIs(executor_module._native_monitor_claimant(SESSION_ID), executor)
                continue_reserve.set()
                self.assertTrue(created.wait(5))
                self.assertEqual(failures, [])
                self.assertIs(executor_module._native_monitor_claimant(SESSION_ID), executor)
            finally:
                continue_reserve.set()
                finish.set()
                worker.join(5)

        self.assertFalse(worker.is_alive())
        self.assertEqual(failures, [])
        self.assertNotIn(SESSION_ID, executor_module._native_monitor_claims)

    def test_reentrant_claim_cannot_replace_owner(self) -> None:
        owner = mock.Mock()
        native_module = self._bridge(owner)
        first = Executor()
        second = Executor()

        def reenter(session_id: str) -> None:
            executor_module._claim_native_monitor(session_id, second)

        with mock.patch.dict(
            sys.modules, {"gsched._m2b_scheduler_native": native_module}
        ), mock.patch.object(gsched, "_m2b_scheduler_native", native_module, create=True), \
                mock.patch.object(executor_module, "_native_monitor_claimant", side_effect=reenter):
            with self.assertRaisesRegex(RuntimeError, "not reentrant"):
                first.reserve_native_monitor(SESSION_ID)

        self.assertNotIn(SESSION_ID, executor_module._native_monitor_claims)
        native_module.create_empty.assert_not_called()

    def test_retirement_failure_keeps_claim_until_success(self) -> None:
        owner = mock.Mock()
        owner.retire.side_effect = RuntimeError("owner still pinned")
        native_module = self._bridge(owner)
        first = Executor()
        second = Executor()

        with mock.patch.dict(
            sys.modules, {"gsched._m2b_scheduler_native": native_module}
        ), mock.patch.object(gsched, "_m2b_scheduler_native", native_module, create=True):
            first.reserve_native_monitor(SESSION_ID)
            with self.assertRaisesRegex(RuntimeError, "owner still pinned"):
                first.retire_native_monitor(SESSION_ID)
            with self.assertRaisesRegex(ValueError, "another Executor"):
                second.recover_native_monitor(SESSION_ID)
            owner.retire.side_effect = None
            first.retire_native_monitor(SESSION_ID)

        self.assertNotIn(SESSION_ID, executor_module._native_monitor_claims)


if __name__ == "__main__":
    unittest.main()
