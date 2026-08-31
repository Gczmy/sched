from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest import mock

from gsched import state
from gsched.allocator import Allocator, FREE_UTIL_CONFIRM_SAMPLES
from gsched.dispatcher import Dispatcher


class GPUFreeProbeDebounceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config_path = os.path.join(self.tmp.name, "config.json")
        with open(self.config_path, "w", encoding="utf-8") as stream:
            json.dump(
                {
                    "schema_version": 1,
                    "user": "test",
                    "node": "debounce-node",
                    "state_dir": self.tmp.name,
                    "gpus": [0],
                    "default_project": "default",
                    "projects": {},
                    "venvs": {},
                },
                stream,
            )
        self.env = mock.patch.dict(
            os.environ,
            {"SCHED_STATE": self.tmp.name, "SCHED_CONFIG": self.config_path},
            clear=False,
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        state._hostname_cache.clear()
        state._hostname_last_good.clear()
        state._pinned_host.clear()
        self.addCleanup(state._hostname_cache.clear)
        self.addCleanup(state._hostname_last_good.clear)
        self.addCleanup(state._pinned_host.clear)
        state.init_db()
        with state.connect() as conn:
            state.init_gpus(conn, [0])
        self.allocator = Allocator([0])

    def gpu_status(self) -> str:
        with state.connect() as conn:
            return str(state.get_gpu(conn, 0)["status"])

    def util_stage_exists(self, sample_no: int) -> bool:
        base = self.allocator._confirm_flag("free_util_confirm", 0)
        return os.path.exists(f"{base}.{sample_no}")

    def test_single_util_only_spike_keeps_card_free_and_clean_resets(self) -> None:
        self.allocator._compute_pids_by_card = mock.Mock(return_value={})
        self.allocator._util_opt = mock.Mock(side_effect=[2, 0, 2, 2, 2])

        self.assertEqual([], self.allocator.probe_free())
        self.assertEqual("free", self.gpu_status())
        self.assertTrue(self.util_stage_exists(1))
        self.assertEqual([], self.allocator.available_gpus())
        self.assertIsNone(self.allocator.assign("spike-must-not-launch"))

        self.assertEqual([], self.allocator.probe_free())
        self.assertEqual("free", self.gpu_status())
        self.assertEqual([0], self.allocator.available_gpus())
        for stage in range(1, FREE_UTIL_CONFIRM_SAMPLES):
            self.assertFalse(self.util_stage_exists(stage))

        # The clean sample broke the sequence: two new positives are not yet
        # enough, and only the third new consecutive positive confirms it.
        for _ in range(FREE_UTIL_CONFIRM_SAMPLES - 1):
            self.assertEqual([], self.allocator.probe_free())
            self.assertEqual("free", self.gpu_status())
        self.assertEqual([0], self.allocator.probe_free())
        self.assertEqual("unmanaged", self.gpu_status())

    def test_confirming_util_sample_closes_pool_before_assignment(self) -> None:
        self.allocator._compute_pids_by_card = mock.Mock(return_value={})
        self.allocator._util_opt = mock.Mock(return_value=1)

        for _ in range(FREE_UTIL_CONFIRM_SAMPLES - 1):
            self.assertEqual([], self.allocator.probe_free())
            self.assertEqual([], self.allocator.available_gpus())
            self.assertIsNone(self.allocator.assign("pre-confirm-must-not-launch"))

        self.assertEqual([0], self.allocator.probe_free())
        self.assertEqual("unmanaged", self.gpu_status())
        self.assertEqual([], self.allocator.available_gpus())
        self.assertIsNone(self.allocator.assign("would-have-launched"))

        # Confirmation remains saturated until a clean sample.  If a DB
        # transition had rolled back, the next positive would fail closed
        # immediately instead of opening another multi-tick window.
        for stage in range(1, FREE_UTIL_CONFIRM_SAMPLES):
            self.assertTrue(self.util_stage_exists(stage))

    def test_preconfirmation_suppresses_exclusive_and_shared_dispatch_paths(
        self,
    ) -> None:
        self.allocator._compute_pids_by_card = mock.Mock(return_value={})
        self.allocator._util_opt = mock.Mock(side_effect=[1, 0])
        self.allocator.mem_total = mock.Mock(return_value=24.0)

        self.assertEqual([], self.allocator.probe_free())
        self.assertEqual("free", self.gpu_status())

        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = {
            "co_locate": True,
            "co_locate_safety": 0.7,
            "co_locate_max_jobs": 3,
        }
        dispatcher.allocator = self.allocator
        dispatcher._projects = {}
        dispatcher._gpu_max_jobs = {}
        dispatcher._frozen_gpus = set()
        dispatcher._cap_warned = set()
        dispatcher.log_line = mock.Mock()
        with state.connect() as conn:
            self.assertIsNone(
                dispatcher._assign_in_tx(
                    conn,
                    "exclusive-suppressed",
                    {"resources": {"gpu_share": False}},
                )
            )
            self.assertIsNone(
                dispatcher._assign_in_tx(
                    conn,
                    "shared-suppressed",
                    {"resources": {"gpu_share": True, "vram_gib": 1.0}},
                )
            )

        # A clean next sample resets both the persistent streak and transient
        # dispatch gate; the same exclusive path can allocate immediately.
        self.assertEqual([], self.allocator.probe_free())
        with state.connect() as conn:
            self.assertEqual(
                0,
                dispatcher._assign_in_tx(
                    conn,
                    "exclusive-after-clean",
                    {"resources": {"gpu_share": False}},
                ),
            )

    def test_compute_pid_is_immediately_unmanaged(self) -> None:
        self.allocator._compute_pids_by_card = mock.Mock(
            return_value={0: [12345]}
        )
        self.allocator._util_opt = mock.Mock(
            side_effect=AssertionError("util must not soften a compute PID")
        )

        self.assertEqual([0], self.allocator.probe_free())
        self.assertEqual("unmanaged", self.gpu_status())
        self.allocator._util_opt.assert_not_called()

    def test_indeterminate_compute_probe_is_immediately_unmanaged(self) -> None:
        self.allocator._compute_pids_by_card = mock.Mock(return_value=None)
        self.allocator._util_opt = mock.Mock(
            side_effect=AssertionError("util must not soften probe uncertainty")
        )

        self.assertEqual([0], self.allocator.probe_free())
        self.assertEqual("unmanaged", self.gpu_status())
        self.allocator._util_opt.assert_not_called()


if __name__ == "__main__":
    unittest.main()
