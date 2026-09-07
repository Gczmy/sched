from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import tempfile
import unittest
from unittest import mock

from gsched import cli, config


class NumericConfigTests(unittest.TestCase):
    def load(self, **overrides):
        cfg = {
            "user": "test", "node": "test-node", "gpus": [0],
            "projects": {"p": {"root": "/tmp/project"}}, "venvs": {},
            **overrides,
        }
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "config.json")
            with open(path, "w", encoding="utf-8") as stream:
                json.dump(cfg, stream)
            return config.load_config(path, apply_runtime_state=False)

    def test_gpu_capacity_rejects_nonfinite_values_before_allocator(self):
        for value in (float("nan"), float("inf"), -float("inf"), 10**400, True, "24", 0):
            with self.subTest(value=value), self.assertRaises(config.ConfigError):
                self.load(gpus=[{"idx": 0, "mem_gib": value}])
        cfg = self.load(gpus=[{"idx": 0, "mem_gib": 24.5, "max_jobs": 2}])
        self.assertEqual(([0], {0: 24.5}, {0: 2}), config.parse_gpus(cfg))

    def test_gpu_container_and_indices_match_status_contract(self):
        for value in ("", {}, False, 0, [-1], [{"idx": -1}], [True]):
            with self.subTest(value=value), self.assertRaises(config.ConfigError):
                self.load(gpus=value)
        for value in (None, []):
            with self.subTest(value=value):
                self.assertEqual(([], {}, {}), config.parse_gpus(self.load(gpus=value)))

    def test_affinity_and_job_cap_reject_silent_coercion(self):
        with self.assertRaises(config.ConfigError):
            self.load(projects={"p": {"root": "/tmp/project", "gpu_affinity": [-1]}})
        for value in (2.5, True, "3"):
            with self.subTest(value=value), self.assertRaises(config.ConfigError):
                self.load(co_locate_max_jobs=value)

    def test_gpu_set_mem_rejects_nonfinite_values_without_opening_state(self):
        for value in (float("nan"), float("inf"), -float("inf")):
            with self.subTest(value=value), mock.patch("gsched.cli.state.connect") as connect:
                with contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(1, cli.cmd_gpu_set_mem(argparse.Namespace(idx=0, gib=value)))
                connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
