import argparse
import concurrent.futures
import json
import os
import time
from unittest import mock

from gsched import cli
from test_review_cli_state import TempStateCase


class ConfigConcurrencyTests(TempStateCase):
    def test_concurrent_config_patches_preserve_both_updates(self):
        paths = []
        for key, value in (("gpu_job_cpus", 3), ("max_cpu_jobs", 4)):
            path = os.path.join(self.tmp.name, key + ".json")
            with open(path, "w") as stream:
                json.dump({key: value}, stream)
            paths.append(path)
        original = cli._load_cfg

        def slow_read():
            value = original()
            time.sleep(0.05)  # Allow a second writer to reach the same old file.
            return value

        with mock.patch("gsched.cli._load_cfg", side_effect=slow_read), mock.patch("builtins.print"):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                pending = [pool.submit(cli.cmd_config_set, argparse.Namespace(file=path, yes=True)) for path in paths]
                self.assertEqual([0, 0], [future.result(timeout=5) for future in pending])
        with open(self.config_path) as stream:
            config = json.load(stream)
        self.assertEqual(3, config["gpu_job_cpus"])
        self.assertEqual(4, config["max_cpu_jobs"])
        self.assertFalse(os.path.exists(self.config_path + ".tmp-set"))

    def test_invalid_gpu_patch_returns_failure_without_modifying_config(self):
        patch = os.path.join(self.tmp.name, "bad.json")
        with open(patch, "w") as stream:
            json.dump({"gpus": [{"idx": 0, "mem_gib": float("nan")}]}, stream)
        code, _, error = self.capture(cli.cmd_config_set, argparse.Namespace(file=patch, yes=True))
        self.assertEqual(1, code)
        self.assertIn("新配置校验失败", error)
        with open(self.config_path) as stream:
            self.assertEqual(self.cfg, json.load(stream))
