from __future__ import annotations

import argparse
import json
import os
import subprocess
from unittest import mock

from gsched import cli, config, state
from gsched.dispatcher import Dispatcher
from gsched.fingerprint import compute_fingerprint
from test_review_cli_state import TempStateCase


class ExecutionInputTests(TempStateCase):
    def test_init_requires_explicit_compute_node_without_writing_config(self):
        path = os.path.join(self.tmp.name, "new-config.json")
        answers = ["test-user", "", self.state_root, self.tmp.name, "/usr/bin/python3"]
        with mock.patch("builtins.input", side_effect=answers):
            self.assertEqual(1, cli.cmd_init(argparse.Namespace(config=path)))
        self.assertFalse(os.path.exists(path))

    def test_root_template_requires_explicit_default_project(self):
        cfg = {"projects": {"example": {"root": self.tmp.name}}}
        with self.assertRaisesRegex(config.ConfigError, "default_project"):
            config.resolve_template("{ROOT}/data", cfg)
        cfg["default_project"] = "example"
        self.assertEqual(self.tmp.name + "/data", config.resolve_template("{ROOT}/data", cfg))

    def fingerprint(self, *, cwd=None, env=None, artifacts=None, stages=None):
        return compute_fingerprint(
            ["echo", "result"], stages, cwd or self.tmp.name, False, {},
            execution_env=env, artifacts=artifacts,
        )

    def test_fingerprint_covers_environment_directory_and_output_rules(self):
        baseline = self.fingerprint(env={"SEED": "1"})[0]
        self.assertNotEqual(baseline, self.fingerprint(env={"SEED": "2"})[0])
        self.assertNotEqual(baseline, self.fingerprint(cwd=self.state_root, env={"SEED": "1"})[0])
        self.assertNotEqual(baseline, self.fingerprint(env={"SEED": "1"}, artifacts={"out": {"path": "new"}})[0])
        self.assertEqual(self.fingerprint(env={"A": "1", "B": "2"}), self.fingerprint(env={"B": "2", "A": "1"}))
        first = self.fingerprint(stages=[{"cmd": ["echo"], "artifacts": {"out": {"path": "old"}}}])
        second = self.fingerprint(stages=[{"cmd": ["echo"], "artifacts": {"out": {"path": "new"}}}])
        self.assertNotEqual(first[0], second[0])
        self.assertNotEqual(first[1], second[1])

    def test_preview_and_dispatch_agree_on_batch_and_default_environment(self):
        self.cfg["task_default_env"] = {"A": "default", "B": "default"}
        spec = {"id": "task", "cmd": ["echo", "result"], "stages": None,
                "cwd_abs": self.tmp.name, "git": False, "env": {"A": "task"},
                "artifacts": {"out": {"path": "result"}}, "project": "p"}
        batch_env = {"A": "batch", "B": "batch"}
        fp = compute_fingerprint(spec["cmd"], None, self.tmp.name, False, {},
                                 execution_env=config.task_environment(self.cfg, batch_env, spec["env"]),
                                 artifacts=spec["artifacts"])[0]
        self.seed_batch(job_status="done")
        with state.connect() as conn:
            conn.execute("UPDATE jobs SET fingerprint=?", (fp,))
        with open(os.path.join(self.tmp.name, "result"), "w") as stream:
            stream.write("result")
        norm = {"project": "p", "depends_on": [], "env": batch_env, "tasks": [spec]}
        self.assertEqual(1, cli._dry_run_preview(norm, self.cfg)["n_skip"])
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher.venv_paths = {}
        self.assertEqual(fp, dispatcher._snapshot_fingerprint(spec, self.tmp.name, "test", batch_env)[0])
        norm["env"] = {"B": "changed"}
        self.assertEqual(0, cli._dry_run_preview(norm, self.cfg)["n_skip"])

    def test_preview_refuses_artifact_through_parent_symlink(self):
        self.seed_batch(job_status="done")
        target = os.path.join(self.tmp.name, "outside")
        os.mkdir(target)
        with open(os.path.join(target, "result"), "w") as stream:
            stream.write("result")
        os.symlink(target, os.path.join(self.tmp.name, "link"))
        spec = {"id": "task", "cmd": ["echo"], "stages": None, "cwd_abs": self.tmp.name,
                "git": False, "artifacts": {"out": {"path": "link/result"}}, "project": "p"}
        norm = {"project": "p", "depends_on": [], "tasks": [spec]}
        with mock.patch("gsched.fingerprint.compute_fingerprint", return_value=("fp-1", None, None)):
            self.assertEqual(0, cli._dry_run_preview(norm, self.cfg)["n_skip"])

    def run_args(self, **overrides):
        return argparse.Namespace(cmd=["--", "echo", "ok"], venv="test", cwd=self.tmp.name,
                                  gpus=None, cpu_only=True, cpus=None, duration=None,
                                  out=None, project="p", dry_run=True, **overrides)

    def test_run_rejects_invalid_resources_and_unknown_project_in_preview(self):
        self.cfg["venvs"] = {"test": "/usr/bin/python3"}
        for values in ({"gpus": 0}, {"gpus": -1}, {"gpus": 1}, {"gpus": 2, "cpu_only": False},
                       {"cpus": 0}, {"cpus": -1}, {"duration": 0}, {"project": "absent"}):
            args = self.run_args()
            vars(args).update(values)
            with self.subTest(values=values), mock.patch("gsched.cli._load_cfg", return_value=self.cfg):
                self.assertEqual(1, self.capture(cli.cmd_run, args)[0])

    def test_run_preserves_quoted_argv_and_venv_path(self):
        venv_bin = os.path.join(self.tmp.name, "venv", "bin")
        os.makedirs(venv_bin)
        fake_python = os.path.join(venv_bin, "python")
        with open(fake_python, "w") as stream:
            stream.write("#!/bin/sh\nprintf '%s' \"$1\"\n")
        os.chmod(fake_python, 0o700)
        self.cfg["venvs"] = {"test": fake_python}
        args = self.run_args()
        args.dry_run = False
        args.cmd = ["--", "python", "it's a quoted argument"]
        with mock.patch("gsched.cli._load_cfg", return_value=self.cfg), mock.patch("gsched.cli._ensure_running_locked", return_value="test"):
            result, _, error = self.capture(cli.cmd_run, args)
        self.assertEqual(0, result, error)
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks").fetchone()[0])
        completed = subprocess.run(spec["cmd"], cwd=spec["cwd_abs"], env={**os.environ, **spec["env"]}, capture_output=True, text=True)
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertEqual("it's a quoted argument", completed.stdout)
