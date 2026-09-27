from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import tempfile
import unittest
from typing import Any
from unittest import mock

from gsched import cli, state
from gsched.fingerprint import compute_fingerprint


PROFILE_DIGEST = "a" * 64
PROFILE_METADATA: dict[str, Any] = {
    "_native_exec_profile_id": "m2b-preparation-v1",
    "_native_exec_profile_sha256": PROFILE_DIGEST,
    "_native_exec_project_root_identity_sha256": "b" * 64,
    "_native_exec_submitted_argv": [
        "/usr/bin/python3",
        "experiments/m2b_runtime_bootstrap.py",
    ],
}


class NativeExecFingerprintTests(unittest.TestCase):
    def _fingerprint(self, **kwargs) -> str:
        fingerprint, stages, revision = compute_fingerprint(
            ["/usr/bin/python3", "job.py"],
            None,
            ".",
            False,
            {},
            **kwargs,
        )
        self.assertIsNone(stages)
        self.assertIsNone(revision)
        self.assertIsNotNone(fingerprint)
        return str(fingerprint)

    def test_native_digest_is_opt_in_and_preserves_main_schema2_golden(self) -> None:
        main_payload = json.dumps(
            {
                "schema": 2,
                "cmd": ["/usr/bin/python3", "job.py"],
                "cwd": os.path.realpath("."),
                "env": {},
                "artifacts": {},
                "task_artifacts": {},
                "rev": None,
                "dirty": None,
                "runtime": None,
            }, sort_keys=True
        )
        baseline = hashlib.sha256(main_payload.encode()).hexdigest()

        self.assertEqual(baseline, self._fingerprint())
        self.assertEqual(
            baseline,
            self._fingerprint(native_exec_profile_sha256=None),
        )
        self.assertEqual(
            baseline,
            self._fingerprint(native_exec_profile_sha256=""),
        )

        native_payload = json.dumps(
            {
                "schema": 2,
                "cmd": ["/usr/bin/python3", "job.py"],
                "cwd": os.path.realpath("."),
                "env": {},
                "artifacts": {},
                "task_artifacts": {},
                "rev": None,
                "dirty": None,
                "runtime": None,
                "native_exec_profile_sha256": PROFILE_DIGEST,
            }, sort_keys=True
        )
        self.assertEqual(
            hashlib.sha256(native_payload.encode()).hexdigest(),
            self._fingerprint(
                native_exec_profile_sha256=PROFILE_DIGEST,
            ),
        )

    def test_ordinary_echo_fingerprint_keeps_main_schema2_golden(self) -> None:
        fingerprint, stages, revision = compute_fingerprint(
            ["/bin/echo", "ok"],
            None,
            "/tmp",
            False,
            {},
        )

        self.assertEqual(
            "315a960e756711ee6362284f3bb9f31c85444740575faf68bd98ffab196526da",
            fingerprint,
        )
        self.assertIsNone(stages)
        self.assertIsNone(revision)

    def test_native_digest_requires_exactly_64_hex_characters(self) -> None:
        for invalid in (
            "f" * 63,
            "f" * 65,
            "g" * 64,
            "A" * 64,
            True,
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self._fingerprint(native_exec_profile_sha256=invalid)
            with self.subTest(
                root_invalid=invalid
            ), self.assertRaises(ValueError):
                self._fingerprint(
                    native_exec_project_root_identity_sha256=invalid
                )

    def test_native_root_identity_digest_also_binds_fingerprint(self) -> None:
        profile_only = self._fingerprint(
            native_exec_profile_sha256=PROFILE_DIGEST
        )
        profile_and_root = self._fingerprint(
            native_exec_profile_sha256=PROFILE_DIGEST,
            native_exec_project_root_identity_sha256="b" * 64,
        )
        self.assertNotEqual(profile_only, profile_and_root)

    def test_native_digest_binds_each_stage_and_task_fingerprint(self) -> None:
        stages = [{"cmd": ["python", "one.py"]}, {"cmd": ["python", "two.py"]}]
        base_task, base_stages, _ = compute_fingerprint(
            None, stages, ".", False, {}
        )
        native_task, native_stages, _ = compute_fingerprint(
            None,
            stages,
            ".",
            False,
            {},
            native_exec_profile_sha256=PROFILE_DIGEST,
        )

        self.assertNotEqual(base_stages, native_stages)
        self.assertNotEqual(base_task, native_task)


class NativeExecCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state_root = os.path.join(self.tmp.name, "state")
        self.config_path = os.path.join(self.tmp.name, "config.json")
        self.cfg = {
            "schema_version": 1,
            "user": "test",
            "node": "native-cli-node",
            "state_dir": self.state_root,
            "gpus": [],
            "default_project": "p",
            "projects": {
                "p": {
                    "root": self.tmp.name,
                    "git": False,
                    "gpu_quota": 0,
                }
            },
            "venvs": {},
            "task_default_env": {"PYTHONNOUSERSITE": "1"},
            "native_exec_profiles": {
                "m2b-preparation-v1": {
                    "mode": "strict",
                    "project": "p",
                    "batch_name": "native-submit",
                    "task_id": "task",
                    "submitted_argv": list(
                        PROFILE_METADATA["_native_exec_submitted_argv"]
                    ),
                }
            },
        }
        with open(self.config_path, "w", encoding="utf-8") as stream:
            json.dump(self.cfg, stream)
        self.env = mock.patch.dict(
            os.environ,
            {
                "SCHED_STATE": self.state_root,
                "SCHED_CONFIG": self.config_path,
                "SCHED_ALLOW_FOREIGN_WRITE": "1",
            },
            clear=False,
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        self._clear_state_caches()
        self.addCleanup(self._clear_state_caches)
        state.init_db()

    @staticmethod
    def _clear_state_caches() -> None:
        state._hostname_cache.clear()
        state._hostname_last_good.clear()
        state._pinned_host.clear()

    @staticmethod
    def _capture(function, *args):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = function(*args)
        return result, stdout.getvalue(), stderr.getvalue()

    def _task(self, *, native: bool = True) -> dict:
        task = {
            "id": "task",
            "cmd": list(PROFILE_METADATA["_native_exec_submitted_argv"]),
            "stages": None,
            "cwd_abs": self.tmp.name,
            "git": False,
            "env": {},
            "resources": {"gpu": 0, "cpus": 1},
            "duration_min": 5,
            "max_retry": 0,
            "artifacts": {},
            "paths_escape": False,
            "probes": None,
            "max_parallel": None,
            "_force_rerun": None,
            "progress_regex": None,
            "runtime": None,
            "runtime_prefix": None,
        }
        if native:
            task.update(PROFILE_METADATA)
        return task

    def test_sched_run_cannot_claim_reserved_native_batch_name(self) -> None:
        fixed_strftime = "20260831123456789012"
        reserved_name = "run-echo-456789"
        self.cfg["venvs"] = {"py": "/usr/bin/python3"}
        self.cfg["native_exec_profiles"]["m2b-preparation-v1"][
            "batch_name"
        ] = reserved_name
        with open(self.config_path, "w", encoding="utf-8") as stream:
            json.dump(self.cfg, stream)
        args = argparse.Namespace(
            cmd=["--", "/bin/echo", "ok"],
            venv="py",
            gpus=1,
            cpu_only=False,
            cpus=None,
            cwd=None,
            out=None,
            duration=None,
            dry_run=False,
            project="p",
        )

        with mock.patch.object(cli, "datetime") as fake_datetime, mock.patch.object(
            cli,
            "_ensure_running_locked",
            side_effect=AssertionError("reserved sched run must not wake daemon"),
        ):
            fake_datetime.now.return_value.strftime.return_value = fixed_strftime
            rc, _stdout, stderr = self._capture(cli.cmd_run, args)

        self.assertEqual(1, rc)
        self.assertIn("native_exec_profile 保留", stderr)
        with state.connect() as conn:
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM batches WHERE name=?",
                    (reserved_name,),
                ).fetchone()[0],
            )

    def _norm(self, *, name: str = "native-submit", native: bool = True) -> dict:
        return {
            "name": name,
            "mode": "strict" if native else "mix",
            "depends_on": [],
            "cwd": "{ROOT}",
            "env": {},
            "notify": None,
            "project": "p",
            "priority": 0,
            "tasks": [self._task(native=native)],
        }

    def _raw_strict_batch(self) -> dict:
        return {
            "name": "native-submit",
            "mode": "strict",
            "project": "p",
            "cwd": self.tmp.name,
            "tasks": [
                {
                    "id": "task",
                    "cmd": list(
                        PROFILE_METADATA["_native_exec_submitted_argv"]
                    ),
                    "git": False,
                    "resources": {"gpu": 0, "cpus": 1},
                    "max_retry": 0,
                }
            ],
        }

    def _seed_resubmit_target(self, *, mode: str, metadata: dict | None = None) -> None:
        spec = self._task(native=False)
        if metadata:
            spec.update(metadata)
        batch_id = "batch-20260830-000000"
        with state.connect() as conn:
            state.insert_batch(
                conn,
                batch_id,
                "batch",
                mode,
                [],
                None,
                self.tmp.name,
                {},
                project="p",
            )
            conn.execute(
                "UPDATE batches SET status='active' WHERE id=?",
                (batch_id,),
            )
            state.insert_task(conn, batch_id, "task", 1, spec, 0, "p")
            state.insert_job(
                conn,
                f"{batch_id}-task-v1",
                batch_id,
                "task",
                1,
                "old-fingerprint",
                None,
                "p",
            )
            state.update_job(
                conn,
                f"{batch_id}-task-v1",
                status="failed",
                failure="fixture",
                finished_at=state.now(),
            )

    def test_dry_run_passes_digest_only_for_complete_native_metadata(self) -> None:
        cases = (
            ("ordinary", self._norm(native=False), False),
            ("partial", self._norm(native=False), False),
            ("native", self._norm(native=True), True),
        )
        cases[1][1]["tasks"][0]["_native_exec_profile_id"] = "partial"
        for label, norm, expects_digest in cases:
            with self.subTest(case=label), mock.patch(
                "gsched.fingerprint.compute_fingerprint",
                return_value=("fingerprint", None, None),
            ) as fingerprint:
                cli._dry_run_preview(
                    norm,
                    self.cfg,
                    use_state=False,
                )

            kwargs = fingerprint.call_args.kwargs
            if expects_digest:
                self.assertEqual(
                    PROFILE_DIGEST,
                    kwargs["native_exec_profile_sha256"],
                )
            else:
                self.assertNotIn("native_exec_profile_sha256", kwargs)

    def test_submit_persists_native_metadata_and_binds_fingerprint(self) -> None:
        batch_path = os.path.join(self.tmp.name, "batch.json")
        with open(batch_path, "w", encoding="utf-8") as stream:
            json.dump({}, stream)
        norm = self._norm()
        args = argparse.Namespace(batch=batch_path, dry_run=False, json=False)
        with mock.patch.object(cli, "_load_cfg", return_value=self.cfg), mock.patch.object(
            cli, "validate_batch", return_value=norm
        ), mock.patch.object(
            cli, "check_dependency_cycle", return_value=None
        ), mock.patch.object(
            cli, "_is_foreign_host", return_value=False
        ), mock.patch(
            "gsched.fingerprint.compute_fingerprint",
            return_value=("native-fingerprint", None, None),
        ) as fingerprint, mock.patch.object(
            cli, "_ensure_running_locked", return_value="awake"
        ):
            rc, _stdout, stderr = self._capture(cli.cmd_submit, args)

        self.assertEqual(0, rc, stderr)
        self.assertEqual(
            PROFILE_DIGEST,
            fingerprint.call_args.kwargs["native_exec_profile_sha256"],
        )
        with state.connect() as conn:
            task_row = conn.execute(
                "SELECT spec FROM tasks ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            job_row = conn.execute(
                "SELECT fingerprint FROM jobs ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
        persisted = json.loads(task_row["spec"])
        self.assertEqual(PROFILE_METADATA, {
            key: persisted[key] for key in PROFILE_METADATA
        })
        self.assertEqual("native-fingerprint", job_row["fingerprint"])

    def test_real_schema_strict_submit_persists_resolved_profile(self) -> None:
        batch_path = os.path.join(self.tmp.name, "real-batch.json")
        raw = self._raw_strict_batch()
        with open(batch_path, "w", encoding="utf-8") as stream:
            json.dump(raw, stream)
        expected_task = cli.validate_batch(raw, self.cfg)["tasks"][0]
        args = argparse.Namespace(batch=batch_path, dry_run=False, json=False)

        with mock.patch(
            "gsched.fingerprint._git_worktree_state",
            side_effect=AssertionError("strict submit must not probe git"),
        ) as git_probe, mock.patch.object(
            cli, "_load_cfg", return_value=self.cfg
        ), mock.patch.object(
            cli, "check_dependency_cycle", return_value=None
        ), mock.patch.object(
            cli, "_is_foreign_host", return_value=False
        ), mock.patch.object(
            cli, "_ensure_running_locked", return_value="awake"
        ):
            rc, _stdout, stderr = self._capture(cli.cmd_submit, args)

        self.assertEqual(0, rc, stderr)
        git_probe.assert_not_called()
        with state.connect() as conn:
            task_row = conn.execute(
                "SELECT spec FROM tasks ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
        persisted = json.loads(task_row["spec"])
        self.assertEqual(
            expected_task["_native_exec_profile_id"],
            persisted["_native_exec_profile_id"],
        )
        self.assertEqual(
            expected_task["_native_exec_profile_sha256"],
            persisted["_native_exec_profile_sha256"],
        )
        self.assertEqual(
            expected_task["_native_exec_submitted_argv"],
            persisted["_native_exec_submitted_argv"],
        )

    def test_strict_name_is_consumed_by_every_terminal_history(self) -> None:
        batch_path = os.path.join(self.tmp.name, "one-shot-batch.json")
        with open(batch_path, "w", encoding="utf-8") as stream:
            json.dump(self._raw_strict_batch(), stream)
        args = argparse.Namespace(batch=batch_path, dry_run=False, json=False)

        for status in ("done", "blocked", "discarded"):
            with self.subTest(status=status):
                with state.connect() as conn:
                    state.insert_batch(
                        conn,
                        f"historical-{status}",
                        "native-submit",
                        "strict",
                        [],
                        None,
                        self.tmp.name,
                        {},
                        project="p",
                    )
                    conn.execute(
                        "UPDATE batches SET status=? WHERE id=?",
                        (status, f"historical-{status}"),
                    )
                with mock.patch.object(
                    cli, "_load_cfg", return_value=self.cfg
                ), mock.patch.object(
                    cli, "check_dependency_cycle", return_value=None
                ), mock.patch.object(
                    cli, "_is_foreign_host", return_value=False
                ), mock.patch(
                    "gsched.fingerprint.compute_fingerprint"
                ) as fingerprint, mock.patch.object(
                    cli, "_ensure_running_locked"
                ) as ensure_running:
                    rc, _stdout, stderr = self._capture(
                        cli.cmd_submit, args
                    )

                self.assertEqual(1, rc)
                self.assertIn("已消费", stderr)
                fingerprint.assert_not_called()
                ensure_running.assert_not_called()
                with state.connect() as conn:
                    self.assertEqual(
                        1,
                        conn.execute(
                            "SELECT COUNT(*) FROM batches"
                        ).fetchone()[0],
                    )
                    conn.execute("DELETE FROM batches")

    def test_submit_rejects_native_argv_drift_before_fingerprint_or_write(self) -> None:
        batch_path = os.path.join(self.tmp.name, "batch.json")
        with open(batch_path, "w", encoding="utf-8") as stream:
            json.dump({}, stream)
        norm = self._norm()
        norm["tasks"][0]["_native_exec_submitted_argv"] = ["/bin/false"]
        args = argparse.Namespace(batch=batch_path, dry_run=False, json=False)
        with mock.patch.object(cli, "_load_cfg", return_value=self.cfg), mock.patch.object(
            cli, "validate_batch", return_value=norm
        ), mock.patch.object(
            cli, "check_dependency_cycle", return_value=None
        ), mock.patch.object(
            cli, "_is_foreign_host", return_value=False
        ), mock.patch(
            "gsched.fingerprint.compute_fingerprint"
        ) as fingerprint:
            rc, _stdout, stderr = self._capture(cli.cmd_submit, args)

        self.assertEqual(1, rc)
        self.assertIn("submitted argv", stderr)
        fingerprint.assert_not_called()
        with state.connect() as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0])

    def test_submit_rejects_empty_native_digest_before_fingerprint_or_write(self) -> None:
        batch_path = os.path.join(self.tmp.name, "batch.json")
        with open(batch_path, "w", encoding="utf-8") as stream:
            json.dump({}, stream)
        norm = self._norm()
        norm["tasks"][0]["_native_exec_profile_sha256"] = ""
        args = argparse.Namespace(batch=batch_path, dry_run=False, json=False)
        with mock.patch.object(cli, "_load_cfg", return_value=self.cfg), mock.patch.object(
            cli, "validate_batch", return_value=norm
        ), mock.patch.object(
            cli, "check_dependency_cycle", return_value=None
        ), mock.patch.object(
            cli, "_is_foreign_host", return_value=False
        ), mock.patch(
            "gsched.fingerprint.compute_fingerprint"
        ) as fingerprint:
            rc, _stdout, stderr = self._capture(cli.cmd_submit, args)

        self.assertEqual(1, rc)
        self.assertIn("sha256", stderr)
        fingerprint.assert_not_called()
        with state.connect() as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0])

    def test_resubmit_rejects_strict_batch_before_fingerprint_or_write(self) -> None:
        self._seed_resubmit_target(mode="strict")
        args = argparse.Namespace(
            task="batch:task",
            failed=False,
            resubmit_all=False,
            dry_run=False,
        )
        with mock.patch.object(cli, "_load_cfg", return_value=self.cfg), mock.patch(
            "gsched.fingerprint.compute_fingerprint"
        ) as fingerprint, mock.patch.object(
            state, "insert_task"
        ) as insert_task, mock.patch.object(
            state, "insert_job"
        ) as insert_job:
            rc, _stdout, stderr = self._capture(cli.cmd_resubmit, args)

        self.assertEqual(1, rc)
        self.assertIn("strict", stderr)
        fingerprint.assert_not_called()
        insert_task.assert_not_called()
        insert_job.assert_not_called()

    def test_retry_rejects_strict_batch_before_state_write(self) -> None:
        self._seed_resubmit_target(mode="strict")
        args = argparse.Namespace(task="batch:task")
        with mock.patch.object(
            cli, "_load_cfg", return_value=self.cfg
        ), mock.patch.object(state, "update_job") as update_job:
            rc, _stdout, stderr = self._capture(cli.cmd_retry, args)

        self.assertEqual(1, rc)
        self.assertIn("strict", stderr)
        update_job.assert_not_called()

    def test_retry_rejects_native_metadata_in_legacy_mix_batch(self) -> None:
        self._seed_resubmit_target(
            mode="mix",
            metadata={"_native_exec_profile_id": "partial-corrupt-state"},
        )
        args = argparse.Namespace(task="batch:task")
        with mock.patch.object(
            cli, "_load_cfg", return_value=self.cfg
        ), mock.patch.object(state, "update_job") as update_job:
            rc, _stdout, stderr = self._capture(cli.cmd_retry, args)

        self.assertEqual(1, rc)
        self.assertIn("原生执行绑定", stderr)
        update_job.assert_not_called()

    def test_resubmit_rejects_any_native_metadata_even_in_dry_run(self) -> None:
        self._seed_resubmit_target(
            mode="mix",
            metadata={"_native_exec_profile_id": "partial-corrupt-state"},
        )
        args = argparse.Namespace(
            task="batch:task",
            failed=False,
            resubmit_all=False,
            dry_run=True,
        )
        with mock.patch.object(cli, "_load_cfg", return_value=self.cfg), mock.patch.object(
            state, "launch_marker_active", return_value=False
        ), mock.patch(
            "gsched.fingerprint.compute_fingerprint"
        ) as fingerprint, mock.patch.object(
            state, "insert_task"
        ) as insert_task, mock.patch.object(
            state, "insert_job"
        ) as insert_job:
            rc, _stdout, stderr = self._capture(cli.cmd_resubmit, args)

        self.assertEqual(1, rc)
        self.assertIn("原生执行绑定", stderr)
        fingerprint.assert_not_called()
        insert_task.assert_not_called()
        insert_job.assert_not_called()

    def test_config_set_treats_native_profiles_as_cold(self) -> None:
        patch_path = os.path.join(self.tmp.name, "patch.json")
        with open(patch_path, "w", encoding="utf-8") as stream:
            json.dump(
                {
                    "native_exec_profiles": {
                        "m2b-preparation-v1": {
                            "submitted_argv": ["/bin/false"]
                        }
                    }
                },
                stream,
            )
        args = argparse.Namespace(file=patch_path, yes=True)
        with mock.patch.object(cli, "_load_cfg", return_value=self.cfg), mock.patch.object(
            state, "connect"
        ) as connect:
            rc, _stdout, stderr = self._capture(cli.cmd_config_set, args)

        self.assertEqual(1, rc)
        self.assertIn("native_exec_profiles", stderr)
        self.assertIn("冷键", stderr)
        connect.assert_not_called()

    def test_config_set_treats_referenced_project_root_as_cold(self) -> None:
        new_root = os.path.join(self.tmp.name, "new-root")
        os.mkdir(new_root)
        patch_path = os.path.join(self.tmp.name, "root-patch.json")
        with open(patch_path, "w", encoding="utf-8") as stream:
            json.dump({"projects": {"p": {"root": new_root}}}, stream)
        args = argparse.Namespace(file=patch_path, yes=True)
        with mock.patch.object(
            cli, "_load_cfg", return_value=self.cfg
        ), mock.patch.object(state, "connect") as connect:
            rc, _stdout, stderr = self._capture(cli.cmd_config_set, args)

        self.assertEqual(1, rc)
        self.assertIn("native_exec_project_roots", stderr)
        self.assertIn("冷键", stderr)
        connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
