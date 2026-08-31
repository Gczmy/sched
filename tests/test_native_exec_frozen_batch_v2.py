from __future__ import annotations

import copy
import io
import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from gsched import cli
from gsched.native_exec import (
    NATIVE_EXEC_PROFILE_V2_SCHEMA,
    NATIVE_EXEC_V2_CONTRACT_FIELD,
    NATIVE_EXEC_V2_ROOT_KEYS,
    NATIVE_EXEC_V2_TASK_KEYS,
    NativeExecProfileError,
    native_exec_profile_sha256,
    reattest_native_exec_profile,
    resolve_native_exec_profile,
    validate_native_exec_profiles,
)
from gsched.schema import SchemaError, validate_batch


class FrozenBatchProfileV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = os.path.join(self.tmp.name, "project")
        self.runtime = os.path.join(self.tmp.name, "runtime")
        os.mkdir(self.root)
        os.mkdir(self.runtime)
        self.argv = ["/usr/bin/python3", "-I", "-S", "bootstrap.py"]
        self.batch_env = {
            "PYTHONNOUSERSITE": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "OMP_NUM_THREADS": "1",
            "MKL_NUM_THREADS": "1",
            "OPENBLAS_NUM_THREADS": "1",
            "NUMEXPR_NUM_THREADS": "1",
        }
        self.profile = {
            "schema": NATIVE_EXEC_PROFILE_V2_SCHEMA,
            "mode": "strict",
            "project": "p",
            "batch_name": "frozen-v2",
            "task_id": "probe",
            "submitted_argv": list(self.argv),
            "cwd": "{PROJECT:p}",
            "depends_on": [],
            "_protocol": "scheduler_probe_runtime_end_attestation_v1",
            "batch_env": dict(self.batch_env),
            "task_env": {},
            "runtime": {"prefix": self.runtime},
            "duration_min": 1440,
            "max_retry": 0,
            "resources": {"gpu": 0, "cpus": 1},
            "artifacts": {},
        }
        self.cfg = {
            "schema_version": 1,
            "user": "test",
            "node": "test-node",
            "gpus": [],
            "default_project": "p",
            "projects": {"p": {"root": self.root, "git": True}},
            "venvs": {},
            "native_exec_profiles": {"frozen-profile-v2": copy.deepcopy(self.profile)},
        }

    def batch(self) -> dict:
        return {
            "name": "frozen-v2",
            "project": "p",
            "mode": "strict",
            "cwd": "{PROJECT:p}",
            "depends_on": [],
            "_protocol": "scheduler_probe_runtime_end_attestation_v1",
            "env": dict(self.batch_env),
            "tasks": [
                {
                    "id": "probe",
                    "cmd": list(self.argv),
                    "env": {},
                    "runtime": {"prefix": self.runtime},
                    "duration_min": 1440,
                    "max_retry": 0,
                    "artifacts": {},
                    "resources": {"gpu": 0, "cpus": 1},
                }
            ],
        }

    def contract(self) -> dict:
        return {
            "cwd": self.profile["cwd"],
            "depends_on": [],
            "_protocol": self.profile["_protocol"],
            "batch_env": dict(self.batch_env),
            "task_env": {},
            "runtime": {"prefix": self.runtime},
            "duration_min": 1440,
            "max_retry": 0,
            "resources": {"gpu": 0, "cpus": 1},
            "artifacts": {},
        }

    def test_exact_frozen_batch_normalizes_without_changing_logical_argv(self) -> None:
        normalized = validate_batch(self.batch(), self.cfg)
        task = normalized["tasks"][0]

        self.assertEqual(self.batch_env, normalized["env"])
        self.assertEqual({}, task["env"])
        self.assertEqual({"prefix": self.runtime}, task["runtime"])
        self.assertEqual(self.runtime, task["runtime_prefix"])
        self.assertIs(task["git"], False)
        self.assertEqual(self.argv, task["cmd"])
        self.assertEqual(self.argv, task["_native_exec_submitted_argv"])
        self.assertEqual(self.contract(), task[NATIVE_EXEC_V2_CONTRACT_FIELD])

    def test_v2_resolve_and_reattest_bind_the_complete_contract(self) -> None:
        resolved = resolve_native_exec_profile(
            self.cfg,
            mode="strict",
            project="p",
            batch_name="frozen-v2",
            task_id="probe",
            submitted_argv=self.argv,
            batch_contract=self.contract(),
        )
        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(NATIVE_EXEC_PROFILE_V2_SCHEMA, resolved["schema"])
        self.assertEqual(self.contract(), resolved["contract"])
        self.assertEqual(
            resolved,
            reattest_native_exec_profile(
                self.cfg,
                mode="strict",
                project="p",
                batch_name="frozen-v2",
                task_id="probe",
                profile_id=resolved["profile_id"],
                profile_sha256=resolved["profile_sha256"],
                submitted_argv=self.argv,
                batch_contract=self.contract(),
            ),
        )
        drifted = self.contract()
        drifted["_protocol"] = "other"
        with self.assertRaises(NativeExecProfileError):
            reattest_native_exec_profile(
                self.cfg,
                mode="strict",
                project="p",
                batch_name="frozen-v2",
                task_id="probe",
                profile_id=resolved["profile_id"],
                profile_sha256=resolved["profile_sha256"],
                submitted_argv=self.argv,
                batch_contract=drifted,
            )

    def test_root_and_task_keysets_are_exact_and_git_must_be_absent(self) -> None:
        for key in sorted(NATIVE_EXEC_V2_ROOT_KEYS):
            spec = self.batch()
            spec.pop(key)
            with self.subTest(scope="root", missing=key), self.assertRaises(SchemaError):
                validate_batch(spec, self.cfg)
        for key in sorted(NATIVE_EXEC_V2_TASK_KEYS):
            spec = self.batch()
            spec["tasks"][0].pop(key)
            with self.subTest(scope="task", missing=key), self.assertRaises(SchemaError):
                validate_batch(spec, self.cfg)
        for key, value in {
            "notify": False,
            "priority": 0,
            "force_rerun": False,
            "sweep": None,
            "gpus": [],
        }.items():
            spec = self.batch()
            spec[key] = value
            with self.subTest(scope="root", extra=key), self.assertRaises(SchemaError):
                validate_batch(spec, self.cfg)
        for key, value in {
            "git": False,
            "cwd": "{PROJECT:p}",
            "stages": None,
            "paths_escape": False,
            "probes": None,
            "progress_regex": "x",
        }.items():
            spec = self.batch()
            spec["tasks"][0][key] = value
            with self.subTest(scope="task", extra=key), self.assertRaises(SchemaError):
                validate_batch(spec, self.cfg)

    def test_every_frozen_public_value_rejects_drift(self) -> None:
        other_runtime = os.path.join(self.tmp.name, "other-runtime")
        os.mkdir(other_runtime)
        mutations = []

        def changed(mutator):
            value = self.batch()
            mutator(value)
            mutations.append(value)

        changed(lambda value: value.__setitem__("cwd", self.root))
        changed(lambda value: value["depends_on"].append("prior"))
        changed(lambda value: value.__setitem__("_protocol", "other"))
        changed(lambda value: value["env"].__setitem__("OMP_NUM_THREADS", "2"))
        changed(lambda value: value["tasks"][0]["env"].__setitem__("X", "1"))
        changed(lambda value: value["tasks"][0].__setitem__("runtime", {"prefix": other_runtime}))
        changed(lambda value: value["tasks"][0].__setitem__("duration_min", 1440.0))
        changed(lambda value: value["tasks"][0].__setitem__("max_retry", 1))
        changed(lambda value: value["tasks"][0].__setitem__("resources", {"gpu": 0, "cpus": 2}))
        changed(lambda value: value["tasks"][0].__setitem__("artifacts", {"x": {"path": "x"}}))
        changed(lambda value: value.__setitem__("name", "other"))
        changed(lambda value: value.__setitem__("project", "other"))
        changed(lambda value: value.__setitem__("mode", "mix"))
        changed(lambda value: value["tasks"][0].__setitem__("id", "other"))
        changed(lambda value: value["tasks"][0]["cmd"].append("--other"))

        for spec in mutations:
            with self.subTest(spec=spec), self.assertRaises(SchemaError):
                validate_batch(spec, self.cfg)

    def test_v2_registry_is_exact_and_digest_binds_contract(self) -> None:
        validated = validate_native_exec_profiles(
            self.cfg["native_exec_profiles"], projects=self.cfg["projects"]
        )
        digest = validated["frozen-profile-v2"]["profile_sha256"]
        changed = copy.deepcopy(self.profile)
        changed["_protocol"] = "changed-label"
        self.assertNotEqual(digest, native_exec_profile_sha256("frozen-profile-v2", changed))

        for mutation in ("missing", "extra"):
            profile = copy.deepcopy(self.profile)
            if mutation == "missing":
                profile.pop("artifacts")
            else:
                profile["enabled"] = True
            with self.subTest(mutation=mutation), self.assertRaises(NativeExecProfileError):
                validate_native_exec_profiles(
                    {"frozen-profile-v2": profile}, projects=self.cfg["projects"]
                )

    def test_v1_digest_vector_remains_byte_identical(self) -> None:
        legacy = {
            "mode": "strict",
            "project": "p",
            "batch_name": "native-batch",
            "task_id": "native-task",
            "submitted_argv": ["/usr/bin/python3", "-I", "-S", "job.py"],
        }
        self.assertEqual(
            "becb9e55be94b93a955565f6746dc6537bbfc215fd9ba2aa6eac37f4fded3100",
            native_exec_profile_sha256("formal-v1", legacy),
        )

    def test_bool_and_float_numeric_aliases_are_rejected(self) -> None:
        mutations = []
        for duration in (True, 1440.0):
            spec = self.batch()
            spec["tasks"][0]["duration_min"] = duration
            mutations.append(spec)
        for resources in (
            {"gpu": False, "cpus": 1},
            {"gpu": 0, "cpus": True},
            {"gpu": 0.0, "cpus": 1},
        ):
            spec = self.batch()
            spec["tasks"][0]["resources"] = resources
            mutations.append(spec)
        for spec in mutations:
            with self.subTest(spec=spec), self.assertRaises(SchemaError):
                validate_batch(spec, self.cfg)

    def test_cli_submission_fails_before_state_fingerprint_or_process_work(self) -> None:
        args = SimpleNamespace(batch="frozen.json", dry_run=False, json=False)
        with (
            mock.patch("builtins.open", return_value=io.StringIO(json.dumps(self.batch()))),
            mock.patch("gsched.cli._load_cfg", return_value=self.cfg),
            mock.patch("gsched.cli.state.connect") as state_connect,
            mock.patch("gsched.fingerprint.compute_fingerprint") as fingerprint,
            mock.patch("subprocess.Popen") as popen,
            mock.patch("sys.stderr", new_callable=io.StringIO) as stderr,
        ):
            result = cli.cmd_submit(args)

        self.assertEqual(1, result)
        self.assertIn("仅完成冻结合同兼容校验", stderr.getvalue())
        state_connect.assert_not_called()
        fingerprint.assert_not_called()
        popen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
