from __future__ import annotations

import copy
import json
import os
import tempfile
import unittest
from typing import Any

from gsched import config
from gsched.native_exec import (
    NATIVE_EXEC_INTERNAL_FIELDS,
    NativeExecProfileError,
    native_exec_profile_sha256,
    reattest_native_exec_profile,
    resolve_native_exec_profile,
    validate_native_exec_profiles,
)
from gsched.schema import SchemaError, validate_batch


class NativeExecProfileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.argv: list[str] = ["/usr/bin/python3", "-I", "-S", "job.py"]
        self.profile: dict[str, Any] = {
            "mode": "strict",
            "project": "p",
            "batch_name": "native-batch",
            "task_id": "native-task",
            "submitted_argv": list(self.argv),
        }
        self.cfg: dict[str, Any] = {
            "schema_version": 1,
            "user": "test",
            "node": "test-node",
            "gpus": [],
            "default_project": "p",
            "projects": {"p": {"root": self.tmp.name, "git": False}},
            "venvs": {"py": "/different/python"},
            "native_exec_profiles": {"formal-v1": copy.deepcopy(self.profile)},
        }

    def strict_batch(self, **updates):
        spec = {
            "name": "native-batch",
            "mode": "strict",
            "project": "p",
            "tasks": [
                {
                    "id": "native-task",
                    "cmd": list(self.argv),
                    "git": False,
                    "resources": {"gpu": 0, "cpus": 1},
                    "max_retry": 0,
                }
            ],
        }
        spec.update(updates)
        return spec

    def test_profile_digest_is_canonical_and_binds_profile_identity(self) -> None:
        digest = native_exec_profile_sha256("formal-v1", self.profile)

        self.assertEqual(64, len(digest))
        self.assertEqual(digest, native_exec_profile_sha256("formal-v1", {
            "submitted_argv": list(self.argv),
            "task_id": "native-task",
            "batch_name": "native-batch",
            "project": "p",
            "mode": "strict",
        }))
        self.assertNotEqual(
            digest, native_exec_profile_sha256("renamed-v1", self.profile)
        )
        changed = copy.deepcopy(self.profile)
        changed["submitted_argv"].append("--changed")
        self.assertNotEqual(
            digest, native_exec_profile_sha256("formal-v1", changed)
        )
        with self.assertRaises(NativeExecProfileError):
            native_exec_profile_sha256("bad/profile", self.profile)
        with self.assertRaises(NativeExecProfileError):
            native_exec_profile_sha256("formal-v1", {})

    def test_registry_requires_exact_schema_and_unique_match_tuple(self) -> None:
        invalid_values: list[Any] = []
        missing = copy.deepcopy(self.profile)
        missing.pop("task_id")
        invalid_values.append({"formal-v1": missing})
        extra = copy.deepcopy(self.profile)
        extra["enabled"] = True
        invalid_values.append({"formal-v1": extra})
        wrong_mode = copy.deepcopy(self.profile)
        wrong_mode["mode"] = "mix"
        invalid_values.append({"formal-v1": wrong_mode})
        bad_argv = copy.deepcopy(self.profile)
        bad_argv["submitted_argv"] = []
        invalid_values.append({"formal-v1": bad_argv})
        relative_argv = copy.deepcopy(self.profile)
        relative_argv["submitted_argv"] = ["python3", "job.py"]
        invalid_values.append({"formal-v1": relative_argv})
        invalid_values.append({"bad/profile": copy.deepcopy(self.profile)})
        non_string_key = copy.deepcopy(self.profile)
        non_string_key[1] = "extra"
        invalid_values.append({"formal-v1": non_string_key})
        unknown_project = copy.deepcopy(self.profile)
        unknown_project["project"] = "unknown"
        invalid_values.append({"formal-v1": unknown_project})
        invalid_values.append(
            {
                "formal-v1": copy.deepcopy(self.profile),
                "duplicate-v1": copy.deepcopy(self.profile),
            }
        )
        duplicate_batch_name = copy.deepcopy(self.profile)
        duplicate_batch_name["task_id"] = "different-task"
        invalid_values.append(
            {
                "formal-v1": copy.deepcopy(self.profile),
                "different-v1": duplicate_batch_name,
            }
        )

        for raw in invalid_values:
            with self.subTest(raw=raw), self.assertRaises(NativeExecProfileError):
                validate_native_exec_profiles(raw, projects=self.cfg["projects"])

    def test_profile_project_root_must_exist_and_not_be_a_symlink(self) -> None:
        missing_projects = copy.deepcopy(self.cfg["projects"])
        missing_projects["p"]["root"] = os.path.join(
            self.tmp.name, "missing"
        )
        with self.assertRaises(NativeExecProfileError):
            validate_native_exec_profiles(
                self.cfg["native_exec_profiles"],
                projects=missing_projects,
            )

        target = os.path.join(self.tmp.name, "target")
        link = os.path.join(self.tmp.name, "link")
        os.mkdir(target)
        os.symlink(target, link)
        linked_projects = copy.deepcopy(self.cfg["projects"])
        linked_projects["p"]["root"] = link
        with self.assertRaises(NativeExecProfileError):
            validate_native_exec_profiles(
                self.cfg["native_exec_profiles"],
                projects=linked_projects,
            )

    def test_load_config_validates_cold_profile_map(self) -> None:
        valid_path = os.path.join(self.tmp.name, "valid-config.json")
        with open(valid_path, "w", encoding="utf-8") as stream:
            json.dump(self.cfg, stream)
        loaded = config.load_config(valid_path, apply_runtime_state=False)
        self.assertEqual(self.cfg["native_exec_profiles"], loaded["native_exec_profiles"])

        invalid_profiles: list[Any] = [
            None,
            [],
            {"formal-v1": {**self.profile, "extra": 1}},
        ]
        for invalid in invalid_profiles:
            with self.subTest(invalid=invalid):
                bad = copy.deepcopy(self.cfg)
                bad["native_exec_profiles"] = invalid
                path = os.path.join(self.tmp.name, f"bad-{len(str(invalid))}.json")
                with open(path, "w", encoding="utf-8") as stream:
                    json.dump(bad, stream)
                with self.assertRaises(config.ConfigError):
                    config.load_config(path, apply_runtime_state=False)

    def test_resolver_returns_detached_exact_profile(self) -> None:
        resolved = resolve_native_exec_profile(
            self.cfg,
            mode="strict",
            project="p",
            batch_name="native-batch",
            task_id="native-task",
            submitted_argv=self.argv,
        )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(
            {
                "mode",
                "project",
                "batch_name",
                "task_id",
                "submitted_argv",
                "profile_id",
                "profile_sha256",
            },
            set(resolved),
        )
        self.assertEqual("formal-v1", resolved["profile_id"])
        resolved["submitted_argv"].append("--caller-mutation")
        self.assertEqual(self.argv, self.profile["submitted_argv"])
        self.assertEqual(self.argv, self.cfg["native_exec_profiles"]["formal-v1"]["submitted_argv"])

    def test_resolver_requires_every_exact_match_field(self) -> None:
        base = {
            "mode": "strict",
            "project": "p",
            "batch_name": "native-batch",
            "task_id": "native-task",
            "submitted_argv": list(self.argv),
        }
        changes = {
            "mode": "mix",
            "project": "other",
            "batch_name": "other-batch",
            "task_id": "other-task",
            "submitted_argv": self.argv + ["--other"],
        }
        for key, value in changes.items():
            with self.subTest(key=key):
                candidate = dict(base)
                candidate[key] = value
                self.assertIsNone(
                    resolve_native_exec_profile(self.cfg, **candidate)
                )

    def test_persisted_reattestation_fails_closed_on_every_drift(self) -> None:
        resolved = resolve_native_exec_profile(
            self.cfg,
            mode="strict",
            project="p",
            batch_name="native-batch",
            task_id="native-task",
            submitted_argv=self.argv,
        )
        assert resolved is not None
        persisted = {
            "mode": "strict",
            "project": "p",
            "batch_name": "native-batch",
            "task_id": "native-task",
            "profile_id": resolved["profile_id"],
            "profile_sha256": resolved["profile_sha256"],
            "submitted_argv": list(resolved["submitted_argv"]),
        }
        self.assertEqual(
            resolved, reattest_native_exec_profile(self.cfg, **persisted)
        )

        drift_values = {
            "mode": "mix",
            "project": "other",
            "batch_name": "other-batch",
            "task_id": "other-task",
            "profile_id": "other-v1",
            "profile_sha256": "0" * 64,
            "submitted_argv": self.argv + ["--other"],
        }
        for key, value in drift_values.items():
            with self.subTest(key=key):
                drifted = copy.deepcopy(persisted)
                drifted[key] = value
                with self.assertRaises(NativeExecProfileError):
                    reattest_native_exec_profile(self.cfg, **drifted)

        for key, values in (
            ("profile_id", (None, "")),
            ("profile_sha256", (None, "", "A" * 64)),
            ("submitted_argv", (None, [])),
        ):
            for value in values:
                with self.subTest(empty_field=key, value=value):
                    drifted = copy.deepcopy(persisted)
                    drifted[key] = value
                    with self.assertRaises(NativeExecProfileError):
                        reattest_native_exec_profile(self.cfg, **drifted)

        changed_cfg = copy.deepcopy(self.cfg)
        changed_cfg["native_exec_profiles"]["formal-v1"]["submitted_argv"].append(
            "--admin-change"
        )
        with self.assertRaises(NativeExecProfileError):
            reattest_native_exec_profile(changed_cfg, **persisted)

    def test_strict_exact_match_injects_only_scheduler_owned_binding(self) -> None:
        normalized = validate_batch(self.strict_batch(), self.cfg)
        task = normalized["tasks"][0]
        resolved = resolve_native_exec_profile(
            self.cfg,
            mode="strict",
            project="p",
            batch_name="native-batch",
            task_id="native-task",
            submitted_argv=self.argv,
        )
        assert resolved is not None

        self.assertEqual("strict", normalized["mode"])
        self.assertEqual("formal-v1", task["_native_exec_profile_id"])
        self.assertEqual(
            resolved["profile_sha256"], task["_native_exec_profile_sha256"]
        )
        self.assertEqual(self.argv, task["_native_exec_submitted_argv"])
        self.assertIsNot(self.argv, task["_native_exec_submitted_argv"])

    def test_native_profile_batch_name_is_reserved_from_mix_mode(self) -> None:
        spec = self.strict_batch()
        spec["mode"] = "mix"
        with self.assertRaisesRegex(SchemaError, "保留"):
            validate_batch(spec, self.cfg)

    def test_strict_rejects_non_exact_or_non_single_cmd_submission(self) -> None:
        cases: list[tuple[dict[str, Any], dict[str, Any]]] = []
        no_profile_cfg = copy.deepcopy(self.cfg)
        no_profile_cfg.pop("native_exec_profiles")
        cases.append((self.strict_batch(), no_profile_cfg))
        wrong_name = self.strict_batch(name="other")
        cases.append((wrong_name, self.cfg))
        wrong_cmd = self.strict_batch()
        wrong_cmd["tasks"][0]["cmd"].append("--other")
        cases.append((wrong_cmd, self.cfg))
        two_tasks = self.strict_batch()
        two_tasks["tasks"].append(copy.deepcopy(two_tasks["tasks"][0]))
        two_tasks["tasks"][1]["id"] = "second"
        cases.append((two_tasks, self.cfg))
        stages = self.strict_batch()
        stages["tasks"][0]["stages"] = None
        cases.append((stages, self.cfg))
        implicit_retry = self.strict_batch()
        implicit_retry["tasks"][0].pop("max_retry")
        cases.append((implicit_retry, self.cfg))
        retry = self.strict_batch()
        retry["tasks"][0]["max_retry"] = 1
        cases.append((retry, self.cfg))
        other_cwd = self.strict_batch(cwd=os.path.join(self.tmp.name, "other"))
        cases.append((other_cwd, self.cfg))
        explicit_runtime = self.strict_batch()
        explicit_runtime["tasks"][0]["runtime"] = {"prefix": self.tmp.name}
        cases.append((explicit_runtime, self.cfg))
        missing_git = self.strict_batch()
        missing_git["tasks"][0].pop("git")
        cases.append((missing_git, self.cfg))
        git_probe = self.strict_batch()
        git_probe["tasks"][0]["git"] = True
        cases.append((git_probe, self.cfg))
        null_git = self.strict_batch()
        null_git["tasks"][0]["git"] = None
        cases.append((null_git, self.cfg))
        artifacts = self.strict_batch()
        artifacts["tasks"][0]["artifacts"] = {
            "out": {"path": "out.json", "check": "json"}
        }
        cases.append((artifacts, self.cfg))
        paths_escape = self.strict_batch()
        paths_escape["tasks"][0]["paths_escape"] = True
        cases.append((paths_escape, self.cfg))
        probes = self.strict_batch()
        probes["tasks"][0]["probes"] = {"ready_on_log": "READY"}
        cases.append((probes, self.cfg))
        batch_env = self.strict_batch(env={"PATH": "/untrusted"})
        cases.append((batch_env, self.cfg))
        task_env = self.strict_batch()
        task_env["tasks"][0]["env"] = {"PYTHONPATH": "/untrusted"}
        cases.append((task_env, self.cfg))
        gpu_resource = self.strict_batch()
        gpu_resource["tasks"][0]["resources"] = {"gpu": 1, "cpus": 1}
        cases.append((gpu_resource, self.cfg))
        extra_cpu = self.strict_batch()
        extra_cpu["tasks"][0]["resources"] = {"gpu": 0, "cpus": 2}
        cases.append((extra_cpu, self.cfg))
        sweep = self.strict_batch(sweep={"matrix": {"seed": [1]}})
        cases.append((sweep, self.cfg))
        cases.append((self.strict_batch(sweep=None), self.cfg))

        template_cfg = copy.deepcopy(self.cfg)
        template_cfg["native_exec_profiles"]["formal-v1"]["submitted_argv"] = [
            "{VENV:py}", "-I", "-S", "job.py"
        ]
        template_batch = self.strict_batch()
        template_batch["tasks"][0]["cmd"] = [
            "{VENV:py}", "-I", "-S", "job.py"
        ]
        cases.append((template_batch, template_cfg))

        for spec, cfg in cases:
            with self.subTest(spec=spec), self.assertRaises(SchemaError):
                validate_batch(spec, cfg)

    def test_user_cannot_supply_internal_fields_at_batch_or_task_scope(self) -> None:
        for field in sorted(NATIVE_EXEC_INTERNAL_FIELDS):
            with self.subTest(scope="batch", field=field):
                spec = self.strict_batch()
                spec[field] = "spoof"
                with self.assertRaises(SchemaError):
                    validate_batch(spec, self.cfg)
            with self.subTest(scope="task", field=field):
                spec = self.strict_batch()
                spec["tasks"][0][field] = "spoof"
                with self.assertRaises(SchemaError):
                    validate_batch(spec, self.cfg)

    def test_mix_normalization_is_byte_identical_with_or_without_registry(self) -> None:
        spec = {
            "name": "ordinary",
            "mode": "mix",
            "project": "p",
            "tasks": [
                {
                    "id": "task",
                    "cmd": ["/bin/true"],
                    "resources": {"gpu": 0, "cpus": 1},
                }
            ],
        }
        cfg_without = copy.deepcopy(self.cfg)
        cfg_without.pop("native_exec_profiles")

        before = json.dumps(validate_batch(spec, cfg_without), separators=(",", ":"))
        after = json.dumps(validate_batch(spec, self.cfg), separators=(",", ":"))

        self.assertEqual(before, after)
        normalized = json.loads(after)
        self.assertTrue(
            NATIVE_EXEC_INTERNAL_FIELDS.isdisjoint(normalized["tasks"][0])
        )


if __name__ == "__main__":
    unittest.main()
