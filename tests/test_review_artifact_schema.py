from __future__ import annotations

import math
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from gsched import state
from gsched.artifacts import (
    ArtifactError,
    check_artifact,
    check_declared_artifacts,
    unlink_artifact,
)
from gsched.dispatcher import Dispatcher
from gsched.executor import Executor, process_start_token
from gsched.fingerprint import compute_fingerprint
from gsched.schema import MAX_NORMALIZED_TASKS, SchemaError, validate_batch


class SchemaFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = self.tmp.name
        self.cfg = {
            "schema_version": 1,
            "user": "test",
            "node": "test-node",
            "gpus": [0, 1],
            "default_project": "p",
            "projects": {"p": {"root": self.root, "git": False}},
            "venvs": {},
        }

    def batch(self, **updates):
        spec = {
            "name": "review-batch",
            "project": "p",
            "tasks": [{"id": "task", "cmd": ["/bin/true"]}],
        }
        spec.update(updates)
        return spec

    def assert_rejected(self, spec) -> None:
        with self.assertRaises(SchemaError):
            validate_batch(spec, self.cfg)


class ReviewSchemaTests(SchemaFixture):
    """Submission-time contracts for S-H10, S-M05, S-M06, and S-L01."""

    def test_s_h10_rejects_non_mapping_or_non_string_probe_rules(self) -> None:
        invalid = ["not-an-object", [], {"fail_on_log": 12}]
        for probes in invalid:
            with self.subTest(probes=probes):
                spec = self.batch()
                spec["tasks"][0]["probes"] = probes
                self.assert_rejected(spec)

    def test_s_h10_rejects_invalid_artifact_regex_at_submission(self) -> None:
        for staged in (False, True):
            with self.subTest(staged=staged):
                artifact = {"result": {"path": "result.txt", "regex": "["}}
                task = {"id": "task", "cmd": ["/bin/true"], "artifacts": artifact}
                if staged:
                    task = {
                        "id": "task",
                        "stages": [{"cmd": ["/bin/true"], "artifacts": artifact}],
                    }
                self.assert_rejected(self.batch(tasks=[task]))

    def test_s_h10_rejects_pathological_or_nul_artifact_patterns(self) -> None:
        deeply_nested = "(" * 1_000 + "x" + ")" * 1_000
        for field, value in (
            ("regex", deeply_nested),
            ("regex", "a\0b"),
            ("has_key", "a\0b"),
        ):
            with self.subTest(field=field, value_length=len(value)):
                task = {
                    "id": "task",
                    "cmd": ["/bin/true"],
                    "artifacts": {
                        "result": {"path": "result.txt", field: value}
                    },
                }
                self.assert_rejected(self.batch(tasks=[task]))

    def test_s_h10_pathological_stored_regex_is_a_failed_rule(self) -> None:
        path = os.path.join(self.root, "artifact.txt")
        with open(path, "w", encoding="utf-8") as stream:
            stream.write("content")
        deeply_nested = "(" * 1_000 + "x" + ")" * 1_000

        self.assertIsNotNone(check_artifact(path, {"regex": deeply_nested}))

    def test_s_h10_bad_stored_regex_is_a_failed_rule_not_a_tick_exception(self) -> None:
        path = os.path.join(self.root, "artifact.txt")
        with open(path, "w", encoding="utf-8") as stream:
            stream.write("content")
        dispatcher = Dispatcher.__new__(Dispatcher)

        result = dispatcher._check_artifacts(
            {"result": {"path": path, "regex": "["}}, self.root
        )

        self.assertFalse(result)
        self.assertIsNotNone(check_artifact(path, {"regex": "["}))

    def test_runtime_rejects_intermediate_symlink_escape_without_opt_in(
        self,
    ) -> None:
        outside = tempfile.TemporaryDirectory()
        self.addCleanup(outside.cleanup)
        artifact = os.path.join(outside.name, "result.txt")
        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write("valid")
        os.symlink(outside.name, os.path.join(self.root, "redirect"))
        spec = {
            "artifacts": {
                "result": {
                    "path": os.path.join("redirect", "result.txt"),
                    "min_bytes": 1,
                }
            },
            "paths_escape": False,
        }

        self.assertFalse(check_declared_artifacts(spec, self.root))
        spec["paths_escape"] = True
        self.assertTrue(check_declared_artifacts(spec, self.root))

    def test_confined_artifact_root_symlink_is_rejected_for_read_and_unlink(
        self,
    ) -> None:
        outside = tempfile.TemporaryDirectory()
        self.addCleanup(outside.cleanup)
        artifact = os.path.join(outside.name, "result.txt")
        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write("must survive")
        root_link = os.path.join(self.root, "replaced-cwd")
        os.symlink(outside.name, root_link)
        spec = {
            "artifacts": {"result": {"path": "result.txt", "min_bytes": 1}},
            "paths_escape": False,
        }

        self.assertFalse(check_declared_artifacts(spec, root_link))
        self.assertFalse(unlink_artifact(root_link, "result.txt"))
        with self.assertRaises(ArtifactError):
            unlink_artifact(
                root_link,
                "result.txt",
                raise_on_error=True,
            )
        self.assertTrue(os.path.isfile(artifact))

    def test_strict_unlink_distinguishes_deleted_absent_and_io_failure(self) -> None:
        artifact = os.path.join(self.root, "strict-result.txt")
        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write("result")

        self.assertTrue(
            unlink_artifact(
                self.root,
                "strict-result.txt",
                raise_on_error=True,
            )
        )
        self.assertFalse(
            unlink_artifact(
                self.root,
                "strict-result.txt",
                raise_on_error=True,
            )
        )

        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write("result")
        with mock.patch(
            "gsched.artifacts.os.unlink",
            side_effect=PermissionError("denied"),
        ):
            self.assertFalse(unlink_artifact(self.root, "strict-result.txt"))
            with self.assertRaisesRegex(ArtifactError, "denied"):
                unlink_artifact(
                    self.root,
                    "strict-result.txt",
                    raise_on_error=True,
                )
        self.assertTrue(os.path.isfile(artifact))


    def test_s_h10_rejects_unknown_artifact_rules_at_submit_and_runtime(
        self,
    ) -> None:
        spec = self.batch()
        spec["tasks"][0]["artifacts"] = {
            "result": {"path": "result.txt", "min_size": 100}
        }
        self.assert_rejected(spec)

        path = os.path.join(self.root, "result.txt")
        with open(path, "w", encoding="utf-8") as stream:
            stream.write("x")
        self.assertIsNotNone(
            check_artifact(path, {"path": path, "min_size": 100})
        )

    def test_s_h10_bounds_artifact_count_and_aggregate_rule_size(self) -> None:
        too_many = {
            f"result-{index}": {"path": f"result-{index}.txt"}
            for index in range(65)
        }
        spec = self.batch()
        spec["tasks"][0]["artifacts"] = too_many
        self.assert_rejected(spec)

        oversized_rules = {
            f"result-{index}": {
                "path": f"result-{index}.json",
                "has_key": "x" * 2_000,
            }
            for index in range(64)
        }
        spec = self.batch()
        spec["tasks"][0]["artifacts"] = oversized_rules
        self.assert_rejected(spec)

    def test_s_h10_deeply_nested_stored_json_fails_without_recursion_escape(
        self,
    ) -> None:
        path = os.path.join(self.root, "nested.json")
        with open(path, "w", encoding="utf-8") as stream:
            stream.write("[" * 400_000 + "0" + "]" * 400_000)

        self.assertIsNotNone(check_artifact(path, {"check": "json"}))

    def test_progress_regex_catastrophic_backtracking_is_time_bounded(self) -> None:
        import time

        from gsched.artifacts import bounded_regex_last_match

        started = time.monotonic()
        result = bounded_regex_last_match("(a+)+$", b"a" * 4_095 + b"!")

        self.assertIsNone(result)
        self.assertLess(time.monotonic() - started, 2)

    def test_task_level_project_override_is_rejected_as_unimplemented(self) -> None:
        spec = self.batch()
        spec["tasks"][0]["project"] = "p"
        self.assert_rejected(spec)

    def test_artifact_validation_rejects_symlinks(self) -> None:
        target = os.path.join(self.root, "real-artifact.txt")
        link = os.path.join(self.root, "artifact-link.txt")
        with open(target, "w", encoding="utf-8") as stream:
            stream.write("valid")
        os.symlink(target, link)

        self.assertIsNotNone(check_artifact(link, {}))

    def test_artifact_regex_validation_is_bounded_for_oversized_input(self) -> None:
        import json
        import sys

        path = os.path.join(self.root, "oversized.txt")
        with open(path, "w", encoding="utf-8") as stream:
            stream.write("a" * (2 * 1024 * 1024) + "!")
        code = (
            "import json,sys;"
            "from gsched.artifacts import check_artifact;"
            "print(json.dumps(check_artifact("
            "sys.argv[1], {'regex': '(a+)+$'})))"
        )
        try:
            completed = subprocess.run(
                [sys.executable, "-c", code, path],
                capture_output=True,
                text=True,
                timeout=5,
            )
        except subprocess.TimeoutExpired:
            self.fail("oversized artifact validation exceeded its five-second bound")

        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertIsNotNone(json.loads(completed.stdout))

    def test_s_m05_rejects_unsafe_batch_names(self) -> None:
        invalid = ["/absolute", "..", "../outside", "a/b", "a:b", "界" * 300]
        for name in invalid:
            with self.subTest(name=name):
                self.assert_rejected(self.batch(name=name))

    def test_s_m05_rejects_unsafe_task_ids(self) -> None:
        invalid = ["/absolute", "..", "../outside", "a/b", "a:b", "界" * 300]
        for task_id in invalid:
            with self.subTest(task_id=task_id):
                self.assert_rejected(
                    self.batch(tasks=[{"id": task_id, "cmd": ["/bin/true"]}])
                )

    def test_s_m05_rejects_unsafe_dependency_names(self) -> None:
        invalid = ["/absolute", "..", "../outside", "a/b", "a:b", "界" * 300]
        for dependency in invalid:
            with self.subTest(dependency=dependency):
                self.assert_rejected(self.batch(depends_on=[dependency]))

    def test_s_m06_task_env_requires_string_mapping(self) -> None:
        invalid = [["BROKEN"], {"N": 1}, {"FLAG": False}]
        for env in invalid:
            with self.subTest(env=env):
                spec = self.batch()
                spec["tasks"][0]["env"] = env
                self.assert_rejected(spec)

    def test_s_m06_task_and_stage_command_tokens_are_safe_strings(self) -> None:
        for staged in (False, True):
            for token in (7, None, b"bytes", "bad\0token"):
                with self.subTest(staged=staged, token=token):
                    if staged:
                        tasks = [{"id": "task", "stages": [{"cmd": [token]}]}]
                    else:
                        tasks = [{"id": "task", "cmd": [token]}]
                    self.assert_rejected(self.batch(tasks=tasks))

    def test_s_m06_env_names_and_values_are_safe_strings(self) -> None:
        invalid_envs = [
            {"": "value"},
            {"1INVALID": "value"},
            {"BAD-NAME": "value"},
            {"BAD=NAME": "value"},
            {"BAD\0NAME": "value"},
            {"VALID_NAME": "bad\0value"},
        ]
        for scope in ("batch", "task"):
            for env in invalid_envs:
                with self.subTest(scope=scope, env=env):
                    spec = self.batch()
                    if scope == "batch":
                        spec["env"] = env
                    else:
                        spec["tasks"][0]["env"] = env
                    self.assert_rejected(spec)

    def test_s_m06_env_rejects_shell_bootstrap_and_loader_injection(self) -> None:
        dangerous = (
            "BASH_ENV",
            "ENV",
            "LD_PRELOAD",
            "LD_AUDIT",
            "LD_LIBRARY_PATH",
            "DYLD_INSERT_LIBRARIES",
            "DYLD_LIBRARY_PATH",
            "DYLD_FRAMEWORK_PATH",
        )
        for scope in ("batch", "task"):
            for name in dangerous:
                with self.subTest(scope=scope, name=name):
                    spec = self.batch()
                    env = {name: os.path.join(self.root, "attacker")}
                    if scope == "batch":
                        spec["env"] = env
                    else:
                        spec["tasks"][0]["env"] = env
                    self.assert_rejected(spec)

    def test_s_m06_duration_is_finite_and_strictly_positive(self) -> None:
        invalid = [0, -1, math.nan, math.inf, -math.inf, True, "1"]
        for duration in invalid:
            with self.subTest(duration=duration):
                spec = self.batch()
                spec["tasks"][0]["duration_min"] = duration
                self.assert_rejected(spec)

    def test_s_m06_paths_escape_is_boolean(self) -> None:
        for value in ("false", 0, 1, [], {}):
            with self.subTest(value=value):
                spec = self.batch()
                spec["tasks"][0]["paths_escape"] = value
                self.assert_rejected(spec)

    def test_s_m06_all_numeric_resource_fields_reject_non_finite_values(self) -> None:
        for value in (math.nan, math.inf, -math.inf):
            with self.subTest(value=value):
                spec = self.batch()
                spec["tasks"][0]["resources"] = {"vram_gib": value}
                self.assert_rejected(spec)

    def test_s_l01_rejects_accepted_fields_with_no_runtime_consumer(self) -> None:
        cases = []
        cases.append(self.batch(gpus=[0]))
        task_retry = self.batch()
        task_retry["tasks"][0]["retry_transform"] = {"cmd": ["/bin/false"]}
        cases.append(task_retry)
        stage_retry = self.batch(
            tasks=[{
                "id": "task",
                "stages": [{"cmd": ["/bin/true"], "retry_transform": {}}],
            }]
        )
        cases.append(stage_retry)
        stage_probe = self.batch(
            tasks=[{
                "id": "task",
                "stages": [{"cmd": ["/bin/true"], "probes": {"ready_on_log": "ok"}}],
            }]
        )
        cases.append(stage_probe)

        for index, spec in enumerate(cases):
            with self.subTest(case=index):
                self.assert_rejected(spec)

    def test_sweep_task_limit_is_checked_before_product_or_deepcopy(self) -> None:
        spec = self.batch(
            sweep={
                "matrix": {
                    "seed": list(range(MAX_NORMALIZED_TASKS + 1)),
                }
            }
        )

        with mock.patch("itertools.product") as product, mock.patch(
            "copy.deepcopy"
        ) as deepcopy:
            self.assert_rejected(spec)

        product.assert_not_called()
        deepcopy.assert_not_called()

    def test_plain_and_sweep_batches_share_the_normalized_task_limit(self) -> None:
        task = {"id": "task", "cmd": ["/bin/true"]}
        plain = self.batch(tasks=[task] * (MAX_NORMALIZED_TASKS + 1))
        swept = self.batch(
            tasks=[task, task],
            sweep={"matrix": {"seed": list(range(MAX_NORMALIZED_TASKS // 2 + 1))}},
        )

        for spec in (plain, swept):
            with self.subTest(sweep="sweep" in spec):
                self.assert_rejected(spec)


class ReviewFingerprintTests(unittest.TestCase):
    """Content-sensitive, fail-closed git fingerprints for S-H02."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        clean_git_env = mock.patch.dict(
            os.environ,
            {"GIT_CONFIG_NOSYSTEM": "1", "HOME": self.tmp.name},
            clear=False,
        )
        clean_git_env.start()
        self.addCleanup(clean_git_env.stop)

    def _git(self, repo: str, *args: str) -> str:
        completed = subprocess.run(
            ["git", "-C", repo, *args],
            capture_output=True,
            text=True,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        return completed.stdout

    def _make_repo(self, name: str) -> str:
        repo = os.path.join(self.tmp.name, name)
        os.makedirs(repo)
        self._git(repo, "init", "-q")
        self._git(repo, "config", "user.email", "review@example.test")
        self._git(repo, "config", "user.name", "Review Test")
        with open(os.path.join(repo, "train.py"), "w", encoding="utf-8") as stream:
            stream.write("print('committed')\n")
        self._git(repo, "add", "train.py")
        self._git(repo, "commit", "-qm", "initial")
        return repo

    def _actual_fingerprint(self, repo: str) -> str:
        fingerprint, _stages, _rev = compute_fingerprint(
            ["python", "train.py"], None, repo, True, {}
        )
        self.assertIsNotNone(fingerprint)
        return fingerprint

    @staticmethod
    def _completed(argv, rc=0, stdout=""):
        return subprocess.CompletedProcess(argv, rc, stdout=stdout, stderr="")

    def _fingerprint_with_mutable_diff(self, phase: dict[str, str]) -> str | None:
        def run(argv, **_kwargs):
            if "--is-inside-work-tree" in argv:
                return self._completed(argv, stdout="true\n")
            if "--verify" in argv:
                return self._completed(argv, stdout="a" * 40 + "\n")
            if "status" in argv:
                return self._completed(argv, stdout=" M train.py\n")
            if any(str(arg).startswith("diff") for arg in argv):
                return self._completed(argv, stdout=phase["diff"])
            return self._completed(argv)

        with mock.patch("gsched.fingerprint.subprocess.run", side_effect=run):
            fingerprint, _stages, _rev = compute_fingerprint(
                ["python", "train.py"], None, "/repo", True, {}
            )
        return fingerprint

    def test_s_h02_dirty_content_a_to_b_changes_fingerprint(self) -> None:
        phase = {"diff": "dirty-A"}
        first = self._fingerprint_with_mutable_diff(phase)
        phase["diff"] = "dirty-B"
        second = self._fingerprint_with_mutable_diff(phase)
        self.assertNotEqual(first, second)

    def test_s_h02_staged_content_a_to_b_changes_fingerprint(self) -> None:
        phase = {"diff": "staged-A"}
        first = self._fingerprint_with_mutable_diff(phase)
        phase["diff"] = "staged-B"
        second = self._fingerprint_with_mutable_diff(phase)
        self.assertNotEqual(first, second)

    def test_s_h02_git_content_query_failure_disables_fingerprint_skip(self) -> None:
        def run(argv, **_kwargs):
            if "--is-inside-work-tree" in argv:
                return self._completed(argv, stdout="true\n")
            if "--verify" in argv:
                return self._completed(argv, stdout="a" * 40 + "\n")
            return self._completed(argv, rc=1)

        with mock.patch("gsched.fingerprint.subprocess.run", side_effect=run):
            fingerprint, _stages, _rev = compute_fingerprint(
                ["python", "train.py"], None, "/repo", True, {}
            )
        self.assertIsNone(fingerprint)

    def test_s_h02_git_true_rejects_a_non_worktree_probe(self) -> None:
        def run(argv, **_kwargs):
            if "--is-inside-work-tree" in argv:
                return self._completed(argv, stdout="false\n")
            raise AssertionError(f"unexpected Git query after non-worktree probe: {argv}")

        with mock.patch("gsched.fingerprint.subprocess.run", side_effect=run):
            fingerprint, stages, revision = compute_fingerprint(
                ["python", "train.py"], None, "/repo", True, {}
            )

        self.assertIsNone(fingerprint)
        self.assertIsNone(stages)
        self.assertIsNone(revision)

    def test_s_h02_arbitrary_untracked_outputs_do_not_change_fingerprint(self) -> None:
        repo = self._make_repo("untracked")
        output = os.path.join(repo, "training-output.bin")
        with open(output, "wb") as stream:
            stream.write(b"output-A")
        first = self._actual_fingerprint(repo)
        with open(output, "wb") as stream:
            stream.write(b"output-B")
        second = self._actual_fingerprint(repo)

        self.assertEqual(first, second)

    def test_s_h02_unstaged_tracked_content_changes_fingerprint(self) -> None:
        repo = self._make_repo("unstaged")
        tracked = os.path.join(repo, "train.py")
        with open(tracked, "w", encoding="utf-8") as stream:
            stream.write("print('unstaged-A')\n")
        first = self._actual_fingerprint(repo)
        with open(tracked, "w", encoding="utf-8") as stream:
            stream.write("print('unstaged-B')\n")
        second = self._actual_fingerprint(repo)

        self.assertNotEqual(first, second)

    def test_s_h02_staged_tracked_content_changes_fingerprint(self) -> None:
        repo = self._make_repo("staged")
        tracked = os.path.join(repo, "train.py")
        with open(tracked, "w", encoding="utf-8") as stream:
            stream.write("print('staged-A')\n")
        self._git(repo, "add", "train.py")
        first = self._actual_fingerprint(repo)
        with open(tracked, "w", encoding="utf-8") as stream:
            stream.write("print('staged-B')\n")
        self._git(repo, "add", "train.py")
        second = self._actual_fingerprint(repo)

        self.assertNotEqual(first, second)

    def test_s_h02_broken_git_repository_fails_closed(self) -> None:
        repo = self._make_repo("broken")
        with open(os.path.join(repo, ".git", "HEAD"), "w", encoding="utf-8") as stream:
            stream.write("ref: refs/heads/missing\n")

        fingerprint, stage_fingerprints, revision = compute_fingerprint(
            ["python", "train.py"], None, repo, None, {}
        )

        self.assertIsNone(fingerprint)
        self.assertIsNone(stage_fingerprints)
        self.assertIsNone(revision)

    def test_s_h02_git_extensions_cannot_execute_during_fingerprinting(self) -> None:
        for extension in ("fsmonitor-hook", "external-diff", "textconv"):
            with self.subTest(extension=extension):
                repo = self._make_repo(extension)
                marker = os.path.join(repo, f"{extension}.executed")
                script = os.path.join(repo, f"{extension}.sh")
                with open(script, "w", encoding="utf-8") as stream:
                    stream.write(
                        "#!/bin/sh\n"
                        f"printf executed > {marker!r}\n"
                        "exit 0\n"
                    )
                os.chmod(script, 0o700)

                if extension == "fsmonitor-hook":
                    self._git(repo, "config", "core.fsmonitor", script)
                elif extension == "external-diff":
                    self._git(repo, "config", "diff.external", script)
                else:
                    attributes = os.path.join(repo, ".gitattributes")
                    with open(attributes, "w", encoding="utf-8") as stream:
                        stream.write("*.py diff=attack\n")
                    self._git(repo, "add", ".gitattributes")
                    self._git(repo, "commit", "-qm", "add attributes")
                    self._git(repo, "config", "diff.attack.textconv", script)

                with open(
                    os.path.join(repo, "train.py"), "w", encoding="utf-8"
                ) as stream:
                    stream.write("print('dirty')\n")
                fingerprint, _stages, _rev = compute_fingerprint(
                    ["python", "train.py"], None, repo, True, {}
                )

                self.assertIsNotNone(fingerprint)
                self.assertFalse(
                    os.path.exists(marker),
                    f"configured {extension} executed during fingerprinting",
                )



class ReviewExecutorLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_darwin_without_proc_rejects_second_resolution_ps_start_token(
        self,
    ) -> None:
        completed = subprocess.CompletedProcess(
            ["ps"],
            0,
            stdout="Sun Aug 30 12:34:56 2026\n",
            stderr="",
        )
        with mock.patch(
            "builtins.open",
            side_effect=FileNotFoundError("/proc is unavailable"),
        ), mock.patch(
            "gsched.executor.sys.platform",
            "darwin",
        ), mock.patch(
            "gsched.executor.subprocess.run",
            return_value=completed,
        ):
            self.assertIsNone(process_start_token(2**31 - 1))

    @unittest.skipUnless(sys.platform == "darwin", "requires Darwin")
    def test_darwin_process_start_token_is_stable_microsecond_identity(
        self,
    ) -> None:
        first = process_start_token(os.getpid())
        second = process_start_token(os.getpid())

        self.assertRegex(first or "", r"\Adarwin:\d+:\d+\Z")
        self.assertEqual(first, second)



    def test_marker_setup_failure_does_not_kill_reused_group_after_leader_exit(
        self,
    ) -> None:
        proc = mock.Mock(pid=4242)
        proc.poll.return_value = 7
        log_stream = mock.Mock()
        log_fd = os.open(os.devnull, os.O_WRONLY)
        self.addCleanup(os.close, log_fd)
        log_stream.fileno.return_value = log_fd
        marker = os.path.join(self.tmp.name, "launch", "job.launch")
        with mock.patch.object(
            state,
            "open_private_text",
            side_effect=[log_stream, OSError("marker setup failed")],
        ), mock.patch(
            "gsched.executor.subprocess.Popen",
            return_value=proc,
        ), mock.patch(
            "gsched.executor.subprocess.run",
            return_value=mock.Mock(returncode=1, stdout=""),
        ), mock.patch(
            "gsched.executor.os.killpg",
        ) as killpg:
            with self.assertRaisesRegex(OSError, "marker setup failed"):
                Executor().launch(
                    cmd=["/bin/true"],
                    stages=None,
                    cwd=self.tmp.name,
                    env={"SCHED_LAUNCH_MARKER": marker},
                    gpu=None,
                    log_path=os.path.join(self.tmp.name, "job.log"),
                )

        self.assertEqual([], [call for call in killpg.call_args_list if call.args[1] != 0])
        proc.wait.assert_called_once_with(timeout=1)

    def test_plain_command_without_sidecars_remains_direct_exec(self) -> None:
        proc = mock.Mock(pid=4343)
        command = ["/bin/echo", "plain"]
        with mock.patch(
            "gsched.executor.subprocess.Popen",
            return_value=proc,
        ) as popen:
            Executor().launch(
                cmd=command,
                stages=None,
                cwd=self.tmp.name,
                env={},
                gpu=None,
                log_path=os.path.join(self.tmp.name, "plain.log"),
            )

        self.assertEqual(command, popen.call_args.args[0])

    def test_local_supervisor_completion_requires_a_normal_shell_exit(
        self,
    ) -> None:
        executor = Executor()
        proc = mock.Mock()
        executor._procs[4242] = proc

        proc.poll.return_value = None
        self.assertFalse(executor.local_supervisor_completed(4242))
        proc.poll.return_value = -signal.SIGKILL
        self.assertFalse(executor.local_supervisor_completed(4242))
        proc.poll.return_value = 137
        self.assertTrue(executor.local_supervisor_completed(4242))
        self.assertIsNone(executor.local_supervisor_completed(5252))

    def test_successful_sigkill_marks_the_exact_local_group_dead(self) -> None:
        executor = Executor()
        proc = mock.Mock()
        proc.poll.return_value = -signal.SIGKILL
        executor._procs[6262] = proc

        with mock.patch("gsched.executor.os.killpg") as killpg:
            self.assertTrue(executor.kill_pgid(6262, signal.SIGKILL))
            self.assertFalse(executor.alive(6262))

        killpg.assert_called_once_with(6262, signal.SIGKILL)
        proc.wait.assert_called_once_with(timeout=1)

    def test_supervisor_preserves_normal_child_rc_sidecar(self) -> None:
        rc_dir = os.path.join(self.tmp.name, "normal-rc")
        os.makedirs(rc_dir)
        executor = Executor()
        pgid = executor.launch(
            cmd=["/bin/sh", "-c", "exit 7"],
            stages=None,
            cwd=self.tmp.name,
            env={"SCHED_RC_DIR": rc_dir, "SCHED_RC_PREFIX": "normal"},
            gpu=None,
            log_path=os.path.join(self.tmp.name, "normal.log"),
        )
        proc = executor._procs[pgid]
        try:
            self.assertEqual(7, proc.wait(timeout=3))
            with open(
                os.path.join(rc_dir, f"normal-{pgid}.rc"),
                encoding="utf-8",
            ) as stream:
                self.assertEqual("7", stream.read().strip())
        finally:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except OSError:
                pass
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)

    def test_supervisor_keeps_leader_until_same_group_background_descendant_exits_and_preserves_foreground_rc(
        self,
    ) -> None:
        rc_dir = os.path.join(self.tmp.name, "descendant-rc")
        os.makedirs(rc_dir)
        descendant_path = os.path.join(self.tmp.name, "descendant.pid")
        foreground_done_path = os.path.join(self.tmp.name, "foreground.done")
        descendant_done_path = os.path.join(self.tmp.name, "descendant.done")
        executor = Executor()
        pgid = executor.launch(
            cmd=[
                "/bin/sh",
                "-c",
                "(sleep 1.5; : > \"$DESCENDANT_DONE_PATH\") & "
                "echo $! > \"$DESCENDANT_PID_PATH\"; "
                "sleep 0.1; : > \"$FOREGROUND_DONE_PATH\"; exit 7",
            ],
            stages=None,
            cwd=self.tmp.name,
            env={
                "DESCENDANT_PID_PATH": descendant_path,
                "FOREGROUND_DONE_PATH": foreground_done_path,
                "DESCENDANT_DONE_PATH": descendant_done_path,
                "SCHED_RC_DIR": rc_dir,
                "SCHED_RC_PREFIX": "descendant",
            },
            gpu=None,
            log_path=os.path.join(self.tmp.name, "descendant.log"),
        )
        proc = executor._procs[pgid]
        descendant_pid: int | None = None
        try:
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try:
                    with open(descendant_path, encoding="utf-8") as stream:
                        descendant_pid = int(stream.read().strip())
                    if os.path.isfile(foreground_done_path):
                        break
                except (FileNotFoundError, ValueError):
                    pass
                time.sleep(0.01)
            else:
                self.fail("foreground completion and descendant pid were not published")

            self.assertIsNone(proc.poll())
            self.assertEqual(pgid, proc.pid)
            self.assertEqual(pgid, os.getpgid(pgid))
            self.assertEqual(pgid, os.getpgid(descendant_pid))
            self.assertEqual(7, proc.wait(timeout=3))
            self.assertTrue(os.path.isfile(descendant_done_path))

            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                try:
                    os.kill(descendant_pid, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.01)
            else:
                self.fail("same-group background descendant survived supervisor exit")

            with open(
                os.path.join(rc_dir, f"descendant-{pgid}.rc"),
                encoding="utf-8",
            ) as stream:
                self.assertEqual("7", stream.read().strip())
        finally:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)



    def test_term_ignoring_child_cannot_outlive_wrapper_group_leader(self) -> None:
        rc_dir = os.path.join(self.tmp.name, "rc")
        os.makedirs(rc_dir)
        log_path = os.path.join(self.tmp.name, "job.log")
        executor = Executor()
        pgid = executor.launch(
            cmd=[
                "/bin/sh",
                "-c",
                "trap '' TERM; printf 'ready\\n'; sleep 30",
            ],
            stages=None,
            cwd=self.tmp.name,
            env={"SCHED_RC_DIR": rc_dir, "SCHED_RC_PREFIX": "job"},
            gpu=None,
            log_path=log_path,
        )
        proc = executor._procs[pgid]
        try:
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try:
                    with open(log_path, encoding="utf-8") as stream:
                        if "ready" in stream.read():
                            break
                except OSError:
                    pass
                time.sleep(0.01)
            else:
                self.fail("TERM-ignoring child did not become ready")

            os.killpg(pgid, signal.SIGTERM)
            time.sleep(0.2)

            self.assertIsNone(proc.poll())
            os.killpg(pgid, 0)
        finally:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait(timeout=3)



class ReviewStageCheckpointTests(unittest.TestCase):
    """Explicit, fingerprinted stage sidecars for S-H01."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _launch(
        self,
        *,
        current_fingerprint: str = "current",
        sidecar_fingerprint: str | None = None,
        invalid_sidecar: bool = False,
        artifact_text: str = "valid artifact",
        rule: dict | None = None,
        force_rerun: bool = False,
    ) -> str:
        import json

        artifact = os.path.join(self.tmp.name, "stage.out")
        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write(artifact_text)
        checkpoint_dir = os.path.join(self.tmp.name, "stage-checkpoints", "job-v2")
        if sidecar_fingerprint is not None or invalid_sidecar:
            os.makedirs(checkpoint_dir, mode=0o700, exist_ok=True)
            sidecar = os.path.join(checkpoint_dir, "stage-0.json")
            with open(sidecar, "w", encoding="utf-8") as stream:
                if invalid_sidecar:
                    stream.write("{not-json")
                else:
                    json.dump(
                        {
                            "schema_version": 1,
                            "fingerprint": sidecar_fingerprint,
                        },
                        stream,
                    )
        fake_proc = mock.Mock(pid=321)
        fake_proc.poll.return_value = None
        with mock.patch("gsched.executor.subprocess.Popen", return_value=fake_proc) as popen:
            Executor().launch(
                cmd=None,
                stages=[{
                    "cmd": ["python", "run-stage.py"],
                    "artifacts": {"out": {"path": artifact, **(rule or {})}},
                }],
                cwd=self.tmp.name,
                env={},
                gpu=None,
                log_path=os.path.join(self.tmp.name, "stage.log"),
                stage_fingerprints={"0": current_fingerprint},
                stage_checkpoint_dir=checkpoint_dir,
                force_rerun=force_rerun,
            )
        return popen.call_args.args[0][-1]

    def test_s_h01_existing_artifact_without_sidecar_reruns(self) -> None:
        wrapper = self._launch()
        self.assertIn("run-stage.py", wrapper)
        self.assertNotIn("产物已存在, 跳过", wrapper)

    def test_s_h01_matching_sidecar_and_valid_rules_skip(self) -> None:
        wrapper = self._launch(sidecar_fingerprint="current")
        self.assertNotIn("run-stage.py", wrapper)
        self.assertIn("跳过", wrapper)

    def test_s_h01_code_or_command_fingerprint_change_reruns(self) -> None:
        for old, current in (("code-A", "code-B"), ("cmd-A", "cmd-B")):
            with self.subTest(old=old, current=current):
                wrapper = self._launch(
                    sidecar_fingerprint=old,
                    current_fingerprint=current,
                )
                self.assertIn("run-stage.py", wrapper)

    def test_s_h01_matching_sidecar_with_failed_artifact_rule_reruns(self) -> None:
        wrapper = self._launch(
            sidecar_fingerprint="current",
            artifact_text="x",
            rule={"min_bytes": 100},
        )
        self.assertIn("run-stage.py", wrapper)

    def test_s_h01_force_rerun_ignores_matching_sidecar(self) -> None:
        wrapper = self._launch(
            sidecar_fingerprint="current",
            force_rerun=True,
        )
        self.assertIn("run-stage.py", wrapper)

    def test_s_h01_malformed_sidecar_fails_closed_and_reruns(self) -> None:
        wrapper = self._launch(invalid_sidecar=True)
        self.assertIn("run-stage.py", wrapper)


    def test_s_h01_rerunning_upstream_invalidates_every_downstream_checkpoint(
        self,
    ) -> None:
        import json

        checkpoint_dir = os.path.join(self.tmp.name, "stage-checkpoints", "chain")
        os.makedirs(checkpoint_dir, mode=0o700)
        stages = []
        for index in range(2):
            artifact = os.path.join(self.tmp.name, f"stage-{index}.out")
            with open(artifact, "w", encoding="utf-8") as stream:
                stream.write("x" if index == 0 else "valid downstream")
            with open(
                os.path.join(checkpoint_dir, f"stage-{index}.json"),
                "w",
                encoding="utf-8",
            ) as stream:
                json.dump(
                    {"schema_version": 1, "fingerprint": f"fingerprint-{index}"},
                    stream,
                )
            stages.append(
                {
                    "cmd": ["/bin/echo", f"run-stage-{index}"],
                    "artifacts": {
                        "out": {
                            "path": artifact,
                            "min_bytes": 2 if index == 0 else 1,
                        }
                    },
                }
            )

        fake_proc = mock.Mock(pid=322)
        fake_proc.poll.return_value = None
        with mock.patch("gsched.executor.subprocess.Popen", return_value=fake_proc) as popen:
            Executor().launch(
                cmd=None,
                stages=stages,
                cwd=self.tmp.name,
                env={},
                gpu=None,
                log_path=os.path.join(self.tmp.name, "stage-chain.log"),
                stage_fingerprints={
                    "0": "fingerprint-0",
                    "1": "fingerprint-1",
                },
                stage_checkpoint_dir=checkpoint_dir,
            )

        wrapper = popen.call_args.args[0][-1]
        self.assertIn("run-stage-0", wrapper)
        self.assertIn("run-stage-1", wrapper)
        self.assertLess(wrapper.index("stage-1.json"), wrapper.index("run-stage-0"))

    def test_s_h01_success_atomically_records_the_stage_producer(self) -> None:
        import json
        import stat

        artifact = os.path.join(self.tmp.name, "created.out")
        checkpoint_dir = os.path.join(self.tmp.name, "stage-checkpoints", "job-v2")
        executor = Executor()
        pid = executor.launch(
            cmd=None,
            stages=[{
                "cmd": ["/bin/sh", "-c", f"printf ok > {artifact}"],
                "artifacts": {"out": {"path": artifact, "min_bytes": 2}},
            }],
            cwd=self.tmp.name,
            env={},
            gpu=None,
            log_path=os.path.join(self.tmp.name, "stage-success.log"),
            stage_fingerprints={"0": "producer-fingerprint"},
            stage_checkpoint_dir=checkpoint_dir,
        )
        self.assertEqual(0, executor._procs[pid].wait(timeout=5))
        sidecar = os.path.join(checkpoint_dir, "stage-0.json")
        with open(sidecar, encoding="utf-8") as stream:
            self.assertEqual(
                {"schema_version": 1, "fingerprint": "producer-fingerprint"},
                json.load(stream),
            )
        self.assertEqual(0o700, stat.S_IMODE(os.stat(checkpoint_dir).st_mode))
        self.assertEqual(0o600, stat.S_IMODE(os.stat(sidecar).st_mode))

    def test_s_h01_failed_stage_never_records_a_checkpoint(self) -> None:
        checkpoint_dir = os.path.join(self.tmp.name, "stage-checkpoints", "job-v2")
        executor = Executor()
        pid = executor.launch(
            cmd=None,
            stages=[{
                "cmd": ["/bin/sh", "-c", "exit 7"],
                "artifacts": {
                    "out": {"path": os.path.join(self.tmp.name, "never-created.out")}
                },
            }],
            cwd=self.tmp.name,
            env={},
            gpu=None,
            log_path=os.path.join(self.tmp.name, "stage-failure.log"),
            stage_fingerprints={"0": "must-not-be-recorded"},
            stage_checkpoint_dir=checkpoint_dir,
        )
        self.assertEqual(7, executor._procs[pid].wait(timeout=5))
        self.assertFalse(os.path.exists(os.path.join(checkpoint_dir, "stage-0.json")))

    def test_s_h01_zero_exit_with_missing_or_invalid_artifact_has_no_checkpoint(
        self,
    ) -> None:
        for case in ("missing", "invalid"):
            with self.subTest(case=case):
                artifact = os.path.join(self.tmp.name, f"{case}.out")
                checkpoint_dir = os.path.join(
                    self.tmp.name, "stage-checkpoints", case
                )
                if case == "missing":
                    command = ["/bin/true"]
                else:
                    command = ["/bin/sh", "-c", 'printf x > "$TEST_ARTIFACT"']
                executor = Executor()
                pid = executor.launch(
                    cmd=None,
                    stages=[{
                        "cmd": command,
                        "artifacts": {
                            "out": {"path": artifact, "min_bytes": 2}
                        },
                    }],
                    cwd=self.tmp.name,
                    env={"TEST_ARTIFACT": artifact},
                    gpu=None,
                    log_path=os.path.join(self.tmp.name, f"{case}.log"),
                    stage_fingerprints={"0": f"{case}-producer"},
                    stage_checkpoint_dir=checkpoint_dir,
                )

                rc = executor._procs[pid].wait(timeout=5)
                self.assertFalse(
                    os.path.exists(os.path.join(checkpoint_dir, "stage-0.json"))
                )
                self.assertNotEqual(0, rc)

    def test_s_h01_final_settlement_validates_declared_stage_artifacts(self) -> None:
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher._drop_launch_marker = mock.Mock()
        dispatcher._load_task_spec = mock.Mock(return_value={
            "cwd_abs": self.tmp.name,
            "artifacts": {},
            "stages": [{
                "cmd": ["/bin/true"],
                "artifacts": {
                    "stage-output": {
                        "path": os.path.join(self.tmp.name, "missing-stage.out")
                    }
                },
            }],
        })
        dispatcher.log_line = mock.Mock()
        dispatcher._consume_profile = mock.Mock()
        dispatcher._drop_profile = mock.Mock()
        dispatcher._release_gpu_for_job = mock.Mock()
        dispatcher._maybe_retry = mock.Mock()
        dispatcher._consume_pending_cancel_before_requeue = mock.Mock(
            return_value=False
        )
        job = {
            "id": "job-v1",
            "kill_reason": None,
            "rc": 0,
            "pgid": None,
            "gpu": None,
        }

        with mock.patch(
            "gsched.dispatcher.state.get_job", return_value=job
        ), mock.patch("gsched.dispatcher.state.update_job") as update_job:
            dispatcher._handle_job_done(object(), job, 0)

        update_job.assert_called_once()
        self.assertEqual("failed", update_job.call_args.kwargs["status"])
        self.assertEqual("artifact", update_job.call_args.kwargs["failure"])

    def test_s_h01_dispatcher_uses_job_scoped_checkpoint_directory(self) -> None:
        import json

        config_path = os.path.join(self.tmp.name, "config.json")
        config_data = {
            "schema_version": 1,
            "user": "test",
            "node": "review-node",
            "gpus": [],
            "default_project": "p",
            "projects": {"p": {"root": self.tmp.name, "git": False}},
            "venvs": {},
        }
        with open(config_path, "w", encoding="utf-8") as stream:
            json.dump(config_data, stream)
        with mock.patch.dict(
            os.environ,
            {"SCHED_STATE": self.tmp.name, "SCHED_CONFIG": config_path},
            clear=False,
        ):
            state._hostname_cache.clear()
            state._hostname_last_good.clear()
            state._pinned_host.clear()
            state.init_db()
            spec = {
                "id": "task",
                "cmd": None,
                "stages": [{"cmd": ["/bin/true"], "artifacts": {}}],
                "cwd_abs": self.tmp.name,
                "git": False,
                "env": {},
                "resources": {"gpu": 0, "cpus": 1},
                "artifacts": {},
                "_force_rerun": True,
            }
            with state.connect() as conn:
                state.insert_batch(
                    conn, "batch", "batch", "mix", [], None, self.tmp.name, {},
                    project="p",
                )
                conn.execute("UPDATE batches SET status='active' WHERE id='batch'")
                state.insert_task(conn, "batch", "task", 1, spec, 0, "p")
                state.insert_job(
                    conn, "job-v1", "batch", "task", 1, "task-fp",
                    {"0": "stage-fp"}, "p",
                )
            dispatcher = Dispatcher.__new__(Dispatcher)
            dispatcher.cfg = config_data
            dispatcher.host_dir = os.path.join(self.tmp.name, "review-node")
            dispatcher.venv_paths = {}
            dispatcher._launch_inflight = {}
            dispatcher.executor = mock.Mock()
            dispatcher.executor.launch.return_value = 123
            dispatcher._drop_job_rc = mock.Mock()
            dispatcher._should_skip = mock.Mock(return_value=False)
            dispatcher._clean_stale_artifacts = mock.Mock()
            dispatcher._job_log_path = mock.Mock(
                return_value=os.path.join(self.tmp.name, "job.log")
            )
            dispatcher._job_rc_prefix = mock.Mock(return_value="job-v1")
            dispatcher._launch_marker_path = mock.Mock(
                return_value=os.path.join(self.tmp.name, "launch", "job-v1")
            )
            dispatcher._drop_launch_marker = mock.Mock()
            dispatcher.log_line = mock.Mock()
            with state.connect() as conn, mock.patch(
                "gsched.dispatcher.compute_fingerprint",
                return_value=("task-fp", {"0": "stage-fp"}, None),
            ):
                job = state.get_job(conn, "job-v1")
                launched = dispatcher._launch_job(conn, job, None)
            self.assertTrue(launched)
            kwargs = dispatcher.executor.launch.call_args.kwargs
            self.assertEqual({"0": "stage-fp"}, kwargs["stage_fingerprints"])
            self.assertEqual(
                os.path.join(
                    dispatcher.host_dir, "stage_checkpoints", "job-v1"
                ),
                kwargs["stage_checkpoint_dir"],
            )
            self.assertTrue(kwargs["force_rerun"])


if __name__ == "__main__":
    unittest.main()
