"""Generic feedback fixes: pure checks and isolated state; no training/daemon."""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import json
import os
import subprocess
import shlex
import tempfile
import unittest
from unittest import mock

from gsched import artifacts, cli, config, daemon, integration, state
from gsched.dispatcher import Dispatcher
from gsched.executor import _artifact_validation_command
from gsched.schema import SchemaError, validate_batch
from test_review_cli_state import TempStateCase


class ArtifactDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "result.json")
        self.content = b'{"ok":true,"value":1,"nested":{"items":[true,null]}}'
        with open(self.path, "wb") as stream:
            stream.write(self.content)

    def test_typed_json_equals_and_bounded_evidence(self):
        rule = {"path": "result.json", "json_equals": {"ok": True, "value": 1.0, "nested.items": [True, None]}}
        detail = artifacts.inspect_artifact(self.path, rule)
        self.assertTrue(detail["passed"])
        self.assertEqual(hashlib.sha256(self.content).hexdigest(), detail["sha256"])
        self.assertEqual(len(self.content), detail["file_size"])
        self.assertIsNone(artifacts.check_artifact(self.path, rule))
        for equality in ({"ok": 1}, {"value": True}, {"nested.items": [1, None]}):
            with self.subTest(equality=equality):
                bad = artifacts.inspect_artifact(self.path, {"json_equals": equality})
                self.assertEqual("json_value_mismatch", bad["reason_code"])
        missing = artifacts.inspect_artifact(self.path, {"json_equals": {"missing": None}})
        self.assertEqual("missing_json_key", missing["reason_code"])

    def test_invalid_json_equals_rejected_at_submit_and_runtime(self):
        cfg = {"user": "test", "node": "node", "projects": {"p": {"root": self.tmp.name, "git": False}}, "venvs": {}}
        for equality in ({}, [], {"v": float("nan")}, {"v": float("inf")}, {"a\0b": 1}):
            with self.subTest(equality=equality):
                rule = {"path": "result.json", "json_equals": equality}
                spec = {"name": "batch", "project": "p", "tasks": [{"id": "task", "cmd": ["/bin/true"], "artifacts": {"r": rule}}]}
                with self.assertRaises(SchemaError):
                    validate_batch(spec, cfg)
                self.assertEqual("invalid_rule", artifacts.inspect_artifact(self.path, rule)["reason_code"])

    def test_regex_errors_are_not_reported_as_no_match(self):
        cases = [
            (subprocess.TimeoutExpired("child", 1), "regex_timeout"),
            (OSError(11, "resource unavailable"), "regex_child_start_error"),
            (subprocess.CompletedProcess([], 1, b"", b"error"), "regex_child_error"),
            (subprocess.CompletedProcess([], 0, b"not-json", b""), "regex_child_output_invalid"),
            (subprocess.CompletedProcess([], 0, b"null", b""), "regex_no_match"),
        ]
        for response, reason in cases:
            with self.subTest(reason=reason):
                kwargs = {"side_effect": response} if isinstance(response, Exception) else {"return_value": response}
                with mock.patch("gsched.artifacts.subprocess.run", **kwargs):
                    detail = artifacts.inspect_artifact(self.path, {"regex": "ok"})
                self.assertEqual(reason, detail["reason_code"])
                self.assertFalse(detail["passed"])
                self.assertIsNotNone(detail["message"])

    def test_zero_length_match_passes_and_stderr_is_bounded(self):
        response = subprocess.CompletedProcess([], 0, b'""', b"")
        with mock.patch("gsched.artifacts.subprocess.run", return_value=response):
            self.assertEqual("", artifacts.bounded_regex_last_match("ok", self.content, max_match_chars=0))
            detail = artifacts.inspect_artifact(self.path, {"regex": "ok"})
        self.assertTrue(detail["passed"])
        self.assertNotIn("match", detail["regex"])
        response = subprocess.CompletedProcess([], 1, b"", b"x" * 10_000)
        with mock.patch("gsched.artifacts.subprocess.run", return_value=response):
            detail = artifacts.inspect_artifact(self.path, {"regex": "ok"})
        self.assertEqual(2048, len(detail["regex"]["stderr"]))

    def test_filesystem_errors_and_content_errors_are_distinct(self):
        self.assertEqual("missing_file", artifacts.inspect_artifact(self.path + ".missing", {})["reason_code"])
        self.assertEqual("too_small", artifacts.inspect_artifact(self.path, {"min_bytes": 1000})["reason_code"])
        with open(self.path, "w") as stream:
            stream.write("not json")
        self.assertEqual("invalid_json", artifacts.inspect_artifact(self.path, {"check": "json"})["reason_code"])
        link = self.path + ".link"
        os.symlink(self.path, link)
        self.assertFalse(artifacts.inspect_artifact(link, {})["passed"])

    def test_json_decoder_value_error_is_a_failed_rule_not_a_tick_error(self):
        with mock.patch("gsched.artifacts.json.loads", side_effect=ValueError("integer too long")):
            self.assertEqual("invalid_json", artifacts.inspect_artifact(self.path, {"check": "json"})["reason_code"])

    def test_changed_file_is_rejected_and_large_existence_checks_do_not_hash(self):
        original_read = os.read
        changed = False

        def read_and_change(fd, count):
            nonlocal changed
            data = original_read(fd, count)
            if not changed:
                changed = True
                with open(self.path, "ab") as stream:
                    stream.write(b" ")
            return data

        with mock.patch("gsched.artifacts.os.read", side_effect=read_and_change):
            self.assertEqual("file_changed", artifacts.inspect_artifact(self.path, {"check": "json"})["reason_code"])
        with mock.patch("gsched.artifacts.os.read", side_effect=AssertionError("existence check read content")):
            detail = artifacts.inspect_artifact(self.path, {"min_bytes": 1})
        self.assertTrue(detail["passed"])
        self.assertIsNone(detail["sha256"])

    def test_stage_failures_do_not_hide_task_checks(self):
        rule = {"r": {"path": "result.json", "json_equals": {"ok": True}}}
        spec = {"artifacts": rule, "stages": [{"artifacts": {"s": {"path": "absent"}}}]}
        checks = artifacts.inspect_declared_artifacts(spec, self.tmp.name)
        self.assertEqual(["task", "stage:0"], [row["scope"] for row in checks])
        self.assertEqual([True, False], [row["passed"] for row in checks])
        self.assertFalse(artifacts.check_declared_artifacts(spec, self.tmp.name))
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.log_line = mock.Mock()
        job = {"id": "job", "batch_id": "batch", "task_id": "task", "version": 1}
        spec["cwd_abs"] = self.tmp.name
        self.assertFalse(dispatcher._completion_artifacts_valid(job, spec, "exit_zero"))
        self.assertEqual(2, dispatcher.log_line.call_count)
        logged = json.loads(dispatcher.log_line.call_args.args[0].split(" ", 1)[1])
        self.assertEqual("missing_file", logged["reason_code"])
        self.assertEqual(1, logged["version"])

    def test_generated_stage_validator_keeps_diagnostics_in_task_log(self):
        command = _artifact_validation_command({"output": {"path": "missing"}}, self.tmp.name,
                                               paths_escape=False, stage_index=2)
        argv = shlex.split(command)
        program = argv[argv.index("-c") + 1]
        output = io.StringIO()
        with mock.patch("sys.argv", ["-c", *argv[argv.index("-c") + 2:]]), \
             contextlib.redirect_stdout(output), self.assertRaises(SystemExit) as exited:
            exec(compile(program, "<stage-validator>", "exec"), {})
        self.assertEqual(1, exited.exception.code)
        record = json.loads(output.getvalue().removeprefix("[sched] artifact_check "))
        self.assertEqual(2, record["stage_index"])
        self.assertEqual("missing_file", record["checks"]["output"]["reason_code"])


class FeedbackRequestTests(TempStateCase):
    def request(self, verb="request", *, rid="repair", status="failed", revision=None):
        return [verb, rid, "--json", "--expect-kind", "task", "--expect-id", "batch-20260829-000000:task",
                "--expect-status", status, "--expect-version", "1", "--expect-revision",
                str(self.batch_revision() if revision is None else revision), "--", "retry", "batch-20260829-000000:task"]

    def test_missing_precondition_fails_before_state_access(self):
        args = ["request", "missing", "--json", "--expect-kind", "task", "--expect-id", "b:t",
                "--expect-version", "1", "--expect-revision", "0", "--", "cancel", "b:t", "--yes"]
        with mock.patch("gsched.cli.state.init_db", side_effect=AssertionError("initialized")), \
             mock.patch("gsched.cli.state.connect", side_effect=AssertionError("opened DB")):
            code, output, _ = self.capture(cli.main, args)
        self.assertEqual(64, code)
        error = json.loads(output)["error"]
        self.assertEqual(["expect_status"], error["missing_fields"])
        self.assertEqual("none", error["effect_of_this_invocation"])
        self.assertFalse(error["request_record_created_this_invocation"])

    def test_validate_checks_exact_command_without_state_or_rid_reservation(self):
        args = self.request("request-validate", revision=0)
        with mock.patch("gsched.cli.state.connect", side_effect=AssertionError("opened DB")), \
             mock.patch("gsched.cli.load_config", side_effect=AssertionError("loaded config")):
            code, output, _ = self.capture(cli.main, args)
        self.assertEqual(0, code)
        value = json.loads(output)
        self.assertTrue(value["valid"])
        self.assertFalse(value["state_checked"])
        with state.connect() as conn:
            self.assertEqual(0, conn.execute("SELECT count(*) FROM operation_requests").fetchone()[0])
        code, output, _ = self.capture(cli.main, [*args, "--invalid"])
        self.assertEqual(64, code)
        self.assertEqual("invalid_command", json.loads(output)["error"]["reason_code"])

    def test_json_result_replay_preserves_original_binding_and_one_effect(self):
        self.seed_batch()
        args = self.request()
        with mock.patch("gsched.cli._ensure_running_locked", return_value="not started"):
            first = self.capture(cli.main, args)
            second = self.capture(cli.main, args)
        self.assertEqual(0, first[0], first[2])
        self.assertEqual(0, second[0], second[2])
        a, b = json.loads(first[1]), json.loads(second[1])
        self.assertEqual(a["result"], b["result"])
        self.assertFalse(a["replayed"])
        self.assertTrue(b["replayed"])
        self.assertEqual("none", b["effect_of_this_invocation"])
        with state.connect() as conn:
            self.assertEqual(1, conn.execute("SELECT count(*) FROM operation_requests").fetchone()[0])
        # A malformed new invocation cannot negate an older successful RID.
        changed = list(args)
        changed[changed.index("--expect-status") + 1] = "running"
        code, output, _ = self.capture(cli.main, changed)
        self.assertEqual(64, code)
        self.assertEqual("request_binding_mismatch", json.loads(output)["error"]["reason_code"])
        receipt = integration.readonly_status("repair")
        self.assertEqual(0, receipt["code"])

    def test_cas_conflict_is_durable_and_machine_readable(self):
        self.seed_batch()
        with mock.patch("gsched.cli._run_captured_mutation", side_effect=AssertionError("dispatched")):
            code, output, _ = self.capture(cli.main, self.request(revision=0))
        self.assertEqual(65, code)
        value = json.loads(output)
        self.assertFalse(value["dispatch_entered"])
        self.assertTrue(value["request_record_created_this_invocation"])
        self.assertEqual("precondition_conflict", value["result"]["error"]["reason_code"])
        self.assertEqual("revision_changed", value["result"]["error"]["conflict_reason"])
        self.assertEqual(self.batch_revision(), value["result"]["error"]["actual"]["revision"])
        receipt = integration.readonly_status("repair")
        self.assertEqual(value["result"], receipt["result"])
        self.assertEqual("database", receipt["receipt_source"])
        self.assertIsNotNone(receipt["finished_at"])

    def test_many_queries_use_one_snapshot_and_do_not_migrate(self):
        with mock.patch.object(state, "connect", wraps=state.connect) as connect, \
             mock.patch.object(state, "init_db", side_effect=AssertionError("migration")):
            code, output, _ = self.capture(cli.main, ["request-status-many", "a", "b", "--json"])
        self.assertEqual(0, code)
        self.assertEqual(1, connect.call_count)
        value = json.loads(output)
        self.assertEqual(["a", "b"], [row["request_id"] for row in value["requests"]])
        self.assertFalse(value["ticket_fallback_atomic"])
        self.assertTrue(all(row["phase"] == "not_found" for row in value["requests"]))

    def test_wait_is_read_only_bounded_and_keeps_original_rid(self):
        delivered = integration._empty_request_status("original")
        delivered.update(instance_id="a" * 32, found=True, phase="delivered", code=0,
                         request_kind="submission", binding_sha256="b" * 64)
        done = {**delivered, "phase": "done"}
        with mock.patch("gsched.integration.request_status", side_effect=[delivered, done]) as query, \
             mock.patch("gsched.cli.time.sleep") as sleep, \
             mock.patch("gsched.cli.state.init_db", side_effect=AssertionError("migration")):
            code, output, _ = self.capture(cli.main, ["request-status", "original", "--wait-sec", "1", "--json"])
        self.assertEqual(0, code)
        self.assertEqual("done", json.loads(output)["phase"])
        self.assertFalse(json.loads(output)["wait_timed_out"])
        self.assertEqual([mock.call("original"), mock.call("original")], query.call_args_list)
        sleep.assert_called_once()
        for value in ("-1", "nan", "61"):
            with self.subTest(value=value):
                code, _, _ = self.capture(cli.main, ["request-status", "original", "--wait-sec", value, "--json"])
                self.assertEqual(1, code)

    def test_timeout_does_not_create_or_resubmit(self):
        with mock.patch("gsched.cli.time.monotonic", side_effect=[0, 2]), \
             mock.patch("gsched.cli.cmd_submit", side_effect=AssertionError("submitted")):
            code, output, _ = self.capture(cli.main, ["request-status", "absent", "--wait-sec", "1", "--json"])
        self.assertEqual(0, code)
        self.assertTrue(json.loads(output)["wait_timed_out"])
        self.assertEqual("not_found", json.loads(output)["phase"])

    def test_wrong_instance_and_batch_bounds_reject_queries(self):
        for args in (["request-status", "a", "--expect-instance", "a" * 32],
                     ["request-status-many", "a", "a"],
                     ["request-status-many", *[f"id{i}" for i in range(101)]]):
            code, _, _ = self.capture(cli.main, [*args, "--json"])
            self.assertEqual(1, code)

    def test_wait_preserves_delivered_evidence_when_receipt_temporarily_missing(self):
        delivered = integration._empty_request_status("original")
        delivered.update(instance_id="a" * 32, found=True, phase="delivered", code=0,
                         request_kind="submission", binding_sha256="b" * 64)
        missing = integration._empty_request_status("original")
        missing["instance_id"] = "a" * 32
        with mock.patch("gsched.integration.request_status", side_effect=[delivered, missing]), \
             mock.patch("gsched.cli.time.monotonic", side_effect=[0, 0.1, 2]), \
             mock.patch("gsched.cli.time.sleep"):
            code, output, _ = self.capture(cli.main, ["request-status", "original", "--wait-sec", "1", "--json"])
        self.assertEqual(0, code)
        record = json.loads(output)
        self.assertEqual("delivered", record["phase"])
        self.assertTrue(record["observation_incomplete"])
        self.assertTrue(record["wait_timed_out"])

    def test_unreadable_receipt_is_not_reported_not_found(self):
        with mock.patch("gsched.integration.request_status", side_effect=OSError("unavailable")):
            code, output, _ = self.capture(cli.main, ["request-status", "original", "--json"])
        self.assertEqual(1, code)
        record = json.loads(output)
        self.assertEqual("query_unavailable", record["reason_code"])
        self.assertNotIn("phase", record)

    def test_permission_failure_in_metadata_stat_is_not_absence(self):
        with mock.patch("gsched.integration.os.stat", side_effect=PermissionError(13, "permission denied")):
            code, output, _ = self.capture(cli.main, ["request-status", "original", "--json"])
        self.assertEqual(1, code)
        self.assertEqual("query_unavailable", json.loads(output)["reason_code"])
        with mock.patch("gsched.integration.os.stat", side_effect=PermissionError(13, "permission denied")):
            with self.assertRaises(PermissionError):
                integration.load_ticket("original")

    def test_ticket_evidence_is_not_a_database_acceptance_receipt(self):
        ticket = {"request_id": "delayed", "binding": {"payload_sha256": "a" * 64},
                  "phase": "delivered", "created_at": state.now(), "code": 0,
                  "result": {"persisted": False}}
        with mock.patch("gsched.integration.load_ticket", return_value=ticket):
            record = integration.readonly_status("delayed")
        self.assertEqual("ticket", record["receipt_source"])
        self.assertTrue(record["receipt_persisted"])
        self.assertTrue(record["delivery_confirmed"])
        self.assertFalse(record["batch_persisted"])
        self.assertEqual("awaiting_receipt", record["reason_code"])
        ticket["phase"] = "intent"
        ticket["code"] = None
        ticket["result"] = None
        with mock.patch("gsched.integration.load_ticket", return_value=ticket):
            record = integration.readonly_status("delayed")
        self.assertEqual("unknown", record["phase"])
        self.assertIsNone(record["delivery_confirmed"])

    def test_artifact_check_does_not_initialize_or_change_failed_job(self):
        job_id = self.seed_batch()
        with mock.patch("gsched.cli._is_foreign_host", return_value=False), \
             mock.patch("gsched.cli.state.init_db", side_effect=AssertionError("initialized")):
            code, output, _ = self.capture(cli.main, ["artifact-check", "batch-20260829-000000:task", "--version", "1", "--json"])
        self.assertEqual(0, code)
        value = json.loads(output)
        self.assertTrue(value["passed"])
        self.assertEqual("failed", value["recorded_status"])
        self.assertFalse(value["historical_failure_reconstructed"])
        with state.connect() as conn:
            self.assertEqual("failed", state.get_job(conn, job_id)["status"])
        with mock.patch("gsched.cli._is_foreign_host", return_value=True), \
             mock.patch("gsched.cli.state.connect", side_effect=AssertionError("opened DB")):
            self.assertEqual(2, self.capture(cli.main, ["artifact-check", "b:t", "--json"])[0])

    def test_config_set_rejects_new_conflict_and_can_repair_legacy_conflict(self):
        self.cfg["gpus"] = [1]
        with open(self.config_path, "w") as stream:
            json.dump(self.cfg, stream)
        patch_path = os.path.join(self.tmp.name, "patch.json")
        with open(patch_path, "w") as stream:
            json.dump({"projects": {"p": {"gpu_affinity": [0], "gpu_affinity_hard": True}}}, stream)
        code, _, error = self.capture(cli.cmd_config_set, argparse.Namespace(file=patch_path, yes=True))
        self.assertEqual(1, code)
        self.assertIn("无交集", error)
        self.assertEqual(self.cfg, config.load_config())
        self.cfg["projects"]["p"].update(gpu_affinity=[0], gpu_affinity_hard=True)
        with open(self.config_path, "w") as stream:
            json.dump(self.cfg, stream)
        # Reading and the patch path must remain available for old conflicts.
        code, output, _ = self.capture(cli.cmd_config_get, argparse.Namespace())
        self.assertEqual(0, code)
        self.assertEqual([0], json.loads(output)["projects"]["p"]["gpu_affinity"])
        self.assertEqual("fail", daemon.check(fake=True)[0]["level"])
        with open(patch_path, "w") as stream:
            json.dump({"projects": {"p": {"gpu_affinity": [1]}}}, stream)
        code, _, error = self.capture(cli.cmd_config_set, argparse.Namespace(file=patch_path, yes=True))
        self.assertEqual(0, code, error)
        self.assertEqual([1], config.load_config()["projects"]["p"]["gpu_affinity"])


class AffinityPoolTests(unittest.TestCase):
    def cfg(self, pool, **project):
        return {"user": "test", "node": "node", "gpus": pool,
                "venvs": {},
                "projects": {"p": {"root": "/tmp", "gpu_affinity": [0], "gpu_affinity_hard": True, **project}}}

    def test_explicit_disjoint_pool_rejected_but_read_and_repair_allowed(self):
        cfg = self.cfg([1])
        config._validate(cfg, "fixture")
        with self.assertRaisesRegex(config.ConfigError, "无交集"):
            config.validate_gpu_affinity_pool(cfg)
        spec = {"name": "b", "project": "p", "tasks": [{"id": "t", "cmd": ["true"], "git": False}]}
        with self.assertRaises(SchemaError):
            validate_batch(spec, cfg)
        spec["tasks"][0]["resources"] = {"gpu": 0}
        validate_batch(spec, cfg)

    def test_auto_pool_soft_affinity_disabled_project_and_intersection(self):
        for cfg in (self.cfg([]), self.cfg(None), self.cfg([0, 1]), self.cfg([{"idx": 0}]),
                    self.cfg([1], gpu_affinity_hard=False), self.cfg([1], gpu_enabled=False)):
            with self.subTest(cfg=cfg):
                config.validate_gpu_affinity_pool(cfg)


if __name__ == "__main__":
    unittest.main()
