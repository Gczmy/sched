"""Private artifact-only request fixtures, with no daemon/training processes."""
import copy
import json
import os
import sqlite3
import unittest
from unittest import mock

from gsched import artifacts, artifact_validation as initial, artifact_revalidation as reval, cli, state
from gsched.dispatcher import Dispatcher
from test_review_cli_state import TempStateCase


class ArtifactRevalidationTests(TempStateCase):
    batch = "batch-20260829-000000"

    def prepare(self, *, context="exit_zero", rc=0, verified=True, failure="artifact", reason="regex_timeout"):
        self.job = self.seed_batch(job_status="running")
        self.path = os.path.join(self.tmp.name, "result.json")
        with open(self.path, "w") as stream:
            stream.write('{"ok":true}')
        with state.connect() as conn:
            self.spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=?", (self.batch,)).fetchone()[0])
            self.spec["artifacts"] = {"result": {"path": "result.json", "check": "json", "regex": "ok"}}
            self.spec["max_retry"] = 0
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id=?", (json.dumps(self.spec), self.batch))
            state.update_job(conn, self.job, pgid=123, rc=rc, kill_reason="probe_ready" if context == "probe_ready" else None)
            regex = {"reason_code": reason, "elapsed_ms": 10, "match": None, "returncode": None,
                     "errno": None, "stderr": None}
            with mock.patch("gsched.artifacts.bounded_regex_result", return_value=regex):
                checks = artifacts.inspect_declared_artifacts(self.spec, self.tmp.name)
            wait = {"source": "local_supervisor_wait", "subject": "scheduler_supervisor_command_chain",
                    "pid": 123, "start_token": "proc:456", "returncode": rc,
                    "group_clean": True, "binding_verified": verified}
            self.original = initial.record_initial(conn, state.get_job(conn, self.job), self.spec,
                                                   context, checks, rc=rc, ordinary_wait=wait)
            state.update_job(conn, self.job, status="blocked", failure=failure, finished_at="2026-08-29 10:00:05")
            conn.execute("UPDATE batches SET status='blocked' WHERE id=?", (self.batch,))
        self.instance = json.loads(self.capture(cli.main, ["identity", "--json"])[1])["instance_id"]
        self.active = mock.patch.object(reval, "group_absent", return_value=True)
        self.active.start()
        self.addCleanup(self.active.stop)
        self.host = mock.patch("socket.gethostname", return_value="review-node")
        self.host.start()
        self.addCleanup(self.host.stop)
        self.passing = mock.patch("gsched.artifacts.bounded_regex_result", return_value={
            "reason_code": "passed", "match": "", "elapsed_ms": 1, "returncode": 0, "errno": None, "stderr": None})
        self.passing.start()
        self.addCleanup(self.passing.stop)
        return self.original

    def args(self, *, rid="revalidate", settle=False, reopen=False, revision=None, original=None, retries=0):
        return ["request", rid, "--json", "--expect-kind", "task", "--expect-id", self.batch + ":task",
                "--expect-status", "blocked", "--expect-version", "1", "--expect-revision",
                str(self.batch_revision() if revision is None else revision), "--expect-instance", self.instance,
                "--expect-project", "p", "--", "artifact-revalidate", self.batch + ":task",
                "--validation-id", original or self.original["validation_id"], "--system-retries", str(retries),
                *(["--settle"] if settle else []), *(["--reopen"] if reopen else []), "--yes"]

    def run_request(self, **kwargs):
        code, out, error = self.capture(cli.main, self.args(**kwargs))
        self.assertEqual(0, code, error)
        return json.loads(out)["result"]["effect"]

    def events(self, *args):
        code, out, error = self.capture(cli.main, ["artifact-revalidations", self.batch + ":task", *args, "--json"])
        self.assertEqual(0, code, error)
        return json.loads(out)

    def test_only_revalidation_preserves_original_failure_times_files_versions(self):
        self.prepare()
        before = self.capture(cli.main, ["task", self.batch + ":task", "--json"])[1]
        revision = self.batch_revision()
        with mock.patch("gsched.executor.Executor.launch", side_effect=AssertionError("training")), \
             mock.patch.object(artifacts, "unlink_artifact", side_effect=AssertionError("cleanup")):
            effect = self.run_request()
        self.assertTrue(effect["artifact_rules_passed"])
        self.assertFalse(effect["settled"])
        self.assertEqual("validated_without_settlement", effect["reason"])
        self.assertEqual(revision, self.batch_revision())
        self.assertEqual(before, self.capture(cli.main, ["task", self.batch + ":task", "--json"])[1])
        with state.connect() as conn:
            self.assertEqual(self.original, initial.decode(conn.execute("SELECT * FROM artifact_validations").fetchone()))
        self.assertEqual(1, len(self.events()["events"]))

    def test_settlement_preserves_original_wait_and_failure_history(self):
        self.prepare()
        effect = self.run_request(settle=True)
        self.assertTrue(effect["settled"])
        self.assertEqual("done", effect["status"])
        with state.connect() as conn:
            job = state.get_job(conn, self.job)
            self.assertEqual((0, 0, 1, "2026-08-29 10:00:05"), (job["rc"], job["retries"], job["version"], job["finished_at"]))
            self.assertIsNone(job["failure"])
            self.assertFalse(initial.decode(conn.execute("SELECT * FROM artifact_validations").fetchone())["passed"])
        detail = self.events("--event-id", effect["revalidation_id"])["events"][0]["payload"]
        self.assertEqual("artifact", detail["previous"]["failure"])
        self.assertEqual("blocked", detail["previous"]["status"])
        self.assertTrue(detail["settled"])

    def test_same_rid_replays_without_reads_or_new_events_and_changed_binding_rejected(self):
        self.prepare()
        args = self.args(settle=True)
        self.assertEqual(0, self.capture(cli.main, args)[0])
        with mock.patch.object(artifacts, "inspect_declared_artifacts", side_effect=AssertionError("reinspection")):
            code, out, _ = self.capture(cli.main, args)
        self.assertEqual(0, code)
        self.assertTrue(json.loads(out)["replayed"])
        self.assertEqual(1, len(self.events()["events"]))
        self.assertEqual(64, self.capture(cli.main, [*args, "--reopen"])[0])

    def test_unknown_request_is_not_reobserved(self):
        self.prepare()
        args = self.args()
        request, command, expectation = cli._request_envelope(cli._build_parser().parse_args(args))
        binding = json.dumps({"command": command, "expect": expectation}, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        with state.connect() as conn:
            conn.execute("INSERT INTO operation_requests(request_id,argv,status,created_at) VALUES(?,?,'started',?)", (request, binding, state.now()))
        with mock.patch.object(artifacts, "inspect_declared_artifacts", side_effect=AssertionError("unknown replay")):
            self.assertEqual(75, self.capture(cli.main, args)[0])
        self.assertEqual([], self.events()["events"])

    def test_stale_cas_does_not_create_observation_and_replay_stays_rejected(self):
        self.prepare()
        args = self.args(revision=self.batch_revision() - 1, settle=True)
        with mock.patch.object(artifacts, "inspect_declared_artifacts", side_effect=AssertionError("stale read")):
            self.assertEqual(65, self.capture(cli.main, args)[0])
            self.assertEqual(65, self.capture(cli.main, args)[0])
        self.assertEqual([], self.events()["events"])

    def test_changed_file_bytes_refuse_settlement_even_if_rules_still_pass(self):
        self.prepare()
        with open(self.path, "w") as stream:
            stream.write('{"ok":true,"other":1}')
        effect = self.run_request(settle=True)
        self.assertTrue(effect["artifact_rules_passed"])
        self.assertFalse(effect["settled"])
        self.assertEqual("file_identity_changed", effect["reason"])
        self.assertEqual("blocked", effect["status"])

    def test_digest_change_is_rejected_even_with_same_metadata(self):
        self.prepare()
        old = self.original["payload"]["checks"]
        altered = copy.deepcopy(old)
        altered[0]["sha256"] = "a" * 64
        self.assertEqual("file_digest_changed", reval.provenance_reason(old, altered))

    def test_changed_spec_fingerprint_or_training_retry_cannot_rebind_original(self):
        self.prepare()
        for key, value in (("fingerprint", "different"), ("retries", 1), ("started_at", "different")):
            with state.connect() as conn:
                old = state.get_job(conn, self.job)[key]
                state.update_job(conn, self.job, **{key: value})
            self.assertEqual(65, self.capture(cli.main, self.args(rid=key, settle=True))[0])
            with state.connect() as conn:
                state.update_job(conn, self.job, **{key: old})
        with state.connect() as conn:
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id=?", (json.dumps(dict(self.spec, cmd=["different"])), self.batch))
        self.assertEqual(65, self.capture(cli.main, self.args(rid="spec", settle=True))[0])
        self.assertEqual([], self.events()["events"])

    def test_original_wait_missing_prevents_inspection_and_settlement(self):
        self.prepare(verified=False)
        with mock.patch.object(artifacts, "inspect_declared_artifacts", side_effect=AssertionError("missing authority")):
            effect = self.run_request(settle=True)
        self.assertFalse(effect["settled"])
        self.assertEqual("original_wait_unavailable", effect["reason"])

    def test_nonzero_execution_never_recovers_as_artifact_success(self):
        self.prepare(context="exit_nonzero", rc=1)
        effect = self.run_request(settle=True)
        self.assertFalse(effect["settled"])
        self.assertEqual("original_exit_not_successful", effect["reason"])

    def test_intentional_ready_probe_can_recover_without_changing_raw_exit(self):
        self.prepare(context="probe_ready", rc=137)
        effect = self.run_request(settle=True)
        self.assertTrue(effect["settled"])
        with state.connect() as conn:
            self.assertEqual(137, state.get_job(conn, self.job)["rc"])

    def test_active_or_unknown_group_is_audited_and_not_settled(self):
        self.prepare()
        with mock.patch.object(reval, "group_absent", return_value=False):
            effect = self.run_request(settle=True)
        self.assertEqual("process_group_active_or_unknown", effect["reason"])
        self.assertFalse(effect["settled"])

    def test_group_reappearing_after_reads_refuses_publication(self):
        self.prepare()
        with mock.patch.object(reval, "group_absent", side_effect=[True, False]):
            effect = self.run_request(settle=True)
        self.assertEqual("process_group_active_or_unknown", effect["reason"])
        self.assertFalse(effect["settled"])

    def test_file_changed_after_validation_refuses_publication(self):
        self.prepare()
        inspect = artifacts.inspect_declared_artifacts
        def changed(*args, **kwargs):
            result = inspect(*args, **kwargs)
            with open(self.path, "w") as stream:
                stream.write('{"ok":true,"changed":true}')
            return result
        with mock.patch.object(artifacts, "inspect_declared_artifacts", side_effect=changed):
            effect = self.run_request(settle=True)
        self.assertTrue(effect["artifact_rules_passed"])
        self.assertFalse(effect["settled"])
        self.assertEqual("publication_file_identity_changed_or_unknown", effect["reason"])

    def test_path_escape_cannot_be_used_to_settle(self):
        self.prepare()
        with state.connect() as conn:
            job = dict(state.get_job(conn, self.job))
            spec = dict(self.spec, paths_escape=True)
            # Reader-only fixture: source and spec match but scope is unsupported.
            source = copy.deepcopy(self.original)
            source["payload"]["rules"][0]["paths_escape"] = True
            self.assertEqual("escaped_artifact_scope_unsupported", reval.authority_reason(conn, job, spec, source))

    def test_original_backend_wait_changes_and_unknown_generations_are_rejected(self):
        self.prepare()
        with state.connect() as conn:
            row = state.get_job(conn, self.job)
            source = copy.deepcopy(self.original)
            source["payload"]["wait"] = {"source": "execution_attempt", "observation": {"pid": 123}}
            with mock.patch.object(initial, "wait_snapshot", return_value={"source": "unknown"}):
                self.assertEqual("original_wait_changed", reval.authority_reason(conn, row, self.spec, source))
            state.insert_task(conn, self.batch, "task", 2, self.spec, 0, "p")
            state.insert_job(conn, "unknown-generation", self.batch, "task", 2, "fp", None, "p")
            state.update_job(conn, "unknown-generation", status="interrupted")
            self.assertEqual("task_execution_unresolved", reval.authority_reason(conn, row, self.spec, self.original))

    def test_reopen_does_not_ignore_other_failed_tasks(self):
        self.prepare()
        with state.connect() as conn:
            state.insert_task(conn, self.batch, "other", 1, dict(self.spec, id="other"), 1, "p")
            state.insert_job(conn, "other", self.batch, "other", 1, "fp", None, "p")
            state.update_job(conn, "other", status="failed", rc=1)
        effect = self.run_request(settle=True, reopen=True)
        self.assertTrue(effect["settled"])
        detail = self.events("--event-id", effect["revalidation_id"])["events"][0]["payload"]
        self.assertFalse(detail["batch_reopened"])
        with state.connect() as conn:
            self.assertEqual("blocked", state.get_batch(conn, self.batch)["status"])

    def test_gateway_cannot_inspect_even_with_foreign_write_override(self):
        self.prepare()
        with mock.patch("gsched.cli._is_foreign_host", return_value=True), \
             mock.patch.object(artifacts, "inspect_declared_artifacts", side_effect=AssertionError("gateway")):
            self.assertEqual(65, self.capture(cli.main, self.args(settle=True))[0])
        self.assertEqual([], self.events()["events"])

    def test_schema12_query_does_not_migrate_and_writer_adds_empty_table_only(self):
        self.prepare()
        before_revision = self.batch_revision()
        with state.connect() as conn:
            conn.execute("DROP TRIGGER artifact_revalidation_immutable")
            conn.execute("DROP TRIGGER artifact_revalidation_retained")
            conn.execute("DROP TABLE artifact_revalidations")
            conn.execute("PRAGMA user_version=12")
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migration")):
            self.assertFalse(self.events()["available"])
        state.init_db()
        self.assertEqual(before_revision, self.batch_revision())
        with state.connect() as conn:
            self.assertEqual(self.original, initial.decode(conn.execute("SELECT * FROM artifact_validations").fetchone()))
        self.assertEqual([], self.events()["events"])

    def test_schema13_migration_failure_is_atomic_and_does_not_rewrite_initial_evidence(self):
        self.prepare()
        before = self.batch_revision()
        with state.connect() as conn:
            conn.execute("DROP TRIGGER artifact_revalidation_immutable")
            conn.execute("DROP TRIGGER artifact_revalidation_retained")
            conn.execute("DROP TABLE artifact_revalidations")
            conn.execute("PRAGMA user_version=12")
        with mock.patch.object(reval, "SCHEMA", reval.SCHEMA + "\nSELECT missing_revalidation_function();"), \
             self.assertRaises(sqlite3.OperationalError):
            state.init_db()
        self.assertEqual(before, self.batch_revision())
        with state.connect() as conn:
            self.assertEqual(12, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertFalse(reval.available(conn))
            self.assertEqual(self.original, initial.decode(conn.execute("SELECT * FROM artifact_validations").fetchone()))

    def test_invalid_event_detail_or_cursor_is_not_an_empty_result(self):
        self.prepare()
        event = self.run_request()["revalidation_id"]
        for args in (("--limit", "0"), ("--cursor", "bad"), ("--event-id", "a" * 64),
                     ("--cursor", event, "--event-id", event), ("--version", "2")):
            self.assertEqual(1, self.capture(cli.main, ["artifact-revalidations", self.batch + ":task", *args, "--json"])[0])

    def test_launch_marker_uses_dispatcher_exact_path_and_is_never_removed(self):
        self.prepare()
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.host_dir = state.host_dir()
        path = dispatcher._launch_marker_path({"id": self.job})
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as stream:
            stream.write("123 proc:456\n")
        effect = self.run_request(settle=True)
        self.assertEqual("launch_state_unresolved", effect["reason"])
        self.assertTrue(os.path.exists(path))

    def test_cancel_intent_wins_without_consuming_or_deleting_it(self):
        self.prepare()
        with state.connect() as conn:
            state.insert_control_request(conn, self.job)
        effect = self.run_request(settle=True)
        self.assertEqual("cancel_pending", effect["reason"])
        with state.connect() as conn:
            self.assertEqual("pending", conn.execute("SELECT status FROM control_requests WHERE job_id=?", (self.job,)).fetchone()[0])

    def test_retry_only_classified_system_errors_is_finite_and_separate_from_training(self):
        self.prepare()
        failed = copy.deepcopy(self.original["payload"]["checks"])
        passed = copy.deepcopy(failed)
        passed[0].update(passed=True, reason_code="passed")
        with mock.patch.object(artifacts, "inspect_declared_artifacts", side_effect=[failed, failed, passed]) as inspect, \
             mock.patch.object(reval.time, "sleep") as sleep:
            effect = self.run_request(settle=True, retries=2)
        self.assertTrue(effect["settled"])
        self.assertEqual(3, inspect.call_count)
        self.assertEqual([mock.call(0.1), mock.call(0.2)], sleep.call_args_list)
        detail = self.events("--event-id", effect["revalidation_id"])["events"][0]["payload"]
        self.assertEqual(3, len(detail["attempts"]))
        self.assertEqual(0, detail["previous"]["retries"])

    def test_content_errors_and_unknown_child_exit_are_not_automatically_retried(self):
        self.prepare()
        for reason in ("regex_no_match", "invalid_json", "json_value_mismatch", "regex_child_error"):
            failed = copy.deepcopy(self.original["payload"]["checks"])
            failed[0].update(reason_code=reason)
            with mock.patch.object(artifacts, "inspect_declared_artifacts", return_value=failed) as inspect, \
                 mock.patch.object(reval.time, "sleep", side_effect=AssertionError("retry content")):
                effect = self.run_request(rid=reason, settle=True, retries=2)
            self.assertFalse(effect["settled"])
            self.assertEqual(1, inspect.call_count)

    def test_same_version_failure_events_and_first_failure_are_immutable(self):
        self.prepare()
        self.run_request(rid="one")
        self.run_request(rid="two")
        self.assertEqual(2, len(self.events()["events"]))
        for sql in ("UPDATE artifact_revalidations SET settled=1", "DELETE FROM artifact_revalidations"):
            with state.connect() as conn, self.assertRaises(sqlite3.IntegrityError):
                conn.execute(sql)

    def test_readonly_event_query_is_bounded_and_never_inspects_files(self):
        self.prepare()
        self.run_request(rid="one")
        self.run_request(rid="two")
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migration")), \
             mock.patch.object(artifacts, "inspect_declared_artifacts", side_effect=AssertionError("file inspection")):
            first = self.events("--limit", "1")
            self.assertTrue(first["truncated"])
            self.assertNotIn("payload", first["events"][0])
            second = self.events("--limit", "1", "--cursor", first["next_cursor"])
            self.assertFalse(second["truncated"])
            detail = self.events("--event-id", first["events"][0]["event_id"])
            self.assertIn("payload", detail["events"][0])

    def test_direct_write_or_incomplete_instance_is_rejected_before_state(self):
        args = ["artifact-revalidate", "b:t", "--validation-id", "a" * 64, "--settle", "--yes"]
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migration")), \
             mock.patch.object(state, "connect", side_effect=AssertionError("state")):
            self.assertEqual(64, self.capture(cli.main, args)[0])
            self.assertEqual(64, self.capture(cli.main, ["request", "invalid", "--json", "--expect-kind", "task",
                "--expect-id", "b:t", "--expect-status", "blocked", "--expect-version", "1",
                "--expect-revision", "1", "--", *args])[0])

    def test_record_failure_rolls_back_settlement_event_and_receipt_effect(self):
        self.prepare()
        with mock.patch.object(initial, "MAX_RECORD_BYTES", 20):
            self.assertEqual(65, self.capture(cli.main, self.args(settle=True))[0])
        self.assertEqual([], self.events()["events"])
        with state.connect() as conn:
            self.assertEqual("blocked", state.get_job(conn, self.job)["status"])

    def test_explicit_reopen_only_after_success_and_without_other_failed_tasks(self):
        self.prepare()
        with state.connect() as conn:
            state.insert_task(conn, self.batch, "pending", 1, dict(self.spec, id="pending"), 1, "p")
            state.insert_job(conn, "pending", self.batch, "pending", 1, "pending-fp", None, "p")
        effect = self.run_request(settle=True, reopen=True)
        detail = self.events("--event-id", effect["revalidation_id"])["events"][0]["payload"]
        self.assertTrue(detail["batch_reopened"])
        with state.connect() as conn:
            self.assertEqual("active", state.get_batch(conn, self.batch)["status"])
            self.assertEqual("pending", state.get_job(conn, "pending")["status"])

    def test_inspection_budget_prevents_regex_or_file_reads_after_deadline(self):
        self.prepare()
        with mock.patch("gsched.artifacts.os.open", side_effect=AssertionError("open expired file")):
            detail = artifacts.inspect_artifact(self.path, {}, deadline=0)
        self.assertEqual("validation_budget_exceeded", detail["reason_code"])


if __name__ == "__main__":
    unittest.main()
