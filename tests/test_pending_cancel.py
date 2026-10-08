"""Exact group cancellation on disposable fixtures, no real workers/signals."""
import json
from pathlib import Path
from unittest import mock

from gsched import artifact_validation, cli, execution_state, pending_cancel, state
from test_review_cli_state import TempStateCase
from test_batch_failure_policy import seed_attempt


class PendingCancelTests(TempStateCase):
    def setUp(self):
        super().setUp()
        host = mock.patch("gsched.cli._is_foreign_host", return_value=False)
        host.start()
        self.addCleanup(host.stop)
        with state.connect() as conn:
            state.insert_batch(conn, "group", "display-name", "mix", [], None, self.tmp.name, {}, project="p")
            for index, task in enumerate(("a", "b", "c")):
                spec = {"cmd": ["/bin/true"], "cwd_abs": self.tmp.name, "artifacts": {}, "resources": {"gpu": 0}}
                state.insert_task(conn, "group", task, 1, spec, index, "p")
                state.insert_job(conn, f"group-{task}-v1", "group", task, 1, "fp-" + task, None, "p")

    def facts(self, *tasks):
        selectors = [{"task_id": task, "version": 1} for task in tasks]
        code, out, err = self.capture(cli.main, ["task-facts", "group", "--tasks-json", json.dumps(selectors), "--json"])
        self.assertEqual(0, code, err)
        return json.loads(out)

    def request(self, rid="cancel-group", *, facts=None):
        facts = facts or self.facts("a", "b")
        return ["request", rid, "--json", "--expect-kind", "batch", "--expect-id", "group",
                "--expect-status", facts["batch_status"], "--expect-revision", str(facts["batch_revision"]),
                "--expect-instance", facts["instance_id"], "--", "cancel-pending", "group",
                "--tasks-json", json.dumps([task["binding"] for task in facts["tasks"]]), "--yes"]

    def statuses(self):
        with state.connect() as conn:
            return {row["task_id"]: row["status"] for row in conn.execute("SELECT * FROM jobs WHERE batch_id='group'")}

    def test_query_is_exact_passive_bounded_and_field_sets_unchanged(self):
        revision = self.batch_revision("group")
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migrated")), mock.patch.object(pending_cancel, "check_local_files", side_effect=AssertionError("read files")):
            output = self.facts("a", "b")
        self.assertEqual("sched-task-facts-v1", output["contract"])
        self.assertIsNone(output["cancel_ready"])
        self.assertFalse(output["truncated"] or output["launch_markers_checked"])
        self.assertTrue(all(task["recorded_never_started"] for task in output["tasks"]))
        self.assertEqual(revision, self.batch_revision("group"))
        code, _, _ = self.capture(cli.main, ["task-facts", "display-name", "--tasks-json", '[{"task_id":"a","version":1}]', "--json"])
        self.assertEqual(1, code)

    def test_atomic_group_cancel_replays_without_control_requests_or_signals(self):
        before = self.facts("a", "b")
        request = self.request(facts=before)
        with mock.patch("os.killpg", side_effect=AssertionError("signal")), mock.patch.object(state, "insert_control_request", side_effect=AssertionError("kill request")):
            code, out, err = self.capture(cli.main, request)
        self.assertEqual(0, code, err)
        effect = json.loads(out)["result"]["effect"]
        self.assertEqual(2, effect["count"])
        self.assertFalse(effect["signals_sent"] or effect["running_tasks_touched"] or effect["artifacts_deleted"])
        self.assertEqual({"a": "cancelled", "b": "cancelled", "c": "pending"}, self.statuses())
        revision = self.batch_revision("group")
        self.assertEqual(before["batch_revision"] + 2, revision)
        self.assertEqual(0, self.capture(cli.main, request)[0])
        self.assertEqual(revision, self.batch_revision("group"))
        self.assertEqual(65, self.capture(cli.main, self.request("stale-new", facts=before))[0])

    def test_latest_waiting_states_are_pending_and_unselected_running_is_untouched(self):
        with state.connect() as conn:
            state.update_job(conn, "group-a-v1", status="waiting_dep")
            state.update_job(conn, "group-b-v1", status="waiting_quota")
            state.update_job(conn, "group-c-v1", status="running", started_at=state.now(), pgid=123)
        self.assertEqual(0, self.capture(cli.main, self.request())[0])
        self.assertEqual("running", self.statuses()["c"])
        with state.connect() as conn:
            self.assertEqual(123, state.get_job(conn, "group-c-v1")["pgid"])

    def test_running_race_rejects_whole_group(self):
        request = self.request()
        with state.connect() as conn:
            state.update_job(conn, "group-b-v1", status="running", pgid=123)
        self.assertEqual(65, self.capture(cli.main, request)[0])
        self.assertEqual("pending", self.statuses()["a"])

    def test_spec_fingerprint_and_attempt_changes_even_without_batch_revision_conflict(self):
        for kind in ("spec", "fingerprint", "attempt"):
            with self.subTest(kind=kind):
                request = self.request("changed-" + kind)
                with state.connect() as conn:
                    revision = state.get_batch(conn, "group")["revision"]
                    if kind == "spec":
                        conn.execute("UPDATE tasks SET spec=? WHERE batch_id='group' AND id='b'", (json.dumps({"cmd": ["/bin/false"]}),))
                    elif kind == "fingerprint":
                        conn.execute("UPDATE jobs SET fingerprint='changed' WHERE id='group-b-v1'")
                    else:
                        seed_attempt(conn, "group-b-v1")
                    self.assertEqual(revision, state.get_batch(conn, "group")["revision"])
                self.assertEqual(65, self.capture(cli.main, request)[0])
                self.assertEqual({"a": "pending", "b": "pending", "c": "pending"}, self.statuses())

    def test_all_generations_start_records_and_history_gaps_refuse(self):
        with state.connect() as conn:
            state.update_job(conn, "group-b-v1", status="failed", started_at=state.now(), rc=1)
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id='group' AND id='b'").fetchone()[0])
            state.insert_task(conn, "group", "b", 2, spec, 1, "p")
            state.insert_job(conn, "group-b-v2", "group", "b", 2, "fp-b", None, "p")
        selectors = '[{"task_id":"a","version":1},{"task_id":"b","version":2}]'
        code, out, err = self.capture(cli.main, ["task-facts", "group", "--tasks-json", selectors, "--json"])
        self.assertEqual(0, code, err)
        facts = json.loads(out)
        self.assertFalse(facts["tasks"][1]["recorded_never_started"])
        self.assertEqual(65, self.capture(cli.main, self.request(facts=facts))[0])
        self.assertEqual("pending", self.statuses()["a"])
        with state.connect() as conn:
            conn.execute("DELETE FROM jobs WHERE id='group-b-v1'")
            conn.execute("DELETE FROM tasks WHERE batch_id='group' AND id='b' AND version=1")
        code, out, _ = self.capture(cli.main, ["task-facts", "group", "--tasks-json", selectors, "--json"])
        self.assertEqual(0, code)
        self.assertIn("generation_history_incomplete", json.loads(out)["tasks"][1]["refusal_reasons"])

    def test_marker_profile_log_and_orphan_rc_files_refuse_without_deleting(self):
        import hashlib
        prefix = hashlib.sha256(b"group-b-v1").hexdigest()[:24]
        paths = [Path(state.launch_marker_path("group-b-v1")), Path(state.host_dir()) / "profiles/group-b-v1.json",
                 Path(state.host_dir()) / "logs/group/b-v1.log", Path(state.host_dir()) / "rc" / (prefix + "-123.rc")]
        for index, path in enumerate(paths):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("unknown")
            with self.subTest(path=path):
                code, _, _ = self.capture(cli.main, self.request("files-" + str(index)))
                self.assertEqual(65, code)
                self.assertEqual({"a": "pending", "b": "pending", "c": "pending"}, self.statuses())
                self.assertTrue(path.exists())
            path.unlink()

    def test_metadata_legacy_recovery_and_validation_records_refuse(self):
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id='group' AND id='b'").fetchone()[0])
            spec["_native_exec_profile_id"] = "retired"
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id='group' AND id='b'", (json.dumps(spec),))
        self.assertEqual(65, self.capture(cli.main, self.request())[0])
        with state.connect() as conn:
            spec.pop("_native_exec_profile_id")
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id='group' AND id='b'", (json.dumps(spec),))
            conn.execute("INSERT INTO recovery_watch(root_job_id,checkpoint_sha256,last_progress_at) VALUES ('group-b-v1',?,0)", ("a" * 64,))
        self.assertEqual(65, self.capture(cli.main, self.request("recovery"))[0])

    def test_original_not_started_closed_attempt_is_distinct_from_unknown_intent(self):
        with state.connect() as conn:
            conn.execute("UPDATE batches SET status='active' WHERE id='group'")
            state.update_job(conn, "group-b-v1", status="running", started_at=state.now())
            execution_state.reserve(conn, "group-b-v1", {"backend_id": "linux_fd", "backend_config_sha256": "a" * 64}, {"sha256": "b" * 64}, state.hostname(), 1)
            execution_state.launch_intent(conn, "group-b-v1")
            execution_state.observe(conn, "group-b-v1", {"status": "not_started", "group_clean": True, "pid": None, "returncode": None})
            state.update_job(conn, "group-b-v1", status="pending")
        facts = self.facts("a", "b")
        self.assertTrue(facts["tasks"][1]["recorded_never_started"])
        self.assertTrue(facts["tasks"][1]["generations"][0]["original_not_started_verified"])
        log = Path(state.host_dir()) / "logs/group/b-v1.log"
        log.parent.mkdir(parents=True)
        log.write_text("")  # Known prelaunch log, not inferred worker output.
        self.assertEqual(0, self.capture(cli.main, self.request(facts=facts))[0])
        self.assertTrue(log.exists())

    def test_statement_failure_rolls_back_partial_group_and_records_rejection(self):
        with state.connect() as conn:
            conn.execute("CREATE TRIGGER reject_second_cancel BEFORE UPDATE OF status ON jobs WHEN NEW.id='group-b-v1' AND NEW.status='cancelled' BEGIN SELECT RAISE(ABORT,'injected'); END")
        code, _, _ = self.capture(cli.main, self.request())
        self.assertNotEqual(0, code)
        self.assertEqual({"a": "pending", "b": "pending", "c": "pending"}, self.statuses())
        with state.connect() as conn:
            self.assertEqual("done", conn.execute("SELECT status FROM operation_requests WHERE request_id='cancel-group'").fetchone()[0])

    def test_direct_missing_instance_inline_file_and_unknown_rid_rejected(self):
        self.assertEqual(64, self.capture(cli.main, ["cancel-pending", "group", "--tasks-json", "[]", "--yes"])[0])
        args = self.request()
        index = args.index("--expect-instance")
        self.assertEqual(64, self.capture(cli.main, args[:index] + args[index + 2:])[0])
        invalid = args.copy()
        invalid[invalid.index("--tasks-json") + 1] = "/tmp/input.json"
        self.assertEqual(64, self.capture(cli.main, invalid)[0])
        rid, command, expectation = cli._request_envelope(cli._build_parser().parse_args(args))
        with state.connect() as conn:
            conn.execute("INSERT INTO operation_requests(request_id,argv,status,created_at) VALUES (?,?,'started',?)", (rid, canonical({"command": command, "expect": expectation}), state.now()))
        self.assertEqual(75, self.capture(cli.main, args)[0])
        self.assertEqual("pending", self.statuses()["a"])

    def test_bounds_duplicate_and_oversized_or_unavailable_histories_rejected(self):
        bad = ["[]", "null", '[{"task_id":"a","version":true}]', '[{"task_id":"a","version":"latest"}]',
               json.dumps([{"task_id": "a", "version": 1}] * 2), json.dumps([{"task_id": str(i), "version": 1} for i in range(101)])]
        for raw in bad:
            with self.subTest(raw=raw[:40]), self.assertRaises(ValueError):
                pending_cancel.normalize(raw)
        with mock.patch.object(pending_cancel, "MAX_GENERATIONS", 0):
            self.assertEqual(1, self.capture(cli.main, ["task-facts", "group", "--tasks-json", '[{"task_id":"a","version":1}]', "--json"])[0])
        with mock.patch.object(pending_cancel, "MAX_BYTES", 1):
            self.assertEqual(1, self.capture(cli.main, ["task-facts", "group", "--tasks-json", '[{"task_id":"a","version":1}]', "--json"])[0])

    def test_foreign_host_even_with_override_never_cancels(self):
        with mock.patch("gsched.cli._is_foreign_host", return_value=True), mock.patch.dict("os.environ", SCHED_ALLOW_FOREIGN_WRITE="1"):
            self.assertEqual(65, self.capture(cli.main, self.request())[0])
        self.assertEqual("pending", self.statuses()["a"])

    def test_prior_unknown_request_is_not_erased_or_reinterpreted(self):
        args = self.request("earlier-unknown")
        rid, command, expectation = cli._request_envelope(cli._build_parser().parse_args(args))
        with state.connect() as conn:
            conn.execute("INSERT INTO operation_requests(request_id,argv,status,created_at) VALUES (?,?,'started',?)", (rid, canonical({"command": command, "expect": expectation}), state.now()))
        facts = self.facts("a", "b")
        self.assertIn("historical_operation_result_unknown", facts["tasks"][0]["refusal_reasons"])
        self.assertEqual(65, self.capture(cli.main, self.request("different-group", facts=facts))[0])
        with state.connect() as conn:
            self.assertEqual("started", conn.execute("SELECT status FROM operation_requests WHERE request_id=?", (rid,)).fetchone()[0])
        self.assertEqual("pending", self.statuses()["a"])

    def test_retry_reset_cannot_hide_immutable_prior_execution_validation(self):
        with state.connect() as conn:
            state.update_job(conn, "group-b-v1", status="running", pgid=123, rc=1, started_at=state.now())
            job = state.get_job(conn, "group-b-v1")
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id='group' AND id='b'").fetchone()[0])
            artifact_validation.record_initial(conn, job, spec, "exit_nonzero", [], rc=1, ordinary_wait={
                "source": "local_supervisor_wait", "subject": "scheduler_supervisor_command_chain", "returncode": 1,
                "pid": 123, "start_token": "proc:456", "group_clean": True, "binding_verified": True})
            state.update_job(conn, "group-b-v1", status="pending", pgid=None, rc=None, started_at=None, finished_at=None, retries=0)
        facts = self.facts("a", "b")
        self.assertIn("prior_artifact_validation", facts["tasks"][1]["refusal_reasons"])
        self.assertEqual(65, self.capture(cli.main, self.request(facts=facts))[0])
        self.assertEqual("pending", self.statuses()["a"])

    def test_missing_yes_has_no_group_effect(self):
        args = self.request()
        self.assertEqual(1, self.capture(cli.main, args[:-1])[0])
        self.assertEqual("pending", self.statuses()["a"])

    def test_huge_version_gap_is_bounded_without_allocating_a_huge_range(self):
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id='group' AND id='b'").fetchone()[0])
            version = 2**63 - 1
            state.insert_task(conn, "group", "b", version, spec, 1, "p")
            state.insert_job(conn, "group-b-max", "group", "b", version, "fp-b", None, "p")
        code, out, err = self.capture(cli.main, ["task-facts", "group", "--tasks-json", json.dumps([{"task_id": "b", "version": version}]), "--json"])
        self.assertEqual(0, code, err)
        self.assertIn("generation_history_incomplete", json.loads(out)["tasks"][0]["refusal_reasons"])

    def test_old_or_incomplete_schema_query_never_grants_never_started_authority(self):
        with state.connect() as conn:
            conn.execute("PRAGMA user_version=9")
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migrated")):
            facts = self.facts("a")
        self.assertFalse(facts["tasks"][0]["recorded_never_started"])
        with state.connect() as conn:
            self.assertEqual(9, conn.execute("PRAGMA user_version").fetchone()[0])
            conn.execute("PRAGMA user_version=15")
            conn.execute("DROP TABLE execution_attempts")
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migrated")):
            code, out, err = self.capture(cli.main, ["task-facts", "group", "--tasks-json", '[{"task_id":"a","version":1}]', "--json"])
        self.assertEqual(1, code, err)
        self.assertEqual("", out)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
