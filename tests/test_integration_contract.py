"""Identity, request receipt and ownership failure boundaries on isolated state."""
import json
import os
from unittest import mock

from gsched import cli, integration, state
from test_review_cli_state import TempStateCase


class IntegrationContractTests(TempStateCase):
    def query(self, *args):
        code, output, error = self.capture(cli.main, [*args, "--json"])
        self.assertEqual(code, 0, error)
        return json.loads(output)

    def test_identity_is_stable_across_initialization_and_is_read_only(self):
        first = self.query("identity")
        self.assertTrue(first["available"])
        state.init_db()
        with mock.patch.object(state, "init_db", side_effect=AssertionError("read migrated")):
            self.assertEqual(first, self.query("identity"))
        with state.connect() as conn:
            with self.assertRaises(Exception):
                conn.execute("DELETE FROM scheduler_identity")

    def test_legacy_identity_does_not_migrate(self):
        with state.connect() as conn:
            conn.execute("DROP TRIGGER scheduler_identity_retained")
            conn.execute("DROP TABLE scheduler_identity")
            conn.execute("PRAGMA user_version=9")
        with mock.patch.object(state, "init_db", side_effect=AssertionError("read migrated")):
            reply = self.query("identity")
            self.assertIsNone(reply["instance_id"])
            self.assertEqual(reply["reason"], "migration_required")

    def test_schema9_receipt_without_structured_result_is_read_only(self):
        with state.connect() as conn:
            conn.execute("INSERT INTO operation_requests(request_id, argv, status, code, created_at) VALUES ('legacy', '{}', 'done', 0, ?)", (state.now(),))
            conn.execute("ALTER TABLE operation_requests DROP COLUMN result_json")
            conn.execute("PRAGMA user_version=9")
        with mock.patch.object(state, "init_db", side_effect=AssertionError("read migrated")):
            reply = self.query("request-status", "legacy")
        self.assertEqual(reply["phase"], "done")
        self.assertIsNone(reply["instance_id"])
        self.assertIsNone(reply["result"])

    def test_current_schema_missing_identity_is_never_recreated(self):
        with state.connect() as conn:
            conn.execute("DROP TRIGGER scheduler_identity_retained")
            conn.execute("DELETE FROM scheduler_identity")
        with self.assertRaises(state.StateError):
            state.init_db()
        with state.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM scheduler_identity").fetchone()[0], 0)

    def test_task_project_and_receipt_survive_output_compaction(self):
        self.seed_batch(job_status="pending")
        target = "batch-20260829-000000:task"
        task = self.query("task", target)
        self.assertEqual(task["project"], "p")
        self.assertEqual(task["instance_id"], self.query("identity")["instance_id"])
        argv = ["request", "once", "--expect-kind", "task", "--expect-id", target,
                "--expect-status", "pending", "--expect-version", "1",
                "--expect-revision", str(task["batch_revision"]),
                "--expect-instance", task["instance_id"], "--expect-project", "p",
                "--", "cancel", target, "--yes"]
        code, _, error = self.capture(cli.main, argv)
        self.assertEqual(code, 0, error)
        before = self.query("request-status", "once")
        self.assertEqual(before["result"]["effect"]["status"], "cancelled")
        with state.connect() as conn:
            state.compact_operation_outputs(conn, keep_recent=0)
        after = self.query("request-status", "once")
        self.assertTrue(after["output_compacted"])
        self.assertEqual(after["result"], before["result"])
        with mock.patch.object(cli, "_run_captured_mutation", side_effect=AssertionError("replayed")):
            self.assertEqual(self.capture(cli.main, argv)[0], 0)

    def test_wrong_instance_or_project_cannot_mutate(self):
        self.seed_batch()
        target = "batch-20260829-000000:task"
        task = self.query("task", target)
        for key, value in (("instance", "0" * 32), ("project", "other")):
            args = ["request", "wrong-" + key, "--expect-kind", "task", "--expect-id", target,
                    "--expect-status", "failed", "--expect-version", "1", "--expect-revision",
                    str(task["batch_revision"]), "--expect-" + key, value, "--", "cancel", target, "--yes"]
            self.assertEqual(self.capture(cli.main, args)[0], 65)
        self.assertEqual(self.query("task", target)["jobs"][0]["status"], "failed")

    def test_unknown_and_missing_receipts_are_distinct(self):
        with state.connect() as conn:
            conn.execute("INSERT INTO operation_requests(request_id, argv, status, created_at) VALUES ('lost', '{}', 'started', ?)", (state.now(),))
        self.assertEqual(self.query("request-status", "lost")["phase"], "unknown")
        self.assertEqual(self.query("request-status", "absent")["phase"], "not_found")


class SubmissionContractTests(TempStateCase):
    def setUp(self):
        super().setUp()
        self.path = os.path.join(self.tmp.name, "batch.json")
        self.spec = {"schema_version": 1, "name": "idempotent", "project": "p",
                     "tasks": [{"id": "job", "cmd": ["/bin/true"], "gpus": 0}]}
        with open(self.path, "w") as stream:
            json.dump(self.spec, stream)
        wake = mock.patch.object(cli, "_ensure_running_locked", return_value="not started for test")
        wake.start()
        self.addCleanup(wake.stop)

    def submit(self, request_id="submission-1", extra=()):
        return self.capture(cli.main, ["submit", self.path, "--request-id", request_id, "--json", *extra])

    def receipt(self, request_id="submission-1"):
        code, output, error = self.capture(cli.main, ["request-status", request_id, "--json"])
        self.assertEqual(code, 0, error)
        return json.loads(output)

    def gateway(self):
        stack = __import__("contextlib").ExitStack()
        stack.enter_context(mock.patch.dict(os.environ, {"SCHED_ALLOW_FOREIGN_WRITE": ""}))
        stack.enter_context(mock.patch("socket.gethostname", return_value="gateway"))
        stack.enter_context(mock.patch.object(state, "init_db", side_effect=AssertionError("gateway initialized DB")))
        return stack

    def consume(self):
        from gsched.dispatcher import Dispatcher
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg, dispatcher.executor = self.cfg, None
        dispatcher.log_line = lambda message: None
        dispatcher._drain_submit_inbox()
        dispatcher._process_control_requests()

    def test_local_replay_and_content_conflict_keep_one_batch(self):
        first = self.submit()
        self.assertEqual(first[0], 0, first)
        result = json.loads(first[1])
        self.assertTrue(result["persisted"])
        second = self.submit()
        self.assertEqual(second[0], 0, second)
        self.assertEqual(json.loads(second[1])["batch_id"], result["batch_id"])
        with open(self.path, "w") as stream:
            json.dump(dict(self.spec, name="another"), stream)
        self.assertEqual(self.submit()[0], 64)
        with state.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM batches").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT count(*) FROM submission_requests").fetchone()[0], 1)

    def test_gateway_replay_then_atomic_daemon_receipt(self):
        with self.gateway():
            first = self.submit()
            second = self.submit()
            self.assertEqual(first[0], 0, first)
            self.assertEqual(second[0], 0, second)
            first_json = json.loads(first[1])
            self.assertFalse(first_json["persisted"])
            self.assertEqual(first_json["batch_id"], json.loads(second[1])["batch_id"])
            self.assertEqual(self.receipt()["phase"], "delivered")
        payloads = [name for name in os.listdir(state.submission_inbox_dir()) if name.endswith(".json")]
        self.assertEqual(len(payloads), 1)
        self.consume()
        receipt = self.receipt()
        self.assertEqual(receipt["phase"], "done")
        self.assertEqual(receipt["code"], 0)
        self.assertTrue(receipt["result"]["persisted"])
        self.assertEqual(receipt["result"]["batch_id"], first_json["batch_id"])
        self.assertEqual(self.submit()[0], 0)

    def test_gateway_rejection_is_durable_and_never_republished(self):
        with self.gateway():
            first = self.submit(extra=("--expect-project", "other"))
            self.assertEqual(first[0], 0, first)
        self.consume()
        receipt = self.receipt()
        self.assertEqual(receipt["phase"], "done")
        self.assertEqual(receipt["code"], 1)
        self.assertFalse(receipt["result"]["persisted"])
        with self.gateway():
            self.assertEqual(self.submit(extra=("--expect-project", "other"))[0], 1)
        with state.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM batches").fetchone()[0], 0)

    def test_lost_gateway_receipt_recovers_from_original_daemon_commit(self):
        real_save = integration.save_ticket
        count = [0]
        def lose_final(ticket):
            count[0] += 1
            if count[0] == 2:
                raise OSError("receipt response lost")
            return real_save(ticket)
        with self.gateway(), mock.patch.object(integration, "save_ticket", side_effect=lose_final):
            self.assertEqual(self.submit()[0], 75)
        self.assertEqual(self.receipt()["phase"], "unknown")
        self.consume()
        self.assertTrue(self.receipt()["result"]["persisted"])
        with self.gateway():
            self.assertEqual(self.submit()[0], 0)
        with state.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM batches").fetchone()[0], 1)

    def test_uncertain_delivery_cannot_be_replaced_or_reported_rejected(self):
        with self.gateway(), mock.patch.object(cli, "_cmd_submit_impl", return_value=2) as invoke:
            self.assertEqual(self.submit()[0], 75)
            self.assertEqual(self.submit()[0], 75)
            self.assertEqual(invoke.call_count, 1)
        self.assertEqual(self.receipt()["phase"], "unknown")

    def test_malformed_success_output_remains_unknown_and_is_not_redispatched(self):
        with self.gateway(), mock.patch.object(cli, "_cmd_submit_impl", return_value=0) as invoke:
            self.assertEqual(self.submit()[0], 75)
            self.assertEqual(self.submit()[0], 75)
            self.assertEqual(invoke.call_count, 1)
        self.assertEqual(self.receipt()["phase"], "unknown")

    def test_submission_id_cannot_be_reused_as_mutation(self):
        self.assertEqual(self.submit()[0], 0)
        result = self.capture(cli.main, ["request", "submission-1", "--expect-revision", "0", "--", "daemon", "drain"])
        self.assertEqual(result[0], 64, result)

    def test_failed_transaction_keeps_batch_and_receipt_atomic(self):
        with mock.patch.object(integration, "complete_submission", side_effect=RuntimeError("crash before commit")):
            self.assertEqual(self.submit()[0], 75)
        with state.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM batches").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT count(*) FROM submission_requests").fetchone()[0], 0)
        self.assertEqual(self.receipt()["phase"], "unknown")


    def test_two_processes_publish_one_original_batch(self):
        import subprocess
        import sys
        script = ("from gsched import cli; cli._ensure_running_locked=lambda:'test only'; "
                  "raise SystemExit(cli.main(__import__('sys').argv[1:]))")
        argv = [sys.executable, "-B", "-c", script, "submit", self.path,
                "--request-id", "concurrent", "--json"]
        children = [subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    for _ in range(2)]
        replies = []
        for child in children:
            stdout, stderr = child.communicate(timeout=30)
            self.assertIn(child.returncode, (0, 75), stderr)
            if child.returncode == 0:
                replies.append(json.loads(stdout))
        self.assertTrue(replies)
        self.assertEqual(len({reply["batch_id"] for reply in replies}), 1)
        code, stdout, _ = self.submit("concurrent")
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout)["batch_id"], replies[0]["batch_id"])
        with state.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM batches").fetchone()[0], 1)

    def test_expectations_without_request_are_rejected(self):
        code, _, _ = self.capture(cli.main, ["submit", self.path, "--expect-project", "other"])
        self.assertEqual(code, 64)
        with state.connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM batches").fetchone()[0], 0)
