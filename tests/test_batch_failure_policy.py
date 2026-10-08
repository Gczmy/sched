"""Failure isolation on private fixtures; no daemon or real worker processes."""
import json
import os
import sqlite3
import unittest
from unittest import mock

from gsched import cli, state
from gsched.schema import SchemaError, validate_batch
from test_review_cli_state import TempStateCase
from test_review_dispatcher_gpu import DispatcherStateCase


def seed_attempt(conn, job_id, version=1, phase="unresolved"):
    conn.execute("INSERT INTO execution_attempts"
                 " (attempt_id,job_id,job_version,backend_id,backend_config_sha256,phase,identity,created_at)"
                 " VALUES (?,?,?,?,?,?,?,?)",
                 (job_id + "-attempt", job_id, version, "example", "a" * 64,
                  phase, '{"binding":"immutable"}', state.now()))


class BatchPolicyCliTests(TempStateCase):
    batch = "batch-20260829-000000"

    def query(self):
        code, out, err = self.capture(cli.main, ["batch-policy", self.batch, "--json"])
        self.assertEqual(0, code, err)
        return json.loads(out)

    def identity(self):
        code, out, err = self.capture(cli.main, ["identity", "--json"])
        self.assertEqual(0, code, err)
        return json.loads(out)["instance_id"]

    def request(self, *, rid="policy", policy="continue_independent", reopen=False,
                revision=None, status="active", yes=True):
        args = ["request", rid, "--json", "--expect-kind", "batch", "--expect-id", self.batch,
                "--expect-status", status, "--expect-revision",
                str(self.batch_revision() if revision is None else revision),
                "--expect-instance", self.identity(),
                "--expect-project", "p", "--", "batch-policy", self.batch,
                "--failure-policy", policy]
        return [*args, *(["--reopen"] if reopen else []), *(["--yes"] if yes else [])]

    def test_enum_default_and_normalized_submission(self):
        spec = {"name": "example", "project": "p", "tasks": [
            {"id": "t", "cmd": ["/bin/true"], "resources": {"gpu": 0}}]}
        self.assertEqual("freeze", validate_batch(spec, self.cfg)["failure_policy"])
        for value in (None, True, 0, [], "continue", ""):
            with self.subTest(value=value), self.assertRaises(SchemaError):
                validate_batch(dict(spec, failure_policy=value), self.cfg)
        path = os.path.join(self.tmp.name, "batch.json")
        with open(path, "w") as stream:
            json.dump(dict(spec, failure_policy="continue_independent"), stream)
        with mock.patch.object(cli, "_ensure_running_locked", return_value="not started"):
            code, out, err = self.capture(cli.main, ["submit", path, "--json"])
        self.assertEqual(0, code, err)
        with state.connect() as conn:
            self.assertEqual("continue_independent", state.get_batch(conn, json.loads(out)["batch_id"])["failure_policy"])

    def test_query_is_read_only_and_does_not_add_keys_to_status(self):
        self.seed_batch(job_status="pending")
        revision = self.batch_revision()
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migration")):
            result = self.query()
        self.assertEqual("freeze", result["failure_policy"])
        self.assertEqual("stored", result["source"])
        self.assertEqual("none", result["effect"])
        self.assertTrue(result["task_dag_supported"])
        self.assertEqual(revision, self.batch_revision())
        self.assertNotIn("failure_policy", self.status_json()["batches"][0])

    def test_gateway_delivery_preserves_policy_at_inbox_acceptance(self):
        from gsched.dispatcher import Dispatcher
        path = os.path.join(self.tmp.name, "delivery.json")
        with open(path, "w") as stream:
            json.dump({"name": "delivery", "project": "p", "failure_policy": "continue_independent",
                       "tasks": [{"id": "t", "cmd": ["/bin/true"], "resources": {"gpu": 0}}]}, stream)
        with mock.patch.dict(os.environ, {"SCHED_ALLOW_FOREIGN_WRITE": ""}), \
             mock.patch("socket.gethostname", return_value="gateway"), \
             mock.patch.object(state, "init_db", side_effect=AssertionError("gateway initialized state")):
            code, out, err = self.capture(cli.main, ["submit", path, "--request-id", "delivery", "--json"])
        self.assertEqual(0, code, err)
        result = json.loads(out)
        self.assertFalse(result["persisted"])
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg, dispatcher.executor = self.cfg, None
        dispatcher.log_line = mock.Mock()
        dispatcher._drain_submit_inbox()
        dispatcher._process_control_requests()
        with state.connect() as conn:
            self.assertEqual("continue_independent", state.get_batch(conn, result["batch_id"])["failure_policy"])

    def test_noop_policy_keeps_revision(self):
        self.seed_batch()
        revision = self.batch_revision()
        self.assertEqual(0, self.capture(cli.main, self.request(policy="freeze"))[0])
        self.assertEqual(revision, self.batch_revision())

    def test_direct_write_and_query_reopen_reject_before_state(self):
        for args in (["--failure-policy", "continue_independent", "--yes"], ["--reopen"]):
            with mock.patch.object(state, "connect", side_effect=AssertionError("DB opened")), \
                 mock.patch.object(state, "init_db", side_effect=AssertionError("migration")):
                code, _, _ = self.capture(cli.main, ["batch-policy", self.batch, *args])
            self.assertEqual(64, code)

    def test_request_change_is_atomic_replayable_and_preserves_job(self):
        job_id = self.seed_batch(job_status="running")
        with state.connect() as conn:
            seed_attempt(conn, job_id)
            before = dict(state.get_job(conn, job_id))
            attempt = dict(conn.execute("SELECT * FROM execution_attempts").fetchone())
        revision = self.batch_revision()
        args = self.request()
        first = self.capture(cli.main, args)
        self.assertEqual(0, first[0], first[2])
        with mock.patch.object(cli, "_run_captured_mutation", side_effect=AssertionError("re-dispatch")):
            second = self.capture(cli.main, args)
        a, b = json.loads(first[1]), json.loads(second[1])
        self.assertEqual(a["result"], b["result"])
        self.assertTrue(b["replayed"])
        self.assertEqual("continue_independent", a["result"]["effect"]["failure_policy"])
        self.assertEqual(revision + 1, self.batch_revision())
        with state.connect() as conn:
            self.assertEqual(before, dict(state.get_job(conn, job_id)))
            self.assertEqual(attempt, dict(conn.execute("SELECT * FROM execution_attempts").fetchone()))

    def test_missing_confirmation_rolls_back_and_has_rejected_receipt(self):
        self.seed_batch()
        code, out, _ = self.capture(cli.main, self.request(yes=False))
        self.assertEqual(1, code)
        self.assertEqual("rejected", json.loads(out)["result"]["outcome"])
        self.assertEqual("freeze", self.query()["failure_policy"])

    def test_policy_aba_increments_revision_and_old_cas_stays_rejected(self):
        self.seed_batch()
        old = self.batch_revision()
        self.assertEqual(0, self.capture(cli.main, self.request())[0])
        self.assertEqual(0, self.capture(cli.main, self.request(rid="back", policy="freeze"))[0])
        self.assertEqual(old + 2, self.batch_revision())
        args = self.request(rid="stale", revision=old)
        first = self.capture(cli.main, args)
        second = self.capture(cli.main, args)
        self.assertEqual(65, first[0])
        self.assertEqual("revision_changed", json.loads(first[1])["result"]["error"]["conflict_reason"])
        self.assertEqual(json.loads(first[1])["result"], json.loads(second[1])["result"])
        self.assertTrue(json.loads(second[1])["replayed"])

    def test_blocked_policy_change_does_not_reopen_until_explicit_request(self):
        job_id = self.seed_batch(batch_status="blocked", job_status="pending")
        self.assertEqual(0, self.capture(cli.main, self.request(status="blocked"))[0])
        self.assertEqual("blocked", self.query()["status"])
        self.assertEqual(0, self.capture(cli.main, self.request(rid="reopen", status="blocked", reopen=True))[0])
        self.assertEqual("active", self.query()["status"])
        with state.connect() as conn:
            self.assertEqual("pending", state.get_job(conn, job_id)["status"])
            self.assertEqual(1, conn.execute("SELECT count(*) FROM jobs").fetchone()[0])

    def test_reopen_rejects_failure_only_and_retired_batches(self):
        self.seed_batch(batch_status="blocked")
        self.assertEqual(65, self.capture(cli.main, self.request(status="blocked", reopen=True))[0])
        for status in ("done", "discarded"):
            with state.connect() as conn:
                conn.execute("UPDATE batches SET status=? WHERE id=?", (status, self.batch))
            self.assertEqual(65, self.capture(cli.main, self.request(rid=status, status=status))[0])

    def test_historical_strict_batch_is_readable_but_cannot_be_reopened(self):
        job_id = self.seed_batch(batch_status="blocked", job_status="pending")
        with state.connect() as conn:
            conn.execute("UPDATE batches SET mode='strict' WHERE id=?", (self.batch,))
            before = dict(state.get_job(conn, job_id))
        self.assertEqual("freeze", self.query()["failure_policy"])
        self.assertEqual(65, self.capture(cli.main, self.request(status="blocked", reopen=True))[0])
        self.assertEqual(("freeze", "blocked"), (self.query()["failure_policy"], self.query()["status"]))
        with state.connect() as conn:
            self.assertEqual(before, dict(state.get_job(conn, job_id)))

    def test_request_query_is_not_a_mutation(self):
        self.seed_batch()
        args = self.request()
        args = args[:args.index("--failure-policy")]
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migration")):
            self.assertEqual(64, self.capture(cli.main, args)[0])

    def test_schema10_read_and_migration_preserve_identity_status_and_unknown_attempt(self):
        job_id = self.seed_batch(batch_status="blocked", job_status="interrupted")
        identity = self.identity()
        with state.connect() as conn:
            seed_attempt(conn, job_id)
            before_job = dict(state.get_job(conn, job_id))
            before_attempt = dict(conn.execute("SELECT * FROM execution_attempts").fetchone())
            revision = self.batch_revision()
            conn.execute("DROP TRIGGER revision_batch_failure_policy")
            conn.execute("ALTER TABLE batches DROP COLUMN failure_policy")
            conn.execute("PRAGMA user_version=10")
        with mock.patch.object(state, "init_db", side_effect=AssertionError("read migration")):
            policy = self.query()
        self.assertEqual("legacy_default", policy["source"])
        self.assertEqual("freeze", policy["failure_policy"])
        with state.connect() as conn:
            self.assertEqual(10, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertNotIn("failure_policy", state.get_batch(conn, self.batch).keys())
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(state.DB_SCHEMA_VERSION, conn.execute("PRAGMA user_version").fetchone()[0])
            batch = state.get_batch(conn, self.batch)
            self.assertEqual(("blocked", "freeze", revision),
                             (batch["status"], batch["failure_policy"], batch["revision"]))
            self.assertEqual(before_job, dict(state.get_job(conn, job_id)))
            self.assertEqual(before_attempt, dict(conn.execute("SELECT * FROM execution_attempts").fetchone()))
        self.assertEqual(identity, self.identity())

    def test_schema_migration_failure_is_atomic(self):
        self.seed_batch()
        with state.connect() as conn:
            conn.execute("DROP TRIGGER revision_batch_failure_policy")
            conn.execute("ALTER TABLE batches DROP COLUMN failure_policy")
            conn.execute("PRAGMA user_version=10")
        migrate = state.migrate_batch_failure_policy
        def fail(conn):
            migrate(conn)
            raise sqlite3.OperationalError("injected policy migration failure")
        with mock.patch.object(state, "migrate_batch_failure_policy", side_effect=fail), \
             self.assertRaises(sqlite3.OperationalError):
            state.init_db()
        with state.connect() as conn:
            self.assertEqual(10, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertNotIn("failure_policy", state.get_batch(conn, self.batch).keys())


class BatchPolicySettlementTests(DispatcherStateCase):
    def setup_jobs(self, statuses, *, policy="continue_independent", batch_status="active"):
        self.seed_jobs([(key, {"gpu": 0, "cpus": 1}, status) for key, status in statuses])
        with state.connect() as conn:
            conn.execute("UPDATE batches SET failure_policy=?,status=? WHERE id='batch'", (policy, batch_status))
        dispatcher = self.dispatcher()
        dispatcher._write_marker = mock.Mock()
        dispatcher._notify_batch = mock.Mock()
        return dispatcher

    def batch_status(self):
        with state.connect() as conn:
            return state.get_batch(conn, "batch")["status"]

    def test_freeze_default_blocks_pending_but_does_not_cancel_running(self):
        dispatcher = self.setup_jobs([("a", "failed"), ("b", "pending"), ("c", "running")], policy="freeze")
        with state.connect() as conn:
            running = dict(state.get_job(conn, "c"))
        dispatcher._settle_batch_status()
        self.assertEqual("blocked", self.batch_status())
        with state.connect() as conn:
            self.assertEqual(running, dict(state.get_job(conn, "c")))
        dispatcher._launch_job = mock.Mock()
        dispatcher._dispatch_ready_jobs()
        dispatcher._launch_job.assert_not_called()

    def test_continue_launches_independent_pending_and_preserves_running(self):
        dispatcher = self.setup_jobs([("a", "blocked"), ("b", "pending"), ("c", "running")])
        with state.connect() as conn:
            running = dict(state.get_job(conn, "c"))
        dispatcher._settle_batch_status()
        self.assertEqual("active", self.batch_status())
        launches = []
        dispatcher._launch_job = mock.Mock(side_effect=lambda _conn, job, _gpu: launches.append(job["id"]) or True)
        dispatcher._dispatch_ready_jobs()
        self.assertEqual(["b"], launches)
        with state.connect() as conn:
            self.assertEqual(running, dict(state.get_job(conn, "c")))
        dispatcher._notify_batch.assert_not_called()

    def test_final_failure_emits_terminal_once(self):
        dispatcher = self.setup_jobs([("a", "failed"), ("b", "done")])
        dispatcher._settle_batch_status()
        dispatcher._settle_batch_status()
        self.assertEqual("blocked", self.batch_status())
        dispatcher._notify_batch.assert_called_once_with("batch", "batch", "blocked")

    def test_all_success_still_finishes(self):
        dispatcher = self.setup_jobs([("a", "done"), ("b", "skip")])
        dispatcher._settle_batch_status()
        self.assertEqual("done", self.batch_status())

    def test_unknown_attempt_blocks_success_as_well_as_failure(self):
        dispatcher = self.setup_jobs([("a", "done"), ("b", "skip")])
        with state.connect() as conn:
            seed_attempt(conn, "a")
        dispatcher._settle_batch_status()
        self.assertEqual("active", self.batch_status())
        dispatcher._notify_batch.assert_not_called()

    def test_old_blocked_is_not_automatically_reopened(self):
        dispatcher = self.setup_jobs([("a", "failed"), ("b", "pending")], batch_status="blocked")
        dispatcher._settle_batch_status()
        self.assertEqual("blocked", self.batch_status())
        dispatcher._launch_job = mock.Mock()
        dispatcher._dispatch_ready_jobs()
        dispatcher._launch_job.assert_not_called()

    def test_unknown_attempt_prevents_terminal_publication_without_replay(self):
        dispatcher = self.setup_jobs([("a", "failed"), ("b", "done")])
        with state.connect() as conn:
            seed_attempt(conn, "a")
            before = dict(conn.execute("SELECT * FROM execution_attempts").fetchone())
        dispatcher._settle_batch_status()
        self.assertEqual("active", self.batch_status())
        with state.connect() as conn:
            self.assertEqual(before, dict(conn.execute("SELECT * FROM execution_attempts").fetchone()))
        dispatcher.executor.start.assert_not_called()
        dispatcher._notify_batch.assert_not_called()

    def test_unresolved_marker_prevents_terminal_publication(self):
        dispatcher = self.setup_jobs([("a", "failed"), ("b", "done")])
        dispatcher._batch_has_unresolved_launch_marker = mock.Mock(return_value=True)
        dispatcher._settle_batch_status()
        self.assertEqual("active", self.batch_status())

    def test_old_running_holds_but_old_pending_does_not(self):
        dispatcher = self.setup_jobs([("a", "failed"), ("b", "running")])
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE id='b'").fetchone()[0])
            state.insert_task(conn, "batch", "b", 2, spec, 1, "p")
            state.insert_job(conn, "b-v2", "batch", "b", 2, "new", None, "p")
            state.update_job(conn, "b-v2", status="done")
        dispatcher._settle_batch_status()
        self.assertEqual("active", self.batch_status())
        with state.connect() as conn:
            state.update_job(conn, "b", status="pending")
        dispatcher._settle_batch_status()
        self.assertEqual("blocked", self.batch_status())


if __name__ == "__main__":
    unittest.main()
