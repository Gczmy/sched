"""Passive, bounded cross-task browsing and explicit live pagination."""
import argparse
import base64
import json
from unittest import mock

from gsched import cli, execution_queries, execution_state, state
from test_review_cli_state import TempStateCase


class ExecutionQueryTests(TempStateCase):
    def query(self, **kwargs):
        with state.connect() as conn:
            conn.execute("BEGIN")
            return execution_queries.list_executions(conn, **kwargs)

    def attempted(self, index, *, project="p", owner=False):
        job = self.seed_batch(batch_id=f"batch-{index}", task_id="work", job_status="running")
        with state.connect() as conn:
            conn.execute("UPDATE jobs SET project=? WHERE id=?", (project, job))
            execution_state.reserve(conn, job, {"backend_id": "worker", "backend_config_sha256": "1" * 64},
                                    {"sha256": "2" * 64}, "review-node", 123)
            if owner:
                attempt = execution_state.get(conn, job)["attempt_id"]
                execution_state.bind_owner(conn, job, {"schema": "sched-execution-owner/v1", "owner_id": "3" * 32,
                    "endpoint": "gsched-owner-" + "3" * 32, "pid": 123, "start_ticks": 456,
                    "boot_id": "0" * 8 + "-" + "0" * 4 + "-" + "0" * 4 + "-" + "0" * 4 + "-" + "0" * 12,
                    "attempt_id": attempt, "token": "4" * 64})
        return job

    def test_filtered_joined_facts_are_passive_and_redacted(self):
        job = self.attempted(1, owner=True)
        self.attempted(2, project="other")
        with state.connect() as conn:
            execution_state.owner_observed(conn, job, "unreachable")
        with mock.patch("gsched.execution.PersistentOwner", side_effect=AssertionError("query connected owner")):
            result = self.query(project="p", backend="worker", phase="prepared", owner_status="unreachable")
        self.assertTrue(result["complete"])
        self.assertEqual(1, len(result["items"]))
        item = result["items"][0]
        self.assertEqual(job, item["job_id"])
        self.assertEqual("unreachable", item["attempt"]["owner_health"]["connection_status"])
        self.assertFalse(item["diagnostics"]["wait_result_available"])
        self.assertNotIn("token", item["attempt"]["owner"])
        self.assertNotIn("endpoint", item["attempt"]["owner"])

    def test_live_pages_bound_insertions_and_never_claim_complete_current_state(self):
        jobs = [self.attempted(i) for i in range(4)]
        first = self.query(limit=2)
        self.assertTrue(first["truncated"])
        self.assertFalse(first["complete"])
        self.assertEqual("live", first["consistency"])
        appended = self.attempted(5)
        with state.connect() as conn:
            state.update_job(conn, jobs[2], status="interrupted")
        second = self.query(limit=2, cursor=first["next_cursor"])
        self.assertEqual(jobs, [i["job_id"] for i in first["items"] + second["items"]])
        self.assertFalse(second["truncated"])
        self.assertFalse(second["complete"])
        self.assertNotIn(appended, [i["job_id"] for i in second["items"]])
        self.assertEqual("interrupted", second["items"][0]["diagnostics"]["job_status"])
        self.assertIn(appended, [i["job_id"] for i in self.query()["items"]])

    def test_filter_mutation_is_visible_and_does_not_claim_a_frozen_snapshot(self):
        jobs = [self.attempted(i, owner=True) for i in range(3)]
        first = self.query(limit=1, owner_status="unknown")
        with state.connect() as conn:
            execution_state.owner_observed(conn, jobs[1], "responsive")
        second = self.query(limit=10, owner_status="unknown", cursor=first["next_cursor"])
        self.assertEqual([jobs[2]], [i["job_id"] for i in second["items"]])
        self.assertFalse(second["complete"])

    def test_cursor_binds_scope_filters_and_original_upper_anchor(self):
        for i in range(3):
            self.attempted(i)
        token = self.query(limit=1)["next_cursor"]
        for kwargs in ({"project": "other"}, {"backend": "worker"}):
            with self.assertRaisesRegex(ValueError, "changed filters"):
                self.query(cursor=token, **kwargs)
        with state.connect() as conn:
            with mock.patch.object(state, "db_path", return_value="different-state"):
                with self.assertRaisesRegex(ValueError, "changed filters/node/state"):
                    execution_queries.list_executions(conn, cursor=token)
        with state.connect() as conn:
            conn.execute("UPDATE jobs SET id='replacement' WHERE id='batch-2-work-v1'")
        with self.assertRaisesRegex(ValueError, "anchor disappeared"):
            self.query(cursor=token)

    def test_bad_limits_and_cursors(self):
        for limit in (0, -1, 501, True):
            with self.assertRaises(ValueError):
                self.query(limit=limit)
        for cursor in ("", "!!!", "A" * 5000, base64.urlsafe_b64encode(b"{}").decode()):
            with self.assertRaises(ValueError):
                self.query(cursor=cursor)

    def test_ten_thousand_rows_do_not_fetch_history_per_item(self):
        self.seed_batch()
        with state.connect() as conn:
            conn.executemany("INSERT INTO jobs(id,batch_id,task_id,version,status,project) VALUES(?,?,?,?,?,?)",
                [(f"job-{i}", "batch-20260829-000000", f"task-{i}", 1, "pending", "p") for i in range(10000)])
        statements = []
        with state.connect() as conn:
            conn.set_trace_callback(statements.append)
            result = execution_queries.list_executions(conn, limit=50)
        self.assertEqual(50, len(result["items"]))
        self.assertTrue(result["truncated"])
        self.assertLess(len(statements), 12)
        self.assertTrue(all(i["diagnostics"]["replay_blocked"] for i in result["items"][1:]))

    def test_old_schema_without_execution_tables_is_not_migrated(self):
        self.seed_batch()
        with state.connect() as conn:
            for table in ("execution_owner_operations", "execution_owners", "execution_attempts", "native_sessions"):
                conn.execute("DROP TABLE " + table)
            conn.execute("PRAGMA user_version=1")
        state.set_query_only(True)
        try:
            result = self.query()
            self.assertEqual(1, result["database_schema"])
            self.assertEqual("subprocess", result["items"][0]["diagnostics"]["execution_kind"])
            self.assertEqual([], self.query(backend="worker")["items"])
        finally:
            state.set_query_only(False)
        with state.connect() as conn:
            self.assertEqual(1, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertIsNone(conn.execute("SELECT name FROM sqlite_master WHERE name='execution_attempts'").fetchone())

    def test_cli_batch_resolution_and_existing_task_contract(self):
        self.attempted(1)
        args = argparse.Namespace(task="list", version=None, batch="batch", project=None, backend=None,
                                  phase=None, owner_status=None, limit=0, cursor=None)
        code, _, error = self.capture(cli.cmd_execution, args)
        self.assertEqual(1, code)
        self.assertIn("limit", error)
        args.limit = 50
        code, text, error = self.capture(cli.cmd_execution, args)
        self.assertEqual(0, code, error)
        self.assertEqual("batch-1", json.loads(text)["filters"]["batch_id"])
        args.task = "batch-1:work"
        code, _, error = self.capture(cli.cmd_execution, args)
        self.assertEqual(1, code)
        args.batch = args.limit = None
        code, text, error = self.capture(cli.cmd_execution, args)
        self.assertEqual(0, code, error)
        self.assertEqual("work", json.loads(text)["task_id"])
