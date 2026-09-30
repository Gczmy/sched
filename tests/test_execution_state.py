"""Durable intent, recovery and legacy-history regressions in private fixtures."""
from __future__ import annotations

import argparse
import json
import sqlite3
import os
from unittest import mock

from gsched import cli, execution_state, state
from gsched.dispatcher import Dispatcher
from test_review_cli_state import TempStateCase


class ExecutionStateTests(TempStateCase):
    def test_submit_json_distinguishes_database_commit_and_gateway_delivery(self):
        batch = os.path.join(self.tmp.name, "manifest.json")
        with open(batch, "w", encoding="utf-8") as stream:
            json.dump({"name": "public-submit", "project": "p", "tasks": [
                {"id": "task", "cmd": ["/bin/true"], "git": False, "resources": {"gpu": 0}}]}, stream)
        args = argparse.Namespace(batch=batch, dry_run=False, json=True)
        with mock.patch.object(cli, "_ensure_running_locked", return_value="running"):
            code, output, error = self.capture(cli.cmd_submit, args)
        self.assertEqual(0, code, error)
        committed = json.loads(output)
        self.assertTrue(committed["persisted"])
        self.assertEqual("database", committed["delivery"])
        with state.connect() as conn:
            self.assertIsNotNone(state.get_batch(conn, committed["batch_id"]))
        with mock.patch.dict(os.environ, {"SCHED_ALLOW_FOREIGN_WRITE": ""}), mock.patch.object(
            cli, "_daemon_health", return_value={}
        ), mock.patch.object(state, "connect", side_effect=AssertionError("gateway used writer DB")):
            code, output, error = self.capture(cli.cmd_submit, args)
        self.assertEqual(0, code, error)
        delivered = json.loads(output)
        self.assertFalse(delivered["persisted"])
        self.assertEqual("inbox", delivered["delivery"])
        self.assertNotEqual(committed["batch_id"], delivered["batch_id"])

    def reserve(self):
        job_id = self.seed_batch(job_status="running")
        binding = {"backend_id": "generic", "backend_config_sha256": "1" * 64}
        with state.connect() as conn:
            identity = execution_state.reserve(conn, job_id, binding, {"sha256": "2" * 64}, "review-node", 123)
        return job_id, identity

    def test_launch_intent_is_once_and_identity_cannot_be_rewritten(self):
        job_id, identity = self.reserve()
        with state.connect() as conn:
            execution_state.launch_intent(conn, job_id)
            with self.assertRaises(state.StateError):
                execution_state.launch_intent(conn, job_id)
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("UPDATE execution_attempts SET launch_intent_at=NULL WHERE job_id=?", (job_id,))
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("UPDATE execution_attempts SET identity='{}' WHERE job_id=?", (job_id,))
            self.assertEqual(identity, json.loads(execution_state.get(conn, job_id)["identity"]))

    def test_pending_cancel_blocks_birth_but_processed_cancel_does_not(self):
        job_id, _ = self.reserve()
        with state.connect() as conn:
            conn.execute("INSERT INTO control_requests(job_id,created_at) VALUES(?,?)", (job_id, state.now()))
            with self.assertRaises(state.StateError):
                execution_state.launch_intent(conn, job_id)
            conn.execute("UPDATE control_requests SET status='done' WHERE job_id=?", (job_id,))
            execution_state.launch_intent(conn, job_id)

    def test_terminal_wait_and_cleanup_facts_cannot_be_rewritten(self):
        job_id, _ = self.reserve()
        terminal = {"status": "exited", "pid": 1234, "returncode": 0,
                    "rusage": {"ru_utime": .1}, "group_clean": True}
        with state.connect() as conn:
            execution_state.launch_intent(conn, job_id)
            for invalid in ({**terminal, "status": "success"},
                            {**terminal, "returncode": None},
                            {**terminal, "group_clean": False}):
                with self.assertRaises(state.StateError):
                    execution_state.observe(conn, job_id, invalid)
            execution_state.observe(conn, job_id, terminal)
            finished = execution_state.get(conn, job_id)["finished_at"]
            execution_state.observe(conn, job_id, terminal)
            self.assertEqual(finished, execution_state.get(conn, job_id)["finished_at"])
            with self.assertRaises(state.StateError):
                execution_state.observe(conn, job_id, {**terminal, "returncode": 1})
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("UPDATE execution_attempts SET phase='running' WHERE job_id=?", (job_id,))

    def test_clean_cannot_requeue_a_consumed_attempt_or_legacy_v2_binding(self):
        job_id, identity = self.reserve()
        with state.connect() as conn:
            state.update_job(conn, job_id, status="skip")
            conn.execute("UPDATE batches SET status='done' WHERE id=?", (identity["batch_id"],))
        args = argparse.Namespace(batch=identity["batch_id"], yes=True)
        code, _, error = self.capture(cli.cmd_clean, args)
        self.assertEqual(1, code, error)
        self.assertIn("attempt", error)
        with state.connect() as conn:
            self.assertEqual("skip", state.get_job(conn, job_id)["status"])
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=?", (identity["batch_id"],)).fetchone()["spec"])
            spec["_native_exec_contract_v2"] = {}
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id=?", (json.dumps(spec), identity["batch_id"]))
        for method, args in ((cli.cmd_clean, args),
                             (cli.cmd_retry, argparse.Namespace(task=identity["batch_id"]+":task"))):
            code, _, error = self.capture(method, args)
            self.assertEqual(1, code, error)
        dispatcher = Dispatcher.__new__(Dispatcher)
        with state.connect() as conn:
            self.assertTrue(dispatcher._job_uses_native_exec(conn, state.get_job(conn, job_id)))

    def test_execution_query_batch_disappearing_after_resolution_is_an_error(self):
        with mock.patch.object(cli, "_resolve_task_ref", return_value=("missing", "task")):
            code, _, error = self.capture(cli.cmd_execution, argparse.Namespace(task="missing:task", version=None, json=True))
        self.assertEqual(1, code, error)
        self.assertIn("不存在", error)

    def query(self, *, version=None):
        code, output, error = self.capture(cli.cmd_execution,
            argparse.Namespace(task="batch-20260829-000000:task", version=version, json=True))
        self.assertEqual(0, code, error)
        return json.loads(output)

    def test_query_records_cancel_wait_and_cleanup_without_changing_raw_attempt(self):
        job_id, _ = self.reserve()
        observation = {"status": "exited", "pid": 1234, "returncode": -15,
                       "rusage": {"ru_utime": .1}, "group_clean": True}
        with state.connect() as conn:
            execution_state.launch_intent(conn, job_id)
            execution_state.cancel_intent(conn, job_id, "cancelled")
            execution_state.observe(conn, job_id, observation)
            before = execution_state.public(execution_state.get(conn, job_id))
        result = self.query()
        self.assertEqual([before], result["attempts"])
        diagnostic = result["diagnostics"][0]
        self.assertEqual("exited", diagnostic["phase"])
        self.assertEqual("cancelled", diagnostic["cancel_reason"])
        self.assertTrue(diagnostic["wait_result_available"])
        self.assertEqual(-15, diagnostic["returncode"])
        self.assertEqual(observation["rusage"], diagnostic["rusage"])
        self.assertEqual("confirmed", diagnostic["cleanup_state"])
        self.assertTrue(diagnostic["replay_blocked"])
        self.assertIsNone(diagnostic["uncertainty_reason"])

    def test_query_distinguishes_wait_from_pending_cleanup_and_lost_authority(self):
        job_id, _ = self.reserve()
        with state.connect() as conn:
            execution_state.launch_intent(conn, job_id)
            execution_state.observe(conn, job_id, {"status": "cleanup_pending", "pid": 1234,
                "returncode": 0, "rusage": {"ru_stime": .2}, "group_clean": False})
        diagnostic = self.query()["diagnostics"][0]
        self.assertTrue(diagnostic["wait_result_available"])
        self.assertEqual("pending", diagnostic["cleanup_state"])
        with state.connect() as conn:
            # A vanished group and a job rc are not the lost original wait.
            execution_state.observe(conn, job_id, {"status": "authority_lost", "pid": 1234,
                "returncode": None, "rusage": None, "group_clean": True})
            state.update_job(conn, job_id, status="interrupted", rc=0)
        diagnostic = self.query()["diagnostics"][0]
        self.assertEqual("owner_authority_lost", diagnostic["uncertainty_reason"])
        self.assertEqual("confirmed", diagnostic["cleanup_state"])
        self.assertFalse(diagnostic["wait_result_available"])
        self.assertIsNone(diagnostic["returncode"])

    def test_query_selects_version_and_does_not_invent_subprocess_observations(self):
        job_id = self.seed_batch(job_status="done")
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks").fetchone()["spec"])
            spec["execution"] = {"backend": "generic"}
            state.insert_task(conn, "batch-20260829-000000", "task", 2, spec, 0, "p")
            state.insert_job(conn, "second-job", "batch-20260829-000000", "task", 2, "fp2", None, "p")
        result = self.query()
        self.assertEqual([1, 2], [d["job_version"] for d in result["diagnostics"]])
        ordinary, pending = result["diagnostics"]
        self.assertEqual(job_id, ordinary["job_id"])
        self.assertEqual("subprocess", ordinary["execution_kind"])
        self.assertFalse(ordinary["wait_result_available"])
        self.assertIsNone(ordinary["returncode"])
        self.assertEqual("unknown", ordinary["cleanup_state"])
        self.assertEqual("not_reserved", pending["phase"])
        self.assertFalse(pending["replay_blocked"])
        self.assertEqual([pending], self.query(version=2)["diagnostics"])

    def test_consumed_attempt_cannot_be_reset_by_manual_retry(self):
        job_id, identity = self.reserve()
        with state.connect() as conn:
            execution_state.launch_intent(conn, job_id)
            state.update_job(conn, job_id, status="failed")
        result, _, error = self.capture(cli.cmd_retry, argparse.Namespace(task="batch-20260829-000000:task"))
        self.assertEqual(1, result, error)
        self.assertIn("attempt", error)
        with state.connect() as conn:
            self.assertEqual("failed", state.get_job(conn, job_id)["status"])
            self.assertEqual(identity["attempt_id"], execution_state.get(conn, job_id)["attempt_id"])

    def test_crash_before_launch_intent_is_not_started_and_never_replayed(self):
        job_id, identity = self.reserve()
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.executor = mock.Mock()
        dispatcher.executor.configured_owner.return_value = None
        dispatcher._release_gpu_for_job = mock.Mock()
        with state.connect() as conn:
            dispatcher._reap_configured_executions(conn)
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            attempt = execution_state.get(conn, job_id)
            self.assertEqual("interrupted", job["status"])
            self.assertEqual("execution_not_started", job["failure"])
            self.assertEqual("not_started", attempt["phase"])
            self.assertEqual(identity["attempt_id"], attempt["attempt_id"])
            self.assertIsNone(attempt["launch_intent_at"])
        diagnostic = self.query()["diagnostics"][0]
        self.assertEqual("not_started", diagnostic["phase"])
        self.assertFalse(diagnostic["wait_result_available"])
        self.assertEqual("confirmed", diagnostic["cleanup_state"])
        dispatcher._release_gpu_for_job.assert_called_once()


class LegacyExecutionMigrationTests(TempStateCase):
    def legacy_schema(self, version):
        with state.connect() as conn:
            conn.execute("DROP TRIGGER execution_attempt_identity_immutable")
            conn.execute("DROP TABLE execution_attempts")
            if version < 4:
                conn.execute("DROP TRIGGER native_session_monitor_launch_immutable")
            if version == 1:
                conn.execute("DROP TABLE native_sessions")
            elif version < 4:
                conn.execute("ALTER TABLE native_sessions DROP COLUMN monitor_launch_attempted_at")
                if version == 2:
                    conn.execute("ALTER TABLE native_sessions DROP COLUMN log_attempted_at")
            conn.execute(f"PRAGMA user_version={version}")
            self.assertTrue(state._schema_is_complete(conn, version))

    def test_v1_to_v4_history_queries_do_not_migrate_or_create_attempts(self):
        for version in (1, 2, 3, 4):
            with self.subTest(version=version):
                state.init_db()
                batch = f"legacy-{version}"
                job_id = self.seed_batch(batch_id=batch, name=batch, batch_status="done", job_status="done")
                self.legacy_schema(version)
                state.set_query_only(True)
                try:
                    with mock.patch.object(state, "_initialize_database", side_effect=AssertionError("query migrated history")):
                        result, output, error = self.capture(cli.cmd_execution,
                            argparse.Namespace(task=f"{batch}:task", version=None))
                        self.assertEqual(0, result, error)
                        self.assertEqual([], json.loads(output)["attempts"])
                        self.assertEqual("subprocess", json.loads(output)["diagnostics"][0]["execution_kind"])
                        with state.connect() as conn:
                            self.assertEqual("done", state.get_job(conn, job_id)["status"])
                            self.assertEqual(version, conn.execute("PRAGMA user_version").fetchone()[0])
                finally:
                    state.set_query_only(False)
                state.init_db()
                with state.connect() as conn:
                    self.assertEqual(state.DB_SCHEMA_VERSION, conn.execute("PRAGMA user_version").fetchone()[0])
                    self.assertEqual("done", state.get_job(conn, job_id)["status"])

    def test_v4_session_is_preserved_and_new_legacy_launch_is_rejected(self):
        job_id = self.seed_batch()
        with state.connect() as conn:
            conn.execute("INSERT INTO native_sessions(session_id,job_id,job_version,evaluation_domain,owner_kind,"
                         "profile_id,profile_sha256,project_root_path,project_root_identity_sha256,log_relative_path,phase,created_at)"
                         " VALUES(?,?,1,'isolated_integration','unbound','old',?,?,?,'logs/old.log','reserved',?)",
                         ("a" * 32, job_id, "b" * 64, self.tmp.name, "c" * 64, state.now()))
            before = dict(state.get_native_session(conn, "a" * 32))
        self.legacy_schema(4)
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(before, dict(state.get_native_session(conn, "a" * 32)))
            with self.assertRaisesRegex(state.StateError, "retired"):
                state.mark_native_monitor_launch_attempted(conn, "a" * 32, 1, 2)

    def test_legacy_v2_v3_v4_queries_do_not_migrate_or_infer_wait_from_job_rc(self):
        for version in (2, 3, 4):
            with self.subTest(version=version):
                state.init_db()
                batch = f"old-session-{version}"
                job_id = self.seed_batch(batch_id=batch, name=batch, job_status="done")
                session_id = f"{version:032x}"
                with state.connect() as conn:
                    conn.execute("INSERT INTO native_sessions(session_id,job_id,job_version,evaluation_domain,"
                        "owner_kind,profile_id,profile_sha256,project_root_path,project_root_identity_sha256,"
                        "log_relative_path,phase,created_at) VALUES(?,?,1,'isolated_integration','unbound',"
                        "'old',?,?,?,'logs/old.log','reserved',?)",
                        (session_id, job_id, "b" * 64, self.tmp.name, f"{version:064x}", state.now()))
                self.legacy_schema(version)
                state.set_query_only(True)
                try:
                    with mock.patch.object(state, "_initialize_database", side_effect=AssertionError("migrated")):
                        code, output, error = self.capture(cli.cmd_execution,
                            argparse.Namespace(task=f"{batch}:task", version=1, json=True))
                    self.assertEqual(0, code, error)
                    result = json.loads(output)
                    self.assertEqual([], result["attempts"])
                    self.assertNotIn("project_root_path", result["legacy_sessions"][0])
                    self.assertIsNone(result["legacy_sessions"][0]["monitor_launch_attempted_at"])
                    diagnostic = result["diagnostics"][0]
                    self.assertEqual(session_id, diagnostic["legacy_session_id"])
                    self.assertEqual("legacy", diagnostic["execution_kind"])
                    self.assertEqual("legacy_wait_unavailable", diagnostic["uncertainty_reason"])
                    self.assertTrue(diagnostic["replay_blocked"])
                    self.assertFalse(diagnostic["wait_result_available"])
                    self.assertIsNone(diagnostic["returncode"])
                    self.assertEqual("unknown", diagnostic["cleanup_state"])
                    with state.connect() as conn:
                        self.assertEqual(version, conn.execute("PRAGMA user_version").fetchone()[0])
                        self.assertEqual("done", state.get_job(conn, job_id)["status"])
                finally:
                    state.set_query_only(False)

    def test_strict_history_without_session_stays_retired(self):
        job_id = self.seed_batch(job_status="done")
        with state.connect() as conn:
            conn.execute("UPDATE batches SET mode='strict'")
        code, output, error = self.capture(cli.cmd_execution,
            argparse.Namespace(task="batch:task", version=None, json=True))
        self.assertEqual(0, code, error)
        diagnostic = json.loads(output)["diagnostics"][0]
        self.assertEqual(job_id, diagnostic["job_id"])
        self.assertEqual("retired", diagnostic["phase"])
        self.assertEqual("legacy_wait_unavailable", diagnostic["uncertainty_reason"])
        self.assertTrue(diagnostic["replay_blocked"])
