"""Bounded history work, durable acknowledgement and recorded health fixtures."""
from __future__ import annotations

import json
import hashlib
from pathlib import Path
import time
from unittest import mock

from gsched import execution_state, state
from gsched.execution.persistent import OwnerUnavailable
from gsched.execution_policy import ExecutionPolicyError, digest, validate_backends
import test_persistent_owner as helpers
from test_review_cli_state import TempStateCase


class OwnerOperationsTests(TempStateCase):
    reserve = helpers.PersistentStateTests.reserve
    query = helpers.PersistentStateTests.query
    owner_binding = helpers.PersistentStateTests.owner_binding
    bound = helpers.PersistentStateTests.bound
    dispatcher = helpers.PersistentStateTests.dispatcher

    def terminal(self):
        job_id, identity, binding = self.bound()
        observation = {"status": "exited", "pid": 333, "returncode": 7,
                       "rusage": {"ru_utime": .1}, "group_clean": True}
        with state.connect() as conn:
            execution_state.observe(conn, job_id, observation)
            state.update_job(conn, job_id, status="failed", rc=7)
        return job_id, identity, binding, observation

    def test_acknowledgement_requires_committed_terminal_and_does_not_change_wait(self):
        job_id, _, _ = self.bound()
        with state.connect() as conn:
            with self.assertRaises(state.StateError):
                execution_state.owner_cleanup_result(conn, job_id, outcome="closed")
            observation = {"status": "not_started", "pid": None, "group_clean": True}
            execution_state.observe(conn, job_id, observation)
            state.update_job(conn, job_id, status="interrupted")
            with self.assertRaises(state.StateError):
                execution_state.owner_cleanup_result(conn, job_id, outcome="closed")
            conn.commit()
            before = dict(execution_state.get(conn, job_id))
            execution_state.owner_cleanup_result(conn, job_id, outcome="closed")
            self.assertEqual(before, dict(execution_state.get(conn, job_id)))
            self.assertEqual("acknowledged", execution_state.owner_health(conn, job_id)["cleanup_state"])

    def test_failed_cleanup_backs_off_survives_restart_and_never_replays(self):
        job_id, _, _, terminal = self.terminal()
        owner = mock.Mock()
        dispatcher = self.dispatcher(owner)
        dispatcher.executor.retire_configured_execution.side_effect = OwnerUnavailable()
        with state.connect() as conn:
            dispatcher._reap_configured_executions(conn)
            health = execution_state.owner_health(conn, job_id)
            self.assertEqual("pending", health["cleanup_state"])
            self.assertEqual("unreachable", health["connection_status"])
            self.assertEqual(1, health["cleanup_attempts"])
            self.assertEqual("owner_unreachable", health["cleanup_error"])
            dispatcher._reap_configured_executions(conn)
            self.assertEqual(1, dispatcher.executor.retire_configured_execution.call_count)
            self.assertEqual([], execution_state.due_owner_cleanup(conn, limit=8))
        replacement = self.dispatcher(owner)
        replacement.executor.retire_configured_execution.return_value = "owner_lost"
        with mock.patch("gsched.execution_state.time.time", return_value=health["retry_after"] + 1):
            with state.connect() as conn:
                replacement._reap_configured_executions(conn)
                self.assertEqual("acknowledged", execution_state.owner_health(conn, job_id)["cleanup_state"])
                self.assertEqual("owner_lost", execution_state.owner_health(conn, job_id)["acknowledgement"])
                self.assertEqual(terminal, json.loads(execution_state.get(conn, job_id)["observation"]))
                self.assertEqual(7, state.get_job(conn, job_id)["rc"])
        owner._launch.assert_not_called()

    def test_recorded_health_query_does_not_connect_or_expose_binding_secrets(self):
        job_id, _, binding, _ = self.terminal()
        with state.connect() as conn:
            execution_state.owner_observed(conn, job_id, "unreachable")
        with mock.patch("gsched.execution.persistent.PersistentOwner", side_effect=AssertionError("query connected")):
            result = self.query()
        health = result["attempts"][0]["owner_health"]
        self.assertEqual("recorded", health["source"])
        self.assertEqual("unreachable", health["connection_status"])
        self.assertIsNotNone(health["last_observed_at"])
        self.assertNotIn(binding["token"], json.dumps(result))
        self.assertNotIn(binding["endpoint"], json.dumps(result))

    def test_crash_after_close_before_metadata_commit_retries_only_acknowledgement(self):
        job_id, _, _, terminal = self.terminal()
        dispatcher = self.dispatcher(mock.Mock())
        save_result = execution_state.owner_cleanup_result

        def fail_after_save(conn, *args, **kwargs):
            save_result(conn, *args, **kwargs)
            raise state.StateError("private fixture metadata commit failed")

        with self.assertRaises(state.StateError):
            with state.connect() as conn, mock.patch.object(execution_state, "owner_cleanup_result", side_effect=fail_after_save):
                dispatcher._reap_configured_executions(conn)
        replacement = self.dispatcher(mock.Mock())
        replacement.executor.retire_configured_execution.return_value = "owner_lost"
        with state.connect() as conn:
            self.assertEqual("pending", execution_state.owner_health(conn, job_id)["cleanup_state"])
            replacement._reap_configured_executions(conn)
            self.assertEqual(terminal, json.loads(execution_state.get(conn, job_id)["observation"]))
            self.assertEqual(7, state.get_job(conn, job_id)["rc"])
            self.assertEqual("owner_lost", execution_state.owner_health(conn, job_id)["acknowledgement"])
        replacement.executor.restore_configured_owner.return_value._launch.assert_not_called()

    def test_v6_readonly_leaves_bindings_unchanged_and_writer_backfills_once(self):
        job_id, _, binding, _ = self.terminal()
        with state.connect() as conn:
            conn.execute("DROP TABLE execution_owner_operations")
            conn.execute("DROP INDEX idx_jobs_status")
            conn.execute("PRAGMA user_version=6")
            self.assertTrue(state._schema_is_complete(conn, 6))
        state.set_query_only(True)
        try:
            with mock.patch.object(state, "_initialize_database", side_effect=AssertionError("readonly migration")):
                result = self.query()
                self.assertEqual("unknown", result["attempts"][0]["owner_health"]["cleanup_state"])
                with state.connect() as conn:
                    self.assertEqual(6, conn.execute("PRAGMA user_version").fetchone()[0])
                    self.assertEqual(binding, execution_state.get_owner_binding(conn, job_id))
        finally:
            state.set_query_only(False)
        state.init_db()
        with state.connect() as conn:
            self.assertEqual("pending", execution_state.owner_health(conn, job_id)["cleanup_state"])
            self.assertEqual(binding, execution_state.get_owner_binding(conn, job_id))
        with mock.patch.object(execution_state, "migrate_owner_operations", side_effect=AssertionError("rescanned history")):
            state.init_db()

    def test_ten_thousand_acknowledged_records_do_not_expand_a_tick_or_cache(self):
        job_id, identity, binding, terminal = self.terminal()
        with state.connect() as conn:
            batch = identity["batch_id"]
            for i in range(10020):
                item = f"history-{i:05}"
                task_id = f"task-{i:05}"
                attempt_id = f"{i + 1:032x}"
                item_identity = {**identity, "job_id": item, "task_id": task_id, "attempt_id": attempt_id}
                item_binding = {**binding, "attempt_id": attempt_id}
                conn.execute("INSERT INTO jobs(id,batch_id,task_id,version,status) VALUES(?,?,?,1,'done')", (item, batch, task_id))
                conn.execute("INSERT INTO execution_attempts(attempt_id,job_id,job_version,backend_id,backend_config_sha256,phase,identity,observation,created_at)"
                    " VALUES(?,?,1,'generic',?,'exited',?,?,?)", (attempt_id, item, "1" * 64, json.dumps(item_identity), json.dumps(terminal), state.now()))
                conn.execute("INSERT INTO execution_owners(job_id,binding) VALUES(?,?)", (item, json.dumps(item_binding)))
                conn.execute("INSERT INTO execution_owner_operations(job_id,cleanup_state) VALUES(?,?)", (item, "acknowledged" if i < 10000 else "pending"))
            plan = conn.execute("EXPLAIN QUERY PLAN SELECT o.job_id FROM execution_owner_operations o JOIN jobs j ON j.id=o.job_id"
                " WHERE o.cleanup_state='pending' AND o.retry_after<=? AND j.status!='running' ORDER BY o.retry_after,o.job_id LIMIT 8", (time.time(),)).fetchall()
            self.assertTrue(any("execution_owner_cleanup_due" in row["detail"] for row in plan), plan)
            running_plan = conn.execute("EXPLAIN QUERY PLAN SELECT j.* FROM jobs j JOIN execution_attempts e ON e.job_id=j.id WHERE j.status='running'").fetchall()
            self.assertTrue(any("idx_jobs_status" in row["detail"] for row in running_plan), running_plan)
        dispatcher = self.dispatcher(mock.Mock())
        with state.connect() as conn:
            for expected in (8, 16, 21):
                dispatcher._reap_configured_executions(conn)
                self.assertEqual(expected, dispatcher.executor.retire_configured_execution.call_count)
            dispatcher._reap_configured_executions(conn)
            self.assertEqual(21, dispatcher.executor.retire_configured_execution.call_count)
            self.assertEqual([], execution_state.due_owner_cleanup(conn, limit=8))
            self.assertEqual(10021, conn.execute("SELECT COUNT(*) FROM execution_owners").fetchone()[0])
        self.assertFalse(hasattr(dispatcher, "_execution_acknowledged"))

    def test_slow_endpoints_yield_after_time_budget_without_dropping_queue(self):
        job_id, _, _, _ = self.terminal()
        dispatcher = self.dispatcher(mock.Mock())
        with state.connect() as conn, mock.patch("gsched.dispatcher.time.monotonic", side_effect=[0, 3]):
            dispatcher._reap_owner_cleanup(conn)
            dispatcher.executor.retire_configured_execution.assert_not_called()
            self.assertEqual("pending", execution_state.owner_health(conn, job_id)["cleanup_state"])


class OwnerConfigurationTests(TempStateCase):
    def profile(self):
        return {"kind": "linux_fd_owner", "executable": "/bin/true", "sha256": "1" * 64,
                "argv": ["true"], "env": {}, "projects": ["p"], "input_slots": {}}

    def test_owner_limits_reject_wrong_types_unknown_keys_and_wrong_backend(self):
        profile = self.profile()
        invalid = ({"prepare_timeout_sec": 0}, {"prepare_timeout_sec": 301},
                   {"prepare_timeout_sec": True}, {"terminal_retention_sec": 59},
                   {"terminal_retention_sec": 604801}, {"terminal_retention_sec": float("nan")},
                   {"terminal_retention_sec": float("inf")}, {"terminal_retention_sec": 10 ** 1000},
                   {"unknown": 60}, None)
        for setting in invalid:
            with self.subTest(setting=setting), self.assertRaises(ExecutionPolicyError):
                validate_backends({**self.cfg, "execution_backends": {"test": {**profile, "owner": setting}}})
        with self.assertRaises(ExecutionPolicyError):
            validate_backends({**self.cfg, "execution_backends": {"test": {**profile, "kind": "linux_fd", "owner": {}}}})

    def test_owner_settings_are_cold_and_old_default_profile_hash_stays_unchanged(self):
        from gsched.dispatcher import CONFIG_COLD_KEYS
        profile = self.profile()
        self.assertEqual(digest(profile), digest(validate_backends({**self.cfg, "execution_backends": {"test": profile}})["test"]))
        changed = {**profile, "owner": {"prepare_timeout_sec": 1, "terminal_retention_sec": 604800}}
        self.assertEqual(changed, validate_backends({**self.cfg, "execution_backends": {"test": changed}})["test"])
        self.assertNotEqual(digest(profile), digest(changed))
        self.assertIn("execution_backends", CONFIG_COLD_KEYS)

    def test_executor_passes_selected_retention_and_task_duration_to_original_owner(self):
        from gsched.executor import Executor
        executable = Path(self.tmp.name) / "worker"
        executable.write_bytes(b"private fixture executable")
        profile = {**self.profile(), "executable": str(executable),
                   "sha256": hashlib.sha256(executable.read_bytes()).hexdigest(),
                   "owner": {"prepare_timeout_sec": 2, "terminal_retention_sec": 120}}
        spec = {"cwd_abs": self.tmp.name, "duration_min": 3, "_execution_binding": {"inputs": {}}}
        with mock.patch("gsched.execution.persistent.PersistentLinuxFdBackend") as backend:
            Executor().prepare_configured_execution("private-job", spec, profile, {"attempt_id": "1" * 32}, None,
                                                   str(Path(self.tmp.name) / "logs" / "worker.log"))
        options = backend.return_value.prepare.call_args.kwargs
        self.assertEqual(2, options["prepare_timeout"])
        self.assertEqual(120, options["terminal_retention"])
        self.assertEqual(180, options["duration_seconds"])
