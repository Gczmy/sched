"""Synthetic allocation fixtures: no processes, GPU probes, or daemon."""
import json
import sqlite3
import time
import unittest
from unittest import mock

from gsched import allocation, artifact_validation, cli, execution_state, pending_cancel, state
from gsched.dispatcher import Dispatcher
from gsched.execution_policy import digest
from gsched.executor import Executor
from test_review_cli_state import TempStateCase


class AllocationTests(TempStateCase):
    batch = "batch-20260829-000000"

    def prepare(self, *, gpu=False):
        job = self.seed_batch(job_status="running")
        with state.connect() as conn:
            state.update_job(conn, job, pgid=None, rc=None)
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=?", (self.batch,)).fetchone()[0])
            spec["env"] = {"SECRET": "undisclosed-secret"}
            if gpu:
                spec["resources"].update(gpu=1, vram_gib=4, gpu_share=True)
                state.init_gpus(conn, [0])
                conn.execute("INSERT INTO gpu_jobs VALUES(0,?,4,?)", (job, state.now()))
                state.update_job(conn, job, gpu=0)
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id=?", (json.dumps(spec), self.batch))
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.fake = True
        dispatcher.allocator = mock.Mock(_uuid_map={"GPU-example": 0}, _uuid_observed_at=time.time())
        return job, spec, dispatcher

    def reserve(self, job, spec, dispatcher):
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            return allocation.reserve(conn, job, spec, dispatcher)

    def query(self, identifier=None, *args, code=0):
        command = ["allocations", f"{self.batch}:task", "--json", *args]
        if identifier:
            command.extend(["--allocation-id", identifier])
        rc, out, err = self.capture(cli.main, command)
        self.assertEqual(code, rc, err)
        return json.loads(out) if rc == 0 else err

    def test_immutable_intent_binds_instance_spec_and_reservations_not_worker(self):
        job, spec, dispatcher = self.prepare(gpu=True)
        identifier = self.reserve(job, spec, dispatcher)
        result = self.query(identifier)
        item = result["allocations"][0]
        self.assertEqual(digest(spec), item["spec_sha256"])
        self.assertEqual(1, item["cpu_reservation"])
        self.assertEqual(4, item["gpu_reservations"][0]["vram_gib"])
        self.assertEqual("GPU-example", item["gpu_reservations"][0]["gpu_uuid"])
        self.assertTrue(item["gpu_reservations"][0]["simulated"])
        self.assertIsNone(item["worker_identity"])
        self.assertFalse(item["hard_isolation"])
        self.assertFalse(result["settlement_authority"])
        self.assertNotIn("undisclosed-secret", json.dumps(result))
        with state.connect() as conn:
            for statement in ("UPDATE allocations SET ordinal=2", "DELETE FROM allocations",
                              "UPDATE allocation_events SET layer='process'", "DELETE FROM allocation_events"):
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(statement)

    def test_same_second_same_version_retry_creates_distinct_validation_identity(self):
        job, spec, dispatcher = self.prepare()
        first = self.reserve(job, spec, dispatcher)
        with state.connect() as conn:
            state.update_job(conn, job, pgid=123, rc=0)
            a = artifact_validation.record_initial(conn, state.get_job(conn, job), spec, "exit_zero", [{"passed": False}], rc=0)
            state.update_job(conn, job, pgid=None, rc=None)
        second = self.reserve(job, spec, dispatcher)
        with state.connect() as conn:
            state.update_job(conn, job, pgid=124, rc=0)
            b = artifact_validation.record_initial(conn, state.get_job(conn, job), spec, "exit_zero", [{"passed": True}], rc=0)
        self.assertNotEqual(first, second)
        self.assertNotEqual(a["validation_id"], b["validation_id"])
        self.assertEqual(a["payload"]["started_at"], b["payload"]["started_at"])
        self.assertEqual([a["validation_id"]], self.query(first)["allocations"][0]["artifact_validation_ids"])
        self.assertEqual([b["validation_id"]], self.query(second)["allocations"][0]["artifact_validation_ids"])

    def test_monitor_and_scheduler_rc_do_not_replace_original_supervisor_wait(self):
        job, spec, dispatcher = self.prepare()
        identifier = self.reserve(job, spec, dispatcher)
        with state.connect() as conn:
            state.update_job(conn, job, pgid=123, kill_reason="probe_ready", rc=0)
            allocation.ordinary_wait(conn, state.get_job(conn, job),
                {"source": "local_supervisor_wait", "subject": "scheduler_supervisor_command_chain", "pid": 123,
                 "start_token": "proc:42", "group_clean": True, "binding_verified": True, "returncode": -15}, 0)
            state.update_job(conn, job, status="done")
        events = self.query(identifier)["allocations"][0]["events"]
        monitor = next(e for e in events if e["layer"] == "monitor")
        self.assertFalse(monitor["data"]["wait_authority"])
        wait = next(e for e in events if e["layer"] == "process")["data"]
        self.assertEqual(-15, wait["wait"]["returncode"])
        self.assertEqual(0, wait["scheduler_recorded_rc"])
        self.assertTrue(wait["wait"]["verified"])

    def test_backend_child_observation_and_owner_service_are_distinct(self):
        job, spec, dispatcher = self.prepare()
        identifier = self.reserve(job, spec, dispatcher)
        with state.connect() as conn:
            identity = execution_state.reserve(conn, job, {"backend_id": "example", "backend_config_sha256": "a" * 64},
                                               {"sha256": "b" * 64}, "review-node", 42)
            owner = {"schema": "sched-execution-owner/v1", "owner_id": "c" * 32, "attempt_id": identity["attempt_id"],
                     "endpoint": "gsched-owner-" + "c" * 32, "token": "d" * 64, "pid": 99,
                     "start_ticks": 45, "boot_id": "0" * 8 + "-" + "0" * 4 + "-" + "0" * 4 + "-" + "0" * 4 + "-" + "0" * 12}
            execution_state.bind_owner(conn, job, owner)
            execution_state.launch_intent(conn, job)
            state.update_job(conn, job, pgid=123)
            execution_state.observe(conn, job, {"status": "exited", "pid": 123, "returncode": 7, "group_clean": True})
        result = self.query(identifier)
        events = result["allocations"][0]["events"]
        service = next(e for e in events if e["layer"] == "owner")["data"]
        child = next(e for e in events if e["layer"] == "process")["data"]
        self.assertEqual(99, service["binding"]["pid"])
        self.assertEqual(123, child["observation"]["pid"])
        self.assertEqual("execution_owner_service", service["subject"])
        self.assertEqual("execution_backend_direct_child", child["subject"])
        self.assertNotIn(owner["token"], json.dumps(result))
        self.assertNotIn(owner["endpoint"], json.dumps(result))

    def test_passive_query_cannot_probe_start_migrate_or_inspect_artifacts(self):
        job, spec, dispatcher = self.prepare()
        identifier = self.reserve(job, spec, dispatcher)
        with state.connect() as conn:
            before = tuple(state.get_batch(conn, self.batch))
        with mock.patch.object(Dispatcher, "__init__", side_effect=AssertionError("dispatcher")), \
             mock.patch("gsched.state.init_db", side_effect=AssertionError("migration")), \
             mock.patch("gsched.artifacts.inspect_declared_artifacts", side_effect=AssertionError("file")), \
             mock.patch("gsched.executor.Executor.launch", side_effect=AssertionError("launch")):
            result = self.query(identifier)
        self.assertEqual("none", result["effect"])
        with state.connect() as conn:
            self.assertEqual(before, tuple(state.get_batch(conn, self.batch)))

    def test_old_schema_query_does_not_backfill_allocation_or_wait(self):
        job, spec, dispatcher = self.prepare()
        with state.connect() as conn:
            before = tuple(state.get_batch(conn, self.batch))
            conn.execute("DROP TRIGGER revision_job_allocation")
            conn.execute("DROP TRIGGER allocation_clear_pending")
            conn.execute("DROP TABLE allocation_events")
            conn.execute("DROP TABLE allocations")
            conn.execute("ALTER TABLE jobs DROP COLUMN allocation_id")
            conn.execute("PRAGMA user_version=15")
        result = self.query()
        self.assertFalse(result["available"])
        self.assertEqual("migration_required", result["reason"])
        with state.connect() as conn:
            self.assertEqual(15, conn.execute("PRAGMA user_version").fetchone()[0])
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(state.DB_SCHEMA_VERSION, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertIsNone(state.get_job(conn, job)["allocation_id"])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM allocations").fetchone()[0])
            self.assertEqual(before, tuple(state.get_batch(conn, self.batch)))

    def test_stale_topology_is_unknown_not_a_current_uuid(self):
        job, spec, dispatcher = self.prepare(gpu=True)
        dispatcher.allocator._uuid_observed_at = time.time() - 20
        item = self.query(self.reserve(job, spec, dispatcher))["allocations"][0]
        self.assertIsNone(item["gpu_reservations"][0]["gpu_uuid"])
        self.assertEqual("unknown", item["gpu_reservations"][0]["topology_status"])

    def test_pagination_exact_binding_and_invalid_arguments(self):
        job, spec, dispatcher = self.prepare()
        identifiers = [self.reserve(job, spec, dispatcher) for _ in range(3)]
        first = self.query(None, "--limit", "1")
        self.assertTrue(first["truncated"])
        second = self.query(None, "--limit", "1", "--cursor", first["next_cursor"])
        self.assertNotEqual(first["allocations"][0]["allocation_id"], second["allocations"][0]["allocation_id"])
        self.query("f" * 32, code=1)
        self.query(identifiers[0], "--cursor", identifiers[1], code=1)
        self.query(None, "--version", "0", code=1)
        self.query(None, "--limit", "101", code=1)

    def test_kill_wait_is_retained_without_relabeling_legacy_poll_rc(self):
        import signal
        executor = Executor()
        proc = mock.Mock(returncode=-9, _sched_launch_start_token="proc:42")
        executor._procs[123] = proc
        with mock.patch("gsched.executor.os.killpg"):
            self.assertTrue(executor.kill_pgid(123, signal.SIGKILL))
        self.assertFalse(executor.has_process(123))
        self.assertEqual(137, executor.poll_rc(123))
        raw = executor.take_supervisor_wait(123, (123, "proc:42"))
        self.assertEqual(-9, raw["returncode"])
        self.assertNotIn("group_clean", raw)
        self.assertIsNone(executor.take_supervisor_wait(123, (123, "proc:42")))

    def test_retained_wait_cannot_match_reused_pid_or_survive_new_executor(self):
        executor = Executor()
        proc = mock.Mock(returncode=0, _sched_launch_start_token="proc:42")
        executor._retain_supervisor_wait(123, proc)
        self.assertIsNone(executor.take_supervisor_wait(123, (123, "proc:43")))
        executor._retain_supervisor_wait(123, proc)
        self.assertIsNone(Executor().take_supervisor_wait(123, (123, "proc:42")))
        self.assertEqual(0, executor.take_supervisor_wait(123, (123, "proc:42"))["returncode"])

    def test_retry_clears_current_pointer_but_history_prevents_never_started_claim(self):
        job, spec, dispatcher = self.prepare()
        identifier = self.reserve(job, spec, dispatcher)
        with state.connect() as conn:
            state.update_job(conn, job, status="blocked", pgid=123, rc=1)
            state.update_job(conn, job, status="pending", pgid=None, rc=None, started_at=None, retries=0, finished_at=None)
            self.assertIsNone(state.get_job(conn, job)["allocation_id"])
            fact = pending_cancel.facts(conn, state.get_batch(conn, self.batch), [{"task_id": "task", "version": 1}])[0]
            self.assertFalse(fact["recorded_never_started"])
            self.assertIn("prior_allocation_without_not_started_authority", fact["refusal_reasons"])
        self.assertEqual(identifier, self.query(identifier)["allocations"][0]["allocation_id"])

    def test_original_not_started_requires_exact_allocation_attempt_link(self):
        job, spec, dispatcher = self.prepare()
        identifier = self.reserve(job, spec, dispatcher)
        with state.connect() as conn:
            execution_state.reserve(conn, job, {"backend_id": "example", "backend_config_sha256": "a" * 64},
                                    {"sha256": "b" * 64}, "review-node", 42)
            execution_state.observe(conn, job, {"status": "not_started", "pid": None, "returncode": None, "group_clean": True})
            state.update_job(conn, job, status="pending", pgid=None, rc=None, started_at=None, retries=0, finished_at=None)
            fact = pending_cancel.facts(conn, state.get_batch(conn, self.batch), [{"task_id": "task", "version": 1}])[0]
            self.assertTrue(fact["recorded_never_started"], fact)
            # A second unrelated allocation cannot borrow the old not_started.
            state.update_job(conn, job, status="running")
            unrelated = allocation.reserve(conn, job, spec, dispatcher)
            state.update_job(conn, job, status="pending")
            fact = pending_cancel.facts(conn, state.get_batch(conn, self.batch), [{"task_id": "task", "version": 1}])[0]
            self.assertFalse(fact["recorded_never_started"])
            self.assertIn("allocation_not_bound_to_original_not_started:" + unrelated, fact["refusal_reasons"])
        self.assertNotEqual(identifier, unrelated)

    def test_failed_launch_transaction_cannot_leave_a_durable_allocation(self):
        job, spec, dispatcher = self.prepare()
        with self.assertRaisesRegex(RuntimeError, "injected"), state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            allocation.reserve(conn, job, spec, dispatcher)
            raise RuntimeError("injected")
        with state.connect() as conn:
            self.assertIsNone(state.get_job(conn, job)["allocation_id"])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM allocation_events").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM allocations").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
