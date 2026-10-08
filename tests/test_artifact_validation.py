"""Immutable artifact evidence on private fixtures; no workers or daemon."""
import copy
import json
import os
import sqlite3
import unittest
from unittest import mock

from gsched import artifact_validation as validation, artifacts, cli, state
from gsched.dispatcher import Dispatcher
from gsched.execution_policy import digest
from test_review_cli_state import TempStateCase


class ArtifactValidationTests(TempStateCase):
    batch = "batch-20260829-000000"

    def prepared(self, *, version=1, task_id="task", rc=0):
        job = self.seed_batch(job_status="running", version=version, task_id=task_id)
        with state.connect() as conn:
            state.update_job(conn, job, pgid=123, rc=rc)
        return job

    def append(self, job, *, checks=None, ordinary_wait=None, context="exit_zero"):
        with state.connect() as conn:
            row = state.get_job(conn, job)
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
                                          (row["batch_id"], row["task_id"], row["version"])).fetchone()[0])
            return validation.record_initial(conn, row, spec, context,
                                             checks if checks is not None else [], rc=row["rc"],
                                             ordinary_wait=ordinary_wait)

    @staticmethod
    def ordinary(**patch):
        return {"source": "local_supervisor_wait", "subject": "scheduler_supervisor_command_chain",
                "returncode": 0, "pid": 123, "start_token": "proc:456", "group_clean": True,
                "binding_verified": True, **patch}

    def query(self, *args, code=0):
        result, out, error = self.capture(cli.main, ["artifact-validations", f"{self.batch}:task", *args, "--json"])
        self.assertEqual(code, result, error)
        return json.loads(out) if code == 0 else error

    def test_initial_evidence_binds_exact_subject_rules_and_hashes(self):
        job = self.prepared()
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=?", (self.batch,)).fetchone()[0])
            spec.update(env={"SECRET": "not disclosed"}, artifacts={"r": {"path": "r", "check": "json"}},
                        stages=[{"cmd": ["/bin/true"], "artifacts": {"s": {"path": "s"}}}])
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id=?", (json.dumps(spec), self.batch))
        checks = [{"scope": "task", "passed": False, "reason_code": "invalid_json", "sha256": "a" * 64},
                  {"scope": "stage:0", "passed": True, "reason_code": "passed", "sha256": None}]
        item = self.append(job, checks=checks, ordinary_wait=self.ordinary())
        self.assertFalse(item["passed"])
        self.assertTrue(item["wait_verified"])
        self.assertEqual(digest(spec), item["spec_sha256"])
        self.assertEqual(digest(checks), item["evidence_sha256"])
        self.assertEqual(digest(item["payload"]), item["validation_id"])
        self.assertEqual(["task", "stage:0"], [r["scope"] for r in item["payload"]["rules"]])
        self.assertEqual("scheduler_supervisor_command_chain", item["payload"]["wait"]["subject"])
        self.assertNotIn("not disclosed", json.dumps(item))
        self.assertEqual(1, item["job_version"])

    def test_update_and_delete_cannot_rewrite_failure(self):
        job = self.prepared()
        item = self.append(job, checks=[{"passed": False}])
        for sql in ("UPDATE artifact_validations SET passed=1", "DELETE FROM artifact_validations"):
            with state.connect() as conn, self.assertRaises(sqlite3.IntegrityError):
                conn.execute(sql)
        self.assertEqual(item, self.append(job, checks=[{"passed": True}]))

    def test_repeated_completion_does_not_inspect_changed_files(self):
        job = self.prepared()
        self.append(job, checks=[{"passed": False, "reason_code": "missing_file"}])
        dispatcher = Dispatcher.__new__(Dispatcher)
        with state.connect() as conn:
            row = state.get_job(conn, job)
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=?", (self.batch,)).fetchone()[0])
            with mock.patch("gsched.dispatcher.inspect_declared_artifacts", side_effect=AssertionError("reinspection")):
                self.assertFalse(dispatcher._completion_artifacts_valid(row, spec, "exit_zero", conn=conn, rc=0))

    def test_changed_spec_or_rc_cannot_rebind_completion(self):
        job = self.prepared()
        self.append(job)
        with state.connect() as conn:
            row = state.get_job(conn, job)
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=?", (self.batch,)).fetchone()[0])
            with self.assertRaises(state.StateError):
                validation.record_initial(conn, row, dict(spec, cmd=["different"]), "exit_zero", [], rc=0)
            with self.assertRaises(state.StateError):
                validation.record_initial(conn, row, spec, "exit_zero", [], rc=1)
            conn.execute("UPDATE tasks SET spec=? WHERE batch_id=?", (json.dumps(dict(spec, cmd=["changed"])), self.batch))
        with self.assertRaises(state.StateError):
            self.append(job)

    def test_new_retry_preserves_first_failure_with_same_version(self):
        job = self.prepared()
        first = self.append(job, checks=[{"passed": False}])
        with state.connect() as conn:
            state.update_job(conn, job, retries=1, started_at="2026-08-29 10:01:00")
        second = self.append(job, checks=[{"passed": True}])
        self.assertNotEqual(first["validation_id"], second["validation_id"])
        self.assertEqual(1, second["job_version"])
        values = self.query()["validations"]
        self.assertEqual(2, len(values))
        self.assertEqual({False, True}, {v["passed"] for v in values})

    def test_legacy_rc_and_current_files_never_manufacture_original_wait(self):
        item = self.append(self.prepared())
        self.assertTrue(item["passed"])
        self.assertFalse(item["wait_verified"])
        self.assertEqual("original_wait_unavailable", item["payload"]["wait"]["reason"])

    def test_ordinary_wait_requires_exact_pid_strong_identity_cleanup_and_rc(self):
        job = self.prepared()
        with state.connect() as conn:
            row = state.get_job(conn, job)
            self.assertTrue(validation.wait_snapshot(conn, row, 0, self.ordinary())["verified"])
            for patch in ({"pid": 456}, {"pid": True}, {"start_token": None}, {"start_token": "weak"},
                          {"group_clean": False}, {"binding_verified": False}, {"returncode": 1},
                          {"returncode": False}, {"subject": "scientific_worker"}):
                with self.subTest(patch=patch):
                    self.assertFalse(validation.wait_snapshot(conn, row, 0, self.ordinary(**patch))["verified"])
            self.assertFalse(validation.wait_snapshot(conn, row, False, self.ordinary())["verified"])

    def test_backend_terminal_wait_is_bound_and_never_replaced_by_ordinary_rc(self):
        job = self.prepared()
        observation = {"status": "exited", "pid": 123, "returncode": 0, "group_clean": True, "rusage": {"utime": 1}}
        with state.connect() as conn:
            conn.execute("INSERT INTO execution_attempts"
                         " (attempt_id,job_id,job_version,backend_id,backend_config_sha256,phase,identity,observation,created_at)"
                         " VALUES (?,?,?,?,?,?,?,?,?)", ("attempt", job, 1, "example", "a" * 64, "exited",
                                                        '{"immutable":"identity"}', json.dumps(observation), state.now()))
            row = state.get_job(conn, job)
            fact = validation.wait_snapshot(conn, row, 0)
            self.assertTrue(fact["verified"])
            self.assertEqual(observation, fact["observation"])
            for patch in ({"pid": 456}, {"pid": True}, {"group_clean": False}, {"returncode": 1}, {"status": "unknown"}):
                # A pure reader fixture; no rewrite of the immutable production row.
                fake = dict(conn.execute("SELECT * FROM execution_attempts WHERE job_id=?", (job,)).fetchone())
                fake["observation"] = json.dumps({**observation, **patch})
                reader = mock.Mock()
                reader.execute.return_value.fetchone.return_value = fake
                self.assertFalse(validation.wait_snapshot(reader, row, 0, self.ordinary())["verified"])
            for key, value in (("phase", "unresolved"), ("job_version", 2)):
                fake = dict(conn.execute("SELECT * FROM execution_attempts WHERE job_id=?", (job,)).fetchone())
                fake[key] = value
                reader.execute.return_value.fetchone.return_value = fake
                self.assertFalse(validation.wait_snapshot(reader, row, 0)["verified"])

    def test_nonzero_ready_probe_records_raw_exit_without_relabeling_it(self):
        job = self.prepared(rc=137)
        item = self.append(job, context="probe_ready", ordinary_wait=self.ordinary(returncode=137))
        self.assertTrue(item["wait_verified"])
        self.assertEqual(137, item["payload"]["recorded_rc"])
        self.assertEqual("probe_ready", item["context"])

    def test_nonzero_command_chain_is_preserved_even_when_artifacts_pass(self):
        job = self.prepared(rc=1)
        item = self.append(job, context="exit_nonzero", ordinary_wait=self.ordinary(returncode=1))
        self.assertTrue(item["passed"])
        self.assertTrue(item["wait_verified"])
        self.assertEqual(1, item["payload"]["recorded_rc"])
        with state.connect() as conn:
            self.assertEqual("running", state.get_job(conn, job)["status"])

    def test_rollback_retains_neither_record_nor_terminal_state(self):
        job = self.prepared()
        before = self.batch_revision()
        with self.assertRaisesRegex(RuntimeError, "after record"), state.connect() as conn:
            row = state.get_job(conn, job)
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=?", (self.batch,)).fetchone()[0])
            validation.record_initial(conn, row, spec, "exit_zero", [], rc=0)
            state.update_job(conn, job, status="done")
            raise RuntimeError("after record")
        with state.connect() as conn:
            self.assertEqual("running", state.get_job(conn, job)["status"])
            self.assertEqual(0, conn.execute("SELECT count(*) FROM artifact_validations").fetchone()[0])
        self.assertEqual(before, self.batch_revision())

    def test_record_size_is_bounded_without_truncating_history(self):
        job = self.prepared()
        with mock.patch.object(validation, "MAX_RECORD_BYTES", 100), self.assertRaises(state.StateError):
            self.append(job)
        self.assertEqual([], self.query()["validations"])

    def test_readonly_query_does_not_check_files_or_migrate(self):
        item = self.append(self.prepared())
        before = self.batch_revision()
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migration")), \
             mock.patch.object(artifacts, "inspect_declared_artifacts", side_effect=AssertionError("file read")), \
             mock.patch("gsched.cli._is_foreign_host", return_value=True):
            summary = self.query("--version", "1")
            full = self.query("--validation-id", item["validation_id"])
        self.assertFalse(summary["evidence_included"])
        self.assertNotIn("payload", summary["validations"][0])
        self.assertTrue(full["evidence_included"])
        self.assertEqual(item, full["validations"][0])
        self.assertFalse(full["settlement_authority"])
        self.assertFalse(full["historical_failure_reconstructed"])
        self.assertEqual(before, self.batch_revision())

    def test_live_bounded_pages_are_not_a_complete_snapshot(self):
        job = self.prepared()
        ids = set()
        for retry in range(3):
            with state.connect() as conn:
                state.update_job(conn, job, retries=retry)
            ids.add(self.append(job)["validation_id"])
        first = self.query("--limit", "2")
        self.assertTrue(first["truncated"])
        self.assertEqual("live_keyset_not_complete_snapshot", first["pagination"])
        second = self.query("--limit", "2", "--cursor", first["next_cursor"])
        self.assertFalse(second["truncated"])
        self.assertEqual(ids, {v["validation_id"] for v in first["validations"] + second["validations"]})

    def test_detail_cannot_select_other_task_or_version(self):
        first = self.append(self.prepared())
        with state.connect() as conn:
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=?", (self.batch,)).fetchone()[0])
            state.insert_task(conn, self.batch, "other", 1, dict(spec, id="other"), 1, "p")
            state.insert_job(conn, "other", self.batch, "other", 1, "fp", None, "p")
            state.update_job(conn, "other", status="running", rc=0)
            state.insert_task(conn, self.batch, "task", 2, spec, 0, "p")
            state.insert_job(conn, "version2", self.batch, "task", 2, "fp2", None, "p")
            state.update_job(conn, "version2", status="running", rc=0)
        other = self.append("other")
        self.append("version2")
        self.assertEqual(1, len(self.query("--version", "1")["validations"]))
        self.query("--validation-id", other["validation_id"], code=1)
        self.query("--version", "2", "--validation-id", first["validation_id"], code=1)

    def test_invalid_query_parameters_are_rejected(self):
        self.prepared()
        for args in (("--limit", "0"), ("--limit", "101"), ("--version", "0"),
                     ("--cursor", "bad"), ("--validation-id", "bad"),
                     ("--cursor", "a" * 64, "--validation-id", "b" * 64)):
            with self.subTest(args=args):
                self.query(*args, code=1)

    def test_full_evidence_rejects_tampered_header_or_payload(self):
        item = self.append(self.prepared())
        for key, value in (("spec_sha256", "b" * 64), ("job_version", 2), ("passed", 0),
                           ("payload", json.dumps({"schema_version": 1}))):
            tampered = copy.deepcopy(item)
            tampered["payload"] = json.dumps(item["payload"])
            tampered[key] = value
            with self.subTest(key=key), self.assertRaises(state.StateError):
                validation.decode(tampered)

    def legacy_schema(self):
        with state.connect() as conn:
            conn.execute("DROP TRIGGER artifact_validation_immutable")
            conn.execute("DROP TRIGGER artifact_validation_retained")
            conn.execute("DROP TABLE artifact_validations")
            conn.execute("PRAGMA user_version=11")

    def test_schema11_read_is_passive_and_migration_does_not_backfill(self):
        job = self.prepared()
        identity = self.capture(cli.main, ["identity", "--json"])[1]
        with state.connect() as conn:
            before = dict(state.get_job(conn, job))
        self.legacy_schema()
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migration")):
            value = self.query()
        self.assertFalse(value["available"])
        self.assertEqual("migration_required", value["reason"])
        self.assertEqual([], value["validations"])
        self.assertEqual(identity, self.capture(cli.main, ["identity", "--json"])[1])
        with state.connect() as conn:
            self.assertEqual(11, conn.execute("PRAGMA user_version").fetchone()[0])
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(state.DB_SCHEMA_VERSION, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertEqual(before, dict(state.get_job(conn, job)))
            self.assertEqual(0, conn.execute("SELECT count(*) FROM artifact_validations").fetchone()[0])

    def test_migration_failure_rolls_back_new_objects_and_version(self):
        self.prepared()
        self.legacy_schema()
        with mock.patch.object(validation, "SCHEMA", validation.SCHEMA + "\nSELECT missing_function();"), \
             self.assertRaises(sqlite3.OperationalError):
            state.init_db()
        with state.connect() as conn:
            self.assertEqual(11, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertFalse(validation.available(conn))

    def test_file_identity_is_diagnostic_not_a_large_file_content_hash(self):
        path = os.path.join(self.tmp.name, "result")
        with open(path, "w") as stream:
            stream.write("{}")
        with mock.patch("gsched.artifacts.os.read", side_effect=AssertionError("content read")):
            item = artifacts.inspect_artifact(path, {"min_bytes": 1})
        self.assertTrue(item["passed"])
        self.assertIsNone(item["sha256"])
        self.assertEqual({"device", "inode", "mtime_ns", "ctime_ns"}, set(item["file_identity"]))
        self.assertTrue(all(type(value) is str and value.isdigit() for value in item["file_identity"].values()))
        original = os.fstat
        seen = 0
        def changed(fd):
            nonlocal seen
            seen += 1
            if seen == 2:
                with open(path, "a") as stream:
                    stream.write(" ")
            return original(fd)
        with mock.patch("gsched.artifacts.os.fstat", side_effect=changed):
            self.assertEqual("file_changed", artifacts.inspect_artifact(path, {})["reason_code"])

    def test_stat_recheck_errors_are_preserved_as_diagnostics(self):
        path = os.path.join(self.tmp.name, "result")
        with open(path, "w") as stream:
            stream.write("{}")
        for rule in ({}, {"check": "json"}):
            with mock.patch("gsched.artifacts.os.fstat", side_effect=[os.stat(path), PermissionError(13, "denied")]):
                item = artifacts.inspect_artifact(path, rule)
            self.assertFalse(item["passed"])
            self.assertEqual("permission_denied", item["reason_code"])
            self.assertEqual(13, item["errno"])


if __name__ == "__main__":
    unittest.main()
