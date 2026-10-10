"""Private SQLite/filesystem fixtures only; no daemon/worker is launched."""
import json
import os
import sqlite3
import subprocess
from contextlib import closing
from unittest import mock

from gsched import cli, maintenance, snapshot, snapshot_facts, state
from test_review_cli_state import TempStateCase


def downgrade_to_ten():
    """Private synthetic complete schema 10, not an operation on deployed state."""
    with state.connect() as conn:
        for name, sql in conn.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger'").fetchall():
            if any(token in sql for token in ("failure_policy", "depends_on_exact", "allocation_id")):
                conn.execute("DROP TRIGGER " + snapshot_facts._quoted(name))
        for table in snapshot_facts.ADDITIVE_TABLES:
            conn.execute("DROP TABLE " + table)
        for table, column in snapshot_facts.ADDITIVE_COLUMNS:
            conn.execute("ALTER TABLE " + table + " DROP COLUMN " + column)
        conn.execute("PRAGMA user_version=10")
        if not state._schema_is_complete(conn, 10):
            raise AssertionError("fixture must be a complete legacy schema")


class SnapshotTests(TempStateCase):
    def create(self):
        return snapshot.create(writers_quiesced=True)["snapshot_id"]

    def test_io_timeout_invalid_before_opening_window(self):
        for value in (True, False, None, "300", 0, -1, 901, 1.5):
            with self.subTest(value=value), self.assertRaises(snapshot_facts.SnapshotConflict):
                snapshot.create(writers_quiesced=True, io_timeout_sec=value)
            self.assertFalse(snapshot.status()["maintenance_open"])

    def test_default_point_keeps_original_shape_and_timeout(self):
        identifier = self.create()
        manifest = snapshot._json(os.path.join(snapshot._point(identifier), "manifest.json"))
        self.assertNotIn("io_timeout_sec", manifest)
        self.assertNotIn("io_timeout_sec", snapshot.status())
        self.assertNotIn("io_timeout_sec", snapshot.verify(identifier))
        self.assertEqual(30, snapshot._record_timeout(manifest))
        snapshot.close(identifier)

    def test_slow_inventory_explicit_budget_survives_verify_migration_and_rollback(self):
        downgrade_to_ten()
        slow = os.path.join(state.host_dir(), "slow-original.log")
        snapshot._write(slow, b"original evidence")
        clock = [0]
        original_read = snapshot._read
        def delayed_read(path, **kwargs):
            result = original_read(path, **kwargs)
            if os.path.basename(path) == "slow-original.log":
                clock[0] += 31
            return result
        with mock.patch.object(snapshot.time, "monotonic", side_effect=lambda: clock[0]), \
                mock.patch.object(snapshot, "_read", side_effect=delayed_read):
            with self.assertRaisesRegex(snapshot_facts.SnapshotConflict, "time bound"):
                self.create()
            failed = snapshot.status()
            self.assertEqual("creating", failed["phase"])
            snapshot.close(failed["snapshot_id"])
            code, out, err = self.capture(cli.main, ["snapshot", "create", "--writers-quiesced",
                                          "--io-timeout-sec", "300", "--yes", "--json"])
            self.assertEqual(0, code, (out, err))
            created = json.loads(out)
            self.assertEqual(300, created["io_timeout_sec"])
            identifier = created["snapshot_id"]
            self.assertEqual(300, snapshot.status()["io_timeout_sec"])
            self.assertEqual(300, snapshot.verify(identifier)["io_timeout_sec"])
            snapshot.migrate(identifier)
            snapshot.rollback(identifier)
            snapshot.rollback(identifier)
            self.assertEqual(10, snapshot.verify(identifier)["database_schema"])
            self.assertEqual(b"original evidence", original_read(slow))
            snapshot.close(identifier)

    def test_timeout_tamper_never_expands_original_rollback_authority(self):
        identifier = snapshot.create(writers_quiesced=True, io_timeout_sec=300)["snapshot_id"]
        window = snapshot._json(maintenance.window_path())
        snapshot._publish(maintenance.window_path(), {**window, "io_timeout_sec": 900})
        with self.assertRaisesRegex(snapshot_facts.SnapshotConflict, "timeout binding"):
            snapshot.migrate(identifier)
        snapshot._publish(maintenance.window_path(), window)
        path = os.path.join(snapshot._point(identifier), "manifest.json")
        manifest = snapshot._json(path)
        snapshot._publish(path, {**manifest, "io_timeout_sec": 900})
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.rollback(identifier)
        snapshot._publish(path, {**manifest, "io_timeout_sec": True})
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.verify(identifier)
        snapshot.close(identifier)

    def test_schema10_migration_and_controlled_rollback_preserve_unknown_receipt(self):
        self.seed_batch(job_status="pending")
        with state.connect() as conn:
            conn.execute("INSERT INTO operation_requests(request_id,argv,status,created_at) VALUES ('unknown','{}','started',?)", (state.now(),))
        downgrade_to_ten()
        identifier = self.create()
        result = snapshot.verify(identifier)
        self.assertEqual(10, result["database_schema"])
        self.assertFalse(result["rollback_authorized"])
        snapshot.migrate(identifier)
        state.set_query_only(True)
        self.addCleanup(state.set_query_only, False)
        with state.connect() as conn:
            self.assertEqual(state.DB_SCHEMA_VERSION, conn.execute("PRAGMA user_version").fetchone()[0])
        snapshot.rollback(identifier)
        snapshot.rollback(identifier)
        with state.connect() as conn:
            self.assertEqual(10, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertEqual("started", conn.execute("SELECT status FROM operation_requests WHERE request_id='unknown'").fetchone()[0])
            self.assertEqual(result["instance_id"], snapshot_facts.database_facts(conn)["instance_id"])
        snapshot.close(identifier)
        with self.assertRaises(FileNotFoundError):
            snapshot.rollback(identifier)

    def test_gate_blocks_before_initialization_config_write_and_daemon_spawn(self):
        identifier = self.create()
        for args in (["config", "reload"], ["daemon", "start"], ["submit", "absent.json"], ["notify-ack", "absent"]):
            with mock.patch.object(state, "init_db", side_effect=AssertionError("initialized")):
                code, out, err = self.capture(cli.main, args)
                self.assertEqual(2, code, (args, out, err))
        with self.assertRaises(state.SubmissionBlocked):
            state.init_db()
        with self.assertRaises(state.SubmissionBlocked), state.connect():
            pass
        with self.assertRaises(state.SubmissionBlocked), state.submission_lock():
            pass
        with mock.patch("gsched.daemon.subprocess.Popen", side_effect=AssertionError("spawned")):
            from gsched import daemon
            with self.assertRaises(state.SubmissionBlocked):
                daemon.start()
        snapshot.close(identifier)
        state.init_db()

    def test_closed_old_point_is_verified_but_never_a_new_rollback_authority(self):
        identifier = self.create()
        snapshot.close(identifier)
        self.assertTrue(snapshot.verify(identifier)["verified"])
        newer = self.create()
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.rollback(identifier)
        snapshot.close(newer)

    def test_append_receipt_outside_cooperative_protocol_refuses_rollback(self):
        identifier = self.create()
        # Simulate an unaware old writer, not an allowed scheduler operation.
        with closing(sqlite3.connect(state.db_path())) as conn:
            conn.execute("INSERT INTO operation_requests(request_id,argv,status,created_at) VALUES ('late','{}','started',?)", (state.now(),))
            conn.commit()
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.rollback(identifier)
        self.assertEqual("open", snapshot.status()["phase"])

    def test_config_inbox_notification_or_file_drift_never_overwritten(self):
        identifier = self.create()
        with open(os.path.join(state.host_dir(), "new-inbox.json"), "w") as stream:
            stream.write("new fact")
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.rollback(identifier)
        snapshot.close(identifier)

    def test_private_image_tamper_and_path_traversal_fail(self):
        identifier = self.create()
        image = os.path.join(snapshot._point(identifier), "database.db")
        with open(image, "ab") as stream:
            stream.write(b"tamper")
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.verify(identifier)
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.verify("../escape")
        snapshot.close(identifier)

    def test_running_or_unresolved_start_refused_and_failed_create_remains_fenced(self):
        self.seed_batch(job_status="running")
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            self.create()
        self.assertEqual("creating", snapshot.status()["phase"])
        with self.assertRaises(state.SubmissionBlocked):
            state.init_db()
        identifier = snapshot.status()["snapshot_id"]
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.rollback(identifier)
        snapshot.close(identifier)

    def test_cli_confirmations_and_query_do_not_initialize(self):
        code, out, err = self.capture(cli.main, ["snapshot", "create", "--json"])
        self.assertEqual(1, code)
        self.assertFalse(os.path.exists(maintenance.window_path()))
        code, out, err = self.capture(cli.main, ["snapshot", "create", "--yes", "--json"])
        self.assertEqual(1, code)
        self.assertIn("writers-quiesced", json.loads(out)["error"])
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migrated")):
            code, out, err = self.capture(cli.main, ["snapshot", "status", "--json"])
            self.assertEqual(0, code)
        identifier = self.create()
        with mock.patch.object(state, "init_db", side_effect=AssertionError("migrated")):
            code, out, err = self.capture(cli.main, ["snapshot", "verify", identifier, "--json"])
            self.assertEqual(0, code)
        snapshot.close(identifier)

    def test_rollback_crash_after_each_rename_resumes_original_journal(self):
        identifier = self.create()
        real_rename = os.rename
        calls = []
        def crash(source, target):
            real_rename(source, target)
            calls.append((source, target))
            raise OSError("simulated process exit after rename")
        with mock.patch.object(snapshot.os, "rename", side_effect=crash):
            with self.assertRaises(OSError):
                snapshot.rollback(identifier)
        self.assertEqual("restoring", snapshot.status()["phase"])
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.close(identifier)
        # Each resume can fail after the next exact rename, without restarting
        # execution, deleting a record or changing the recovery point identity.
        for _ in range(4):
            with mock.patch.object(snapshot.os, "rename", side_effect=crash):
                try:
                    snapshot.rollback(identifier)
                except OSError:
                    continue
            break
        self.assertEqual("restored", snapshot.rollback(identifier)["phase"])
        self.assertGreaterEqual(len(calls), 2)
        snapshot.close(identifier)

    def test_source_replacement_after_intent_is_not_guessed_on_resume(self):
        identifier = self.create()
        with mock.patch.object(snapshot.os, "rename", side_effect=OSError("before rename")):
            with self.assertRaises(OSError):
                snapshot.rollback(identifier)
        with open(state.db_path(), "ab") as stream:
            stream.write(b"unaware writer")
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.rollback(identifier)

    def test_snapshot_limits_do_not_report_partial_success(self):
        with mock.patch.object(snapshot, "MAX_FILE_BYTES", 0):
            with self.assertRaises(snapshot_facts.SnapshotConflict):
                self.create()
        self.assertEqual("creating", snapshot.status()["phase"])
        snapshot.close(snapshot.status()["snapshot_id"])

    def test_no_implicit_old_writer_attestation(self):
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.create()
        self.assertFalse(snapshot.status()["maintenance_open"])

    def test_durable_close_cannot_revive_rollback_after_unlink_failure(self):
        identifier = self.create()
        real_unlink = os.unlink
        def interrupted(path, *args, **kwargs):
            if path == maintenance.window_path():
                raise OSError("close record durable; window unlink interrupted")
            return real_unlink(path, *args, **kwargs)
        with mock.patch.object(snapshot.os, "unlink", side_effect=interrupted):
            with self.assertRaises(OSError):
                snapshot.close(identifier)
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.rollback(identifier)
        snapshot.close(identifier)
        self.assertFalse(snapshot.status()["maintenance_open"])

    def test_missing_replacement_publication_resumes_durable_intent(self):
        identifier = self.create()
        real_replace = os.replace
        def interrupted(source, target):
            if target.endswith("replacement.db"):
                raise OSError("replacement publication interrupted")
            return real_replace(source, target)
        with mock.patch.object(snapshot.os, "replace", side_effect=interrupted):
            with self.assertRaises(OSError):
                snapshot.rollback(identifier)
        self.assertEqual("restoring", snapshot.status()["phase"])
        self.assertEqual("restored", snapshot.rollback(identifier)["phase"])
        snapshot.close(identifier)

    def test_added_empty_directory_and_snapshot_sidecar_are_not_ignored(self):
        identifier = self.create()
        os.mkdir(os.path.join(state.host_dir(), "new-directory"), 0o700)
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.rollback(identifier)
        image = os.path.join(snapshot._point(identifier), "database.db")
        with open(image + "-wal", "wb") as stream:
            stream.write(b"not self-contained")
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.verify(identifier)
        snapshot.close(identifier)

    def test_nested_writer_cannot_switch_instance_or_inherit_actor_in_new_pid(self):
        with maintenance.gate(), mock.patch.dict(os.environ, {"SCHED_STATE": self.state_root + "-other"}):
            with self.assertRaises(state.StateError), maintenance.gate():
                pass
        identifier = self.create()
        with maintenance.gate(exclusive=True), maintenance.actor():
            with mock.patch.object(maintenance.os, "getpid", return_value=os.getpid() + 1):
                with self.assertRaises(state.SubmissionBlocked):
                    maintenance._check()
        snapshot.close(identifier)

    def test_copy_timeout_keeps_creation_failed_and_fenced(self):
        with mock.patch.object(state.subprocess, "run", side_effect=subprocess.TimeoutExpired("copy", 30)):
            with self.assertRaisesRegex(state.StateError, "time bound"):
                self.create()
        self.assertEqual("creating", snapshot.status()["phase"])
        snapshot.close(snapshot.status()["snapshot_id"])

    def test_restored_database_sidecar_and_source_hot_journal_refused(self):
        identifier = self.create()
        snapshot.rollback(identifier)
        with open(state.db_path() + "-wal", "wb") as stream:
            stream.write(b"new WAL facts")
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            snapshot.rollback(identifier)
        snapshot.close(identifier)

    def test_source_rollback_journal_cannot_be_ignored_by_backup(self):
        with open(state.db_path() + "-journal", "wb") as stream:
            stream.write(b"indeterminate SQLite rollback")
        with self.assertRaises(snapshot_facts.SnapshotConflict):
            self.create()
        snapshot.close(snapshot.status()["snapshot_id"])
