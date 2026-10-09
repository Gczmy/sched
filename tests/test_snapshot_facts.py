"""Private synthetic DB only; no daemon, worker or source-state operation."""
import copy
import sqlite3
import time
from unittest import mock

from gsched import snapshot_facts as facts, state
from test_review_cli_state import TempStateCase


class SnapshotFactsTests(TempStateCase):
    def test_capture_is_passive_and_stable_without_query_order_authority(self):
        self.seed_batch(job_status="pending")
        with state.connect() as conn:
            before = conn.total_changes
            first = facts.database_facts(conn)
            self.assertEqual(first, facts.database_facts(conn))
            facts.assert_migration_only(conn, first)
            self.assertEqual(before, conn.total_changes)

    def test_unknown_receipt_append_cannot_be_lost_in_rollback(self):
        with state.connect() as conn:
            original = facts.database_facts(conn)
            conn.execute("INSERT INTO operation_requests(request_id,argv,status,created_at) VALUES ('unknown','{}','started',?)", (state.now(),))
            with self.assertRaises(facts.SnapshotConflict):
                facts.assert_migration_only(conn, original)

    def test_same_count_replaced_receipts_and_row_changes_are_rejected(self):
        with state.connect() as conn:
            conn.execute("INSERT INTO operation_requests(request_id,argv,status,created_at) VALUES ('one','{}','started',?)", (state.now(),))
            original = facts.database_facts(conn)
            conn.execute("UPDATE operation_requests SET request_id='two'")
            with self.assertRaises(facts.SnapshotConflict):
                facts.assert_migration_only(conn, original)
            conn.execute("UPDATE operation_requests SET request_id='one',status='done',code=0")
            with self.assertRaises(facts.SnapshotConflict):
                facts.assert_migration_only(conn, original)

    def test_new_arbitrary_empty_table_or_default_column_is_not_approved(self):
        with state.connect() as conn:
            original = facts.database_facts(conn)
            conn.execute("CREATE TABLE unaudited(value INTEGER DEFAULT 0)")
            with self.assertRaises(facts.SnapshotConflict):
                facts.assert_migration_only(conn, original)
            conn.execute("DROP TABLE unaudited")
            conn.execute("ALTER TABLE jobs ADD COLUMN unaudited INTEGER DEFAULT 0")
            with self.assertRaises(facts.SnapshotConflict):
                facts.assert_migration_only(conn, original)

    def test_missing_original_table_or_column_binding_is_rejected(self):
        with state.connect() as conn:
            original = facts.database_facts(conn)
            changed = copy.deepcopy(original)
            changed["tables"]["absent"] = changed["tables"]["jobs"]
            with self.assertRaises(facts.SnapshotConflict):
                facts.assert_migration_only(conn, changed)
            changed = copy.deepcopy(original)
            changed["tables"]["jobs"]["columns"].append("absent")
            with self.assertRaises(facts.SnapshotConflict):
                facts.assert_migration_only(conn, changed)

    def test_changed_instance_or_lower_schema_is_refused(self):
        with state.connect() as conn:
            original = facts.database_facts(conn)
            for field, value in (("instance_id", "0" * 32), ("database_schema", state.DB_SCHEMA_VERSION + 1)):
                changed = dict(original, **{field: value})
                with self.assertRaises(facts.SnapshotConflict):
                    facts.assert_migration_only(conn, changed)

    def test_row_bytes_total_and_time_bounds_fail_without_partial_success(self):
        with state.connect() as conn:
            for target, value in (("MAX_ROWS", 0), ("MAX_ROW_BYTES", 0), ("MAX_TOTAL_BYTES", 0)):
                with mock.patch.object(facts, target, value), self.assertRaises(facts.SnapshotConflict):
                    facts.database_facts(conn)
            with self.assertRaises(facts.SnapshotConflict):
                facts.database_facts(conn, deadline=time.monotonic() - 1)

    def test_order_and_duplicate_multiplicity_blob_and_numeric_types(self):
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        conn.execute('CREATE TABLE "strange table" (value)')
        conn.executemany('INSERT INTO "strange table" VALUES (?)', [(1,), (1.0,), ("1",), (b"1",), (None,), (1,)])
        original = facts._rows_digest(conn, "strange table", ["value"], [0, 0], None)
        self.assertEqual(6, original["rows"])
        conn.execute('DELETE FROM "strange table" WHERE rowid=1')
        changed = facts._rows_digest(conn, "strange table", ["value"], [0, 0], None)
        self.assertNotEqual(original, changed)

    def test_unsupported_nonfinite_value_is_not_silently_hashed_as_null(self):
        with self.assertRaises(facts.SnapshotConflict):
            facts._scalar(float("inf"))

    def test_capture_rejects_incomplete_database_without_migration(self):
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        with self.assertRaises(state.StateError):
            facts.database_facts(conn)
