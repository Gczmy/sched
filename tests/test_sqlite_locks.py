from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import unittest
from unittest import mock

if sys.platform == "linux":
    from test_review_cli_state import TempStateCase
else:
    TempStateCase = unittest.TestCase
from gsched import state


@unittest.skipUnless(sys.platform == "linux", "requires Linux POSIX locks")
class SQLiteLockRegression(TempStateCase):
    def locks(self, path):
        entry = os.stat(path)
        identity = f"{os.major(entry.st_dev):02x}:{os.minor(entry.st_dev):02x}:{entry.st_ino}"
        return {
            tuple(line.split()[1:])
            for line in Path("/proc/locks").read_text().splitlines()
            if identity in line.split() and str(os.getpid()) in line.split()
        }

    def assert_contender_blocked(self):
        code = """
import sqlite3,sys
conn=sqlite3.connect(sys.argv[1],timeout=0.1)
try:
    conn.execute('BEGIN IMMEDIATE')
except sqlite3.OperationalError as error:
    assert str(error) == 'database is locked', str(error)
else:
    raise AssertionError('contender acquired live writer lock')
finally:
    conn.close()
"""
        result = subprocess.run(
            [sys.executable, "-I", "-c", code, state.db_path()],
            capture_output=True, text=True, timeout=5,
        )
        self.assertEqual(0, result.returncode, result.stderr)

    def test_permission_repair_retains_db_and_shm_locks(self):
        with state.connect() as writer:
            writer.execute("BEGIN IMMEDIATE")
            writer.execute("INSERT INTO batches(id,name,created_at) VALUES('probe','probe','now')")
            paths = (state.db_path(), state.db_path() + "-shm")
            before = [self.locks(path) for path in paths]
            self.assertTrue(all(before), before)
            for path in (*paths, state.db_path() + "-wal"):
                os.chmod(path, 0o644)
                with mock.patch.object(state.os, "open", side_effect=AssertionError("ordinary open")):
                    state.ensure_private_sqlite_file(path)
                self.assertEqual(0o600, os.stat(path).st_mode & 0o777)
            self.assertEqual(before, [self.locks(path) for path in paths])
            self.assert_contender_blocked()
        with state.connect() as reader:
            self.assertEqual(1, reader.execute("SELECT count(*) FROM batches WHERE id='probe'").fetchone()[0])

    def test_second_connection_preserves_first_writer_locks(self):
        with state.connect() as writer:
            writer.execute("BEGIN IMMEDIATE")
            paths = (state.db_path(), state.db_path() + "-shm")
            before = [self.locks(path) for path in paths]
            with state.connect() as reader:
                reader.execute("SELECT count(*) FROM batches").fetchone()
            self.assertEqual(before, [self.locks(path) for path in paths])
            self.assert_contender_blocked()

    def test_private_snapshot_preserves_same_process_writer_locks(self):
        with state.connect() as writer:
            writer.execute("BEGIN IMMEDIATE")
            paths = (state.db_path(), state.db_path() + "-shm")
            before = [self.locks(path) for path in paths]
            state.set_query_only(True)
            try:
                with state.connect() as reader:
                    reader.execute("SELECT count(*) FROM batches").fetchone()
            finally:
                state.set_query_only(False)
            self.assertEqual(before, [self.locks(path) for path in paths])
            self.assert_contender_blocked()

    def test_sqlite_permissions_reject_symlink_and_fifo(self):
        target = os.path.join(self.tmp.name, "target")
        Path(target).write_text("unchanged")
        os.chmod(target, 0o644)
        link = os.path.join(self.tmp.name, "link")
        os.symlink(target, link)
        fifo = os.path.join(self.tmp.name, "fifo")
        os.mkfifo(fifo)
        for path in (link, fifo):
            with self.subTest(path=path), self.assertRaises(state.StateError):
                state.ensure_private_sqlite_file(path)
        self.assertEqual(0o644, os.stat(target).st_mode & 0o777)

    @unittest.skipUnless(sys.version_info >= (3, 11), "SQLite error codes require Python 3.11+")
    def test_real_busy_commit_retries_commit_without_replaying_body(self):
        body_count = 0
        with mock.patch.object(state.time, "sleep") as sleep:
            with state.connect() as writer:
                body_count += 1
                pending = writer.execute("INSERT INTO batches(id,name,created_at) VALUES('busy','busy','now') RETURNING id")
                sleep.side_effect = lambda delay: pending.close()
            self.assertEqual(1, body_count)
            sleep.assert_called_once_with(state._COMMIT_RETRY_DELAYS[0])
        with state.connect() as reader:
            self.assertEqual(1, reader.execute("SELECT count(*) FROM batches WHERE id='busy'").fetchone()[0])

    def test_body_failure_rolls_back_and_reports_phase(self):
        error = sqlite3.OperationalError("injected protocol")
        error.sqlite_errorcode = 15
        error.sqlite_errorname = "SQLITE_PROTOCOL"
        with self.assertLogs(state.__name__, level="ERROR") as logs:
            with self.assertRaises(sqlite3.OperationalError):
                with state.connect() as writer:
                    writer.execute("INSERT INTO batches(id,name,created_at) VALUES('failed','failed','now')")
                    raise error
        self.assertIn("phase=body", logs.output[0])
        self.assertIn("code=15", logs.output[0])
        with state.connect() as reader:
            self.assertEqual(0, reader.execute("SELECT count(*) FROM batches WHERE id='failed'").fetchone()[0])

    def test_commit_failure_rolls_back_without_replaying_body(self):
        original_connect = sqlite3.connect
        error = sqlite3.OperationalError("injected commit protocol")
        error.sqlite_errorcode = 15
        error.sqlite_errorname = "SQLITE_PROTOCOL"
        calls = []
        class FailingCommit(sqlite3.Connection):
            def commit(connection):
                calls.append("commit")
                raise error
        def factory(*args, **kwargs):
            return original_connect(*args, **kwargs, factory=FailingCommit)
        with mock.patch.object(state.sqlite3, "connect", side_effect=factory):
            with self.assertLogs(state.__name__, level="ERROR") as logs:
                with self.assertRaises(sqlite3.OperationalError) as raised:
                    with state.connect() as writer:
                        calls.append("body")
                        writer.execute("INSERT INTO batches(id,name,created_at) VALUES('failed-commit','failed','now')")
        self.assertIs(error, raised.exception)
        self.assertEqual(["body", "commit"], calls)
        self.assertIn("phase=commit", logs.output[0])
        with state.connect() as reader:
            self.assertEqual(0, reader.execute("SELECT count(*) FROM batches WHERE id='failed-commit'").fetchone()[0])

    def test_rollback_failure_preserves_original_error_and_diagnostic(self):
        original_connect = sqlite3.connect
        error = sqlite3.OperationalError("original failure")
        class FailingRollback(sqlite3.Connection):
            def rollback(connection):
                raise sqlite3.OperationalError("rollback failure")
        def factory(*args, **kwargs):
            return original_connect(*args, **kwargs, factory=FailingRollback)
        with mock.patch.object(state.sqlite3, "connect", side_effect=factory):
            with self.assertLogs(state.__name__, level="ERROR") as logs:
                with self.assertRaises(sqlite3.OperationalError) as raised:
                    with state.connect() as writer:
                        writer.execute("BEGIN IMMEDIATE")
                        raise error
        self.assertIs(error, raised.exception)
        self.assertEqual(2, len(logs.output))
        self.assertIn("phase=rollback", logs.output[1])


class CommitRetryPolicy(unittest.TestCase):
    def error(self, code):
        error = sqlite3.OperationalError("injected")
        if code is not None:
            error.sqlite_errorcode = code
        return error

    def test_unclassified_protocol_io_and_snapshot_errors_are_not_retried(self):
        for code in (None, 15, 10, 517):
            with self.subTest(code=code):
                conn = mock.Mock(in_transaction=True)
                conn.commit.side_effect = self.error(code)
                with mock.patch.object(state.time, "sleep") as sleep:
                    with self.assertRaises(sqlite3.OperationalError):
                        state._commit_transaction(conn)
                    conn.commit.assert_called_once()
                    sleep.assert_not_called()

    def test_busy_without_active_transaction_is_not_retried(self):
        conn = mock.Mock(in_transaction=False)
        conn.commit.side_effect = self.error(5)
        with mock.patch.object(state.time, "sleep") as sleep:
            with self.assertRaises(sqlite3.OperationalError):
                state._commit_transaction(conn)
            conn.commit.assert_called_once()
            sleep.assert_not_called()

    def test_busy_retry_exhaustion_is_bounded(self):
        conn = mock.Mock(in_transaction=True)
        conn.commit.side_effect = self.error(5)
        with mock.patch.object(state.time, "sleep") as sleep:
            with self.assertRaises(sqlite3.OperationalError):
                state._commit_transaction(conn)
            self.assertEqual(4, conn.commit.call_count)
            self.assertEqual([mock.call(d) for d in state._COMMIT_RETRY_DELAYS], sleep.call_args_list)

    @unittest.skipUnless(sys.version_info >= (3, 11), "SQLite error codes require Python 3.11+")
    def test_real_sqlite_busy_commit_can_be_retried(self):
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        conn.execute("CREATE TABLE counter(value)")
        pending = conn.execute("INSERT INTO counter VALUES(1) RETURNING value")
        with mock.patch.object(state.time, "sleep", side_effect=lambda delay: pending.close()) as sleep:
            state._commit_transaction(conn)
            sleep.assert_called_once_with(state._COMMIT_RETRY_DELAYS[0])
        self.assertEqual([(1,)], conn.execute("SELECT value FROM counter").fetchall())

    def test_busy_then_success_returns_without_replaying_body(self):
        conn = mock.Mock(in_transaction=True)
        conn.commit.side_effect = [self.error(5), None]
        with mock.patch.object(state.time, "sleep"):
            state._commit_transaction(conn)
        self.assertEqual(2, conn.commit.call_count)


if __name__ == "__main__":
    unittest.main()
