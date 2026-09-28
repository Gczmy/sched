"""Durable candidate reservation and unique native log binding on Linux."""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

from gsched import state
from gsched.native_exec import native_exec_project_root_identity_sha256
from gsched.native_launch import (
    NativeLaunchPlanError,
    _create_bound_native_session_log,
)


@unittest.skipUnless(sys.platform.startswith("linux"), "retained FD log requires Linux")
class NativeSessionBindingTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="sched-native-session-")
        self.addCleanup(temporary.cleanup)
        self.root = os.path.join(temporary.name, "project")
        os.mkdir(self.root)
        os.mkdir(os.path.join(self.root, "logs"))
        self.root_fd = os.open(
            self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
        )
        self.addCleanup(os.close, self.root_fd)
        self.root_digest = native_exec_project_root_identity_sha256(self.root)
        self.db = sqlite3.connect(os.path.join(temporary.name, "state.db"))
        self.db.row_factory = sqlite3.Row
        self.addCleanup(self.db.close)
        self.db.executescript(state.SCHEMA)
        state.migrate_project_columns(self.db)
        self.profile_digest = "a" * 64
        state.insert_batch(
            self.db, "batch", "batch", "strict", [], None,
            self.root, {}, project="p",
        )
        spec = {
            "cwd_abs": self.root,
            "_native_exec_contract_v2": {},
            "_native_exec_profile_id": "candidate-v2",
            "_native_exec_profile_sha256": self.profile_digest,
            "_native_exec_project_root_identity_sha256": self.root_digest,
        }
        state.insert_task(self.db, "batch", "task", 1, spec, 0, project="p")
        state.insert_job(
            self.db, "job-v1", "batch", "task", 1, None, project="p",
        )
        self.db.commit()

    def claim(self, *, job_id: str = "job-v1", version: int = 1,
              session_id: str = "1" * 32) -> sqlite3.Row:
        if not self.db.in_transaction:
            self.db.execute("BEGIN IMMEDIATE")
        try:
            return state.claim_native_session_candidate(
                self.db,
                job_id=job_id,
                job_version=version,
                session_id=session_id,
                profile_id="candidate-v2",
                profile_sha256=self.profile_digest,
                project_root_path=self.root,
                project_root_identity_sha256=self.root_digest,
            )
        except BaseException:
            self.db.rollback()
            raise

    def activate(self) -> None:
        self.db.execute("UPDATE batches SET status='active' WHERE id='batch'")
        self.db.commit()

    def test_v1_database_migrates_and_read_only_probe_sees_v2(self) -> None:
        old_dir = tempfile.TemporaryDirectory(prefix="sched-native-migrate-")
        self.addCleanup(old_dir.cleanup)
        old_path = os.path.join(old_dir.name, "state.db")
        old = sqlite3.connect(old_path)
        try:
            old.execute("PRAGMA journal_mode=WAL")
            old.executescript(state.SCHEMA)
            old.execute("DROP TABLE native_sessions")
            old.execute("PRAGMA user_version=1")
            old.execute(
                "INSERT INTO batches (id,name,mode,status,created_at)"
                " VALUES ('kept','kept','mix','done','2026-01-01')"
            )
            old.commit()
        finally:
            old.close()
        os.chmod(old_path, 0o600)
        with (
            mock.patch.object(state, "default_state_dir", return_value=old_dir.name),
            mock.patch.object(state, "db_path", return_value=old_path),
        ):
            before = os.stat(old_path).st_mtime_ns
            self.assertFalse(state._database_schema_is_current(old_path))
            self.assertEqual(before, os.stat(old_path).st_mtime_ns)
            state._initialize_database()
            self.assertTrue(state._database_schema_is_current(old_path))
        migrated = sqlite3.connect(old_path)
        try:
            self.assertEqual(
                state.DB_SCHEMA_VERSION,
                migrated.execute("PRAGMA user_version").fetchone()[0],
            )
            self.assertEqual(
                "done", migrated.execute(
                    "SELECT status FROM batches WHERE id='kept'"
                ).fetchone()[0],
            )
        finally:
            migrated.close()

    def test_claim_is_active_latest_pending_and_one_shot(self) -> None:
        with self.assertRaises(state.StateError):
            self.claim()
        self.assertEqual(
            "pending",
            state.get_job(self.db, "job-v1")["status"],
        )
        self.assertEqual(
            0, self.db.execute("SELECT COUNT(*) FROM native_sessions").fetchone()[0]
        )

        self.activate()
        claimed = self.claim()
        self.db.commit()
        self.assertEqual("running", state.get_job(self.db, "job-v1")["status"])
        self.assertEqual(1, claimed["job_version"])
        self.assertEqual("isolated_integration", claimed["evaluation_domain"])
        self.assertEqual("unbound", claimed["owner_kind"])
        self.assertEqual("reserved", claimed["phase"])
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "UPDATE native_sessions SET evaluation_domain='production'"
                " WHERE session_id=?", (claimed["session_id"],)
            )
        self.db.rollback()
        with self.assertRaises(state.StateError):
            self.claim(session_id="2" * 32)
        self.assertEqual(
            1, self.db.execute("SELECT COUNT(*) FROM native_sessions").fetchone()[0]
        )

    def test_latest_version_and_insert_conflict_roll_back_claim(self) -> None:
        self.activate()
        spec = self.db.execute(
            "SELECT spec FROM tasks WHERE batch_id='batch' AND id='task' AND version=1"
        ).fetchone()[0]
        self.db.execute(
            "INSERT INTO tasks (batch_id,id,version,spec,order_idx,project)"
            " VALUES ('batch','task',2,?,0,'p')", (spec,)
        )
        state.insert_job(
            self.db, "job-v2", "batch", "task", 2, None, project="p",
        )
        self.db.commit()
        with self.assertRaises(state.StateError):
            self.claim()
        self.assertEqual("pending", state.get_job(self.db, "job-v1")["status"])

        self.claim(job_id="job-v2", version=2)
        self.db.commit()
        state.insert_task(self.db, "batch", "other", 1, {
            "cwd_abs": self.root,
            "_native_exec_contract_v2": {},
            "_native_exec_profile_id": "candidate-v2",
            "_native_exec_profile_sha256": self.profile_digest,
            "_native_exec_project_root_identity_sha256": self.root_digest,
        }, 1, project="p")
        state.insert_job(
            self.db, "other-job", "batch", "other", 1, None, project="p",
        )
        self.db.commit()
        with self.assertRaises(sqlite3.IntegrityError):
            self.claim(job_id="other-job", session_id="1" * 32)
        self.assertEqual("pending", state.get_job(self.db, "other-job")["status"])

    def test_log_inode_binding_is_durable_and_cannot_reopen(self) -> None:
        self.activate()
        reservation = self.claim()
        self.db.commit()
        fd = _create_bound_native_session_log(
            self.db, session_id=reservation["session_id"],
            project_root_fd=self.root_fd,
        )
        try:
            inode = os.fstat(fd)
        finally:
            os.close(fd)
        bound = state.get_native_session(self.db, reservation["session_id"])
        self.assertEqual("log_bound", bound["phase"])
        self.assertEqual(str(inode.st_dev), bound["log_dev"])
        self.assertEqual(str(inode.st_ino), bound["log_ino"])
        self.assertFalse(self.db.in_transaction)
        with self.assertRaises(NativeLaunchPlanError):
            _create_bound_native_session_log(
                self.db, session_id=reservation["session_id"],
                project_root_fd=self.root_fd,
            )

    def test_failed_log_creation_or_bind_keeps_consumed_reservation(self) -> None:
        self.activate()
        reservation = self.claim()
        self.db.commit()
        log_path = os.path.join(self.root, reservation["log_relative_path"])
        with open(log_path, "xb") as stream:
            stream.write(b"existing")
        with self.assertRaises(NativeLaunchPlanError):
            _create_bound_native_session_log(
                self.db, session_id=reservation["session_id"],
                project_root_fd=self.root_fd,
            )
        with open(log_path, "rb") as stream:
            self.assertEqual(b"existing", stream.read())
        self.assertEqual(
            "reserved", state.get_native_session(self.db, reservation["session_id"])["phase"]
        )
        os.unlink(log_path)
        os.rmdir(os.path.join(self.root, "logs"))
        with self.assertRaises(NativeLaunchPlanError):
            _create_bound_native_session_log(
                self.db, session_id=reservation["session_id"],
                project_root_fd=self.root_fd,
            )
        self.assertEqual(
            "reserved", state.get_native_session(self.db, reservation["session_id"])["phase"]
        )
        os.mkdir(os.path.join(self.root, "logs"))
        with mock.patch.object(
            state, "bind_native_session_log", side_effect=state.StateError("injected")
        ):
            with self.assertRaises(state.StateError):
                _create_bound_native_session_log(
                    self.db, session_id=reservation["session_id"],
                    project_root_fd=self.root_fd,
                )
        self.assertTrue(os.path.exists(log_path))
        self.assertEqual("running", state.get_job(self.db, "job-v1")["status"])
        self.assertEqual(
            "reserved", state.get_native_session(self.db, reservation["session_id"])["phase"]
        )


if __name__ == "__main__":
    unittest.main()
