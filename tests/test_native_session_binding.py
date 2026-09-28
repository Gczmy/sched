"""Durable isolated native session, log, and monitor intent on Linux."""

from __future__ import annotations

import fcntl
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from unittest import mock

from gsched import state
from gsched.native_exec import native_exec_project_root_identity_sha256
from gsched.native_launch import (
    NativeLaunchPlanError,
    _create_bound_native_session_log,
    _commit_native_monitor_launch_intent,
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
        state.migrate_native_monitor_launch_attempts(self.db)
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

    def test_v1_database_migrates_and_read_only_probe_sees_v4(self) -> None:
        old_dir = tempfile.TemporaryDirectory(prefix="sched-native-migrate-")
        self.addCleanup(old_dir.cleanup)
        old_path = os.path.join(old_dir.name, "state.db")
        old = sqlite3.connect(old_path)
        old.row_factory = sqlite3.Row
        try:
            old.execute("PRAGMA journal_mode=WAL")
            old.executescript(state.SCHEMA)
            state.migrate_gpu_jobs(old)
            state.migrate_project_columns(old)
            state.migrate_incidents(old)
            state.migrate_job_progress(old)
            state.migrate_operation_requests(old)
            state.migrate_revisions(old)
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
            self.assertTrue(state._database_schema_is_query_compatible(old_path))
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

    def test_v2_migration_conservatively_consumes_old_reservations(self) -> None:
        old_dir = tempfile.TemporaryDirectory(prefix="sched-native-v2-migrate-")
        self.addCleanup(old_dir.cleanup)
        old_path = os.path.join(old_dir.name, "state.db")
        v2_schema = state.SCHEMA.replace("  log_attempted_at TEXT,\n", "", 1)
        v2_schema = v2_schema.replace("  monitor_launch_attempted_at TEXT,\n", "", 1)
        self.assertNotEqual(state.SCHEMA, v2_schema)
        old = sqlite3.connect(old_path)
        old.row_factory = sqlite3.Row
        try:
            old.execute("PRAGMA journal_mode=WAL")
            old.executescript(v2_schema)
            state.migrate_gpu_jobs(old)
            state.migrate_project_columns(old)
            state.migrate_incidents(old)
            state.migrate_job_progress(old)
            state.migrate_operation_requests(old)
            state.migrate_revisions(old)
            old.execute("PRAGMA user_version=2")
            old.execute(
                "INSERT INTO native_sessions (session_id,job_id,job_version,"
                "evaluation_domain,owner_kind,profile_id,profile_sha256,"
                "project_root_path,project_root_identity_sha256,log_relative_path,"
                "phase,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "2" * 32, "old-job", 1, "isolated_integration", "unbound",
                    "candidate-v2", self.profile_digest, self.root, self.root_digest,
                    "logs/sched-native-" + "2" * 32 + ".log", "reserved", "2026-01-01",
                ),
            )
            old.commit()
        finally:
            old.close()
        os.chmod(old_path, 0o600)
        with (
            mock.patch.object(state, "default_state_dir", return_value=old_dir.name),
            mock.patch.object(state, "db_path", return_value=old_path),
        ):
            self.assertTrue(state._database_schema_is_query_compatible(old_path))
            self.assertFalse(state._database_schema_is_current(old_path))
            state._initialize_database()
            self.assertTrue(state._database_schema_is_current(old_path))
        migrated = sqlite3.connect(old_path)
        try:
            row = migrated.execute(
                "SELECT phase,log_attempted_at,monitor_launch_attempted_at"
                " FROM native_sessions WHERE session_id=?",
                ("2" * 32,),
            ).fetchone()
            self.assertEqual(("reserved", "2026-01-01", "2026-01-01"), row)
            self.assertEqual(
                4, migrated.execute("PRAGMA user_version").fetchone()[0]
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
        self.assertIsNotNone(bound["log_attempted_at"])
        self.assertEqual(str(inode.st_dev), bound["log_dev"])
        self.assertEqual(str(inode.st_ino), bound["log_ino"])
        self.assertFalse(self.db.in_transaction)
        with self.assertRaises(NativeLaunchPlanError):
            _create_bound_native_session_log(
                self.db, session_id=reservation["session_id"],
                project_root_fd=self.root_fd,
            )

    def test_o_excl_failure_consumes_only_log_attempt(self) -> None:
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
        self.assertIsNotNone(
            state.get_native_session(self.db, reservation["session_id"])["log_attempted_at"]
        )
        os.unlink(log_path)
        with self.assertRaises(NativeLaunchPlanError):
            _create_bound_native_session_log(
                self.db, session_id=reservation["session_id"],
                project_root_fd=self.root_fd,
            )
        self.assertFalse(os.path.exists(log_path))

    def test_missing_log_directory_consumes_only_log_attempt(self) -> None:
        self.activate()
        reservation = self.claim()
        self.db.commit()
        log_path = os.path.join(self.root, reservation["log_relative_path"])
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
        with self.assertRaises(NativeLaunchPlanError):
            _create_bound_native_session_log(
                self.db, session_id=reservation["session_id"],
                project_root_fd=self.root_fd,
            )
        self.assertFalse(os.path.exists(log_path))

    def test_failed_binding_consumes_log_attempt_and_keeps_file(self) -> None:
        self.activate()
        reservation = self.claim()
        self.db.commit()
        log_path = os.path.join(self.root, reservation["log_relative_path"])
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
        self.assertIsNotNone(
            state.get_native_session(self.db, reservation["session_id"])["log_attempted_at"]
        )
        with self.assertRaises(NativeLaunchPlanError):
            _create_bound_native_session_log(
                self.db, session_id=reservation["session_id"],
                project_root_fd=self.root_fd,
            )

    def test_log_attempt_cas_and_bind_requires_intent(self) -> None:
        self.activate()
        reservation = self.claim()
        self.db.commit()
        self.db.execute("BEGIN IMMEDIATE")
        with self.assertRaises(state.StateError):
            state.bind_native_session_log(self.db, reservation["session_id"], 1, 1)
        first = state.mark_native_session_log_attempted(
            self.db, reservation["session_id"]
        )
        self.db.commit()
        self.assertIsNotNone(first["log_attempted_at"])
        db_path = self.db.execute("PRAGMA database_list").fetchone()[2]
        with closing(sqlite3.connect(db_path)) as other:
            other.row_factory = sqlite3.Row
            other.execute("BEGIN IMMEDIATE")
            with self.assertRaises(state.StateError):
                state.mark_native_session_log_attempted(
                    other, reservation["session_id"]
                )
            other.rollback()
        self.assertEqual(
            0, self.db.execute(
                "SELECT COUNT(*) FROM native_sessions"
                " WHERE session_id=? AND log_attempted_at IS NULL",
                (reservation["session_id"],),
            ).fetchone()[0],
        )

    def test_shutdown_fences_log_attempt_without_consumption(self) -> None:
        self.activate()
        session_id = self.claim()["session_id"]
        self.db.commit()
        marker_path = os.path.join(self.root, ".daemon-stopping")
        with mock.patch.object(
            state, "submission_shutdown_marker", return_value=marker_path
        ):
            with open(marker_path, "xb"):
                pass
            self.db.execute("BEGIN DEFERRED")
            with self.assertRaisesRegex(state.StateError, "daemon shutdown"):
                state.mark_native_session_log_attempted(self.db, session_id)
            # Even a caller that catches the error and commits cannot consume
            # the one-shot attempt while shutdown is published.
            self.db.commit()
            self.assertIsNone(
                state.get_native_session(self.db, session_id)["log_attempted_at"]
            )
            os.unlink(marker_path)
            with mock.patch.object(
                state.os, "lstat", side_effect=PermissionError("injected")
            ):
                self.db.execute("BEGIN DEFERRED")
                with self.assertRaisesRegex(state.StateError, "cannot inspect"):
                    state.mark_native_session_log_attempted(self.db, session_id)
                self.db.commit()
            self.assertIsNone(
                state.get_native_session(self.db, session_id)["log_attempted_at"]
            )

    def test_uncertain_log_attempt_commit_never_opens_file(self) -> None:
        class CommitUnknown(sqlite3.Connection):
            def commit(self) -> None:
                super().commit()
                raise sqlite3.OperationalError("injected unknown commit result")

        self.activate()
        reservation = self.claim()
        self.db.commit()
        db_path = self.db.execute("PRAGMA database_list").fetchone()[2]
        log_path = os.path.join(self.root, reservation["log_relative_path"])
        with closing(sqlite3.connect(db_path, factory=CommitUnknown)) as other:
            other.row_factory = sqlite3.Row
            with mock.patch(
                "gsched.native_launch._open_project_local_log_fd"
            ) as open_log:
                with self.assertRaisesRegex(
                    sqlite3.OperationalError, "unknown commit result"
                ):
                    _create_bound_native_session_log(
                        other, session_id=reservation["session_id"],
                        project_root_fd=self.root_fd,
                    )
                open_log.assert_not_called()
        self.assertFalse(os.path.exists(log_path))
        self.assertIsNotNone(
            state.get_native_session(self.db, reservation["session_id"])[
                "log_attempted_at"
            ]
        )
        with self.assertRaises(NativeLaunchPlanError):
            _create_bound_native_session_log(
                self.db, session_id=reservation["session_id"],
                project_root_fd=self.root_fd,
            )

    def test_durable_cancel_fences_log_attempt(self) -> None:
        self.activate()
        reservation = self.claim()
        self.db.commit()
        request_id = state.insert_control_request(self.db, "job-v1")
        self.db.commit()
        log_path = os.path.join(self.root, reservation["log_relative_path"])
        for request_status in ("pending", "done"):
            with self.subTest(request_status=request_status):
                with self.assertRaises(state.StateError):
                    _create_bound_native_session_log(
                        self.db, session_id=reservation["session_id"],
                        project_root_fd=self.root_fd,
                    )
                self.assertIsNone(
                    state.get_native_session(
                        self.db, reservation["session_id"]
                    )["log_attempted_at"]
                )
                self.assertFalse(os.path.exists(log_path))
                self.db.execute(
                    "UPDATE control_requests SET status='done' WHERE id=?",
                    (request_id,),
                )
                self.db.commit()

    def test_durable_cancel_fences_log_binding(self) -> None:
        self.activate()
        reservation = self.claim()
        self.db.commit()
        self.db.execute("BEGIN IMMEDIATE")
        state.mark_native_session_log_attempted(
            self.db, reservation["session_id"]
        )
        self.db.commit()
        state.insert_control_request(self.db, "job-v1")
        self.db.commit()
        self.db.execute("BEGIN IMMEDIATE")
        with self.assertRaises(state.StateError):
            state.bind_native_session_log(
                self.db, reservation["session_id"], 1, 1
            )
        self.db.rollback()
        reservation = state.get_native_session(self.db, reservation["session_id"])
        self.assertEqual("reserved", reservation["phase"])
        self.assertIsNotNone(reservation["log_attempted_at"])

    def _bound_log(self) -> tuple[str, int]:
        self.activate()
        reservation = self.claim()
        self.db.commit()
        fd = _create_bound_native_session_log(
            self.db, session_id=reservation["session_id"],
            project_root_fd=self.root_fd,
        )
        self.addCleanup(os.close, fd)
        return reservation["session_id"], fd

    def test_monitor_intent_requires_bound_log_and_exact_retained_fd(self) -> None:
        self.activate()
        reservation = self.claim()
        self.db.commit()
        with self.assertRaises(NativeLaunchPlanError):
            _commit_native_monitor_launch_intent(
                self.db, session_id=reservation["session_id"],
                project_root_fd=self.root_fd, log_fd=self.root_fd,
            )
        self.assertIsNone(state.get_native_session(
            self.db, reservation["session_id"]
        )["monitor_launch_attempted_at"])

        log_fd = _create_bound_native_session_log(
            self.db, session_id=reservation["session_id"],
            project_root_fd=self.root_fd,
        )
        self.addCleanup(os.close, log_fd)
        log_path = os.path.join(self.root, reservation["log_relative_path"])
        read_fd = os.open(log_path, os.O_RDONLY | os.O_CLOEXEC)
        self.addCleanup(os.close, read_fd)
        no_append_fd = os.open(log_path, os.O_WRONLY | os.O_CLOEXEC)
        self.addCleanup(os.close, no_append_fd)
        for bad_fd in (read_fd, no_append_fd):
            with self.assertRaises(NativeLaunchPlanError):
                _commit_native_monitor_launch_intent(
                    self.db, session_id=reservation["session_id"],
                    project_root_fd=self.root_fd, log_fd=bad_fd,
                )
        no_cloexec_fd = os.dup(log_fd)
        self.addCleanup(os.close, no_cloexec_fd)
        fcntl.fcntl(no_cloexec_fd, fcntl.F_SETFD, 0)
        with self.assertRaises(NativeLaunchPlanError):
            _commit_native_monitor_launch_intent(
                self.db, session_id=reservation["session_id"],
                project_root_fd=self.root_fd, log_fd=no_cloexec_fd,
            )
        self.assertIsNone(state.get_native_session(
            self.db, reservation["session_id"]
        )["monitor_launch_attempted_at"])

        self.db.execute("UPDATE native_sessions SET log_ino='999' WHERE session_id=?",
                        (reservation["session_id"],))
        self.db.commit()
        with self.assertRaises(NativeLaunchPlanError):
            _commit_native_monitor_launch_intent(
                self.db, session_id=reservation["session_id"],
                project_root_fd=self.root_fd, log_fd=log_fd,
            )

    def test_monitor_intent_commits_once_and_cannot_be_cleared(self) -> None:
        session_id, log_fd = self._bound_log()
        intent = _commit_native_monitor_launch_intent(
            self.db, session_id=session_id,
            project_root_fd=self.root_fd, log_fd=log_fd,
        )
        self.assertIsNotNone(intent["monitor_launch_attempted_at"])
        self.assertFalse(self.db.in_transaction)
        with self.assertRaises(NativeLaunchPlanError):
            _commit_native_monitor_launch_intent(
                self.db, session_id=session_id,
                project_root_fd=self.root_fd, log_fd=log_fd,
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute(
                "UPDATE native_sessions SET monitor_launch_attempted_at=NULL"
                " WHERE session_id=?", (session_id,),
            )
        self.db.rollback()

    def test_shutdown_fences_monitor_intent_without_consumption(self) -> None:
        session_id, log_fd = self._bound_log()
        inode = os.fstat(log_fd)
        marker_path = os.path.join(self.root, ".daemon-stopping")
        with mock.patch.object(
            state, "submission_shutdown_marker", return_value=marker_path
        ):
            with open(marker_path, "xb"):
                pass
            self.db.execute("BEGIN DEFERRED")
            with self.assertRaisesRegex(state.StateError, "daemon shutdown"):
                state.mark_native_monitor_launch_attempted(
                    self.db, session_id, inode.st_dev, inode.st_ino
                )
            self.db.commit()
            self.assertIsNone(
                state.get_native_session(self.db, session_id)[
                    "monitor_launch_attempted_at"
                ]
            )
            os.unlink(marker_path)
            with mock.patch.object(
                state.os, "lstat", side_effect=PermissionError("injected")
            ):
                self.db.execute("BEGIN DEFERRED")
                with self.assertRaisesRegex(state.StateError, "cannot inspect"):
                    state.mark_native_monitor_launch_attempted(
                        self.db, session_id, inode.st_dev, inode.st_ino
                    )
                self.db.commit()
            self.assertIsNone(
                state.get_native_session(self.db, session_id)[
                    "monitor_launch_attempted_at"
                ]
            )

    def test_monitor_shutdown_check_runs_after_writer_claim(self) -> None:
        session_id, log_fd = self._bound_log()
        inode = os.fstat(log_fd)
        database_path = self.db.execute("PRAGMA database_list").fetchone()["file"]
        observed = []

        def marker_after_writer() -> str:
            with closing(sqlite3.connect(database_path, timeout=0.0)) as other:
                with self.assertRaises(sqlite3.OperationalError):
                    other.execute("BEGIN IMMEDIATE")
            observed.append(True)
            return os.path.join(self.root, ".absent-shutdown-marker")

        with mock.patch.object(
            state, "submission_shutdown_marker", side_effect=marker_after_writer
        ):
            self.db.execute("BEGIN DEFERRED")
            state.mark_native_monitor_launch_attempted(
                self.db, session_id, inode.st_dev, inode.st_ino
            )
        self.db.commit()
        self.assertEqual([True], observed)
        self.assertIsNotNone(
            state.get_native_session(self.db, session_id)[
                "monitor_launch_attempted_at"
            ]
        )

    def test_monitor_intent_rejects_replaced_project_log_path(self) -> None:
        session_id, log_fd = self._bound_log()
        session = state.get_native_session(self.db, session_id)
        log_path = os.path.join(self.root, session["log_relative_path"])
        os.unlink(log_path)
        with open(log_path, "xb") as replacement:
            replacement.write(b"replacement")
        with self.assertRaises(NativeLaunchPlanError):
            _commit_native_monitor_launch_intent(
                self.db, session_id=session_id,
                project_root_fd=self.root_fd, log_fd=log_fd,
            )
        self.assertIsNone(state.get_native_session(
            self.db, session_id
        )["monitor_launch_attempted_at"])

    def test_monitor_intent_writer_competition_and_job_guards(self) -> None:
        session_id, log_fd = self._bound_log()
        inode = os.fstat(log_fd)
        for mutation in (
            "UPDATE batches SET status='blocked' WHERE id='batch'",
            "UPDATE jobs SET status='failed' WHERE id='job-v1'",
            "UPDATE jobs SET pgid=123 WHERE id='job-v1'",
            "UPDATE jobs SET kill_reason='cancelled' WHERE id='job-v1'",
            "INSERT INTO jobs (id,batch_id,task_id,version,status)"
            " VALUES ('job-v2','batch','task',2,'pending')",
        ):
            with self.subTest(mutation=mutation):
                self.db.execute("BEGIN IMMEDIATE")
                self.db.execute(mutation)
                with self.assertRaises(state.StateError):
                    state.mark_native_monitor_launch_attempted(
                        self.db, session_id, inode.st_dev, inode.st_ino
                    )
                self.db.rollback()
        for wrong_dev, wrong_ino in (
            (inode.st_dev + 1, inode.st_ino),
            (inode.st_dev, inode.st_ino + 1),
        ):
            self.db.execute("BEGIN IMMEDIATE")
            with self.assertRaises(state.StateError):
                state.mark_native_monitor_launch_attempted(
                    self.db, session_id, wrong_dev, wrong_ino
                )
            self.db.rollback()
        # The table CHECK also rejects these values in normal operation.  Force
        # a malformed row to prove the CAS itself does not trust that CHECK.
        self.db.execute("PRAGMA ignore_check_constraints=ON")
        try:
            for column, value in (
                ("evaluation_domain", "production"),
                ("owner_kind", "production_owner"),
            ):
                with self.subTest(column=column):
                    self.db.execute("BEGIN IMMEDIATE")
                    self.db.execute(
                        f"UPDATE native_sessions SET {column}=? WHERE session_id=?",
                        (value, session_id),
                    )
                    with self.assertRaises(state.StateError):
                        state.mark_native_monitor_launch_attempted(
                            self.db, session_id, inode.st_dev, inode.st_ino
                        )
                    self.db.rollback()
        finally:
            self.db.execute("PRAGMA ignore_check_constraints=OFF")
        db_path = self.db.execute("PRAGMA database_list").fetchone()[2]
        with closing(sqlite3.connect(db_path, timeout=0)) as other:
            other.row_factory = sqlite3.Row
            self.db.execute("BEGIN IMMEDIATE")
            state.mark_native_monitor_launch_attempted(
                self.db, session_id, inode.st_dev, inode.st_ino
            )
            with self.assertRaises(sqlite3.OperationalError):
                other.execute("BEGIN IMMEDIATE")
            self.db.commit()
            other.execute("BEGIN IMMEDIATE")
            with self.assertRaises(state.StateError):
                state.mark_native_monitor_launch_attempted(
                    other, session_id, inode.st_dev, inode.st_ino
                )
            other.rollback()

    def test_monitor_intent_rejects_any_durable_cancel_record(self) -> None:
        session_id, log_fd = self._bound_log()
        request_id = state.insert_control_request(self.db, "job-v1")
        self.db.commit()
        for request_status in ("pending", "done"):
            with self.subTest(request_status=request_status):
                with self.assertRaises(state.StateError):
                    _commit_native_monitor_launch_intent(
                        self.db, session_id=session_id,
                        project_root_fd=self.root_fd, log_fd=log_fd,
                    )
                self.assertIsNone(state.get_native_session(
                    self.db, session_id
                )["monitor_launch_attempted_at"])
                self.db.execute(
                    "UPDATE control_requests SET status='done' WHERE id=?",
                    (request_id,),
                )
                self.db.commit()

    def test_uncertain_monitor_intent_commit_consumes_without_return(self) -> None:
        class CommitUnknown(sqlite3.Connection):
            def commit(self) -> None:
                super().commit()
                raise sqlite3.OperationalError("injected unknown launch commit")

        session_id, log_fd = self._bound_log()
        db_path = self.db.execute("PRAGMA database_list").fetchone()[2]
        with closing(sqlite3.connect(db_path, factory=CommitUnknown)) as other:
            other.row_factory = sqlite3.Row
            with self.assertRaisesRegex(
                sqlite3.OperationalError, "unknown launch commit"
            ):
                _commit_native_monitor_launch_intent(
                    other, session_id=session_id,
                    project_root_fd=self.root_fd, log_fd=log_fd,
                )
        self.assertIsNotNone(state.get_native_session(
            self.db, session_id
        )["monitor_launch_attempted_at"])
        with self.assertRaises(NativeLaunchPlanError):
            _commit_native_monitor_launch_intent(
                self.db, session_id=session_id,
                project_root_fd=self.root_fd, log_fd=log_fd,
            )

    def test_v3_migration_consumes_existing_bound_session(self) -> None:
        old_dir = tempfile.TemporaryDirectory(prefix="sched-native-v3-migrate-")
        self.addCleanup(old_dir.cleanup)
        db_path = os.path.join(old_dir.name, "state.db")
        v3_schema = state.SCHEMA.replace("  monitor_launch_attempted_at TEXT,\n", "", 1)
        with closing(sqlite3.connect(db_path)) as old:
            old.row_factory = sqlite3.Row
            old.execute("PRAGMA journal_mode=WAL")
            old.executescript(v3_schema)
            state.migrate_gpu_jobs(old)
            state.migrate_project_columns(old)
            state.migrate_incidents(old)
            state.migrate_job_progress(old)
            state.migrate_operation_requests(old)
            state.migrate_revisions(old)
            old.execute("PRAGMA user_version=3")
            for session_id, phase in (("3" * 32, "reserved"), ("4" * 32, "log_bound")):
                old.execute(
                    "INSERT INTO native_sessions ("
                    "session_id,job_id,job_version,evaluation_domain,owner_kind,"
                    "profile_id,profile_sha256,project_root_path,"
                    "project_root_identity_sha256,log_relative_path,"
                    "log_attempted_at,log_dev,log_ino,phase,created_at,log_bound_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        session_id, f"old-{session_id}", 1,
                        "isolated_integration", "unbound", "candidate-v2",
                        self.profile_digest, self.root, self.root_digest,
                        f"logs/sched-native-{session_id}.log", "2026-01-01",
                        "1" if phase == "log_bound" else None,
                        "2" if phase == "log_bound" else None,
                        phase, "2026-01-01",
                        "2026-01-01" if phase == "log_bound" else None,
                    ),
                )
            old.commit()
        os.chmod(db_path, 0o600)
        with (
            mock.patch.object(state, "default_state_dir", return_value=old_dir.name),
            mock.patch.object(state, "db_path", return_value=db_path),
        ):
            self.assertTrue(state._database_schema_is_query_compatible(db_path))
            state._initialize_database()
        with closing(sqlite3.connect(db_path)) as upgraded:
            consumed = upgraded.execute(
                "SELECT monitor_launch_attempted_at FROM native_sessions"
            ).fetchall()
            self.assertEqual(2, len(consumed))
            self.assertTrue(all(row[0] is not None for row in consumed))
            self.assertEqual(4, upgraded.execute("PRAGMA user_version").fetchone()[0])


if __name__ == "__main__":
    unittest.main()
