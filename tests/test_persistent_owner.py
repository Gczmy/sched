"""Original-owner recovery, authentication and transactional crash windows."""
from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import sqlite3
import subprocess
import tempfile
import time
import unittest
from unittest import mock

from gsched import execution_state, state
from gsched.dispatcher import Dispatcher
from gsched.execution import BackendUnavailable, ExecutionEnvelope, ExecutionObservation, LinuxFdBackend
from gsched.execution.persistent import (
    PROTOCOL, OwnerUnavailable, PersistentLinuxFdBackend, PersistentOwner,
    _send, _signed, public_binding,
)
from run_execution_accept import WORKER
import test_execution_state as _helpers
from test_review_cli_state import TempStateCase


class PersistentStateTests(TempStateCase):
    # These additional cases share the existing private scheduler DB fixture.
    reserve = _helpers.ExecutionStateTests.reserve
    query = _helpers.ExecutionStateTests.query
    def owner_binding(self, identity):
        owner_id = secrets.token_hex(16)
        return {"schema": PROTOCOL, "owner_id": owner_id, "endpoint": "gsched-owner-" + owner_id,
                "pid": 12345, "start_ticks": 6789, "boot_id": "0" * 8 + "-" + "0" * 4 + "-" + "0" * 4 + "-" + "0" * 4 + "-" + "0" * 12,
                "attempt_id": identity["attempt_id"], "token": "a" * 64}

    def bound(self, *, launch=True):
        job_id, identity = self.reserve()
        binding = self.owner_binding(identity)
        with state.connect() as conn:
            execution_state.bind_owner(conn, job_id, binding)
            if launch:
                execution_state.launch_intent(conn, job_id)
        return job_id, identity, binding

    def dispatcher(self, owner):
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.executor = mock.Mock()
        dispatcher.executor.configured_owner.return_value = None
        dispatcher.executor.restore_configured_owner.return_value = owner
        dispatcher._release_gpu_for_job = mock.Mock()
        dispatcher._handle_job_done = mock.Mock(side_effect=lambda conn, job, rc, **kw:
            state.update_job(conn, job["id"], status="done", rc=rc, finished_at=state.now()) or [])
        dispatcher.log_line = mock.Mock()
        return dispatcher

    def test_binding_is_immutable_original_attempt_and_not_public_secret(self):
        job_id, identity = self.reserve()
        binding = self.owner_binding(identity)
        with state.connect() as conn:
            with self.assertRaises(state.StateError):
                execution_state.bind_owner(conn, job_id, {**binding, "attempt_id": "f" * 32})
            execution_state.bind_owner(conn, job_id, binding)
            execution_state.launch_intent(conn, job_id)
            with self.assertRaises(state.StateError):
                execution_state.bind_owner(conn, job_id, binding)
            for statement in ("UPDATE execution_owners SET binding='{}'", "DELETE FROM execution_owners"):
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(statement)
        response = self.query()
        self.assertEqual(public_binding(binding), response["attempts"][0]["owner"])
        self.assertNotIn(binding["token"], json.dumps(response))
        self.assertNotIn(binding["endpoint"], json.dumps(response))

    def test_rollback_binding_and_intent_leaves_no_launch_authorization(self):
        job_id, identity = self.reserve()
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            execution_state.bind_owner(conn, job_id, self.owner_binding(identity))
            execution_state.launch_intent(conn, job_id)
            conn.rollback()
            self.assertIsNone(execution_state.get_owner_binding(conn, job_id))
            self.assertIsNone(execution_state.get(conn, job_id)["launch_intent_at"])

    def test_recovery_abandons_prepared_without_starting_and_commits_before_ack(self):
        job_id, _, _ = self.bound()
        owner = mock.Mock()
        owner.poll.return_value = ExecutionObservation("prepared", None)
        owner.abandon_prepared.return_value = ExecutionObservation("not_started", None, group_clean=True)
        owner.cancel_reason = None
        dispatcher = self.dispatcher(owner)
        with state.connect() as conn:
            def acknowledge(_):
                self.assertFalse(conn.in_transaction)
                self.assertEqual("interrupted", state.get_job(conn, job_id)["status"])
                self.assertEqual("not_started", execution_state.get(conn, job_id)["phase"])
            dispatcher.executor.retire_configured_execution.side_effect = acknowledge
            dispatcher._reap_configured_executions(conn)
        owner.abandon_prepared.assert_called_once()
        owner._launch.assert_not_called()
        dispatcher._release_gpu_for_job.assert_called_once()

    def test_transient_reconnect_retains_resources_then_original_wait_settles(self):
        job_id, _, _ = self.bound()
        owner = mock.Mock()
        owner.poll.side_effect = OwnerUnavailable()
        owner.cancel_reason = None
        dispatcher = self.dispatcher(owner)
        with state.connect() as conn:
            dispatcher._reap_configured_executions(conn)
            self.assertEqual("running", state.get_job(conn, job_id)["status"])
            self.assertEqual("unresolved", execution_state.get(conn, job_id)["phase"])
            dispatcher._release_gpu_for_job.assert_not_called()
            dispatcher.executor.retire_configured_execution.assert_not_called()
        self.assertEqual("owner_unreachable", self.query()["diagnostics"][0]["uncertainty_reason"])
        owner.poll.side_effect = None
        owner.poll.return_value = ExecutionObservation("exited", 333, 0, {"ru_utime": .1}, group_clean=True)
        with state.connect() as conn:
            dispatcher._reap_configured_executions(conn)
            self.assertEqual("done", state.get_job(conn, job_id)["status"])
            self.assertEqual(0, json.loads(execution_state.get(conn, job_id)["observation"])["returncode"])

    def test_commit_failure_does_not_ack_and_next_daemon_recovers_wait(self):
        job_id, _, _ = self.bound()
        owner = mock.Mock()
        owner.poll.return_value = ExecutionObservation("exited", 333, 7, {"ru_utime": .1}, group_clean=True)
        owner.cancel_reason = None
        dispatcher = self.dispatcher(owner)
        with state.connect() as real:
            proxy = mock.Mock(wraps=real)
            proxy.commit.side_effect = sqlite3.OperationalError("injected terminal commit failure")
            with self.assertRaises(sqlite3.OperationalError):
                dispatcher._reap_configured_executions(proxy)
            real.rollback()
            dispatcher.executor.retire_configured_execution.assert_not_called()
            self.assertEqual("running", state.get_job(real, job_id)["status"])
            self.assertEqual("launching", execution_state.get(real, job_id)["phase"])
        replacement = self.dispatcher(owner)
        with state.connect() as conn:
            replacement._reap_configured_executions(conn)
            self.assertEqual(7, state.get_job(conn, job_id)["rc"])
        owner._launch.assert_not_called()

    def test_committed_terminal_facts_survive_owner_loss_and_ack_retry(self):
        job_id, _, _ = self.bound()
        terminal = {"status": "exited", "pid": 333, "returncode": 0,
                    "rusage": {"ru_utime": .1}, "group_clean": True}
        with state.connect() as conn:
            execution_state.observe(conn, job_id, terminal)
            state.update_job(conn, job_id, status="done")
        owner = mock.Mock()
        owner.poll.side_effect = AssertionError("committed terminal must not be polled or rewritten")
        dispatcher = self.dispatcher(owner)
        with state.connect() as conn:
            dispatcher._reap_configured_executions(conn)
            self.assertEqual(terminal, json.loads(execution_state.get(conn, job_id)["observation"]))
        dispatcher.executor.retire_configured_execution.assert_called_once_with(job_id)
        dispatcher._handle_job_done.assert_not_called()

    def test_v5_readonly_keeps_unbound_attempt_then_writer_migrates(self):
        job_id, _ = self.reserve()
        with state.connect() as conn:
            conn.execute("DROP TRIGGER execution_owner_binding_immutable")
            conn.execute("DROP TRIGGER execution_owner_binding_retained")
            conn.execute("DROP TABLE execution_owners")
            conn.execute("PRAGMA user_version=5")
        state.set_query_only(True)
        try:
            with mock.patch.object(state, "_initialize_database", side_effect=AssertionError("readonly migration")):
                result = self.query()
                self.assertNotIn("owner", result["attempts"][0])
                with state.connect() as conn:
                    self.assertEqual(5, conn.execute("PRAGMA user_version").fetchone()[0])
        finally:
            state.set_query_only(False)
        state.init_db()
        with state.connect() as conn:
            self.assertEqual(6, conn.execute("PRAGMA user_version").fetchone()[0])
            self.assertIsNone(execution_state.get_owner_binding(conn, job_id))


class PersistentBackendTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            LinuxFdBackend()
        except BackendUnavailable as error:
            if os.environ.get("SCHED_REQUIRE_NATIVE") == "1":
                raise
            raise unittest.SkipTest(str(error))
        cls.build = tempfile.TemporaryDirectory(prefix="sched-owner-worker-")
        cls.executable = Path(cls.build.name) / "worker"
        source = Path(cls.build.name) / "worker.c"
        source.write_text("#define _POSIX_C_SOURCE 200809L\n" + WORKER)
        subprocess.run([shutil.which("cc") or "gcc", "-std=c11", "-Wall", "-Wextra", "-Werror",
                        str(source), "-o", str(cls.executable)], check=True, timeout=30)

    @classmethod
    def tearDownClass(cls):
        cls.build.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="sched-owner-case-")
        self.root = Path(self.tmp.name)
        self.prepared = None

    def tearDown(self):
        (self.root / "release").touch()
        if self.prepared is not None:
            owner = self.prepared.owner
            if owner._process.poll() is None:
                owner.wait(10)
                owner.close()
            else:
                owner._process.wait()
            self.prepared.close()
        self.tmp.cleanup()

    def prepare(self, content=b"hold\n", **kwargs):
        from gsched.execution_policy import sealed_bytes
        executable = os.open(self.executable, os.O_RDONLY | os.O_CLOEXEC)
        cwd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        data = sealed_bytes(content, "owner-test-input")
        try:
            identity = {"schema": "sched_execution_identity/v1", "attempt_id": secrets.token_hex(16),
                        "scheduler_pid": os.getpid()}
            self.prepared = PersistentLinuxFdBackend().prepare(ExecutionEnvelope(("worker", "hold")),
                executable_fd=executable, cwd_fd=cwd, fd_bindings={3: data}, identity=identity, **kwargs)
            return self.prepared
        finally:
            for fd in (data, cwd, executable): os.close(fd)

    def ready(self):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                return json.loads((self.root / "ready.json").read_text())
            except (OSError, ValueError): time.sleep(.01)
        self.fail("worker did not receive its immutable identity")

    def test_reconnect_single_child_identity_and_original_wait(self):
        prepared = self.prepare()
        original = prepared.launch()
        identity = self.ready()
        replacement = PersistentOwner(original.binding)
        pid = replacement.poll().pid
        self.assertEqual(pid, replacement._rpc("start").pid)
        self.assertEqual("run\n", (self.root / "runs.txt").read_text())
        self.assertEqual(original.binding["pid"], int((self.root / "parent.txt").read_text()))
        self.assertEqual(public_binding(original.binding), identity["owner"])
        self.assertEqual(original.binding["attempt_id"], identity["attempt"]["attempt_id"])
        self.assertNotIn(original.binding["token"], json.dumps(identity))
        (self.root / "release").touch()
        observation = replacement.wait(10)
        self.assertEqual(0, observation.returncode)
        self.assertTrue(observation.group_clean)
        self.assertIsNotNone(observation.rusage)
        self.assertEqual(asdict(observation), asdict(original.poll()))

    def test_lost_start_response_reconnects_without_second_birth(self):
        prepared = self.prepare()
        binding = prepared.owner.binding
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
            stream.connect("\0" + binding["endpoint"])
            _send(stream, _signed({"schema": PROTOCOL, "owner_id": binding["owner_id"],
                "nonce": secrets.token_hex(16), "op": "start", "parameters": {}}, binding["token"]))
        self.ready()
        replacement = PersistentOwner(binding)
        self.assertEqual("running", replacement.poll().status)
        replacement._rpc("start")
        self.assertEqual("run\n", (self.root / "runs.txt").read_text())

    def test_bad_authentication_and_process_binding_never_control_child(self):
        owner = self.prepare().launch()
        self.ready()
        for binding in ({**owner.binding, "token": "b" * 64},
                        {**owner.binding, "start_ticks": owner.binding["start_ticks"] + 1}):
            with self.assertRaises(OwnerUnavailable):
                PersistentOwner(binding).cancel(0)
        self.assertEqual("running", owner.poll().status)
        self.assertEqual("run\n", (self.root / "runs.txt").read_text())

    def test_disconnected_cancel_escalates_without_controller_polling(self):
        owner = self.prepare(b"ignore\n").launch()
        self.ready()
        owner.cancel(.05)
        time.sleep(.3)
        observation = PersistentOwner(owner.binding).poll()
        self.assertEqual("exited", observation.status)
        self.assertEqual(-signal.SIGKILL, observation.returncode)
        self.assertTrue(observation.group_clean)

    def test_owner_duration_runs_without_controller_and_preserves_reason(self):
        owner = self.prepare(b"ignore\n", duration_seconds=.15).launch()
        self.ready()
        time.sleep(2.5)
        replacement = PersistentOwner(owner.binding)
        observation = replacement.poll()
        self.assertEqual(-signal.SIGKILL, observation.returncode)
        self.assertTrue(observation.group_clean)
        self.assertEqual("timed_out", replacement.cancel_reason)

    def test_unused_preparation_expires_without_birth_or_replay(self):
        prepared = self.prepare(prepare_timeout=.1)
        time.sleep(.3)
        self.assertEqual("not_started", prepared.owner.poll().status)
        self.assertEqual("not_started", prepared.owner._rpc("start").status)
        self.assertFalse((self.root / "runs.txt").exists())

    def test_owner_loss_is_unknown_instead_of_synthetic_exit(self):
        # Kill only the prepared service of this private fixture (no child exists).
        prepared = self.prepare()
        prepared.owner._process.kill()
        prepared.owner._process.wait()
        with self.assertRaises(OwnerUnavailable) as error:
            PersistentOwner(prepared.owner.binding).poll()
        self.assertTrue(error.exception.lost)
        self.assertFalse((self.root / "runs.txt").exists())


if __name__ == "__main__":
    unittest.main()
