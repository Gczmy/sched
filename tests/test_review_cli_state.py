from __future__ import annotations

import argparse
import builtins
import contextlib
import fcntl
import io
import json
import os
import select
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from gsched import artifacts, cli, config, daemon, state
from gsched.dispatcher import Dispatcher


class TempStateCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state_root = os.path.join(self.tmp.name, "state")
        self.config_path = os.path.join(self.tmp.name, "config.json")
        self.cfg = {
            "schema_version": 1,
            "user": "test",
            "node": "review-node",
            "state_dir": self.state_root,
            "gpus": [],
            "default_project": "p",
            "projects": {"p": {"root": self.tmp.name, "git": False, "gpu_quota": 1}},
            "venvs": {},
        }
        with open(self.config_path, "w", encoding="utf-8") as stream:
            json.dump(self.cfg, stream)
        self.env = mock.patch.dict(
            os.environ,
            {
                "SCHED_STATE": self.state_root,
                "SCHED_CONFIG": self.config_path,
                "SCHED_ALLOW_FOREIGN_WRITE": "1",
            },
            clear=False,
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        self._clear_state_caches()
        self.addCleanup(self._clear_state_caches)
        state.init_db()

    @staticmethod
    def _clear_state_caches() -> None:
        state._hostname_cache.clear()
        state._hostname_last_good.clear()
        state._pinned_host.clear()

    def capture(self, function, *args, **kwargs):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = function(*args, **kwargs)
        return result, stdout.getvalue(), stderr.getvalue()

    def seed_batch(
        self,
        *,
        batch_id: str = "batch-20260829-000000",
        name: str = "batch",
        batch_status: str = "active",
        job_status: str = "failed",
        version: int = 1,
        task_id: str = "task",
    ) -> str:
        spec = {
            "id": task_id,
            "cmd": ["/bin/true"],
            "stages": None,
            "cwd_abs": self.tmp.name,
            "git": False,
            "env": {},
            "resources": {"gpu": 0, "cpus": 1},
            "duration_min": None,
            "max_retry": 1,
            "artifacts": {},
            "paths_escape": False,
            "project": "p",
        }
        job_id = f"{batch_id}-{task_id}-v{version}"
        with state.connect() as conn:
            state.insert_batch(
                conn, batch_id, name, "mix", [], None, self.tmp.name, {}, project="p"
            )
            conn.execute("UPDATE batches SET status=? WHERE id=?", (batch_status, batch_id))
            state.insert_task(conn, batch_id, task_id, version, spec, 0, "p")
            state.insert_job(conn, job_id, batch_id, task_id, version, f"fp-{version}", None, "p")
            state.update_job(
                conn,
                job_id,
                status=job_status,
                rc=1 if job_status == "failed" else 0,
                failure="review failure" if job_status == "failed" else None,
                started_at="2026-08-29 10:00:00",
                finished_at="2026-08-29 10:00:05" if job_status not in ("pending", "running") else None,
            )
        return job_id

    def batch_revision(self, batch_id: str = "batch-20260829-000000") -> int:
        with state.connect() as conn:
            row = conn.execute(
                "SELECT revision FROM batches WHERE id=?",
                (batch_id,),
            ).fetchone()
        return int(row["revision"])

    def status_json(
        self,
        *,
        limit: int = 200,
        cursor: str | None = None,
        job_cursor: str | None = None,
        batch: str | None = None,
    ) -> dict:
        args = argparse.Namespace(
            batch=batch,
            json=True,
            detail=False,
            project=None,
            limit=limit,
            cursor=cursor,
            job_cursor=job_cursor,
        )
        with mock.patch.object(cli, "_load_cfg", return_value=self.cfg), mock.patch.object(
            cli, "_daemon_health", return_value={}
        ):
            rc, stdout, stderr = self.capture(cli.cmd_status, args)
        self.assertEqual(0, rc, stderr)
        return json.loads(stdout)


class ReviewLifecycleRaceTests(TempStateCase):
    def test_dispatcher_publishes_and_daemon_parses_physical_host(self) -> None:
        dispatcher = Dispatcher(self.cfg, fake=True)
        self.addCleanup(dispatcher.log.close)
        with mock.patch("socket.gethostname", return_value=" compute-a "), mock.patch.object(
            dispatcher,
            "_proc_start_time",
            return_value="proc:lease",
        ):
            self.assertTrue(dispatcher.acquire_lock())
            try:
                with open(dispatcher._lock_owner_file(), encoding="utf-8") as stream:
                    published = json.load(stream)
                self.assertEqual("compute-a", published["physical_host"])
                self.assertEqual(published, daemon._read_lease_owner())
            finally:
                dispatcher._cleanup_lock()

    def test_daemon_owner_reader_rejects_fifo_without_blocking(self) -> None:
        os.makedirs(os.path.dirname(daemon._owner_file()), exist_ok=True)
        os.mkfifo(daemon._owner_file(), 0o600)
        program = (
            "from gsched import daemon; "
            "raise SystemExit(0 if daemon._read_lease_owner() is None else 1)"
        )
        proc = subprocess.Popen(
            [sys.executable, "-c", program],
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            _stdout, stderr = proc.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            self.fail("daemon owner FIFO blocked the reader")
        self.assertEqual(0, proc.returncode, stderr)

    def test_daemon_owner_reader_rechecks_the_bounded_descriptor_content(
        self,
    ) -> None:
        os.makedirs(os.path.dirname(daemon._owner_file()), exist_ok=True)
        owner = {
            "schema_version": 1,
            "lease_id": "bounded-owner",
            "pid": 4242,
            "start_token": "proc:424200",
            "physical_host": "compute-a",
        }
        with open(daemon._owner_file(), "w", encoding="utf-8") as stream:
            json.dump(owner, stream)

        with mock.patch.object(
            daemon.os,
            "read",
            return_value=b"x" * 4097,
        ) as bounded_read:
            self.assertIsNone(daemon._read_lease_owner())

        bounded_read.assert_called_once_with(mock.ANY, 4097)

    def test_dispatcher_refuses_to_publish_empty_physical_host(self) -> None:
        for index, physical_host in enumerate(("", "   ")):
            with self.subTest(physical_host=physical_host):
                cfg = {
                    **self.cfg,
                    "node": f"review-empty-host-{index}",
                }
                with open(self.config_path, "w", encoding="utf-8") as stream:
                    json.dump(cfg, stream)
                self._clear_state_caches()
                dispatcher = Dispatcher(cfg, fake=True)
                self.addCleanup(dispatcher.log.close)
                with mock.patch(
                    "socket.gethostname",
                    return_value=physical_host,
                ), mock.patch.object(
                    dispatcher,
                    "_proc_start_time",
                    return_value="proc:lease",
                ):
                    try:
                        acquired = dispatcher.acquire_lock()
                    except (OSError, ValueError, state.StateError):
                        acquired = False

                self.assertFalse(acquired)
                self.assertFalse(os.path.lexists(dispatcher._lock_owner_file()))

    def test_stop_rejects_foreign_or_unattributed_lease_before_pid_probe(
        self,
    ) -> None:
        os.makedirs(os.path.dirname(daemon._owner_file()), exist_ok=True)
        for physical_host in ("remote-host", None, "", "   "):
            with self.subTest(physical_host=physical_host):
                owner = {
                    "schema_version": 1,
                    "lease_id": f"lease-{physical_host or 'missing'}",
                    "pid": 4242,
                    "start_token": "proc:lease",
                }
                if physical_host is not None:
                    owner["physical_host"] = physical_host
                with open(daemon._owner_file(), "w", encoding="utf-8") as stream:
                    json.dump(owner, stream)

                with mock.patch(
                    "socket.gethostname",
                    return_value="local-host",
                ), mock.patch.object(
                    daemon,
                    "_pid_alive",
                    side_effect=AssertionError("foreign PID must not be probed"),
                ) as pid_probe, mock.patch.object(
                    daemon,
                    "process_start_token",
                    side_effect=AssertionError("foreign PID identity must not be probed"),
                ) as identity_probe, mock.patch.object(
                    daemon.os,
                    "kill",
                    side_effect=AssertionError("foreign PID must not be signalled"),
                ) as signal:
                    message = daemon.stop()

                self.assertIn("拒绝", message)
                self.assertTrue(os.path.lexists(daemon._owner_file()))
                pid_probe.assert_not_called()
                identity_probe.assert_not_called()
                signal.assert_not_called()

    def test_local_physical_host_keeps_exact_stop_identity_checks(self) -> None:
        owner = {
            "schema_version": 1,
            "lease_id": "lease-local",
            "pid": 4242,
            "start_token": "proc:lease",
            "physical_host": "local-host",
        }
        os.makedirs(os.path.dirname(daemon._owner_file()), exist_ok=True)
        with open(daemon._owner_file(), "w", encoding="utf-8") as stream:
            json.dump(owner, stream)

        with mock.patch("socket.gethostname", return_value="local-host"), mock.patch.object(
            daemon,
            "_pid_alive",
            return_value=True,
        ), mock.patch.object(
            daemon,
            "process_start_token",
            return_value="proc:lease",
        ), mock.patch.object(
            daemon.os,
            "kill",
            side_effect=PermissionError("test refusal"),
        ) as signal:
            message = daemon.stop()

        self.assertIn("SIGTERM 发送失败", message)
        signal.assert_called_once_with(4242, daemon.signal.SIGTERM)

    def test_stop_rejects_symlink_lease_directory_before_pid_probe(self) -> None:
        owner = {
            "schema_version": 1,
            "lease_id": "lease-symlink",
            "pid": 4242,
            "start_token": "proc:lease",
            "physical_host": "local-host",
        }
        lock_dir = os.path.dirname(daemon._owner_file())
        os.makedirs(os.path.dirname(lock_dir), exist_ok=True)
        redirected = os.path.join(self.tmp.name, "redirected-lease")
        os.makedirs(redirected, mode=0o700)
        os.chmod(redirected, 0o700)
        with open(
            os.path.join(redirected, "owner.json"),
            "w",
            encoding="utf-8",
        ) as stream:
            json.dump(owner, stream)
        os.symlink(redirected, lock_dir)

        with mock.patch(
            "socket.gethostname",
            return_value="local-host",
        ), mock.patch.object(
            daemon,
            "_pid_alive",
            side_effect=AssertionError("symlink lease PID must not be probed"),
        ) as pid_probe, mock.patch.object(
            daemon,
            "process_start_token",
            side_effect=AssertionError("symlink lease identity must not be read"),
        ) as identity_probe, mock.patch.object(
            daemon.os,
            "kill",
            side_effect=AssertionError("symlink lease PID must not be signalled"),
        ) as signal:
            message = daemon.stop()

        self.assertIn("拒绝", message)
        self.assertTrue(os.path.islink(lock_dir))
        pid_probe.assert_not_called()
        identity_probe.assert_not_called()
        signal.assert_not_called()

    def test_stop_rejects_symlink_host_directory_before_pid_probe(self) -> None:
        owner = {
            "schema_version": 1,
            "lease_id": "lease-symlink-host",
            "pid": 4242,
            "start_token": "proc:lease",
            "physical_host": "local-host",
        }
        host_dir = daemon._host_dir()
        original_host = os.path.join(self.tmp.name, "original-host")
        # Keep the redirect inside state_dir so state.host_dir()'s containment
        # check passes; _read_lease_owner itself must still reject the symlink.
        redirected_host = os.path.join(self.state_root, "redirected-host")
        os.rename(host_dir, original_host)
        lock_dir = os.path.join(redirected_host, "dispatcher.lock")
        os.makedirs(lock_dir, mode=0o700)
        os.chmod(redirected_host, 0o700)
        os.chmod(lock_dir, 0o700)
        with open(
            os.path.join(lock_dir, "owner.json"),
            "w",
            encoding="utf-8",
        ) as stream:
            json.dump(owner, stream)
        os.symlink(redirected_host, host_dir)

        with mock.patch(
            "socket.gethostname",
            return_value="local-host",
        ), mock.patch.object(
            daemon,
            "_pid_alive",
            side_effect=AssertionError("symlink host PID must not be probed"),
        ) as pid_probe, mock.patch.object(
            daemon,
            "process_start_token",
            side_effect=AssertionError("symlink host identity must not be read"),
        ) as identity_probe, mock.patch.object(
            daemon.os,
            "kill",
            side_effect=AssertionError("symlink host PID must not be signalled"),
        ) as signal:
            message = daemon.stop()

        self.assertIn("拒绝", message)
        self.assertTrue(os.path.islink(host_dir))
        pid_probe.assert_not_called()
        identity_probe.assert_not_called()
        signal.assert_not_called()

    def test_stop_rejects_writable_owner_before_pid_probe(self) -> None:
        owner = {
            "schema_version": 1,
            "lease_id": "lease-writable-owner",
            "pid": 4242,
            "start_token": "proc:lease",
            "physical_host": "local-host",
        }
        os.makedirs(os.path.dirname(daemon._owner_file()), mode=0o700)
        with open(daemon._owner_file(), "w", encoding="utf-8") as stream:
            json.dump(owner, stream)
        for mode in (0o620, 0o602):
            with self.subTest(mode=oct(mode)):
                os.chmod(daemon._owner_file(), mode)
                with mock.patch(
                    "socket.gethostname",
                    return_value="local-host",
                ), mock.patch.object(
                    daemon,
                    "_pid_alive",
                    side_effect=AssertionError(
                        "writable owner PID must not be probed"
                    ),
                ) as pid_probe, mock.patch.object(
                    daemon,
                    "process_start_token",
                    side_effect=AssertionError(
                        "writable owner identity must not be read"
                    ),
                ) as identity_probe, mock.patch.object(
                    daemon.os,
                    "kill",
                    side_effect=AssertionError(
                        "writable owner PID must not be signalled"
                    ),
                ) as signal:
                    message = daemon.stop()

                self.assertIn("拒绝", message)
                self.assertTrue(os.path.exists(daemon._owner_file()))
                pid_probe.assert_not_called()
                identity_probe.assert_not_called()
                signal.assert_not_called()

    def test_daemon_owner_reader_closes_outer_fds_after_open_failures(self) -> None:
        os.makedirs(os.path.dirname(daemon._owner_file()), mode=0o700)
        with open(daemon._owner_file(), "w", encoding="utf-8") as stream:
            json.dump(
                {
                    "schema_version": 1,
                    "lease_id": "lease-fd-open",
                    "pid": 4242,
                    "start_token": "proc:lease",
                    "physical_host": "local-host",
                },
                stream,
            )
        real_open = os.open
        for fail_call in (2, 3):
            with self.subTest(fail_call=fail_call):
                opened = []

                def failing_open(path, flags, *args, **kwargs):
                    if len(opened) + 1 == fail_call:
                        raise OSError("injected open failure")
                    fd = real_open(path, flags, *args, **kwargs)
                    opened.append(fd)
                    return fd

                with mock.patch.object(daemon.os, "open", side_effect=failing_open):
                    self.assertIsNone(daemon._read_lease_owner())
                for fd in opened:
                    with self.assertRaises(OSError):
                        os.fstat(fd)

    def test_daemon_owner_reader_closes_outer_fds_after_close_failure(self) -> None:
        os.makedirs(os.path.dirname(daemon._owner_file()), mode=0o700)
        with open(daemon._owner_file(), "w", encoding="utf-8") as stream:
            json.dump(
                {
                    "schema_version": 1,
                    "lease_id": "lease-fd-close",
                    "pid": 4242,
                    "start_token": "proc:lease",
                    "physical_host": "local-host",
                },
                stream,
            )
        real_close = os.close
        closed = []

        def close_then_fail_once(fd):
            real_close(fd)
            closed.append(fd)
            if len(closed) == 1:
                raise OSError("injected close failure")

        with mock.patch.object(daemon.os, "close", side_effect=close_then_fail_once):
            self.assertIsNone(daemon._read_lease_owner())

        self.assertEqual(3, len(closed))
        for fd in closed:
            with self.assertRaises(OSError):
                os.fstat(fd)

    def test_fifo_launch_marker_is_bounded_and_fails_closed(self) -> None:
        child_state = os.path.join(self.tmp.name, "fifo-state")
        child_config = os.path.join(self.tmp.name, "fifo-config.json")
        child_cfg = {
            **self.cfg,
            "node": socket.gethostname(),
            "state_dir": child_state,
        }
        with open(child_config, "w", encoding="utf-8") as stream:
            json.dump(child_cfg, stream)
        child_env = {
            **os.environ,
            "SCHED_STATE": child_state,
            "SCHED_CONFIG": child_config,
            "SCHED_ALLOW_FOREIGN_WRITE": "1",
        }
        script = (
            "import os\n"
            "from gsched import state\n"
            "path = state.launch_marker_path('fifo-job')\n"
            "os.makedirs(os.path.dirname(path), exist_ok=True)\n"
            "os.mkfifo(path)\n"
            "print('ready', flush=True)\n"
            "print(state.launch_marker_active('fifo-job'))\n"
        )
        process = subprocess.Popen(
            [sys.executable, "-c", script],
            cwd=os.path.dirname(os.path.dirname(__file__)),
            env=child_env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.addCleanup(
            lambda: process.kill() if process.poll() is None else None
        )
        self.assertIsNotNone(process.stdout)
        ready, _writable, _errors = select.select([process.stdout], [], [], 10)
        if not ready:
            process.kill()
            process.communicate()
            self.fail("FIFO launch marker child did not become ready")
        self.assertEqual("ready", process.stdout.readline().strip())

        try:
            stdout, stderr = process.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            self.fail("FIFO launch marker inspection blocked")

        self.assertEqual(0, process.returncode, stderr)
        self.assertEqual("True", stdout.strip())

    def test_symlink_launch_marker_fails_closed_without_process_probe(self) -> None:
        marker = state.launch_marker_path("symlink-job")
        os.makedirs(os.path.dirname(marker), exist_ok=True)
        target = os.path.join(self.tmp.name, "foreign-launch-marker")
        with open(target, "w", encoding="utf-8") as stream:
            stream.write(f"{os.getpid()} proc:lease\n")
        os.symlink(target, marker)

        with mock.patch("socket.gethostname", return_value="review-node"), mock.patch.object(
            state,
            "_launch_process_start",
            side_effect=AssertionError("symlink marker must not probe a PID"),
        ) as identity_probe, mock.patch.object(
            state.os,
            "killpg",
            side_effect=AssertionError("symlink marker must not probe a process group"),
        ) as pid_probe:
            active = state.launch_marker_active("symlink-job")

        self.assertTrue(active)
        self.assertTrue(os.path.islink(marker))
        identity_probe.assert_not_called()
        pid_probe.assert_not_called()

    def test_oversized_launch_marker_fails_closed_without_unbounded_read_or_probe(
        self,
    ) -> None:
        marker = state.launch_marker_path("oversized-job")
        os.makedirs(os.path.dirname(marker), exist_ok=True)
        with open(marker, "w", encoding="utf-8") as stream:
            stream.write(f"{os.getpid()} proc:{'x' * 8192}\n")
        real_open = builtins.open

        class BoundedMarkerStream:
            def __init__(self, stream):
                self.stream = stream

            def __enter__(self):
                self.stream.__enter__()
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                return self.stream.__exit__(exc_type, exc_value, traceback)

            def __getattr__(self, name):
                return getattr(self.stream, name)

            def read(self, size: int = -1):
                if size < 0 or size > 4097:
                    raise AssertionError("launch marker read was not bounded")
                return self.stream.read(size)

        def guarded_open(path, *args, **kwargs):
            stream = real_open(path, *args, **kwargs)
            if (
                isinstance(path, (str, os.PathLike))
                and os.path.abspath(os.fspath(path)) == os.path.abspath(marker)
            ):
                return BoundedMarkerStream(stream)
            return stream

        with mock.patch("socket.gethostname", return_value="review-node"), mock.patch(
            "builtins.open",
            side_effect=guarded_open,
        ), mock.patch.object(
            state,
            "_launch_process_start",
            side_effect=AssertionError("oversized marker must not probe a PID"),
        ) as identity_probe, mock.patch.object(
            state.os,
            "killpg",
            side_effect=AssertionError("oversized marker must not probe a process group"),
        ) as pid_probe:
            active = state.launch_marker_active("oversized-job")

        self.assertTrue(active)
        self.assertGreater(os.path.getsize(marker), 4096)
        identity_probe.assert_not_called()
        pid_probe.assert_not_called()

    def test_shutdown_marker_replacement_survives_old_token_clear(self) -> None:
        old_token = state.mark_idle_shutdown()
        compare_started = threading.Event()
        replacement_done = threading.Event()
        replacement: dict[str, str] = {}
        real_compare = state.secrets.compare_digest

        def replace_marker() -> None:
            self.assertTrue(compare_started.wait(timeout=2))
            replacement["token"] = state.mark_idle_shutdown()
            replacement_done.set()

        def pause_after_read(current: str, expected: str) -> bool:
            compare_started.set()
            replacement_done.wait(timeout=0.5)
            return real_compare(current, expected)

        thread = threading.Thread(target=replace_marker)
        thread.start()
        with mock.patch.object(state.secrets, "compare_digest", side_effect=pause_after_read):
            self.assertTrue(state.clear_idle_shutdown(old_token))
        thread.join(timeout=2)

        self.assertFalse(thread.is_alive())
        self.assertTrue(replacement_done.is_set())
        with open(state.submission_shutdown_marker(), encoding="utf-8") as stream:
            self.assertEqual(replacement["token"], stream.readline().strip())

    def test_shutdown_marker_helpers_are_neutral_when_lock_is_nested(self) -> None:
        with state.submission_lock():
            token = state.mark_idle_shutdown()
            self.assertEqual(1, state._submission_lock_depth.get())
            self.assertTrue(state.clear_idle_shutdown(token))
            self.assertEqual(1, state._submission_lock_depth.get())
        self.assertEqual(0, state._submission_lock_depth.get())

    def test_daemon_cleanup_preserves_successor_sidecars_after_owner_aba(
        self,
    ) -> None:
        observed = {
            "schema_version": 1,
            "lease_id": "old",
            "pid": 101,
            "start_token": "proc:old",
            "physical_host": socket.gethostname(),
        }
        successor = {
            "schema_version": 1,
            "lease_id": "new",
            "pid": 202,
            "start_token": "proc:new",
            "physical_host": socket.gethostname(),
        }
        os.makedirs(os.path.dirname(daemon._owner_file()), exist_ok=True)
        with open(daemon._owner_file(), "w", encoding="utf-8") as stream:
            json.dump(successor, stream)
        with open(daemon._pid_file(), "w", encoding="utf-8") as stream:
            stream.write("202\n")
        with open(daemon._heartbeat_file(), "w", encoding="utf-8") as stream:
            stream.write("successor\n")

        self.assertFalse(daemon._cleanup(observed))

        with open(daemon._owner_file(), encoding="utf-8") as stream:
            self.assertEqual(successor, json.load(stream))
        with open(daemon._pid_file(), encoding="utf-8") as stream:
            self.assertEqual("202", stream.read().strip())
        with open(daemon._heartbeat_file(), encoding="utf-8") as stream:
            self.assertEqual("successor", stream.read().strip())

    def test_daemon_cleanup_accepts_completed_graceful_self_cleanup(self) -> None:
        observed = {
            "schema_version": 1,
            "lease_id": "graceful",
            "pid": 101,
            "start_token": "proc:old",
            "physical_host": socket.gethostname(),
        }
        os.makedirs(daemon._host_dir(), exist_ok=True)
        with open(daemon._pid_file(), "w", encoding="utf-8") as stream:
            stream.write("101\n")
        with open(daemon._heartbeat_file(), "w", encoding="utf-8") as stream:
            stream.write("old\n")

        self.assertFalse(os.path.lexists(daemon._owner_file()))
        self.assertFalse(os.path.lexists(os.path.dirname(daemon._owner_file())))
        self.assertTrue(daemon._cleanup(observed))
        self.assertFalse(os.path.lexists(daemon._pid_file()))
        self.assertFalse(os.path.lexists(daemon._heartbeat_file()))
        self.assertTrue(daemon._cleanup(observed))

    def test_daemon_cleanup_rejects_incomplete_ownerless_lock(self) -> None:
        observed = {
            "schema_version": 1,
            "lease_id": "graceful",
            "pid": 101,
            "start_token": "proc:old",
            "physical_host": socket.gethostname(),
        }
        os.makedirs(os.path.dirname(daemon._owner_file()), exist_ok=True)
        with open(daemon._pid_file(), "w", encoding="utf-8") as stream:
            stream.write("101\n")
        with open(daemon._heartbeat_file(), "w", encoding="utf-8") as stream:
            stream.write("old\n")

        self.assertFalse(daemon._cleanup(observed))
        self.assertTrue(os.path.isdir(os.path.dirname(daemon._owner_file())))
        self.assertTrue(os.path.isfile(daemon._pid_file()))
        self.assertTrue(os.path.isfile(daemon._heartbeat_file()))

    def test_daemon_cleanup_rejects_invalid_owner_and_symlink_lock(self) -> None:
        observed = {
            "schema_version": 1,
            "lease_id": "graceful",
            "pid": 101,
            "start_token": "proc:old",
            "physical_host": socket.gethostname(),
        }
        lock_dir = os.path.dirname(daemon._owner_file())
        os.makedirs(lock_dir, exist_ok=True)
        with open(daemon._owner_file(), "w", encoding="utf-8") as stream:
            stream.write("{invalid")
        with open(daemon._pid_file(), "w", encoding="utf-8") as stream:
            stream.write("101\n")
        with open(daemon._heartbeat_file(), "w", encoding="utf-8") as stream:
            stream.write("old\n")

        self.assertFalse(daemon._cleanup(observed))
        self.assertTrue(os.path.isfile(daemon._pid_file()))
        self.assertTrue(os.path.isfile(daemon._heartbeat_file()))

        os.unlink(daemon._owner_file())
        os.rmdir(lock_dir)
        redirected_lock = os.path.join(daemon._host_dir(), "redirected.lock")
        os.makedirs(redirected_lock)
        with open(
            os.path.join(redirected_lock, "owner.json"),
            "w",
            encoding="utf-8",
        ) as stream:
            json.dump(observed, stream)
        os.symlink(redirected_lock, lock_dir)

        self.assertFalse(daemon._cleanup(observed))
        self.assertTrue(os.path.islink(lock_dir))
        self.assertTrue(os.path.isfile(daemon._pid_file()))
        self.assertTrue(os.path.isfile(daemon._heartbeat_file()))

    def test_daemon_cleanup_exact_owner_removes_only_sidecars(self) -> None:
        observed = {
            "schema_version": 1,
            "lease_id": "exact",
            "pid": 101,
            "start_token": "proc:old",
            "physical_host": socket.gethostname(),
        }
        lock_dir = os.path.dirname(daemon._owner_file())
        os.makedirs(lock_dir, exist_ok=True)
        with open(daemon._owner_file(), "w", encoding="utf-8") as stream:
            json.dump(observed, stream)
        with open(daemon._pid_file(), "w", encoding="utf-8") as stream:
            stream.write("101\n")
        with open(daemon._heartbeat_file(), "w", encoding="utf-8") as stream:
            stream.write("old\n")

        self.assertTrue(daemon._cleanup(observed))

        with open(daemon._owner_file(), encoding="utf-8") as stream:
            self.assertEqual(observed, json.load(stream))
        self.assertTrue(os.path.isdir(lock_dir))
        self.assertFalse(os.path.lexists(daemon._pid_file()))
        self.assertFalse(os.path.lexists(daemon._heartbeat_file()))

    def test_daemon_cleanup_fails_closed_on_sidecar_or_guard_error(self) -> None:
        observed = {
            "schema_version": 1,
            "lease_id": "exact",
            "pid": 101,
            "start_token": "proc:old",
            "physical_host": socket.gethostname(),
        }
        lock_dir = os.path.dirname(daemon._owner_file())
        os.makedirs(lock_dir, exist_ok=True)
        with open(daemon._owner_file(), "w", encoding="utf-8") as stream:
            json.dump(observed, stream)
        with open(daemon._heartbeat_file(), "w", encoding="utf-8") as stream:
            stream.write("old\n")
        real_unlink = os.unlink

        def reject_heartbeat(path: str) -> None:
            if os.fspath(path) == daemon._heartbeat_file():
                raise PermissionError("review unlink refusal")
            real_unlink(path)

        with mock.patch.object(
            daemon.os,
            "unlink",
            side_effect=reject_heartbeat,
        ):
            self.assertFalse(daemon._cleanup(observed))

        with open(daemon._owner_file(), encoding="utf-8") as stream:
            self.assertEqual(observed, json.load(stream))
        self.assertTrue(os.path.isfile(daemon._heartbeat_file()))

        with mock.patch.object(
            state,
            "open_private_text",
            side_effect=state.StateError("review guard refusal"),
        ):
            self.assertFalse(daemon._cleanup(observed))

    def test_expected_owner_cleanup_serializes_with_self_cleanup(self) -> None:
        observed = {
            "schema_version": 1,
            "lease_id": "graceful",
            "pid": 101,
            "start_token": "proc:old",
            "physical_host": socket.gethostname(),
        }
        lock_dir = os.path.dirname(daemon._owner_file())
        os.makedirs(lock_dir, exist_ok=True)
        with open(daemon._owner_file(), "w", encoding="utf-8") as stream:
            json.dump(observed, stream)
        with open(daemon._pid_file(), "w", encoding="utf-8") as stream:
            stream.write("101\n")
        with open(daemon._heartbeat_file(), "w", encoding="utf-8") as stream:
            stream.write("old\n")
        guard_path = f"{lock_dir}.guard"
        started = threading.Event()
        results: list[bool] = []

        def cleanup_observed() -> None:
            started.set()
            results.append(daemon._cleanup(observed))

        with state.open_private_text(guard_path, "a+") as guard:
            fcntl.flock(guard.fileno(), fcntl.LOCK_EX)
            thread = threading.Thread(target=cleanup_observed)
            thread.start()
            self.assertTrue(started.wait(timeout=2))
            self.assertTrue(thread.is_alive())
            os.unlink(daemon._owner_file())
            os.rmdir(lock_dir)
            os.unlink(daemon._pid_file())
            os.unlink(daemon._heartbeat_file())
            fcntl.flock(guard.fileno(), fcntl.LOCK_UN)
        thread.join(timeout=2)

        self.assertFalse(thread.is_alive())
        self.assertEqual([True], results)

    def test_stop_accepts_dispatcher_graceful_self_cleanup(self) -> None:
        observed = {
            "schema_version": 1,
            "lease_id": "graceful",
            "pid": 101,
            "start_token": "proc:old",
            "physical_host": socket.gethostname(),
        }
        lock_dir = os.path.dirname(daemon._owner_file())
        os.makedirs(lock_dir, exist_ok=True)
        with open(daemon._owner_file(), "w", encoding="utf-8") as stream:
            json.dump(observed, stream)
        with open(daemon._pid_file(), "w", encoding="utf-8") as stream:
            stream.write("101\n")
        with open(daemon._heartbeat_file(), "w", encoding="utf-8") as stream:
            stream.write("old\n")

        identity_calls = 0

        def process_identity(_pid: int) -> str | None:
            nonlocal identity_calls
            identity_calls += 1
            if identity_calls < 3:
                return observed["start_token"]
            os.unlink(daemon._owner_file())
            os.rmdir(lock_dir)
            os.unlink(daemon._pid_file())
            os.unlink(daemon._heartbeat_file())
            return None

        alive = iter((True, False))
        with mock.patch.object(
            daemon,
            "_pid_alive",
            side_effect=lambda _pid: next(alive),
        ), mock.patch.object(
            daemon,
            "process_start_token",
            side_effect=process_identity,
        ), mock.patch.object(
            daemon.os,
            "kill",
        ) as signal, mock.patch.object(
            daemon.time,
            "sleep",
            return_value=None,
        ), mock.patch.object(
            state,
            "submission_lock",
            return_value=contextlib.nullcontext(),
        ), mock.patch.object(
            state,
            "mark_idle_shutdown",
            return_value="stop-token",
        ):
            rc, stdout, stderr = self.capture(
                cli.cmd_daemon,
                argparse.Namespace(action="stop"),
            )

        self.assertEqual(0, rc, stderr)
        self.assertEqual("daemon 已停止 (pid=101)\n", stdout)
        self.assertEqual("", stderr)
        signal.assert_called_once_with(101, daemon.signal.SIGTERM)

    def test_stop_refuses_success_when_sidecar_cleanup_loses_lease(self) -> None:
        observed = {
            "schema_version": 1,
            "lease_id": "old",
            "pid": 101,
            "start_token": "proc:old",
            "physical_host": socket.gethostname(),
        }
        with self.subTest(case="dead-owner-replaced"):
            with mock.patch.object(
                daemon, "_read_lease_owner", return_value=observed
            ), mock.patch.object(
                daemon, "_pid_alive", return_value=False
            ), mock.patch.object(
                daemon, "_heartbeat_fresh", return_value=False
            ), mock.patch.object(
                daemon, "_cleanup", return_value=False
            ):
                text = daemon.stop()
            self.assertIn("拒绝", text)
            self.assertIn("状态", text)

        with self.subTest(case="ownerless-replaced"):
            with mock.patch.object(
                daemon, "_read_lease_owner", return_value=None
            ), mock.patch.object(
                daemon, "_heartbeat_fresh", return_value=False
            ), mock.patch.object(
                daemon, "_cleanup", return_value=False
            ):
                text = daemon.stop()
            self.assertIn("拒绝", text)
            self.assertIn("状态", text)

    def test_ownerless_cleanup_is_serialized_with_successor_publish(self) -> None:
        successor = {
            "schema_version": 1,
            "lease_id": "successor",
            "pid": 303,
            "start_token": "proc:successor",
            "physical_host": socket.gethostname(),
        }
        os.makedirs(daemon._host_dir(), exist_ok=True)
        with open(daemon._pid_file(), "w", encoding="utf-8") as stream:
            stream.write("old\n")
        with open(daemon._heartbeat_file(), "w", encoding="utf-8") as stream:
            stream.write("old\n")
        guard_path = f"{os.path.dirname(daemon._owner_file())}.guard"
        started = threading.Event()
        errors: list[BaseException] = []
        results: list[bool] = []

        def cleanup_ownerless() -> None:
            started.set()
            try:
                results.append(daemon._cleanup(None))
            except BaseException as error:
                errors.append(error)

        with state.open_private_text(guard_path, "a+") as guard:
            fcntl.flock(guard.fileno(), fcntl.LOCK_EX)
            thread = threading.Thread(target=cleanup_ownerless)
            thread.start()
            self.assertTrue(started.wait(timeout=2))
            os.makedirs(os.path.dirname(daemon._owner_file()), exist_ok=True)
            with open(daemon._owner_file(), "w", encoding="utf-8") as stream:
                json.dump(successor, stream)
            with open(daemon._pid_file(), "w", encoding="utf-8") as stream:
                stream.write("303\n")
            with open(daemon._heartbeat_file(), "w", encoding="utf-8") as stream:
                stream.write("successor\n")
            fcntl.flock(guard.fileno(), fcntl.LOCK_UN)
        thread.join(timeout=2)

        self.assertFalse(thread.is_alive())
        self.assertEqual([], errors)
        self.assertEqual([False], results)
        with open(daemon._pid_file(), encoding="utf-8") as stream:
            self.assertEqual("303", stream.read().strip())
        with open(daemon._heartbeat_file(), encoding="utf-8") as stream:
            self.assertEqual("successor", stream.read().strip())


class ReviewLegacyJobStateTests(TempStateCase):
    def test_init_db_atomically_normalizes_legacy_waiting_statuses(self) -> None:
        jobs = {
            self.seed_batch(
                batch_id="legacy-quota",
                name="legacy-quota",
                job_status="pending",
            ): "waiting_quota",
            self.seed_batch(
                batch_id="legacy-dep",
                name="legacy-dep",
                job_status="pending",
            ): "waiting_dep",
        }
        with state.connect() as conn:
            for job_id, legacy_status in jobs.items():
                conn.execute(
                    "UPDATE jobs SET status=? WHERE id=?",
                    (legacy_status, job_id),
                )

        state.init_db()

        with state.connect() as conn:
            statuses = {
                row["id"]: row["status"]
                for row in conn.execute(
                    "SELECT id, status FROM jobs WHERE id IN (?, ?)",
                    tuple(jobs),
                ).fetchall()
            }
        self.assertEqual({job_id: "pending" for job_id in jobs}, statuses)

    def test_cancel_defensively_accepts_each_legacy_pending_status(self) -> None:
        job_id = self.seed_batch(job_status="pending")
        args = argparse.Namespace(
            batch=None,
            yes=True,
            project=None,
            bulk_project=None,
        )
        cases = (
            ("waiting_quota", "batch-20260829-000000"),
            ("waiting_dep", "batch:task"),
        )
        for legacy_status, reference in cases:
            args.batch = reference
            with self.subTest(status=legacy_status):
                with state.connect() as conn:
                    conn.execute(
                        "UPDATE jobs SET status=?, kill_reason=NULL, finished_at=NULL"
                        " WHERE id=?",
                        (legacy_status, job_id),
                    )

                rc, stdout, stderr = self.capture(cli.cmd_cancel, args)

                self.assertEqual(0, rc, stderr)
                self.assertIn(f"已取消排队任务 {job_id}", stdout)
                with state.connect() as conn:
                    row = state.get_job(conn, job_id)
                self.assertEqual("cancelled", row["status"])




class ReviewResubmitStateTests(TempStateCase):
    def _insert_newer_same_name_batch(self, status: str) -> str:
        newer_batch = "batch-20260830-000000"
        with state.connect() as conn:
            source = conn.execute(
                "SELECT spec FROM tasks WHERE batch_id=? AND id='task' AND version=1",
                ("batch-20260829-000000",),
            ).fetchone()
            state.insert_batch(
                conn,
                newer_batch,
                "batch",
                "mix",
                [],
                None,
                self.tmp.name,
                {},
                project="p",
            )
            conn.execute(
                "UPDATE batches SET status=? WHERE id=?",
                (status, newer_batch),
            )
            state.insert_task(
                conn,
                newer_batch,
                "task",
                1,
                json.loads(source["spec"]),
                0,
                "p",
            )
            state.insert_job(
                conn,
                f"{newer_batch}-task-v1",
                newer_batch,
                "task",
                1,
                "newer-fp",
                None,
                "p",
            )
            state.update_job(
                conn,
                f"{newer_batch}-task-v1",
                status="done" if status == "done" else "failed",
            )
        return newer_batch

    def _resubmit_done(self):
        self.seed_batch(batch_status="done", job_status="done")
        marker_dir = os.path.join(self.state_root, "review-node", "markers")
        os.makedirs(marker_dir, exist_ok=True)
        done_marker = os.path.join(marker_dir, "batch.done")
        blocked_marker = os.path.join(marker_dir, "batch.blocked")
        for marker in (done_marker, blocked_marker):
            with open(marker, "w", encoding="utf-8") as stream:
                stream.write("old terminal state")
        args = argparse.Namespace(
            task="batch", failed=False, resubmit_all=True, dry_run=False
        )
        with mock.patch.object(cli, "_load_cfg", return_value=self.cfg), mock.patch(
            "gsched.fingerprint.compute_fingerprint",
            return_value=("new-fp", None, None),
        ), mock.patch.object(state, "launch_marker_active", return_value=False), mock.patch.object(
            cli, "_ensure_running_locked", return_value="awake"
        ):
            rc, stdout, stderr = self.capture(cli.cmd_resubmit, args)
        return rc, stdout, stderr, done_marker, blocked_marker

    def test_s_h03_done_batch_reopens_before_resubmitted_version_runs(self) -> None:
        rc, _stdout, stderr, done_marker, blocked_marker = self._resubmit_done()
        self.assertEqual(0, rc, stderr)
        with state.connect() as conn:
            batch = state.get_batch(conn, "batch-20260829-000000")
            latest = conn.execute(
                "SELECT status FROM jobs WHERE batch_id=? ORDER BY version DESC LIMIT 1",
                (batch["id"],),
            ).fetchone()
        self.assertEqual("active", batch["status"])
        self.assertEqual("pending", latest["status"])
        self.assertTrue(os.path.exists(done_marker))
        self.assertTrue(os.path.exists(blocked_marker))
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.host_dir = os.path.join(self.state_root, "review-node")
        with state.connect() as conn:
            ready = conn.execute(
                "SELECT j.*, b.name AS batch_name FROM jobs j"
                " JOIN batches b ON b.id=j.batch_id"
                " WHERE j.batch_id=? AND j.status='pending'",
                ("batch-20260829-000000",),
            ).fetchall()
            dispatcher._reconcile_ready_batch_markers(conn, ready)
        self.assertFalse(os.path.exists(done_marker))
        self.assertFalse(os.path.exists(blocked_marker))

    def test_s_h03_failed_resubmission_settles_blocked_and_notifies(self) -> None:
        rc, _stdout, stderr, _done_marker, _blocked_marker = self._resubmit_done()
        self.assertEqual(0, rc, stderr)
        with state.connect() as conn:
            latest = conn.execute(
                "SELECT id FROM jobs ORDER BY version DESC LIMIT 1"
            ).fetchone()
            state.update_job(
                conn,
                latest["id"],
                status="failed",
                failure="new failure",
                finished_at=state.now(),
            )
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = {**self.cfg, "notify": {"file": {"enabled": True}}}
        dispatcher.host_dir = os.path.join(self.state_root, "review-node")
        dispatcher._notify_threads = []
        dispatcher._write_marker = mock.Mock()
        dispatcher._remove_marker = mock.Mock()
        dispatcher._notify_batch = mock.Mock()
        dispatcher.log_line = mock.Mock()
        dispatcher._settle_batch_status()
        with state.connect() as conn:
            batch = state.get_batch(conn, "batch-20260829-000000")
        self.assertEqual("blocked", batch["status"])
        dispatcher._write_marker.assert_called_once()
        dispatcher._notify_batch.assert_called_once()

    def test_resubmit_ignores_obsolete_pending_but_still_creates_latest(self) -> None:
        old_job_id = self.seed_batch(batch_status="done", job_status="pending")
        with state.connect() as conn:
            task = conn.execute(
                "SELECT spec, order_idx FROM tasks"
                " WHERE batch_id=? AND id=? AND version=1",
                ("batch-20260829-000000", "task"),
            ).fetchone()
            state.insert_task(
                conn,
                "batch-20260829-000000",
                "task",
                2,
                json.loads(task["spec"]),
                task["order_idx"],
                "p",
            )
            state.insert_job(
                conn,
                "batch-20260829-000000-task-v2",
                "batch-20260829-000000",
                "task",
                2,
                "fp-2",
                None,
                "p",
            )
            state.update_job(
                conn,
                "batch-20260829-000000-task-v2",
                status="done",
            )
        args = argparse.Namespace(
            task="batch-20260829-000000:task",
            failed=False,
            resubmit_all=False,
            dry_run=False,
        )

        with mock.patch.object(
            cli, "_load_cfg", return_value=self.cfg
        ), mock.patch(
            "gsched.fingerprint.compute_fingerprint",
            return_value=("fp-3", None, None),
        ), mock.patch.object(
            state, "launch_marker_active", return_value=False
        ), mock.patch.object(
            cli, "_ensure_running_locked", return_value="awake"
        ):
            rc, _stdout, stderr = self.capture(cli.cmd_resubmit, args)

        self.assertEqual(0, rc, stderr)
        with state.connect() as conn:
            jobs = conn.execute(
                "SELECT id, version, status FROM jobs"
                " WHERE batch_id=? ORDER BY version",
                ("batch-20260829-000000",),
            ).fetchall()
            batch = state.get_batch(conn, "batch-20260829-000000")
        self.assertEqual(old_job_id, jobs[0]["id"])
        self.assertEqual(
            [(1, "pending"), (2, "done"), (3, "pending")],
            [(job["version"], job["status"]) for job in jobs],
        )
        self.assertEqual("active", batch["status"])

    def test_resubmit_rejects_launch_marker_on_obsolete_pending_version(self) -> None:
        old_job_id = self.seed_batch(batch_status="done", job_status="pending")
        with state.connect() as conn:
            task = conn.execute(
                "SELECT spec, order_idx FROM tasks"
                " WHERE batch_id=? AND id=? AND version=1",
                ("batch-20260829-000000", "task"),
            ).fetchone()
            state.insert_task(
                conn,
                "batch-20260829-000000",
                "task",
                2,
                json.loads(task["spec"]),
                task["order_idx"],
                "p",
            )
            state.insert_job(
                conn,
                "batch-20260829-000000-task-v2",
                "batch-20260829-000000",
                "task",
                2,
                "fp-2",
                None,
                "p",
            )
            state.update_job(
                conn,
                "batch-20260829-000000-task-v2",
                status="done",
            )
        args = argparse.Namespace(
            task="batch-20260829-000000:task",
            failed=False,
            resubmit_all=False,
            dry_run=False,
        )

        with mock.patch.object(
            cli, "_load_cfg", return_value=self.cfg
        ), mock.patch.object(
            state,
            "launch_marker_active",
            side_effect=lambda job_id: job_id == old_job_id,
        ), mock.patch.object(
            cli, "_ensure_running_locked"
        ) as ensure_running:
            rc, _stdout, stderr = self.capture(cli.cmd_resubmit, args)

        self.assertEqual(1, rc)
        self.assertIn("进程组终止尚未确认完成", stderr)
        self.assertIn("taskv1", stderr)
        with state.connect() as conn:
            jobs = conn.execute(
                "SELECT version, status FROM jobs WHERE batch_id=?"
                " ORDER BY version",
                ("batch-20260829-000000",),
            ).fetchall()
            batch = state.get_batch(conn, "batch-20260829-000000")
        self.assertEqual(
            [(1, "pending"), (2, "done")],
            [(job["version"], job["status"]) for job in jobs],
        )
        self.assertEqual("done", batch["status"])
        ensure_running.assert_not_called()

    def test_resubmit_old_same_name_batch_preserves_newer_done_marker(self) -> None:
        self.seed_batch(batch_status="done", job_status="done")
        self._insert_newer_same_name_batch("done")
        marker_dir = os.path.join(self.state_root, "review-node", "markers")
        os.makedirs(marker_dir, exist_ok=True)
        done_marker = os.path.join(marker_dir, "batch.done")
        with open(done_marker, "w", encoding="utf-8") as stream:
            stream.write("newer done")
        args = argparse.Namespace(
            task="batch-20260829-000000:task",
            failed=False,
            resubmit_all=False,
            dry_run=False,
        )

        with mock.patch.object(
            cli, "_load_cfg", return_value=self.cfg
        ), mock.patch(
            "gsched.fingerprint.compute_fingerprint",
            return_value=("resubmit-fp", None, None),
        ), mock.patch.object(
            state, "launch_marker_active", return_value=False
        ), mock.patch.object(
            cli, "_ensure_running_locked", return_value="awake"
        ):
            rc, _stdout, stderr = self.capture(cli.cmd_resubmit, args)

        self.assertEqual(0, rc, stderr)
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.host_dir = os.path.join(self.state_root, "review-node")
        with state.connect() as conn:
            old_batch = state.get_batch(conn, "batch-20260829-000000")
            ready = conn.execute(
                "SELECT j.*, b.name AS batch_name FROM jobs j"
                " JOIN batches b ON b.id=j.batch_id"
                " WHERE j.batch_id=? AND j.status='pending'",
                ("batch-20260829-000000",),
            ).fetchall()
            dispatcher._reconcile_ready_batch_markers(conn, ready)
        self.assertEqual("active", old_batch["status"])
        with open(done_marker, encoding="utf-8") as stream:
            self.assertEqual("newer done", stream.read())

    def test_retry_old_same_name_batch_preserves_newer_blocked_marker(self) -> None:
        self.seed_batch(batch_status="blocked", job_status="failed")
        self._insert_newer_same_name_batch("blocked")
        marker_dir = os.path.join(self.state_root, "review-node", "markers")
        os.makedirs(marker_dir, exist_ok=True)
        blocked_marker = os.path.join(marker_dir, "batch.blocked")
        with open(blocked_marker, "w", encoding="utf-8") as stream:
            stream.write("newer blocked")

        with mock.patch.object(
            state, "launch_marker_active", return_value=False
        ), mock.patch.object(
            cli, "_rev_diff_warn", return_value=None
        ), mock.patch.object(
            cli, "_ensure_running_locked", return_value="awake"
        ):
            rc, _stdout, stderr = self.capture(
                cli.cmd_retry,
                argparse.Namespace(task="batch-20260829-000000:task"),
            )

        self.assertEqual(0, rc, stderr)
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.host_dir = os.path.join(self.state_root, "review-node")
        with state.connect() as conn:
            old_batch = state.get_batch(conn, "batch-20260829-000000")
            ready = conn.execute(
                "SELECT j.*, b.name AS batch_name FROM jobs j"
                " JOIN batches b ON b.id=j.batch_id"
                " WHERE j.batch_id=? AND j.status='pending'",
                ("batch-20260829-000000",),
            ).fetchall()
            dispatcher._reconcile_ready_batch_markers(conn, ready)
        self.assertEqual("active", old_batch["status"])
        with open(blocked_marker, encoding="utf-8") as stream:
            self.assertEqual("newer blocked", stream.read())

    def test_retry_old_batch_rejects_newer_same_name_queued_owner(self) -> None:
        old_job_id = self.seed_batch(batch_status="blocked", job_status="failed")
        newer_batch_id = self._insert_newer_same_name_batch("queued")

        with mock.patch.object(
            state, "launch_marker_active", return_value=False
        ), mock.patch.object(
            cli, "_rev_diff_warn", return_value=None
        ), mock.patch.object(
            cli, "_ensure_running_locked"
        ) as ensure_running:
            rc, _stdout, stderr = self.capture(
                cli.cmd_retry,
                argparse.Namespace(task="batch-20260829-000000:task"),
            )

        self.assertEqual(1, rc)
        self.assertIn(newer_batch_id, stderr)
        self.assertIn("拒绝重开旧批次", stderr)
        with state.connect() as conn:
            old_batch = state.get_batch(conn, "batch-20260829-000000")
            old_job = state.get_job(conn, old_job_id)
        self.assertEqual("blocked", old_batch["status"])
        self.assertEqual("failed", old_job["status"])
        ensure_running.assert_not_called()

    def test_resubmit_old_batch_rejects_newer_same_name_active_owner(self) -> None:
        self.seed_batch(batch_status="done", job_status="done")
        newer_batch_id = self._insert_newer_same_name_batch("active")
        args = argparse.Namespace(
            task="batch-20260829-000000:task",
            failed=False,
            resubmit_all=False,
            dry_run=False,
        )

        with mock.patch.object(
            cli, "_load_cfg", return_value=self.cfg
        ), mock.patch(
            "gsched.fingerprint.compute_fingerprint",
            return_value=("resubmit-fp", None, None),
        ), mock.patch.object(
            state, "launch_marker_active", return_value=False
        ), mock.patch.object(
            cli, "_ensure_running_locked"
        ) as ensure_running:
            rc, _stdout, stderr = self.capture(cli.cmd_resubmit, args)

        self.assertEqual(1, rc)
        self.assertIn(newer_batch_id, stderr)
        self.assertIn("拒绝重开旧批次", stderr)
        with state.connect() as conn:
            old_batch = state.get_batch(conn, "batch-20260829-000000")
            old_versions = conn.execute(
                "SELECT version, status FROM jobs WHERE batch_id=?",
                ("batch-20260829-000000",),
            ).fetchall()
        self.assertEqual("done", old_batch["status"])
        self.assertEqual(
            [(1, "done")],
            [(row["version"], row["status"]) for row in old_versions],
        )
        ensure_running.assert_not_called()

    def test_resubmit_claims_writer_before_daemon_can_settle_active_batch(
        self,
    ) -> None:
        self.seed_batch(batch_status="active", job_status="done")
        args = argparse.Namespace(
            task="batch-20260829-000000:task",
            failed=False,
            resubmit_all=False,
            dry_run=False,
        )
        real_insert_task = state.insert_task
        contender_results: list[str] = []

        def insert_after_competing_settle(conn, *insert_args, **insert_kwargs):
            contender = sqlite3.connect(state.db_path(), timeout=0)
            try:
                contender.execute(
                    "UPDATE batches SET status='done' WHERE id=?",
                    ("batch-20260829-000000",),
                )
                contender.commit()
                contender_results.append("committed")
            except sqlite3.OperationalError as error:
                self.assertIn("locked", str(error).lower())
                contender_results.append("locked")
            finally:
                contender.rollback()
                contender.close()
            return real_insert_task(conn, *insert_args, **insert_kwargs)

        with mock.patch.object(
            cli, "_load_cfg", return_value=self.cfg
        ), mock.patch(
            "gsched.fingerprint.compute_fingerprint",
            return_value=("resubmit-fp", None, None),
        ), mock.patch.object(
            state, "launch_marker_active", return_value=False
        ), mock.patch.object(
            state,
            "insert_task",
            side_effect=insert_after_competing_settle,
        ), mock.patch.object(
            cli, "_ensure_running_locked", return_value="awake"
        ):
            rc, _stdout, stderr = self.capture(cli.cmd_resubmit, args)

        self.assertEqual(0, rc, stderr)
        self.assertEqual(["locked"], contender_results)
        with state.connect() as conn:
            batch = state.get_batch(conn, "batch-20260829-000000")
            latest = conn.execute(
                "SELECT status FROM jobs WHERE batch_id=?"
                " ORDER BY version DESC LIMIT 1",
                ("batch-20260829-000000",),
            ).fetchone()
        self.assertEqual("active", batch["status"])
        self.assertEqual("pending", latest["status"])

    def test_retry_claims_writer_before_daemon_can_block_active_batch(self) -> None:
        job_id = self.seed_batch(batch_status="active", job_status="failed")
        real_update_job = state.update_job
        contender_results: list[str] = []

        def update_after_competing_settle(conn, target_job_id, **fields):
            contender = sqlite3.connect(state.db_path(), timeout=0)
            try:
                contender.execute(
                    "UPDATE batches SET status='blocked' WHERE id=?",
                    ("batch-20260829-000000",),
                )
                contender.commit()
                contender_results.append("committed")
            except sqlite3.OperationalError as error:
                self.assertIn("locked", str(error).lower())
                contender_results.append("locked")
            finally:
                contender.rollback()
                contender.close()
            return real_update_job(conn, target_job_id, **fields)

        with mock.patch.object(
            state, "launch_marker_active", return_value=False
        ), mock.patch.object(
            cli, "_rev_diff_warn", return_value=None
        ), mock.patch.object(
            state,
            "update_job",
            side_effect=update_after_competing_settle,
        ), mock.patch.object(
            cli, "_ensure_running_locked", return_value="awake"
        ):
            rc, _stdout, stderr = self.capture(
                cli.cmd_retry,
                argparse.Namespace(task="batch-20260829-000000:task"),
            )

        self.assertEqual(0, rc, stderr)
        self.assertEqual(["locked"], contender_results)
        with state.connect() as conn:
            batch = state.get_batch(conn, "batch-20260829-000000")
            job = state.get_job(conn, job_id)
        self.assertEqual("active", batch["status"])
        self.assertEqual("pending", job["status"])

    def test_bound_resubmit_defers_wake_and_leaves_marker_for_daemon(
        self,
    ) -> None:
        self.seed_batch(batch_status="done", job_status="done")
        marker_dir = os.path.join(self.state_root, "review-node", "markers")
        os.makedirs(marker_dir, exist_ok=True)
        done_marker = os.path.join(marker_dir, "batch.done")
        with open(done_marker, "w", encoding="utf-8") as stream:
            stream.write("old terminal state")
        args = argparse.Namespace(
            request_id="req-bound-resubmit",
            command=["resubmit", "batch-20260829-000000:task"],
            expect_kind="task",
            expect_id="batch-20260829-000000:task",
            expect_status="done",
            expect_version=1,
            expect_quarantined=None,
            expect_revision=self.batch_revision(),
            expect_assignments_json=None,
        )
        wake_observations = []

        def observe_wake():
            with state.connect() as conn:
                ledger = conn.execute(
                    "SELECT status FROM operation_requests WHERE request_id=?",
                    (args.request_id,),
                ).fetchone()
            wake_observations.append(
                (ledger["status"], os.path.exists(done_marker))
            )
            return "awake"

        with mock.patch(
            "gsched.fingerprint.compute_fingerprint",
            return_value=("new-fp", None, None),
        ), mock.patch.object(
            state,
            "launch_marker_active",
            return_value=False,
        ), mock.patch.object(
            cli,
            "_ensure_running_locked",
            side_effect=observe_wake,
        ):
            rc, stdout, stderr = self.capture(cli.cmd_request, args)

        self.assertEqual(0, rc, stderr)
        self.assertIn("已 resubmit", stdout)
        self.assertEqual([("done", True)], wake_observations)
        self.assertTrue(os.path.exists(done_marker))


class ReviewForeignReadTests(TempStateCase):
    def _filesystem_snapshot(self):
        snapshot = {}
        for root, _dirs, files in os.walk(self.state_root):
            for filename in files:
                path = os.path.join(root, filename)
                result = os.stat(path)
                snapshot[os.path.relpath(path, self.state_root)] = (
                    result.st_size,
                    result.st_mtime_ns,
                )
        return snapshot

    def test_s_h04_foreign_queries_never_initialize_migrate_or_open_writable_sqlite(self) -> None:
        for argv in (["status", "--json"], ["history"], ["incidents", "--json"]):
            with self.subTest(argv=argv):
                before = self._filesystem_snapshot()
                calls = []
                real_connect = sqlite3.connect

                def recording_connect(database, *args, **kwargs):
                    calls.append((database, dict(kwargs)))
                    return real_connect(database, *args, **kwargs)

                with mock.patch.dict(os.environ, {"SCHED_ALLOW_FOREIGN_WRITE": ""}), mock.patch(
                    "socket.gethostname", return_value="login-node"
                ), mock.patch.object(state, "init_db") as init_db, mock.patch(
                    "sqlite3.connect", side_effect=recording_connect
                ), mock.patch.object(cli, "_daemon_health", return_value={}):
                    rc, _stdout, stderr = self.capture(cli.main, list(argv))
                self.assertEqual(0, rc, stderr)
                init_db.assert_not_called()
                self.assertTrue(calls)
                for database, kwargs in calls:
                    self.assertIn("mode=ro", str(database))
                    self.assertTrue(kwargs.get("uri"))
                self.assertEqual(before, self._filesystem_snapshot())

    def test_foreign_queries_report_missing_state_without_traceback_or_creation(
        self,
    ) -> None:
        database = state.db_path()
        for suffix in ("", "-wal", "-shm"):
            with contextlib.suppress(FileNotFoundError):
                os.unlink(database + suffix)
        before = self._filesystem_snapshot()

        for argv in (
            ["status", "--json"],
            ["history", "--json"],
            ["incidents", "--json"],
        ):
            with self.subTest(argv=argv), mock.patch.dict(
                os.environ,
                {"SCHED_ALLOW_FOREIGN_WRITE": ""},
            ), mock.patch(
                "socket.gethostname",
                return_value="login-node",
            ), mock.patch.object(
                state,
                "init_db",
            ) as init_db, mock.patch.object(
                cli,
                "_daemon_health",
                return_value={},
            ):
                rc, stdout, stderr = self.capture(cli.main, list(argv))

            self.assertEqual(1, rc)
            self.assertEqual("", stdout)
            self.assertIn("state database does not exist", stderr)
            self.assertNotIn("Traceback", stderr)
            init_db.assert_not_called()
            self.assertEqual(before, self._filesystem_snapshot())

    def test_read_only_connection_without_sidecars_uses_private_stable_snapshot(self) -> None:
        self.seed_batch()
        live_db = state.db_path()
        with contextlib.closing(sqlite3.connect(live_db)) as live:
            live.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        for suffix in ("-wal", "-shm"):
            with contextlib.suppress(FileNotFoundError):
                os.unlink(live_db + suffix)
            self.assertFalse(os.path.exists(live_db + suffix))

        previous_mode = state.read_only()
        state.set_read_only(True)
        try:
            with state.connect() as snapshot:
                main = next(
                    row
                    for row in snapshot.execute("PRAGMA database_list").fetchall()
                    if row["name"] == "main"
                )
                snapshot_db = main["file"]
                self.assertNotEqual(
                    os.path.realpath(live_db),
                    os.path.realpath(snapshot_db),
                )
                self.assertTrue(os.path.isfile(snapshot_db))
                before = snapshot.execute(
                    "SELECT name FROM batches WHERE id=?",
                    ("batch-20260829-000000",),
                ).fetchone()["name"]

                with contextlib.closing(sqlite3.connect(live_db)) as live:
                    live.execute(
                        "UPDATE batches SET name=? WHERE id=?",
                        ("changed-live", "batch-20260829-000000"),
                    )
                    live.commit()

                after = snapshot.execute(
                    "SELECT name FROM batches WHERE id=?",
                    ("batch-20260829-000000",),
                ).fetchone()["name"]
                self.assertEqual("batch", before)
                self.assertEqual(before, after)

            with contextlib.closing(sqlite3.connect(live_db)) as live:
                live_name = live.execute(
                    "SELECT name FROM batches WHERE id=?",
                    ("batch-20260829-000000",),
                ).fetchone()[0]
            self.assertEqual("changed-live", live_name)
        finally:
            state.set_read_only(previous_mode)



    def test_foreign_request_wrapper_cannot_bypass_write_guard(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"SCHED_ALLOW_FOREIGN_WRITE": ""},
        ), mock.patch(
            "socket.gethostname",
            return_value="login-node",
        ), mock.patch.object(
            state,
            "init_db",
        ) as init_db, mock.patch.object(
            cli,
            "cmd_request",
        ) as request:
            rc, _stdout, stderr = self.capture(
                cli.main,
                ["request", "req-foreign", "--expect-revision", "0", "--", "daemon", "start"],
            )

        self.assertEqual(2, rc)
        self.assertIn("写操作", stderr)
        init_db.assert_not_called()
        request.assert_not_called()


class ReviewForeignSubmitDurabilityTests(TempStateCase):
    def _batch_path(self) -> str:
        path = os.path.join(self.tmp.name, "foreign-submit.json")
        with open(path, "w", encoding="utf-8") as stream:
            json.dump(
                {
                    "name": "foreign-submit",
                    "project": "p",
                    "tasks": [
                        {
                            "id": "task",
                            "cmd": ["/bin/true"],
                            "resources": {"gpu": 0, "cpus": 1},
                        }
                    ],
                },
                stream,
            )
        return path

    def _submit_args(self) -> argparse.Namespace:
        return argparse.Namespace(batch=self._batch_path(), dry_run=False, json=False)

    def test_foreign_submit_fsyncs_temp_before_rename_and_directory_after(
        self,
    ) -> None:
        events: list[str] = []
        temp_fds: set[int] = set()
        inbox_fds: set[int] = set()
        real_open_private = state.open_private_text
        real_fsync = os.fsync
        real_open = os.open
        real_replace = os.replace
        inbox_dir = state.submission_inbox_dir()
        cfg = {
            **self.cfg,
            "task_default_env": {"PYTHONNOUSERSITE": "1"},
        }

        class TrackingTempStream:
            def __init__(self, stream):
                self.stream = stream

            def __enter__(self):
                self.stream.__enter__()
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                return self.stream.__exit__(exc_type, exc_value, traceback)

            def __getattr__(self, name):
                return getattr(self.stream, name)

            def flush(self) -> None:
                events.append("flush:temp")
                self.stream.flush()

        def record_open_private(path: str, mode: str, **kwargs):
            stream = real_open_private(path, mode, **kwargs)
            if path.endswith(".tmp"):
                temp_fds.add(stream.fileno())
                return TrackingTempStream(stream)
            return stream

        def record_fsync(fd: int) -> None:
            entry = os.fstat(fd)
            if fd in inbox_fds:
                self.assertTrue(stat.S_ISDIR(entry.st_mode))
                events.append("fsync:inbox")
                return
            if fd in temp_fds:
                self.assertTrue(stat.S_ISREG(entry.st_mode))
                events.append("fsync:temp")
                real_fsync(fd)
                return
            events.append("fsync:other")
            real_fsync(fd)

        def record_open(path, flags, *args, **kwargs):
            fd = real_open(path, flags, *args, **kwargs)
            if os.path.abspath(os.fspath(path)) == os.path.abspath(inbox_dir):
                required = getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
                self.assertEqual(required, flags & required)
                inbox_fds.add(fd)
                events.append("open:inbox-safe")
            return fd

        def record_replace(source: str, destination: str) -> None:
            events.append("replace")
            real_replace(source, destination)

        with mock.patch.dict(
            os.environ,
            {"SCHED_ALLOW_FOREIGN_WRITE": ""},
        ), mock.patch.object(
            cli,
            "_load_cfg",
            return_value=cfg,
        ), mock.patch.object(
            cli,
            "_is_foreign_host",
            return_value=True,
        ), mock.patch.object(
            cli,
            "_daemon_health",
            return_value={"heartbeat_age_s": 0, "tick_ok_age_s": 0, "frozen": False},
        ), mock.patch.object(
            state,
            "connect",
            side_effect=AssertionError("foreign submit must not open state DB"),
        ), mock.patch.object(
            state,
            "open_private_text",
            side_effect=record_open_private,
        ), mock.patch.object(
            cli.os,
            "fsync",
            side_effect=record_fsync,
        ), mock.patch.object(
            cli.os,
            "open",
            side_effect=record_open,
        ), mock.patch.object(
            cli.os,
            "replace",
            side_effect=record_replace,
        ):
            rc, stdout, stderr = self.capture(cli.cmd_submit, self._submit_args())

        self.assertEqual(0, rc, stderr)
        self.assertIn("已投递", stdout)
        expected = (
            "flush:temp",
            "fsync:temp",
            "replace",
            "open:inbox-safe",
            "fsync:inbox",
        )
        positions = [events.index(event) for event in expected]
        self.assertEqual(sorted(positions), positions)
        payloads = [
            name
            for name in os.listdir(inbox_dir)
            if name.startswith("submit-") and name.endswith(".json")
        ]
        self.assertEqual(1, len(payloads))

    def test_foreign_submit_durability_failure_never_claims_delivery(
        self,
    ) -> None:
        real_open_private = state.open_private_text
        real_open = os.open
        real_fsync = os.fsync
        real_replace = os.replace
        inbox_dir = state.submission_inbox_dir()
        cfg = {
            **self.cfg,
            "task_default_env": {"PYTHONNOUSERSITE": "1"},
        }

        class FlushFailingStream:
            def __init__(self, stream, triggered: list[str]):
                self.stream = stream
                self.triggered = triggered

            def __enter__(self):
                self.stream.__enter__()
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                return self.stream.__exit__(exc_type, exc_value, traceback)

            def __getattr__(self, name):
                return getattr(self.stream, name)

            def flush(self) -> None:
                self.triggered.append("temp-flush")
                raise OSError("temp flush failed")

        for failed_stage in (
            "temp-flush",
            "temp-fsync",
            "replace",
            "directory-open",
            "directory-fsync",
        ):
            with self.subTest(failed_stage=failed_stage):
                state.ensure_private_directory(inbox_dir)
                for name in os.listdir(inbox_dir):
                    if name.startswith("submit-"):
                        os.unlink(os.path.join(inbox_dir, name))
                triggered: list[str] = []

                def guarded_open_private(path: str, mode: str, **kwargs):
                    stream = real_open_private(path, mode, **kwargs)
                    if failed_stage == "temp-flush" and path.endswith(".tmp"):
                        return FlushFailingStream(stream, triggered)
                    return stream

                def guarded_open(path, flags, *args, **kwargs):
                    if (
                        failed_stage == "directory-open"
                        and os.path.abspath(os.fspath(path)) == os.path.abspath(inbox_dir)
                        and flags & getattr(os, "O_DIRECTORY", 0)
                    ):
                        triggered.append("directory-open")
                        raise OSError("inbox directory open failed")
                    return real_open(path, flags, *args, **kwargs)

                def guarded_fsync(fd: int) -> None:
                    entry = os.fstat(fd)
                    if failed_stage == "temp-fsync" and stat.S_ISREG(entry.st_mode):
                        triggered.append("temp-fsync")
                        raise OSError("temp fsync failed")
                    if failed_stage == "directory-fsync" and stat.S_ISDIR(entry.st_mode):
                        triggered.append("directory-fsync")
                        raise OSError("inbox directory fsync failed")
                    if stat.S_ISREG(entry.st_mode):
                        real_fsync(fd)

                def guarded_replace(source: str, destination: str) -> None:
                    if failed_stage == "replace":
                        triggered.append("replace")
                        raise OSError("payload rename failed")
                    real_replace(source, destination)

                with mock.patch.dict(
                    os.environ,
                    {"SCHED_ALLOW_FOREIGN_WRITE": ""},
                ), mock.patch.object(
                    cli,
                    "_load_cfg",
                    return_value=cfg,
                ), mock.patch.object(
                    cli,
                    "_is_foreign_host",
                    return_value=True,
                ), mock.patch.object(
                    cli,
                    "_daemon_health",
                    return_value={
                        "heartbeat_age_s": 0,
                        "tick_ok_age_s": 0,
                        "frozen": False,
                    },
                ), mock.patch.object(
                    state,
                    "connect",
                    side_effect=AssertionError("foreign submit must not open state DB"),
                ), mock.patch.object(
                    state,
                    "open_private_text",
                    side_effect=guarded_open_private,
                ), mock.patch.object(
                    cli.os,
                    "open",
                    side_effect=guarded_open,
                ), mock.patch.object(
                    cli.os,
                    "fsync",
                    side_effect=guarded_fsync,
                ), mock.patch.object(
                    cli.os,
                    "replace",
                    side_effect=guarded_replace,
                ):
                    rc, stdout, stderr = self.capture(
                        cli.cmd_submit,
                        self._submit_args(),
                    )

                self.assertNotEqual(0, rc)
                self.assertEqual([failed_stage], triggered)
                self.assertNotIn("已投递", stdout + stderr)


class ReviewPrivateStatePermissions(TempStateCase):
    def test_runtime_state_tree_repairs_and_creates_private_modes(self) -> None:
        root = state.default_state_dir()
        host = state.host_dir()
        database = state.db_path()
        os.chmod(root, 0o777)
        os.chmod(host, 0o777)
        os.chmod(database, 0o666)
        old_umask = os.umask(0)
        dispatcher = None
        try:
            state.init_db()
            with state.submission_lock():
                pass
            dispatcher = Dispatcher(self.cfg, fake=True)
            dispatcher._touch_heartbeat()
            dispatcher._touch_tick_ok()
            dispatcher._write_marker("private", "done", "ok")
            executor = dispatcher.executor
            job_log = os.path.join(host, "logs", "batch", "task-v1.log")
            pgid = executor.launch(
                [sys.executable, "-c", "pass"],
                None,
                self.tmp.name,
                {},
                None,
                job_log,
            )
            for _ in range(100):
                if executor.poll_rc(pgid) is not None:
                    break
                time.sleep(0.01)
        finally:
            if dispatcher is not None:
                dispatcher.log.close()
            os.umask(old_umask)

        private_dirs = (
            root,
            host,
            state.submission_inbox_dir(),
            os.path.join(host, "logs"),
            os.path.join(host, "logs", "batch"),
            os.path.join(host, "markers"),
        )
        private_files = (
            database,
            os.path.join(state.submission_inbox_dir(), ".submit.lock"),
            os.path.join(host, "scheduler.log"),
            os.path.join(host, "daemon.heartbeat"),
            os.path.join(host, "daemon.tick_ok"),
            os.path.join(host, "markers", "private.done"),
            os.path.join(host, "logs", "batch", "task-v1.log"),
        )
        for path in private_dirs:
            with self.subTest(path=path):
                self.assertEqual(
                    0o700,
                    stat.S_IMODE(os.stat(path).st_mode),
                )
        for path in private_files:
            with self.subTest(path=path):
                self.assertEqual(
                    0o600,
                    stat.S_IMODE(os.stat(path).st_mode),
                )


class ReviewIdempotentRequestTests(TempStateCase):
    def test_completed_request_replays_cached_result_without_second_mutation(
        self,
    ) -> None:
        self.seed_batch()
        args = argparse.Namespace(
            request_id="req-123",
            command=["cancel", "batch-20260829-000000", "--yes"],
            expect_kind="batch",
            expect_id="batch-20260829-000000",
            expect_status="active",
            expect_version=None,
            expect_quarantined=None,
            expect_revision=self.batch_revision(),
            expect_assignments_json=None,
        )

        def execute(_argv):
            print("mutated once")
            return 0

        with mock.patch.object(cli, "main", side_effect=execute) as nested:
            first = self.capture(cli.cmd_request, args)
            second = self.capture(cli.cmd_request, args)

        self.assertEqual((0, "mutated once\n", ""), first)
        self.assertEqual(first, second)
        nested.assert_called_once_with(
            ["cancel", "batch-20260829-000000", "--yes"]
        )

    def test_task_precondition_is_atomic_and_completed_replay_ignores_new_state(
        self,
    ) -> None:
        self.seed_batch(job_status="failed", version=1)
        args = argparse.Namespace(
            request_id="req-precondition",
            command=["retry", "batch-20260829-000000:task"],
            expect_kind="task",
            expect_id="batch-20260829-000000:task",
            expect_status="failed",
            expect_version=1,
            expect_quarantined=None,
            expect_revision=self.batch_revision(),
            expect_assignments_json=None,
        )

        contender_blocked = False

        def execute(_argv):
            nonlocal contender_blocked
            contender = sqlite3.connect(state.db_path(), timeout=0)
            try:
                contender.execute(
                    "UPDATE jobs SET status='running'"
                    " WHERE batch_id=? AND task_id=? AND version=1",
                    ("batch-20260829-000000", "task"),
                )
                contender.commit()
            except sqlite3.OperationalError as error:
                contender_blocked = "locked" in str(error).lower()
            finally:
                contender.close()
            if not contender_blocked:
                raise AssertionError(
                    "precondition lock was released before the mutation"
                )
            with state.connect() as conn:
                conn.execute(
                    "UPDATE jobs SET status='done'"
                    " WHERE batch_id=? AND task_id=? AND version=1",
                    ("batch-20260829-000000", "task"),
                )
            print("retried once")
            return 0

        with mock.patch.object(cli, "main", side_effect=execute) as nested:
            first = self.capture(cli.cmd_request, args)
            second = self.capture(cli.cmd_request, args)

        self.assertEqual((0, "retried once\n", ""), first)
        self.assertEqual(first, second)
        nested.assert_called_once()
        self.assertTrue(contender_blocked)

    def test_changed_task_precondition_never_executes_mutation(self) -> None:
        self.seed_batch(job_status="failed", version=2)
        args = argparse.Namespace(
            request_id="req-conflict",
            command=["retry", "batch-20260829-000000:task"],
            expect_kind="task",
            expect_id="batch-20260829-000000:task",
            expect_status="failed",
            expect_version=1,
            expect_quarantined=None,
            expect_revision=self.batch_revision(),
            expect_assignments_json=None,
        )

        with mock.patch.object(cli, "main") as nested:
            rc, stdout, stderr = self.capture(cli.cmd_request, args)

        self.assertEqual(65, rc)
        self.assertEqual("", stdout)
        self.assertIn("precondition", stderr)
        nested.assert_not_called()

    def test_incomplete_request_is_never_replayed_after_unknown_outcome(
        self,
    ) -> None:
        expectation = {
            "kind": "task",
            "id": "batch:task",
            "status": "failed",
            "version": 1,
            "quarantined": None,
            "revision": 0,
            "assignments": None,
        }
        binding = json.dumps(
            {
                "command": ["resubmit", "batch:task"],
                "expect": expectation,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        with state.connect() as conn:
            conn.execute(
                "INSERT INTO operation_requests"
                " (request_id, argv, status, created_at)"
                " VALUES (?, ?, 'started', ?)",
                ("req-unknown", binding, state.now()),
            )
        args = argparse.Namespace(
            request_id="req-unknown",
            command=["resubmit", "batch:task"],
            expect_kind="task",
            expect_id="batch:task",
            expect_status="failed",
            expect_version=1,
            expect_quarantined=None,
            expect_revision=0,
            expect_assignments_json=None,
        )

        with mock.patch.object(cli, "main") as nested:
            rc, stdout, stderr = self.capture(cli.cmd_request, args)

        self.assertEqual(75, rc)
        self.assertEqual("", stdout)
        self.assertIn("outcome unknown", stderr)
        nested.assert_not_called()



    def test_bound_retry_neutralizes_real_inner_commit_and_wakes_after_ledger_commit(
        self,
    ) -> None:
        job_id = self.seed_batch(batch_status="blocked", job_status="failed")
        marker_dir = os.path.join(self.state_root, "review-node", "markers")
        os.makedirs(marker_dir, exist_ok=True)
        blocked_marker = os.path.join(marker_dir, "batch.blocked")
        with open(blocked_marker, "w", encoding="utf-8") as stream:
            stream.write("blocked")
        revision = self.batch_revision()
        args = argparse.Namespace(
            request_id="req-real-inner-commit",
            command=["retry", "batch-20260829-000000:task"],
            expect_kind="task",
            expect_id="batch-20260829-000000:task",
            expect_status="failed",
            expect_version=1,
            expect_quarantined=None,
            expect_revision=revision,
            expect_assignments_json=None,
        )
        real_update = state.update_job
        commit_observations = []
        wake_observations = []

        def update_with_inner_commit(conn, target_job_id, **fields):
            real_update(conn, target_job_id, **fields)
            conn.commit()
            commit_observations.append(conn.in_transaction)

        def wake_after_commit():
            with state.connect() as conn:
                ledger = conn.execute(
                    "SELECT status FROM operation_requests WHERE request_id=?",
                    (args.request_id,),
                ).fetchone()
                job = state.get_job(conn, job_id)
                batch = state.get_batch(conn, "batch-20260829-000000")
            wake_observations.append(
                (
                    ledger["status"],
                    job["status"],
                    batch["status"],
                    os.path.exists(blocked_marker),
                )
            )
            return "awake"

        with mock.patch.object(
            state,
            "update_job",
            side_effect=update_with_inner_commit,
        ), mock.patch.object(
            state,
            "launch_marker_active",
            return_value=False,
        ), mock.patch.object(
            cli,
            "_ensure_running_locked",
            side_effect=wake_after_commit,
        ):
            rc, stdout, stderr = self.capture(cli.cmd_request, args)

        self.assertEqual(0, rc, stderr)
        self.assertIn("已解锁重跑", stdout)
        self.assertEqual([True], commit_observations)
        self.assertEqual([("done", "pending", "active", True)], wake_observations)

    def test_daemon_and_config_requests_execute_without_bound_connection(
        self,
    ) -> None:
        for index, command in enumerate(
            (
                ["daemon", "start"],
                ["config", "set", "-f", "patch.json", "--yes"],
            )
        ):
            with self.subTest(command=command):
                args = argparse.Namespace(
                    request_id=f"req-unbound-{index}",
                    command=command,
                    expect_kind="none",
                    expect_id=None,
                    expect_status=None,
                    expect_version=None,
                    expect_quarantined=None,
                    expect_revision=0,
                    expect_assignments_json=None,
                )
                observations = []

                def execute(_argv):
                    observations.append(state._bound_connection.get())
                    return 0

                with mock.patch.object(cli, "main", side_effect=execute):
                    rc, _stdout, stderr = self.capture(cli.cmd_request, args)

                self.assertEqual(0, rc, stderr)
                self.assertEqual([None], observations)

    def test_revision_detects_same_status_aba_and_returns_code_65(self) -> None:
        self.seed_batch(job_status="failed")
        old_revision = self.batch_revision()
        with state.connect() as conn:
            conn.execute(
                "UPDATE jobs SET status='running' WHERE batch_id=? AND task_id=?",
                ("batch-20260829-000000", "task"),
            )
            conn.execute(
                "UPDATE jobs SET status='failed' WHERE batch_id=? AND task_id=?",
                ("batch-20260829-000000", "task"),
            )
        args = argparse.Namespace(
            request_id="req-aba",
            command=["retry", "batch-20260829-000000:task"],
            expect_kind="task",
            expect_id="batch-20260829-000000:task",
            expect_status="failed",
            expect_version=1,
            expect_quarantined=None,
            expect_revision=old_revision,
            expect_assignments_json=None,
        )

        with mock.patch.object(cli, "main") as nested:
            rc, stdout, stderr = self.capture(cli.cmd_request, args)

        self.assertEqual(65, rc)
        self.assertEqual("", stdout)
        self.assertIn("revision changed", stderr)
        nested.assert_not_called()

    def test_old_done_output_compacts_to_replayable_binding_tombstone(
        self,
    ) -> None:
        self.seed_batch()
        args = argparse.Namespace(
            request_id="req-tombstone",
            command=["cancel", "batch-20260829-000000", "--yes"],
            expect_kind="batch",
            expect_id="batch-20260829-000000",
            expect_status="active",
            expect_version=None,
            expect_quarantined=None,
            expect_revision=self.batch_revision(),
            expect_assignments_json=None,
        )
        with mock.patch.object(cli, "main", return_value=0) as nested:
            with mock.patch("builtins.print") as printed:
                first_rc = cli.cmd_request(args)
                printed.assert_not_called()
            self.assertEqual(0, first_rc)
            with state.connect() as conn:
                conn.execute(
                    "UPDATE operation_requests SET finished_at=?"
                    " WHERE request_id=?",
                    ("2000-01-01 00:00:00", args.request_id),
                )
                state.compact_operation_outputs(
                    conn,
                    keep_recent=0,
                    ttl_days=0,
                )
            replay = self.capture(cli.cmd_request, args)

        self.assertEqual((0, "", ""), replay)
        nested.assert_called_once()
        with state.connect() as conn:
            row = conn.execute(
                "SELECT argv, status, output_compacted, stdout, stderr"
                " FROM operation_requests WHERE request_id=?",
                (args.request_id,),
            ).fetchone()
        self.assertEqual("done", row["status"])
        self.assertEqual(1, row["output_compacted"])
        self.assertIsNone(row["stdout"])
        self.assertIsNone(row["stderr"])
        self.assertTrue(row["argv"])

    def test_request_capture_is_bounded_before_persistence(self) -> None:
        args = argparse.Namespace(
            request_id="req-bounded-output",
            command=["submit", "batch.json"],
            expect_kind="none",
            expect_id=None,
            expect_status=None,
            expect_version=None,
            expect_quarantined=None,
            expect_revision=0,
            expect_assignments_json=None,
        )

        def noisy(_argv):
            print("x" * (cli._REQUEST_CAPTURE_BYTES + 4096))
            return 0

        with mock.patch.object(cli, "main", side_effect=noisy):
            rc, stdout, stderr = self.capture(cli.cmd_request, args)

        self.assertEqual(0, rc, stderr)
        self.assertLessEqual(len(stdout.encode("utf-8")), cli._REQUEST_CAPTURE_BYTES)
        self.assertIn("output truncated", stdout)

    def test_sqlite_revision_triggers_cover_job_and_gpu_assignment_transitions(
        self,
    ) -> None:
        self.seed_batch(job_status="failed")
        batch_before = self.batch_revision()
        with state.connect() as conn:
            conn.execute(
                "UPDATE jobs SET status='running' WHERE batch_id=? AND task_id=?",
                ("batch-20260829-000000", "task"),
            )
        batch_after = self.batch_revision()
        self.assertGreater(batch_after, batch_before)

        with state.connect() as conn:
            state.init_gpus(conn, [0, 1])
            before = {
                row["idx"]: row["revision"]
                for row in conn.execute(
                    "SELECT idx, revision FROM gpus ORDER BY idx"
                ).fetchall()
            }
            conn.execute(
                "UPDATE gpus SET status='assigned', job_id='job-a' WHERE idx=0"
            )
            assigned = conn.execute(
                "SELECT revision FROM gpus WHERE idx=0"
            ).fetchone()["revision"]
            conn.execute(
                "INSERT INTO gpu_jobs (gpu_id, job_id, vram_gib)"
                " VALUES (0, 'job-a', 1.5)"
            )
            inserted = conn.execute(
                "SELECT revision FROM gpus WHERE idx=0"
            ).fetchone()["revision"]
            conn.execute(
                "UPDATE gpu_jobs SET gpu_id=1 WHERE job_id='job-a'"
            )
            moved = {
                row["idx"]: row["revision"]
                for row in conn.execute(
                    "SELECT idx, revision FROM gpus ORDER BY idx"
                ).fetchall()
            }
            conn.execute("DELETE FROM gpu_jobs WHERE job_id='job-a'")
            deleted = conn.execute(
                "SELECT revision FROM gpus WHERE idx=1"
            ).fetchone()["revision"]
            ignore_before = conn.execute(
                "SELECT revision FROM gpus WHERE idx=0"
            ).fetchone()["revision"]
            conn.execute(
                "UPDATE gpus SET ignore_until='2026-08-30 20:00:00'"
                " WHERE idx=0"
            )
            ignored = conn.execute(
                "SELECT revision FROM gpus WHERE idx=0"
            ).fetchone()["revision"]

        self.assertGreater(assigned, before[0])
        self.assertGreater(inserted, assigned)
        self.assertGreater(moved[0], inserted)
        self.assertGreater(moved[1], before[1])
        self.assertGreater(deleted, moved[1])
        self.assertEqual(ignore_before + 1, ignored)

    def test_gpu_ignore_request_invalidates_the_old_revision(self) -> None:
        with state.connect() as conn:
            state.init_gpus(conn, [0])
            gpu = state.get_gpu(conn, 0)

        def request_args(request_id: str) -> argparse.Namespace:
            return argparse.Namespace(
                request_id=request_id,
                command=["gpu-ignore", "0"],
                expect_kind="gpu",
                expect_id="0",
                expect_status=gpu["status"],
                expect_version=None,
                expect_quarantined=gpu["quarantined"],
                expect_revision=gpu["revision"],
                expect_assignments_json="[]",
            )

        rc, stdout, stderr = self.capture(
            cli.cmd_request,
            request_args("req-ignore-first"),
        )
        self.assertEqual(0, rc, stderr)
        self.assertIn("已忽略告警", stdout)
        with state.connect() as conn:
            after = state.get_gpu(conn, 0)
        self.assertEqual(gpu["revision"] + 1, after["revision"])
        self.assertIsNotNone(after["ignore_until"])

        rc, stdout, stderr = self.capture(
            cli.cmd_request,
            request_args("req-ignore-stale"),
        )
        self.assertEqual(65, rc)
        self.assertEqual("", stdout)
        self.assertIn("revision changed", stderr)

    def test_legacy_database_installs_gpu_ignore_revision_trigger(self) -> None:
        with state.connect() as conn:
            conn.execute("DROP TRIGGER IF EXISTS revision_gpu_ignore")
            state.init_gpus(conn, [0])
            before = state.get_gpu(conn, 0)["revision"]

        state.init_db()
        state.init_db()

        with state.connect() as conn:
            trigger_rows = conn.execute(
                "SELECT sql FROM sqlite_master"
                " WHERE type='trigger' AND name='revision_gpu_ignore'"
            ).fetchall()
            self.assertEqual(1, len(trigger_rows))
            conn.execute(
                "UPDATE gpus SET ignore_until='2026-08-30 20:00:00'"
                " WHERE idx=0"
            )
            after = state.get_gpu(conn, 0)["revision"]
            conn.execute(
                "UPDATE gpus SET ignore_until='2026-08-30 20:00:00'"
                " WHERE idx=0"
            )
            unchanged = state.get_gpu(conn, 0)["revision"]
        self.assertEqual(before + 1, after)
        self.assertEqual(after, unchanged)

    def test_gpu_request_binds_revision_and_exact_assignment_snapshot(self) -> None:
        with state.connect() as conn:
            state.init_gpus(conn, [0])
            conn.execute(
                "INSERT INTO gpu_jobs (gpu_id, job_id, vram_gib)"
                " VALUES (0, 'job-a', 1.0)"
            )
            gpu = state.get_gpu(conn, 0)
        args = argparse.Namespace(
            request_id="req-gpu-assignments",
            command=["gpu-ok", "0"],
            expect_kind="gpu",
            expect_id="0",
            expect_status=gpu["status"],
            expect_version=None,
            expect_quarantined=gpu["quarantined"],
            expect_revision=gpu["revision"],
            expect_assignments_json="[]",
        )

        with mock.patch.object(cli, "main") as nested:
            rc, stdout, stderr = self.capture(cli.cmd_request, args)

        self.assertEqual(65, rc)
        self.assertEqual("", stdout)
        self.assertIn("assignments changed", stderr)
        nested.assert_not_called()

    def test_request_gpu_mutation_allowlist_is_explicit(self) -> None:
        with state.connect() as conn:
            state.init_gpus(conn, [0])
            gpu = state.get_gpu(conn, 0)

        cases = (
            (["gpu-free", "0", "--yes"], True),
            (["gpu-ignore", "0"], True),
            (["gpu-ok", "0"], True),
            (["gpu-set-mem", "0", "24"], False),
        )
        for command, allowed in cases:
            request_id = f"req-allow-{command[0]}"
            args = argparse.Namespace(
                request_id=request_id,
                command=command,
                expect_kind="gpu",
                expect_id="0",
                expect_status=gpu["status"],
                expect_version=None,
                expect_quarantined=gpu["quarantined"],
                expect_revision=gpu["revision"],
                expect_assignments_json="[]",
            )

            with self.subTest(command=command), mock.patch.object(
                cli,
                "main",
                return_value=0,
            ) as nested:
                rc, stdout, stderr = self.capture(cli.cmd_request, args)
                with state.connect() as conn:
                    ledger = conn.execute(
                        "SELECT status, code FROM operation_requests"
                        " WHERE request_id=?",
                        (request_id,),
                    ).fetchone()
                if allowed:
                    self.assertEqual(0, rc, stderr)
                    self.assertEqual("", stdout)
                    nested.assert_called_once_with(command)
                    self.assertEqual(("done", 0), tuple(ledger))
                else:
                    self.assertEqual(64, rc)
                    self.assertEqual("", stdout)
                    self.assertIn(
                        "request 只允许调度器 mutation 子命令",
                        stderr,
                    )
                    nested.assert_not_called()
                    self.assertIsNone(ledger)

    def test_bound_request_acquires_reentrant_submission_lease_before_db(
        self,
    ) -> None:
        self.seed_batch()
        args = argparse.Namespace(
            request_id="req-lock-order",
            command=["cancel", "batch-20260829-000000", "--yes"],
            expect_kind="batch",
            expect_id="batch-20260829-000000",
            expect_status="active",
            expect_version=None,
            expect_quarantined=None,
            expect_revision=self.batch_revision(),
            expect_assignments_json=None,
        )
        observed_depths = []
        real_connect = state.connect

        @contextlib.contextmanager
        def recording_connect():
            observed_depths.append(state._submission_lock_depth.get())
            with real_connect() as conn:
                yield conn

        with state.submission_lock():
            with state.submission_lock():
                self.assertEqual(2, state._submission_lock_depth.get())

        with mock.patch.object(
            state,
            "connect",
            side_effect=recording_connect,
        ), mock.patch.object(cli, "main", return_value=0):
            rc, _stdout, stderr = self.capture(cli.cmd_request, args)

        self.assertEqual(0, rc, stderr)
        self.assertTrue(observed_depths)
        self.assertTrue(all(depth > 0 for depth in observed_depths))


class ReviewDaemonGuardAndExitTests(TempStateCase):
    def test_s_h05_foreign_start_stop_and_check_fail_closed(self) -> None:
        for action, method in {"start": "start", "stop": "stop", "check": "check"}.items():
            with self.subTest(action=action), mock.patch.dict(
                os.environ, {"SCHED_ALLOW_FOREIGN_WRITE": ""}
            ), mock.patch("socket.gethostname", return_value="login-node"), mock.patch(
                "gsched.config.load_config", return_value=self.cfg
            ), mock.patch.object(state, "init_db"), mock.patch.object(
                daemon, method
            ) as operation:
                rc, _stdout, _stderr = self.capture(cli.main, ["daemon", action])
                self.assertEqual(2, rc)
                operation.assert_not_called()

    def test_s_h05_foreign_daemon_status_is_read_only(self) -> None:
        with mock.patch.dict(os.environ, {"SCHED_ALLOW_FOREIGN_WRITE": ""}), mock.patch(
            "socket.gethostname", return_value="login-node"
        ), mock.patch("gsched.config.load_config", return_value=self.cfg), mock.patch.object(
            state, "init_db"
        ) as init_db, mock.patch.object(daemon, "status_str", return_value="running") as status:
            rc, stdout, stderr = self.capture(cli.main, ["daemon", "status"])
        self.assertEqual(0, rc, stderr)
        self.assertIn("running", stdout)
        init_db.assert_not_called()
        status.assert_called_once_with()

    def test_s_h05_explicit_foreign_write_override_allows_daemon_lifecycle(self) -> None:
        with mock.patch.dict(os.environ, {"SCHED_ALLOW_FOREIGN_WRITE": "1"}), mock.patch.object(
            state, "init_db"
        ), mock.patch.object(daemon, "start", return_value="daemon 启动成功") as start:
            rc, _stdout, stderr = self.capture(cli.main, ["daemon", "start", "--fake"])
        self.assertEqual(0, rc, stderr)
        start.assert_called_once_with(fake=True)

    def test_s_m08_daemon_refusals_and_failed_checks_return_nonzero(self) -> None:
        cases = [
            ("start", "start", "前置检查未通过, 拒绝启动", None),
            ("stop", "stop", "daemon 在 compute 运行; 请到计算节点执行", None),
            ("check", "check", None, [{"item": "GPU", "detail": "bad", "level": "fail"}]),
        ]
        for action, method, text, issues in cases:
            with self.subTest(action=action), mock.patch.object(
                daemon, method, return_value=issues if issues is not None else text
            ):
                rc, _stdout, _stderr = self.capture(
                    cli.cmd_daemon, argparse.Namespace(action=action, fake=False)
                )
                self.assertNotEqual(0, rc)

    def test_s_m08_idempotent_daemon_states_are_success(self) -> None:
        for action, method, text in (
            ("start", "start", "daemon 已在运行"),
            ("stop", "stop", "daemon 未运行"),
        ):
            with self.subTest(action=action), mock.patch.object(daemon, method, return_value=text):
                rc, _stdout, stderr = self.capture(
                    cli.cmd_daemon, argparse.Namespace(action=action, fake=False)
                )
                self.assertEqual(0, rc, stderr)

    def test_s_m08_daemon_exceptions_become_nonzero_cli_results(self) -> None:
        with mock.patch.object(daemon, "start", side_effect=RuntimeError("boom")):
            rc, _stdout, stderr = self.capture(
                cli.cmd_daemon, argparse.Namespace(action="start", fake=False)
            )
        self.assertNotEqual(0, rc)
        self.assertIn("boom", stderr)

    def test_stop_signal_failure_releases_only_its_submission_stop_marker(self) -> None:
        os.makedirs(os.path.dirname(daemon._owner_file()), exist_ok=True)
        with open(daemon._pid_file(), "w", encoding="utf-8") as stream:
            stream.write("4242\n")
        with open(daemon._heartbeat_file(), "w", encoding="utf-8") as stream:
            stream.write("alive\n")
        with open(daemon._owner_file(), "w", encoding="utf-8") as stream:
            json.dump(
                {
                    "schema_version": 1,
                    "lease_id": "lease-test",
                    "pid": 4242,
                    "start_token": "start-test",
                    "physical_host": socket.gethostname(),
                },
                stream,
            )

        with mock.patch.object(daemon, "_pid_alive", return_value=True), mock.patch.object(
            daemon, "process_start_token", return_value="start-test"
        ), mock.patch.object(
            daemon.os, "kill", side_effect=PermissionError("not permitted")
        ):
            message = daemon.stop()

        self.assertIn("SIGTERM 发送失败", message)
        self.assertTrue(os.path.isfile(daemon._pid_file()))
        self.assertTrue(os.path.isfile(daemon._heartbeat_file()))
        self.assertFalse(state.idle_shutdown_pending())
        with state.submission_connect() as conn:
            self.assertEqual(1, conn.execute("SELECT 1").fetchone()[0])


class ReviewDryRunAndHistoryTests(TempStateCase):
    def test_s_m01_json_dry_run_stdout_is_exactly_one_json_document(self) -> None:
        preview = {
            "tasks": [{"id": "task", "stages": [], "skip": False, "cmd_flat": "/bin/true"}],
            "dep_status": {}, "git_rev": None, "n_skip": 0, "n_run": 1,
        }
        _result, stdout, stderr = self.capture(
            cli._print_dry_run_preview,
            {"name": "batch", "mode": "mix", "tasks": [{}]},
            argparse.Namespace(json=True),
            preview,
            False,
        )
        self.assertEqual(preview, json.loads(stdout))
        self.assertEqual("", stderr)

    def test_s_m01_first_run_dry_run_uses_empty_read_only_state(self) -> None:
        batch_path = os.path.join(self.tmp.name, "first-run.json")
        with open(batch_path, "w", encoding="utf-8") as stream:
            json.dump(
                {
                    "name": "first-run",
                    "project": "p",
                    "tasks": [
                        {
                            "id": "task",
                            "cmd": ["/bin/true"],
                            "resources": {"gpu": 0, "cpus": 1},
                        }
                    ],
                },
                stream,
            )
        missing_db = os.path.join(self.tmp.name, "never-created", "state.db")
        with mock.patch.object(state, "db_path", return_value=missing_db):
            rc, stdout, stderr = self.capture(
                cli.main,
                ["submit", batch_path, "--dry-run", "--json"],
            )
        self.assertEqual(0, rc, stderr)
        preview = json.loads(stdout)
        self.assertEqual(1, preview["n_run"])
        self.assertEqual("task", preview["tasks"][0]["id"])
        self.assertFalse(os.path.exists(missing_db))

    def test_s_m01_force_rerun_preview_never_predicts_skip(self) -> None:
        artifact = os.path.join(self.tmp.name, "artifact")
        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write("valid")
        task = {
            "id": "task", "cmd": ["/bin/true"], "stages": None,
            "cwd_abs": self.tmp.name, "git": False,
            "artifacts": {"out": {"path": artifact}},
            "resources": {"gpu": 0}, "_force_rerun": True,
        }
        preview = cli._dry_run_preview(
            {"name": "batch", "tasks": [task], "depends_on": []}, self.cfg, use_state=False
        )
        self.assertFalse(preview["tasks"][0]["skip"])

    def test_s_m01_code_drift_preview_uses_producer_fingerprint(self) -> None:
        artifact = os.path.join(self.tmp.name, "artifact")
        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write("valid")
        self.seed_batch(batch_id="producer", name="old", batch_status="done", job_status="done")
        task = {
            "id": "task", "cmd": ["/bin/true"], "stages": None,
            "cwd_abs": self.tmp.name, "git": True,
            "artifacts": {"out": {"path": artifact}},
            "resources": {"gpu": 0}, "project": "p",
        }
        with mock.patch(
            "gsched.fingerprint.compute_fingerprint", return_value=("new-code", None, "new-rev")
        ):
            preview = cli._dry_run_preview(
                {"name": "new", "tasks": [task], "depends_on": []}, self.cfg
            )
        self.assertFalse(preview["tasks"][0]["skip"])

    def test_s_m02_history_project_valid_unknown_and_empty_are_normal_results(self) -> None:
        args = argparse.Namespace(batch=None, limit=50, status=None, project="p", json=False)
        with mock.patch.object(cli, "_load_cfg", return_value=self.cfg):
            rc, stdout, stderr = self.capture(cli.cmd_history, args)
        self.assertEqual(0, rc, stderr)
        self.assertIn("无历史", stdout)
        args.project = "missing"
        with mock.patch.object(cli, "_load_cfg", return_value=self.cfg):
            rc, _stdout, stderr = self.capture(cli.cmd_history, args)
        self.assertEqual(1, rc)
        self.assertIn("missing", stderr)

    def test_x_h03_history_json_has_versioned_bounded_envelope(self) -> None:
        self.seed_batch(batch_id="old", name="old", batch_status="done", job_status="done")
        self.seed_batch(batch_id="new", name="new", batch_status="blocked", job_status="failed")
        rc, stdout, stderr = self.capture(cli.main, ["history", "--json", "--limit", "1"])
        self.assertEqual(0, rc, stderr)
        payload = json.loads(stdout)
        self.assertEqual(1, payload["schema_version"])
        self.assertEqual(1, payload["limit"])
        self.assertTrue(payload["truncated"])
        self.assertEqual(1, len(payload["history"]))
        required = {
            "batch_id", "batch_name", "task", "status", "version", "rc", "gpu",
            "started_at", "finished_at", "duration_seconds", "failure",
        }
        self.assertTrue(required.issubset(payload["history"][0]))

    def test_x_h03_empty_history_is_still_a_valid_json_envelope(self) -> None:
        rc, stdout, stderr = self.capture(cli.main, ["history", "--json"])
        self.assertEqual(0, rc, stderr)
        self.assertEqual(
            {
                "schema_version": 1,
                "history": [],
                "limit": 50,
                "truncated": False,
                "next_cursor": None,
            },
            json.loads(stdout),
        )

    def test_x_h03_history_limit_is_clamped_to_one_through_two_hundred(self) -> None:
        for requested, expected in ((0, 1), (999, 200)):
            with self.subTest(requested=requested):
                rc, stdout, stderr = self.capture(
                    cli.main, ["history", "--json", "--limit", str(requested)]
                )
                self.assertEqual(0, rc, stderr)
                self.assertEqual(expected, json.loads(stdout)["limit"])

    def test_history_keyset_cursor_retrieves_next_page_without_overlap(
        self,
    ) -> None:
        for index in range(3):
            batch_id = f"history-page-{index}"
            job_id = self.seed_batch(
                batch_id=batch_id,
                name=batch_id,
                batch_status="done",
                job_status="done",
            )
            with state.connect() as conn:
                conn.execute(
                    "UPDATE jobs SET finished_at=? WHERE id=?",
                    (f"2026-08-29 0{index + 1}:00:00", job_id),
                )

        rc, stdout, stderr = self.capture(
            cli.main,
            ["history", "--json", "--limit", "2"],
        )
        self.assertEqual(0, rc, stderr)
        first = json.loads(stdout)
        self.assertIsInstance(first["next_cursor"], str)

        rc, stdout, stderr = self.capture(
            cli.main,
            [
                "history",
                "--json",
                "--limit",
                "2",
                "--cursor",
                first["next_cursor"],
            ],
        )
        self.assertEqual(0, rc, stderr)
        second = json.loads(stdout)
        first_ids = {row["id"] for row in first["history"]}
        second_ids = {row["id"] for row in second["history"]}
        self.assertFalse(first_ids & second_ids)
        self.assertEqual(3, len(first_ids | second_ids))


class ReviewStatusAndTaskReferenceTests(TempStateCase):
    def test_s_m07_status_uses_latest_version_and_canonical_wait_reason(self) -> None:
        self.seed_batch(batch_status="active", job_status="failed", version=1)
        spec = {
            "id": "task", "cmd": ["/bin/true"], "stages": None,
            "cwd_abs": self.tmp.name, "git": False, "env": {},
            "resources": {"gpu": 0, "cpus": 1}, "artifacts": {}, "project": "p",
        }
        gpu_spec = {
            **spec,
            "resources": {"gpu": 1, "cpus": 1},
        }
        with state.connect() as conn:
            state.insert_task(conn, "batch-20260829-000000", "task", 2, spec, 0, "p")
            state.insert_job(conn, "job-v2", "batch-20260829-000000", "task", 2, "fp2", None, "p")
            state.update_job(conn, "job-v2", status="done", rc=0, finished_at=state.now())
            state.insert_task(conn, "batch-20260829-000000", "occupier", 1, gpu_spec, 1, "p")
            state.insert_job(conn, "job-running", "batch-20260829-000000", "occupier", 1, "fpr", None, "p")
            state.update_job(conn, "job-running", status="running", gpu=0)
            state.insert_task(conn, "batch-20260829-000000", "waiting", 1, gpu_spec, 2, "p")
            state.insert_job(conn, "job-wait", "batch-20260829-000000", "waiting", 1, "fpw", None, "p")
        payload = self.status_json()
        self.assertEqual(1, payload["schema_version"])
        self.assertEqual(
            {("task", 2), ("occupier", 1), ("waiting", 1)},
            {(job["task"], job["version"]) for job in payload["jobs"]},
        )
        self.assertEqual("1/3", payload["batches"][0]["progress"])
        waiting = next(job for job in payload["jobs"] if job["task"] == "waiting")
        self.assertEqual("pending", waiting["status"])
        self.assertEqual("quota", waiting["wait_reason"])
        self.assertEqual("batch-20260829-000000", waiting["batch_id"])
        self.assertEqual("batch", waiting["batch_name"])

    def test_s_m07_status_limit_is_bounded_and_reports_each_truncation(self) -> None:
        self.seed_batch()
        args = argparse.Namespace(batch=None, json=True, detail=False, project=None, limit=5000)
        with mock.patch.object(cli, "_load_cfg", return_value=self.cfg), mock.patch.object(
            cli, "_daemon_health", return_value={}
        ):
            rc, stdout, stderr = self.capture(cli.cmd_status, args)
        self.assertEqual(0, rc, stderr)
        payload = json.loads(stdout)
        self.assertEqual(1000, payload["limit"])
        self.assertEqual({"batches": False, "jobs": False}, payload["truncated"])

    def test_status_limit_pages_batches_before_jobs_with_referential_closure(self) -> None:
        for batch_id, created_at in (
            ("batch-oldest", "2026-08-29 08:00:00"),
            ("batch-middle", "2026-08-29 09:00:00"),
            ("batch-newest", "2026-08-29 10:00:00"),
        ):
            self.seed_batch(
                batch_id=batch_id,
                name=batch_id,
                batch_status="done",
                job_status="done",
            )
            with state.connect() as conn:
                conn.execute(
                    "UPDATE batches SET created_at=? WHERE id=?",
                    (created_at, batch_id),
                )

        first = self.status_json(limit=2)
        second = self.status_json(limit=2)

        emitted_batches = [batch["id"] for batch in first["batches"]]
        emitted_job_batches = [job["batch_id"] for job in first["jobs"]]
        self.assertEqual(["batch-newest", "batch-middle"], emitted_batches)
        self.assertEqual(set(emitted_batches), set(emitted_job_batches))
        self.assertTrue(
            all(job["batch_id"] in emitted_batches for job in first["jobs"])
        )
        self.assertEqual(
            [job["id"] for job in first["jobs"]],
            [job["id"] for job in second["jobs"]],
        )
        self.assertEqual(
            {"batches": True, "jobs": False},
            first["truncated"],
        )

    def test_status_prioritizes_current_batches_before_newer_history(self) -> None:
        for batch_id, status, created_at in (
            ("active-old", "active", "2026-08-29 08:00:00"),
            ("done-middle", "done", "2026-08-29 09:00:00"),
            ("cancelled-new", "cancelled", "2026-08-29 10:00:00"),
        ):
            self.seed_batch(
                batch_id=batch_id,
                name=batch_id,
                batch_status=status,
                job_status="running" if status == "active" else "done",
            )
            with state.connect() as conn:
                conn.execute(
                    "UPDATE batches SET created_at=? WHERE id=?",
                    (created_at, batch_id),
                )

        payload = self.status_json(limit=2)

        self.assertEqual(
            ["active-old", "cancelled-new"],
            [batch["id"] for batch in payload["batches"]],
        )

    def test_status_keyset_cursor_retrieves_next_batch_page_without_overlap(
        self,
    ) -> None:
        for index in range(3):
            self.seed_batch(
                batch_id=f"page-{index}",
                name=f"page-{index}",
                batch_status="done",
                job_status="done",
            )
            with state.connect() as conn:
                conn.execute(
                    "UPDATE batches SET created_at=? WHERE id=?",
                    (f"2026-08-29 0{index + 1}:00:00", f"page-{index}"),
                )

        first = self.status_json(limit=2)
        self.assertIsInstance(first["next_cursor"], str)
        second = self.status_json(limit=2, cursor=first["next_cursor"])

        first_ids = {batch["id"] for batch in first["batches"]}
        second_ids = {batch["id"] for batch in second["batches"]}
        self.assertFalse(first_ids & second_ids)
        self.assertEqual(
            {"page-0", "page-1", "page-2"},
            first_ids | second_ids,
        )

    def test_one_batch_jobs_page_independently_from_batch_cursor(self) -> None:
        self.seed_batch(job_status="done")
        with state.connect() as conn:
            base_spec = json.loads(
                conn.execute(
                    "SELECT spec FROM tasks WHERE batch_id=? AND id='task'",
                    ("batch-20260829-000000",),
                ).fetchone()["spec"]
            )
            for order, task_id in enumerate(("task-b", "task-c"), start=1):
                spec = dict(base_spec, id=task_id)
                state.insert_task(
                    conn,
                    "batch-20260829-000000",
                    task_id,
                    1,
                    spec,
                    order,
                    "p",
                )
                state.insert_job(
                    conn,
                    f"job-{task_id}",
                    "batch-20260829-000000",
                    task_id,
                    1,
                    f"fp-{task_id}",
                    None,
                    "p",
                )
        first = self.status_json(
            batch="batch-20260829-000000",
            limit=2,
        )
        self.assertIsNone(first["next_cursor"])
        self.assertIsInstance(first["next_job_cursor"], str)
        self.assertTrue(first["truncated"]["jobs"])

        second = self.status_json(
            batch="batch-20260829-000000",
            limit=2,
            job_cursor=first["next_job_cursor"],
        )
        first_ids = {job["id"] for job in first["jobs"]}
        second_ids = {job["id"] for job in second["jobs"]}
        self.assertFalse(first_ids & second_ids)
        self.assertEqual(3, len(first_ids | second_ids))
        self.assertEqual(
            ["batch-20260829-000000"],
            [batch["id"] for batch in second["batches"]],
        )

    def test_task_json_is_a_real_single_document_contract(self) -> None:
        self.seed_batch(job_status="failed")
        rc, stdout, stderr = self.capture(
            cli.main,
            ["task", "batch-20260829-000000:task", "--json"],
        )

        self.assertEqual(0, rc, stderr)
        payload = json.loads(stdout)
        self.assertEqual(1, payload["schema_version"])
        self.assertEqual("batch-20260829-000000", payload["batch_id"])
        self.assertEqual("batch", payload["batch_name"])
        self.assertIsInstance(payload["batch_revision"], int)
        self.assertEqual("task", payload["task"])
        self.assertEqual(1, len(payload["jobs"]))
        self.assertEqual("review failure", payload["jobs"][0]["failure"])
        self.assertEqual("", stderr)

    def test_status_exposes_revisions_and_sorted_gpu_assignments(self) -> None:
        self.seed_batch()
        with state.connect() as conn:
            state.init_gpus(conn, [0])
            conn.execute(
                "INSERT INTO gpu_jobs (gpu_id, job_id, vram_gib)"
                " VALUES (0, 'z-job', 2.0)"
            )
            conn.execute(
                "INSERT INTO gpu_jobs (gpu_id, job_id, vram_gib)"
                " VALUES (0, 'a-job', 1.0)"
            )
        payload = self.status_json()

        self.assertIsInstance(payload["batches"][0]["revision"], int)
        self.assertIsInstance(payload["gpus"][0]["revision"], int)
        self.assertEqual(
            [
                {"job_id": "a-job", "vram_gib": 1.0},
                {"job_id": "z-job", "vram_gib": 2.0},
            ],
            payload["gpus"][0]["assignments"],
        )

    def test_x_h02_exact_batch_id_wins_then_latest_name_fallback(self) -> None:
        self.seed_batch(batch_id="batch-old", name="same")
        self.seed_batch(batch_id="batch-new", name="same")
        self.assertEqual(("batch-old", "task"), cli._resolve_task_ref("batch-old:task"))
        self.assertEqual(("batch-new", "task"), cli._resolve_task_ref("same:task"))

    def test_x_h02_task_detail_accepts_status_batch_id_reference(self) -> None:
        self.seed_batch(job_status="failed")
        rc, stdout, stderr = self.capture(
            cli.cmd_task, argparse.Namespace(task="batch-20260829-000000:task")
        )
        self.assertEqual(0, rc, stderr)
        self.assertIn("review failure", stdout)

    def test_x_h02_log_accepts_status_batch_id_reference(self) -> None:
        self.seed_batch(job_status="failed")
        log_dir = os.path.join(self.state_root, "review-node", "logs", "batch-20260829-000000")
        os.makedirs(log_dir, exist_ok=True)
        with open(os.path.join(log_dir, "task-v1.log"), "w", encoding="utf-8") as stream:
            stream.write("review log\n")
        rc, stdout, stderr = self.capture(
            cli.cmd_log,
            argparse.Namespace(task="batch-20260829-000000:task", f=False, n=20),
        )
        self.assertEqual(0, rc, stderr)
        self.assertIn("review log", stdout)

    def test_x_h02_diag_accepts_status_batch_id_reference(self) -> None:
        self.seed_batch(job_status="failed")
        with mock.patch.object(cli, "_load_cfg", return_value=self.cfg), mock.patch.object(
            cli, "_diag_one"
        ) as diag:
            rc, _stdout, stderr = self.capture(
                cli.cmd_diag, argparse.Namespace(task="batch-20260829-000000:task")
            )
        self.assertEqual(0, rc, stderr)
        diag.assert_called_once()

    def test_x_h02_retry_accepts_status_batch_id_reference(self) -> None:
        self.seed_batch(batch_status="blocked", job_status="failed")
        marker_dir = os.path.join(self.state_root, "review-node", "markers")
        os.makedirs(marker_dir, exist_ok=True)
        blocked_marker = os.path.join(marker_dir, "batch.blocked")
        with open(blocked_marker, "w", encoding="utf-8") as stream:
            stream.write("blocked")
        with mock.patch.object(state, "launch_marker_active", return_value=False), mock.patch.object(
            cli, "_rev_diff_warn", return_value=None
        ), mock.patch.object(cli, "_ensure_running_locked", return_value="awake"):
            rc, _stdout, stderr = self.capture(
                cli.cmd_retry, argparse.Namespace(task="batch-20260829-000000:task")
            )
        self.assertEqual(0, rc, stderr)
        with state.connect() as conn:
            self.assertEqual("pending", state.get_job(conn, "batch-20260829-000000-task-v1")["status"])
            self.assertEqual(
                "active",
                state.get_batch(conn, "batch-20260829-000000")["status"],
            )
        self.assertTrue(os.path.exists(blocked_marker))


    def test_task_retry_rejects_discarded_batch(self) -> None:
        job_id = self.seed_batch(
            batch_status="discarded",
            job_status="failed",
        )
        with mock.patch.object(cli, "_ensure_running_locked") as wake:
            rc, _stdout, stderr = self.capture(
                cli.cmd_retry,
                argparse.Namespace(task="batch-20260829-000000:task"),
            )
        self.assertEqual(1, rc)
        self.assertIn("discarded", stderr)
        wake.assert_not_called()
        with state.connect() as conn:
            self.assertEqual("failed", state.get_job(conn, job_id)["status"])

    def test_discard_claims_writer_before_dependency_unlock_can_activate(
        self,
    ) -> None:
        job_id = self.seed_batch(batch_status="queued", job_status="pending")
        real_update_job = state.update_job
        contender_results: list[str] = []

        def update_after_competing_unlock(conn, target_job_id, **fields):
            contender = sqlite3.connect(state.db_path(), timeout=0)
            try:
                contender.execute(
                    "UPDATE batches SET status='active' WHERE id=?",
                    ("batch-20260829-000000",),
                )
                contender.commit()
                contender_results.append("committed")
            except sqlite3.OperationalError as error:
                self.assertIn("locked", str(error).lower())
                contender_results.append("locked")
            finally:
                contender.rollback()
                contender.close()
            return real_update_job(conn, target_job_id, **fields)

        with mock.patch.object(
            state,
            "update_job",
            side_effect=update_after_competing_unlock,
        ):
            rc, _stdout, stderr = self.capture(
                cli.cmd_discard,
                argparse.Namespace(batch="batch-20260829-000000", yes=True),
            )

        self.assertEqual(0, rc, stderr)
        self.assertEqual(["locked"], contender_results)
        with state.connect() as conn:
            batch = state.get_batch(conn, "batch-20260829-000000")
            job = state.get_job(conn, job_id)
        self.assertEqual("discarded", batch["status"])
        self.assertEqual("cancelled", job["status"])


    def test_x_h02_resubmit_accepts_status_batch_id_reference(self) -> None:
        self.seed_batch(job_status="failed")
        args = argparse.Namespace(
            task="batch-20260829-000000:task", failed=False, resubmit_all=False, dry_run=False
        )
        with mock.patch.object(cli, "_load_cfg", return_value=self.cfg), mock.patch.object(
            state, "launch_marker_active", return_value=False
        ), mock.patch("gsched.fingerprint.compute_fingerprint", return_value=("fp2", None, None)), mock.patch.object(
            cli, "_ensure_running_locked", return_value="awake"
        ):
            rc, _stdout, stderr = self.capture(cli.cmd_resubmit, args)
        self.assertEqual(0, rc, stderr)
        with state.connect() as conn:
            row = conn.execute(
                "SELECT version FROM jobs WHERE batch_id=? ORDER BY version DESC LIMIT 1",
                ("batch-20260829-000000",),
            ).fetchone()
        self.assertEqual(2, row["version"])

    def test_x_h02_cancel_accepts_status_batch_id_reference(self) -> None:
        self.seed_batch(job_status="pending")
        args = argparse.Namespace(
            batch="batch-20260829-000000:task", project=None, bulk_project=None, yes=True
        )
        rc, _stdout, stderr = self.capture(cli.cmd_cancel, args)
        self.assertEqual(0, rc, stderr)
        with state.connect() as conn:
            self.assertEqual(
                "cancelled", state.get_job(conn, "batch-20260829-000000-task-v1")["status"]
            )


class ReviewTransactionAndStateRootTests(TempStateCase):
    def test_s_m03_resubmit_fingerprints_before_opening_write_transaction(self) -> None:
        self.seed_batch(job_status="failed")
        observed = []
        active = {}

        @contextlib.contextmanager
        def tracked_submission_connect():
            conn = sqlite3.connect(state.db_path())
            conn.row_factory = sqlite3.Row
            active["conn"] = conn
            try:
                yield conn
                conn.commit()
            finally:
                active.pop("conn", None)
                conn.close()

        def fingerprint(*_args, **_kwargs):
            conn = active.get("conn")
            observed.append(bool(conn and conn.in_transaction))
            return "fp2", None, None

        args = argparse.Namespace(
            task="batch-20260829-000000:task", failed=False, resubmit_all=False, dry_run=False
        )
        with mock.patch.object(cli, "_load_cfg", return_value=self.cfg), mock.patch.object(
            state, "submission_connect", side_effect=tracked_submission_connect
        ), mock.patch.object(state, "launch_marker_active", return_value=False), mock.patch(
            "gsched.fingerprint.compute_fingerprint", side_effect=fingerprint
        ), mock.patch.object(cli, "_ensure_running_locked", return_value="awake"):
            rc, _stdout, stderr = self.capture(cli.cmd_resubmit, args)
        self.assertEqual(0, rc, stderr)
        self.assertEqual([False], observed)

    def test_s_m04_bootstrap_config_sets_runtime_data_root_without_config_path_drift(self) -> None:
        bootstrap = os.path.join(self.tmp.name, "bootstrap")
        data_root = os.path.join(self.tmp.name, "data")
        os.makedirs(bootstrap)
        bootstrap_config = os.path.join(bootstrap, "config.json")
        cfg = dict(self.cfg, state_dir=data_root)
        with open(bootstrap_config, "w", encoding="utf-8") as stream:
            json.dump(cfg, stream)
        with mock.patch.dict(os.environ, {"SCHED_CONFIG": bootstrap_config}, clear=True):
            self._clear_state_caches()
            loaded = config.load_config()
            self.assertEqual(bootstrap_config, config.config_path())
            self.assertEqual(data_root, config.default_state_dir())
            self.assertEqual(os.path.join(data_root, "review-node", "state.db"), state.db_path())
            dispatcher = Dispatcher(loaded, fake=True)
            self.addCleanup(dispatcher.log.close)
            self.assertEqual(os.path.join(data_root, "review-node"), dispatcher.host_dir)
            self.assertTrue(
                dispatcher._job_log_path({
                    "batch_id": "batch", "task_id": "task", "version": 1
                }).startswith(os.path.join(data_root, "review-node", "logs"))
            )
            dispatcher._write_marker("batch", "done", "ok")
            self.assertTrue(os.path.isfile(
                os.path.join(data_root, "review-node", "markers", "batch.done")
            ))
            self.assertEqual(bootstrap_config, config.config_path())


    def test_rejected_state_dir_hot_reload_cannot_move_runtime_state(self) -> None:
        bootstrap_dir = os.path.join(self.tmp.name, "hot-reload")
        os.makedirs(bootstrap_dir)
        bootstrap_config = os.path.join(bootstrap_dir, "config.json")
        original_root = os.path.join(self.tmp.name, "original-state")
        replacement_root = os.path.join(self.tmp.name, "replacement-state")
        with open(bootstrap_config, "w", encoding="utf-8") as stream:
            json.dump(dict(self.cfg, state_dir=original_root), stream)

        with mock.patch.dict(
            os.environ, {"SCHED_CONFIG": bootstrap_config}, clear=True
        ), mock.patch.object(config, "_runtime_state_dir", None):
            loaded = config.load_config()
            dispatcher = Dispatcher.__new__(Dispatcher)
            dispatcher.cfg = loaded
            dispatcher._config_path = bootstrap_config
            dispatcher.log_line = mock.Mock()
            with open(bootstrap_config, "w", encoding="utf-8") as stream:
                json.dump(dict(self.cfg, state_dir=replacement_root), stream)

            self.assertFalse(dispatcher._reload_config_now())
            self.assertEqual(original_root, config.default_state_dir())

    def test_relative_state_dir_is_resolved_from_bootstrap_config_directory(self) -> None:
        bootstrap_dir = os.path.join(self.tmp.name, "bootstrap", "config")
        elsewhere = os.path.join(self.tmp.name, "elsewhere")
        os.makedirs(bootstrap_dir)
        os.makedirs(elsewhere)
        bootstrap_config = os.path.join(bootstrap_dir, "sched.json")
        relative_state_dir = os.path.join("runtime", "state")
        with open(bootstrap_config, "w", encoding="utf-8") as stream:
            json.dump(dict(self.cfg, state_dir=relative_state_dir), stream)
        expected_root = os.path.join(bootstrap_dir, relative_state_dir)

        original_cwd = os.getcwd()
        try:
            os.chdir(elsewhere)
            with mock.patch.dict(
                os.environ, {"SCHED_CONFIG": bootstrap_config}, clear=True
            ), mock.patch.object(config, "_runtime_state_dir", None):
                self._clear_state_caches()
                config.load_config()
                self.assertEqual(expected_root, config.default_state_dir())
                self.assertEqual(
                    os.path.join(expected_root, "review-node", "state.db"),
                    state.db_path(),
                )
                self.assertEqual(bootstrap_config, config.config_path())
        finally:
            os.chdir(original_cwd)

    def test_node_must_be_a_safe_single_path_component_and_host_paths_stay_contained(self) -> None:
        bootstrap_dir = os.path.join(self.tmp.name, "node-config")
        runtime_root = os.path.join(self.tmp.name, "runtime")
        os.makedirs(bootstrap_dir)
        bootstrap_config = os.path.join(bootstrap_dir, "config.json")

        unsafe_nodes = (
            os.path.join(os.sep, "tmp", "escaped-node"),
            "..",
            os.path.join("..", "escaped-node"),
            os.path.join("nested", "node"),
            r"nested\node",
        )
        for unsafe_node in unsafe_nodes:
            with self.subTest(node=unsafe_node):
                with open(bootstrap_config, "w", encoding="utf-8") as stream:
                    json.dump(
                        dict(self.cfg, node=unsafe_node, state_dir=runtime_root),
                        stream,
                    )
                with mock.patch.dict(
                    os.environ, {"SCHED_CONFIG": bootstrap_config}, clear=True
                ), mock.patch.object(config, "_runtime_state_dir", None):
                    with self.assertRaisesRegex(config.ConfigError, "node"):
                        config.load_config()

        with open(bootstrap_config, "w", encoding="utf-8") as stream:
            json.dump(
                dict(self.cfg, node="safe-node", state_dir=runtime_root),
                stream,
            )
        with mock.patch.dict(
            os.environ, {"SCHED_CONFIG": bootstrap_config}, clear=True
        ), mock.patch.object(config, "_runtime_state_dir", None):
            self._clear_state_caches()
            config.load_config()
            expected_host_dir = os.path.join(runtime_root, "safe-node")
            self.assertEqual(expected_host_dir, daemon._host_dir())
            self.assertEqual(
                os.path.join(expected_host_dir, "state.db"),
                state.db_path(),
            )
            self.assertEqual(
                os.path.realpath(runtime_root),
                os.path.commonpath(
                    (
                        os.path.realpath(runtime_root),
                        os.path.realpath(daemon._host_dir()),
                    )
                ),
            )



class ReviewCleanMigrationAndConfigTests(TempStateCase):
    def _replace_task_spec(self, spec: dict) -> None:
        with state.connect() as conn:
            conn.execute(
                "UPDATE tasks SET spec=? WHERE batch_id=? AND id=? AND version=1",
                (
                    json.dumps(spec),
                    "batch-20260829-000000",
                    "task",
                ),
            )

    def _insert_same_name_batch(self, batch_id: str, status: str) -> None:
        with state.connect() as conn:
            state.insert_batch(
                conn,
                batch_id,
                "batch",
                "mix",
                [],
                None,
                self.tmp.name,
                {},
                project="p",
            )
            conn.execute(
                "UPDATE batches SET status=? WHERE id=?",
                (status, batch_id),
            )

    def test_clean_deletes_latest_task_and_stage_artifacts_with_confined_policy(
        self,
    ) -> None:
        self.seed_batch(batch_status="done", job_status="skip")
        work = os.path.join(self.tmp.name, "work")
        outside = os.path.join(self.tmp.name, "outside")
        os.makedirs(work)
        os.makedirs(outside)
        task_artifact = os.path.join(work, "task.txt")
        stage_artifact = os.path.join(work, "stage.txt")
        escaped_artifact = os.path.join(outside, "escaped.txt")
        explicit_escape = os.path.join(outside, "explicit.txt")
        for path in (
            task_artifact,
            stage_artifact,
            escaped_artifact,
            explicit_escape,
        ):
            with open(path, "w", encoding="utf-8") as stream:
                stream.write("artifact")
        os.symlink(outside, os.path.join(work, "redirect"))
        self._replace_task_spec(
            {
                "id": "task",
                "cwd_abs": work,
                "artifacts": {
                    "task": {"path": "task.txt"},
                },
                "paths_escape": False,
                "stages": [
                    {
                        "artifacts": {"stage": {"path": "stage.txt"}},
                        "paths_escape": False,
                    },
                    {
                        "artifacts": {
                            "explicit": {
                                "path": os.path.join("..", "outside", "explicit.txt")
                            }
                        },
                        "paths_escape": True,
                    },
                ],
            }
        )

        with mock.patch.object(
            cli,
            "_ensure_running_locked",
            return_value="test daemon",
        ) as ensure_running:
            rc, _stdout, stderr = self.capture(
                cli.cmd_clean,
                argparse.Namespace(batch="batch-20260829-000000", yes=True),
            )

        self.assertEqual(0, rc, stderr)
        self.assertFalse(os.path.exists(task_artifact))
        self.assertFalse(os.path.exists(stage_artifact))
        self.assertTrue(os.path.isfile(escaped_artifact))
        self.assertFalse(os.path.exists(explicit_escape))
        ensure_running.assert_called_once_with()

    def test_clean_commit_failure_preserves_artifacts_and_fingerprint_state(
        self,
    ) -> None:
        job_id = self.seed_batch(batch_status="done", job_status="skip")
        artifact = os.path.join(self.tmp.name, "result.txt")
        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write("must survive")
        marker_dir = os.path.join(self.state_root, "review-node", "markers")
        os.makedirs(marker_dir, exist_ok=True)
        done_marker = os.path.join(marker_dir, "batch.done")
        with open(done_marker, "w", encoding="utf-8") as stream:
            stream.write("terminal")
        self._replace_task_spec(
            {
                "id": "task",
                "cwd_abs": self.tmp.name,
                "artifacts": {"result": {"path": "result.txt"}},
                "paths_escape": False,
            }
        )
        real_connect = state.connect
        connect_count = 0

        def connect_with_failed_commit():
            nonlocal connect_count
            connect_count += 1
            if connect_count == 1:
                return real_connect()

            @contextlib.contextmanager
            def failed_commit():
                conn = sqlite3.connect(state.db_path())
                conn.row_factory = sqlite3.Row
                try:
                    yield conn
                    conn.rollback()
                    raise sqlite3.OperationalError("injected commit failure")
                finally:
                    conn.close()

            return failed_commit()

        with mock.patch.object(
            state,
            "connect",
            side_effect=connect_with_failed_commit,
        ), self.assertRaisesRegex(sqlite3.OperationalError, "commit failure"):
            cli.cmd_clean(
                argparse.Namespace(batch="batch-20260829-000000", yes=True)
            )

        self.assertTrue(os.path.isfile(artifact))
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            batch = state.get_batch(conn, "batch-20260829-000000")
        self.assertEqual("skip", job["status"])
        self.assertEqual("fp-1", job["fingerprint"])
        self.assertEqual("done", batch["status"])
        self.assertTrue(os.path.isfile(done_marker))

    def test_clean_mixed_success_only_deletes_and_requeues_latest_skip(self) -> None:
        skip_job_id = self.seed_batch(batch_status="done", job_status="skip")
        skip_artifact = os.path.join(self.tmp.name, "skip.txt")
        done_artifact = os.path.join(self.tmp.name, "done.txt")
        for artifact_path in (skip_artifact, done_artifact):
            with open(artifact_path, "w", encoding="utf-8") as stream:
                stream.write("valid")
        self._replace_task_spec(
            {
                "id": "task",
                "cwd_abs": self.tmp.name,
                "artifacts": {"result": {"path": "skip.txt"}},
                "paths_escape": False,
            }
        )
        done_job_id = "batch-20260829-000000-done-task-v1"
        with state.connect() as conn:
            done_spec = {
                "id": "done-task",
                "cwd_abs": self.tmp.name,
                "artifacts": {"result": {"path": "done.txt"}},
                "paths_escape": False,
            }
            state.insert_task(
                conn,
                "batch-20260829-000000",
                "done-task",
                1,
                done_spec,
                1,
                "p",
            )
            state.insert_job(
                conn,
                done_job_id,
                "batch-20260829-000000",
                "done-task",
                1,
                "done-fp",
                None,
                "p",
            )
            state.update_job(conn, done_job_id, status="done")

        with mock.patch.object(
            cli,
            "_ensure_running_locked",
            return_value="test daemon",
        ):
            rc, _stdout, stderr = self.capture(
                cli.cmd_clean,
                argparse.Namespace(batch="batch-20260829-000000", yes=True),
            )

        self.assertEqual(0, rc, stderr)
        self.assertFalse(os.path.exists(skip_artifact))
        self.assertTrue(os.path.isfile(done_artifact))
        with state.connect() as conn:
            skip_job = state.get_job(conn, skip_job_id)
            done_job = state.get_job(conn, done_job_id)
            batch = state.get_batch(conn, "batch-20260829-000000")
        self.assertEqual("pending", skip_job["status"])
        self.assertEqual("done", done_job["status"])
        self.assertIsNone(skip_job["fingerprint"])
        self.assertIsNone(done_job["fingerprint"])
        self.assertEqual("active", batch["status"])

    def test_clean_requeues_only_latest_skip_and_reopens_done_batch(self) -> None:
        old_job_id = self.seed_batch(batch_status="done", job_status="skip")
        latest_job_id = "batch-20260829-000000-task-v2"
        with state.connect() as conn:
            task = conn.execute(
                "SELECT spec, order_idx FROM tasks"
                " WHERE batch_id=? AND id=? AND version=1",
                ("batch-20260829-000000", "task"),
            ).fetchone()
            state.insert_task(
                conn,
                "batch-20260829-000000",
                "task",
                2,
                json.loads(task["spec"]),
                task["order_idx"],
                "p",
            )
            state.insert_job(
                conn,
                latest_job_id,
                "batch-20260829-000000",
                "task",
                2,
                "fp-2",
                {"stage": "stage-fp-2"},
                "p",
            )
            state.update_job(conn, latest_job_id, status="skip")

        marker_dir = os.path.join(self.state_root, "review-node", "markers")
        os.makedirs(marker_dir, exist_ok=True)
        done_marker = os.path.join(marker_dir, "batch.done")
        with open(done_marker, "w", encoding="utf-8") as stream:
            stream.write("terminal")

        wake_observations = []

        def wake_after_publish() -> str:
            with state.connect() as conn:
                job = state.get_job(conn, latest_job_id)
                batch = state.get_batch(conn, "batch-20260829-000000")
            wake_observations.append(
                (job["status"], batch["status"], os.path.exists(done_marker))
            )
            return "test daemon"

        with mock.patch.object(
            cli,
            "_ensure_running_locked",
            side_effect=wake_after_publish,
        ) as ensure_running:
            rc, stdout, stderr = self.capture(
                cli.cmd_clean,
                argparse.Namespace(batch="batch-20260829-000000", yes=True),
            )

        self.assertEqual(0, rc, stderr)
        self.assertIn("批次已回 active", stdout)
        with state.connect() as conn:
            jobs = conn.execute(
                "SELECT id, status, fingerprint, stage_fingerprints FROM jobs"
                " WHERE batch_id=? ORDER BY version",
                ("batch-20260829-000000",),
            ).fetchall()
            batch = state.get_batch(conn, "batch-20260829-000000")
        self.assertEqual([old_job_id, latest_job_id], [job["id"] for job in jobs])
        self.assertEqual(["skip", "pending"], [job["status"] for job in jobs])
        self.assertTrue(
            all(
                job["fingerprint"] is None and job["stage_fingerprints"] is None
                for job in jobs
            )
        )
        self.assertEqual("active", batch["status"])
        self.assertTrue(os.path.exists(done_marker))
        self.assertEqual([("pending", "active", True)], wake_observations)
        ensure_running.assert_called_once_with()

    def test_clean_does_not_requeue_obsolete_skip_when_latest_is_done(self) -> None:
        old_job_id = self.seed_batch(batch_status="done", job_status="skip")
        latest_job_id = "batch-20260829-000000-task-v2"
        with state.connect() as conn:
            task = conn.execute(
                "SELECT spec, order_idx FROM tasks"
                " WHERE batch_id=? AND id=? AND version=1",
                ("batch-20260829-000000", "task"),
            ).fetchone()
            state.insert_task(
                conn,
                "batch-20260829-000000",
                "task",
                2,
                json.loads(task["spec"]),
                task["order_idx"],
                "p",
            )
            state.insert_job(
                conn,
                latest_job_id,
                "batch-20260829-000000",
                "task",
                2,
                "fp-2",
                None,
                "p",
            )
            state.update_job(conn, latest_job_id, status="done")

        with mock.patch.object(cli, "_ensure_running_locked") as ensure_running:
            rc, _stdout, stderr = self.capture(
                cli.cmd_clean,
                argparse.Namespace(batch="batch-20260829-000000", yes=True),
            )

        self.assertEqual(0, rc, stderr)
        with state.connect() as conn:
            jobs = conn.execute(
                "SELECT id, status, fingerprint FROM jobs"
                " WHERE batch_id=? ORDER BY version",
                ("batch-20260829-000000",),
            ).fetchall()
            batch = state.get_batch(conn, "batch-20260829-000000")
        self.assertEqual([old_job_id, latest_job_id], [job["id"] for job in jobs])
        self.assertEqual(["skip", "done"], [job["status"] for job in jobs])
        self.assertTrue(all(job["fingerprint"] is None for job in jobs))
        self.assertEqual("done", batch["status"])
        ensure_running.assert_not_called()

    def test_clean_removes_artifact_before_publishing_runnable_work(self) -> None:
        job_id = self.seed_batch(batch_status="done", job_status="skip")
        artifact = os.path.join(self.tmp.name, "result.txt")
        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write("old")
        self._replace_task_spec(
            {
                "id": "task",
                "cwd_abs": self.tmp.name,
                "artifacts": {"result": {"path": "result.txt"}},
                "paths_escape": False,
            }
        )
        real_unlink = artifacts.unlink_artifact
        deletion_observations = []

        def unlink_while_terminal(
            cwd,
            path,
            *,
            paths_escape=False,
            raise_on_error=False,
        ):
            with state.connect() as conn:
                job = state.get_job(conn, job_id)
                batch = state.get_batch(conn, "batch-20260829-000000")
            deletion_observations.append(
                (job["status"], job["fingerprint"], batch["status"])
            )
            return real_unlink(
                cwd,
                path,
                paths_escape=paths_escape,
                raise_on_error=raise_on_error,
            )

        with mock.patch.object(
            artifacts,
            "unlink_artifact",
            side_effect=unlink_while_terminal,
        ), mock.patch.object(
            cli,
            "_ensure_running_locked",
            return_value="test daemon",
        ):
            rc, _stdout, stderr = self.capture(
                cli.cmd_clean,
                argparse.Namespace(batch="batch-20260829-000000", yes=True),
            )

        self.assertEqual(0, rc, stderr)
        self.assertEqual([("skip", None, "done")], deletion_observations)
        self.assertFalse(os.path.exists(artifact))
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            batch = state.get_batch(conn, "batch-20260829-000000")
        self.assertEqual("pending", job["status"])
        self.assertEqual("active", batch["status"])

    def test_clean_delete_error_never_publishes_partially_cleaned_batch(
        self,
    ) -> None:
        job_id = self.seed_batch(batch_status="done", job_status="skip")
        first_artifact = os.path.join(self.tmp.name, "first.txt")
        outside = os.path.join(self.tmp.name, "outside")
        os.makedirs(outside)
        second_artifact = os.path.join(outside, "second.txt")
        for artifact_path in (first_artifact, second_artifact):
            with open(artifact_path, "w", encoding="utf-8") as stream:
                stream.write("old")
        os.symlink(outside, os.path.join(self.tmp.name, "redirect"))
        self._replace_task_spec(
            {
                "id": "task",
                "cwd_abs": self.tmp.name,
                "artifacts": {
                    "first": {"path": "first.txt"},
                    "second": {"path": "redirect/second.txt"},
                },
                "paths_escape": False,
            }
        )
        marker_dir = os.path.join(self.state_root, "review-node", "markers")
        os.makedirs(marker_dir, exist_ok=True)
        done_marker = os.path.join(marker_dir, "batch.done")
        with open(done_marker, "w", encoding="utf-8") as stream:
            stream.write("terminal")

        with mock.patch.object(
            cli,
            "_ensure_running_locked",
        ) as ensure_running, self.assertRaisesRegex(
            state.StateError,
            "可能已删除部分产物",
        ):
            cli.cmd_clean(
                argparse.Namespace(batch="batch-20260829-000000", yes=True)
            )

        self.assertFalse(os.path.exists(first_artifact))
        self.assertTrue(os.path.isfile(second_artifact))
        self.assertTrue(os.path.isfile(done_marker))
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            batch = state.get_batch(conn, "batch-20260829-000000")
        self.assertEqual("skip", job["status"])
        self.assertIsNone(job["fingerprint"])
        self.assertEqual("done", batch["status"])
        ensure_running.assert_not_called()

    def test_clean_second_commit_failure_preserves_marker_without_publication(
        self,
    ) -> None:
        job_id = self.seed_batch(batch_status="done", job_status="skip")
        with state.connect() as conn:
            state.insert_batch(
                conn,
                "downstream-id",
                "downstream",
                "mix",
                ["batch"],
                None,
                self.tmp.name,
                {},
                project="p",
            )
        artifact = os.path.join(self.tmp.name, "result.txt")
        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write("old")
        self._replace_task_spec(
            {
                "id": "task",
                "cwd_abs": self.tmp.name,
                "artifacts": {"result": {"path": "result.txt"}},
                "paths_escape": False,
            }
        )
        marker_dir = os.path.join(self.state_root, "review-node", "markers")
        os.makedirs(marker_dir, exist_ok=True)
        done_marker = os.path.join(marker_dir, "batch.done")
        with open(done_marker, "w", encoding="utf-8") as stream:
            stream.write("terminal")
        real_connect = state.connect
        connect_count = 0

        def fail_second_phase_commit():
            nonlocal connect_count
            connect_count += 1
            if connect_count <= 2:
                return real_connect()

            @contextlib.contextmanager
            def failed_commit():
                conn = sqlite3.connect(state.db_path())
                conn.row_factory = sqlite3.Row
                try:
                    yield conn
                    conn.rollback()
                    raise sqlite3.OperationalError("second commit failure")
                finally:
                    conn.close()

            return failed_commit()

        with mock.patch.object(
            state,
            "connect",
            side_effect=fail_second_phase_commit,
        ), self.assertRaisesRegex(sqlite3.OperationalError, "second commit failure"):
            cli.cmd_clean(
                argparse.Namespace(batch="batch-20260829-000000", yes=True)
            )

        self.assertFalse(os.path.exists(artifact))
        self.assertTrue(os.path.isfile(done_marker))
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            batch = state.get_batch(conn, "batch-20260829-000000")
        self.assertEqual("skip", job["status"])
        self.assertIsNone(job["fingerprint"])
        self.assertEqual("done", batch["status"])
        self.assertEqual(["batch.done"], os.listdir(marker_dir))

        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.host_dir = state.host_dir()
        dispatcher.log_line = mock.Mock()
        dispatcher._batch_has_unresolved_launch_marker = mock.Mock(
            return_value=False
        )
        dispatcher._unlock_dependent_batches()
        with state.connect() as conn:
            downstream = state.get_batch(conn, "downstream-id")
        self.assertEqual("queued", downstream["status"])

    def test_clean_leaves_marker_for_pre_dispatch_reconciliation(self) -> None:
        job_id = self.seed_batch(batch_status="done", job_status="skip")
        marker_dir = os.path.join(self.state_root, "review-node", "markers")
        os.makedirs(marker_dir, exist_ok=True)
        done_marker = os.path.join(marker_dir, "batch.done")
        with open(done_marker, "w", encoding="utf-8") as stream:
            stream.write("terminal")

        with mock.patch.object(
            cli,
            "_ensure_running_locked",
            return_value="test daemon",
        ) as ensure_running:
            rc, _stdout, stderr = self.capture(
                cli.cmd_clean,
                argparse.Namespace(batch="batch-20260829-000000", yes=True),
            )

        self.assertEqual(0, rc, stderr)
        self.assertTrue(os.path.isfile(done_marker))
        ensure_running.assert_called_once_with()
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            batch = state.get_batch(conn, "batch-20260829-000000")
        self.assertEqual("pending", job["status"])
        self.assertEqual("active", batch["status"])

        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher.host_dir = os.path.join(self.state_root, "review-node")
        dispatcher.log_line = mock.Mock()
        with state.connect() as conn:
            ready = conn.execute(
                "SELECT j.*, b.name AS batch_name FROM jobs j"
                " JOIN batches b ON b.id=j.batch_id"
                " WHERE j.id=?",
                (job_id,),
            ).fetchall()
            dispatcher._reconcile_ready_batch_markers(conn, ready)

        self.assertFalse(os.path.exists(done_marker))

    def test_clean_old_same_name_batch_preserves_newer_done_marker(self) -> None:
        old_job_id = self.seed_batch(batch_status="done", job_status="skip")
        newer_batch_id = "batch-20260830-000000"
        with state.connect() as conn:
            task = conn.execute(
                "SELECT spec FROM tasks WHERE batch_id=? AND id='task' AND version=1",
                ("batch-20260829-000000",),
            ).fetchone()
            state.insert_batch(
                conn,
                newer_batch_id,
                "batch",
                "mix",
                [],
                None,
                self.tmp.name,
                {},
                project="p",
            )
            conn.execute(
                "UPDATE batches SET status='done' WHERE id=?",
                (newer_batch_id,),
            )
            state.insert_task(
                conn,
                newer_batch_id,
                "task",
                1,
                json.loads(task["spec"]),
                0,
                "p",
            )
            state.insert_job(
                conn,
                f"{newer_batch_id}-task-v1",
                newer_batch_id,
                "task",
                1,
                "new-fp",
                None,
                "p",
            )
            state.update_job(
                conn,
                f"{newer_batch_id}-task-v1",
                status="done",
            )
        marker_dir = os.path.join(self.state_root, "review-node", "markers")
        os.makedirs(marker_dir, exist_ok=True)
        done_marker = os.path.join(marker_dir, "batch.done")
        with open(done_marker, "w", encoding="utf-8") as stream:
            stream.write("newer terminal batch")

        with mock.patch.object(
            cli,
            "_ensure_running_locked",
            return_value="test daemon",
        ):
            rc, _stdout, stderr = self.capture(
                cli.cmd_clean,
                argparse.Namespace(batch="batch-20260829-000000", yes=True),
            )

        self.assertEqual(0, rc, stderr)
        self.assertTrue(os.path.isfile(done_marker))
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.host_dir = os.path.join(self.state_root, "review-node")
        with state.connect() as conn:
            old_job = state.get_job(conn, old_job_id)
            old_batch = state.get_batch(conn, "batch-20260829-000000")
            ready = conn.execute(
                "SELECT j.*, b.name AS batch_name FROM jobs j"
                " JOIN batches b ON b.id=j.batch_id WHERE j.id=?",
                (old_job_id,),
            ).fetchall()
            dispatcher._reconcile_ready_batch_markers(conn, ready)
        self.assertEqual("pending", old_job["status"])
        self.assertEqual("active", old_batch["status"])
        self.assertTrue(os.path.isfile(done_marker))

        with state.connect() as conn:
            state.update_job(conn, old_job_id, status="done")
        dispatcher.log_line = mock.Mock()
        dispatcher._notify_batch = mock.Mock()
        dispatcher._settle_batch_status()
        with open(done_marker, encoding="utf-8") as stream:
            self.assertEqual("newer terminal batch", stream.read())

    def test_clean_rejects_same_name_queued_owner_before_artifact_deletion(
        self,
    ) -> None:
        job_id = self.seed_batch(batch_status="done", job_status="skip")
        artifact = os.path.join(self.tmp.name, "shared-result.txt")
        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write("must survive")
        self._replace_task_spec(
            {
                "id": "task",
                "cwd_abs": self.tmp.name,
                "artifacts": {"result": {"path": "shared-result.txt"}},
                "paths_escape": False,
            }
        )
        newer_batch_id = "batch-20260830-queued"
        self._insert_same_name_batch(newer_batch_id, "queued")

        with mock.patch.object(cli, "_ensure_running_locked") as ensure_running:
            rc, _stdout, stderr = self.capture(
                cli.cmd_clean,
                argparse.Namespace(batch="batch-20260829-000000", yes=True),
            )

        self.assertEqual(1, rc)
        self.assertIn(newer_batch_id, stderr)
        self.assertTrue(os.path.isfile(artifact))
        with state.connect() as conn:
            old_batch = state.get_batch(conn, "batch-20260829-000000")
            old_job = state.get_job(conn, job_id)
        self.assertEqual("done", old_batch["status"])
        self.assertEqual("skip", old_job["status"])
        self.assertEqual("fp-1", old_job["fingerprint"])
        ensure_running.assert_not_called()

    def test_clean_phase_two_rechecks_same_name_nonterminal_owner(self) -> None:
        job_id = self.seed_batch(batch_status="done", job_status="skip")
        artifact = os.path.join(self.tmp.name, "phase-two-result.txt")
        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write("fixture")
        self._replace_task_spec(
            {
                "id": "task",
                "cwd_abs": self.tmp.name,
                "artifacts": {"result": {"path": "phase-two-result.txt"}},
                "paths_escape": False,
            }
        )
        newer_batch_id = "batch-20260830-late"

        def inject_conflict(*_args, **_kwargs) -> bool:
            self._insert_same_name_batch(newer_batch_id, "active")
            return False

        with mock.patch.object(
            artifacts,
            "unlink_artifact",
            side_effect=inject_conflict,
        ), self.assertRaisesRegex(
            state.StateError,
            newer_batch_id,
        ):
            cli.cmd_clean(
                argparse.Namespace(batch="batch-20260829-000000", yes=True)
            )

        with state.connect() as conn:
            old_batch = state.get_batch(conn, "batch-20260829-000000")
            old_job = state.get_job(conn, job_id)
        self.assertEqual("done", old_batch["status"])
        self.assertEqual("skip", old_job["status"])
        self.assertIsNone(old_job["fingerprint"])

    def test_clean_phase_two_claims_writer_before_daemon_settlement(self) -> None:
        job_id = self.seed_batch(batch_status="blocked", job_status="skip")
        real_conflict_check = cli._same_name_nonterminal_conflict
        check_count = 0
        contender_results: list[str] = []

        def check_after_competing_settle(conn, batch_id):
            nonlocal check_count
            check_count += 1
            if check_count == 2:
                contender = sqlite3.connect(state.db_path(), timeout=0)
                try:
                    contender.execute(
                        "UPDATE batches SET status='done' WHERE id=?",
                        (batch_id,),
                    )
                    contender.commit()
                    contender_results.append("committed")
                except sqlite3.OperationalError as error:
                    self.assertIn("locked", str(error).lower())
                    contender_results.append("locked")
                finally:
                    contender.rollback()
                    contender.close()
            return real_conflict_check(conn, batch_id)

        with mock.patch.object(
            cli,
            "_same_name_nonterminal_conflict",
            side_effect=check_after_competing_settle,
        ), mock.patch.object(cli, "_ensure_running_locked") as ensure_running:
            rc, _stdout, stderr = self.capture(
                cli.cmd_clean,
                argparse.Namespace(batch="batch-20260829-000000", yes=True),
            )

        self.assertEqual(0, rc, stderr)
        self.assertEqual(2, check_count)
        self.assertEqual(["locked"], contender_results)
        with state.connect() as conn:
            old_batch = state.get_batch(conn, "batch-20260829-000000")
            old_job = state.get_job(conn, job_id)
        self.assertEqual("blocked", old_batch["status"])
        self.assertEqual("pending", old_job["status"])
        ensure_running.assert_not_called()

    def test_clean_cancel_before_dispatch_replaces_done_with_blocked_marker(
        self,
    ) -> None:
        job_id = self.seed_batch(batch_status="done", job_status="skip")
        marker_dir = os.path.join(self.state_root, "review-node", "markers")
        os.makedirs(marker_dir, exist_ok=True)
        done_marker = os.path.join(marker_dir, "batch.done")
        blocked_marker = os.path.join(marker_dir, "batch.blocked")
        with open(done_marker, "w", encoding="utf-8") as stream:
            stream.write("old done")
        with mock.patch.object(
            cli,
            "_ensure_running_locked",
            return_value="test daemon",
        ):
            rc, _stdout, stderr = self.capture(
                cli.cmd_clean,
                argparse.Namespace(batch="batch-20260829-000000", yes=True),
            )
        self.assertEqual(0, rc, stderr)
        with state.connect() as conn:
            state.update_job(
                conn,
                job_id,
                status="cancelled",
                finished_at=state.now(),
            )

        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.cfg = self.cfg
        dispatcher.host_dir = os.path.join(self.state_root, "review-node")
        dispatcher.log_line = mock.Mock()
        dispatcher._notify_batch = mock.Mock()
        dispatcher._settle_batch_status()

        with state.connect() as conn:
            batch = state.get_batch(conn, "batch-20260829-000000")
        self.assertEqual("blocked", batch["status"])
        self.assertFalse(os.path.exists(done_marker))
        self.assertTrue(os.path.isfile(blocked_marker))

    def test_clean_refuses_active_idle_shutdown_without_side_effects(self) -> None:
        job_id = self.seed_batch(batch_status="done", job_status="skip")
        artifact = os.path.join(self.tmp.name, "result.txt")
        with open(artifact, "w", encoding="utf-8") as stream:
            stream.write("old")
        self._replace_task_spec(
            {
                "id": "task",
                "cwd_abs": self.tmp.name,
                "artifacts": {"result": {"path": "result.txt"}},
                "paths_escape": False,
            }
        )

        with mock.patch.object(
            state,
            "submission_shutdown_active",
            return_value=True,
        ), self.assertRaises(state.SubmissionBlocked):
            cli.cmd_clean(
                argparse.Namespace(batch="batch-20260829-000000", yes=True)
            )

        self.assertTrue(os.path.isfile(artifact))
        with state.connect() as conn:
            job = state.get_job(conn, job_id)
            batch = state.get_batch(conn, "batch-20260829-000000")
        self.assertEqual("skip", job["status"])
        self.assertEqual("fp-1", job["fingerprint"])
        self.assertEqual("done", batch["status"])

    def test_init_db_migration_failure_rolls_back_all_schema_steps(self) -> None:
        database = state.db_path()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(database + suffix)
            except FileNotFoundError:
                pass

        def fail_mid_migration(conn):
            conn.execute("CREATE TABLE injected_partial (value INTEGER)")
            raise sqlite3.OperationalError("injected migration failure")

        with mock.patch.object(
            state,
            "migrate_project_columns",
            side_effect=fail_mid_migration,
        ), self.assertRaisesRegex(sqlite3.OperationalError, "migration failure"):
            state.init_db()

        with contextlib.closing(sqlite3.connect(database)) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertEqual(set(), tables)

    def test_main_fails_closed_when_database_migration_fails(self) -> None:
        with mock.patch.object(
            state,
            "init_db",
            side_effect=sqlite3.OperationalError("injected migration failure"),
        ), mock.patch.object(cli, "cmd_clean") as clean:
            rc, _stdout, stderr = self.capture(
                cli.main,
                ["clean", "batch-20260829-000000", "--yes"],
            )

        self.assertNotEqual(0, rc)
        self.assertIn("初始化", stderr)
        clean.assert_not_called()

    def test_config_request_replay_reports_applied_file_when_reload_enqueue_fails(
        self,
    ) -> None:
        patch_path = os.path.join(self.tmp.name, "config-patch.json")
        with open(patch_path, "w", encoding="utf-8") as stream:
            json.dump({"projects": {"p": {"gpu_quota": 2}}}, stream)
        args = argparse.Namespace(
            request_id="config-applied-reload-failed",
            command=["config", "set", "-f", patch_path, "--yes"],
            expect_kind="none",
            expect_id=None,
            expect_status=None,
            expect_version=None,
            expect_quarantined=None,
            expect_revision=0,
            expect_assignments_json=None,
        )

        with mock.patch.object(
            state,
            "insert_control_request",
            side_effect=sqlite3.OperationalError("reload enqueue failed"),
        ) as reload_request:
            first = self.capture(cli.cmd_request, args)
            replay = self.capture(cli.cmd_request, args)

        self.assertEqual(0, first[0])
        self.assertEqual(first, replay)
        self.assertIn("配置文件已应用", first[1] + first[2])
        self.assertNotIn("未写入", first[1] + first[2])
        reload_request.assert_called_once()
        with open(self.config_path, encoding="utf-8") as stream:
            applied = json.load(stream)
        self.assertEqual(2, applied["projects"]["p"]["gpu_quota"])
        with state.connect() as conn:
            request = conn.execute(
                "SELECT status, code FROM operation_requests WHERE request_id=?",
                (args.request_id,),
            ).fetchone()
        self.assertEqual("done", request["status"])
        self.assertEqual(0, request["code"])


class ReviewAcceptanceCleanupTests(unittest.TestCase):
    def test_failed_daemon_stop_preserves_all_claimed_auxiliary_roots(self) -> None:
        repo = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        with tempfile.TemporaryDirectory() as temp:
            state_root = os.path.join(temp, "state")
            auxiliary_root = os.path.join(temp, "project")
            record = os.path.join(temp, "removed.txt")
            os.mkdir(state_root)
            os.mkdir(auxiliary_root)
            os.mkdir(os.path.join(state_root, "runtime"))
            with open(
                os.path.join(state_root, "runtime", "state.db"),
                "wb",
            ):
                pass
            with open(
                os.path.join(state_root, "config.json"),
                "w",
                encoding="utf-8",
            ) as stream:
                stream.write("{}\n")
            script = r"""
set -u
cd "$1"
state_root=$2
auxiliary_root=$3
record=$4
source tests/acceptance_cleanup.sh
SCHED_ACCEPT_CLEANUP_ROOTS=("$auxiliary_root" "$state_root")
SCHED_ACCEPT_CLEANUP_TOKENS=("aux-token" "state-token")
SCHED_ACCEPT_CLEANUP_DEVS=("1" "1")
SCHED_ACCEPT_CLEANUP_INOS=("1" "1")
SCHED_ACCEPT_CLEANUP_ANCHORS=("aux-anchor" "state-anchor")
SCHED_ACCEPT_CLEANUP_MARKER_DEVS=("2" "2")
SCHED_ACCEPT_CLEANUP_MARKER_INOS=("2" "2")
sched_accept_root_owned() { return 0; }
sched_accept_stop_daemon() { return 1; }
sched_accept_verify_quiescent() { return 0; }
sched_accept_remove_claimed_root() { printf '%s\n' "$1" >> "$record"; }
sched_accept_cleanup
"""
            completed = subprocess.run(
                [
                    "bash",
                    "-c",
                    script,
                    "cleanup-test",
                    repo,
                    state_root,
                    auxiliary_root,
                    record,
                ],
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)
            self.assertFalse(
                os.path.exists(record),
                "auxiliary roots must be preserved when daemon shutdown is unproven",
            )


if __name__ == "__main__":
    unittest.main()
