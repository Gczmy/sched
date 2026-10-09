from __future__ import annotations

import argparse
import copy
import getpass
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
from unittest import mock

from gsched import cli, daemon, resources, state, supervisor, recovery, recovery_state
from gsched.dispatcher import Dispatcher
from gsched.executor import process_start_token
from test_review_cli_state import TempStateCase
import test_recovery_queue as queue_fixture


class FakeChild:
    def __init__(self, rc):
        self.pid = 123456
        self.rc = rc
    def poll(self): return self.rc
    def wait(self): return self.rc
    def send_signal(self, sig): self.rc = 0


class SupervisorUnitTests(TempStateCase):
    def setUp(self):
        super().setUp()
        self.preflight = mock.patch.object(daemon, "check", return_value=[])
        self.preflight.start()
        self.addCleanup(self.preflight.stop)

    def test_restarts_only_after_owned_child_wait_and_normal_exit_finishes(self):
        failed, success = FakeChild(-9), FakeChild(0)
        with mock.patch.object(subprocess, "Popen", side_effect=[failed, success]) as launch, mock.patch.object(supervisor.time, "sleep"), mock.patch.object(supervisor.time, "monotonic", side_effect=[0, 2, 3]):
            self.assertEqual(0, supervisor.foreground(fake=True, supervise=True, restart_delay_sec=1))
        self.assertEqual(2, launch.call_count)
        self.assertFalse(os.path.exists(os.path.join(state.host_dir(), supervisor.CONTROL)))

    def test_one_shot_and_restart_limit_do_not_loop_forever(self):
        with mock.patch.object(subprocess, "Popen", return_value=FakeChild(1)) as launch:
            self.assertEqual(1, supervisor.foreground(fake=True))
            self.assertEqual(1, launch.call_count)
        with mock.patch.object(subprocess, "Popen", return_value=FakeChild(1)) as launch, mock.patch.object(supervisor.time, "sleep"), mock.patch.object(supervisor.time, "monotonic", side_effect=[0, 2, 3]):
            self.assertEqual(1, supervisor.foreground(fake=True, supervise=True, restart_delay_sec=1, max_restarts=1))
            self.assertEqual(2, launch.call_count)

    def test_stop_in_restart_gap_prevents_new_child(self):
        def sleep(_): supervisor.request_stop()
        with mock.patch.object(subprocess, "Popen", return_value=FakeChild(-9)) as launch, mock.patch.object(supervisor.time, "sleep", side_effect=sleep), mock.patch.object(supervisor.time, "monotonic", side_effect=[0, 0, 0]):
            self.assertEqual(0, supervisor.foreground(fake=True, supervise=True, restart_delay_sec=1))
            self.assertEqual(1, launch.call_count)

    def test_stale_heartbeat_never_allows_live_lease_takeover(self):
        dispatcher = Dispatcher(self.cfg, fake=True)
        self.addCleanup(dispatcher.log.close)
        self.assertTrue(dispatcher.acquire_lock())
        self.addCleanup(dispatcher._cleanup_lock)
        os.utime(daemon._heartbeat_file(), (1, 1))
        with mock.patch.object(subprocess, "Popen") as launch:
            self.assertEqual(1, supervisor.foreground(fake=True, supervise=True))
        launch.assert_not_called()

    def test_foreign_or_invalid_lease_and_duplicate_supervisor_fail_closed(self):
        dispatcher = Dispatcher(self.cfg, fake=True)
        self.addCleanup(dispatcher.log.close)
        owner = {"schema_version": 1, "lease_id": "l", "pid": os.getpid(), "start_token": process_start_token(os.getpid()), "physical_host": "foreign"}
        state.ensure_private_directory(dispatcher.lock_dir)
        with state.open_private_text(dispatcher._lock_owner_file(), "w") as stream: json.dump(owner, stream)
        with mock.patch.object(subprocess, "Popen") as launch:
            self.assertEqual(1, supervisor.foreground(fake=True))
            Path(dispatcher._lock_owner_file()).write_text("invalid")
            self.assertEqual(1, supervisor.foreground(fake=True))
        launch.assert_not_called()

    def test_unknown_control_prevents_restart_but_cannot_cancel_running_child(self):
        child = FakeChild(None)
        poll_count = 0
        def poll():
            nonlocal poll_count
            poll_count += 1
            return None if poll_count < 3 else 0
        child.poll = poll
        child.wait = lambda: 0
        child.send_signal = mock.Mock()
        with mock.patch.object(subprocess, "Popen", return_value=child) as launch, mock.patch.object(supervisor, "_explicit_stop", return_value=False), mock.patch.object(supervisor, "_stopped", side_effect=[False, True]), mock.patch.object(supervisor.time, "sleep"):
            self.assertEqual(1, supervisor.foreground(fake=True, supervise=True))
        child.send_signal.assert_not_called()
        self.assertEqual(1, launch.call_count)

    def test_duplicate_supervisor_mutex_prevents_child_creation(self):
        import fcntl
        path = os.path.join(state.host_dir(), "daemon.supervisor.lock")
        with state.open_private_text(path, "a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            with mock.patch.object(subprocess, "Popen") as launch:
                self.assertEqual(1, supervisor.foreground(fake=True, supervise=True))
            launch.assert_not_called()

    def test_foreground_options_and_compute_node_guard(self):
        with mock.patch.dict(os.environ, {"SCHED_ALLOW_FOREIGN_WRITE": "0"}), mock.patch("socket.gethostname", return_value="other"):
            rc, _, error = self.capture(cli.main, ["daemon", "foreground", "--supervise", "--fake"])
            self.assertEqual(2, rc, error)
        rc, _, error = self.capture(cli.main, ["daemon", "status", "--supervise"])
        self.assertEqual(1, rc, error)
        for value in (float("nan"), 0, 301, True):
            with self.assertRaises(ValueError): supervisor.foreground(fake=True, restart_delay_sec=value)

    def test_fresh_ownerless_lock_stays_protected_but_verified_dead_owner_can_reclaim(self):
        old = Dispatcher(self.cfg, fake=True)
        self.addCleanup(old.log.close)
        state.ensure_private_directory(old.lock_dir)
        new = Dispatcher(self.cfg, fake=True)
        self.addCleanup(new.log.close)
        self.assertFalse(new.acquire_lock())
        owner = {"schema_version": 1, "lease_id": "dead", "pid": 999999999, "start_token": "proc:1", "physical_host": socket.gethostname()}
        with state.open_private_text(old._lock_owner_file(), "w") as stream: json.dump(owner, stream)
        with mock.patch.object(new, "_pid_exists", return_value=False): self.assertTrue(new.acquire_lock())
        new._cleanup_lock()


class RecoverySettlementConvergenceTests(TempStateCase):
    spec = queue_fixture.RecoveryQueueTests.spec
    task = queue_fixture.RecoveryQueueTests.task
    seed = queue_fixture.RecoveryQueueTests.seed
    store = queue_fixture.RecoveryQueueTests.store
    latest = queue_fixture.RecoveryQueueTests.latest

    def setUp(self):
        super().setUp()
        self.root = Path(self.tmp.name)
        self.worker = self.root / "worker.py"
        self.worker.write_bytes((Path(__file__).parent / "fixtures" / "recovery_worker.py").read_bytes())
        (self.root / "settings.json").write_text(json.dumps({"total": 5, "oom_at": [2]}))
        self.cfg["projects"]["p"]["root"] = str(self.root)

    def test_direct_cli_submission_persists_recovery_binding_and_context_authority(self):
        task = self.task("smoke")
        batch = self.root / "submit.json"
        batch.write_text(json.dumps({"name": "direct", "project": "p", "cwd": str(self.root), "tasks": [task]}))
        Path(self.config_path).write_text(json.dumps(self.cfg))
        with mock.patch.object(cli, "_ensure_running_locked", return_value="test daemon"):
            rc, out, error = self.capture(cli.main, ["submit", str(batch), "--json"])
        self.assertEqual(0, rc, error)
        bid = json.loads(out)["batch_id"]
        with state.connect() as conn:
            stored = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=?", (bid,)).fetchone()[0])
            job = conn.execute("SELECT * FROM jobs WHERE batch_id=?", (bid,)).fetchone()
        self.assertEqual("smoke", stored["recovery"]["mode"])
        self.assertEqual(job["fingerprint"], stored["_recovery_fingerprint"])
        recovery.verify_binding(stored, job["fingerprint"])
        self.assertEqual(job["id"], json.loads(recovery.context(state.host_dir(), job, stored))["job_id"])

    def test_restart_reconciles_recorded_authority_after_atomic_publication_failure(self):
        spec = self.spec("run")
        job = self.seed("formal", spec, "failed")
        self.store(job, spec).save({"next": 2})
        with state.connect() as conn:
            state.update_job(conn, job["id"], rc=42, failure="oom")
            recovery_state.record_clean(conn, job, spec, "oom", "wait")
            with mock.patch.object(state, "insert_job", side_effect=RuntimeError("transaction failure")), self.assertRaises(RuntimeError):
                recovery_state.create_next(conn, state.host_dir(), job, spec)
        dispatcher = Dispatcher(self.cfg, fake=True)
        self.addCleanup(dispatcher.log.close)
        dispatcher._reconcile_recovery_settlements()
        dispatcher._reconcile_recovery_settlements()
        self.assertEqual(2, self.latest()["version"])
        with state.connect() as conn:
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM recovery_queue").fetchone()[0])
            self.assertEqual(("failed", 42), tuple(conn.execute("SELECT status,rc FROM jobs WHERE id=?", (job["id"],)).fetchone()))


class SupervisorProcessTests(TempStateCase):
    def setUp(self):
        super().setUp()
        self.cfg["user"] = getpass.getuser()
        self.cfg["max_cpu_jobs"] = 1
        # These isolated execution fixtures do not exercise lease admission.
        # Dedicated cluster-lease tests cover the fail-closed production guard.
        self.cfg["lease_validation"] = {"mode": "observe"}
        Path(self.config_path).write_text(json.dumps(self.cfg))
        self.repo = str(Path(__file__).resolve().parents[1])
        self.env = dict(os.environ, PYTHONPATH=self.repo, PYTHONDONTWRITEBYTECODE="1")
        self.output_path = Path(self.tmp.name) / "foreground.log"
        self.output = self.output_path.open("w")
        self.addCleanup(self.output.close)
        self.proc = None
        self.addCleanup(self.shutdown)

    def launch(self, *, supervise=True, delay=1):
        program = "from gsched import dispatcher; dispatcher.POLL_SEC=1; from gsched import cli; raise SystemExit(cli.main(" + repr(["daemon", "foreground", "--fake", "--restart-delay-sec", str(delay), "--max-restarts", "3"] + (["--supervise"] if supervise else [])) + "))"
        self.proc = subprocess.Popen([sys.executable, "-c", program], env=self.env, stdout=self.output, stderr=subprocess.STDOUT)
        return self.wait_owner()

    def wait_owner(self, old=None, timeout=45):
        deadline = time.monotonic()+timeout
        while time.monotonic()<deadline:
            if self.proc.poll() is not None:
                self.fail(self.output_path.read_text())
            owner = daemon._read_lease_owner()
            if owner and owner["pid"] != old and daemon._started_child_ready(owner["pid"], owner["start_token"]):
                return owner
            time.sleep(.1)
        self.fail(str(daemon.health_snapshot())+self.output_path.read_text())

    def shutdown(self):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try: self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                daemon.stop()
                self.proc.kill()
                self.proc.wait(timeout=5)

    def test_actual_killed_daemon_restarts_but_explicit_stop_ends_supervisor(self):
        owner = self.launch()
        os.kill(owner["pid"], signal.SIGKILL)
        replacement = self.wait_owner(old=owner["pid"])
        self.assertNotEqual(owner["lease_id"], replacement["lease_id"])
        message = daemon.stop()
        self.assertNotIn("失败", message)
        self.assertEqual(0, self.proc.wait(timeout=15), self.output_path.read_text())
        self.assertFalse(os.path.exists(daemon._owner_file()))

    def test_stop_during_real_restart_delay_never_starts_replacement(self):
        owner = self.launch(delay=3)
        os.kill(owner["pid"], signal.SIGKILL)
        deadline = time.monotonic()+10
        while "exit rc=-9" not in self.output_path.read_text() and time.monotonic()<deadline: time.sleep(.05)
        daemon.stop()
        self.assertEqual(0, self.proc.wait(timeout=10), self.output_path.read_text())
        self.assertEqual(1, self.output_path.read_text().count("foreground daemon pid="))

    def test_drain_stop_when_idle_is_not_restarted_and_resume_does_not_spawn(self):
        self.launch()
        resources.set_drain(stop=True)
        self.assertEqual(0, self.proc.wait(timeout=15), self.output_path.read_text())
        self.assertIsNotNone(resources.drain_state())
        resources.resume()
        self.assertFalse(os.path.exists(daemon._owner_file()))
        self.assertEqual(1, self.output_path.read_text().count("foreground daemon pid="))

    def test_supervisor_signal_stops_child_without_restart(self):
        self.launch()
        self.proc.terminate()
        self.assertEqual(0, self.proc.wait(timeout=15), self.output_path.read_text())
        self.assertFalse(os.path.exists(daemon._owner_file()))
        self.assertEqual(1, self.output_path.read_text().count("foreground daemon pid="))

    def cli_data(self, *args):
        result = subprocess.run([sys.executable, "-m", "gsched.cli", *args], env=self.env, capture_output=True, text=True, timeout=20)
        self.assertEqual(0, result.returncode, result.stdout+result.stderr)
        return json.loads(result.stdout)

    def wait_status(self, batch, status, timeout=60):
        deadline = time.monotonic()+timeout
        while time.monotonic()<deadline:
            data = self.cli_data("status", batch, "--json")
            if data["jobs"] and data["jobs"][0]["status"] == status:
                return data["jobs"][0]
            if data["jobs"] and data["jobs"][0]["status"] in ("blocked", "failed", "cancelled"):
                logs = subprocess.run([sys.executable, "-m", "gsched.cli", "log", f"{batch}:task", "-n", "60"], env=self.env, capture_output=True, text=True, timeout=15)
                self.fail(str(data)+logs.stdout+logs.stderr+self.output_path.read_text())
            time.sleep(.2)
        self.fail(str(data)+self.output_path.read_text())

    def native_group(self):
        from gsched.execution import BackendUnavailable, LinuxFdBackend
        try: LinuxFdBackend()
        except BackendUnavailable:
            if os.environ.get("SCHED_REQUIRE_NATIVE") == "1": raise
            self.skipTest("native backend was not explicitly built")
        root = Path(self.tmp.name)
        (root / "worker.py").write_bytes((Path(__file__).parent / "fixtures" / "recovery_worker.py").read_bytes())
        (root / "settings.json").write_text(json.dumps({"total": 5, "hold_step": 2}))
        executable = os.path.realpath(sys.executable)
        self.cfg["execution_backends"] = {"group": {"kind": "linux_fd_owner", "executable": executable, "sha256": hashlib.sha256(Path(executable).read_bytes()).hexdigest(), "argv": [executable, "worker.py"], "env": {"PYTHONPATH": self.repo, "CHECK_IDENTITY_FD": "1"}, "projects": ["p"], "input_slots": {}}}
        Path(self.config_path).write_text(json.dumps(self.cfg))
        owner = self.launch()
        declaration = {"protocol": recovery.PROTOCOL, "mode": "smoke", "code": {"worker.py": hashlib.sha256((root / "worker.py").read_bytes()).hexdigest()}, "config": {"settings.json": hashlib.sha256((root / "settings.json").read_bytes()).hexdigest()}, "inputs": {}, "retry": {"cooldown_sec": 0}}
        task = {"id": "task", "cmd": [executable, "worker.py"], "git": False, "resources": {"gpu": 0}, "max_retry": 0, "execution": {"backend": "group", "inputs": {}}, "recovery": declaration}
        batch_path = root / "batch.json"
        batch_path.write_text(json.dumps({"name": "smoke", "project": "p", "tasks": [task]}))
        smoke = self.cli_data("submit", str(batch_path), "--json")["batch_id"]
        smoke_job = self.wait_status(smoke, "done")
        task["recovery"] = dict(declaration, mode="run", smoke_job_id=smoke_job["id"])
        batch_path.write_text(json.dumps({"name": "formal", "project": "p", "tasks": [task]}))
        batch = self.cli_data("submit", str(batch_path), "--json")["batch_id"]
        running = self.wait_status(batch, "running")
        deadline = time.monotonic()+10
        while not (root / "starts.jsonl").exists() and time.monotonic()<deadline: time.sleep(.05)
        self.assertTrue((root / "starts.jsonl").exists())
        return owner, batch, running

    def test_persistent_original_wait_survives_actual_supervisor_restart_without_new_attempt(self):
        owner, batch, job = self.native_group()
        original = self.cli_data("execution", f"{batch}:task", "--json")["attempts"][0]
        os.kill(owner["pid"], signal.SIGKILL)
        self.wait_owner(old=owner["pid"])
        (Path(self.tmp.name) / "release").touch()
        done = self.wait_status(batch, "done")
        final = self.cli_data("execution", f"{batch}:task", "--json")["attempts"]
        self.assertEqual(1, done["version"])
        self.assertEqual(1, len(final))
        self.assertEqual(original["attempt_id"], final[0]["attempt_id"])
        self.assertEqual(0, final[0]["observation"]["returncode"])
        self.assertEqual(1, len((Path(self.tmp.name) / "starts.jsonl").read_text().splitlines()))

    def test_forced_worker_kill_uses_last_checkpoint_and_new_version_with_original_wait(self):
        owner, batch, job = self.native_group()
        root = Path(self.tmp.name)
        deadline = time.monotonic()+10
        while time.monotonic()<deadline:
            observed = self.cli_data("recovery", f"{batch}:task", "--json")
            if observed["checkpoint"]["state"] == "verified": break
            time.sleep(.05)
        self.assertEqual("verified", observed["checkpoint"]["state"])
        old = self.cli_data("execution", f"{batch}:task", "--json")["attempts"][0]
        os.kill(old["observation"]["pid"], signal.SIGKILL)
        (root / "release").touch()
        done = self.wait_status(batch, "done", timeout=60)
        self.assertEqual(2, done["version"])
        self.assertEqual(list(range(5)), json.loads((root / "result.json").read_text())["results"])
        attempts = self.cli_data("execution", f"{batch}:task", "--json")["attempts"]
        self.assertEqual(2, len(attempts))
        first = next(row for row in attempts if row["attempt_id"] == old["attempt_id"])
        self.assertEqual(-9, first["observation"]["returncode"])
        self.assertTrue(first["observation"]["group_clean"])
        self.assertNotEqual(attempts[0]["attempt_id"], attempts[1]["attempt_id"])
        self.assertEqual(2, len((root / "starts.jsonl").read_text().splitlines()))
