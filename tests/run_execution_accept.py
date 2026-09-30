"""Bounded, real local CLI/daemon acceptance with self-contained CPU workers.

Run on Linux after explicitly building the optional backend. The script creates
only a private temporary scheduler instance. It never contacts another host.
"""
from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time


WORKER = r'''
#include <ctype.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
int main(int argc, char **argv) {
    if (argc != 2) return 20;
    char input[4096], identity[8192];
    ssize_t length = read(3, input, sizeof(input)-1);
    ssize_t identities = read(4, identity, sizeof(identity)-1);
    if (length < 0 || identities < 0) return 21;
    input[length] = 0; identity[identities] = 0;
    FILE *out = fopen("identity.json", "w");
    if (!out) return 22;
    fputs(identity, out); fclose(out);
    out = fopen("parent.txt", "w");
    if (!out) return 23;
    fprintf(out, "%ld\n", (long)getppid()); fclose(out);
    out = fopen("runs.txt", "a");
    if (!out) return 24;
    fputs("run\n", out); fclose(out);
    if (!strcmp(argv[1], "text")) {
        for (ssize_t i = 0; i < length; ++i) input[i] = (char)toupper((unsigned char)input[i]);
        out = fopen("result.txt", "w"); fputs(input, out); fclose(out); return 0;
    }
    if (!strcmp(argv[1], "math")) {
        int number = atoi(input);
        out = fopen("result.txt", "w"); fprintf(out, "%d\n", number * number); fclose(out); return 0;
    }
    if (!strcmp(argv[1], "hold")) {
        if (!strncmp(input, "ignore", 6)) signal(SIGTERM, SIG_IGN);
        out = fopen("ready.json", "w");
        if (!out) return 26;
        fputs(identity, out); fclose(out);
        struct timespec pause = {0, 100000000};
        while (access("release", F_OK)) nanosleep(&pause, NULL);
        return 0;
    }
    return 25;
}
'''


class Acceptance:
    def __init__(self, root: Path):
        self.root = root
        self.repository = Path(__file__).resolve().parents[1]
        self.projects = {name: root / name for name in ("text", "math")}
        for path in self.projects.values(): path.mkdir()
        self.executable = root / "worker"
        source = root / "worker.c"
        source.write_text("#define _POSIX_C_SOURCE 200809L\n" + WORKER)
        compiler = shutil.which("cc") or shutil.which("gcc")
        if not compiler: raise RuntimeError("acceptance requires a local C compiler")
        subprocess.run([compiler, "-std=c11", "-Wall", "-Wextra", "-Werror", str(source), "-o", str(self.executable)], check=True, timeout=30)
        self.cfg = {"schema_version": 1, "node": socket.gethostname(), "user": getpass.getuser(),
                    "state_dir": str(root / "state"), "gpus": [0], "cpus_total": 4,
                    "max_cpu_jobs": 4, "default_project": "text", "venvs": {"python": sys.executable},
                    "projects": {name: {"root": str(path), "git": False} for name, path in self.projects.items()},
                    "execution_backends": {}}
        sha = hashlib.sha256(self.executable.read_bytes()).hexdigest()
        for name in ("text", "math", "hold"):
            self.cfg["execution_backends"][name] = {
                "kind": "linux_fd", "executable": str(self.executable), "sha256": sha,
                "argv": ["generic-worker", name], "env": {}, "projects": ["text", "math"],
                "input_slots": {"3": {"max_bytes": 4096}},
            }
        self.cfg["execution_backends"]["hold-owner"] = {
            **self.cfg["execution_backends"]["hold"], "kind": "linux_fd_owner",
            "owner": {"prepare_timeout_sec": 10, "terminal_retention_sec": 120}}
        self.config_path = root / "config.json"
        self.config_path.write_text(json.dumps(self.cfg))
        self.env = dict(os.environ, SCHED_STATE=str(root / "state"), SCHED_CONFIG=str(self.config_path),
                        SCHED_FAKE_GPUS="0:24", PYTHONPATH=str(self.repository), PYTHONDONTWRITEBYTECODE="1")
        self.env.pop("SCHED_ALLOW_FOREIGN_WRITE", None)
        self.sequence = 0
        self.daemon_pid = None
        self.orphans = set()

    def cli(self, *args, expect=0, timeout=45, env=None):
        result = subprocess.run([sys.executable, "-m", "gsched.cli", *map(str, args)],
                                env=self.env if env is None else env, capture_output=True,
                                text=True, timeout=timeout, cwd=self.root)
        if expect is not None:
            assert result.returncode == expect, (args, result.returncode, result.stdout, result.stderr)
        return result

    def data(self, *args): return json.loads(self.cli(*args).stdout)

    def snapshot(self, batch=None):
        return self.data("status", *([batch] if batch else []), "--json")

    def wait_job(self, batch, expected, *, timeout=100, predicate=None):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            last = self.snapshot(batch)
            if (last["jobs"] and last["jobs"][0]["status"] in expected and
                    (predicate is None or predicate(last["jobs"][0]))):
                return last["jobs"][0]
            time.sleep(0.5)
        raise AssertionError((batch, expected, last))

    def batch(self, project, backend, content, *, duration=2, name=None):
        self.sequence += 1
        name = name or f"accept-{self.sequence}"
        relative = f"input-{self.sequence}.txt"
        (self.projects[project] / relative).write_bytes(content)
        task = {"id": "work", "cmd": self.cfg["execution_backends"][backend]["argv"],
                "git": False, "max_retry": 0, "duration_min": duration,
                "resources": {"gpu": 0, "cpus": 1},
                "execution": {"backend": backend, "inputs": {"3": {
                    "path": relative, "sha256": hashlib.sha256(content).hexdigest()}}}}
        spec = {"name": name, "project": project, "tasks": [task]}
        path = self.root / f"batch-{self.sequence}.json"
        path.write_text(json.dumps(spec))
        return name, path, spec

    def submit(self, path):
        result = self.data("submit", path, "--json")
        assert result["schema_version"] == 1 and result["delivery"] == "database" and result["persisted"] is True, result
        return result["batch_id"]

    def attempt(self, batch):
        response = self.data("execution", f"{batch}:work", "--json")
        attempts = response["attempts"]
        assert len(attempts) == 1, attempts
        diagnostic = next(d for d in response["diagnostics"]
                          if d["job_version"] == attempts[0]["job_version"])
        assert diagnostic["attempt_id"] == attempts[0]["attempt_id"], response
        assert diagnostic["phase"] == attempts[0]["phase"], response
        assert diagnostic["replay_blocked"] is True, response
        if "owner" in attempts[0]:
            health = attempts[0]["owner_health"]
            assert health["source"] == "recorded", health
            assert health["connection_status"] in ("unknown", "responsive", "unreachable", "lost"), health
            assert "token" not in attempts[0]["owner"] and "endpoint" not in attempts[0]["owner"], response
        return attempts[0]

    def wait_ready(self, batch, project="text"):
        attempt_id = self.attempt(batch)["attempt_id"]
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                identity = json.loads((self.projects[project] / "ready.json").read_text())
                if identity.get("attempt", identity)["attempt_id"] == attempt_id:
                    return
            except (OSError, ValueError, KeyError): pass
            time.sleep(.01)
        raise AssertionError("native fixture failed to install its signal behavior")

    def wait_owner_acknowledgement(self, batch):
        # Job settlement and service acknowledgement intentionally commit in
        # separate transactions. Observe both without assuming atomicity.
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            attempt = self.attempt(batch)
            if attempt["owner_health"]["cleanup_state"] == "acknowledged":
                return attempt
            time.sleep(.5)
        raise AssertionError(attempt)

    def start(self):
        self.cli("daemon", "start", "--fake")
        self.daemon_pid = self.data("daemon", "status", "--json")["pid"]

    @staticmethod
    def reap_known(pid):
        if pid is None: return
        try: os.waitpid(pid, 0)
        except ChildProcessError: pass

    def stop(self):
        for project in self.projects.values(): (project / "release").touch()
        orphan_reapers = []
        for pid in self.orphans:
            thread = threading.Thread(target=self.reap_known, args=(pid,), daemon=True)
            thread.start()
            orphan_reapers.append(thread)
        for thread in orphan_reapers:
            thread.join(timeout=10)
            assert not thread.is_alive(), "owned orphan did not exit after fixture release"
        reaper = threading.Thread(target=self.reap_known, args=(self.daemon_pid,), daemon=True)
        reaper.start()
        if self.data("daemon", "status", "--json")["process_state"] != "stopped":
            self.cli("daemon", "stop", timeout=150)
        reaper.join(timeout=1)

    def basic(self):
        for project, mode, content, expected in (("text", "text", b"hello interface\n", "HELLO INTERFACE\n"),
                                                  ("math", "math", b"13\n", "169\n")):
            _, path, _ = self.batch(project, mode, content)
            batch = self.submit(path)
            job = self.wait_job(batch, {"done"})
            attempt = self.attempt(batch)
            assert attempt["phase"] == "exited", attempt
            observation = attempt["observation"]
            assert observation["status"] == "exited" and observation["returncode"] == 0 and observation["group_clean"] is True, observation
            assert observation["rusage"] is not None, observation
            identity = json.loads((self.projects[project] / "identity.json").read_text())
            assert identity == attempt["identity"], (identity, attempt)
            assert identity["job_id"] == job["id"] and identity["batch_id"] == batch and identity["version"] == 1, identity
            assert identity["scheduler_pid"] == int((self.projects[project] / "parent.txt").read_text()), identity
            assert (self.projects[project] / "result.txt").read_text() == expected
        print("PASS: two projects, sealed FD inputs and actual scheduler identity/wait/rusage", flush=True)

    def rejection(self):
        _, path, spec = self.batch("text", "text", b"reject\n")
        spec["tasks"][0]["cmd"] = ["unconfigured-program"]
        path.write_text(json.dumps(spec))
        self.cli("submit", path, expect=1)
        spec["tasks"][0]["cmd"] = self.cfg["execution_backends"]["text"]["argv"]
        spec["mode"] = "strict"; path.write_text(json.dumps(spec))
        self.cli("submit", path, expect=1)
        spec.pop("mode"); spec["tasks"][0]["execution"]["backend"] = "absent"; path.write_text(json.dumps(spec))
        self.cli("submit", path, expect=1)
        print("PASS: nonadministrator argv, absent backend and retired strict admission rejected", flush=True)

    def cancel(self):
        (self.projects["text"] / "release").unlink(missing_ok=True)
        _, path, _ = self.batch("text", "hold", b"ignore\n")
        batch = self.submit(path)
        self.wait_job(batch, {"running"})
        self.wait_ready(batch)
        attempt_id = self.attempt(batch)["attempt_id"]
        self.cli("submit", path, expect=1)
        assert self.attempt(batch)["attempt_id"] == attempt_id
        self.cli("cancel", f"{batch}:work", "--yes")
        self.wait_job(batch, {"cancelled"})
        observation = self.attempt(batch)["observation"]
        assert observation["returncode"] == -signal.SIGKILL and observation["group_clean"] is True, observation
        print("PASS: active duplicate rejected, cancellation escalates TERM to KILL and releases only a clean managed group", flush=True)

    def duration(self):
        _, path, _ = self.batch("text", "hold", b"ignore\n", duration=0.001)
        batch = self.submit(path)
        self.wait_job(batch, {"timed_out"})
        observation = self.attempt(batch)["observation"]
        assert observation["returncode"] == -signal.SIGKILL and observation["group_clean"] is True, observation
        print("PASS: duration timeout cancels through original owner and records actual wait", flush=True)

    def drain(self):
        self.cli("daemon", "drain")
        _, path, _ = self.batch("math", "math", b"5\n")
        batch = self.submit(path)
        job = self.wait_job(batch, {"pending"}, predicate=lambda job: job["wait_reason"] == "draining")
        assert job["wait_reason"] == "draining", job
        assert self.data("execution", f"{batch}:work", "--json")["attempts"] == []
        self.cli("daemon", "resume")
        self.wait_job(batch, {"done"})
        assert self.attempt(batch)["identity"]["version"] == 1
        print("PASS: drain retains pending without launch attempts, resume executes original version", flush=True)

    def drift(self):
        self.cli("daemon", "drain")
        _, path, _ = self.batch("text", "text", b"drift\n")
        batch = self.submit(path)
        original = self.executable.read_bytes()
        self.executable.write_bytes(original + b"changed")
        try:
            self.cli("daemon", "resume")
            self.wait_job(batch, {"failed"})
            attempt = self.attempt(batch)
            assert attempt["phase"] == "not_started", attempt
            assert attempt["observation"]["pid"] is None, attempt
        finally:
            self.executable.write_bytes(original)
        print("PASS: executable digest drift fails before child birth without ordinary fallback", flush=True)

    def ordinary(self):
        self.sequence += 1
        path = self.root / "ordinary.json"
        path.write_text(json.dumps({"name": f"ordinary-{self.sequence}", "project": "math", "tasks": [
            {"id": "work", "cmd": [sys.executable, "-c", "from pathlib import Path; Path('ordinary.txt').write_text('ok')"],
             "git": False, "resources": {"gpu": 0}, "max_retry": 0}]}))
        batch = self.submit(path); self.wait_job(batch, {"done"})
        assert (self.projects["math"] / "ordinary.txt").read_text() == "ok"
        assert self.data("execution", f"{batch}:work", "--json")["attempts"] == []
        print("PASS: ordinary mix tasks retain their execution and state semantics", flush=True)

    def missing(self):
        # Independent package copy omits compiled artifacts; never rename a
        # shared extension while another local test may be using it.
        separate = self.root / "missing-instance"
        separate.mkdir()
        package_root = separate / "package"
        shutil.copytree(self.repository / "gsched", package_root / "gsched",
                        ignore=shutil.ignore_patterns("*.so", "*.pyd", "__pycache__", "*.pyc"))
        missing = Acceptance(separate)
        missing.env["PYTHONPATH"] = str(package_root)
        try:
            check = missing.cli("daemon", "check", expect=1)
            assert "execution backend" in check.stdout + check.stderr, check
            missing.cli("daemon", "start", "--fake", expect=1)
            assert not (missing.projects["text"] / "runs.txt").exists()
            missing.cfg["execution_backends"] = {}
            missing.config_path.write_text(json.dumps(missing.cfg))
            missing.start()
            missing.ordinary()
        finally:
            missing.stop()
        print("PASS: missing compiled backend rejects before child birth without a subprocess fallback", flush=True)

    def restart(self):
        project = self.projects["text"]
        (project / "release").unlink(missing_ok=True)
        _, path, _ = self.batch("text", "hold", b"restart\n", duration=5)
        batch = self.submit(path)
        self.wait_job(batch, {"running"})
        attempt = self.attempt(batch)
        child = attempt["observation"]["pid"]
        scheduler = attempt["identity"]["scheduler_pid"]
        assert scheduler == self.daemon_pid and child is not None, attempt
        # Fault injection only: target is the exact scheduler that delivered FD4
        # in this private instance. Normal cleanup always uses CLI daemon stop.
        os.kill(scheduler, signal.SIGKILL)
        self.reap_known(scheduler)
        self.orphans.add(child)
        self.restart_crashed(scheduler)
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            current = self.attempt(batch)
            if current["phase"] == "unresolved": break
            time.sleep(.5)
        else: raise AssertionError(current)
        assert current["attempt_id"] == attempt["attempt_id"]
        assert current["observation"]["returncode"] is None and current["observation"]["rusage"] is None, current
        assert current["observation"]["group_clean"] is False, current
        assert self.wait_job(batch, {"running"})["version"] == 1
        before = (project / "runs.txt").read_text()
        (project / "release").touch()
        self.reap_known(child)
        self.orphans.discard(child)
        job = self.wait_job(batch, {"interrupted"})
        assert job["version"] == 1 and job["failure"] == "execution_authority_lost", job
        final = self.attempt(batch)
        assert final["attempt_id"] == attempt["attempt_id"] and final["phase"] == "unresolved", final
        assert final["observation"]["returncode"] is None and final["observation"]["group_clean"] is True, final
        diagnostic = self.data("execution", f"{batch}:work", "--json")["diagnostics"][0]
        assert diagnostic["uncertainty_reason"] == "owner_authority_lost", diagnostic
        assert diagnostic["wait_result_available"] is False and diagnostic["returncode"] is None, diagnostic
        assert diagnostic["cleanup_state"] == "confirmed", diagnostic
        assert (project / "runs.txt").read_text() == before
        print("PASS: daemon restart never reconstructs wait authority, releases only vanished group, records interrupted without replay", flush=True)

    def restart_crashed(self, scheduler):
        print("INFO: injected local daemon crash; waiting for its public heartbeat lease to expire", flush=True)
        # Some virtualized hosts move wall time backwards after a clock sync.
        # A null age can then mean timestamp_in_future, not an expired lease.
        # Wait for actual public lease expiry without forcing start or editing it.
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            health = self.data("daemon", "status", "--json")
            if (health["read_error"] is None and health["process_state"] == "stopped"
                    and (health["heartbeat_age_s"] is None or health["heartbeat_age_s"] > 61)):
                # Retire the dead instance through CLI before starting its
                # successor. Another wall-clock correction between status and
                # start must not turn an idempotent "already running" reply
                # into evidence that a new dispatcher actually started.
                stopped = self.cli("daemon", "stop", expect=None)
                if stopped.returncode == 0:
                    break
                again = self.data("daemon", "status", "--json")
                assert (again["read_error"] == "timestamp_in_future" or
                        (again["heartbeat_age_s"] is not None and
                         again["heartbeat_age_s"] < 60)), (stopped, again)
            time.sleep(1)
        else: raise AssertionError(health)
        self.start()
        assert self.daemon_pid != scheduler, self.data("daemon", "status", "--json")

    def persistent_restart(self):
        for path in self.projects.values(): (path / "release").unlink(missing_ok=True)
        _, normal, _ = self.batch("text", "hold-owner", b"reconnect\n", duration=5)
        normal_batch = self.submit(normal)
        self.wait_job(normal_batch, {"running"})
        self.wait_ready(normal_batch)
        original = self.attempt(normal_batch)
        identity = json.loads((self.projects["text"] / "identity.json").read_text())
        assert identity["schema"] == "sched_execution_owner_identity/v1", identity
        assert identity["attempt"] == original["identity"] and identity["owner"] == original["owner"]
        assert int((self.projects["text"] / "parent.txt").read_text()) == original["owner"]["pid"]
        _, timeout, _ = self.batch("math", "hold-owner", b"ignore\n", duration=.01)
        timeout_batch = self.submit(timeout)
        self.wait_job(timeout_batch, {"running"})
        self.wait_ready(timeout_batch, "math")
        timed_attempt = self.attempt(timeout_batch)
        scheduler = original["identity"]["scheduler_pid"]
        assert scheduler == self.daemon_pid
        os.kill(scheduler, signal.SIGKILL)
        self.reap_known(scheduler)
        services = {original["owner"]["pid"], timed_attempt["owner"]["pid"]}
        self.orphans.update(services)
        self.restart_crashed(scheduler)
        timed = self.wait_job(timeout_batch, {"timed_out"})
        assert timed["version"] == 1
        timed_observation = self.attempt(timeout_batch)["observation"]
        assert timed_observation["returncode"] == -signal.SIGKILL and timed_observation["group_clean"] is True, timed_observation
        assert self.wait_job(normal_batch, {"running"})["version"] == 1
        assert self.snapshot()["cpu"]["used"] == 1, self.snapshot()
        assert self.attempt(normal_batch)["attempt_id"] == original["attempt_id"]
        before = (self.projects["text"] / "runs.txt").read_text()
        (self.projects["text"] / "release").touch()
        self.wait_job(normal_batch, {"done"})
        final = self.wait_owner_acknowledgement(normal_batch)
        assert final["attempt_id"] == original["attempt_id"] and final["owner"] == original["owner"]
        assert final["observation"]["returncode"] == 0 and final["observation"]["group_clean"] is True
        assert final["observation"]["rusage"] is not None
        assert final["owner_health"]["cleanup_state"] == "acknowledged", final
        assert final["owner_health"]["acknowledgement"] == "closed", final
        assert (self.projects["text"] / "runs.txt").read_text() == before
        assert self.snapshot()["cpu"]["used"] == 0, self.snapshot()
        for pid in services:
            self.reap_known(pid)
            self.orphans.discard(pid)
        print("PASS: original persistent owners survive daemon crash, timeout independently, reconnect real wait and never replay", flush=True)

    def persistent_loss(self):
        project = self.projects["text"]
        (project / "release").unlink(missing_ok=True)
        _, path, _ = self.batch("text", "hold-owner", b"lost-owner\n", duration=5)
        batch = self.submit(path)
        self.wait_job(batch, {"running"})
        self.wait_ready(batch)
        original = self.attempt(batch)
        service, child = original["owner"]["pid"], original["observation"]["pid"]
        assert service != self.daemon_pid and child is not None
        # Fault injection targets only this fixture's exact FD4-bound service.
        from gsched.execution.persistent import start_ticks
        assert start_ticks(service) == original["owner"]["start_ticks"]
        os.kill(service, signal.SIGKILL)
        self.orphans.add(child)
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            attempt = self.attempt(batch)
            if attempt["phase"] == "unresolved": break
            time.sleep(.5)
        else: raise AssertionError(attempt)
        assert attempt["observation"]["returncode"] is None and attempt["observation"]["group_clean"] is False
        assert self.wait_job(batch, {"running"})["version"] == 1
        assert self.snapshot()["cpu"]["used"] == 1
        before = (project / "runs.txt").read_text()
        (project / "release").touch()
        self.reap_known(child)
        self.orphans.discard(child)
        job = self.wait_job(batch, {"interrupted"})
        assert job["failure"] == "execution_authority_lost" and job["version"] == 1
        final = self.attempt(batch)
        assert final["attempt_id"] == original["attempt_id"] and final["owner"] == original["owner"]
        assert final["observation"]["returncode"] is None and final["observation"]["rusage"] is None
        assert final["observation"]["group_clean"] is True and self.snapshot()["cpu"]["used"] == 0
        assert final["owner_health"]["connection_status"] == "lost", final
        assert (project / "runs.txt").read_text() == before
        print("PASS: lost persistent owner retains resources until vanished group, records unknown wait and never replays", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", action="append", choices=("basic", "rejection", "cancel", "duration", "drain", "drift", "ordinary", "missing", "restart", "persistent_restart", "persistent_loss"))
    args = parser.parse_args()
    if sys.platform != "linux": raise SystemExit("acceptance requires local Linux")
    from gsched.execution import BackendUnavailable, LinuxFdBackend
    try:
        LinuxFdBackend()
        native_available = True
    except BackendUnavailable:
        if os.environ.get("SCHED_REQUIRE_NATIVE") == "1":
            raise
        native_available = False
    cases = args.case or (("rejection", "basic", "drain", "cancel", "duration", "drift", "ordinary", "missing", "restart", "persistent_restart", "persistent_loss")
                          if native_available else ("ordinary", "missing"))
    if not native_available and any(case not in ("ordinary", "missing") for case in cases):
        raise SystemExit("requested acceptance case requires an explicitly built native backend")
    if set(cases).intersection(("restart", "persistent_restart", "persistent_loss")):
        # Adopt only descendants of this isolated acceptance process so the
        # injected crash cannot leak worker zombies into the host's init.
        import ctypes
        if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), "test child adoption unavailable")
    with tempfile.TemporaryDirectory(prefix="sched-execution-accept-") as temporary:
        acceptance = Acceptance(Path(temporary))
        if not native_available:
            acceptance.cfg["execution_backends"] = {}
            acceptance.config_path.write_text(json.dumps(acceptance.cfg))
        try:
            acceptance.start()
            for case in cases:
                getattr(acceptance, case)()
        except BaseException:
            for path in (Path(temporary) / "state" / socket.gethostname()).glob("*.log"):
                print(f"--- {path.name} ---\n{path.read_text(errors='replace')[-10000:]}", file=sys.stderr)
            raise
        finally:
            acceptance.stop()


if __name__ == "__main__": main()
