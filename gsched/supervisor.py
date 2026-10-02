"""Foreground daemon ownership and optional restart after an actual child wait."""
from __future__ import annotations

import fcntl
import json
import os
import secrets
import signal
import stat
import socket
import subprocess
import sys
import time

from . import daemon, resources, state
from .executor import process_start_token

CONTROL = "daemon.supervisor.json"


def _read():
    directory = os.open(state.host_dir(), os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    descriptor = None
    try:
        root = os.fstat(directory)
        if root.st_uid != os.getuid() or stat.S_IMODE(root.st_mode) & 0o022:
            raise ValueError("supervisor directory is not private")
        descriptor = os.open(CONTROL, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_size > 4096 or info.st_nlink != 1 or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o022:
            raise ValueError("invalid supervisor control file")
        raw = os.read(descriptor, 4097)
        if len(raw) > 4096:
            raise ValueError("supervisor control exceeds limit")
        value = json.loads(raw)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory)
    if type(value) is not dict:
        raise ValueError("invalid supervisor control")
    if (set(value) != {"schema_version", "session_id", "pid", "start_token", "physical_host", "stop"} or type(value["schema_version"]) is not int or value["schema_version"] != 1 or type(value["pid"]) is not int or value["pid"] <= 0 or type(value["stop"]) is not bool or not isinstance(value["session_id"], str) or len(value["session_id"]) != 32 or not all(c in "0123456789abcdef" for c in value["session_id"]) or not isinstance(value["start_token"], str) or not value["start_token"] or not isinstance(value["physical_host"], str) or not value["physical_host"].strip()):
        raise ValueError("invalid supervisor control")
    return value


def request_stop():
    """Publish stop before inspecting the daemon lease, including restart gaps."""
    if not os.path.lexists(os.path.join(state.host_dir(), CONTROL)):
        return False  # Existing background-daemon stop retains its original path.
    expected = _read()
    with state.submission_lock():
        current = _read()
        if current["session_id"] != expected["session_id"]:
            raise ValueError("supervisor changed during stop")
        if current["physical_host"] != socket.gethostname():
            raise ValueError("supervisor belongs to another physical host")
        resources.write_private_json(CONTROL, dict(current, stop=True))
        resources._sync_control_directory()
        return True


def _stopped(session):
    try:
        value = _read()
        return value["session_id"] != session or value["stop"]
    except (OSError, ValueError, RecursionError):
        return True  # Missing, changed or malformed control cannot authorize restart.


def _explicit_stop(session):
    try:
        value = _read()
        return value["session_id"] == session and value["stop"]
    except (OSError, ValueError, RecursionError):
        return False  # Indeterminate controls cannot cancel a running child.


def foreground(*, fake=False, supervise=False, restart_delay_sec=3, max_restarts=0):
    """Own one daemon child at a time. Never steal a live/stale/unknown lease."""
    if type(restart_delay_sec) not in (int, float) or not 1 <= restart_delay_sec <= 300:
        raise ValueError("restart delay must be within 1..300 seconds")
    if type(max_restarts) is not int or not 0 <= max_restarts <= 100000:
        raise ValueError("max restarts must be 0..100000; 0 is unlimited")
    issues = daemon.check(fake=fake)
    if any(item.get("level") == "fail" for item in issues):
        print("daemon foreground preflight failed", file=sys.stderr, flush=True)
        return 1
    state.ensure_private_directory(state.host_dir())
    with state.open_private_text(os.path.join(state.host_dir(), "daemon.supervisor.lock"), "a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("another foreground supervisor owns this node", file=sys.stderr, flush=True)
            return 1
        owner = daemon._read_lease_owner()
        if owner is None and os.path.lexists(daemon._owner_file()):
            print("daemon lease is indeterminate; foreground refused", file=sys.stderr, flush=True)
            return 1
        if owner is not None and (owner["physical_host"] != socket.gethostname() or (daemon._pid_alive(owner["pid"]) and process_start_token(owner["pid"]) in (None, owner["start_token"]))):
            print("daemon lease is live or belongs to another host; foreground refused", file=sys.stderr, flush=True)
            return 1
        session = secrets.token_hex(16)
        token = process_start_token(os.getpid())
        if token is None:
            raise ValueError("supervisor process identity unavailable")
        control = {"schema_version": 1, "session_id": session, "pid": os.getpid(), "start_token": token, "physical_host": socket.gethostname(), "stop": False}
        with state.submission_lock():
            resources.write_private_json(CONTROL, control)
            resources._sync_control_directory()
        stopping = False
        previous = {}
        def stop_signal(signum, frame):
            nonlocal stopping
            stopping = True
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.signal(sig, stop_signal)
        child = None
        restarts = 0
        env = dict(os.environ)
        if fake:
            env["SCHED_FAKE_GPUS"] = env.get("SCHED_FAKE_GPUS", "0,1,2,3")
        else:
            env.pop("SCHED_FAKE_GPUS", None)
        try:
            while True:
                with state.submission_lock():
                    if stopping or _stopped(session):
                        return 0
                    child = subprocess.Popen([sys.executable, "-m", "gsched.dispatcher_main", "--daemon"], env=env, stdin=subprocess.DEVNULL, start_new_session=True)
                print(f"foreground daemon pid={child.pid} restart={restarts}", flush=True)
                stop_sent = False
                deadline = None
                while child.poll() is None:
                    if stopping or _explicit_stop(session):
                        if not stop_sent:
                            # Popen owns this exact child, whose wait is not yet
                            # consumed. No PID from heartbeat or a foreign lease.
                            child.send_signal(signal.SIGTERM)
                            stop_sent = True
                            deadline = time.monotonic() + daemon.STOP_TIMEOUT_SEC
                        elif time.monotonic() >= deadline:
                            print("foreground stop timed out; child/lease retained", file=sys.stderr, flush=True)
                            return 1
                    time.sleep(.2)
                rc = child.wait()
                print(f"foreground daemon exit rc={rc}", flush=True)
                if stopping or _explicit_stop(session):
                    return 0
                if _stopped(session):
                    return 1
                if rc == 0:
                    return 0
                if not supervise:
                    return 1
                if max_restarts and restarts >= max_restarts:
                    print("foreground restart limit reached", file=sys.stderr, flush=True)
                    return 1
                restarts += 1
                # A preserved drain remains effective in the replacement daemon.
                # Only raw child wait grants this restart; heartbeat never does.
                deadline = time.monotonic() + restart_delay_sec
                while time.monotonic() < deadline:
                    if stopping or _stopped(session):
                        return 0
                    time.sleep(.2)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
            # Do not clear controls if an owned child is still unresolved/alive.
            if child is None or child.poll() is not None:
                with state.submission_lock():
                    try:
                        current = _read()
                        if current["session_id"] == session:
                            os.unlink(os.path.join(state.host_dir(), CONTROL))
                            resources._sync_control_directory()
                    except FileNotFoundError:
                        pass
