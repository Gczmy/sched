"""Authenticated access to a surviving original Linux child owner."""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import secrets
import select
import socket
import struct
import subprocess
import sys
import time

from .backend import ExecutionEnvelope, ExecutionObservation, LinuxFdBackend, Prepared
from ..execution_policy import canonical_bytes, sealed_bytes

PROTOCOL = "sched-execution-owner/v1"
IDENTITY_SCHEMA = "sched_execution_owner_identity/v1"
MAX_FRAME = 2 * 1024 * 1024


class OwnerUnavailable(RuntimeError):
    def __init__(self, *, lost=False, boot_changed=False):
        super().__init__("persistent execution owner is unavailable")
        self.lost = lost
        self.boot_changed = boot_changed


def start_ticks(pid):
    text = Path(f"/proc/{pid}/stat").read_text()
    fields = text[text.rindex(")") + 2:].split()
    if fields[0] == "Z":
        raise ProcessLookupError(pid)
    return int(fields[19])


def boot_id():
    return Path("/proc/sys/kernel/random/boot_id").read_text().strip()


def validate_binding(value):
    keys = {"schema", "owner_id", "endpoint", "pid", "start_ticks", "boot_id", "attempt_id", "token"}
    if type(value) is not dict or set(value) != keys or value["schema"] != PROTOCOL:
        raise ValueError("invalid execution owner binding")
    for key, length in (("owner_id", 32), ("attempt_id", 32), ("token", 64)):
        if type(value[key]) is not str or re.fullmatch("[0-9a-f]{" + str(length) + "}", value[key]) is None:
            raise ValueError("invalid execution owner binding")
    if value["endpoint"] != "gsched-owner-" + value["owner_id"]:
        raise ValueError("invalid execution owner endpoint")
    if any(type(value[k]) is not int or value[k] <= 0 for k in ("pid", "start_ticks")):
        raise ValueError("invalid execution owner process")
    if type(value["boot_id"]) is not str or re.fullmatch(r"[0-9a-f-]{36}", value["boot_id"]) is None:
        raise ValueError("invalid execution owner boot")
    return dict(value)


def public_binding(value):
    return {k: v for k, v in validate_binding(value).items() if k not in ("token", "endpoint")}


def _signed(value, token):
    value = dict(value)
    value["mac"] = hmac.new(bytes.fromhex(token), canonical_bytes(value), hashlib.sha256).hexdigest()
    return value


def _verified(value, token):
    if type(value) is not dict or type(value.get("mac")) is not str:
        raise ValueError("invalid owner authentication")
    body = {k: v for k, v in value.items() if k != "mac"}
    expected = _signed(body, token)["mac"]
    if not hmac.compare_digest(value["mac"], expected):
        raise ValueError("invalid owner authentication")
    return body


def _read_exact(stream, count):
    chunks = []
    while count:
        chunk = stream.recv(count)
        if not chunk:
            raise EOFError("owner connection ended")
        chunks.append(chunk)
        count -= len(chunk)
    return b"".join(chunks)


def _receive(stream):
    length = struct.unpack("!I", _read_exact(stream, 4))[0]
    if not 0 < length <= MAX_FRAME:
        raise ValueError("owner frame exceeds limit")
    return json.loads(_read_exact(stream, length))


def _send(stream, value):
    raw = canonical_bytes(value)
    if not 0 < len(raw) <= MAX_FRAME:
        raise ValueError("owner frame exceeds limit")
    stream.sendall(struct.pack("!I", len(raw)) + raw)


def _peer(stream):
    return struct.unpack("3i", stream.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))


class PersistentOwner:
    def __init__(self, binding, *, process=None):
        self.binding = validate_binding(binding)
        self._process = process
        self._closed = False
        self._last = None
        self.cancel_reason = None
        self.close_outcome = None

    def _rpc(self, op, **parameters):
        if self._closed:
            raise RuntimeError("execution owner is closed")
        binding = self.binding
        try:
            if self._process is not None and self._process.poll() is not None:
                raise OwnerUnavailable(lost=True)
            if boot_id() != binding["boot_id"]:
                raise OwnerUnavailable(lost=True, boot_changed=True)
            if start_ticks(binding["pid"]) != binding["start_ticks"]:
                raise OwnerUnavailable(lost=True)
        except (FileNotFoundError, ProcessLookupError):
            raise OwnerUnavailable(lost=True) from None
        except (OSError, ValueError):
            raise OwnerUnavailable() from None
        nonce = secrets.token_hex(16)
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
                stream.settimeout(1.0)
                stream.connect("\0" + binding["endpoint"])
                pid, uid, _ = _peer(stream)
                if pid != binding["pid"] or uid != os.getuid() or start_ticks(pid) != binding["start_ticks"]:
                    raise OwnerUnavailable()
                request = {"schema": PROTOCOL, "owner_id": binding["owner_id"], "nonce": nonce,
                           "op": op, "parameters": parameters}
                _send(stream, _signed(request, binding["token"]))
                response = _verified(_receive(stream), binding["token"])
            if (set(response) != {"schema", "owner_id", "nonce", "observation", "cancel_reason", "error"}
                    or response["schema"] != PROTOCOL or response["owner_id"] != binding["owner_id"]
                    or response["nonce"] != nonce):
                raise ValueError("owner response belongs to a different request")
            if response["error"] is not None:
                raise RuntimeError("execution owner rejected operation")
            observation = ExecutionObservation(**response["observation"])
            self.cancel_reason = response["cancel_reason"]
            self._last = observation
            return observation
        except (OSError, EOFError, ValueError, TypeError) as error:
            raise OwnerUnavailable() from error

    @property
    def pid(self):
        return self.poll().pid

    def poll(self):
        return self._rpc("poll")

    def _launch(self, envelope, stdio):
        observation = self._rpc("start")
        if observation.status == "not_started" and observation.launch_error is not None:
            raise OSError(observation.launch_error, "execution failed before child creation")

    def abandon_prepared(self):
        return self._rpc("abandon_prepared")

    def cancel(self, grace_period=2.0):
        if type(grace_period) not in (int, float) or not math.isfinite(grace_period) or not 0 <= grace_period <= 300:
            raise ValueError("invalid owner cancellation grace")
        self._rpc("cancel", grace_period=grace_period)

    def wait(self, timeout=None):
        if timeout is not None and (type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout < 0):
            raise ValueError("invalid wait timeout")
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            observation = self.poll()
            if observation.status in ("exited", "not_started", "prepared"):
                return observation
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("execution has not settled")
            time.sleep(.02)

    def close(self):
        if self._closed:
            if self._process is not None:
                self._process.wait(timeout=3)
            return
        try:
            self._rpc("close")
            self.close_outcome = "closed"
        except OwnerUnavailable as error:
            if not error.lost:
                raise
            self.close_outcome = "owner_lost"
        self._closed = True
        if self._process is not None:
            self._process.wait(timeout=3)


class PersistentLinuxFdBackend:
    capabilities = LinuxFdBackend.capabilities | {"persistent_owner_v1"}
    interface_version = LinuxFdBackend.interface_version

    def __init__(self):
        LinuxFdBackend()  # No fallback if the native kernel capability is absent.

    def prepare(self, envelope, *, executable_fd, cwd_fd, fd_bindings, identity,
                duration_seconds=None, prepare_timeout=30., terminal_retention=3600., constraints=None):
        if type(envelope) is not ExecutionEnvelope or envelope.cwd is not None:
            raise ValueError("persistent backend requires an FD envelope")
        for value in (prepare_timeout, terminal_retention):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError("owner retention must be finite and positive")
        if duration_seconds is not None and (type(duration_seconds) not in (int, float)
                or not math.isfinite(duration_seconds) or duration_seconds <= 0):
            raise ValueError("duration must be finite and positive")
        attempt_id = identity["attempt_id"]
        owner_id = secrets.token_hex(16)
        token = secrets.token_hex(32)
        endpoint = "gsched-owner-" + owner_id
        startup = {"argv": envelope.argv, "env": dict(envelope.env), "executable_fd": executable_fd,
                   "cwd_fd": cwd_fd, "bindings": {str(k): v for k, v in fd_bindings.items()},
                   "identity": identity, "owner_id": owner_id, "token": token, "endpoint": endpoint,
                   "duration": duration_seconds, "prepare_timeout": prepare_timeout,
                   "terminal_retention": terminal_retention}
        if constraints is not None:
            from .constraints import LaunchConstraints, CONSTRAINTS_VERSION
            if type(constraints) is not LaunchConstraints:
                raise TypeError("constraints require LaunchConstraints")
            constraints.validate()
            if getattr(LinuxFdBackend()._module, "constraints_interface_version", None) != CONSTRAINTS_VERSION:
                from .backend import BackendUnavailable
                raise BackendUnavailable("native constraints interface absent", reason="native_constraints_unavailable")
            if constraints.cgroup_procs_fd in fd_bindings.values():
                raise ValueError("cgroup control descriptor cannot be bound to the program")
            startup["constraints"] = {"cpu_affinity": constraints.cpu_affinity,
                "cgroup_procs_fd": constraints.cgroup_procs_fd, "interface_version": CONSTRAINTS_VERSION}
        raw = canonical_bytes(startup)
        if len(raw) > MAX_FRAME:
            raise ValueError("owner startup exceeds limit")
        descriptors = {executable_fd, cwd_fd, *fd_bindings.values()}
        if constraints is not None and constraints.cgroup_procs_fd is not None:
            descriptors.add(constraints.cgroup_procs_fd)
        for descriptor in descriptors:
            if type(descriptor) is not int or descriptor < 0:
                raise ValueError("invalid owner descriptor")
            os.fstat(descriptor)
        boot = sealed_bytes(raw, "sched-owner-startup")
        package_root = str(Path(__file__).resolve().parents[2])
        entry = "import sys; sys.path.insert(0," + repr(package_root) + "); from gsched.execution.persistent import serve; serve(int(sys.argv[1]))"
        try:
            process = subprocess.Popen([sys.executable, "-I", "-B", "-c", entry, str(boot)],
                pass_fds=tuple(sorted(descriptors | {boot})), close_fds=True, start_new_session=True,
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                env={"PATH": os.defpath})
        finally:
            os.close(boot)
        try:
            binding = {"schema": PROTOCOL, "owner_id": owner_id, "endpoint": endpoint, "pid": process.pid,
                       "start_ticks": start_ticks(process.pid), "boot_id": boot_id(), "attempt_id": attempt_id,
                       "token": token}
            owner = PersistentOwner(binding, process=process)
            deadline = time.monotonic() + 10
            while True:
                try:
                    owner.poll()
                    break
                except OwnerUnavailable:
                    if process.poll() is not None or time.monotonic() >= deadline:
                        raise OwnerUnavailable(lost=process.poll() is not None) from None
                    time.sleep(.02)
            return Prepared(envelope, owner)
        except BaseException:
            # No start was sent. Prepared expiry safely closes an unreferenced service.
            if process.poll() is not None:
                process.wait()
            raise


def serve(startup_fd):
    raw = os.pread(startup_fd, MAX_FRAME + 1, 0)
    os.close(startup_fd)
    if len(raw) > MAX_FRAME:
        raise ValueError("owner startup exceeds limit")
    startup = json.loads(raw)
    binding = {"schema": PROTOCOL, "owner_id": startup["owner_id"], "endpoint": startup["endpoint"],
               "pid": os.getpid(), "start_ticks": start_ticks(os.getpid()), "boot_id": boot_id(),
               "attempt_id": startup["identity"]["attempt_id"], "token": startup["token"]}
    validate_binding(binding)
    bindings = {int(k): v for k, v in startup["bindings"].items()}
    inherited = {startup["executable_fd"], startup["cwd_fd"], *bindings.values()}
    constraints = None
    if "constraints" in startup:
        from .constraints import LaunchConstraints
        declaration = dict(startup["constraints"])
        declaration["cpu_affinity"] = tuple(declaration["cpu_affinity"])
        constraints = LaunchConstraints(**declaration)
        if constraints.cgroup_procs_fd is not None:
            inherited.add(constraints.cgroup_procs_fd)
    wrapper = {"schema": IDENTITY_SCHEMA, "attempt": startup["identity"], "owner": public_binding(binding)}
    identity_fd = sealed_bytes(canonical_bytes(wrapper), "sched-owner-identity")
    bindings[4] = identity_fd
    inherited.add(identity_fd)
    try:
        prepared = LinuxFdBackend().prepare(ExecutionEnvelope(tuple(startup["argv"]), startup["env"]),
            executable_fd=startup["executable_fd"], cwd_fd=startup["cwd_fd"], fd_bindings=bindings, constraints=constraints)
    finally:
        for descriptor in inherited:
            os.close(descriptor)
    consumed = False
    final = None
    cancel_reason = None
    created = time.monotonic()
    launched = None
    finished = None
    stop = False

    def observe():
        nonlocal final, consumed, cancel_reason, finished
        current = time.monotonic()
        if not consumed and current - created >= startup["prepare_timeout"]:
            prepared.close()
            consumed = True
            final = ExecutionObservation("not_started", None, group_clean=True)
        observation = final if final is not None else prepared.owner.poll()
        if (launched is not None and startup["duration"] is not None
                and current - launched >= startup["duration"]
                and observation.status in ("running", "cleanup_pending") and cancel_reason is None):
            cancel_reason = "timed_out"
            prepared.owner.cancel()
            observation = prepared.owner.poll()
        if observation.status in ("exited", "not_started") and observation.group_clean is True:
            if finished is None:
                finished = current
        return observation

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind("\0" + binding["endpoint"])
        listener.listen(8)
        while not stop:
            observation = observe()
            if finished is not None and time.monotonic() - finished >= startup["terminal_retention"]:
                break
            if not select.select([listener], [], [], .05)[0]:
                continue
            stream, _ = listener.accept()
            with stream:
                stream.settimeout(.2)
                try:
                    _, uid, _ = _peer(stream)
                    if uid != os.getuid():
                        continue
                    request = _verified(_receive(stream), binding["token"])
                    if (set(request) != {"schema", "owner_id", "nonce", "op", "parameters"}
                            or request["schema"] != PROTOCOL or request["owner_id"] != binding["owner_id"]
                            or type(request["nonce"]) is not str
                            or re.fullmatch("[0-9a-f]{32}", request["nonce"]) is None
                            or type(request["parameters"]) is not dict):
                        continue
                    op, parameters = request["op"], request["parameters"]
                    error = None
                    if op == "start" and not parameters:
                        if not consumed:
                            consumed = True
                            launched = time.monotonic()
                            try:
                                prepared.launch()
                            except OSError:
                                pass  # Native owner records original launch failure.
                    elif op == "abandon_prepared" and not parameters:
                        if not consumed:
                            prepared.close()
                            consumed = True
                            final = ExecutionObservation("not_started", None, group_clean=True)
                    elif op == "cancel" and set(parameters) == {"grace_period"}:
                        grace = parameters["grace_period"]
                        if type(grace) not in (int, float) or not math.isfinite(grace) or not 0 <= grace <= 300:
                            error = "invalid_grace"
                        elif observation.status in ("running", "cleanup_pending"):
                            cancel_reason = cancel_reason or "cancelled"
                            prepared.owner.cancel(grace)
                    elif op == "close" and not parameters:
                        if not consumed:
                            prepared.close()
                            consumed = True
                            final = ExecutionObservation("not_started", None, group_clean=True)
                        observation = observe()
                        if observation.status in ("exited", "not_started") and observation.group_clean is True:
                            stop = True
                        else:
                            error = "owner_active"
                    elif op != "poll" or parameters:
                        error = "invalid_operation"
                    observation = observe()
                    response = {"schema": PROTOCOL, "owner_id": binding["owner_id"], "nonce": request["nonce"],
                                "observation": asdict(observation), "cancel_reason": cancel_reason, "error": error}
                    _send(stream, _signed(response, binding["token"]))
                except (OSError, EOFError, ValueError, TypeError):
                    # A lost response or invalid request never repeats process creation.
                    continue
    if final is None:
        prepared.owner.close()
    prepared.close()
