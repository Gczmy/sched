"""Small lifecycle API with original-parent wait authority.

The optional Linux backend accepts already retained executable/working-directory
FDs.  Descriptor bindings map child descriptor numbers to caller-owned source
FDs; preparation duplicates them and never takes ownership of the caller's FDs.
There are no plugin discovery, application callbacks or opaque protocol fields.
"""

from __future__ import annotations

import os
import math
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

INTERFACE_VERSION = "sched-execution/v1"


class BackendUnavailable(RuntimeError):
    """A requested backend is absent or lacks required kernel capabilities."""

    def __init__(self, message="", *, reason="backend_unavailable"):
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class ExecutionEnvelope:
    argv: tuple[str, ...]
    env: Mapping[str, str] = field(default_factory=dict)
    cwd: str | None = None
    interface_version: str = INTERFACE_VERSION

    def __post_init__(self) -> None:
        if self.interface_version != INTERFACE_VERSION:
            raise ValueError("unsupported execution interface version")
        if type(self.argv) is not tuple or not self.argv:
            raise ValueError("argv must be a nonempty tuple")
        for item in self.argv:
            if type(item) is not str or "\0" in item:
                raise ValueError("argv must contain strings without NUL")
        if not self.argv[0]:
            raise ValueError("argv[0] must be nonempty")
        if not isinstance(self.env, Mapping):
            raise TypeError("env must be an explicit mapping")
        copied = dict(self.env)
        for key, value in copied.items():
            if type(key) is not str or not key or "=" in key or "\0" in key:
                raise ValueError("invalid environment name")
            if type(value) is not str or "\0" in value:
                raise ValueError("invalid environment value")
        if self.cwd is not None and (type(self.cwd) is not str or "\0" in self.cwd):
            raise ValueError("cwd must be a string without NUL")
        object.__setattr__(self, "env", MappingProxyType(copied))


@dataclass(frozen=True)
class ExecutionObservation:
    status: str
    pid: int | None
    returncode: int | None = None
    rusage: Mapping[str, int | float] | None = None
    launch_error: int | None = None
    group_clean: bool | None = None
    interface_version: str = INTERFACE_VERSION


_retained: dict[str, "Owner"] = {}
_registry_lock = threading.RLock()


def retained_owners() -> tuple["Owner", ...]:
    """Original in-memory owners, not restart recovery or PID reattachment."""
    with _registry_lock:
        return tuple(_retained.values())


class Owner:
    """Single original-parent owner, retained until explicit terminal close."""

    def __init__(self, *, native=None):
        self.owner_id = uuid.uuid4().hex
        self._creator_pid = os.getpid()
        self._creator_thread = threading.current_thread()
        self._native = native
        self._proc: subprocess.Popen | None = None
        self._state = "prepared"
        self._lock = threading.RLock()
        self._closed = False
        self._cancel_deadline: float | None = None
        self._pass_fds: tuple[int, ...] = ()
        self._start_new_session = True
        self._launch_error: int | None = None
        self._constraint_error_fd: int | None = None

    @property
    def pid(self) -> int | None:
        """Original child identity without consuming a wait observation."""
        self._check()
        if self._native is not None:
            return self._native.pid()
        return None if self._proc is None else self._proc.pid

    @property
    def process(self) -> subprocess.Popen:
        """Original Popen for existing scheduler supervisor integration only."""
        self._check()
        if self._proc is None:
            raise RuntimeError("owner has no original subprocess handle")
        return self._proc

    def _check(self) -> None:
        if self._closed:
            raise RuntimeError("execution owner is closed")
        if os.getpid() != self._creator_pid:
            raise RuntimeError("execution owner belongs to another process")
        if threading.current_thread() is not self._creator_thread:
            raise RuntimeError("execution owner belongs to its original thread")

    def _retain(self) -> None:
        with _registry_lock:
            _retained[self.owner_id] = self

    def _launch(self, envelope: ExecutionEnvelope, stdio: tuple[int | None, int | None]) -> None:
        with self._lock:
            self._check()
            if self._state != "prepared":
                raise RuntimeError("launch already attempted")
            # The handle is registered and consumed before entering process creation.
            self._state = "starting"
            self._retain()
            try:
                if self._native is not None:
                    self._native.start()
                else:
                    options = {"pass_fds": self._pass_fds} if os.name == "posix" else {}
                    if os.name != "posix" and self._pass_fds:
                        raise BackendUnavailable("pass_fds requires POSIX")
                    self._proc = subprocess.Popen(
                        list(envelope.argv), env=dict(envelope.env), cwd=envelope.cwd,
                        stdin=subprocess.DEVNULL, stdout=stdio[0], stderr=stdio[1],
                        close_fds=True,
                        start_new_session=(os.name == "posix" and self._start_new_session),
                        **options,
                    )
                    self._proc._sched_execution_owner = self
                self._state = "running"
            except OSError as error:
                if self._native is None and self._proc is None:
                    # Popen reports ordinary exec failure only after reaping
                    # its failed child; asynchronous interruption stays unknown.
                    self._state = "not_started"
                    self._launch_error = error.errno
                else:
                    self._state = "starting" if self._native is not None else "running"
                raise
            except BaseException:
                # Native owns its PID before returning to Python.  Popen return
                # interruption cannot reconstruct a lost handle and must not replay.
                self._state = ("starting" if self._native is not None else
                               "running" if self._proc is not None else "authority_lost")
                raise

    def _inspect_constraint_error(self):
        if self._constraint_error_fd is not None:
            try:
                raw = os.read(self._constraint_error_fd, 64)
            except BlockingIOError:
                raw = None
            if raw is not None:
                if raw:
                    self._launch_error = int(raw) if raw.strip().isdigit() and len(raw) < 16 else 5
                os.close(self._constraint_error_fd)
                self._constraint_error_fd = None

    def poll(self) -> ExecutionObservation:
        with self._lock:
            self._check()
            self._inspect_constraint_error()
            if self._state == "prepared":
                return ExecutionObservation("prepared", None)
            if self._native is not None:
                info = self._native.poll()
                if (self._cancel_deadline is not None and
                        time.monotonic() >= self._cancel_deadline and
                        info["status"] in ("running", "cleanup_pending")):
                    self._native.send_signal(signal.SIGKILL)
                    self._cancel_deadline = None
                    info = self._native.poll()
                self._state = info["status"]
                return ExecutionObservation(**info)
            if self._state in ("authority_lost", "not_started"):
                return ExecutionObservation(self._state, None,
                                            launch_error=self._launch_error,
                                            group_clean=True if self._state == "not_started" else None)
            assert self._proc is not None
            result = self._proc.poll()
            if result is not None:
                self._inspect_constraint_error()
            group_clean = None
            if os.name == "posix" and self._start_new_session and result is not None:
                try:
                    os.killpg(self._proc.pid, 0)
                    group_clean = False
                except ProcessLookupError:
                    group_clean = True
            self._state = ("cleanup_pending" if group_clean is False else "exited") if result is not None else "running"
            if (self._cancel_deadline is not None and time.monotonic() >= self._cancel_deadline and
                    self._state in ("running", "cleanup_pending")):
                self._signal_subprocess(signal.SIGKILL)
                self._cancel_deadline = None
            return ExecutionObservation(self._state, self._proc.pid, result, group_clean=group_clean,
                                        launch_error=self._launch_error)

    def _signal_subprocess(self, signum: int) -> None:
        assert self._proc is not None
        if os.name == "posix" and self._start_new_session:
            try:
                os.killpg(self._proc.pid, signum)
            except ProcessLookupError:
                pass
        elif signum == signal.SIGTERM:
            self._proc.terminate()
        else:
            self._proc.kill()

    def cancel(self, grace_period: float = 2.0) -> None:
        if type(grace_period) not in (int, float) or not math.isfinite(grace_period) or grace_period < 0:
            raise ValueError("grace_period must be finite and nonnegative")
        with self._lock:
            self._check()
            observation = self.poll()
            if observation.status == "authority_lost":
                raise RuntimeError("cannot cancel without original child authority")
            if observation.status in ("exited", "prepared", "not_started"):
                return
            if self._native is not None:
                self._native.send_signal(signal.SIGTERM)
            else:
                self._signal_subprocess(signal.SIGTERM)
            # Poll drives escalation. Repeated cancellation must not defer KILL.
            if self._cancel_deadline is None:
                self._cancel_deadline = time.monotonic() + grace_period

    def wait(self, timeout: float | None = None) -> ExecutionObservation:
        if timeout is not None and (type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout < 0):
            raise ValueError("timeout must be finite and nonnegative")
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            observation = self.poll()
            if observation.status in ("exited", "authority_lost", "prepared", "not_started"):
                return observation
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("child has not reached a terminal observation")
            time.sleep(0.005)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._check()
            observation = self.poll()
            if observation.status not in ("prepared", "exited", "not_started"):
                raise RuntimeError("cannot close active or unresolved owner")
            if self._native is not None:
                self._native.close()
            if self._constraint_error_fd is not None:
                os.close(self._constraint_error_fd)
                self._constraint_error_fd = None
            self._closed = True
            with _registry_lock:
                _retained.pop(self.owner_id, None)


class Prepared:
    """Owned preparation with one launch attempt, including failed attempts."""

    def __init__(self, envelope: ExecutionEnvelope, owner: Owner,
                 stdio: tuple[int | None, int | None] = (None, None),
                 owned_stdio: tuple[int, ...] = ()):
        self.envelope = envelope
        self.owner = owner
        self._stdio = stdio
        self._owned_stdio = owned_stdio
        self._consumed = False
        self._closed = False

    def _close_stdio(self) -> None:
        for descriptor in self._owned_stdio:
            os.close(descriptor)
        self._owned_stdio = ()

    def launch(self) -> Owner:
        if self._closed or self._consumed:
            raise RuntimeError("prepared execution already consumed or closed")
        self._consumed = True
        try:
            self.owner._launch(self.envelope, self._stdio)
        finally:
            self._close_stdio()
        return self.owner

    def close(self) -> None:
        if self._closed:
            return
        if not self._consumed:
            self.owner.close()
        self._close_stdio()
        self._closed = True

    def __enter__(self) -> "Prepared":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class SubprocessBackend:
    interface_version = INTERFACE_VERSION
    capabilities = frozenset({"direct_child_wait_v1", "process_group_v1"}) if os.name == "posix" else frozenset({"direct_child_wait_v1"})

    def prepare(self, envelope: ExecutionEnvelope, *, stdout_fd: int | None = None,
                stderr_fd: int | None = None, pass_fds: tuple[int, ...] = (),
                start_new_session: bool = True, constraints=None) -> Prepared:
        if type(envelope) is not ExecutionEnvelope:
            raise TypeError("prepare requires ExecutionEnvelope")
        if type(start_new_session) is not bool:
            raise ValueError("start_new_session must be boolean")
        if type(pass_fds) is not tuple or any(type(fd) is not int or fd < 0 for fd in pass_fds):
            raise ValueError("pass_fds must be a tuple of nonnegative integers")
        if os.name != "posix" and pass_fds:
            raise BackendUnavailable("pass_fds requires POSIX")
        for descriptor in pass_fds:
            os.fstat(descriptor)
        if constraints is not None:
            from .constraints import LaunchConstraints
            if type(constraints) is not LaunchConstraints:
                raise TypeError("constraints require LaunchConstraints")
            constraints.validate()
            if (constraints.cgroup_procs_fd is not None
                    and constraints.cgroup_procs_fd in (*pass_fds, stdout_fd, stderr_fd)):
                raise ValueError("cgroup control descriptor cannot be passed to the program")
        owned: list[int] = []
        owner = None
        try:
            descriptors = []
            for descriptor in (stdout_fd, stderr_fd):
                if descriptor is None:
                    descriptors.append(None)
                else:
                    if type(descriptor) is not int or descriptor < 0:
                        raise ValueError("invalid stdio descriptor")
                    duplicate = os.dup(descriptor)
                    os.set_inheritable(duplicate, False)
                    owned.append(duplicate)
                    descriptors.append(duplicate)
            owner = Owner()
            owner._pass_fds = pass_fds
            owner._start_new_session = start_new_session
            if constraints is not None:
                from .constraints import SUBPROCESS_ENTRY
                from ..execution_policy import canonical_bytes, sealed_bytes
                cgroup = None
                if constraints.cgroup_procs_fd is not None:
                    cgroup = os.dup(constraints.cgroup_procs_fd)
                    owned.append(cgroup)
                raw = canonical_bytes({"argv": envelope.argv, "env": dict(envelope.env),
                    "cpu_affinity": constraints.cpu_affinity, "cgroup_procs_fd": cgroup})
                if len(raw) > 2 * 1024 * 1024:
                    raise ValueError("constraint bootstrap exceeds 2 MiB")
                boot = sealed_bytes(raw, "sched-launch-constraints")
                owned.append(boot)
                error_read, error_write = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)
                owner._constraint_error_fd = error_read
                owned.append(error_write)
                owner._pass_fds = tuple(sorted(set(pass_fds) | {boot, error_write} | ({cgroup} if cgroup is not None else set())))
                envelope = ExecutionEnvelope((sys.executable, "-I", "-S", "-B", "-c", SUBPROCESS_ENTRY,
                    str(boot), str(error_write)), {"PATH": os.defpath}, envelope.cwd)
            return Prepared(envelope, owner, tuple(descriptors), tuple(owned))
        except BaseException:
            for descriptor in owned:
                os.close(descriptor)
            if owner is not None and owner._constraint_error_fd is not None:
                os.close(owner._constraint_error_fd)
            raise


class LinuxFdBackend:
    interface_version = INTERFACE_VERSION
    capabilities = frozenset({"fd_exec_v1", "direct_child_wait_v1", "wait_rusage_v1", "process_group_v1"})

    def __init__(self) -> None:
        if sys.platform != "linux":
            raise BackendUnavailable("Linux FD backend requires Linux", reason="non_linux")
        try:
            from . import _fdexec
        except ImportError as error:
            raise BackendUnavailable("native backend is not explicitly built/installed", reason="native_unavailable") from error
        if _fdexec.interface_version != INTERFACE_VERSION:
            raise BackendUnavailable("native execution interface version mismatch", reason="native_interface_mismatch")
        try:
            _fdexec.check_capabilities()
        except OSError as error:
            raise BackendUnavailable("kernel lacks required execveat/close_range support", reason="kernel_fd_exec_unavailable") from error
        self._module = _fdexec

    def prepare(self, envelope: ExecutionEnvelope, *, executable_fd: int,
                cwd_fd: int | None = None,
                fd_bindings: Mapping[int, int] | None = None, constraints=None) -> Prepared:
        if type(envelope) is not ExecutionEnvelope:
            raise TypeError("prepare requires ExecutionEnvelope")
        if envelope.cwd is not None:
            raise ValueError("FD backend requires cwd_fd, not a pathname cwd")
        if type(executable_fd) is not int or executable_fd < 0:
            raise ValueError("invalid executable descriptor")
        if cwd_fd is not None and (type(cwd_fd) is not int or cwd_fd < 0):
            raise ValueError("invalid working-directory descriptor")
        bindings = dict(fd_bindings or {})
        for target, source in bindings.items():
            if type(target) is not int or type(source) is not int or min(target, source) < 0:
                raise ValueError("descriptor bindings must map nonnegative integers")
            if target > 65535:
                raise ValueError("descriptor target exceeds interface limit")
        options = ()
        if constraints is not None:
            from .constraints import LaunchConstraints, CONSTRAINTS_VERSION
            if type(constraints) is not LaunchConstraints:
                raise TypeError("constraints require LaunchConstraints")
            constraints.validate()
            if constraints.cgroup_procs_fd in bindings.values():
                raise ValueError("cgroup control descriptor cannot be bound to the program")
            if getattr(self._module, "constraints_interface_version", None) != CONSTRAINTS_VERSION:
                raise BackendUnavailable("native constraints interface absent", reason="native_constraints_unavailable")
            options = (constraints.cpu_affinity, -1 if constraints.cgroup_procs_fd is None else constraints.cgroup_procs_fd)
        native = self._module.prepare(
            executable_fd, envelope.argv,
            tuple(f"{key}={value}" for key, value in sorted(envelope.env.items())),
            -1 if cwd_fd is None else cwd_fd, tuple(sorted(bindings.items())), *options,
        )
        return Prepared(envelope, Owner(native=native))
