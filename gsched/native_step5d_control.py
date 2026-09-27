"""Stop-only Step 5D request owner and direct-parent control exchange.

This module prepares a retained NativeLaunchPlan but never launches a process.
Only a separately reviewed isolated test adapter may map and exec that plan.
The peer endpoint is private, never copied or returned. No daemon route, logical
execution, seventh field or formal publication is provided.
"""
from __future__ import annotations

import errno
import hashlib
import os
import secrets
import socket
import stat
import struct
import sys
import threading
import time
from pathlib import Path
from typing import Any

from . import _native_step5d_linux as linux
from . import native_step5d_protocol as protocol
from . import native_step5d_wire as wire
from .native_launch import NativeLaunchPlan, _project_root_identity
from .native_deployment import NativeDeployment, require_deployment
from .native_step5d import _create_step5d_no_data_launch_owner

_SESSION_AUTHORITY = object()
_OUTER = struct.Struct("!I")
_UCRED = struct.Struct("3i")
_MSG_NOSIGNAL = 0x4000
_SCM_CREDENTIALS = 2


def _fail(reason: str) -> None:
    raise protocol.Step5DProtocolViolation(reason)


def _require_linux() -> None:
    if not sys.platform.startswith("linux"):
        _fail("platform_or_capability_unavailable")
    if (
        not callable(getattr(socket.socket, "sendmsg", None))
        or int(socket.SOL_SOCKET) != 1
        or int(getattr(socket, "SCM_CREDENTIALS", _SCM_CREDENTIALS)) != 2
        or int(getattr(socket, "MSG_NOSIGNAL", _MSG_NOSIGNAL)) != 0x4000
        or _UCRED.size != 12
    ):
        _fail("platform_or_capability_unavailable")


def _root_observation(root_fd: int, path: str, *, deployment: NativeDeployment) -> dict[str, Any]:
    """Reopen every absolute component without following a symlink."""
    if (
        type(path) is not str or not path.startswith("/") or path == "/"
        or "\x00" in path or any(p in ("", ".", "..") for p in path[1:].split("/"))
    ):
        _fail("project_root_identity_mismatch")
    cursor = -1
    try:
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
        cursor = os.open("/", flags)
        for component in path[1:].split("/"):
            try:
                next_fd = os.open(component, flags, dir_fd=cursor)
            except OSError as exc:
                if exc.errno in (errno.ELOOP, errno.ENOENT, errno.ENOTDIR):
                    raise protocol.Step5DProtocolViolation("project_root_identity_mismatch") from exc
                raise
            os.close(cursor)
            cursor = next_fd
        retained, observed = os.fstat(root_fd), os.fstat(cursor)
        fields = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid")
        if (
            not stat.S_ISDIR(retained.st_mode) or retained.st_nlink <= 0
            or observed.st_nlink <= 0
            or any(getattr(retained, f) != getattr(observed, f) for f in fields)
        ):
            _fail("project_root_identity_mismatch")
        return {"project": require_deployment(deployment).project, "canonical_absolute_path": path,
                **{f: getattr(retained, f) for f in fields}}
    except OSError as exc:
        raise protocol.Step5DProtocolViolation("project_root_io_failure") from exc
    finally:
        if cursor >= 0:
            os.close(cursor)


def _peer_observation() -> dict[str, Any]:
    try:
        identity = linux.observe_process_identity(os.getpid())
        return {"peer_role": protocol.PEER_ROLE,
                "credentials": {"pid": os.getpid(), "uid": os.getuid(), "gid": os.getgid()},
                "process_identity": identity}
    except (OSError, linux.Step5DLinuxError) as exc:
        raise protocol.Step5DProtocolViolation("peer_identity_io_failure") from exc


def _arm(channel: socket.socket, deadline: float) -> None:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        _fail("control_channel_io_failure")
    channel.settimeout(remaining)


def _receive_exact(channel: socket.socket, length: int, deadline: float) -> bytes:
    chunks = []
    remaining = length
    while remaining:
        _arm(channel, deadline)
        try:
            chunk = channel.recv(remaining)
        except InterruptedError:
            continue
        except OSError as exc:
            raise protocol.Step5DProtocolViolation("control_channel_io_failure") from exc
        if not chunk:
            _fail("control_channel_io_failure")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _receive_frame(channel: socket.socket, deadline: float) -> bytes:
    outer = _receive_exact(channel, _OUTER.size, deadline)
    length, = _OUTER.unpack(outer)
    if not wire.NATIVE_LAUNCH_WIRE_HEADER_BYTES < length <= (
        wire.NATIVE_LAUNCH_WIRE_HEADER_BYTES + wire.NATIVE_LAUNCH_WIRE_MAX_BODY
    ):
        _fail("control_message_invalid")
    return outer + _receive_exact(channel, length, deadline)


def _receive_eof(channel: socket.socket, deadline: float) -> None:
    while True:
        _arm(channel, deadline)
        try:
            trailing = channel.recv(1)
            break
        except InterruptedError:
            continue
        except OSError as exc:
            raise protocol.Step5DProtocolViolation("control_channel_io_failure") from exc
    if trailing:
        _fail("control_replay_detected")


def _send_ack(channel: socket.socket, frame: bytes, deadline: float) -> None:
    credentials = _UCRED.pack(os.getpid(), os.getuid(), os.getgid())
    offset = 0
    while offset < len(frame):
        _arm(channel, deadline)
        try:
            # Only the first successful nonempty segment carries user-supplied
            # credentials. EINTR consumes no bytes; later credentials are kernel-owned.
            written = channel.sendmsg(
                [frame[offset:]],
                [(socket.SOL_SOCKET, _SCM_CREDENTIALS, credentials)] if offset == 0 else [],
                _MSG_NOSIGNAL,
            )
        except InterruptedError:
            continue
        except OSError as exc:
            raise protocol.Step5DProtocolViolation("control_channel_io_failure") from exc
        if written <= 0 or written > len(frame) - offset:
            _fail("control_channel_io_failure")
        offset += written
    while True:
        _arm(channel, deadline)
        try:
            channel.shutdown(socket.SHUT_WR)
            return
        except InterruptedError:
            continue
        except OSError as exc:
            raise protocol.Step5DProtocolViolation("control_channel_io_failure") from exc


class NativeStep5DRequestOwner:
    """Consume each generated nonce once for this creating-process lifetime."""

    __slots__ = ("_pid", "_lock", "_consumed_nonces", "_deployment")

    def __init__(self, *, deployment: NativeDeployment) -> None:
        self._deployment = require_deployment(deployment)
        _require_linux()
        self._pid = os.getpid()
        self._lock = threading.Lock()
        self._consumed_nonces: set[str] = set()

    def _require_owner(self) -> None:
        if os.getpid() != self._pid:
            _fail("peer_credentials_mismatch")

    def _claim_nonce(self, nonce: str) -> None:
        self._require_owner()  # Do not acquire an inherited potentially locked lock.
        if type(nonce) is not str or len(nonce) != 64 or any(c not in "0123456789abcdef" for c in nonce):
            _fail("request_schema_mismatch")
        with self._lock:
            if nonce in self._consumed_nonces:
                _fail("launch_nonce_already_consumed")
            self._consumed_nonces.add(nonce)

    def prepare(
        self, *, target_phase_profile: str, run_id: str, launch_marker: str,
        project_root_path: str, project_root_fd: int, launcher_fd: int,
        launcher_sha256: str, log_relative_path: str, log_fd: int,
    ) -> NativeStep5DPreparedStop:
        self._require_owner()
        _require_linux()
        nonce = secrets.token_hex(32)
        self._claim_nonce(nonce)  # Before socket, memfd, retained copies or child.
        root = _root_observation(project_root_fd, project_root_path, deployment=self._deployment)
        peer = _peer_observation()
        body = protocol.build_request_body(
            deployment=self._deployment,
            target_phase_profile=target_phase_profile,
            scheduler_identity_prefix=protocol.scheduler_prefix(
                deployment=self._deployment,
                target_phase_profile=target_phase_profile, run_id=run_id,
                launch_marker=launch_marker,
            ), launch_nonce=nonce, control_peer_expectation=peer,
            project_root_expectation=root,
        )
        frame = protocol.encode_request_frame(body, deployment=self._deployment)
        digests = protocol.request_digests(frame, deployment=self._deployment)
        request_fds: list[int] = []
        transport_owner = None
        retained_root = -1
        try:
            request_fd, request_fds = linux._sealed_readonly_request(frame)
            retained_root = os.dup(project_root_fd)
            os.set_inheritable(retained_root, False)
            transport_owner = _create_step5d_no_data_launch_owner(
                profile_id="m2b-step5d-" + target_phase_profile,
                profile_sha256=hashlib.sha256(protocol.canonical_json_bytes(
                    list(self._deployment.argv(target_phase_profile))
                )).hexdigest(),
                project_root_identity_sha256=_project_root_identity(
                    project_root_path, os.fstat(retained_root)
                ), project_root_path=project_root_path,
                logical_submitted_argv=self._deployment.argv(target_phase_profile),
                launcher_sha256=launcher_sha256,
                request_frame_sha256=digests.request_frame_sha256,
                request_body_sha256=digests.request_body_sha256,
                log_relative_path=log_relative_path, launcher_fd=launcher_fd,
                request_fd=request_fd, project_root_fd=retained_root, log_fd=log_fd,
            )
            session = NativeStep5DPreparedStop(
                _SESSION_AUTHORITY, transport_owner, retained_root, body, frame, peer, root,
                deployment=self._deployment,
            )
            transport_owner = None
            retained_root = -1
            return session
        finally:
            if transport_owner is not None:
                transport_owner.close()
            if retained_root >= 0:
                os.close(retained_root)
            for fd in request_fds:
                os.close(fd)


class NativeStep5DPreparedStop:
    """One stop-only exchange; peer and monitor binding never leave the owner."""

    __slots__ = ("_pid", "_owner", "_root_fd", "_body", "_frame", "_peer", "_root",
                 "_state", "_deadline", "_child_pid", "_challenge", "_terminal", "_failure", "_deployment")
    formal_ready = False
    scientific_result = False
    logical_python_executed = False
    scheduler_role_authority_claimed = False
    external_anchor_authenticated = False
    external_formal_authority_claimed = False

    def __init__(self, authority: object, owner: Any, root_fd: int, body: bytes,
                 frame: bytes, peer: dict[str, Any], root: dict[str, Any], *, deployment: NativeDeployment) -> None:
        if authority is not _SESSION_AUTHORITY:
            _fail("authority_invariant_failure")
        self._deployment = require_deployment(deployment)
        self._pid = os.getpid()
        self._owner, self._root_fd = owner, root_fd
        self._body, self._frame, self._peer, self._root = body, frame, peer, root
        self._state = "PREPARED"
        self._deadline = 0.0
        self._child_pid = None
        self._challenge = self._terminal = self._failure = None

    def _require(self, state: str) -> None:
        if os.getpid() != self._pid:
            _fail("peer_credentials_mismatch")
        if self._failure is not None:
            raise self._failure
        if self._state != state:
            self._reject("control_sequence_mismatch")

    def _reject(self, reason: str) -> None:
        if self._failure is None:
            self._failure = protocol.Step5DProtocolViolation(reason)
        self._state = "FAILED"
        raise self._failure

    @property
    def plan(self) -> NativeLaunchPlan:
        self._require("PREPARED")
        return self._owner.plan

    @property
    def request_body(self) -> bytes:
        return self._body

    @property
    def request_frame(self) -> bytes:
        return self._frame

    def mark_child_started(self, child_pid: int) -> None:
        """Test adapter records its actual fork result; no process is created here."""
        self._require("PREPARED")
        if type(child_pid) is not int or child_pid <= 0 or child_pid == self._pid:
            self._reject("peer_credentials_mismatch")
        self._child_pid = child_pid
        self._deadline = time.monotonic() + protocol.CONTROL_DEADLINE_MS / 1000
        # Release the parent's native endpoint copy so native EOF is observable.
        # The child already inherited its independent descriptor table at fork.
        self._owner.plan.close()
        self._state = "STARTED"

    def exchange_stop(self) -> dict[str, Any]:
        self._require("STARTED")
        channel = self._owner._peer_endpoint
        try:
            first = _receive_frame(channel, self._deadline)
            decoded = wire.decode_native_launch_frame(first)
            if decoded.sequence == protocol.CONTROL_FAILURE_SEQUENCE:
                terminal = self._validate_failure(first)
            else:
                challenge = protocol.parse_challenge_frame(first)
                request = protocol.validate_request_body(self._body, deployment=self._deployment)
                digests = protocol.request_digests(self._frame, deployment=self._deployment)
                for key, expected in (
                    ("request_frame_sha256", digests.request_frame_sha256),
                    ("request_body_sha256", digests.request_body_sha256),
                    ("launch_nonce", request["launch_nonce"]),
                    ("target_phase_profile", request["target_phase_profile"]),
                ):
                    if challenge[key] != expected:
                        _fail("request_digest_mismatch" if "sha256" in key else
                              "control_nonce_mismatch" if "nonce" in key else "control_message_invalid")
                self._revalidate()
                self._challenge = challenge
                self._state = "CHALLENGE_BOUND"
                _send_ack(channel, protocol.encode_ack_frame(challenge), self._deadline)
                self._state = "ACK_SENT"
                final = _receive_frame(channel, self._deadline)
                decoded = wire.decode_native_launch_frame(final)
                if decoded.sequence == protocol.CONTROL_FAILURE_SEQUENCE:
                    terminal = self._validate_failure(final)
                else:
                    terminal = protocol.parse_receipt_frame(final, expected_binding=challenge)
            _receive_eof(channel, self._deadline)
            if terminal["schema"] == protocol.RECEIPT_SCHEMA:
                self._revalidate()
            self._terminal = terminal
            self._state = "TERMINAL"
            return dict(terminal)
        except protocol.Step5DProtocolViolation as exc:
            self._failure = exc
            self._state = "FAILED"
            raise
        except (OSError, ValueError) as exc:
            failure = protocol.Step5DProtocolViolation(
                "control_channel_io_failure" if isinstance(exc, OSError) else "control_message_invalid"
            )
            self._failure, self._state = failure, "FAILED"
            raise failure from exc

    def _validate_failure(self, frame: bytes) -> dict[str, Any]:
        failure = protocol.parse_failure_frame(frame)
        digests = protocol.request_digests(self._frame, deployment=self._deployment)
        for key in ("request_frame_sha256", "request_body_sha256"):
            if failure[key] is not None and failure[key] != getattr(digests, key):
                _fail("request_digest_mismatch")
        return failure

    def _revalidate(self) -> None:
        if _peer_observation() != self._peer:
            _fail("peer_process_identity_mismatch")
        if _root_observation(self._root_fd, self._root["canonical_absolute_path"],
                             deployment=self._deployment) != self._root:
            _fail("project_root_identity_mismatch")

    def confirm_child_exit(self, waited_pid: int, wait_status: int) -> dict[str, Any]:
        """Bind the isolated adapter's exact same-child wait observation."""
        self._require("TERMINAL")
        if type(waited_pid) is not int or waited_pid != self._child_pid or type(wait_status) is not int:
            self._reject("peer_credentials_mismatch")
        expected = 69 if self._terminal["schema"] == protocol.RECEIPT_SCHEMA else self._terminal["exit_code"]
        if not os.WIFEXITED(wait_status) or os.WEXITSTATUS(wait_status) != expected:
            self._reject("control_message_invalid")
        self._state = "WAIT_CONFIRMED"
        return {"schema": "sched_step5d_isolated_stop_observation/v1",
                "child_pid": waited_pid, "child_exit_code": expected,
                "terminal": dict(self._terminal), "formal_ready": False,
                "scientific_result": False, "logical_python_executed": False}

    def close(self) -> None:
        if self._state == "CLOSED":
            return
        try:
            self._owner.close()
        finally:
            if self._root_fd >= 0:
                os.close(self._root_fd)
                self._root_fd = -1
            self._state = "CLOSED"

    def __enter__(self) -> NativeStep5DPreparedStop:
        self._require("PREPARED")
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


__all__ = ["NativeStep5DRequestOwner", "NativeStep5DPreparedStop"]
