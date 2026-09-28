"""Fail-closed native-launch plan foundation.

The public batch command is a logical, auditable request.  It is never an
actual launcher argv.  This module reserves an internal, fixed native entry
interface and validates retained descriptors before a future Linux FD-exec
backend is allowed to consume them.

No current submission path creates a :class:`NativeLaunchPlan`, and the
executor backend deliberately remains unavailable.  In particular, this file
does not authorize pathname execution, ``subprocess.Popen`` fallback, or
execution of ``logical_submitted_argv``.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import sqlite3
import socket
import stat
import struct
import sys
from typing import Any


NATIVE_LAUNCH_PLAN_SCHEMA = "sched_native_launch_plan_v1"
NATIVE_ACTUAL_ARGV = (
    "m2b-exec-monitor[native-entry-v1]",
    "--native-entry-v1",
)
NATIVE_ACTUAL_ENV_ITEMS: tuple[tuple[str, str], ...] = ()
NATIVE_REQUEST_FD = 3
NATIVE_CONTROL_FD = 4
NATIVE_PROJECT_ROOT_FD = 5
NATIVE_LAUNCH_WIRE_SCHEMA = "m2b_scheduler_native_launch_control_frame/v1"
NATIVE_LAUNCH_WIRE_MAGIC = b"M2BNLC01"
NATIVE_LAUNCH_WIRE_VERSION = 1
NATIVE_LAUNCH_WIRE_OUTER_HEADER_BYTES = 4
NATIVE_LAUNCH_WIRE_HEADER_BYTES = 20
NATIVE_LAUNCH_WIRE_MAX_BODY = 1024 * 1024
NATIVE_LAUNCH_CHANNEL_REQUEST = 1
NATIVE_LAUNCH_MESSAGE_REQUEST = 1
NATIVE_LAUNCH_FLAGS_NONE = 0
NATIVE_LAUNCH_REQUEST_SEQUENCE = 0

_LOWER_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_MAX_LAUNCHER_BYTES = 256 * 1024 * 1024
_MAX_REQUEST_BYTES = 4 + NATIVE_LAUNCH_WIRE_HEADER_BYTES + NATIVE_LAUNCH_WIRE_MAX_BODY
_PLAN_AUTHORITY = object()
_OUTER_HEADER = struct.Struct("!I")
_WIRE_HEADER = struct.Struct("!8sBBBBII")
_LINUX_O_PATH = getattr(os, "O_PATH", 0o10000000)


class NativeLaunchPlanError(ValueError):
    """A retained native-launch plan is malformed or has drifted."""


class NativeLaunchUnavailable(RuntimeError):
    """The reviewed FD-exec backend is unavailable and no fallback is legal."""


def _digest(value: Any, where: str) -> str:
    if type(value) is not str or _LOWER_SHA256_RE.fullmatch(value) is None:
        raise NativeLaunchPlanError(f"{where} must be lowercase 64-hex")
    return value


def _logical_argv(value: Any) -> tuple[str, ...]:
    if type(value) not in (list, tuple) or not value:
        raise NativeLaunchPlanError("logical submitted argv must be non-empty")
    detached: list[str] = []
    for index, token in enumerate(value):
        if type(token) is not str or not token or "\0" in token:
            raise NativeLaunchPlanError(
                f"logical submitted argv[{index}] must be a non-empty NUL-free string"
            )
        detached.append(token)
    if not os.path.isabs(detached[0]) or os.path.normpath(detached[0]) != detached[0]:
        raise NativeLaunchPlanError(
            "logical submitted argv[0] must be a normalized absolute path"
        )
    return tuple(detached)


def _fd(value: Any, where: str) -> int:
    if type(value) is not int or value < 0:
        raise NativeLaunchPlanError(f"{where} must be a nonnegative integer FD")
    return value


def _log_relative_path(value: Any) -> str:
    if (
        type(value) is not str
        or not value
        or "\0" in value
        or "\\" in value
        or os.path.isabs(value)
        or os.path.normpath(value) != value
    ):
        raise NativeLaunchPlanError(
            "native log relative path must be normalized POSIX-style text"
        )
    parts = value.split("/")
    if len(parts) < 2 or parts[0] != "logs" or any(
        part in ("", ".", "..") for part in parts
    ):
        raise NativeLaunchPlanError(
            "native log relative path must name a file below logs/"
        )
    return value


def _stable_fd_sha256(fd: int, *, maximum: int, where: str) -> tuple[str, os.stat_result]:
    try:
        before = os.fstat(fd)
    except OSError as exc:
        raise NativeLaunchPlanError(f"{where} cannot be fstat'ed") from exc
    if not stat.S_ISREG(before.st_mode):
        raise NativeLaunchPlanError(f"{where} must be a retained regular file")
    if before.st_size < 0 or before.st_size > maximum:
        raise NativeLaunchPlanError(f"{where} exceeds its byte bound")
    digest = hashlib.sha256()
    offset = 0
    try:
        while offset < before.st_size:
            chunk = os.pread(fd, min(1024 * 1024, before.st_size - offset), offset)
            if not chunk:
                raise NativeLaunchPlanError(f"{where} ended before its retained size")
            digest.update(chunk)
            offset += len(chunk)
        after = os.fstat(fd)
    except OSError as exc:
        raise NativeLaunchPlanError(f"{where} retained read failed") from exc
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_uid,
        before.st_gid,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_uid,
        after.st_gid,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if identity_before != identity_after or offset != before.st_size:
        raise NativeLaunchPlanError(f"{where} identity drifted during retained rehash")
    return digest.hexdigest(), after


def _project_root_identity(path: str, root_stat: os.stat_result) -> str:
    preimage = {
        "schema": "sched_native_exec_project_root_identity_v1",
        "canonical_path": path,
        "st_dev": root_stat.st_dev,
        "st_ino": root_stat.st_ino,
    }
    encoded = json.dumps(
        preimage,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_project_root_fd(
    project_root_fd: int,
    project_root_path: str,
    project_root_identity_sha256: str,
) -> None:
    """Re-attest a borrowed root FD before and after project-local I/O."""
    _fd(project_root_fd, "project_root_fd")
    _digest(
        project_root_identity_sha256,
        "native launch project-root identity sha256",
    )
    if (
        type(project_root_path) is not str
        or project_root_path == "/"
        or not os.path.isabs(project_root_path)
        or os.path.normpath(project_root_path) != project_root_path
        or os.path.realpath(project_root_path) != project_root_path
    ):
        raise NativeLaunchPlanError(
            "native launch project-root path must be canonical, non-root, and absolute"
        )
    try:
        root_stat = os.fstat(project_root_fd)
        descriptor_flags = fcntl.fcntl(project_root_fd, fcntl.F_GETFD)
        root_flags = fcntl.fcntl(project_root_fd, fcntl.F_GETFL)
    except OSError as exc:
        raise NativeLaunchPlanError(
            "native project-root FD cannot be inspected"
        ) from exc
    if descriptor_flags & fcntl.FD_CLOEXEC == 0:
        raise NativeLaunchPlanError(
            "native project-root FD must remain CLOEXEC"
        )
    if not stat.S_ISDIR(root_stat.st_mode):
        raise NativeLaunchPlanError("native project-root FD must be a directory")
    if root_stat.st_nlink == 0:
        raise NativeLaunchPlanError("native project-root FD is unlinked")
    if root_flags & _LINUX_O_PATH:
        raise NativeLaunchPlanError("native project-root FD must not use O_PATH")
    if (root_flags & os.O_ACCMODE) != os.O_RDONLY:
        raise NativeLaunchPlanError("native project-root FD must be read-only")
    try:
        path_stat = os.stat(project_root_path, follow_symlinks=False)
    except OSError as exc:
        raise NativeLaunchPlanError(
            "native project-root path cannot be re-attested"
        ) from exc
    if (
        not stat.S_ISDIR(path_stat.st_mode)
        or (path_stat.st_dev, path_stat.st_ino)
        != (root_stat.st_dev, root_stat.st_ino)
    ):
        raise NativeLaunchPlanError("native project-root path identity drifted")
    if (
        _project_root_identity(project_root_path, root_stat)
        != project_root_identity_sha256
    ):
        raise NativeLaunchPlanError("native project-root FD identity drifted")


def _open_project_local_log_fd(
    *,
    project_root_fd: int,
    project_root_path: str,
    project_root_identity_sha256: str,
    log_relative_path: str,
) -> int:
    """Create one fresh private log below a borrowed, re-attested root FD.

    The caller owns the returned source FD and must close it after the plan
    factory has made its retained copy. This helper never closes the borrowed
    root FD or removes a created log after a later validation failure.
    """
    if not sys.platform.startswith("linux"):
        raise NativeLaunchUnavailable(
            "native retained log creation requires Linux; no fallback is allowed"
        )
    _validate_project_root_fd(
        project_root_fd, project_root_path, project_root_identity_sha256
    )
    relative_log = _log_relative_path(log_relative_path)
    parts = relative_log.split("/")
    cursor = project_root_fd
    cursor_owned = False
    log_fd = -1
    try:
        for component in parts[:-1]:
            try:
                next_cursor = os.open(
                    component,
                    os.O_RDONLY
                    | os.O_DIRECTORY
                    | os.O_NOFOLLOW
                    | os.O_CLOEXEC,
                    dir_fd=cursor,
                )
            except OSError as exc:
                raise NativeLaunchPlanError(
                    "native log parent must be a retained non-symlink directory"
                ) from exc
            try:
                parent_stat = os.fstat(next_cursor)
                if not stat.S_ISDIR(parent_stat.st_mode) or parent_stat.st_nlink == 0:
                    raise NativeLaunchPlanError(
                        "native log parent directory identity is invalid"
                    )
            except BaseException:
                os.close(next_cursor)
                raise
            previous_cursor = cursor
            previous_owned = cursor_owned
            cursor = next_cursor
            cursor_owned = True
            if previous_owned:
                os.close(previous_cursor)
        try:
            log_fd = os.open(
                parts[-1],
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | os.O_APPEND
                | os.O_NOFOLLOW
                | os.O_CLOEXEC,
                0o600,
                dir_fd=cursor,
            )
        except OSError as exc:
            raise NativeLaunchPlanError(
                "native log path must be fresh and creatable below retained root"
            ) from exc
        try:
            os.fchmod(log_fd, 0o600)
            log_stat = os.fstat(log_fd)
            descriptor_flags = fcntl.fcntl(log_fd, fcntl.F_GETFD)
            log_flags = fcntl.fcntl(log_fd, fcntl.F_GETFL)
        except OSError as exc:
            raise NativeLaunchPlanError("native log FD setup failed") from exc
        if (
            not stat.S_ISREG(log_stat.st_mode)
            or log_stat.st_nlink != 1
            or stat.S_IMODE(log_stat.st_mode) != 0o600
            or descriptor_flags & fcntl.FD_CLOEXEC == 0
            or (log_flags & os.O_ACCMODE) != os.O_WRONLY
            or log_flags & os.O_APPEND == 0
        ):
            raise NativeLaunchPlanError("native log FD is not private append-only")
        _validate_project_local_log(project_root_fd, relative_log, log_stat)
        _validate_project_root_fd(
            project_root_fd, project_root_path, project_root_identity_sha256
        )
        if cursor_owned:
            os.close(cursor)
            cursor_owned = False
        owned = log_fd
        log_fd = -1
        return owned
    finally:
        if log_fd >= 0:
            os.close(log_fd)
        if cursor_owned:
            os.close(cursor)


def _create_bound_native_session_log(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    project_root_fd: int,
) -> int:
    """Create and durably bind one candidate session's retained log FD.

    The claim/session transaction must already be committed.  An open or
    indeterminate transaction cannot make the new file's reservation durable.
    A separate committed CAS consumes the sole log-open attempt before any
    filesystem open.  Any later failure leaves that attempt consumed and keeps
    an O_EXCL file if one was created.  This helper does not arm a launch or
    grant a native monitor owner.
    """
    from . import state

    if not isinstance(conn, sqlite3.Connection) or conn.in_transaction:
        raise NativeLaunchPlanError(
            "native session reservation must be committed before log creation"
        )
    session = state.get_native_session(conn, session_id)
    if (
        session is None
        or session["phase"] != "reserved"
        or session["log_attempted_at"] is not None
        or session["evaluation_domain"] != "isolated_integration"
        or session["owner_kind"] != "unbound"
    ):
        raise NativeLaunchPlanError("native session has no unbound log reservation")
    try:
        conn.execute("BEGIN IMMEDIATE")
        session = state.mark_native_session_log_attempted(conn, session_id)
        conn.commit()
        if conn.in_transaction:
            raise NativeLaunchPlanError("native log attempt commit is unresolved")
    except BaseException:
        conn.rollback()
        raise
    log_fd = _open_project_local_log_fd(
        project_root_fd=project_root_fd,
        project_root_path=session["project_root_path"],
        project_root_identity_sha256=session["project_root_identity_sha256"],
        log_relative_path=session["log_relative_path"],
    )
    try:
        log_stat = os.fstat(log_fd)
        _validate_project_local_log(
            project_root_fd, session["log_relative_path"], log_stat
        )
        conn.execute("BEGIN IMMEDIATE")
        state.bind_native_session_log(
            conn, session_id, log_stat.st_dev, log_stat.st_ino
        )
        conn.commit()
        if conn.in_transaction:
            raise NativeLaunchPlanError("native log binding commit is unresolved")
        _validate_project_local_log(
            project_root_fd, session["log_relative_path"], log_stat
        )
        return log_fd
    except BaseException:
        try:
            conn.rollback()
        finally:
            os.close(log_fd)
        raise


def _validate_project_local_log(
    project_root_fd: int,
    log_relative_path: str,
    log_stat: os.stat_result,
) -> None:
    """Bind a retained log FD to one non-symlink path below root/logs/."""
    parts = log_relative_path.split("/")
    cursor = os.dup(project_root_fd)
    try:
        for component in parts[:-1]:
            try:
                next_cursor = os.open(
                    component,
                    os.O_RDONLY
                    | os.O_DIRECTORY
                    | os.O_NOFOLLOW
                    | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=cursor,
                )
            except OSError as exc:
                raise NativeLaunchPlanError(
                    "native log parent must be a retained non-symlink directory"
                ) from exc
            try:
                directory_stat = os.fstat(next_cursor)
                if not stat.S_ISDIR(directory_stat.st_mode) or directory_stat.st_nlink == 0:
                    raise NativeLaunchPlanError(
                        "native log parent directory identity is invalid"
                    )
            except Exception:
                os.close(next_cursor)
                raise
            os.close(cursor)
            cursor = next_cursor
        try:
            path_stat = os.stat(parts[-1], dir_fd=cursor, follow_symlinks=False)
        except OSError as exc:
            raise NativeLaunchPlanError(
                "native log path cannot be resolved below retained project root"
            ) from exc
        if not stat.S_ISREG(path_stat.st_mode):
            raise NativeLaunchPlanError("native log path must not be a symlink")
        if (path_stat.st_dev, path_stat.st_ino) != (log_stat.st_dev, log_stat.st_ino):
            raise NativeLaunchPlanError(
                "native log FD does not match its project-local path"
            )
        if path_stat.st_nlink != 1 or log_stat.st_nlink != 1:
            raise NativeLaunchPlanError(
                "native log must have exactly one project-local hard link"
            )
    finally:
        os.close(cursor)


def _copy_retained_source_fd(source_fd: int, *, request: bool) -> int:
    if request and sys.platform.startswith("linux"):
        copied = -1
        try:
            copied = os.open(
                f"/proc/self/fd/{source_fd}",
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0),
            )
            os.lseek(copied, 0, os.SEEK_SET)
            return copied
        except Exception:
            if copied >= 0:
                try:
                    os.close(copied)
                except OSError:
                    pass
            raise
    return os.dup(source_fd)


def _pread_exact(fd: int, size: int, where: str) -> bytes:
    chunks: list[bytes] = []
    offset = 0
    try:
        while offset < size:
            chunk = os.pread(fd, min(1024 * 1024, size - offset), offset)
            if not chunk:
                raise NativeLaunchPlanError(f"{where} ended before its retained size")
            chunks.append(chunk)
            offset += len(chunk)
    except OSError as exc:
        raise NativeLaunchPlanError(f"{where} retained read failed") from exc
    return b"".join(chunks)


def _stable_fd_bytes(fd: int, *, maximum: int, where: str) -> bytes:
    """Read one retained regular-file snapshot without changing its offset."""
    try:
        before = os.fstat(fd)
    except OSError as exc:
        raise NativeLaunchPlanError(f"{where} cannot be fstat'ed") from exc
    if not stat.S_ISREG(before.st_mode):
        raise NativeLaunchPlanError(f"{where} must be a retained regular file")
    if before.st_size < 0 or before.st_size > maximum:
        raise NativeLaunchPlanError(f"{where} exceeds its byte bound")
    content = _pread_exact(fd, before.st_size, where)
    try:
        after = os.fstat(fd)
    except OSError as exc:
        raise NativeLaunchPlanError(f"{where} cannot be re-fstat'ed") from exc
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_uid,
        before.st_gid,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_uid,
        after.st_gid,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if identity_before != identity_after or len(content) != before.st_size:
        raise NativeLaunchPlanError(f"{where} identity drifted during retained read")
    return content


def _validate_native_request_frame(frame: bytes) -> bytes:
    if len(frame) < _OUTER_HEADER.size + _WIRE_HEADER.size:
        raise NativeLaunchPlanError("sealed native request frame is truncated")
    (payload_length,) = _OUTER_HEADER.unpack_from(frame)
    if payload_length < _WIRE_HEADER.size:
        raise NativeLaunchPlanError("sealed native request payload is too short")
    if payload_length > _WIRE_HEADER.size + NATIVE_LAUNCH_WIRE_MAX_BODY:
        raise NativeLaunchPlanError("sealed native request payload is too large")
    if len(frame) != _OUTER_HEADER.size + payload_length:
        raise NativeLaunchPlanError("sealed native request frame length drifted")
    (
        magic,
        version,
        channel,
        message_type,
        flags,
        sequence,
        body_length,
    ) = _WIRE_HEADER.unpack_from(frame, _OUTER_HEADER.size)
    if magic != NATIVE_LAUNCH_WIRE_MAGIC:
        raise NativeLaunchPlanError("sealed native request magic drifted")
    if version != NATIVE_LAUNCH_WIRE_VERSION:
        raise NativeLaunchPlanError("sealed native request version drifted")
    if channel != NATIVE_LAUNCH_CHANNEL_REQUEST:
        raise NativeLaunchPlanError("sealed native request channel drifted")
    if message_type != NATIVE_LAUNCH_MESSAGE_REQUEST:
        raise NativeLaunchPlanError("sealed native request message type drifted")
    if (
        flags != NATIVE_LAUNCH_FLAGS_NONE
        or sequence != NATIVE_LAUNCH_REQUEST_SEQUENCE
    ):
        raise NativeLaunchPlanError("sealed native request flags or sequence drifted")
    if body_length == 0 or body_length > NATIVE_LAUNCH_WIRE_MAX_BODY:
        raise NativeLaunchPlanError("sealed native request body size is invalid")
    if body_length != payload_length - _WIRE_HEADER.size:
        raise NativeLaunchPlanError("sealed native request body length drifted")
    return frame[_OUTER_HEADER.size + _WIRE_HEADER.size :]


class NativeLaunchPlan:
    """One-shot owner of retained descriptors for the fixed native entry.

    Construction is intentionally restricted to the module-private factory.
    The factory makes retained copies of every caller-owned descriptor.  The
    sealed request is reopened through its live Linux FD to give the plan an
    independent offset; the other descriptors are duplicated.  The factory
    independently binds the complete request frame and its opaque body to
    explicitly named SHA-256 values, validates all copies, and closes them on
    any partial failure.  The current executor also closes them when it refuses
    the not-yet-implemented backend.
    """

    __slots__ = (
        "profile_id",
        "profile_sha256",
        "project_root_identity_sha256",
        "project_root_path",
        "logical_submitted_argv",
        "launcher_sha256",
        "request_frame_sha256",
        "request_body_sha256",
        "log_relative_path",
        "_owned_fds",
        "_closed",
    )

    def __init__(
        self,
        authority: object,
        *,
        profile_id: str,
        profile_sha256: str,
        project_root_identity_sha256: str,
        project_root_path: str,
        logical_submitted_argv: tuple[str, ...],
        launcher_sha256: str,
        request_frame_sha256: str,
        request_body_sha256: str,
        log_relative_path: str,
        owned_fds: tuple[int, int, int, int, int],
    ) -> None:
        if authority is not _PLAN_AUTHORITY:
            raise NativeLaunchPlanError(
                "native launch plans require scheduler-internal authority"
            )
        self.profile_id = profile_id
        self.profile_sha256 = profile_sha256
        self.project_root_identity_sha256 = project_root_identity_sha256
        self.project_root_path = project_root_path
        self.logical_submitted_argv = logical_submitted_argv
        self.launcher_sha256 = launcher_sha256
        self.request_frame_sha256 = request_frame_sha256
        self.request_body_sha256 = request_body_sha256
        self.log_relative_path = log_relative_path
        self._owned_fds = owned_fds
        self._closed = False

    @property
    def schema(self) -> str:
        return NATIVE_LAUNCH_PLAN_SCHEMA

    @property
    def actual_argv(self) -> tuple[str, str]:
        return NATIVE_ACTUAL_ARGV

    @property
    def actual_env_items(self) -> tuple[tuple[str, str], ...]:
        return NATIVE_ACTUAL_ENV_ITEMS

    @property
    def owned_fds(self) -> tuple[int, int, int, int, int]:
        if self._closed:
            raise NativeLaunchPlanError("native launch plan is already closed")
        return self._owned_fds

    @property
    def launcher_fd(self) -> int:
        return self.owned_fds[0]

    @property
    def request_fd(self) -> int:
        return self.owned_fds[1]

    @property
    def control_fd(self) -> int:
        return self.owned_fds[2]

    @property
    def project_root_fd(self) -> int:
        return self.owned_fds[3]

    @property
    def log_fd(self) -> int:
        return self.owned_fds[4]

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._closed:
            return
        owned = self._owned_fds
        self._owned_fds = (-1, -1, -1, -1, -1)
        self._closed = True
        for fd in owned:
            try:
                os.close(fd)
            except OSError:
                pass

    def validate_live_fds(self) -> None:
        """Revalidate all retained descriptors without executing anything."""
        if self._closed:
            raise NativeLaunchPlanError("native launch plan is already closed")
        if not sys.platform.startswith("linux"):
            raise NativeLaunchUnavailable(
                "native retained-FD launch requires Linux; no fallback is allowed"
            )
        required_fcntl = (
            "F_GETFD",
            "FD_CLOEXEC",
            "F_GETFL",
            "F_GET_SEALS",
            "F_SEAL_SEAL",
            "F_SEAL_SHRINK",
            "F_SEAL_GROW",
            "F_SEAL_WRITE",
        )
        if any(not hasattr(fcntl, name) for name in required_fcntl):
            raise NativeLaunchUnavailable(
                "native retained-FD launch requires Linux sealing constants"
            )
        for fd in self.owned_fds:
            try:
                descriptor_flags = fcntl.fcntl(fd, fcntl.F_GETFD)
            except OSError as exc:
                raise NativeLaunchPlanError("native launch retained FD is not live") from exc
            if descriptor_flags & fcntl.FD_CLOEXEC == 0:
                raise NativeLaunchPlanError(
                    "native launch source descriptors must remain CLOEXEC"
                )

        launcher_digest, launcher_stat = _stable_fd_sha256(
            self.launcher_fd,
            maximum=_MAX_LAUNCHER_BYTES,
            where="native launcher FD",
        )
        if launcher_digest != self.launcher_sha256:
            raise NativeLaunchPlanError("native launcher retained digest drifted")
        if launcher_stat.st_mode & 0o111 == 0:
            raise NativeLaunchPlanError("native launcher retained file is not executable")
        launcher_flags = fcntl.fcntl(self.launcher_fd, fcntl.F_GETFL)
        if (launcher_flags & os.O_ACCMODE) != os.O_RDONLY:
            raise NativeLaunchPlanError("native launcher FD must be read-only")

        required_seals = (
            fcntl.F_SEAL_SEAL
            | fcntl.F_SEAL_SHRINK
            | fcntl.F_SEAL_GROW
            | fcntl.F_SEAL_WRITE
        )
        try:
            seals = fcntl.fcntl(self.request_fd, fcntl.F_GET_SEALS)
        except OSError as exc:
            raise NativeLaunchPlanError(
                "native request FD seals cannot be inspected"
            ) from exc
        if seals & required_seals != required_seals:
            raise NativeLaunchPlanError("native request FD is not fully sealed")
        request_flags = fcntl.fcntl(self.request_fd, fcntl.F_GETFL)
        if (request_flags & os.O_ACCMODE) != os.O_RDONLY:
            raise NativeLaunchPlanError("native request FD must be read-only")
        try:
            request_offset = os.lseek(self.request_fd, 0, os.SEEK_CUR)
        except OSError as exc:
            raise NativeLaunchPlanError("native request FD must be seekable") from exc
        if request_offset != 0:
            raise NativeLaunchPlanError("native request FD offset must be exactly zero")
        request_frame = _stable_fd_bytes(
            self.request_fd,
            maximum=_MAX_REQUEST_BYTES,
            where="sealed native request FD",
        )
        request_body = _validate_native_request_frame(request_frame)
        request_frame_digest = hashlib.sha256(request_frame).hexdigest()
        request_body_digest = hashlib.sha256(request_body).hexdigest()
        if request_frame_digest != self.request_frame_sha256:
            raise NativeLaunchPlanError("sealed native request frame digest drifted")
        if request_body_digest != self.request_body_sha256:
            raise NativeLaunchPlanError("sealed native request body digest drifted")

        try:
            control_stat = os.fstat(self.control_fd)
        except OSError as exc:
            raise NativeLaunchPlanError("native control FD cannot be fstat'ed") from exc
        if not stat.S_ISSOCK(control_stat.st_mode):
            raise NativeLaunchPlanError("native control FD must be a socket")
        control_copy = os.dup(self.control_fd)
        channel: socket.socket | None = None
        try:
            channel = socket.socket(fileno=control_copy)
            if channel.family != socket.AF_UNIX:
                raise NativeLaunchPlanError("native control FD must be AF_UNIX")
            if channel.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) != socket.SOCK_STREAM:
                raise NativeLaunchPlanError(
                    "native control FD must be a SOCK_STREAM socket"
                )
            channel.getpeername()
        except OSError as exc:
            raise NativeLaunchPlanError("native control FD must be connected") from exc
        finally:
            if channel is None:
                try:
                    os.close(control_copy)
                except OSError:
                    pass
            else:
                channel.close()

        _validate_project_root_fd(
            self.project_root_fd,
            self.project_root_path,
            self.project_root_identity_sha256,
        )

        try:
            log_stat = os.fstat(self.log_fd)
        except OSError as exc:
            raise NativeLaunchPlanError("native log FD cannot be fstat'ed") from exc
        if not stat.S_ISREG(log_stat.st_mode):
            raise NativeLaunchPlanError("native log FD must be a regular file")
        if stat.S_IMODE(log_stat.st_mode) != 0o600:
            raise NativeLaunchPlanError("native log FD must have mode 0600")
        log_flags = fcntl.fcntl(self.log_fd, fcntl.F_GETFL)
        if (log_flags & os.O_ACCMODE) != os.O_WRONLY:
            raise NativeLaunchPlanError("native log FD must be write-only")
        if log_flags & os.O_APPEND == 0:
            raise NativeLaunchPlanError("native log FD must use append mode")
        _validate_project_local_log(
            self.project_root_fd,
            self.log_relative_path,
            log_stat,
        )


def _create_native_launch_plan(
    *,
    profile_id: Any,
    profile_sha256: Any,
    project_root_identity_sha256: Any,
    project_root_path: Any,
    logical_submitted_argv: Any,
    launcher_sha256: Any,
    request_frame_sha256: Any,
    request_body_sha256: Any,
    log_relative_path: Any,
    launcher_fd: Any,
    request_fd: Any,
    control_fd: Any,
    project_root_fd: Any,
    log_fd: Any,
) -> NativeLaunchPlan:
    """Duplicate and validate scheduler-owned retained descriptors.

    This private factory is the only supported constructor.  A future
    dispatcher may call it only after cold-config and persisted-state
    re-attestation; no public batch field is routed here today.
    """
    if type(profile_id) is not str or _SAFE_IDENTIFIER_RE.fullmatch(profile_id) is None:
        raise NativeLaunchPlanError("native launch profile id is invalid")
    profile_digest = _digest(profile_sha256, "native launch profile sha256")
    root_digest = _digest(
        project_root_identity_sha256,
        "native launch project-root identity sha256",
    )
    launcher_digest = _digest(launcher_sha256, "native launcher sha256")
    request_frame_digest = _digest(
        request_frame_sha256,
        "native request frame sha256",
    )
    request_body_digest = _digest(
        request_body_sha256,
        "native request body sha256",
    )
    if (
        type(project_root_path) is not str
        or project_root_path == "/"
        or not os.path.isabs(project_root_path)
        or os.path.normpath(project_root_path) != project_root_path
        or os.path.realpath(project_root_path) != project_root_path
    ):
        raise NativeLaunchPlanError(
            "native launch project-root path must be canonical, non-root, and absolute"
        )
    logical_argv = _logical_argv(logical_submitted_argv)
    relative_log = _log_relative_path(log_relative_path)
    source_fds = (
        _fd(launcher_fd, "launcher_fd"),
        _fd(request_fd, "request_fd"),
        _fd(control_fd, "control_fd"),
        _fd(project_root_fd, "project_root_fd"),
        _fd(log_fd, "log_fd"),
    )
    if len(set(source_fds)) != len(source_fds):
        raise NativeLaunchPlanError("native launch source descriptors must be distinct")

    duplicates: list[int] = []
    plan: NativeLaunchPlan | None = None
    try:
        for index, source_fd in enumerate(source_fds):
            duplicates.append(
                _copy_retained_source_fd(source_fd, request=index == 1)
            )
        plan = NativeLaunchPlan(
            _PLAN_AUTHORITY,
            profile_id=profile_id,
            profile_sha256=profile_digest,
            project_root_identity_sha256=root_digest,
            project_root_path=project_root_path,
            logical_submitted_argv=logical_argv,
            launcher_sha256=launcher_digest,
            request_frame_sha256=request_frame_digest,
            request_body_sha256=request_body_digest,
            log_relative_path=relative_log,
            owned_fds=tuple(duplicates),  # type: ignore[arg-type]
        )
        plan.validate_live_fds()
        return plan
    except Exception:
        if plan is not None:
            plan.close()
        else:
            for duplicate in duplicates:
                try:
                    os.close(duplicate)
                except OSError:
                    pass
        raise
