"""Linux observations and sealed requests for the isolated Step 5D owner.

Ported from the reviewed main fixture at fa1cd85. No fixture import, child
creation, data access, scheduler deployment or authority publication occurs.
"""
from __future__ import annotations

import ctypes
import errno
import fcntl
import hashlib
import os
import re
import select
import sys
from pathlib import Path
from typing import Any

from . import native_step5d_protocol as protocol

_BOOT_ID = re.compile(
    rb"^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-"
    rb"[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\n?$"
)


class Step5DLinuxError(RuntimeError):
    """An exact Linux observation could not be established."""


def _require_linux() -> None:
    if not sys.platform.startswith("linux"):
        raise Step5DLinuxError("Linux is required; no fallback is allowed")

def _libc_has_fd_constructor(name: str) -> bool:
    if not sys.platform.startswith("linux"):
        return False
    try:
        function = getattr(ctypes.CDLL(None, use_errno=True), name)
    except (AttributeError, OSError):
        return False
    return callable(function)


def _call_libc_fd_constructor(
    name: str,
    *,
    argtypes: tuple[Any, ...],
    arguments: tuple[Any, ...],
) -> int:
    """Call an exact libc wrapper for one Linux FD-creating syscall."""

    try:
        function = getattr(ctypes.CDLL(None, use_errno=True), name)
    except (AttributeError, OSError) as exc:
        raise Step5DLinuxError(
            f"required Linux {name} Python/libc wrapper is unavailable"
        ) from exc
    function.argtypes = list(argtypes)
    function.restype = ctypes.c_int
    ctypes.set_errno(0)
    descriptor = int(function(*arguments))
    if descriptor < 0:
        error_number = ctypes.get_errno() or errno.EIO
        raise OSError(error_number, os.strerror(error_number))
    return descriptor


def _pidfd_open(pid: int, flags: int) -> int:
    python_api = getattr(os, "pidfd_open", None)
    if callable(python_api):
        return int(python_api(pid, flags))
    return _call_libc_fd_constructor(
        "pidfd_open",
        argtypes=(ctypes.c_int, ctypes.c_uint),
        arguments=(pid, flags),
    )


def _memfd_create(name: str, flags: int) -> int:
    python_api = getattr(os, "memfd_create", None)
    if callable(python_api):
        return int(python_api(name, flags))
    encoded_name = name.encode("ascii")
    if not encoded_name or b"\x00" in encoded_name:
        raise Step5DLinuxError("memfd name must be nonempty NUL-free ASCII")
    return _call_libc_fd_constructor(
        "memfd_create",
        argtypes=(ctypes.c_char_p, ctypes.c_uint),
        arguments=(encoded_name, flags),
    )



def _read_bounded(path: str, *, maximum: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(16384, maximum + 1 - total))
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)
            total += len(chunk)
            if total > maximum:
                raise Step5DLinuxError(f"bounded proc read exceeded {maximum}: {path}")
    finally:
        os.close(descriptor)


def _canonical_unsigned(token: str) -> int:
    if not token or not token.isascii() or not token.isdecimal():
        raise Step5DLinuxError("proc unsigned token is malformed")
    if len(token) > 1 and token.startswith("0"):
        raise Step5DLinuxError("proc unsigned token has a leading zero")
    return int(token, 10)


def _canonical_unsigned_bytes(token: bytes) -> int:
    if (
        not token
        or any(byte < ord("0") or byte > ord("9") for byte in token)
        or (len(token) > 1 and token.startswith(b"0"))
    ):
        raise Step5DLinuxError("proc unsigned token is malformed")
    return int(token, 10)


def _parse_stat(raw: bytes, *, expected_pid: int) -> tuple[int, int, int, int]:
    effective = raw[:-1] if raw.endswith(b"\n") else raw
    pivot = effective.rfind(b") ")
    prefix = f"{expected_pid} (".encode("ascii")
    if pivot < len(prefix) or not effective.startswith(prefix):
        raise Step5DLinuxError("proc stat pid/comm prefix is ambiguous")
    suffix = effective[pivot + 2 :].split(b" ")
    if len(suffix) <= 19 or any(token == b"" for token in suffix):
        raise Step5DLinuxError("proc stat suffix is truncated")
    ppid = _canonical_unsigned_bytes(suffix[1])
    process_group_id = _canonical_unsigned_bytes(suffix[2])
    start_ticks = _canonical_unsigned_bytes(suffix[19])
    if ppid <= 0 or process_group_id <= 0 or start_ticks <= 0:
        raise Step5DLinuxError("proc stat identity values must be positive")
    return expected_pid, ppid, process_group_id, start_ticks


def _parse_cgroup(raw: bytes) -> list[dict[str, Any]]:
    if b"\x00" in raw or b"\r" in raw:
        raise Step5DLinuxError("proc cgroup contains forbidden bytes")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise Step5DLinuxError("proc cgroup is not UTF-8") from exc
    if text.endswith("\n"):
        text = text[:-1]
    if not text or "\n\n" in text:
        raise Step5DLinuxError("proc cgroup has an empty record")
    records: list[dict[str, Any]] = []
    for line in text.split("\n"):
        pieces = line.split(":", 2)
        if len(pieces) != 3:
            raise Step5DLinuxError("proc cgroup record is malformed")
        hierarchy = _canonical_unsigned(pieces[0])
        raw_controllers = pieces[1]
        if raw_controllers:
            controllers = raw_controllers.split(",")
            if any(not value for value in controllers) or len(set(controllers)) != len(
                controllers
            ):
                raise Step5DLinuxError("proc cgroup controllers are malformed")
            for controller in controllers:
                controller.encode("utf-8")
            controllers.sort(key=lambda value: value.encode("utf-8"))
        else:
            controllers = []
        path = pieces[2]
        if not path.startswith("/"):
            raise Step5DLinuxError("proc cgroup path is not absolute")
        records.append(
            {
                "hierarchy_id": hierarchy,
                "controllers": controllers,
                "path": path,
            }
        )
    encoded_records = [protocol.canonical_json_bytes(record) for record in records]
    if len(encoded_records) != len(set(encoded_records)):
        raise Step5DLinuxError("proc cgroup contains a duplicate canonical record")
    records.sort(
        key=lambda record: (
            record["hierarchy_id"],
            tuple(value.encode("utf-8") for value in record["controllers"]),
            record["path"].encode("utf-8"),
        )
    )
    return records


def _pidfd_is_live(pidfd: int) -> bool:
    poller = select.poll()
    poller.register(pidfd, select.POLLIN | select.POLLERR | select.POLLHUP)
    return poller.poll(0) == []


def observe_process_identity(pid: int) -> dict[str, Any]:
    """Construct the inherited six-field process identity under one pidfd."""

    _require_linux()
    if type(pid) is not int or pid <= 0:
        raise Step5DLinuxError("process identity pid must be positive")
    pidfd = _pidfd_open(pid, 0)
    try:
        first = _parse_stat(
            _read_bounded(f"/proc/{pid}/stat", maximum=1 << 20),
            expected_pid=pid,
        )
        boot = _read_bounded("/proc/sys/kernel/random/boot_id", maximum=64)
        if _BOOT_ID.fullmatch(boot) is None:
            raise Step5DLinuxError("boot id token is malformed")
        boot_token = boot.rstrip(b"\n").lower()
        cgroup_records = _parse_cgroup(
            _read_bounded(f"/proc/{pid}/cgroup", maximum=1 << 20)
        )
        second = _parse_stat(
            _read_bounded(f"/proc/{pid}/stat", maximum=1 << 20),
            expected_pid=pid,
        )
        if first != second or not _pidfd_is_live(pidfd):
            raise Step5DLinuxError("process identity bracket drifted or exited")
        _pid, parent_pid, process_group_id, start_ticks = first
        return {
            "boot_id_sha256": hashlib.sha256(boot_token).hexdigest(),
            "pid": pid,
            "start_ticks": start_ticks,
            "parent_pid": parent_pid,
            "process_group_id": process_group_id,
            "cgroup_identity_sha256": hashlib.sha256(
                protocol.canonical_json_bytes(cgroup_records)
            ).hexdigest(),
        }
    finally:
        os.close(pidfd)


def _project_root_expectation(root_fd: int, root_path: Path, *, deployment) -> dict[str, Any]:
    from .native_deployment import require_deployment
    require_deployment(deployment)
    canonical = root_path.resolve(strict=True)
    if canonical != root_path or root_path.is_symlink():
        raise Step5DLinuxError("project root must already be canonical and non-symlink")
    status = os.fstat(root_fd)
    if not stat_is_directory(status.st_mode) or status.st_nlink <= 0:
        raise Step5DLinuxError("project root descriptor is not one linked directory")
    return {
        "project": deployment.project,
        "canonical_absolute_path": str(canonical),
        "st_dev": status.st_dev,
        "st_ino": status.st_ino,
        "st_mode": status.st_mode,
        "st_uid": status.st_uid,
        "st_gid": status.st_gid,
    }


def stat_is_directory(mode: int) -> bool:
    return (mode & 0o170000) == 0o040000



def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise Step5DLinuxError("short fixture descriptor write")
        offset += written


def _sealed_readonly_request(frame: bytes) -> tuple[int, list[int]]:
    mfd_cloexec = getattr(os, "MFD_CLOEXEC", 0x0001)
    mfd_allow_sealing = getattr(os, "MFD_ALLOW_SEALING", 0x0002)
    f_add_seals = getattr(fcntl, "F_ADD_SEALS", 1024 + 9)
    f_seal_seal = getattr(fcntl, "F_SEAL_SEAL", 0x0001)
    f_seal_shrink = getattr(fcntl, "F_SEAL_SHRINK", 0x0002)
    f_seal_grow = getattr(fcntl, "F_SEAL_GROW", 0x0004)
    f_seal_write = getattr(fcntl, "F_SEAL_WRITE", 0x0008)
    writable = -1
    readonly = -1
    try:
        writable = _memfd_create(
            "m2b-step5d-request", mfd_cloexec | mfd_allow_sealing
        )
        _write_all(writable, frame)
        required = f_seal_write | f_seal_grow | f_seal_shrink | f_seal_seal
        fcntl.fcntl(writable, f_add_seals, required)
        readonly = os.open(
            f"/proc/self/fd/{writable}", os.O_RDONLY | os.O_CLOEXEC
        )
        os.lseek(readonly, 0, os.SEEK_SET)
        return readonly, [writable, readonly]
    except BaseException:
        for descriptor in (readonly, writable):
            if descriptor >= 0:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
        raise
