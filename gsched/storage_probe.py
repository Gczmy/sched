"""Bounded read-only Linux filesystem/user-quota helper; no scheduler state.

Run only by the compute daemon with a deadline, never by gateway queries.
Quota ABI: Linux include/uapi/linux/quota.h (Q_GETQUOTA, if_dqblk).
"""
from __future__ import annotations

import ctypes
import errno
import json
import os
from pathlib import Path
import re
import sys
import time

MAX_PATHS = 128


class Quota(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint64) for name in (
        "block_hard", "block_soft", "space_used", "inode_hard", "inode_soft",
        "inodes_used", "block_grace", "inode_grace")] + [("valid", ctypes.c_uint32)]


def decode_quota(value):
    # Space usage is bytes; block limits are 1024-byte quota blocks. Soft
    # limits are conservative ceilings, even when grace has not expired.
    result = {"source": "linux_quotactl_user", "scope": "current_uid_only",
              "uid": os.getuid(), "other_scopes": "group_project_remote_not_verified"}
    for kind, valid, used, limits, scale in (
        ("bytes", 3, value.space_used, (value.block_hard, value.block_soft), 1024),
        ("inodes", 12, value.inodes_used, (value.inode_hard, value.inode_soft), 1),
    ):
        known = value.valid & valid == valid
        limit = min((number * scale for number in limits if number), default=None) if known else None
        result[kind] = {"known": known, "used": used if known else None,
                        "limit": limit, "headroom": max(0, limit - used) if limit is not None else None,
                        "status": "bounded" if limit is not None else "no_user_limit" if known else "unknown"}
    return result


def mount_source(path):
    with Path("/proc/self/mountinfo").open() as stream:
        text = stream.read(1024 * 1024 + 1)
    if len(text.encode()) > 1024 * 1024:
        return None
    found = []
    for line in text.splitlines():
        before, _, after = line.partition(" - ")
        columns, filesystem = before.split(), after.split()
        if len(columns) < 6 or len(filesystem) < 2:
            continue
        unescape = lambda s: re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), s)
        mount = unescape(columns[4])
        if path == mount or path.startswith(mount.rstrip("/") + "/"):
            found.append((len(mount), filesystem[0], unescape(filesystem[1])))
    return max(found, default=None)


def quota(path):
    unknown = {"source": "linux_quotactl_user", "scope": "current_uid_only", "uid": os.getuid(),
               "other_scopes": "group_project_remote_not_verified",
               "bytes": {"known": False, "status": "unknown", "headroom": None},
               "inodes": {"known": False, "status": "unknown", "headroom": None}}
    try:
        mount = mount_source(path)
        if (mount is None or mount[1] not in {"ext2", "ext3", "ext4", "xfs"}
                or not mount[2].startswith("/dev/") or ctypes.sizeof(Quota) != 72):
            return {**unknown, "reason": "unsupported_or_remote_filesystem"}
        call = ctypes.CDLL(None, use_errno=True).quotactl
        call.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_void_p]
        call.restype = ctypes.c_int
        value = Quota()
        if call(ctypes.c_int(0x800007 << 8), os.fsencode(mount[2]), os.getuid(), ctypes.byref(value)) != 0:
            return {**unknown, "reason": "quota_unavailable", "errno": ctypes.get_errno()}
        return decode_quota(value)
    except (OSError, AttributeError, ValueError):
        return {**unknown, "reason": "quota_unavailable"}


def sample(paths):
    observed_at = time.time()
    output, quota_cache = {}, {}
    for requested in paths:
        try:
            resolved = os.path.realpath(requested)
            path = resolved
            for _ in range(256):
                try:
                    info = os.stat(path)
                    break
                except FileNotFoundError:
                    parent = os.path.dirname(path)
                    if parent == path:
                        raise
                    path = parent
            else:
                raise ValueError("path depth bound")
            fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NONBLOCK)
            try:
                info, fs = os.fstat(fd), os.fstatvfs(fd)
            finally:
                os.close(fd)
            key = str(info.st_dev)
            if key not in quota_cache:
                quota_cache[key] = quota(path)
            output[requested] = {"filesystem_id": key, "resolved_path": resolved,
                "sample_path": path, "bytes_available": fs.f_bavail * fs.f_frsize,
                "inodes_available": fs.f_favail if fs.f_files > 0 else None,
                "readonly": bool(fs.f_flag & getattr(os, "ST_RDONLY", 1)), "quota": quota_cache[key]}
        except (OSError, ValueError) as error:
            output[requested] = {"filesystem_id": None, "reason": "filesystem_unavailable",
                                 "errno": error.errno if isinstance(error, OSError) else errno.EINVAL}
    return {"schema_version": 1, "observed_at": observed_at, "paths": output}


def main():
    if sys.platform != "linux":
        raise SystemExit("storage helper requires Linux compute context")
    raw = sys.stdin.read(64 * 1024 + 1)
    paths = json.loads(raw)
    if (len(raw.encode()) > 64 * 1024 or not isinstance(paths, list) or len(paths) > MAX_PATHS
            or any(not isinstance(p, str) or not os.path.isabs(p) or "\0" in p or len(p) > 4096 for p in paths)):
        raise SystemExit("invalid bounded storage probe input")
    print(json.dumps(sample(paths), allow_nan=False))


if __name__ == "__main__":
    main()
