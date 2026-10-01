"""Passive checks of administrator files; never reserve or launch an attempt."""
from __future__ import annotations

import errno
import hashlib
import os
import stat
import sys

from ..execution_policy import MAX_EXECUTABLE_BYTES, project_roots, validate_backends


def _executable(profile):
    descriptor = os.open(profile["executable"], os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            return "executable_not_regular"
        if before.st_size > MAX_EXECUTABLE_BYTES:
            return "executable_too_large"
        digest = hashlib.sha256()
        header = b""
        remaining = MAX_EXECUTABLE_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            header = (header + chunk[:4])[:4]
            digest.update(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        if not remaining:
            return "executable_too_large"
        if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            return "executable_changed"
        if digest.hexdigest() != profile["sha256"]:
            return "executable_digest_mismatch"
        # The actual launcher executes a sealed copy, whose mode is independent
        # of the original file's executable bits. Preflight must match that rule.
        if header != b"\x7fELF":
            return "executable_not_elf"
        return None
    finally:
        os.close(descriptor)


def _root(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        if not os.access(".", os.X_OK, dir_fd=descriptor, effective_ids=True):
            return "project_root_unsearchable"
        return None
    finally:
        os.close(descriptor)


def _probe(function, subject):
    try:
        return function()
    except OSError as error:
        suffix = {errno.ENOENT: "missing", errno.ELOOP: "symlink", errno.ENOTDIR: "not_directory",
                  errno.EACCES: "unreadable", errno.EPERM: "unreadable"}.get(error.errno, "io_error")
        return subject + "_" + suffix
    except Exception:
        return subject + "_probe_failed"


def deployment_checks(cfg):
    profiles = validate_backends(cfg)
    roots = project_roots(cfg)
    checks = []
    for backend_id, profile in sorted(profiles.items()):
        probes = [("execution_executable", None, "executable", lambda p=profile: _executable(p))]
        probes += [("execution_project_root", project, "project_root", lambda p=project: _root(roots[p]))
                   for project in sorted(profile["projects"])]
        for check_id, project, subject, function in probes:
            performed = sys.platform == "linux"
            reason = _probe(function, subject) if performed else "non_linux"
            checks.append({"id": check_id, "subject": backend_id, "project": project,
                           "performed": performed, "item": check_id, "reason": reason,
                           "detail": reason or "administrator binding preflight passed",
                           "level": "fail" if reason else "ok"})
    return checks
