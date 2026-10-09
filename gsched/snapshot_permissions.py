"""Fenced, bounded preparation of legacy directory permissions for snapshots."""
from __future__ import annotations

from contextlib import closing
import hashlib
import os
from pathlib import Path
import secrets
import sqlite3
import stat
import time

from . import maintenance, snapshot, snapshot_facts as facts, snapshot_management, state

FORMAT = "sched-upgrade-snapshot-permissions/v1"


def _entry(path):
    info = os.lstat(path)
    if info.st_uid != os.getuid() or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
        raise facts.SnapshotConflict("permission preparation requires owned nonsymlink files/directories")
    return {"dev": info.st_dev, "ino": info.st_ino, "mode": stat.S_IMODE(info.st_mode),
            "uid": info.st_uid, "bytes": info.st_size, "mtime_ns": info.st_mtime_ns,
            "ctime_ns": info.st_ctime_ns, "directory": stat.S_ISDIR(info.st_mode)}


def _scan(*, deadline=None):
    root = state.host_dir()
    entries, total = {}, 0
    deadline = deadline if deadline is not None else time.monotonic() + 30

    def failed(error):
        raise facts.SnapshotConflict("permission inventory is unreadable") from error

    for parent, names, files in os.walk(root, followlinks=False, onerror=failed):
        if len(entries) + len(names) + len(files) > snapshot.MAX_FILES:
            raise facts.SnapshotConflict("permission inventory entry bound exceeded")
        for path in [parent, *(os.path.join(parent, n) for n in sorted(names + files))]:
            relative = os.path.relpath(path, root)
            if relative in snapshot.DATABASE_FILES or relative in entries:
                continue
            entry = _entry(path)
            entries[relative] = entry
            if not entry["directory"]:
                if entry["bytes"] > snapshot.MAX_FILE_BYTES:
                    raise facts.SnapshotConflict("permission inventory file byte bound exceeded")
                total += entry["bytes"]
            if len(entries) > snapshot.MAX_FILES or total > snapshot.MAX_TOTAL_BYTES or time.monotonic() > deadline:
                raise facts.SnapshotConflict("permission inventory entry/byte/time bound exceeded")
    return entries


def _database(*, deadline):
    snapshot._source_guard()
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise facts.SnapshotConflict("permission preparation time bound exceeded")
    with state._read_only_database(state.db_path(), copy_timeout=remaining) as (path, immutable):
        uri = Path(path).absolute().as_uri() + "?mode=ro" + ("&immutable=1" if immutable else "")
        with closing(sqlite3.connect(uri, uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            snapshot._quiescent(conn)
            result = facts.database_facts(conn, deadline=deadline)
    return {"instance_id": result["instance_id"], "schema": result["database_schema"],
            "sha256": hashlib.sha256(facts._encoded(result)).hexdigest()}


def _plan(*, deadline):
    if snapshot.status()["maintenance_open"]:
        raise facts.SnapshotConflict("close the existing upgrade window before permission preparation")
    binding = snapshot._binding()
    database = _database(deadline=deadline)
    entries = _scan(deadline=deadline)
    targets = {path: entry for path, entry in entries.items() if entry["directory"] and entry["mode"] & 0o077}
    value = {"binding": binding, "database": database,
             "config": snapshot._file_fact(binding["config_path"]), "entries": entries}
    encoded = facts._encoded(value)
    if len(encoded) > snapshot.MAX_MANIFEST_BYTES:
        raise facts.SnapshotConflict("permission plan metadata byte bound exceeded")
    result = {"schema_version": 1, "contract": FORMAT, "plan_sha256": hashlib.sha256(encoded).hexdigest(),
              "instance_id": database["instance_id"], "database_schema": database["schema"],
              "entries": len(entries), "directory_count": sum(e["directory"] for e in entries.values()),
              "repair_count": len(targets), "repairs": [{"path": path, "old_mode": entry["mode"],
                  "new_mode": entry["mode"] & ~0o077} for path, entry in sorted(targets.items())],
              "rollback_authorized": False, "daemon_started": False, "database_migrated": False}
    return value, targets, result


def _directory_fd(root_fd, relative, entries):
    fd = os.dup(root_fd)
    current = ""
    try:
        for component in ([] if relative == "." else relative.split(os.sep)):
            current = os.path.join(current, component)
            new = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
            os.close(fd)
            fd = new
            info = os.fstat(fd)
            original = entries[current]
            if (info.st_dev, info.st_ino, info.st_uid) != (original["dev"], original["ino"], original["uid"]):
                raise facts.SnapshotConflict("permission directory binding changed")
        return fd
    except BaseException:
        os.close(fd)
        raise


def prepare(*, dry_run=False, writers_quiesced=False, expect_plan=None):
    if dry_run:
        # Existing shared fence only; preview never initializes control files.
        with snapshot_management._reader():
            _, _, result = _plan(deadline=time.monotonic() + 30)
        return {**result, "dry_run": True, "permissions_changed": False}
    if not writers_quiesced or not isinstance(expect_plan, str) or len(expect_plan) != 64:
        raise facts.SnapshotConflict("permission preparation requires --writers-quiesced and original --expect-plan")
    with maintenance.gate(exclusive=True):
        deadline = time.monotonic() + 30
        value, targets, result = _plan(deadline=deadline)
        if expect_plan != result["plan_sha256"]:
            raise facts.SnapshotConflict("permission plan changed; preview again before applying")
        identifier = secrets.token_hex(16)
        audit_path = os.path.join(maintenance.directory(), "permissions-" + identifier + ".json")
        audit = {"format": FORMAT, "preparation_id": identifier, "phase": "preparing",
                 "binding": value["binding"], "plan_sha256": expect_plan, "repairs": result["repairs"], "changed": []}
        snapshot._publish(audit_path, audit)
        root_fd = None
        expected = dict(value["entries"])
        try:
            root_fd = os.open(state.host_dir(), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            root = os.fstat(root_fd)
            original_root = value["entries"]["."]
            if (root.st_dev, root.st_ino, root.st_uid, stat.S_IMODE(root.st_mode), root.st_ctime_ns) != (
                    original_root["dev"], original_root["ino"], original_root["uid"],
                    original_root["mode"], original_root["ctime_ns"]):
                raise facts.SnapshotConflict("permission root binding changed")
            for relative, original in sorted(targets.items()):
                if time.monotonic() > deadline:
                    raise facts.SnapshotConflict("permission preparation time bound exceeded; tightening retained")
                fd = _directory_fd(root_fd, relative, value["entries"])
                try:
                    info = os.fstat(fd)
                    if (stat.S_IMODE(info.st_mode), info.st_ctime_ns) != (original["mode"], original["ctime_ns"]):
                        raise facts.SnapshotConflict("permission directory metadata changed")
                    os.fchmod(fd, original["mode"] & ~0o077)
                    info = os.fstat(fd)
                    expected[relative] = {**original, "mode": stat.S_IMODE(info.st_mode), "ctime_ns": info.st_ctime_ns}
                    audit["changed"].append(relative)
                finally:
                    os.close(fd)
            if (_scan(deadline=deadline) != expected or snapshot._binding() != value["binding"]
                    or snapshot._file_fact(value["binding"]["config_path"]) != value["config"]
                    or _database(deadline=deadline) != value["database"]):
                raise facts.SnapshotConflict("state drift during permission preparation; tightened permissions retained")
            audit["phase"] = "completed"
            snapshot._publish(audit_path, audit)
        except BaseException as error:
            audit.update(phase="incomplete", error=str(error)[:1000])
            snapshot._publish(audit_path, audit)
            raise
        finally:
            if root_fd is not None:
                os.close(root_fd)
        return {**result, "dry_run": False, "permissions_changed": bool(targets),
                "preparation_id": identifier, "phase": "completed"}
