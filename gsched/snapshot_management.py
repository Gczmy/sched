"""Bounded passive catalog and fenced retention of closed snapshot payloads.

Closing/audit records and manifests are retained. No current database, config,
request, execution or scope is read for authority, rewritten, or removed.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import math
import os
import re
import stat
import time

from . import maintenance, snapshot, snapshot_facts as facts, state

FORMAT = "sched-upgrade-snapshot-management/v1"
MAX_ENTRIES = 10000
MAX_BYTES = 1024 * 1024 * 1024
MAX_SECONDS = 30
KEEP_FILES = {"manifest.json", "rollback.json"}


def _base(**values):
    return {"schema_version": 1, "contract": FORMAT, "rollback_authorized": False,
            "daemon_started": False, **values}


def _journal(identifier):
    return os.path.join(maintenance.directory(), "prune-" + snapshot._identifier(identifier) + ".json")


@contextmanager
def _reader():
    """Shared existing fence only: a query never initializes control/state."""
    try:
        import fcntl
    except ImportError as error:
        raise state.StateError("snapshot catalog requires POSIX flock") from error
    directory = maintenance.directory()
    if not os.path.lexists(directory):
        yield False
        return
    snapshot._stat(directory, directory=True, private=True)
    path = os.path.join(directory, "gate.lock")
    before = snapshot._stat(path, private=True)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if snapshot._signature(os.fstat(fd)) != snapshot._signature(before):
            raise facts.SnapshotConflict("snapshot catalog fence identity changed")
        deadline = time.monotonic() + maintenance.LOCK_TIMEOUT
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise facts.SnapshotConflict("snapshot catalog fence busy")
                time.sleep(.02)
        yield True
    finally:
        os.close(fd)


def _ids():
    directory = maintenance.directory()
    points = os.path.join(directory, "points")
    found, count = set(), 0
    deadline = time.monotonic() + MAX_SECONDS
    for path, point_entries in ((directory, False), (points, True)):
        if not os.path.lexists(path):
            continue
        snapshot._stat(path, directory=True, private=True)
        with os.scandir(path) as entries:
            for entry in entries:
                count += 1
                if count > MAX_ENTRIES or time.monotonic() > deadline:
                    raise facts.SnapshotConflict("snapshot catalog entry/time bound exceeded")
                if point_entries:
                    found.add(snapshot._identifier(entry.name))
                    snapshot._stat(entry.path, directory=True, private=True)
                else:
                    match = re.fullmatch(r"(?:closed|prune)-([0-9a-f]{32})\.json", entry.name)
                    if match:
                        snapshot._stat(entry.path, private=True)
                        found.add(match[1])
    return sorted(found)


def _closed(identifier):
    path = os.path.join(maintenance.directory(), "closed-" + identifier + ".json")
    try:
        record = snapshot._json(path)
    except FileNotFoundError:
        return None
    if (record.get("format") != snapshot.FORMAT or record.get("snapshot_id") != identifier
            or record.get("phase") != "closed" or type(record.get("binding")) is not dict):
        raise facts.SnapshotConflict("snapshot close record binding invalid")
    return record


def _epoch(value):
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def _summary(identifier):
    point = snapshot._point(identifier)
    closed = _closed(identifier)
    pruning = snapshot._json(_journal(identifier)) if os.path.lexists(_journal(identifier)) else None
    if pruning is not None:
        _validate_journal(identifier, pruning)
    manifest_path = os.path.join(point, "manifest.json")
    manifest = snapshot._json(manifest_path) if os.path.lexists(manifest_path) else None
    if manifest is not None and (manifest.get("format") != snapshot.FORMAT or manifest.get("snapshot_id") != identifier):
        raise facts.SnapshotConflict("snapshot catalog manifest binding invalid")
    if manifest is not None and (type(manifest.get("database_facts")) is not dict
            or type(manifest.get("created_at")) is not str or len(manifest["created_at"]) > 64):
        raise facts.SnapshotConflict("snapshot catalog summary fields invalid")
    phase = pruning["phase"] if pruning else "closed" if closed else "incomplete"
    window = snapshot.status()
    if window.get("snapshot_id") == identifier:
        phase = window.get("phase", "unknown")
    complete = bool(closed and manifest and closed.get("manifest_sha256") ==
                    snapshot._file_fact(manifest_path, private=True)["sha256"])
    epoch = closed.get("closed_at_epoch") if closed else None
    return {"snapshot_id": identifier, "phase": phase,
            "created_at": manifest.get("created_at") if manifest else None,
            "closed_at": closed.get("closed_at") if closed else None,
            "closed_at_epoch": epoch if _epoch(epoch) else None,
            "instance_id": (manifest.get("database_facts") or {}).get("instance_id") if manifest else None,
            "database_schema": (manifest.get("database_facts") or {}).get("database_schema") if manifest else None,
            "complete_manifest_recorded": complete, "rollback_authorized": False}


def catalog(*, limit=20, cursor=None):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise facts.SnapshotConflict("snapshot catalog limit must be 1..100")
    if cursor is not None:
        snapshot._identifier(cursor)
    with _reader() as present:
        identifiers = [i for i in _ids() if cursor is None or i > cursor] if present else []
        rows = [_summary(i) for i in identifiers[:limit]]
        truncated = len(identifiers) > limit
        result = _base(effect="none", query="snapshot_list", snapshots=rows, truncated=truncated,
                     next_cursor=rows[-1]["snapshot_id"] if truncated else None,
                     pagination="live_keyset", maintenance_open=snapshot.status()["maintenance_open"])
        if len(facts._encoded(result)) > 4 * 1024 * 1024:
            raise facts.SnapshotConflict("snapshot catalog output byte bound exceeded")
        return result


def _inventory(point):
    """Freeze the exact private copy tree; reject links/unexpected payloads."""
    directories, files, total = {}, {}, 0
    deadline = time.monotonic() + MAX_SECONDS
    def failure(error):
        raise facts.SnapshotConflict("snapshot prune tree unreadable") from error
    for parent, names, leaves in os.walk(point, followlinks=False, onerror=failure):
        if len(directories) + len(files) + len(names) + len(leaves) > MAX_ENTRIES or time.monotonic() > deadline:
            raise facts.SnapshotConflict("snapshot prune entry/time bound exceeded")
        info = snapshot._stat(parent, directory=True, private=True)
        relative = os.path.relpath(parent, point)
        directories[relative] = [info.st_dev, info.st_ino]
        for name in names:
            snapshot._stat(os.path.join(parent, name), directory=True, private=True)
        for name in leaves:
            path = os.path.join(parent, name)
            info = snapshot._stat(path, private=True)
            if info.st_nlink != 1:
                raise facts.SnapshotConflict("snapshot prune refuses multiply linked files")
            relative = os.path.relpath(path, point)
            if relative not in KEEP_FILES and not (relative in {"database.db", "config.json"}
                    or relative.startswith("files" + os.sep)
                    or relative in {os.path.join("before-rollback", n) for n in snapshot.DATABASE_FILES}):
                raise facts.SnapshotConflict("snapshot prune contains unexpected file")
            value = snapshot._file_fact(path, private=True)
            if snapshot._signature(snapshot._stat(path, private=True)) != snapshot._signature(info):
                raise facts.SnapshotConflict("snapshot prune file changed while binding inventory")
            files[relative] = {**value, "dev": info.st_dev, "ino": info.st_ino,
                               "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns}
            total += value["bytes"]
            if total > MAX_BYTES or time.monotonic() > deadline:
                raise facts.SnapshotConflict("snapshot prune byte/time bound exceeded")
    return {"files": files, "directories": directories}


def _options(retention_days, keep_last, as_of):
    if type(retention_days) is not int or not 0 <= retention_days <= 36500:
        raise facts.SnapshotConflict("retention-days must be 0..36500")
    if type(keep_last) is not int or not 0 <= keep_last <= 100:
        raise facts.SnapshotConflict("keep-last must be 0..100")
    if as_of is None:
        as_of = int(time.time())
    if type(as_of) is not int or not 1 <= as_of <= time.time():
        raise facts.SnapshotConflict("as-of must be a positive nonfuture Unix timestamp")
    return {"retention_days": retention_days, "keep_last": keep_last, "as_of": as_of}


def _plan(identifier, options):
    rows = [_summary(i) for i in _ids()]
    item = next((row for row in rows if row["snapshot_id"] == identifier), None)
    reasons = []
    if snapshot.status()["maintenance_open"]:
        reasons.append("maintenance_open")
    if item is None or item["phase"] != "closed" or not item["complete_manifest_recorded"]:
        reasons.append("complete_closed_point_required")
    if item and item["closed_at_epoch"] is None:
        reasons.append("closed_time_not_recorded")
    cutoff = options["as_of"] - options["retention_days"] * 86400
    if item and item["closed_at_epoch"] is not None and item["closed_at_epoch"] > cutoff:
        reasons.append("retention_period_not_elapsed")
    newest = sorted((r for r in rows if r["phase"] == "closed" and r["closed_at_epoch"] is not None),
                    key=lambda r: (r["closed_at_epoch"], r["snapshot_id"]), reverse=True)
    if any(r["snapshot_id"] == identifier for r in newest[:options["keep_last"]]):
        reasons.append("retained_by_keep_last")
    if reasons:
        return _base(snapshot_id=identifier, eligible=False, reasons=reasons, **options)
    point, manifest = snapshot._verified(identifier)
    closed = _closed(identifier)
    if closed["binding"] != manifest["binding"]:
        raise facts.SnapshotConflict("snapshot closed/manifest bindings differ")
    inventory = _inventory(point)
    expected_files = {"manifest.json", "database.db", "config.json"} | {
        os.path.join("files", name) for name in manifest["files"]}
    expected_directories = {".", "files"} | {
        os.path.normpath(os.path.join("files", name)) for name in manifest["directories"]}
    if os.path.lexists(os.path.join(point, "rollback.json")):
        rollback = snapshot._json(os.path.join(point, "rollback.json"))
        if (rollback.get("format") != snapshot.FORMAT or rollback.get("snapshot_id") != identifier
                or rollback.get("manifest_sha256") != closed.get("manifest_sha256")
                or rollback.get("replacement") != manifest["database"]
                or type(rollback.get("originals")) is not dict or "state.db" not in rollback["originals"]
                or set(rollback["originals"]) - snapshot.DATABASE_FILES):
            raise facts.SnapshotConflict("snapshot prune rollback journal invalid")
        expected_files.add("rollback.json")
        expected_directories.add("before-rollback")
        for name, expected in rollback["originals"].items():
            relative = os.path.join("before-rollback", name)
            value = inventory["files"].get(relative)
            if value is None or {k: value[k] for k in ("bytes", "sha256")} != expected:
                raise facts.SnapshotConflict("snapshot prune retained rollback image mismatch")
            expected_files.add(relative)
    if set(inventory["files"]) != expected_files or set(inventory["directories"]) != expected_directories:
        raise facts.SnapshotConflict("snapshot prune contains unrecorded files/directories")
    body = {"snapshot_id": identifier, "options": options, "binding": manifest["binding"],
            "close_record": snapshot._file_fact(os.path.join(maintenance.directory(), "closed-" + identifier + ".json"), private=True),
            **inventory}
    digest = hashlib.sha256(facts._encoded(body)).hexdigest()
    return _base(snapshot_id=identifier, eligible=True, reasons=[], plan_sha256=digest,
                 payload_bytes=sum(v["bytes"] for n, v in inventory["files"].items() if n not in KEEP_FILES),
                 files_to_remove=sum(n not in KEEP_FILES for n in inventory["files"]), plan=body, **options)


def _validate_journal(identifier, journal):
    body = journal.get("plan")
    if (journal.get("contract") != FORMAT or journal.get("snapshot_id") != identifier
            or journal.get("phase") not in {"pruning", "pruned"} or type(body) is not dict
            or body.get("snapshot_id") != identifier
            or hashlib.sha256(facts._encoded(body)).hexdigest() != journal.get("plan_sha256")):
        raise facts.SnapshotConflict("snapshot prune journal binding invalid")
    # Recheck every retained relative name before using a crash journal.
    for field in ("files", "directories"):
        if type(body.get(field)) is not dict or len(body[field]) > MAX_ENTRIES:
            raise facts.SnapshotConflict("snapshot prune journal inventory invalid")
        for name in body[field]:
            if (type(name) is not str or os.path.isabs(name) or os.path.normpath(name) != name
                    or name == ".." or name.startswith(".." + os.sep)):
                raise facts.SnapshotConflict("snapshot prune journal path invalid")
    if "." not in body["directories"] or "manifest.json" not in body["files"]:
        raise facts.SnapshotConflict("snapshot prune audit bindings missing")
    if type(body.get("options")) is not dict or type(body.get("binding")) is not dict or type(body.get("close_record")) is not dict:
        raise facts.SnapshotConflict("snapshot prune journal authority bindings missing")
    for value in body["files"].values():
        if (type(value) is not dict or set(value) != {"bytes", "sha256", "dev", "ino", "mtime_ns", "ctime_ns"}
                or any(type(value.get(k)) is not int or value[k] < 0 for k in ("bytes", "dev", "ino", "mtime_ns", "ctime_ns"))
                or type(value.get("sha256")) is not str or re.fullmatch(r"[0-9a-f]{64}", value["sha256"]) is None):
            raise facts.SnapshotConflict("snapshot prune journal file binding invalid")
    for value in body["directories"].values():
        if type(value) is not list or len(value) != 2 or any(type(n) is not int or n < 0 for n in value):
            raise facts.SnapshotConflict("snapshot prune journal directory binding invalid")


def _directory(point, name, plan):
    path = point
    for relative in ["."] + ([] if name == "." else [os.path.join(*name.split(os.sep)[:i]) for i in range(1, len(name.split(os.sep)) + 1)]):
        path = os.path.join(point, relative)
        info = snapshot._stat(path, directory=True, private=True)
        if [info.st_dev, info.st_ino] != plan["directories"].get(relative):
            raise facts.SnapshotConflict("snapshot prune original directory changed")
    return path


def _remove(identifier, journal):
    plan = journal["plan"]
    point = snapshot._point(identifier)
    deadline = time.monotonic() + MAX_SECONDS
    current = _inventory(point)
    if (any(plan[field].get(name) != value for field in ("files", "directories") for name, value in current[field].items())
            or any(name not in current["files"] for name in KEEP_FILES & set(plan["files"]))):
        raise facts.SnapshotConflict("snapshot prune tree drifted from original intent")
    for name, expected in sorted(plan["files"].items()):
        if time.monotonic() > deadline:
            raise facts.SnapshotConflict("snapshot prune time bound exceeded; same plan may resume")
        parent_name = os.path.dirname(name) or "."
        path = os.path.join(point, name)
        if not os.path.lexists(path):
            if name in KEEP_FILES:
                raise facts.SnapshotConflict("snapshot prune retained audit file missing")
            # Missing is allowed only under a durable original pruning intent.
            continue
        parent = _directory(point, parent_name, plan)
        info = snapshot._stat(path, private=True)
        actual = {**snapshot._file_fact(path, private=True), "dev": info.st_dev, "ino": info.st_ino,
                  "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns}
        if snapshot._signature(snapshot._stat(path, private=True)) != snapshot._signature(info):
            raise facts.SnapshotConflict("snapshot prune file changed during final digest check")
        if actual != expected:
            raise facts.SnapshotConflict("snapshot prune original file changed")
        if name in KEEP_FILES:
            continue
        fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            if [os.fstat(fd).st_dev, os.fstat(fd).st_ino] != plan["directories"][parent_name]:
                raise facts.SnapshotConflict("snapshot prune parent changed")
            current = os.stat(os.path.basename(name), dir_fd=fd, follow_symlinks=False)
            if (not stat.S_ISREG(current.st_mode) or current.st_nlink != 1
                    or snapshot._signature(current) != snapshot._signature(info)):
                raise facts.SnapshotConflict("snapshot prune file changed before unlink")
            os.unlink(os.path.basename(name), dir_fd=fd)
            os.fsync(fd)
        finally:
            os.close(fd)
    for name in sorted((n for n in plan["directories"] if n != "."), key=lambda n: (n.count(os.sep), n), reverse=True):
        path = os.path.join(point, name)
        if os.path.lexists(path):
            _directory(point, name, plan)
            if os.listdir(path):
                raise facts.SnapshotConflict("snapshot prune found unrecorded directory content")
            os.rmdir(path)
            snapshot._sync(os.path.dirname(path))
    # Retain the point inode, manifest, rollback journal and permanent close record.
    expected = KEEP_FILES & set(plan["files"])
    if set(os.listdir(_directory(point, ".", plan))) != expected:
        raise facts.SnapshotConflict("snapshot prune found unrecorded audit-root content")


def prune(identifier, *, retention_days=30, keep_last=2, as_of=None, dry_run=False, expect_plan=None):
    identifier = snapshot._identifier(identifier)
    if not dry_run and (as_of is None or type(expect_plan) is not str or re.fullmatch(r"[0-9a-f]{64}", expect_plan) is None):
        raise facts.SnapshotConflict("prune apply requires the preview's --as-of and --expect-plan")
    options = _options(retention_days, keep_last, as_of)
    lock = _reader() if dry_run else maintenance.gate(exclusive=True)
    with lock:
        path = _journal(identifier)
        if os.path.lexists(path):
            journal = snapshot._json(path)
            _validate_journal(identifier, journal)
            if journal["plan"]["options"] != options:
                raise facts.SnapshotConflict("prune replay must preserve original retention/as-of binding")
            preview = _base(snapshot_id=identifier, eligible=True, reasons=[], **options,
                            plan_sha256=journal["plan_sha256"], phase=journal["phase"], resume=True)
        else:
            preview = _plan(identifier, options)
            journal = None
        if dry_run:
            return {k: v for k, v in {**preview, "effect": "none", "dry_run": True}.items() if k != "plan"}
        if snapshot.status()["maintenance_open"] or not preview["eligible"]:
            raise facts.SnapshotConflict("snapshot prune is ineligible: " + ",".join(preview["reasons"] or ["maintenance_open"]))
        if preview["plan_sha256"] != expect_plan:
            raise facts.SnapshotConflict("snapshot prune preview changed; no new deletion intent recorded")
        if journal is None:
            journal = _base(snapshot_id=identifier, phase="pruning", plan=preview["plan"], plan_sha256=expect_plan)
        if journal["plan"]["binding"] != snapshot._binding():
            raise facts.SnapshotConflict("snapshot prune host/state/config binding changed")
        if journal["plan"]["close_record"] != snapshot._file_fact(os.path.join(maintenance.directory(), "closed-" + identifier + ".json"), private=True):
            raise facts.SnapshotConflict("snapshot prune close record changed")
        if journal["phase"] != "pruned":
            if not os.path.lexists(path):
                snapshot._publish(path, journal)  # Intent precedes the first unlink.
            _remove(identifier, journal)
            journal.update(phase="pruned", pruned_at_epoch=time.time())
            snapshot._publish(path, journal)
        else:
            _remove(identifier, journal)  # Idempotent replay still verifies retained audit bytes/inodes.
        return _base(snapshot_id=identifier, phase="pruned", effect="closed_snapshot_payload_removed",
                     plan_sha256=expect_plan, audit_retained=True, current_state_modified=False)
