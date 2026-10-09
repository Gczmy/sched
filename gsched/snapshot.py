"""Private upgrade recovery points; never a historical execution rollback.

The CLI owns this filesystem protocol. An open cooperative maintenance window
blocks all aware writers. Older clients must have been quiesced explicitly.
Only audited schema migration is allowed before rollback; all original durable
facts and non-database files must remain identical. No operation starts daemon.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import socket
import sqlite3
import stat
import time
from contextlib import closing

from . import maintenance, snapshot_facts as facts, state
from .config import config_path

FORMAT = "sched-upgrade-snapshot/v1"
MAX_FILE_BYTES = 128 * 1024 * 1024
MAX_TOTAL_BYTES = 512 * 1024 * 1024
MAX_FILES = 10000
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
DATABASE_FILES = {"state.db", "state.db-wal", "state.db-shm"}


def _identifier(value):
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{32}", value) is None:
        raise facts.SnapshotConflict("snapshot id must be 32 lowercase hexadecimal characters")
    return value


def _sync(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _stat(path, *, directory=False, private=False):
    info = os.lstat(path)
    correct = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if not correct or info.st_uid != os.getuid() or (private and info.st_mode & 0o077):
        raise facts.SnapshotConflict("snapshot path must be an owned nonsymlink " + ("directory" if directory else "regular file"))
    return info


def _signature(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _read(path, *, limit=MAX_FILE_BYTES, private=False):
    before = _stat(path, private=private)
    if before.st_size > limit:
        raise facts.SnapshotConflict("snapshot file byte bound exceeded")
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0))
    with os.fdopen(fd, "rb") as stream:
        if _signature(os.fstat(stream.fileno())) != _signature(before):
            raise facts.SnapshotConflict("snapshot file identity changed")
        data = stream.read(limit + 1)
        after = os.fstat(stream.fileno())
    if len(data) > limit or _signature(before) != _signature(after) or _signature(_stat(path)) != _signature(before):
        raise facts.SnapshotConflict("snapshot file changed while reading")
    return data


def _file_fact(path, *, private=False):
    data = _read(path, private=private)
    return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def _write(path, data):
    # Immutable images use O_EXCL. Publication records use _publish below.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def _publish(path, value):
    data = facts._encoded(value)
    if len(data) > MAX_MANIFEST_BYTES:
        raise facts.SnapshotConflict("snapshot manifest bound exceeded")
    temporary = path + "." + secrets.token_hex(16) + ".tmp"
    try:
        _write(temporary, data)
        os.replace(temporary, path)
        _sync(os.path.dirname(path))
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _json(path):
    try:
        value = json.loads(_read(path, limit=MAX_MANIFEST_BYTES, private=True))
    except (ValueError, TypeError) as error:
        raise facts.SnapshotConflict("invalid snapshot record") from error
    if type(value) is not dict:
        raise facts.SnapshotConflict("invalid snapshot record")
    return value


def _point(identifier):
    return os.path.join(maintenance.directory(), "points", _identifier(identifier))


def _binding():
    host = state.host_dir()
    info = _stat(host, directory=True, private=True)
    return {"state_path": host, "state_dev": info.st_dev, "state_ino": info.st_ino,
            "node": state.hostname(), "physical_host": socket.gethostname(),
            "uid": os.getuid(), "config_path": os.path.abspath(os.path.expanduser(config_path()))}


def _inventory(*, destination=None):
    """Bounded entire node tree, excluding only SQLite's DB/WAL/SHM."""
    root = state.host_dir()
    found, budget, directories = {}, 0, {}
    deadline = time.monotonic() + 30
    def walk_error(error):
        raise facts.SnapshotConflict("snapshot inventory is unreadable") from error
    for parent, names, files in os.walk(root, followlinks=False, onerror=walk_error):
        if len(directories) + len(found) + len(names) + len(files) > MAX_FILES or time.monotonic() > deadline:
            raise facts.SnapshotConflict("snapshot inventory entry/time bound exceeded")
        before = _stat(parent, directory=True, private=True)
        relative = os.path.relpath(parent, root)
        directories[relative] = (before.st_dev, before.st_ino)
        if destination is not None:
            state.ensure_private_directory(os.path.join(destination, relative))
        for name in names:
            _stat(os.path.join(parent, name), directory=True, private=True)
        for name in sorted(files):
            path = os.path.join(parent, name)
            relative_file = os.path.relpath(path, root)
            if relative_file in DATABASE_FILES:
                continue
            data = _read(path)
            budget += len(data)
            if len(found) >= MAX_FILES or budget > MAX_TOTAL_BYTES:
                raise facts.SnapshotConflict("snapshot inventory bound exceeded")
            found[relative_file] = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            if destination is not None:
                target = os.path.join(destination, relative_file)
                state.ensure_private_directory(os.path.dirname(target))
                _write(target, data)
    for relative, identity in directories.items():
        now = _stat(os.path.join(root, relative), directory=True, private=True)
        if (now.st_dev, now.st_ino) != identity:
            raise facts.SnapshotConflict("snapshot directory was replaced")
    return {"files": found, "directories": sorted(directories)}


def _connect_image(path):
    # Images are checkpoint-complete private copies, never shared source DBs.
    _stat(path, private=True)
    for sidecar in (path + "-wal", path + "-shm", path + "-journal"):
        if os.path.lexists(sidecar):
            raise facts.SnapshotConflict("snapshot image must be self-contained without SQLite sidecars")
    conn = sqlite3.connect(Path(path).absolute().as_uri() + "?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _source_guard():
    if os.path.lexists(state.db_path() + "-journal"):
        raise facts.SnapshotConflict("unexpected source rollback journal prevents upgrade snapshot")
    for name in DATABASE_FILES:
        source = os.path.join(state.host_dir(), name)
        if os.path.lexists(source) and _stat(source, private=True).st_size > MAX_FILE_BYTES:
            raise facts.SnapshotConflict("source SQLite file byte bound exceeded")


def _source_image(path):
    _source_guard()
    with state._read_only_database(state.db_path(), copy_timeout=30) as (source, immutable):
        state._require_wal_snapshot(source)
        uri = Path(source).absolute().as_uri() + "?mode=ro" + ("&immutable=1" if immutable else "")
        conn = sqlite3.connect(uri, uri=True)
        target = None
        try:
            state._require_supported_schema(conn)
            _write(path, b"")
            target = sqlite3.connect(path)
            deadline = time.monotonic() + 30
            def progress(status, remaining, total):
                if time.monotonic() > deadline:
                    raise facts.SnapshotConflict("snapshot backup time bound exceeded")
            conn.backup(target, pages=256, progress=progress)
        finally:
            if target is not None:
                target.close()
            conn.close()
    # SQLite backup retains WAL journal mode but contains all committed pages;
    # no image WAL may be omitted or treated as empty by the verifier.
    if os.path.exists(path + "-wal") and os.path.getsize(path + "-wal"):
        raise facts.SnapshotConflict("snapshot backup has an uncheckpointed WAL")
    with closing(_connect_image(path)) as conn:
        if [row[0] for row in conn.execute("PRAGMA quick_check")] != ["ok"]:
            raise facts.SnapshotConflict("snapshot SQLite integrity check failed")
        result = facts.database_facts(conn, deadline=time.monotonic() + 30)
    return result


def _quiescent(conn):
    host = state.host_dir()
    for name in (".lock", "daemon.pid", "daemon.heartbeat", "daemon.supervisor.json"):
        if os.path.lexists(os.path.join(host, name)):
            raise facts.SnapshotConflict("daemon/supervisor ownership is present or indeterminate; drain and wait before snapshot")
    launch = os.path.join(host, "launch")
    if os.path.lexists(launch):
        _stat(launch, directory=True, private=True)
        if os.listdir(launch):
            raise facts.SnapshotConflict("unresolved launch files prevent snapshot")
    checks = [
        ("jobs", "status='running'"), ("gpu_jobs", "1"),
        ("gpus", "job_id IS NOT NULL OR status IN ('assigned','releasing')"),
        ("execution_attempts", "phase NOT IN ('exited','not_started')"),
        ("execution_owner_operations", "cleanup_state!='acknowledged'"),
        ("cpu_assignments", "1"),
        ("native_sessions", "1"),
    ]
    tables = set(facts._tables(conn))
    for table, condition in checks:
        if table in tables and conn.execute("SELECT 1 FROM " + facts._quoted(table) + " WHERE " + condition + " LIMIT 1").fetchone():
            raise facts.SnapshotConflict("active or unresolved resource/execution facts prevent snapshot: " + table)
    for table, terminal in (("cpu_scopes", ("removed", "abandoned")), ("device_scopes", ("released", "abandoned"))):
        if table in tables:
            events = table[:-1] + "_events"
            sql = ("SELECT 1 FROM " + table + " s WHERE COALESCE((SELECT kind FROM " + events
                   + " e WHERE e.scope_id=s.scope_id ORDER BY seq DESC LIMIT 1),'unknown') NOT IN (?,?) LIMIT 1")
            if conn.execute(sql, terminal).fetchone():
                raise facts.SnapshotConflict("unresolved original scope prevents snapshot")


def _verified(identifier):
    path = _point(identifier)
    _stat(path, directory=True, private=True)
    manifest = _json(os.path.join(path, "manifest.json"))
    if manifest.get("format") != FORMAT or manifest.get("snapshot_id") != identifier:
        raise facts.SnapshotConflict("snapshot manifest identity mismatch")
    files = manifest.get("files")
    if type(files) is not dict or len(files) > MAX_FILES:
        raise facts.SnapshotConflict("invalid snapshot inventory")
    directories = manifest.get("directories")
    if (type(directories) is not list or not directories or len(directories) > MAX_FILES
            or any(type(name) is not str for name in directories)
            or sorted(set(directories)) != directories):
        raise facts.SnapshotConflict("invalid snapshot directory inventory")
    for name in directories:
        if os.path.isabs(name) or os.path.normpath(name) != name or name == ".." or name.startswith("../"):
            raise facts.SnapshotConflict("invalid snapshot directory path")
        parent = os.path.join(path, "files")
        _stat(parent, directory=True, private=True)
        for component in name.split(os.sep):
            parent = os.path.join(parent, component)
            _stat(parent, directory=True, private=True)
    total = 0
    for name, expected in files.items():
        if (type(name) is not str or name in DATABASE_FILES or os.path.isabs(name)
                or os.path.normpath(name) != name or name == ".." or name.startswith("../")):
            raise facts.SnapshotConflict("invalid snapshot relative path")
        parent = os.path.join(path, "files")
        for component in name.split(os.sep)[:-1]:
            parent = os.path.join(parent, component)
            _stat(parent, directory=True, private=True)
        actual = _file_fact(os.path.join(path, "files", name), private=True)
        total += actual["bytes"]
        if actual != expected or total > MAX_TOTAL_BYTES:
            raise facts.SnapshotConflict("snapshot file digest mismatch or total byte bound exceeded")
    if _file_fact(os.path.join(path, "config.json"), private=True) != manifest.get("config"):
        raise facts.SnapshotConflict("snapshot config digest mismatch")
    image = os.path.join(path, "database.db")
    if _file_fact(image, private=True) != manifest.get("database"):
        raise facts.SnapshotConflict("snapshot database digest mismatch")
    with closing(_connect_image(image)) as conn:
        if [r[0] for r in conn.execute("PRAGMA quick_check")] != ["ok"]:
            raise facts.SnapshotConflict("snapshot database integrity failure")
        observed = facts.database_facts(conn, deadline=time.monotonic() + 30)
        if observed != manifest.get("database_facts"):
            raise facts.SnapshotConflict("snapshot database facts mismatch")
    return path, manifest


def verify(identifier):
    _, value = _verified(_identifier(identifier))
    return {"schema_version": 1, "contract": FORMAT, "snapshot_id": identifier,
            "verified": True, "instance_id": value["database_facts"]["instance_id"],
            "database_schema": value["database_facts"]["database_schema"],
            "rollback_authorized": False, "daemon_started": False}


def create(*, writers_quiesced=False):
    if not writers_quiesced:
        raise facts.SnapshotConflict("create requires --writers-quiesced: stop unaware old writer clients first")
    with maintenance.gate(exclusive=True):
        if os.path.lexists(maintenance.window_path()):
            raise facts.SnapshotConflict("an upgrade window already exists; inspect it rather than replace its identity")
        binding = _binding()
        identifier = secrets.token_hex(16)
        path = _point(identifier)
        if os.path.lexists(path):
            raise facts.SnapshotConflict("snapshot point identity already exists")
        state.ensure_private_directory(path)
        window = {"format": FORMAT, "snapshot_id": identifier, "binding": binding, "phase": "creating"}
        _publish(maintenance.window_path(), window)
        # Failures deliberately retain the window and any partial private image.
        # close can abandon creating, but rollback cannot use partial evidence.
        image = os.path.join(path, "database.db")
        source = _source_image(image)
        with closing(_connect_image(image)) as conn:
            _quiescent(conn)
        config = _read(binding["config_path"])
        _write(os.path.join(path, "config.json"), config)
        inventory = _inventory(destination=os.path.join(path, "files"))
        manifest = {"format": FORMAT, "snapshot_id": identifier, "binding": binding,
                    "created_at": state.now(), "database_facts": source,
                    "database": _file_fact(image, private=True), **inventory,
                    "config": {"bytes": len(config), "sha256": hashlib.sha256(config).hexdigest()}}
        _publish(os.path.join(path, "manifest.json"), manifest)
        _, verified = _verified(identifier)
        _unchanged(verified)
        _source_guard()
        with state._read_only_database(state.db_path(), copy_timeout=30) as (current, immutable):
            uri = Path(current).absolute().as_uri() + "?mode=ro" + ("&immutable=1" if immutable else "")
            with closing(sqlite3.connect(uri, uri=True)) as conn:
                if facts.database_facts(conn) != source:
                    raise facts.SnapshotConflict("source changed during snapshot creation")
        window["phase"] = "open"
        window["manifest_sha256"] = _file_fact(os.path.join(path, "manifest.json"), private=True)["sha256"]
        _publish(maintenance.window_path(), window)
        return {**verify(identifier), "phase": "open", "maintenance_open": True}


def _window(identifier, *, allow_closed=False):
    value = _json(maintenance.window_path())
    if value.get("format") != FORMAT or value.get("snapshot_id") != _identifier(identifier) or value.get("binding") != _binding():
        raise facts.SnapshotConflict("upgrade window instance/path/host binding mismatch")
    if not allow_closed and os.path.lexists(os.path.join(maintenance.directory(), "closed-" + identifier + ".json")):
        raise facts.SnapshotConflict("upgrade window was closed; old rollback authority cannot be revived")
    return value


def _unchanged(manifest):
    binding = _binding()
    if manifest.get("binding") != binding or _file_fact(binding["config_path"]) != manifest.get("config"):
        raise facts.SnapshotConflict("snapshot configuration or state directory binding changed")
    if _inventory() != {"files": manifest.get("files"), "directories": manifest.get("directories")}:
        raise facts.SnapshotConflict("post-snapshot non-database files changed; rollback refused")


def _eligible(identifier, window):
    path, manifest = _verified(identifier)
    if window.get("manifest_sha256") != _file_fact(os.path.join(path, "manifest.json"), private=True)["sha256"]:
        raise facts.SnapshotConflict("upgrade window manifest binding mismatch")
    _unchanged(manifest)
    _source_guard()
    with state._read_only_database(state.db_path(), copy_timeout=30) as (source, immutable):
        state._require_wal_snapshot(source)
        uri = Path(source).absolute().as_uri() + "?mode=ro" + ("&immutable=1" if immutable else "")
        with closing(sqlite3.connect(uri, uri=True)) as conn:
            _quiescent(conn)
            facts.assert_migration_only(conn, manifest["database_facts"], deadline=time.monotonic() + 30)
    return path, manifest


def migrate(identifier):
    with maintenance.gate(exclusive=True):
        window = _window(identifier)
        if window.get("phase") not in {"open", "migrated"}:
            raise facts.SnapshotConflict("migration requires a complete open upgrade window")
        _eligible(identifier, window)
        with maintenance.actor():
            state.init_db()
        _eligible(identifier, window)
        window["phase"] = "migrated"
        _publish(maintenance.window_path(), window)
        return {"schema_version": 1, "contract": FORMAT, "snapshot_id": identifier,
                "phase": "migrated", "database_schema": state.DB_SCHEMA_VERSION,
                "maintenance_open": True, "daemon_started": False}


def close(identifier):
    with maintenance.gate(exclusive=True):
        window = _window(identifier, allow_closed=True)
        if window.get("phase") == "restoring":
            raise facts.SnapshotConflict("incomplete rollback must be resumed before closing maintenance")
        if window.get("phase") not in {"creating", "open", "migrated", "restored"}:
            raise facts.SnapshotConflict("unknown upgrade window phase")
        # Retain the binding permanently. Old points cannot be reused after close.
        record = os.path.join(maintenance.directory(), "closed-" + identifier + ".json")
        _publish(record, {**window, "phase": "closed", "closed_at": state.now(), "closed_at_epoch": time.time()})
        os.unlink(maintenance.window_path())
        _sync(maintenance.directory())
        return {"schema_version": 1, "contract": FORMAT, "snapshot_id": identifier,
                "phase": "closed", "rollback_authorized": False, "daemon_started": False}


def status():
    try:
        window = _json(maintenance.window_path())
    except FileNotFoundError:
        return {"schema_version": 1, "contract": FORMAT, "maintenance_open": False, "daemon_started": False}
    return {"schema_version": 1, "contract": FORMAT, "maintenance_open": True,
            "snapshot_id": window.get("snapshot_id"), "phase": window.get("phase"),
            "rollback_authorized": False, "daemon_started": False}


def rollback(identifier):
    """Journaled DB-only replace. Every pre-upgrade/current image is retained."""
    with maintenance.gate(exclusive=True):
        window = _window(identifier)
        if window.get("phase") == "restored":
            path, manifest = _verified(identifier)
            if window.get("manifest_sha256") != _file_fact(os.path.join(path, "manifest.json"), private=True)["sha256"]:
                raise facts.SnapshotConflict("restored window manifest binding changed")
            _unchanged(manifest)
            if (any(os.path.lexists(state.db_path() + suffix) for suffix in ("-wal", "-shm", "-journal"))
                    or _file_fact(state.db_path()) != manifest["database"]):
                raise facts.SnapshotConflict("restored database changed")
            return {"schema_version": 1, "contract": FORMAT, "snapshot_id": identifier,
                    "phase": "restored", "maintenance_open": True, "daemon_started": False}
        if window.get("phase") not in {"open", "migrated", "restoring"}:
            raise facts.SnapshotConflict("rollback requires a verified unused upgrade window")
        path = _point(identifier)
        archive = os.path.join(path, "before-rollback")
        journal_path = os.path.join(path, "rollback.json")
        if window["phase"] != "restoring":
            path, manifest = _eligible(identifier, window)
            state.ensure_private_directory(archive)
            if os.stat(archive).st_dev != os.stat(state.host_dir()).st_dev:
                raise facts.SnapshotConflict("rollback requires same-filesystem atomic renames")
            originals = {}
            for name in sorted(DATABASE_FILES):
                source = os.path.join(state.host_dir(), name)
                if os.path.lexists(source):
                    originals[name] = _file_fact(source, private=True)
            journal = {"format": FORMAT, "snapshot_id": identifier, "originals": originals,
                       "replacement": manifest["database"], "manifest_sha256": window["manifest_sha256"]}
            _publish(journal_path, journal)
            window["phase"] = "restoring"
            _publish(maintenance.window_path(), window)
        # Interrupted renames resume against exact digests, never guessed state.
        _, manifest = _verified(identifier)
        if window.get("manifest_sha256") != _file_fact(os.path.join(path, "manifest.json"), private=True)["sha256"]:
            raise facts.SnapshotConflict("rollback resume manifest binding changed")
        _unchanged(manifest)
        journal = _json(journal_path)
        if (journal.get("format") != FORMAT or journal.get("snapshot_id") != identifier
                or journal.get("manifest_sha256") != window.get("manifest_sha256")
                or journal.get("replacement") != manifest["database"]
                or type(journal.get("originals")) is not dict
                or "state.db" not in journal["originals"] or set(journal["originals"]) - DATABASE_FILES):
            raise facts.SnapshotConflict("rollback journal binding mismatch")
        _stat(archive, directory=True, private=True)
        replacement = os.path.join(archive, "replacement.db")
        # Intent is durable before publishing any replacement. A crash before
        # publication leaves original files untouched and can resume safely.
        installed = (os.path.lexists(os.path.join(archive, "state.db"))
                     and os.path.lexists(state.db_path())
                     and _file_fact(state.db_path(), private=True) == journal["replacement"])
        if not os.path.lexists(replacement) and not installed:
            temporary = replacement + "." + secrets.token_hex(16) + ".tmp"
            try:
                _write(temporary, _read(os.path.join(path, "database.db"), private=True))
                os.replace(temporary, replacement)
                _sync(archive)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
        for name in sorted(journal["originals"]):
            source, retained = os.path.join(state.host_dir(), name), os.path.join(archive, name)
            expected = journal["originals"][name]
            if os.path.lexists(retained):
                if _file_fact(retained, private=True) != expected:
                    raise facts.SnapshotConflict("retained rollback image changed")
                if os.path.lexists(source):
                    if name != "state.db" or _file_fact(source, private=True) != journal["replacement"]:
                        raise facts.SnapshotConflict("unexpected source file during rollback resume")
            else:
                if _file_fact(source, private=True) != expected:
                    raise facts.SnapshotConflict("source changed after rollback intent")
                os.rename(source, retained)
                _sync(archive)
                _sync(state.host_dir())
        for name in DATABASE_FILES - set(journal["originals"]):
            if os.path.lexists(os.path.join(state.host_dir(), name)):
                raise facts.SnapshotConflict("unexpected SQLite sidecar during rollback")
        if os.path.lexists(replacement):
            if os.path.lexists(state.db_path()) or _file_fact(replacement, private=True) != journal["replacement"]:
                raise facts.SnapshotConflict("rollback replacement conflict")
            os.rename(replacement, state.db_path())
            _sync(archive)
            _sync(state.host_dir())
        if _file_fact(state.db_path(), private=True) != journal["replacement"]:
            raise facts.SnapshotConflict("installed rollback image mismatch")
        window["phase"] = "restored"
        _publish(maintenance.window_path(), window)
        return {"schema_version": 1, "contract": FORMAT, "snapshot_id": identifier,
                "phase": "restored", "maintenance_open": True, "daemon_started": False}
