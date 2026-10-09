"""Bounded database facts for upgrade rollback; never restore or grant execution.

This is a component of the controlled snapshot CLI, not a standalone backup
or rollback authority. Files, maintenance ownership and runtime quiescence must
also be verified at the operation boundary.
"""
from __future__ import annotations

import hashlib
import json
import math
import time

from . import state
from .integration import instance_id

MAX_TABLES = 256
MAX_COLUMNS = 128
MAX_ROWS = 500_000
MAX_ROW_BYTES = 8 * 1024 * 1024
MAX_TOTAL_BYTES = 256 * 1024 * 1024
ADDITIVE_TABLES = frozenset({
    "artifact_validations", "artifact_revalidations", "task_dependency_events",
    "allocations", "allocation_events", "daemon_leases", "daemon_lease_events",
    "cpu_assignments", "cpu_scopes", "cpu_scope_events", "device_scopes",
    "device_scope_events", "device_inventory_bindings",
})
# Only audited post-schema-10 migration defaults are acceptable. An arbitrary
# new table/column with default-valued rows is not proof of a harmless migration.
ADDITIVE_COLUMNS = {
    ("batches", "failure_policy"): "freeze",
    ("batches", "depends_on_exact"): "[]",
    ("jobs", "allocation_id"): None,
}


class SnapshotConflict(state.StateError):
    pass


def _quoted(name):
    if not isinstance(name, str) or not name or len(name) > 256 or "\x00" in name:
        raise SnapshotConflict("invalid snapshot SQL identifier")
    return '"' + name.replace('"', '""') + '"'


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("ascii")


def _scalar(value):
    if value is None or isinstance(value, (str, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    if isinstance(value, bytes):
        return {"sqlite_blob_hex": value.hex()}
    raise SnapshotConflict("unsupported snapshot SQLite value")


def _tables(conn):
    names = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    if not names or len(names) > MAX_TABLES:
        raise SnapshotConflict("snapshot table bound exceeded")
    return names


def _columns(conn, table):
    rows = conn.execute("PRAGMA table_xinfo(" + _quoted(table) + ")").fetchall()
    if not rows or len(rows) > MAX_COLUMNS or any(r[6] != 0 for r in rows):
        raise SnapshotConflict("snapshot column bound or generated-column policy violated")
    return [r[1] for r in rows]


def _rows_digest(conn, table, columns, budget, deadline):
    sql = "SELECT " + ",".join(_quoted(c) for c in columns) + " FROM " + _quoted(table)
    hashes = []
    for row in conn.execute(sql):
        budget[0] += 1
        if budget[0] > MAX_ROWS or (deadline is not None and time.monotonic() > deadline):
            raise SnapshotConflict("snapshot row/time bound exceeded")
        data = _encoded([_scalar(v) for v in row])
        budget[1] += len(data)
        if len(data) > MAX_ROW_BYTES or budget[1] > MAX_TOTAL_BYTES:
            raise SnapshotConflict("snapshot byte bound exceeded")
        hashes.append(hashlib.sha256(data).digest())
    # Independent of query order; repeated identical rows retain multiplicity.
    hashes.sort()
    return {"rows": len(hashes), "sha256": hashlib.sha256(b"".join(hashes)).hexdigest()}


def database_facts(conn, *, deadline=None):
    """Read a supported complete private database; do not migrate or write it."""
    state._require_supported_schema(conn)
    schema = conn.execute("PRAGMA user_version").fetchone()[0]
    iid = instance_id(conn)
    if schema < 10 or iid is None:
        raise SnapshotConflict("snapshot requires an existing schema-10-or-newer instance")
    budget, tables = [0, 0], {}
    for name in _tables(conn):
        columns = _columns(conn, name)
        tables[name] = {"columns": columns, **_rows_digest(conn, name, columns, budget, deadline)}
    return {"format": "sched-upgrade-database-facts/v1", "instance_id": iid,
            "database_schema": schema, "tables": tables}


def assert_migration_only(conn, baseline, *, deadline=None):
    """Reject lost/changed/new facts, including unknown receipts and new births.

    This check is necessary, not sufficient, for rollback. It cannot attest
    runtime isolation or a maintenance fence and never authorizes execution.
    """
    state._require_supported_schema(conn)
    if (type(baseline) is not dict or baseline.get("format") != "sched-upgrade-database-facts/v1"
            or type(baseline.get("database_schema")) is not int
            or not 10 <= baseline["database_schema"] <= state.DB_SCHEMA_VERSION
            or instance_id(conn) != baseline.get("instance_id")
            or conn.execute("PRAGMA user_version").fetchone()[0] < baseline["database_schema"]):
        raise SnapshotConflict("snapshot instance/schema binding changed")
    original = baseline.get("tables")
    if not isinstance(original, dict) or not 1 <= len(original) <= MAX_TABLES:
        raise SnapshotConflict("invalid snapshot table facts")
    current = set(_tables(conn))
    if set(original) - current or current - set(original) - ADDITIVE_TABLES:
        raise SnapshotConflict("snapshot table set changed outside audited migrations")
    budget = [0, 0]
    for name in sorted(current):
        columns = _columns(conn, name)
        if name not in original:
            # A migration may create an empty table, never a new receipt/event.
            if conn.execute("SELECT 1 FROM " + _quoted(name) + " LIMIT 1").fetchone() is not None:
                raise SnapshotConflict("post-snapshot durable facts exist")
            continue
        before = original[name]
        if type(before) is not dict:
            raise SnapshotConflict("invalid snapshot table facts")
        old_columns = before.get("columns")
        if (not isinstance(old_columns, list) or not 1 <= len(old_columns) <= MAX_COLUMNS
                or any(type(c) is not str for c in old_columns)
                or len(set(old_columns)) != len(old_columns) or set(old_columns) - set(columns)):
            raise SnapshotConflict("snapshot columns were removed or are invalid")
        for added in set(columns) - set(old_columns):
            key = (name, added)
            if key not in ADDITIVE_COLUMNS:
                raise SnapshotConflict("unaudited post-snapshot column")
            # SQLite IS distinguishes NULL and handles its audited scalar default.
            if conn.execute("SELECT 1 FROM " + _quoted(name) + " WHERE " + _quoted(added)
                            + " IS NOT ? LIMIT 1", (ADDITIVE_COLUMNS[key],)).fetchone() is not None:
                raise SnapshotConflict("post-snapshot column contains new facts")
        observed = _rows_digest(conn, name, old_columns, budget, deadline)
        if observed != {"rows": before.get("rows"), "sha256": before.get("sha256")}:
            raise SnapshotConflict("post-snapshot rows changed; rollback refused")
