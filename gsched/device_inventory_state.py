"""Frozen original allocation/device mapping; no hardware or install effects."""
from __future__ import annotations

import json
import math
import time

from . import allocation, cpu_scope_state as cpu, device_inventory as inventory, device_scope_state as devices, state
from .execution import DeviceIntent
from .execution_policy import digest
from .integration import canonical

SCHEMA = """
CREATE TABLE IF NOT EXISTS device_inventory_bindings (
 scope_id TEXT PRIMARY KEY REFERENCES device_scopes(scope_id),
 allocation_id TEXT NOT NULL UNIQUE REFERENCES allocations(allocation_id),
 job_id TEXT NOT NULL REFERENCES jobs(id), payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS device_inventory_job ON device_inventory_bindings(job_id,scope_id);
CREATE TRIGGER IF NOT EXISTS device_inventory_immutable BEFORE UPDATE ON device_inventory_bindings
 BEGIN SELECT RAISE(ABORT,'device inventory binding is immutable'); END;
CREATE TRIGGER IF NOT EXISTS device_inventory_retained BEFORE DELETE ON device_inventory_bindings
 BEGIN SELECT RAISE(ABORT,'device inventory binding is retained'); END;
"""
MAX_RECORD = 256 * 1024
MAX_EVIDENCE = 4 * 1024 * 1024
KEYS = {"schema_version", "scope_id", "allocation_id", "job_id", "instance_id", "version",
        "lease_id", "allocation_sha256", "device_scope_sha256", "device_intent_sha256",
        "inventory_sha256", "inventory", "observed_at", "frozen_at"}


def _time(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise state.StateError("invalid device inventory observation time")
    return value


def _encoded(value):
    encoded = canonical(value)
    if len(encoded.encode()) > MAX_RECORD:
        raise state.StateError("device inventory binding exceeds record bound")
    return encoded


def _context(value, intent):
    parent = intent.scope.intent.parent
    if (value["context"]["boot_id"] != parent.boot_id
            or value["context"]["mount_namespace"] != parent.mount_namespace):
        raise state.StateError("device inventory differs from original CPU scope kernel context")


def _gpu_claims(conn, original):
    claims = [dict(row) for row in conn.execute(
        "SELECT gpu_id,vram_gib FROM gpu_jobs WHERE job_id=? ORDER BY gpu_id", (original["job_id"],))]
    expected = [{k: r[k] for k in ("gpu_id", "vram_gib")} for r in original["gpu_reservations"]]
    if claims != expected:
        raise state.StateError("device inventory requires all original GPU reservations")


def _original(conn, scope_id, *, active=False):
    value, events = devices.load(conn, scope_id)
    original, _ = cpu._allocation(conn, value["allocation_id"], value["job_id"], active=active)
    if digest(original) != value["allocation_sha256"]:
        raise state.StateError("device inventory original allocation digest differs")
    declared = original["declared_resources"].get("gpu")
    if (type(declared) is not int or declared not in (0, 1)
            or type(original["gpu_reservations"]) is not list or len(original["gpu_reservations"]) != declared):
        raise state.StateError("device inventory original GPU reservation cardinality differs from declaration")
    if active:
        devices._active(conn, value)
        _gpu_claims(conn, original)
        _, cpu_events = cpu.load(conn, scope_id)
        if cpu_events[-1]["kind"] != "configured":
            raise state.StateError("device inventory requires original unconsumed CPU scope")
    return value, events, original, DeviceIntent.from_dict(value["intent"])


def freeze(conn, scope_id, sampled, observed_at):
    """Explicit writer before install_intent; never backfill or replace."""
    devices._writer(conn)
    if conn.execute("PRAGMA user_version").fetchone()[0] < 22:
        raise state.StateError("device inventory binding migration required")
    frozen_at = _time(time.time())
    if not 0 <= frozen_at - _time(observed_at) <= inventory.MAX_AGE:
        raise state.StateError("device inventory observation is stale or future")
    # Bound and copy the caller's complete evidence before any persistent write.
    sampled = json.loads(_encoded(sampled))
    value, events, original, intent = _original(conn, scope_id, active=True)
    if events[-1]["kind"] != "reserved":
        raise state.StateError("device inventory cannot be added after installation intent")
    policy = inventory.select_policy(sampled, original["gpu_reservations"], now=frozen_at)
    if policy != intent.policy:
        raise state.StateError("device inventory policy differs from original exact device intent")
    _context(sampled, intent)
    payload = {"schema_version": 1, **{k: value[k] for k in
        ("scope_id", "allocation_id", "job_id", "instance_id", "version", "lease_id", "allocation_sha256")},
        "device_scope_sha256": digest(value), "device_intent_sha256": digest(value["intent"]),
        "inventory_sha256": digest(sampled), "inventory": sampled,
        "observed_at": observed_at, "frozen_at": frozen_at}
    encoded = _encoded(payload)
    conn.execute("INSERT INTO device_inventory_bindings VALUES(?,?,?,?,?)",
        (scope_id, value["allocation_id"], value["job_id"], encoded, digest(payload)))
    allocation.record(conn, value["job_id"], "resource", {
        "event": "device_inventory_frozen", "scope_id": scope_id, "inventory_binding_sha256": digest(payload),
        "inventory_sha256": payload["inventory_sha256"], "admission_granted": False,
        "wait_authority_granted": False, "physical_boundary_verified": False}, allocation_id=value["allocation_id"])
    return payload


def _size(conn, scope_id):
    row = conn.execute("SELECT length(CAST(payload AS BLOB)) FROM device_inventory_bindings WHERE scope_id=?", (scope_id,)).fetchone()
    if row is None or row[0] > MAX_RECORD:
        raise state.StateError("original device inventory binding missing or oversized")
    return row[0] + devices._evidence_size(conn, scope_id)


def load(conn, scope_id):
    identifier = devices._hex(scope_id, 32)
    if conn.execute("PRAGMA user_version").fetchone()[0] < 22:
        raise state.StateError("device inventory binding migration required")
    if _size(conn, identifier) > MAX_EVIDENCE:
        raise state.StateError("linked device inventory evidence exceeds read bound")
    row = conn.execute("SELECT * FROM device_inventory_bindings WHERE scope_id=?", (identifier,)).fetchone()
    binding = json.loads(row["payload"])
    if (type(binding) is not dict or set(binding) != KEYS or digest(binding) != row["payload_sha256"]
            or type(binding["schema_version"]) is not int or binding["schema_version"] != 1
            or type(binding["version"]) is not int or binding["version"] < 1
            or any(binding[k] != row[k] for k in ("scope_id", "allocation_id", "job_id"))
            or digest(binding["inventory"]) != binding["inventory_sha256"]
            or not 0 <= _time(binding["frozen_at"]) - _time(binding["observed_at"]) <= inventory.MAX_AGE):
        raise state.StateError("original device inventory binding corrupt")
    value, _, original, intent = _original(conn, identifier)
    if (any(binding[k] != value[k] for k in
            ("scope_id", "allocation_id", "job_id", "instance_id", "version", "lease_id", "allocation_sha256"))
            or digest(value) != binding["device_scope_sha256"]
            or digest(value["intent"]) != binding["device_intent_sha256"]
            or inventory.select_policy(binding["inventory"], original["gpu_reservations"], now=binding["frozen_at"]) != intent.policy):
        raise state.StateError("frozen device inventory no longer matches original allocation/intent")
    _context(binding["inventory"], intent)
    return binding


def verify_current(conn, scope_id, sampled, observed_at):
    """Future controller's fresh pre-effect check; no install or launch grant."""
    if conn.in_transaction:
        raise state.StateError("device hardware checks must occur outside DB writer")
    now = _time(time.time())
    if not 0 <= now - _time(observed_at) <= inventory.MAX_AGE:
        raise state.StateError("current device inventory observation is stale or future")
    binding = load(conn, scope_id)
    _, events, original, intent = _original(conn, scope_id, active=True)
    if events[-1]["kind"] not in {"reserved", "install_intent", "installed"}:
        raise state.StateError("device effect consumed or original installation unknown")
    sampled = json.loads(_encoded(sampled))
    if (sampled != binding["inventory"] or digest(sampled) != binding["inventory_sha256"]
            or inventory.select_policy(sampled, original["gpu_reservations"], now=now) != intent.policy):
        raise state.StateError("fresh device mapping differs or original GPU topology expired")
    return {"inventory_binding_sha256": digest(binding), "inventory_sha256": binding["inventory_sha256"],
        "runtime_probed": False, "admission_granted": False, "wait_authority_granted": False,
        "physical_boundary_verified": False, "semantics": "caller_sample_verified_not_install_or_launch"}


def query(conn, *, scope_id=None, limit=20, cursor=None):
    if type(limit) is not int or not 1 <= limit <= 100 or (scope_id is not None and cursor is not None):
        raise ValueError("device inventory binding query limit/cursor invalid")
    for identifier in (scope_id, cursor):
        if identifier is not None:
            devices._hex(identifier, 32)
    base = {"runtime_probed": False, "admission_granted": False, "wait_authority_granted": False,
        "physical_boundary_verified": False, "semantics": "frozen_device_inventory_not_kernel_health"}
    if conn.execute("PRAGMA user_version").fetchone()[0] < 22:
        return {**base, "available": False, "reason": "migration_required", "bindings": [], "truncated": False, "next_cursor": None}
    if not state._schema_is_complete(conn, 22):
        raise state.StateError("device inventory binding schema incomplete")
    ids = [scope_id] if scope_id else [r[0] for r in conn.execute(
        "SELECT scope_id FROM device_inventory_bindings WHERE scope_id>? ORDER BY scope_id LIMIT ?", (cursor or "", limit + 1))]
    budget = 0
    for identifier in ids[:limit]:
        budget += _size(conn, identifier)
        if budget > MAX_EVIDENCE:
            raise state.StateError("device inventory query exceeds evidence budget; reduce limit")
    results = []
    for identifier in ids[:limit]:
        value = load(conn, identifier)
        item = {k: v for k, v in value.items() if k != "inventory"}
        item["inventory_binding_sha256"] = digest(value)
        if scope_id:
            item["inventory"] = value["inventory"]
        results.append(item)
    return {**base, "available": True, "reason": None, "bindings": results,
        "truncated": len(ids) > limit, "next_cursor": results[-1]["scope_id"] if len(ids) > limit else None,
        "pagination": "live_keyset_not_complete_snapshot"}
