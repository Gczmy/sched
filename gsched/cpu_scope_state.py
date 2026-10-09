"""Append-only CPU scope lifecycle, without filesystem or execution authority.

Each external effect requires a separately committed intent. An uncertain
effect is never retried by name, and an absent scope is not proof of removal.
This ledger never performs filesystem effects or grants execution authority.
"""
from __future__ import annotations

import json
import re

from . import allocation, state
from .execution import CpuScopeBinding, CpuScopeIntent
from .execution_policy import digest
from .integration import canonical, instance_id

SCHEMA = """
CREATE TABLE IF NOT EXISTS cpu_scopes (
 scope_id TEXT PRIMARY KEY,
 allocation_id TEXT NOT NULL UNIQUE REFERENCES allocations(allocation_id),
 job_id TEXT NOT NULL REFERENCES jobs(id),
 payload TEXT NOT NULL,
 payload_sha256 TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS cpu_scope_job ON cpu_scopes(job_id,scope_id);
CREATE TABLE IF NOT EXISTS cpu_scope_events (
 event_id TEXT PRIMARY KEY,
 scope_id TEXT NOT NULL REFERENCES cpu_scopes(scope_id),
 seq INTEGER NOT NULL CHECK(seq > 0),
 kind TEXT NOT NULL,
 payload TEXT NOT NULL,
 UNIQUE(scope_id,seq)
);
CREATE TRIGGER IF NOT EXISTS cpu_scope_immutable BEFORE UPDATE ON cpu_scopes
 BEGIN SELECT RAISE(ABORT,'CPU scope intent is immutable'); END;
CREATE TRIGGER IF NOT EXISTS cpu_scope_retained BEFORE DELETE ON cpu_scopes
 BEGIN SELECT RAISE(ABORT,'CPU scope intent is retained'); END;
CREATE TRIGGER IF NOT EXISTS cpu_scope_event_immutable BEFORE UPDATE ON cpu_scope_events
 BEGIN SELECT RAISE(ABORT,'CPU scope event is immutable'); END;
CREATE TRIGGER IF NOT EXISTS cpu_scope_event_retained BEFORE DELETE ON cpu_scope_events
 BEGIN SELECT RAISE(ABORT,'CPU scope event is retained'); END;
"""
MAX_EVENTS = 256
MAX_BYTES = 1024 * 1024
KINDS = {"reserved", "create_intent", "inode_bound", "configure_intent", "configured",
         "launch_intent", "cleanup_ready", "cleanup_intent", "removed", "abandoned", "unknown"}
TERMINAL = {"removed", "abandoned"}
OBSERVATION_KEYS = {"scope_configured", "populated", "direct_process_count", "effective_cpus",
                    "effective_mems", "admission_granted", "wait_authority_granted", "device_isolation"}


def _hex(value, count):
    if type(value) is not str or re.fullmatch("[0-9a-f]{" + str(count) + "}", value) is None:
        raise state.StateError("invalid CPU scope identity")
    return value


def _encoded(value):
    result = canonical(value)
    if len(result.encode()) > MAX_BYTES:
        raise state.StateError("CPU scope evidence exceeds bound")
    return result


def _writer(conn):
    if not conn.in_transaction:
        raise state.StateError("CPU scope effects require an explicit writer transaction")


def _allocation(conn, identifier, job_id, *, active=False):
    from .cpu_isolation import allocation_binding
    value, binding = allocation_binding(conn, identifier, job_id)
    if value["instance_id"] != instance_id(conn):
        raise state.StateError("CPU scope belongs to another instance")
    if active:
        cpus = [row[0] for row in conn.execute("SELECT cpu FROM cpu_assignments WHERE allocation_id=? AND job_id=? ORDER BY cpu", (identifier, job_id))]
        if cpus != binding["cpus"]:
            raise state.StateError("CPU scope requires all original CPU claims")
    return value, binding


def reserve(conn, job_id, intent):
    """Same writer as allocation/CPU claims, before any mkdir or other effect."""
    _writer(conn)
    if type(intent) is not CpuScopeIntent:
        raise ValueError("CPU scope reservation requires a typed intent")
    job = state.get_job(conn, job_id)
    if job is None or job["status"] != "running" or job["pgid"] is not None or not job["allocation_id"]:
        raise state.StateError("CPU scope requires an original unlaunched running allocation")
    batch = state.get_batch(conn, job["batch_id"])
    latest = conn.execute("SELECT MAX(version) FROM jobs WHERE batch_id=? AND task_id=?", (job["batch_id"], job["task_id"])).fetchone()[0]
    if batch["status"] != "active" or latest != job["version"]:
        raise state.StateError("CPU scope requires the current active generation")
    value, cpu = _allocation(conn, job["allocation_id"], job_id, active=True)
    lease = value.get("lease_identity")
    if (intent.binding_sha256 != digest(value) or list(intent.cpus) != cpu["cpus"]
            or type(lease) is not dict or lease.get("lease_id") != cpu["lease_id"]
            or lease.get("instance_id") != value["instance_id"]):
        raise state.StateError("CPU scope intent differs from original allocation/lease/CPU binding")
    payload = {"schema_version": 1, "scope_id": intent.scope_id, "allocation_id": value["allocation_id"],
               "allocation_sha256": digest(value), "job_id": job_id, "version": value["version"],
               "instance_id": value["instance_id"], "lease_id": lease["lease_id"],
               "intent": intent.to_dict(), "created_at": state.now()}
    conn.execute("INSERT INTO cpu_scopes VALUES(?,?,?,?,?)", (intent.scope_id, value["allocation_id"], job_id, _encoded(payload), digest(payload)))
    return _append(conn, payload, [], "reserved", {})


def _append(conn, value, events, kind, data):
    if len(events) >= MAX_EVENTS:
        raise state.StateError("CPU scope event limit reached; original claims retained")
    payload = {"schema_version": 1, "scope_id": value["scope_id"], "allocation_id": value["allocation_id"],
               "job_id": value["job_id"], "seq": len(events) + 1,
               "previous_event_id": events[-1]["event_id"] if events else None,
               "kind": kind, "data": data, "observed_at": state.now()}
    identifier = digest(payload)
    conn.execute("INSERT INTO cpu_scope_events VALUES(?,?,?,?,?)", (identifier, value["scope_id"], payload["seq"], kind, _encoded(payload)))
    allocation.record(conn, value["job_id"], "resource", {"event": "cpu_scope_" + kind,
        "scope_id": value["scope_id"], "scope_event_id": identifier, "wait_authority_granted": False,
        "physical_boundary_verified": False}, allocation_id=value["allocation_id"])
    return {"event_id": identifier, **payload}


def _observation(value, intent, *, configured=False):
    if (type(value) is not dict or set(value) != OBSERVATION_KEYS
            or any(type(value[key]) is not bool for key in ("scope_configured", "populated", "admission_granted", "wait_authority_granted"))
            or value["admission_granted"] or value["wait_authority_granted"]
            or value["device_isolation"] != "not_configured"
            or type(value["direct_process_count"]) is not int or not 0 <= value["direct_process_count"] <= 10000):
        raise state.StateError("invalid passive CPU scope observation")
    for key, bound in (("effective_cpus", 65536), ("effective_mems", 4096)):
        cpus = value[key]
        if (type(cpus) is not list or len(cpus) > bound or any(type(cpu) is not int or not 0 <= cpu < 1048576 for cpu in cpus)
                or cpus != sorted(set(cpus))):
            raise state.StateError("invalid CPU scope observed capacity")
    if configured and (not value["scope_configured"] or value["populated"] or value["direct_process_count"]
                       or value["effective_cpus"] != list(intent.cpus) or value["effective_mems"] != list(intent.mems)):
        raise state.StateError("CPU scope configuration is not an exact empty binding")


def _validate_transition(value, events, kind, data):
    phase = events[-1]["kind"] if events else None
    intent = CpuScopeIntent.from_dict(value["intent"])
    bound = next((event["data"]["binding"] for event in events if event["kind"] == "inode_bound"), None)
    if type(data) is not dict or kind not in KINDS or phase in TERMINAL:
        raise state.StateError("CPU scope transition invalid or already terminal")
    allowed = {"reserved": {None}, "create_intent": {"reserved"}, "inode_bound": {"create_intent"},
               "configure_intent": {"inode_bound"}, "configured": {"configure_intent"},
               "launch_intent": {"configured"}, "cleanup_intent": {"inode_bound", "configure_intent", "configured", "launch_intent", "unknown"},
               "cleanup_ready": {"inode_bound", "configure_intent", "configured", "launch_intent", "unknown"},
               "removed": {"cleanup_intent"}, "abandoned": {"reserved"}, "unknown": KINDS - TERMINAL - {"unknown"}}
    if phase not in allowed[kind]:
        if not (kind == "cleanup_intent" and phase == "cleanup_ready"):
            raise state.StateError("CPU scope effect consumed or recovery cannot replay it")
    if kind == "inode_bound":
        if set(data) != {"binding"}:
            raise state.StateError("CPU scope inode data invalid")
        binding = CpuScopeBinding.from_dict(data["binding"])
        if binding.intent != intent or bound is not None:
            raise state.StateError("CPU scope original inode cannot be replaced")
    elif kind == "configured":
        if set(data) != {"observation"} or bound is None:
            raise state.StateError("CPU scope configuration requires its original inode")
        _observation(data["observation"], intent, configured=True)
    elif kind == "cleanup_ready":
        if (set(data) != {"cleanup_source"} or bound is None
                or data["cleanup_source"] not in ("ordinary_group_gone", "adoption_group_gone", "stop_group_gone", "configured_group_clean", "configured_not_started", "launch_not_started")
                or any(event["kind"] in ("cleanup_ready", "cleanup_intent") for event in events)):
            raise state.StateError("CPU scope cleanup request requires original binding and unconsumed effect")
    elif kind == "cleanup_intent":
        if set(data) != {"observation", "cleanup_source"} or bound is None:
            raise state.StateError("CPU scope cleanup requires its original inode")
        _observation(data["observation"], intent)
        if (data["observation"]["populated"] or data["observation"]["direct_process_count"]
                or data["cleanup_source"] not in ("ordinary_group_gone", "adoption_group_gone", "stop_group_gone", "configured_group_clean", "configured_not_started", "launch_not_started")):
            raise state.StateError("CPU scope cleanup requires known empty scope and identified source")
        if any(event["kind"] == "cleanup_intent" for event in events):
            raise state.StateError("CPU scope cleanup effect consumed; do not infer removal from absence")
    elif kind == "removed":
        if data != {"scope_removed": True, "wait_authority_granted": False} or type(data.get("scope_removed")) is not bool or type(data.get("wait_authority_granted")) is not bool:
            raise state.StateError("CPU scope removal is not original wait authority")
    elif kind == "unknown":
        if set(data) != {"reason"} or type(data["reason"]) is not str or re.fullmatch("[a-z][a-z0-9_]{0,127}", data["reason"]) is None:
            raise state.StateError("CPU scope uncertainty requires a bounded reason code")
    elif data:
        raise state.StateError("CPU scope intent event has unexpected fields")


def load(conn, scope_id):
    """Verify immutable original binding and whole bounded chain; no probes."""
    row = conn.execute("SELECT * FROM cpu_scopes WHERE scope_id=?", (_hex(scope_id, 32),)).fetchone()
    if row is None:
        raise state.StateError("CPU scope intent missing")
    value = json.loads(row["payload"])
    if (type(value) is not dict or set(value) != {"schema_version", "scope_id", "allocation_id", "allocation_sha256", "job_id", "version", "instance_id", "lease_id", "intent", "created_at"}
            or digest(value) != row["payload_sha256"]
            or value.get("scope_id") != row["scope_id"] or value.get("allocation_id") != row["allocation_id"]
            or value.get("job_id") != row["job_id"] or value.get("schema_version") != 1):
        raise state.StateError("CPU scope original binding corrupt")
    origin, cpu = _allocation(conn, row["allocation_id"], row["job_id"])
    intent = CpuScopeIntent.from_dict(value["intent"])
    lease = origin.get("lease_identity")
    if (type(lease) is not dict or lease.get("instance_id") != value["instance_id"] or lease.get("lease_id") != value["lease_id"]
            or intent.scope_id != row["scope_id"] or intent.binding_sha256 != digest(origin)
            or value["allocation_sha256"] != digest(origin) or value["instance_id"] != origin["instance_id"]
            or value["version"] != origin["version"] or value["lease_id"] != cpu["lease_id"] or list(intent.cpus) != cpu["cpus"]):
        raise state.StateError("CPU scope is not bound to original allocation")
    rows = conn.execute("SELECT * FROM cpu_scope_events WHERE scope_id=? ORDER BY seq LIMIT ?", (scope_id, MAX_EVENTS + 1)).fetchall()
    if not rows or len(rows) > MAX_EVENTS:
        raise state.StateError("CPU scope lifecycle missing or exceeds bound")
    events = []
    for row in rows:
        event = json.loads(row["payload"])
        if (type(event) is not dict or set(event) != {"schema_version", "scope_id", "allocation_id", "job_id", "seq", "previous_event_id", "kind", "data", "observed_at"}
                or digest(event) != row["event_id"] or event.get("scope_id") != scope_id
                or event.get("job_id") != value["job_id"] or event.get("allocation_id") != value["allocation_id"]
                or event.get("kind") != row["kind"] or event.get("schema_version") != 1
                or type(event.get("seq")) is not int or event["seq"] != row["seq"] or row["seq"] != len(events) + 1
                or event.get("previous_event_id") != (events[-1]["event_id"] if events else None)):
            raise state.StateError("CPU scope event chain corrupt")
        _validate_transition(value, events, event["kind"], event["data"])
        events.append({"event_id": row["event_id"], **event})
    return value, events


def advance(conn, scope_id, expected_event_id, kind, data=None):
    """CAS consumes one effect. Caller commits before acting outside the DB."""
    _writer(conn)
    value, events = load(conn, scope_id)
    if events[-1]["event_id"] != _hex(expected_event_id, 64):
        raise state.StateError("CPU scope lifecycle changed; effect refused")
    _allocation(conn, value["allocation_id"], value["job_id"], active=True)
    data = {} if data is None else data
    _validate_transition(value, events, kind, data)
    if kind in {"create_intent", "configure_intent", "launch_intent"}:
        job = state.get_job(conn, value["job_id"])
        batch = state.get_batch(conn, job["batch_id"])
        latest = conn.execute("SELECT MAX(version) FROM jobs WHERE batch_id=? AND task_id=?", (job["batch_id"], job["task_id"])).fetchone()[0]
        if job["status"] != "running" or job["allocation_id"] != value["allocation_id"] or job["pgid"] is not None or job["kill_reason"] or batch["status"] != "active" or job["version"] != latest:
            raise state.StateError("CPU scope effect no longer belongs to an active unlaunched generation")
    return _append(conn, value, events, kind, data)


def release_allowed(conn, allocation_id, job_id):
    """Unknown or nonterminal scope retains claims even after terminal job CAS."""
    row = conn.execute("SELECT scope_id FROM cpu_scopes WHERE allocation_id=? AND job_id=?", (allocation_id, job_id)).fetchone()
    if row is None:
        from .allocation import _allocation as decode_allocation
        original = conn.execute("SELECT * FROM allocations WHERE allocation_id=? AND job_id=?", (allocation_id, job_id)).fetchone()
        if original is not None and decode_allocation(original).get("cpu_binding", {}).get("mode") == "cgroup":
            raise state.StateError("original cgroup allocation scope intent missing; retain resources")
        return True  # No retroactive scope binding for legacy affinity jobs.
    _, events = load(conn, row[0])
    return events[-1]["kind"] in TERMINAL


def unresolved(conn):
    """A cold switch to off must not bypass an old unresolved scope."""
    row = conn.execute("SELECT scope_id FROM cpu_scopes WHERE COALESCE((SELECT kind FROM cpu_scope_events e WHERE e.scope_id=cpu_scopes.scope_id ORDER BY seq DESC LIMIT 1),'missing') NOT IN ('removed','abandoned') ORDER BY scope_id LIMIT 1").fetchone()
    if row is None:
        return False
    load(conn, row[0])  # Missing/corrupt evidence is an error, not an empty pool.
    return True


def request_cleanup(conn, job, *, cleanup_source):
    """Execution guard has proved no child/clean group; never probe in writer."""
    identifier = dict(job).get("allocation_id")
    if not identifier or conn.execute("PRAGMA user_version").fetchone()[0] < 19:
        return True
    row = conn.execute("SELECT scope_id FROM cpu_scopes WHERE allocation_id=? AND job_id=?", (identifier, job["id"])).fetchone()
    if row is None:
        release_allowed(conn, identifier, job["id"])
        return True
    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")
    _, events = load(conn, row[0])
    last = events[-1]
    if last["kind"] in TERMINAL:
        return True
    if last["kind"] == "reserved":
        advance(conn, row[0], last["event_id"], "abandoned")
        return True
    if last["kind"] == "create_intent":
        advance(conn, row[0], last["event_id"], "unknown", {"reason": "creation_result_unknown"})
        return False
    if (last["kind"] in ("cleanup_ready", "cleanup_intent")
            or any(e["kind"] in ("cleanup_ready", "cleanup_intent") for e in events)
            or not any(e["kind"] == "inode_bound" for e in events)):
        return False
    advance(conn, row[0], last["event_id"], "cleanup_ready", {"cleanup_source": cleanup_source})
    return False


def query(conn, *, scope_id=None, limit=20, cursor=None):
    if (type(limit) is not int or not 1 <= limit <= 100 or (cursor is not None and scope_id is not None)):
        raise ValueError("CPU scope query limit/cursor invalid")
    if cursor is not None:
        _hex(cursor, 32)
    if scope_id is not None:
        _hex(scope_id, 32)
    base = {"runtime_probed": False, "admission_granted": False, "wait_authority_granted": False,
            "physical_boundary_verified": False, "semantics": "recorded_scope_lifecycle_not_kernel_health"}
    if conn.execute("PRAGMA user_version").fetchone()[0] < 19:
        return {**base, "available": False, "reason": "migration_required", "scopes": [], "truncated": False, "next_cursor": None}
    if not state._schema_is_complete(conn, 19):
        raise state.StateError("CPU scope schema incomplete")
    ids = ([scope_id] if scope_id is not None else [row[0] for row in conn.execute("SELECT scope_id FROM cpu_scopes WHERE scope_id>? ORDER BY scope_id LIMIT ?", (cursor or "", limit + 1))])
    results = []
    for identifier in ids[:limit]:
        value, events = load(conn, identifier)
        result = {**value, "recorded_phase": events[-1]["kind"], "last_event_id": events[-1]["event_id"],
                  "cpu_release_recorded_ready": events[-1]["kind"] in TERMINAL,
                  "creation_consumed": any(event["kind"] == "create_intent" for event in events),
                  "launch_consumed": any(event["kind"] == "launch_intent" for event in events),
                  "original_inode_binding": next((event["data"]["binding"] for event in events if event["kind"] == "inode_bound"), None)}
        if scope_id is not None:
            result["events"] = events
        results.append(result)
    return {**base, "available": True, "reason": None, "scopes": results, "truncated": len(ids) > limit,
            "next_cursor": results[-1]["scope_id"] if len(ids) > limit else None,
            "pagination": "live_keyset_not_complete_snapshot"}
