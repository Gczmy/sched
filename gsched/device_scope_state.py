"""Immutable original-scope device intent and one-shot effect ledger.

No BPF/kernel/filesystem probes, installation, detachment or wait inference.
The scheduler integration must commit an intent before the external effect.
"""
from __future__ import annotations

import json
import re

from . import allocation, cpu_scope_state as cpu, state
from .execution import DeviceBinding, DeviceIntent
from .execution_policy import digest
from .integration import canonical, instance_id

SCHEMA = """
CREATE TABLE IF NOT EXISTS device_scopes (
 scope_id TEXT PRIMARY KEY REFERENCES cpu_scopes(scope_id),
 allocation_id TEXT NOT NULL UNIQUE REFERENCES allocations(allocation_id),
 job_id TEXT NOT NULL REFERENCES jobs(id), payload TEXT NOT NULL, payload_sha256 TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS device_scope_job ON device_scopes(job_id,scope_id);
CREATE TABLE IF NOT EXISTS device_scope_events (
 event_id TEXT PRIMARY KEY, scope_id TEXT NOT NULL REFERENCES device_scopes(scope_id),
 seq INTEGER NOT NULL CHECK(seq > 0), kind TEXT NOT NULL, payload TEXT NOT NULL,
 UNIQUE(scope_id,seq)
);
CREATE TRIGGER IF NOT EXISTS device_scope_immutable BEFORE UPDATE ON device_scopes
 BEGIN SELECT RAISE(ABORT,'device scope intent is immutable'); END;
CREATE TRIGGER IF NOT EXISTS device_scope_retained BEFORE DELETE ON device_scopes
 BEGIN SELECT RAISE(ABORT,'device scope intent is retained'); END;
CREATE TRIGGER IF NOT EXISTS device_scope_event_immutable BEFORE UPDATE ON device_scope_events
 BEGIN SELECT RAISE(ABORT,'device scope event is immutable'); END;
CREATE TRIGGER IF NOT EXISTS device_scope_event_retained BEFORE DELETE ON device_scope_events
 BEGIN SELECT RAISE(ABORT,'device scope event is retained'); END;
"""
MAX_EVENTS = 64
MAX_RECORD = 256 * 1024
MAX_CHAIN = 1024 * 1024
MAX_EVIDENCE = 4 * 1024 * 1024
KINDS = {"reserved", "install_intent", "installed", "launch_intent", "unknown", "released", "abandoned"}
TERMINAL = {"released", "abandoned"}


def _encoded(value):
    result = canonical(value)
    if len(result.encode()) > MAX_RECORD:
        raise state.StateError("device evidence exceeds record bound")
    return result


def _hex(value, size):
    if type(value) is not str or re.fullmatch("[0-9a-f]{" + str(size) + "}", value) is None:
        raise state.StateError("invalid device scope identity")
    return value


def _writer(conn):
    if not conn.in_transaction:
        raise state.StateError("device effect requires explicit writer transaction")


def _active(conn, value):
    cpu._allocation(conn, value["allocation_id"], value["job_id"], active=True)
    job = state.get_job(conn, value["job_id"])
    batch = state.get_batch(conn, job["batch_id"])
    latest = conn.execute("SELECT MAX(version) FROM jobs WHERE batch_id=? AND task_id=?", (job["batch_id"], job["task_id"])).fetchone()[0]
    if (job["status"] != "running" or job["allocation_id"] != value["allocation_id"]
            or job["pgid"] is not None or job["kill_reason"] or batch["status"] != "active"
            or job["version"] != value["version"] or job["version"] != latest):
        raise state.StateError("device effect requires original active unlaunched generation")


def reserve(conn, job_id, intent):
    _writer(conn)
    if type(intent) is not DeviceIntent:
        raise ValueError("device reservation requires typed original-scope intent")
    scope_id = intent.scope.intent.scope_id
    original, events = cpu.load(conn, scope_id)
    _, cpu_binding = cpu._allocation(conn, original["allocation_id"], original["job_id"], active=True)
    bound = next((e["data"]["binding"] for e in events if e["kind"] == "inode_bound"), None)
    if (cpu_binding["mode"] != "cgroup" or original["job_id"] != job_id or events[-1]["kind"] != "configured"
            or bound != intent.scope.to_dict() or any(e["kind"] == "launch_intent" for e in events)):
        raise state.StateError("device intent requires original configured scope before CPU launch")
    value = {"schema_version": 1, "scope_id": scope_id, "allocation_id": original["allocation_id"],
        "allocation_sha256": original["allocation_sha256"], "instance_id": original["instance_id"],
        "job_id": job_id, "version": original["version"], "lease_id": original["lease_id"],
        "cpu_scope_sha256": digest(original), "configured_event_id": events[-1]["event_id"],
        "intent": intent.to_dict(), "created_at": state.now()}
    _active(conn, value)
    conn.execute("INSERT INTO device_scopes VALUES(?,?,?,?,?)", (scope_id, value["allocation_id"], job_id, _encoded(value), digest(value)))
    return _append(conn, value, [], "reserved", {})


def _transition(value, events, kind, data):
    phase = events[-1]["kind"] if events else None
    predecessors = {"reserved": {None}, "install_intent": {"reserved"}, "installed": {"install_intent"},
        "launch_intent": {"installed"}, "unknown": KINDS - TERMINAL - {"unknown"},
        "abandoned": {"reserved"}, "released": KINDS - TERMINAL}
    if (type(data) is not dict or kind not in KINDS or phase not in predecessors[kind]):
        raise state.StateError("device effect consumed, unknown or already terminal")
    if kind == "installed":
        if set(data) != {"binding", "observation"}:
            raise state.StateError("device installation needs exact original binding/observation")
        binding = DeviceBinding.from_dict(data["binding"])
        observation = data["observation"]
        if (binding.intent != DeviceIntent.from_dict(value["intent"])
                or type(observation) is not dict
                or set(observation) != {"device_attachment_verified", "program_id", "admission_granted", "wait_authority_granted"}
                or type(observation["program_id"]) is not int or observation["program_id"] != binding.program_id
                or observation["device_attachment_verified"] is not True
                or observation["admission_granted"] is not False or observation["wait_authority_granted"] is not False):
            raise state.StateError("device installation differs or grants unsupported authority")
    elif kind == "unknown":
        if set(data) != {"reason"} or type(data["reason"]) is not str or re.fullmatch("[a-z][a-z0-9_]{0,127}", data["reason"]) is None:
            raise state.StateError("device uncertainty requires bounded reason")
    elif kind == "released":
        if (set(data) != {"cpu_removed_event_id", "semantics"}
                or data["semantics"] != "original_scope_removed_not_program_gc_or_wait"):
            raise state.StateError("device release requires original CPU removal evidence")
        _hex(data["cpu_removed_event_id"], 64)
    elif data:
        raise state.StateError("unexpected device intent fields")


def _append(conn, value, events, kind, data):
    if len(events) >= MAX_EVENTS:
        raise state.StateError("device event limit reached; original resources retained")
    payload = {"schema_version": 1, "scope_id": value["scope_id"], "allocation_id": value["allocation_id"],
        "job_id": value["job_id"], "seq": len(events) + 1, "kind": kind, "data": data,
        "previous_event_id": events[-1]["event_id"] if events else None, "observed_at": state.now()}
    encoded = _encoded(payload)
    size = conn.execute("SELECT COALESCE(SUM(length(CAST(payload AS BLOB))),0) FROM device_scope_events WHERE scope_id=?", (value["scope_id"],)).fetchone()[0]
    if size + len(encoded.encode()) > MAX_CHAIN:
        raise state.StateError("device chain exceeds bound; resources retained")
    identifier = digest(payload)
    conn.execute("INSERT INTO device_scope_events VALUES(?,?,?,?,?)", (identifier, value["scope_id"], payload["seq"], kind, encoded))
    allocation.record(conn, value["job_id"], "resource", {"event": "device_scope_" + kind,
        "scope_id": value["scope_id"], "device_event_id": identifier,
        "wait_authority_granted": False, "physical_boundary_verified": False}, allocation_id=value["allocation_id"])
    return {"event_id": identifier, **payload}


def _evidence_size(conn, identifier):
    result = conn.execute("SELECT length(CAST(d.payload AS BLOB)) + length(CAST(c.payload AS BLOB)) + length(CAST(a.payload AS BLOB)) FROM device_scopes d JOIN cpu_scopes c ON c.scope_id=d.scope_id JOIN allocations a ON a.allocation_id=c.allocation_id WHERE d.scope_id=?", (identifier,)).fetchone()
    if result is None:
        raise state.StateError("device/CPU/allocation evidence binding missing")
    total = result[0]
    for table, maximum in (("device_scope_events", MAX_EVENTS), ("cpu_scope_events", cpu.MAX_EVENTS)):
        size, count = conn.execute("SELECT COALESCE(SUM(size),0),COUNT(*) FROM (SELECT length(CAST(payload AS BLOB)) AS size FROM " + table + " WHERE scope_id=? ORDER BY seq LIMIT ?)", (identifier, maximum + 1)).fetchone()
        if count > maximum:
            raise state.StateError("linked device event count exceeds bound")
        total += size
    return total


def load(conn, scope_id):
    identifier = _hex(scope_id, 32)
    sizes = conn.execute("SELECT length(CAST(payload AS BLOB)) FROM device_scopes WHERE scope_id=?", (identifier,)).fetchone()
    if sizes is None or sizes[0] > MAX_RECORD:
        raise state.StateError("device original intent missing or oversized")
    size, count, largest = conn.execute("SELECT COALESCE(SUM(size),0),COUNT(*),COALESCE(MAX(size),0) FROM (SELECT length(CAST(payload AS BLOB)) AS size FROM device_scope_events WHERE scope_id=? ORDER BY seq LIMIT ?)", (identifier, MAX_EVENTS + 1)).fetchone()
    if not 1 <= count <= MAX_EVENTS or size > MAX_CHAIN or largest > MAX_RECORD:
        raise state.StateError("device chain missing or exceeds bound")
    if _evidence_size(conn, identifier) > MAX_EVIDENCE:
        raise state.StateError("linked device evidence exceeds read bound")
    row = conn.execute("SELECT * FROM device_scopes WHERE scope_id=?", (identifier,)).fetchone()
    value = json.loads(row["payload"])
    keys = {"schema_version", "scope_id", "allocation_id", "allocation_sha256", "instance_id", "job_id",
            "version", "lease_id", "cpu_scope_sha256", "configured_event_id", "intent", "created_at"}
    if (type(value) is not dict or set(value) != keys or digest(value) != row["payload_sha256"]
            or value["schema_version"] != 1 or type(value["schema_version"]) is not int
            or type(value["version"]) is not int or value["version"] < 1
            or any(value[k] != row[k] for k in ("scope_id", "allocation_id", "job_id"))):
        raise state.StateError("device original binding corrupt")
    original, cpu_events = cpu.load(conn, identifier)
    _, cpu_binding = cpu._allocation(conn, original["allocation_id"], original["job_id"])
    intent = DeviceIntent.from_dict(value["intent"])
    bound = next((e["data"]["binding"] for e in cpu_events if e["kind"] == "inode_bound"), None)
    configured = next((e for e in cpu_events if e["event_id"] == value["configured_event_id"]), None)
    if (cpu_binding["mode"] != "cgroup" or intent.scope.to_dict() != bound or digest(original) != value["cpu_scope_sha256"]
            or value["instance_id"] != instance_id(conn) or configured is None or configured["kind"] != "configured"
            or any(value[k] != original[k] for k in ("allocation_id", "allocation_sha256", "job_id", "version", "lease_id", "instance_id"))):
        raise state.StateError("device policy no longer matches original CPU allocation/inode")
    rows = conn.execute("SELECT * FROM device_scope_events WHERE scope_id=? ORDER BY seq LIMIT ?", (identifier, MAX_EVENTS + 1)).fetchall()
    events = []
    for row in rows:
        event = json.loads(row["payload"])
        keys = {"schema_version", "scope_id", "allocation_id", "job_id", "seq", "kind", "data", "previous_event_id", "observed_at"}
        if (type(event) is not dict or set(event) != keys or digest(event) != row["event_id"]
                or type(event["schema_version"]) is not int or event["schema_version"] != 1
                or type(event["seq"]) is not int or event["seq"] != row["seq"] or event["seq"] != len(events) + 1
                or any(event[k] != value[k] for k in ("scope_id", "allocation_id", "job_id"))
                or event["kind"] != row["kind"] or row["scope_id"] != identifier
                or event["previous_event_id"] != (events[-1]["event_id"] if events else None)):
            raise state.StateError("device event chain corrupt")
        _transition(value, events, event["kind"], event["data"])
        if event["kind"] == "released" and not any(e["event_id"] == event["data"]["cpu_removed_event_id"] and e["kind"] == "removed" for e in cpu_events):
            raise state.StateError("device release is not an original CPU removal event")
        events.append({"event_id": row["event_id"], **event})
    return value, events


def advance(conn, scope_id, expected_event_id, kind, data=None):
    _writer(conn)
    value, events = load(conn, scope_id)
    if events[-1]["event_id"] != _hex(expected_event_id, 64):
        raise state.StateError("device lifecycle changed; effect refused")
    data = {} if data is None else data
    _transition(value, events, kind, data)
    _, cpu_events = cpu.load(conn, scope_id)
    if kind in {"install_intent", "installed", "launch_intent"}:
        _active(conn, value)
        if cpu_events[-1]["kind"] != "configured":
            raise state.StateError("device effect requires unconsumed original CPU launch")
    elif kind == "released":
        if cpu_events[-1]["kind"] != "removed" or cpu_events[-1]["event_id"] != data["cpu_removed_event_id"]:
            raise state.StateError("device release requires recorded original scope removal, not absence")
    return _append(conn, value, events, kind, data)


def for_allocation(conn, allocation_id, job_id):
    if conn.execute("PRAGMA user_version").fetchone()[0] < 21:
        return None
    row = conn.execute("SELECT scope_id,job_id FROM device_scopes WHERE allocation_id=?", (allocation_id,)).fetchone()
    if row is None:
        return None
    if row["job_id"] != job_id:
        raise state.StateError("device allocation belongs to another job")
    return load(conn, row["scope_id"])


def release_allowed(conn, allocation_id, job_id):
    entry = for_allocation(conn, allocation_id, job_id)
    return entry is None or entry[1][-1]["kind"] in TERMINAL


def finish_removed(conn, allocation_id, job_id):
    """Execution cleanup guard calls this only after original CPU removal."""
    entry = for_allocation(conn, allocation_id, job_id)
    if entry is None or entry[1][-1]["kind"] in TERMINAL:
        return True
    _writer(conn)
    value, events = entry
    _, cpu_events = cpu.load(conn, value["scope_id"])
    if cpu_events[-1]["kind"] != "removed":
        return False
    advance(conn, value["scope_id"], events[-1]["event_id"], "released", {
        "cpu_removed_event_id": cpu_events[-1]["event_id"],
        "semantics": "original_scope_removed_not_program_gc_or_wait"})
    return True


def cpu_launch_guard(conn, allocation_id, job_id):
    entry = for_allocation(conn, allocation_id, job_id)
    if entry is not None and entry[1][-1]["kind"] != "launch_intent":
        raise state.StateError("CPU launch cannot bypass original device launch intent")


def unresolved(conn):
    if conn.execute("PRAGMA user_version").fetchone()[0] < 21:
        return False
    row = conn.execute("SELECT scope_id FROM device_scopes WHERE COALESCE((SELECT kind FROM device_scope_events e WHERE e.scope_id=device_scopes.scope_id ORDER BY seq DESC LIMIT 1),'missing') NOT IN ('released','abandoned') ORDER BY scope_id LIMIT 1").fetchone()
    if row is None:
        return False
    load(conn, row[0])
    return True


def query(conn, *, scope_id=None, limit=20, cursor=None):
    if type(limit) is not int or not 1 <= limit <= 100 or (scope_id is not None and cursor is not None):
        raise ValueError("device scope query limit/cursor invalid")
    for identifier in (scope_id, cursor):
        if identifier is not None:
            _hex(identifier, 32)
    base = {"runtime_probed": False, "admission_granted": False, "wait_authority_granted": False,
        "physical_boundary_verified": False, "semantics": "recorded_device_intent_not_kernel_health"}
    if conn.execute("PRAGMA user_version").fetchone()[0] < 21:
        return {**base, "available": False, "reason": "migration_required", "scopes": [], "truncated": False, "next_cursor": None}
    if not state._schema_is_complete(conn, 21):
        raise state.StateError("device schema incomplete")
    ids = [scope_id] if scope_id else [r[0] for r in conn.execute("SELECT scope_id FROM device_scopes WHERE scope_id>? ORDER BY scope_id LIMIT ?", (cursor or "", limit + 1))]
    budget = 0
    for identifier in ids[:limit]:
        budget += _evidence_size(conn, identifier)
        if budget > MAX_EVIDENCE:
            raise state.StateError("device query exceeds evidence budget; reduce limit")
    results = []
    for identifier in ids[:limit]:
        value, events = load(conn, identifier)
        item = {**value, "recorded_phase": events[-1]["kind"], "last_event_id": events[-1]["event_id"],
            "installation_consumed": any(e["kind"] == "install_intent" for e in events),
            "launch_consumed": any(e["kind"] == "launch_intent" for e in events),
            "release_recorded_ready": events[-1]["kind"] in TERMINAL,
            "original_program_binding": next((e["data"]["binding"] for e in events if e["kind"] == "installed"), None)}
        if scope_id:
            item["events"] = events
        results.append(item)
    return {**base, "available": True, "reason": None, "scopes": results, "truncated": len(ids) > limit,
        "next_cursor": results[-1]["scope_id"] if len(ids) > limit else None,
        "pagination": "live_keyset_not_complete_snapshot"}
