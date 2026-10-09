"""Append-only launch/resource associations. Observations never grant execution.

The allocation is committed before a child can start. It is an intent, not proof
of birth, worker identity, wait, physical GPU ownership, or kernel isolation.
"""
from __future__ import annotations

import json
import os
import re
import secrets
import time

from . import state
from .execution_policy import digest
from .integration import canonical, instance_id

MAX_BYTES = 4 * 1024 * 1024
SCHEMA = """
CREATE TABLE IF NOT EXISTS allocations (
 allocation_id TEXT PRIMARY KEY,
 job_id TEXT NOT NULL REFERENCES jobs(id),
 ordinal INTEGER NOT NULL CHECK(ordinal > 0),
 payload TEXT NOT NULL,
 payload_sha256 TEXT NOT NULL,
 UNIQUE(job_id,ordinal)
);
CREATE INDEX IF NOT EXISTS allocation_job ON allocations(job_id,allocation_id);
CREATE TABLE IF NOT EXISTS allocation_events (
 event_id TEXT PRIMARY KEY,
 allocation_id TEXT NOT NULL REFERENCES allocations(allocation_id),
 job_id TEXT NOT NULL REFERENCES jobs(id),
 seq INTEGER NOT NULL CHECK(seq > 0),
 layer TEXT NOT NULL,
 payload TEXT NOT NULL,
 UNIQUE(allocation_id,seq)
);
CREATE INDEX IF NOT EXISTS allocation_event_order ON allocation_events(allocation_id,seq);
CREATE INDEX IF NOT EXISTS allocation_event_job ON allocation_events(job_id,event_id);
CREATE TRIGGER IF NOT EXISTS allocation_immutable BEFORE UPDATE ON allocations
 BEGIN SELECT RAISE(ABORT,'allocation is immutable'); END;
CREATE TRIGGER IF NOT EXISTS allocation_retained BEFORE DELETE ON allocations
 BEGIN SELECT RAISE(ABORT,'allocation history is retained'); END;
CREATE TRIGGER IF NOT EXISTS allocation_event_immutable BEFORE UPDATE ON allocation_events
 BEGIN SELECT RAISE(ABORT,'allocation observation is immutable'); END;
CREATE TRIGGER IF NOT EXISTS allocation_event_retained BEFORE DELETE ON allocation_events
 BEGIN SELECT RAISE(ABORT,'allocation observations are retained'); END;
"""


def migrate(conn):
    if "allocation_id" not in {row[1] for row in conn.execute("PRAGMA table_info(jobs)")}:
        conn.execute("ALTER TABLE jobs ADD COLUMN allocation_id TEXT")
    conn.execute("""CREATE TRIGGER IF NOT EXISTS revision_job_allocation
        AFTER UPDATE OF allocation_id ON jobs WHEN OLD.allocation_id IS NOT NEW.allocation_id
        BEGIN UPDATE batches SET revision=revision+1 WHERE id=NEW.batch_id; END""")
    conn.execute("""CREATE TRIGGER IF NOT EXISTS allocation_clear_pending
        AFTER UPDATE OF status ON jobs WHEN NEW.status='pending' AND NEW.allocation_id IS NOT NULL
        BEGIN UPDATE jobs SET allocation_id=NULL WHERE id=NEW.id; END""")


def _bounded(value):
    encoded = canonical(value)
    if len(encoded.encode()) > MAX_BYTES:
        raise state.StateError("allocation evidence exceeds 4 MiB bound")
    return encoded


def record(conn, job_id, layer, data, *, allocation_id=None):
    """Caller supplies source facts within the original writer transaction."""
    job = state.get_job(conn, job_id)
    identifier = allocation_id or (dict(job).get("allocation_id") if job is not None else None)
    if identifier is None:
        return None  # No invented history for migrated or never-launched jobs.
    row = conn.execute("SELECT * FROM allocations WHERE allocation_id=? AND job_id=?", (identifier, job_id)).fetchone()
    if row is None:
        raise state.StateError("allocation binding is missing")
    previous = conn.execute("SELECT * FROM allocation_events WHERE allocation_id=? ORDER BY seq DESC LIMIT 1", (identifier,)).fetchone()
    if previous is not None:
        prior = _event(previous)
        if prior["layer"] == layer and prior["data"] == data:
            return prior
    payload = {"schema_version": 1, "allocation_id": identifier, "job_id": job_id,
               "seq": previous["seq"] + 1 if previous else 1, "layer": layer,
               "previous_event_id": previous["event_id"] if previous else None,
               "data": data, "observed_at": state.now()}
    encoded = _bounded(payload)
    event_id = digest(payload)
    conn.execute("INSERT INTO allocation_events VALUES(?,?,?,?,?,?)",
                 (event_id, identifier, job_id, payload["seq"], layer, encoded))
    return {"event_id": event_id, **payload}


def reserve(conn, job_id, spec, dispatcher):
    from .resources import host_mem_gib
    if not conn.in_transaction:
        raise state.StateError("allocation reservation requires launch transaction")
    job = state.get_job(conn, job_id)
    batch = state.get_batch(conn, job["batch_id"])
    if job["status"] != "running" or batch["status"] != "active" or job["pgid"] is not None:
        raise state.StateError("allocation requires the final active running claim")
    iid = instance_id(conn)
    if iid is None:
        raise state.StateError("allocation requires immutable instance identity")
    task = conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
                        (job["batch_id"], job["task_id"], job["version"])).fetchone()
    if task is None or json.loads(task[0]) != spec:
        raise state.StateError("allocation spec differs from frozen task")
    reservations = [dict(row) for row in conn.execute("SELECT gpu_id,vram_gib FROM gpu_jobs WHERE job_id=? ORDER BY gpu_id", (job_id,))]
    allocator = getattr(dispatcher, "allocator", None)
    topology = getattr(allocator, "_uuid_map", None)
    sampled_at = getattr(allocator, "_uuid_observed_at", None)
    fresh = (isinstance(topology, dict) and type(sampled_at) in (int, float)
             and 0 <= time.time() - sampled_at <= 5)
    for card in reservations:
        card.update(gpu_uuid=next((uuid for uuid, idx in topology.items() if idx == card["gpu_id"]), None) if fresh else None,
                    topology_observed_at=sampled_at if fresh else None,
                    topology_status="recorded_sample" if fresh else "unknown",
                    simulated=bool(getattr(dispatcher, "fake", False)))
    identifier = secrets.token_hex(16)
    ordinal = conn.execute("SELECT COALESCE(MAX(ordinal),0)+1 FROM allocations WHERE job_id=?", (job_id,)).fetchone()[0]
    payload = {"schema_version": 1, "allocation_id": identifier, "ordinal": ordinal, "instance_id": iid,
               "job_id": job_id, "batch_id": job["batch_id"], "task_id": job["task_id"],
               "version": job["version"], "spec_sha256": digest(spec), "fingerprint": job["fingerprint"],
               "started_at": job["started_at"], "retries": job["retries"],
               "node": state.hostname(), "scheduler_pid": os.getpid(),
               "cpu_reservation": dispatcher._task_cpus(spec),
               "host_memory_reservation_gib": host_mem_gib(spec, getattr(dispatcher, "cfg", {})),
               "declared_resources": {key: value for key, value in (spec.get("resources") or {}).items()
                                      if key in {"gpu", "cpus", "host_mem_gib", "vram_gib", "gpu_share", "disk_gib", "disk_inodes"}},
               "gpu_reservations": reservations,
               "backend_binding_sha256": digest(spec["_execution_binding"]) if spec.get("_execution_binding") else None,
               "semantics": "launch_intent_not_process_birth", "hard_isolation": False,
               "lease_identity": None, "worker_identity": None, "observed_at": state.now()}
    storage = getattr(dispatcher, "_storage_launch_observation", None)
    lease = getattr(dispatcher, "_cluster_lease", None)
    if lease is not None:
        payload["lease_identity"] = {"lease_id": lease.owner["lease_id"], "instance_id": lease.origin["instance_id"],
                                     "recorded_allocation_state": lease.decision["allocation_state"]}
    capacity = getattr(dispatcher, "_cpu_capacity", None)
    if isinstance(capacity, dict):
        payload["cpu_capacity"] = capacity
    if (isinstance(storage, dict) and storage.get("allowed") is True and storage.get("job_id") == job_id
            and storage.get("spec_sha256") == digest(spec)):
        payload["storage_filesystems"] = [item["filesystem_id"] for item in storage["filesystems"] if "task" in item["roles"]]
        payload["storage_observation_sha256"] = digest(storage)
    # Resource declarations are scalar schema-validated data, not env/cmd/root.
    encoded = _bounded(payload)
    conn.execute("INSERT INTO allocations VALUES(?,?,?,?,?)", (identifier, job_id, ordinal, encoded, digest(payload)))
    conn.execute("UPDATE jobs SET allocation_id=? WHERE id=?", (identifier, job_id))
    record(conn, job_id, "resource", {"event": "reservation_committed", "hard_isolation": False,
                                    "physical_ownership_verified": False})
    return identifier


def state_change(conn, before, fields):
    keys = {"status", "rc", "failure", "kill_reason", "pgid", "finished_at"}
    changed = {key: {"before": before.get(key), "after": value} for key, value in fields.items()
               if key in keys and before.get(key) != value}
    if not before.get("allocation_id") or not changed:
        return
    reason = fields.get("kill_reason")
    layer = "monitor" if isinstance(reason, str) and reason.startswith("probe_") else "scheduler"
    record(conn, before["id"], layer, {"changes": changed, "source": "scheduler_state_update",
                                    "wait_authority": False, "worker_identity": None}, allocation_id=before["allocation_id"])


def ordinary_wait(conn, job, fact, recorded_rc):
    from .artifact_validation import wait_snapshot
    if fact is not None:
        # Verify against the raw wait, not an application sidecar override.
        wait = wait_snapshot(conn, job, fact.get("returncode"), fact)
    else:
        wait = {"source": "legacy_settlement", "verified": False,
                "reason": "original_wait_unavailable", "observation": None}
    return record(conn, job["id"], "process", {"event": "settlement_wait", "wait": wait,
                  "scheduler_recorded_rc": recorded_rc, "worker_identity": None})


def _allocation(row):
    try:
        payload = json.loads(row["payload"])
        if (digest(payload) != row["payload_sha256"] or payload["allocation_id"] != row["allocation_id"]
                or payload["job_id"] != row["job_id"] or payload["ordinal"] != row["ordinal"]):
            raise ValueError("binding")
        return payload
    except (ValueError, TypeError, KeyError, RecursionError) as error:
        raise state.StateError("allocation evidence digest/binding mismatch") from error


def _event(row):
    try:
        payload = json.loads(row["payload"])
        if digest(payload) != row["event_id"] or any(payload[key] != row[key] for key in ("allocation_id", "job_id", "seq", "layer")):
            raise ValueError("binding")
        return {"event_id": row["event_id"], **payload}
    except (ValueError, TypeError, KeyError, RecursionError) as error:
        raise state.StateError("allocation event digest/binding mismatch") from error


def query(conn, batch, task, *, version=None, allocation_id=None, limit=20, cursor=None):
    if type(limit) is not int or not 1 <= limit <= 100 or (version is not None and (type(version) is not int or version < 1)):
        raise ValueError("limit must be 1..100; version must be positive")
    for value in (allocation_id, cursor):
        if value is not None and (not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{32}", value) is None):
            raise ValueError("allocation ID/cursor must be 32 lowercase hex characters")
    if allocation_id and cursor:
        raise ValueError("allocation-id and cursor are mutually exclusive")
    if conn.execute("PRAGMA user_version").fetchone()[0] < 16:
        return {"available": False, "reason": "migration_required", "allocations": [], "truncated": False, "next_cursor": None}
    if not state._schema_is_complete(conn, 16):
        raise state.StateError("allocation schema is incomplete")
    clauses, params = ["j.batch_id=?", "j.task_id=?"], [batch, task]
    if version is not None:
        clauses.append("j.version=?")
        params.append(version)
    if allocation_id:
        clauses.append("a.allocation_id=?")
        params.append(allocation_id)
    if cursor:
        clauses.append("a.allocation_id>?")
        params.append(cursor)
    rows = conn.execute("SELECT a.* FROM allocations a JOIN jobs j ON j.id=a.job_id WHERE " +
                        " AND ".join(clauses) + " ORDER BY a.allocation_id LIMIT ?", (*params, limit + 1)).fetchall()
    if allocation_id and not rows:
        raise ValueError("allocation not found for exact task/version")
    values = []
    used_bytes = 0
    for row in rows[:limit]:
        item = _allocation(row)
        if item["instance_id"] != instance_id(conn) or item["batch_id"] != batch or item["task_id"] != task or (version is not None and item["version"] != version):
            raise state.StateError("allocation task/instance binding mismatch")
        if not allocation_id:
            item = {key: item[key] for key in ("allocation_id", "ordinal", "instance_id", "job_id", "batch_id", "task_id", "version", "spec_sha256", "observed_at", "semantics")}
        else:
            size = conn.execute("SELECT COALESCE(SUM(size),0) FROM (SELECT length(CAST(payload AS BLOB)) AS size FROM allocation_events WHERE allocation_id=? ORDER BY seq LIMIT 1001)", (allocation_id,)).fetchone()[0]
            if size > MAX_BYTES:
                raise ValueError("allocation event evidence exceeds 4 MiB bound")
            events = conn.execute("SELECT * FROM allocation_events WHERE allocation_id=? ORDER BY seq LIMIT 1001", (allocation_id,)).fetchall()
            item["events"] = [_event(event) for event in events[:1000]]
            item["events_truncated"] = len(events) > 1000
            previous = None
            for seq, event in enumerate(item["events"], 1):
                if event["seq"] != seq or event["previous_event_id"] != previous or event["job_id"] != item["job_id"]:
                    raise state.StateError("allocation event chain is incomplete")
                previous = event["event_id"]
            # These are immutable references; use the existing evidence query
            # for complete artifact details. No current file is opened here.
            size = conn.execute("SELECT COALESCE(SUM(size),0) FROM (SELECT length(CAST(payload AS BLOB)) AS size FROM artifact_validations WHERE job_id=? ORDER BY validation_id LIMIT 1001)", (row["job_id"],)).fetchone()[0]
            if size > MAX_BYTES:
                raise ValueError("allocation artifact references exceed 4 MiB bound")
            validations = conn.execute("SELECT * FROM artifact_validations WHERE job_id=? ORDER BY validation_id LIMIT 1001", (row["job_id"],)).fetchall()
            from .artifact_validation import decode
            refs = [decode(v) for v in validations[:1000]]
            item["artifact_validation_ids"] = [v["validation_id"] for v in refs if v["payload"].get("allocation_id") == allocation_id]
            item["artifact_references_truncated"] = len(validations) > 1000
        used_bytes += len(_bounded(item).encode())
        if used_bytes > MAX_BYTES:
            raise ValueError("allocation query exceeds 4 MiB; use a smaller limit")
        values.append(item)
    return {"available": True, "reason": None, "allocations": values, "truncated": len(rows) > limit,
            "next_cursor": values[-1]["allocation_id"] if len(rows) > limit else None,
            "ordering": "allocation_id", "pagination": "live_keyset_not_complete_snapshot"}
