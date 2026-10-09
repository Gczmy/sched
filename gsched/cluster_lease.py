"""Immutable daemon origin and recorded Slurm validation; no execution authority."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time

from . import state
from .execution_policy import digest
from .integration import instance_id

SCHEMA = """
CREATE TABLE IF NOT EXISTS daemon_leases (
 lease_id TEXT PRIMARY KEY, instance_id TEXT NOT NULL, payload TEXT NOT NULL, sha256 TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS daemon_lease_events (
 lease_id TEXT NOT NULL REFERENCES daemon_leases(lease_id), seq INTEGER NOT NULL,
 kind TEXT NOT NULL, payload TEXT NOT NULL, sha256 TEXT NOT NULL, PRIMARY KEY(lease_id,seq)
);
CREATE INDEX IF NOT EXISTS daemon_lease_kind ON daemon_lease_events(lease_id,kind,seq);
CREATE TRIGGER IF NOT EXISTS daemon_lease_immutable BEFORE UPDATE ON daemon_leases BEGIN SELECT RAISE(ABORT,'immutable daemon origin'); END;
CREATE TRIGGER IF NOT EXISTS daemon_lease_retained BEFORE DELETE ON daemon_leases BEGIN SELECT RAISE(ABORT,'retain daemon origin'); END;
CREATE TRIGGER IF NOT EXISTS daemon_lease_event_immutable BEFORE UPDATE ON daemon_lease_events BEGIN SELECT RAISE(ABORT,'immutable daemon observation'); END;
CREATE TRIGGER IF NOT EXISTS daemon_lease_event_retained BEFORE DELETE ON daemon_lease_events BEGIN SELECT RAISE(ABORT,'retain daemon observation'); END;
"""
MAX_AGE = 45
_pending_probe = None


def policy(cfg):
    supplied = cfg.get("lease_validation", {})
    if not isinstance(supplied, dict) or set(supplied) - {"mode", "unknown_policy", "interval_sec"}:
        raise ValueError("lease_validation 必须为受支持字段的对象")
    value = {"mode": "auto", "unknown_policy": "pause", "interval_sec": 30, **supplied}
    if value["mode"] not in ("auto", "enforce", "observe") or value["unknown_policy"] not in ("pause", "allow"):
        raise ValueError("lease_validation mode/unknown_policy 非法")
    if type(value["interval_sec"]) is not int or not 1 <= value["interval_sec"] <= 30:
        raise ValueError("lease_validation.interval_sec 必须为 1..30 整数")
    return value


def _text(path, bound):
    with Path(path).open() as stream:
        result = stream.read(bound + 1)
    if len(result.encode()) > bound:
        raise ValueError("kernel context bound")
    return result


def kernel_context():
    from .executor import process_start_token
    value = {"pid": os.getpid(), "start_token": process_start_token(os.getpid()),
             "physical_host": socket.gethostname().strip(), "uid": os.getuid(),
             "affinity": None, "cpus_allowed_list": None, "cgroups": None}
    try:
        cpus = sorted(os.sched_getaffinity(0))
        if not cpus or len(cpus) > 65536 or any(type(c) is not int or not 0 <= c < 2 ** 20 for c in cpus):
            raise ValueError("affinity bound")
        value["affinity"] = cpus
    except (OSError, AttributeError, ValueError):
        pass
    try:
        raw = _text("/proc/self/cgroup", 8192)
        entries = [line.split(":", 2) for line in raw.splitlines()]
        if not 1 <= len(entries) <= 32 or any(len(e) != 3 or not e[0].isdecimal() or not e[2].startswith("/") for e in entries):
            raise ValueError("cgroup shape")
        value["cgroups"] = [{"hierarchy": e[0], "controllers": e[1], "path": e[2]} for e in entries]
        for line in _text("/proc/self/status", 65536).splitlines():
            if line.startswith("Cpus_allowed_list:"):
                text = line.split(":", 1)[1].strip()
                if len(text) <= 32768 and re.fullmatch(r"[0-9,-]+", text):
                    value["cpus_allowed_list"] = text
    except (OSError, ValueError):
        pass
    return value


def slurm_environment():
    # No generic environment snapshot, command, token, or customer root.
    values = {}
    for key in ("SLURM_JOB_ID", "SLURM_STEP_ID", "SLURM_CPUS_ON_NODE", "SLURM_CPUS_PER_TASK", "SLURM_NTASKS", "SLURM_CLUSTER_NAME"):
        value = os.environ.get(key)
        if value is not None:
            values[key] = value if re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", value) else None
    return values


def probe(job_id):
    global _pending_probe
    process = None
    try:
        if _pending_probe is not None:
            if _pending_probe.poll() is None:
                raise ValueError("Slurm helper still pending")
            for stream in (_pending_probe.stdin, _pending_probe.stdout, _pending_probe.stderr):
                if stream is not None:
                    stream.close()
            _pending_probe = None
        process = subprocess.Popen([sys.executable, "-m", "gsched.lease_probe"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)
        output, _ = process.communicate(json.dumps({"job_id": job_id}), timeout=5)
        if process.returncode or len(output.encode()) > 1024 * 1024:
            raise ValueError("Slurm helper failed or exceeded bound")
        value = json.loads(output)
        if (not isinstance(value, dict) or type(value.get("known")) is not bool
                or not 0 <= time.time() - value["observed_at"] <= 5):
            raise ValueError("Slurm helper observation invalid")
        return value
    except (OSError, ValueError, KeyError, TypeError, RecursionError, subprocess.SubprocessError):
        if process is not None and process.poll() is None:
            try:
                process.kill()
            except OSError:
                pass
            _pending_probe = process
        return {"observed_at": time.time(), "known": False, "reason": "slurm_probe_unavailable_or_timeout"}


def binding(job):
    return {k: job[k] for k in ("job_id", "user_id", "start_time", "restart_count", "nodes", "cpus")}


def decide(origin, current, sample, *, frozen_binding=None, invalid_latched=False):
    reasons, unknown = [], []
    job_id = origin["slurm_environment"].get("SLURM_JOB_ID")
    for key in ("pid", "start_token", "physical_host", "uid", "affinity", "cgroups", "cpus_allowed_list"):
        if origin.get(key) is None or current.get(key) is None:
            unknown.append("kernel_" + key + "_unknown")
        elif origin[key] != current[key]:
            reasons.append("kernel_" + key + "_changed")
    if not job_id:
        unknown.append("slurm_origin_absent")
    elif not sample.get("known") or not 0 <= time.time() - sample.get("observed_at", 0) <= MAX_AGE:
        unknown.append("slurm_observation_unknown_or_stale")
    elif sample.get("missing") is True:
        if sample.get("job_id") == job_id:
            reasons.append("original_slurm_job_absent")
        else:
            unknown.append("slurm_observation_binding_invalid")
    else:
        job = sample.get("job")
        if not isinstance(job, dict) or job.get("job_id") != job_id:
            unknown.append("slurm_observation_binding_invalid")
        else:
            if job["states"] != ["RUNNING"]:
                reasons.append("original_slurm_job_not_running")
            if job["user_id"] != origin["uid"]:
                reasons.append("original_slurm_job_uid_changed")
            if origin["physical_host"] not in job["hosts"] and origin["physical_host"].split(".")[0] not in job["hosts"]:
                unknown.append("slurm_node_hostname_not_verified")
            if job["start_time"] > origin["started_at"] or (frozen_binding is not None and binding(job) != frozen_binding):
                reasons.append("original_slurm_allocation_replaced_or_resized")
            if job.get("end_time") and job["end_time"] <= time.time():
                reasons.append("original_slurm_allocation_end_reached")
            groups = current.get("cgroups") or []
            matching = [g for g in groups if re.search(r"(?:^|/)job_" + re.escape(job_id) + r"(?:/|$)", g["path"])]
            if not matching:
                unknown.append("job_cgroup_membership_not_verified")
            step = origin["slurm_environment"].get("SLURM_STEP_ID")
            if step and matching and not any(re.search(r"(?:^|/)step_" + re.escape(step) + r"(?:/|$)", g["path"]) for g in matching):
                unknown.append("step_cgroup_membership_not_verified")
    if invalid_latched:
        reasons.append("allocation_invalid_latched")
    invalid = invalid_latched or bool(reasons)
    settings = origin["policy"]
    enforced = settings["mode"] == "enforce" or (settings["mode"] == "auto" and "SLURM_JOB_ID" in origin["slurm_environment"])
    allowed = not enforced or (not invalid and (not unknown or settings["unknown_policy"] == "allow"))
    return {"allocation_state": "invalid" if invalid else "unknown" if unknown else "valid",
            "invalid_latched": invalid, "reasons": sorted(set(reasons)), "unknown": sorted(set(unknown)),
            "enforced": enforced, "dispatch_allowed": allowed, "hard_isolation": False}


def encode(value):
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(text.encode()) > 256 * 1024:
        raise ValueError("daemon lease evidence exceeds 256 KiB")
    return text


def event(conn, lease_id, kind, data):
    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")
    previous = conn.execute("SELECT seq,sha256 FROM daemon_lease_events WHERE lease_id=? ORDER BY seq DESC LIMIT 1", (lease_id,)).fetchone()
    seq = previous["seq"] + 1 if previous else 1
    value = {"lease_id": lease_id, "seq": seq, "kind": kind, "previous_sha256": previous["sha256"] if previous else None,
             "recorded_at": time.time(), "data": data}
    conn.execute("INSERT INTO daemon_lease_events VALUES(?,?,?,?,?)", (lease_id, seq, kind, encode(value), digest(value)))
    return value


def decode(row):
    if len(row["payload"].encode()) > 256 * 1024:
        raise state.StateError("daemon lease evidence exceeds record bound")
    value = json.loads(row["payload"])
    if digest(value) != row["sha256"] or value.get("lease_id") != row["lease_id"]:
        raise state.StateError("daemon lease evidence binding/hash mismatch")
    if "seq" in row.keys() and (value.get("seq") != row["seq"] or value.get("kind") != row["kind"]):
        raise state.StateError("daemon lease event identity mismatch")
    return value


class Monitor:
    def __init__(self, cfg, owner):
        self.owner = dict(owner)
        context = kernel_context()
        self.current_context = context
        env = slurm_environment()
        self.origin = {**context, "schema_version": 1, "lease_id": owner["lease_id"], "node": state.hostname(),
                       "started_at": time.time(), "slurm_environment": env, "policy": policy(cfg)}
        if any(self.origin[k] != owner[k] for k in ("pid", "start_token", "physical_host")):
            raise ValueError("daemon origin differs from exact process owner")
        self.sample = probe(env["SLURM_JOB_ID"]) if env.get("SLURM_JOB_ID") else {"known": False, "observed_at": time.time()}
        self.frozen_binding = binding(self.sample["job"]) if self.sample.get("known") and self.sample.get("job") else None
        self.origin["initial_slurm_binding"] = self.frozen_binding
        self.invalid_latched = False
        self.decision = decide(self.origin, context, self.sample, frozen_binding=self.frozen_binding)
        self.invalid_latched = self.decision["invalid_latched"]
        self.finished = False
        self.last_check = 0
        self.notice_started = False
        with state.connect() as conn:
            self.origin["instance_id"] = instance_id(conn)
            conn.execute("INSERT INTO daemon_leases VALUES(?,?,?,?)", (owner["lease_id"], self.origin["instance_id"], encode(self.origin), digest(self.origin)))
            self._record(conn, context)
            if self.invalid_latched:
                self._incident(conn)
        self.last_check = time.time()
        self._recorded_decision = self.decision
        self._incident_recorded = self.invalid_latched

    def _incident(self, conn):
        state.insert_incident(conn, ts=state.now(), kind="lease_invalid", gpu_idx=None, job_id=None, batch_id=None,
            payload_json=json.dumps({"lease_id": self.owner["lease_id"], "reasons": self.decision["reasons"],
                                     "dispatch_paused": self.decision["enforced"]}))

    def _record(self, conn, context):
        event(conn, self.owner["lease_id"], "check", {**self.decision, "current_context": context,
              "slurm_observation": self.sample, "slurm_binding": self.frozen_binding})

    def update(self, *, force=False):
        now = time.time()
        current = kernel_context()
        self.current_context = current
        job_id = self.origin["slurm_environment"].get("SLURM_JOB_ID")
        if job_id and (force or not 0 <= now - self.sample["observed_at"] < self.origin["policy"]["interval_sec"]):
            self.sample = probe(job_id)
        decision = decide(self.origin, current, self.sample, frozen_binding=self.frozen_binding, invalid_latched=self.invalid_latched)
        if self.frozen_binding is None and self.sample.get("job") and not decision["invalid_latched"]:
            self.frozen_binding = binding(self.sample["job"])
        changed = decision != self._recorded_decision
        needs_incident = decision["invalid_latched"] and not self._incident_recorded
        self.decision = decision
        self.invalid_latched = decision["invalid_latched"]
        if changed or needs_incident or force or not 0 <= now - self.last_check < self.origin["policy"]["interval_sec"]:
            with state.connect() as conn:
                self._record(conn, current)
                if needs_incident:
                    self._incident(conn)
            # Advance audit acknowledgements only after the transaction commits.
            self.last_check = time.time()
            self._recorded_decision = self.decision
            self._incident_recorded = self._incident_recorded or needs_incident
        return self.decision["dispatch_allowed"]

    def finish(self, reason):
        if self.finished:
            return
        with state.connect() as conn:
            event(conn, self.owner["lease_id"], "exit", {"reason": reason, "context": kernel_context(),
                "last_allocation_state": self.decision["allocation_state"], "invalid_latched": self.invalid_latched,
                "semantics": "daemon_lease_release_not_worker_wait"})
        self.finished = True


def query(conn, *, lease_id=None, limit=20, cursor=None, after_seq=0):
    from .daemon import _read_lease_owner, health_snapshot
    if type(limit) is not int or not 1 <= limit <= 100 or type(after_seq) is not int or not 0 <= after_seq < 2 ** 63:
        raise ValueError("lease limit 必须为 1..100，after-seq 必须为非负整数")
    for value in (lease_id, cursor):
        if value is not None and not re.fullmatch(r"[0-9a-f]{32}", value):
            raise ValueError("lease identity 必须为 32 位 hex")
    if lease_id and cursor:
        raise ValueError("lease-id 与 cursor 互斥")
    if after_seq and not lease_id:
        raise ValueError("after-seq 必须绑定 exact lease-id")
    if conn.execute("PRAGMA user_version").fetchone()[0] < 17:
        return {"available": False, "reason": "migration_required", "leases": [], "truncated": False, "next_cursor": None}
    clauses, params = [], []
    if lease_id:
        clauses.append("lease_id=?")
        params.append(lease_id)
    if cursor:
        clauses.append("lease_id>?")
        params.append(cursor)
    selection = "SELECT lease_id FROM daemon_leases" + (" WHERE " + " AND ".join(clauses) if clauses else "") + " ORDER BY lease_id LIMIT ?"
    ids = [r[0] for r in conn.execute(selection, (*params, limit + 1))]
    # Bound retained evidence before materializing any payload, including the
    # independently selected latest check/exit and exact event page.
    budget = 0
    for selected in ids[:limit]:
        budget += conn.execute("SELECT length(CAST(payload AS BLOB)) FROM daemon_leases WHERE lease_id=?", (selected,)).fetchone()[0]
        for kind in ("check", "exit"):
            size = conn.execute("SELECT length(CAST(payload AS BLOB)) FROM daemon_lease_events WHERE lease_id=? AND kind=? ORDER BY seq DESC LIMIT 1", (selected, kind)).fetchone()
            budget += size[0] if size else 0
        if lease_id:
            budget += conn.execute("SELECT COALESCE(SUM(size),0) FROM (SELECT length(CAST(payload AS BLOB)) AS size FROM daemon_lease_events WHERE lease_id=? AND seq>? ORDER BY seq LIMIT ?)", (selected, after_seq, limit)).fetchone()[0]
        if budget > 3 * 1024 * 1024:
            raise ValueError("daemon lease query exceeds evidence bound; reduce limit")
    rows = [conn.execute("SELECT * FROM daemon_leases WHERE lease_id=?", (selected,)).fetchone() for selected in ids[:limit]]
    if lease_id and not rows:
        raise ValueError("exact daemon lease 不存在")
    owner, items, total = _read_lease_owner(), [], 0
    health = health_snapshot()
    for row in rows:
        origin = decode(row)
        if origin["instance_id"] != instance_id(conn) or row["instance_id"] != origin["instance_id"]:
            raise state.StateError("daemon origin instance mismatch")
        latest = conn.execute("SELECT * FROM daemon_lease_events WHERE lease_id=? AND kind='check' ORDER BY seq DESC LIMIT 1", (row["lease_id"],)).fetchone()
        end = conn.execute("SELECT * FROM daemon_lease_events WHERE lease_id=? AND kind='exit' ORDER BY seq DESC LIMIT 1", (row["lease_id"],)).fetchone()
        recorded, exited = decode(latest) if latest else None, decode(end) if end else None
        age = time.time() - recorded["recorded_at"] if recorded else None
        current_owner = (owner is not None and all(owner[k] == origin[k] for k in ("lease_id", "pid", "start_token", "physical_host"))
                         and health["process_state"] != "stopped"
                         and (health["query_host"] != origin["physical_host"] or health["process_state"] == "running")
                         and _read_lease_owner() == owner)
        effective = (recorded["data"]["allocation_state"] if recorded and current_owner and not exited and 0 <= age <= MAX_AGE else "unknown")
        item = {"origin": origin, "recorded_check": recorded, "recorded_exit": exited,
                "current_owner_binding": current_owner, "observation_age_s": age,
                "allocation_state": effective, "admission_granted": False}
        if lease_id:
            events = conn.execute("SELECT * FROM daemon_lease_events WHERE lease_id=? AND seq>? ORDER BY seq LIMIT ?", (lease_id, after_seq, limit)).fetchall()
            item["events"] = [decode(e) for e in events[:limit]]
            previous = conn.execute("SELECT sha256 FROM daemon_lease_events WHERE lease_id=? AND seq=?", (lease_id, after_seq)).fetchone() if after_seq else None
            expected = previous[0] if previous else None
            for seq, entry in enumerate(item["events"], after_seq + 1):
                if entry["seq"] != seq or entry["previous_sha256"] != expected:
                    raise state.StateError("daemon lease event chain incomplete")
                expected = digest(entry)
            item["events_truncated"] = bool(events and conn.execute("SELECT 1 FROM daemon_lease_events WHERE lease_id=? AND seq>? LIMIT 1", (lease_id, events[-1]["seq"])).fetchone())
            item["next_seq"] = item["events"][-1]["seq"] if item["events_truncated"] else None
        total += len(json.dumps(item, allow_nan=False).encode())
        if total > 4 * 1024 * 1024:
            raise ValueError("daemon lease query exceeds 4 MiB; reduce limit")
        items.append(item)
    return {"available": True, "leases": items, "truncated": len(ids) > limit,
            "next_cursor": ids[limit - 1] if len(ids) > limit else None}
