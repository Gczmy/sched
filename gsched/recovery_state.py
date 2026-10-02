"""Durable recovery authorization and FIFO; never resets a consumed attempt."""
from __future__ import annotations

import json
import math
import time

from . import recovery, state

SCHEMA = """
CREATE TABLE IF NOT EXISTS recovery_settlements (
 job_id TEXT PRIMARY KEY REFERENCES jobs(id),
 outcome TEXT NOT NULL CHECK(outcome IN ('oom','interrupted')),
 authority TEXT NOT NULL CHECK(authority IN ('wait','group_gone','not_started')),
 binding_sha256 TEXT NOT NULL,
 observed_at REAL NOT NULL,
 decision TEXT NOT NULL DEFAULT 'pending' CHECK(decision IN ('pending','queued','denied','cancelled')),
 reason TEXT,
 successor_job_id TEXT UNIQUE REFERENCES jobs(id)
);
CREATE TABLE IF NOT EXISTS recovery_queue (
 seq INTEGER PRIMARY KEY AUTOINCREMENT,
 job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
 predecessor_job_id TEXT NOT NULL UNIQUE REFERENCES recovery_settlements(job_id),
 root_job_id TEXT NOT NULL REFERENCES jobs(id),
 round INTEGER NOT NULL CHECK(round > 0),
 first_queued_at REAL NOT NULL,
 queued_at REAL NOT NULL,
 not_before REAL NOT NULL,
 checkpoint_sha256 TEXT NOT NULL,
 binding_sha256 TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS recovery_queue_predecessor ON recovery_queue(predecessor_job_id);
CREATE TRIGGER IF NOT EXISTS recovery_settlement_immutable
 BEFORE UPDATE ON recovery_settlements
 WHEN NEW.job_id != OLD.job_id OR NEW.outcome != OLD.outcome
 OR NEW.authority != OLD.authority OR NEW.binding_sha256 != OLD.binding_sha256
 OR NEW.observed_at != OLD.observed_at
 OR (OLD.decision != 'pending' AND (NEW.decision != OLD.decision OR NEW.reason IS NOT OLD.reason OR NEW.successor_job_id IS NOT OLD.successor_job_id))
 BEGIN SELECT RAISE(ABORT,'recovery settlement cannot be rewritten'); END;
CREATE TRIGGER IF NOT EXISTS recovery_queue_immutable
 BEFORE UPDATE ON recovery_queue
 BEGIN SELECT RAISE(ABORT,'recovery queue lineage cannot be rewritten'); END;
CREATE TRIGGER IF NOT EXISTS recovery_settlement_retained
 BEFORE DELETE ON recovery_settlements
 BEGIN SELECT RAISE(ABORT,'recovery settlement history is retained'); END;
CREATE TRIGGER IF NOT EXISTS recovery_queue_retained
 BEFORE DELETE ON recovery_queue
 BEGIN SELECT RAISE(ABORT,'recovery queue history is retained'); END;
"""

DEFAULTS = {"oom": True, "interrupted": True, "cooldown_sec": 30, "max_attempts": 0, "min_free_gib_by_round": [12], "no_progress_sec": 1800, "max_no_progress_sec": 0}


def normalize_policy(raw):
    if type(raw) is not dict or set(raw) - set(DEFAULTS):
        raise recovery.RecoveryError("recovery.retry requires known policy fields")
    policy = {**DEFAULTS, **raw}
    for key in ("oom", "interrupted"):
        if type(policy[key]) is not bool:
            raise recovery.RecoveryError(f"recovery.retry.{key} requires a boolean")
    if not policy["oom"] and not policy["interrupted"]:
        raise recovery.RecoveryError("recovery.retry must enable an outcome")
    value = policy["cooldown_sec"]
    if type(value) not in (int, float) or not 0 <= value <= 86400 or not math.isfinite(value):
        raise recovery.RecoveryError("recovery.retry.cooldown_sec must be finite and within 0..86400")
    if type(policy["max_attempts"]) is not int or not 0 <= policy["max_attempts"] <= 100000:
        raise recovery.RecoveryError("recovery.retry.max_attempts must be 0..100000; 0 is unlimited")
    tiers = policy["min_free_gib_by_round"]
    if type(tiers) is not list or not 1 <= len(tiers) <= 16 or any(type(v) not in (int, float) or not 12 <= v <= 4096 or not math.isfinite(v) for v in tiers) or any(b < a for a, b in zip(tiers, tiers[1:])):
        raise recovery.RecoveryError("recovery.retry.min_free_gib_by_round requires 1..16 nondecreasing finite values >=12")
    for key in ("no_progress_sec", "max_no_progress_sec"):
        value = policy[key]
        if type(value) not in (int, float) or not 0 <= value <= 31536000 or not math.isfinite(value):
            raise recovery.RecoveryError(f"recovery.retry.{key} must be finite and within 0..31536000")
    return policy


def record_clean(conn, job, spec, outcome, authority):
    """Called only by scheduler settlement after original wait or group-gone proof."""
    declaration = spec.get("recovery")
    if declaration is None or declaration["mode"] != "run" or "retry" not in declaration:
        return False
    if outcome not in ("oom", "interrupted") or authority not in ("wait", "group_gone", "not_started") or (outcome == "oom" and authority != "wait"):
        raise state.StateError("recovery settlement lacks exit/cleanup authority")
    current = state.get_job(conn, job["id"])
    if current is None or current["status"] not in ("failed", "blocked", "interrupted"):
        return False
    if outcome == "oom" and (current["rc"] is None or current["rc"] == 0 or current["failure"] != "oom"):
        raise state.StateError("OOM recovery requires recorded failed wait")
    attempt = conn.execute("SELECT * FROM execution_attempts WHERE job_id=?", (job["id"],)).fetchone()
    if attempt is not None:
        observed = json.loads(attempt["observation"] or "{}")
        expected = {"wait": "exited", "group_gone": "authority_lost", "not_started": "not_started"}[authority]
        if observed.get("status") != expected or observed.get("group_clean") is not True or (authority == "wait" and observed.get("returncode") != current["rc"]):
            raise state.StateError("recovery contradicts original execution observation")
    elif authority == "not_started":
        raise state.StateError("not_started recovery requires original execution observation")
    conn.execute("INSERT OR IGNORE INTO recovery_settlements(job_id,outcome,authority,binding_sha256,observed_at) VALUES(?,?,?,?,?)",
                 (job["id"], outcome, authority, spec["_recovery_binding"], time.time()))
    recorded = conn.execute("SELECT * FROM recovery_settlements WHERE job_id=?", (job["id"],)).fetchone()
    if (recorded["outcome"], recorded["authority"], recorded["binding_sha256"]) != (outcome, authority, spec["_recovery_binding"]):
        raise state.StateError("recovery settlement differs from persisted facts")
    return True


def _decision(conn, job_id, decision, reason, successor=None):
    conn.execute("UPDATE recovery_settlements SET decision=?,reason=?,successor_job_id=? WHERE job_id=? AND decision='pending'",
                 (decision, reason, successor, job_id))


def create_next(conn, host_dir, job, spec):
    """Append once within the caller's settlement transaction; preserve the old row."""
    declaration = spec.get("recovery")
    if declaration is None or declaration["mode"] != "run" or "retry" not in declaration:
        return False
    settlement = conn.execute("SELECT * FROM recovery_settlements WHERE job_id=?", (job["id"],)).fetchone()
    if settlement is None:
        return False
    if settlement["decision"] != "pending":
        return settlement["decision"] == "queued"
    current = conn.execute("SELECT j.*,b.status AS batch_status FROM jobs j JOIN batches b ON b.id=j.batch_id WHERE j.id=? AND j.version=(SELECT MAX(version) FROM jobs WHERE batch_id=j.batch_id AND task_id=j.task_id)", (job["id"],)).fetchone()
    cancelled = conn.execute("SELECT 1 FROM control_requests WHERE job_id=? AND op='cancel' AND status='pending'", (job["id"],)).fetchone()
    if current is None or current["status"] not in ("failed", "blocked", "interrupted") or current["batch_status"] != "active" or current["kill_reason"] or cancelled:
        _decision(conn, job["id"], "cancelled", "state_changed")
        return False
    policy = normalize_policy(declaration["retry"])
    if not policy[settlement["outcome"]] or settlement["binding_sha256"] != spec["_recovery_binding"]:
        _decision(conn, job["id"], "denied", "policy")
        return False
    previous = conn.execute("SELECT * FROM recovery_queue WHERE job_id=?", (job["id"],)).fetchone()
    round_number = previous["round"] + 1 if previous else 1
    if policy["max_attempts"] and round_number + 1 > policy["max_attempts"]:
        _decision(conn, job["id"], "denied", "attempt_limit")
        return False
    try:
        context = json.loads(recovery.context(host_dir, job, spec, create=False))
        checkpoint = recovery.CheckpointStore(context).load()
        if checkpoint is None:
            # A clean interruption before any durable progress can start from
            # the initial state. Never treat a lost existing checkpoint as empty.
            if settlement["outcome"] != "interrupted" or (previous and previous["checkpoint_sha256"] != recovery.digest(None)):
                raise recovery.RecoveryError("checkpoint absent")
        receipt = recovery.report(host_dir, job, spec)
        checkpoint_sha = recovery.digest(checkpoint)
        if receipt is not None and (receipt["outcome"] != "oom" or receipt["checkpoint_sha256"] != checkpoint_sha):
            raise recovery.RecoveryError("report differs from durable checkpoint")
    except (ValueError, OSError):
        _decision(conn, job["id"], "denied", "checkpoint_invalid_or_absent")
        return False
    task = conn.execute("SELECT * FROM tasks WHERE batch_id=? AND id=? AND version=?", (job["batch_id"], job["task_id"], job["version"])).fetchone()
    if task is None or json.loads(task["spec"]) != spec:
        _decision(conn, job["id"], "denied", "spec_changed")
        return False
    next_version = current["version"] + 1
    next_id = f"{job['batch_id']}-{job['task_id']}-v{next_version}"
    next_spec = dict(spec, _force_rerun=True)
    timestamp = time.time()
    conn.execute("SAVEPOINT recovery_publish")
    try:
        state.insert_task(conn, job["batch_id"], job["task_id"], next_version, next_spec, task["order_idx"], task["project"])
        state.insert_job(conn, next_id, job["batch_id"], job["task_id"], next_version,
                         spec["_recovery_fingerprint"], project=task["project"])
        conn.execute("INSERT INTO recovery_queue(job_id,predecessor_job_id,root_job_id,round,first_queued_at,queued_at,not_before,checkpoint_sha256,binding_sha256) VALUES(?,?,?,?,?,?,?,?,?)",
                     (next_id, job["id"], previous["root_job_id"] if previous else job["id"], round_number,
                      previous["first_queued_at"] if previous else timestamp, timestamp,
                      timestamp + policy["cooldown_sec"], checkpoint_sha, spec["_recovery_binding"]))
        from . import recovery_watch
        recovery_watch.progress(conn, previous["root_job_id"] if previous else job["id"], checkpoint_sha, timestamp)
        _decision(conn, job["id"], "queued", settlement["outcome"], next_id)
    except BaseException:
        conn.execute("ROLLBACK TO recovery_publish")
        conn.execute("RELEASE recovery_publish")
        raise
    conn.execute("RELEASE recovery_publish")
    return True


def eligible(conn, job, *, timestamp=None):
    entry = conn.execute("SELECT * FROM recovery_queue WHERE job_id=?", (job["id"],)).fetchone()
    if entry is None:
        return True
    normal = conn.execute("SELECT 1 FROM jobs j WHERE j.batch_id=? AND j.version=(SELECT MAX(version) FROM jobs WHERE batch_id=j.batch_id AND task_id=j.task_id) AND j.status IN ('pending','waiting_quota','running') AND NOT EXISTS(SELECT 1 FROM recovery_queue q WHERE q.job_id=j.id) LIMIT 1", (job["batch_id"],)).fetchone()
    if normal is not None:
        return False
    head = conn.execute("SELECT q.job_id FROM recovery_queue q JOIN jobs j ON j.id=q.job_id WHERE j.batch_id=? AND j.status IN ('pending','waiting_quota') AND j.version=(SELECT MAX(version) FROM jobs WHERE batch_id=j.batch_id AND task_id=j.task_id) ORDER BY q.seq LIMIT 1", (job["batch_id"],)).fetchone()
    if head is None or head["job_id"] != job["id"]:
        return False
    if (time.time() if timestamp is None else timestamp) < entry["not_before"]:
        return False
    settlement = conn.execute("SELECT * FROM recovery_settlements WHERE job_id=?", (entry["predecessor_job_id"],)).fetchone()
    return bool(settlement and settlement["decision"] == "queued" and settlement["successor_job_id"] == job["id"] and settlement["binding_sha256"] == entry["binding_sha256"])


def public(conn, job_id):
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('recovery_queue','recovery_settlements')")}
    if len(tables) != 2:
        return {"queue": None, "settlement": None}
    queue = conn.execute("SELECT * FROM recovery_queue WHERE job_id=?", (job_id,)).fetchone()
    settlement = conn.execute("SELECT * FROM recovery_settlements WHERE job_id=?", (job_id,)).fetchone()
    result = {"queue": dict(queue) if queue else None, "settlement": dict(settlement) if settlement else None}
    from . import recovery_watch
    result.update(recovery_watch.public(conn, job_id, queue))
    return result
