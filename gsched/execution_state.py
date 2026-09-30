"""Durable generic execution attempts. Only the scheduler writes this state."""
from __future__ import annotations

import json
import os
import secrets
import sqlite3

from . import state
from .execution_policy import IDENTITY_SCHEMA, INTERFACE_VERSION, canonical_bytes

SCHEMA = """
CREATE TABLE IF NOT EXISTS execution_attempts (
  attempt_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
  job_version INTEGER NOT NULL CHECK(job_version > 0),
  backend_id TEXT NOT NULL,
  backend_config_sha256 TEXT NOT NULL,
  phase TEXT NOT NULL CHECK(phase IN ('prepared','launching','running','exited','not_started','unresolved')),
  identity TEXT NOT NULL,
  observation TEXT,
  cancel_reason TEXT,
  created_at TEXT NOT NULL,
  launch_intent_at TEXT,
  finished_at TEXT
);
CREATE TRIGGER IF NOT EXISTS execution_attempt_identity_immutable
BEFORE UPDATE ON execution_attempts
WHEN NEW.attempt_id != OLD.attempt_id OR NEW.job_id != OLD.job_id
  OR NEW.job_version != OLD.job_version OR NEW.backend_id != OLD.backend_id
  OR NEW.backend_config_sha256 != OLD.backend_config_sha256 OR NEW.identity != OLD.identity
  OR (OLD.launch_intent_at IS NOT NULL AND NEW.launch_intent_at IS NOT OLD.launch_intent_at)
BEGIN SELECT RAISE(ABORT, 'execution attempt identity and launch intent are immutable'); END;
CREATE TRIGGER IF NOT EXISTS execution_attempt_terminal_immutable
BEFORE UPDATE ON execution_attempts
WHEN OLD.phase IN ('exited','not_started') AND (
  NEW.phase != OLD.phase OR NEW.observation IS NOT OLD.observation
  OR NEW.finished_at IS NOT OLD.finished_at)
BEGIN SELECT RAISE(ABORT, 'terminal execution observation is immutable'); END;
"""


def get(conn: sqlite3.Connection, job_id: str):
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_attempts'").fetchone()
    if not exists:
        return None
    return conn.execute("SELECT * FROM execution_attempts WHERE job_id=?", (job_id,)).fetchone()


def _current_authorization(conn, job_id: str):
    job = conn.execute(
        "SELECT j.*, b.status AS batch_status FROM jobs j JOIN batches b ON b.id=j.batch_id"
        " WHERE j.id=? AND j.version=(SELECT MAX(v.version) FROM jobs v"
        " WHERE v.batch_id=j.batch_id AND v.task_id=j.task_id)", (job_id,)).fetchone()
    cancellation = conn.execute("SELECT 1 FROM control_requests WHERE job_id=? AND op='cancel' AND status='pending'", (job_id,)).fetchone()
    if job is None or job["status"] != "running" or job["batch_status"] != "active" or job["kill_reason"] or cancellation:
        raise state.StateError("execution launch no longer authorized by current task state")
    return job


def reserve(conn, job_id: str, binding: dict, profile: dict, node: str, scheduler_start_ticks: int) -> dict:
    job = _current_authorization(conn, job_id)
    if get(conn, job_id) is not None:
        raise state.StateError("execution attempt already consumed; no replay is permitted")
    attempt_id = secrets.token_hex(16)
    identity = {
        "schema": IDENTITY_SCHEMA, "interface_version": INTERFACE_VERSION,
        "attempt_id": attempt_id, "job_id": job_id, "batch_id": job["batch_id"],
        "task_id": job["task_id"], "version": job["version"], "node": node,
        "scheduler_pid": os.getpid(), "scheduler_start_ticks": scheduler_start_ticks,
        "backend_id": binding["backend_id"], "backend_sha256": profile["sha256"],
        "backend_config_sha256": binding["backend_config_sha256"],
    }
    conn.execute("INSERT INTO execution_attempts (attempt_id,job_id,job_version,backend_id,backend_config_sha256,phase,identity,created_at) VALUES(?,?,?,?,?,'prepared',?,?)",
                 (attempt_id, job_id, job["version"], binding["backend_id"], binding["backend_config_sha256"], canonical_bytes(identity).decode(), state.now()))
    return identity


def launch_intent(conn, job_id: str) -> None:
    _current_authorization(conn, job_id)
    cursor = conn.execute("UPDATE execution_attempts SET phase='launching',launch_intent_at=? WHERE job_id=? AND phase='prepared' AND launch_intent_at IS NULL",
                          (state.now(), job_id))
    if cursor.rowcount != 1:
        raise state.StateError("execution launch intent already consumed")


def observe(conn, job_id: str, observation: dict, *, phase: str | None = None) -> None:
    phases = {"running": "running", "cleanup_pending": "running", "exited": "exited",
              "not_started": "not_started", "authority_lost": "unresolved"}
    observed_phase = phases.get(observation.get("status"))
    if observed_phase is None or (phase is not None and phase != observed_phase):
        raise state.StateError("invalid execution observation phase")
    phase = observed_phase
    terminal = phase in ("exited", "not_started")
    if terminal and observation.get("group_clean") is not True:
        raise state.StateError("terminal execution requires proven process-group cleanup")
    if phase == "exited" and type(observation.get("returncode")) is not int:
        raise state.StateError("exited execution requires the original wait result")
    row = get(conn, job_id)
    if row is None:
        raise state.StateError("execution attempt does not exist")
    encoded = canonical_bytes(observation).decode()
    if row["phase"] in ("exited", "not_started"):
        if row["phase"] != phase or row["observation"] != encoded:
            raise state.StateError("terminal execution observation cannot be rewritten")
        return
    conn.execute("UPDATE execution_attempts SET phase=?,observation=?,finished_at=? WHERE job_id=?",
                 (phase, encoded, state.now() if terminal else None, job_id))


def cancel_intent(conn, job_id: str, reason: str) -> None:
    conn.execute("UPDATE execution_attempts SET cancel_reason=COALESCE(cancel_reason,?) WHERE job_id=?", (reason, job_id))


def public(row) -> dict:
    value = dict(row)
    value["identity"] = json.loads(value["identity"])
    value["observation"] = json.loads(value["observation"]) if value["observation"] else None
    return value
