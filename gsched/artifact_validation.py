"""Immutable artifact observations, not a scientific or execution authority.

Only dispatcher completion paths create initial records. Historical job.rc and
current files cannot manufacture an original wait. Queries never inspect files.
"""
from __future__ import annotations

import json
import re

from . import state
from .execution_policy import digest
from .integration import instance_id

MAX_RECORD_BYTES = 4 * 1024 * 1024
SCHEMA = """
CREATE TABLE IF NOT EXISTS artifact_validations (
 validation_id TEXT PRIMARY KEY,
 completion_key TEXT NOT NULL UNIQUE,
 job_id TEXT NOT NULL REFERENCES jobs(id),
 job_version INTEGER NOT NULL CHECK(job_version > 0),
 instance_id TEXT NOT NULL,
 batch_revision INTEGER NOT NULL CHECK(batch_revision >= 0),
 context TEXT NOT NULL CHECK(context IN ('exit_zero','exit_nonzero','probe_ready')),
 spec_sha256 TEXT NOT NULL,
 rules_sha256 TEXT NOT NULL,
 wait_sha256 TEXT NOT NULL,
 evidence_sha256 TEXT NOT NULL,
 passed INTEGER NOT NULL CHECK(passed IN (0,1)),
 wait_verified INTEGER NOT NULL CHECK(wait_verified IN (0,1)),
 payload TEXT NOT NULL,
 observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS artifact_validation_job ON artifact_validations(job_id,validation_id);
CREATE TRIGGER IF NOT EXISTS artifact_validation_immutable BEFORE UPDATE ON artifact_validations
 BEGIN SELECT RAISE(ABORT,'artifact validation cannot be rewritten'); END;
CREATE TRIGGER IF NOT EXISTS artifact_validation_retained BEFORE DELETE ON artifact_validations
 BEGIN SELECT RAISE(ABORT,'artifact validation history is retained'); END;
"""


def available(conn):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='artifact_validations'").fetchone() is not None


def rules_snapshot(spec):
    groups = [{"scope": "task", "artifacts": spec.get("artifacts", {}),
               "paths_escape": spec.get("paths_escape", False)}]
    for index, stage in enumerate(spec.get("stages") or []):
        groups.append({"scope": f"stage:{index}", "artifacts": stage.get("artifacts", {}),
                       "paths_escape": stage.get("paths_escape", False)})
    return groups


def completion_key(job, context):
    # Ordinary legacy retries reuse a version: retain each original observation
    # by its recorded retry counter and start, never overwrite the first failure.
    job = dict(job)
    return digest({"job_id": job["id"], "version": job["version"], "context": context,
                   "retries": job.get("retries", 0), "started_at": job.get("started_at")})


def prior_completion(conn, job, context):
    row = conn.execute("SELECT * FROM artifact_validations WHERE completion_key=?",
                       (completion_key(job, context),)).fetchone()
    return decode(row) if row is not None else None


def wait_snapshot(conn, job, rc, ordinary_wait=None):
    from .executor import _is_strong_start_token
    row = conn.execute("SELECT * FROM execution_attempts WHERE job_id=?", (job["id"],)).fetchone()
    if row is not None:
        observation = json.loads(row["observation"] or "null")
        verified = (row["job_version"] == job["version"] and row["phase"] == "exited"
                    and isinstance(observation, dict) and observation.get("status") == "exited"
                    and observation.get("group_clean") is True and type(rc) is int
                    and type(observation.get("pid")) is int and observation["pid"] > 0
                    and observation["pid"] == job["pgid"]
                    and type(observation.get("returncode")) is int and observation["returncode"] == rc)
        return {"source": "execution_attempt", "verified": verified, "attempt_id": row["attempt_id"],
                "backend_id": row["backend_id"], "backend_config_sha256": row["backend_config_sha256"],
                "identity_sha256": digest(json.loads(row["identity"])), "phase": row["phase"],
                "observation": observation}
    if ordinary_wait is not None:
        # This is supplied only from an actual local supervisor Popen poll. It
        # identifies the wrapper/command-chain wait, never a scientific worker.
        fact = dict(ordinary_wait)
        fact["verified"] = (fact.get("source") == "local_supervisor_wait"
                            and fact.get("binding_verified") is True and fact.get("group_clean") is True
                            and type(rc) is int and type(fact.get("pid")) is int and fact["pid"] > 0
                            and fact["pid"] == job["pgid"] and _is_strong_start_token(fact.get("start_token"))
                            and fact.get("subject") == "scheduler_supervisor_command_chain"
                            and type(fact.get("returncode")) is int and fact["returncode"] == rc)
        return fact
    return {"source": "legacy_settlement", "verified": False, "recorded_rc": rc,
            "observation": None, "reason": "original_wait_unavailable"}


def record_initial(conn, job, spec, context, checks, *, rc, ordinary_wait=None):
    """Append within the same writer transaction as the original settlement."""
    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")
    if context not in ("exit_zero", "exit_nonzero", "probe_ready"):
        raise state.StateError("invalid artifact completion context")
    current = state.get_job(conn, job["id"])
    task = conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
                        (job["batch_id"], job["task_id"], job["version"])).fetchone()
    if (current is None or task is None or current["status"] != "running"
            or any(current[key] != job[key] for key in ("batch_id", "task_id", "version", "started_at", "retries"))
            or current["rc"] != rc or json.loads(task["spec"]) != spec):
        raise state.StateError("artifact completion binding changed")
    old = prior_completion(conn, job, context)
    if old is not None:
        if old["spec_sha256"] != digest(spec) or old["payload"]["recorded_rc"] != rc:
            raise state.StateError("artifact completion differs from original observation")
        return old
    rules = rules_snapshot(spec)
    wait = wait_snapshot(conn, current, rc, ordinary_wait)
    batch = state.get_batch(conn, current["batch_id"])
    instance = instance_id(conn)
    if instance is None:
        raise state.StateError("artifact validation requires persistent instance identity")
    payload = {"schema_version": 1, "instance_id": instance, "job_id": current["id"],
               "batch_id": current["batch_id"], "task_id": current["task_id"], "version": current["version"],
               "retries": current["retries"], "started_at": current["started_at"],
               "fingerprint": current["fingerprint"], "context": context, "spec_sha256": digest(spec),
               "recorded_rc": rc, "kill_reason": current["kill_reason"], "wait": wait,
               "rules": rules, "checks": checks, "passed": all(item["passed"] for item in checks),
               "observed_at": state.now(), "batch_revision": batch["revision"]}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    if len(encoded.encode()) > MAX_RECORD_BYTES:
        raise state.StateError("artifact validation exceeds bounded record size; settlement retained")
    identifier = digest(payload)
    conn.execute("INSERT INTO artifact_validations"
                 " (validation_id,completion_key,job_id,job_version,instance_id,batch_revision,context,"
                 " spec_sha256,rules_sha256,wait_sha256,evidence_sha256,passed,wait_verified,payload,observed_at)"
                 " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 (identifier, completion_key(current, context), current["id"], current["version"], instance,
                  batch["revision"], context, digest(spec), digest(rules), digest(wait), digest(checks),
                  int(payload["passed"]), int(wait["verified"]), encoded, payload["observed_at"]))
    return decode(conn.execute("SELECT * FROM artifact_validations WHERE validation_id=?", (identifier,)).fetchone())


def decode(row, *, include_payload=True):
    result = dict(row)
    result["passed"] = bool(result["passed"])
    result["wait_verified"] = bool(result["wait_verified"])
    result.pop("completion_key", None)
    if include_payload:
        try:
            payload = json.loads(result["payload"])
            required = {"schema_version", "spec_sha256", "rules", "wait", "checks", "job_id", "version",
                        "instance_id", "batch_revision", "context", "observed_at", "passed", "recorded_rc"}
            if (not isinstance(payload, dict) or not required <= payload.keys()
                    or payload["schema_version"] != 1 or not isinstance(payload["wait"], dict)
                    or type(payload["wait"].get("verified")) is not bool or type(payload["passed"]) is not bool):
                raise ValueError("invalid record structure")
        except (ValueError, TypeError, RecursionError) as error:
            raise state.StateError("artifact validation payload is invalid") from error
        if digest(payload) != result["validation_id"]:
            raise state.StateError("artifact validation evidence digest mismatch")
        if (result["spec_sha256"] != payload["spec_sha256"]
                or result["rules_sha256"] != digest(payload["rules"])
                or result["wait_sha256"] != digest(payload["wait"])
                or result["evidence_sha256"] != digest(payload["checks"])
                or result["job_id"] != payload["job_id"] or result["job_version"] != payload["version"]
                or result["instance_id"] != payload["instance_id"]
                or result["batch_revision"] != payload["batch_revision"]
                or result["context"] != payload["context"] or result["observed_at"] != payload["observed_at"]
                or result["passed"] != payload["passed"] or result["wait_verified"] != payload["wait"]["verified"]):
            raise state.StateError("artifact validation binding digest mismatch")
        result["payload"] = payload
    else:
        result.pop("payload", None)
    return result


def list_records(conn, batch, task, *, version=None, limit=20, cursor=None, validation_id=None):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("limit must be 1..100")
    if version is not None and (type(version) is not int or version < 1):
        raise ValueError("version must be positive")
    for value in (cursor, validation_id):
        if value is not None and (not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None):
            raise ValueError("validation ID/cursor must be a SHA-256 identifier")
    if cursor and validation_id:
        raise ValueError("validation-id and cursor are mutually exclusive")
    if not available(conn):
        return {"available": False, "reason": "migration_required", "validations": [], "truncated": False,
                "next_cursor": None, "effect": "none"}
    clauses, params = ["j.batch_id=?", "j.task_id=?"], [batch, task]
    if version is not None:
        clauses.append("v.job_version=?")
        params.append(version)
    if cursor:
        clauses.append("v.validation_id>?")
        params.append(cursor)
    if validation_id:
        clauses.append("v.validation_id=?")
        params.append(validation_id)
    fields = "v.*" if validation_id else ",".join("v." + name for name in (
        "validation_id", "job_id", "job_version", "instance_id", "batch_revision", "context", "spec_sha256",
        "rules_sha256", "wait_sha256", "evidence_sha256", "passed", "wait_verified", "observed_at"))
    rows = conn.execute("SELECT " + fields + " FROM artifact_validations v JOIN jobs j ON j.id=v.job_id WHERE "
                        + " AND ".join(clauses) + " ORDER BY v.validation_id LIMIT ?", (*params, limit + 1)).fetchall()
    if validation_id and not rows:
        raise ValueError("validation not found for exact task/version")
    truncated = len(rows) > limit
    values = [decode(row, include_payload=bool(validation_id)) for row in rows[:limit]]
    return {"available": True, "reason": None, "validations": values, "truncated": truncated,
            "next_cursor": values[-1]["validation_id"] if truncated else None, "effect": "none",
            "ordering": "validation_id", "pagination": "live_keyset_not_complete_snapshot"}
