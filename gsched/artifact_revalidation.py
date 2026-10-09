"""CAS-bound artifact-only observations and settlement, never job execution."""
from __future__ import annotations

from contextvars import ContextVar
import errno
import hashlib
import json
import os
import re
import time

from . import artifacts, artifact_validation as initial, state
from .execution_policy import digest
from .integration import canonical, instance_id

request_id = ContextVar("artifact_revalidation_request_id", default=None)
MAX_RULES = 32
BUDGET_SEC = 4.0
SCHEMA = """
CREATE TABLE IF NOT EXISTS artifact_revalidations (
 event_id TEXT PRIMARY KEY,
 request_id TEXT NOT NULL UNIQUE REFERENCES operation_requests(request_id),
 initial_validation_id TEXT NOT NULL REFERENCES artifact_validations(validation_id),
 job_id TEXT NOT NULL REFERENCES jobs(id),
 job_version INTEGER NOT NULL CHECK(job_version>0),
 passed INTEGER NOT NULL CHECK(passed IN (0,1)),
 settled INTEGER NOT NULL CHECK(settled IN (0,1)),
 reason TEXT NOT NULL,
 payload TEXT NOT NULL,
 observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS artifact_revalidation_job ON artifact_revalidations(job_id,event_id);
CREATE TRIGGER IF NOT EXISTS artifact_revalidation_immutable BEFORE UPDATE ON artifact_revalidations
 BEGIN SELECT RAISE(ABORT,'artifact revalidation cannot be rewritten'); END;
CREATE TRIGGER IF NOT EXISTS artifact_revalidation_retained BEFORE DELETE ON artifact_revalidations
 BEGIN SELECT RAISE(ABORT,'artifact revalidation history is retained'); END;
"""


def available(conn):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='artifact_revalidations'").fetchone() is not None


def group_absent(pid):
    """No PID/log inference of wait: this is an additional current safety check."""
    if type(pid) is not int or pid <= 0:
        return False
    try:
        os.killpg(pid, 0)
    except ProcessLookupError:
        return True
    except OSError:
        return False
    return False


def source_binding(conn, job, spec, record):
    payload = record["payload"]
    if (record["instance_id"] != instance_id(conn) or record["job_id"] != job["id"]
            or record["job_version"] != job["version"] or record["spec_sha256"] != digest(spec)
            or record["rules_sha256"] != digest(initial.rules_snapshot(spec))
            or any(payload.get(k) != job[k] for k in ("batch_id", "task_id", "fingerprint", "retries", "started_at"))):
        raise ValueError("original validation binding changed")
    if payload.get("allocation_id") != dict(job).get("allocation_id"):
        raise ValueError("original allocation binding changed")
    if not isinstance(payload["checks"], list) or not 1 <= len(payload["checks"]) <= MAX_RULES:
        raise ValueError("artifact-only revalidation requires 1..32 original rule observations")


def authority_reason(conn, job, spec, record):
    p = record["payload"]
    if job["status"] not in ("failed", "blocked") or job["failure"] != "artifact" or record["passed"]:
        return "not_original_artifact_failure"
    if (job["rc"] != p["recorded_rc"] or job["kill_reason"] != p["kill_reason"]
            or not record["wait_verified"]):
        return "original_wait_unavailable"
    ordinary = p["context"] == "exit_zero" and type(p["recorded_rc"]) is int and p["recorded_rc"] == 0 and p["kill_reason"] is None
    ready = p["context"] == "probe_ready" and p["kill_reason"] == "probe_ready"
    if not (ordinary or ready):
        return "original_exit_not_successful"
    wait = p["wait"]
    if wait["source"] == "execution_attempt":
        if digest(initial.wait_snapshot(conn, job, job["rc"])) != record["wait_sha256"]:
            return "original_wait_changed"
        pid = wait["observation"].get("pid")
    elif wait["source"] == "local_supervisor_wait":
        if not initial.wait_snapshot(conn, job, job["rc"], wait)["verified"]:
            return "original_wait_changed"
        pid = wait.get("pid")
    else:
        return "original_wait_unavailable"
    # No execution/recovery/legacy attempt on any generation of this task may
    # be reinterpreted as absent merely because the latest row is terminal.
    rows = conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id=?", (job["batch_id"], job["task_id"])).fetchall()
    for row in rows:
        if row["status"] in ("running", "interrupted"):
            return "task_execution_unresolved"
        if conn.execute("SELECT 1 FROM execution_attempts WHERE job_id=? AND phase NOT IN ('exited','not_started')", (row["id"],)).fetchone():
            return "task_execution_unresolved"
        if conn.execute("SELECT 1 FROM native_sessions WHERE job_id=?", (row["id"],)).fetchone():
            return "legacy_execution_unsupported"
        # Same durable marker path as dispatcher; presence/uncertainty blocks,
        # and revalidation never removes an old marker or signals its process.
        prefix = hashlib.sha256(str(row["id"]).encode("utf-8")).hexdigest()[:24]
        marker = os.path.join(state.host_dir(), "launch", prefix + ".launch")
        try:
            os.stat(marker, follow_symlinks=False)
        except FileNotFoundError:
            pass
        except OSError:
            return "launch_state_unknown"
        else:
            return "launch_state_unresolved"
    if conn.execute("SELECT 1 FROM control_requests WHERE job_id=? AND status='pending' AND op='cancel'", (job["id"],)).fetchone():
        return "cancel_pending"
    if conn.execute("SELECT 1 FROM gpu_jobs WHERE job_id=?", (job["id"],)).fetchone():
        return "allocation_not_released"
    if any(group["paths_escape"] for group in p["rules"]):
        return "escaped_artifact_scope_unsupported"
    if not group_absent(pid):
        return "process_group_active_or_unknown"
    return None


def provenance_reason(original, current):
    index = {(c.get("scope"), c.get("name")): c for c in original}
    if len(index) != len(original) or set(index) != {(c.get("scope"), c.get("name")) for c in current}:
        return "rule_observations_changed"
    for check in current:
        old = index[check["scope"], check["name"]]
        if old.get("rule_sha256") != check.get("rule_sha256"):
            return "rule_digest_changed"
        # Content digests where available plus exact unchanged kernel identity.
        # Existence-only rules do not pretend to have a full-file content hash.
        identity = old.get("file_identity")
        if not isinstance(identity, dict) or set(identity) != {"device", "inode", "mtime_ns", "ctime_ns"}:
            return "original_file_evidence_unavailable"
        if identity != check.get("file_identity") or old.get("file_size") != check.get("file_size"):
            return "file_identity_changed"
        if old.get("sha256") is not None and old["sha256"] != check.get("sha256"):
            return "file_digest_changed"
        if old.get("sha256") is None and old.get("reason_code") != "passed":
            return "original_file_evidence_unavailable"
    return None


def transient(check):
    if check.get("reason_code") == "regex_timeout":
        return True
    codes = {errno.EAGAIN, errno.ENOMEM, errno.EMFILE, errno.ENFILE, errno.ETIMEDOUT, errno.ESTALE}
    return check.get("reason_code") in ("regex_child_start_error", "io_error") and check.get("errno") in codes


def observe(spec, *, system_retries):
    deadline = time.monotonic() + BUDGET_SEC
    attempts = []
    for index in range(system_retries + 1):
        checks = artifacts.inspect_declared_artifacts(spec, spec.get("cwd_abs") or ".", deadline=deadline)
        attempts.append({"index": index, "observed_at": state.now(), "checks": checks,
                         "passed": all(c["passed"] for c in checks)})
        failures = [c for c in checks if not c["passed"]]
        if not failures or not all(transient(c) for c in failures) or index == system_retries:
            break
        delay = 0.1 * (2 ** index)
        if time.monotonic() + delay >= deadline:
            break
        time.sleep(delay)
    return attempts


def publication_metadata(spec, checks):
    """Recheck identities without rerunning validators; never follow escapes."""
    observed = []
    deadline = time.monotonic() + 0.5
    for group in initial.rules_snapshot(spec):
        rules = {name: {"path": rule["path"], "min_bytes": 0} for name, rule in group["artifacts"].items()}
        values = artifacts.inspect_artifacts(rules, spec.get("cwd_abs") or ".", deadline=deadline)
        observed.extend({"scope": group["scope"], "name": name, **item} for name, item in values.items())
    by_key = {(item["scope"], item["name"]): item for item in observed}
    matches = all((item["scope"], item["name"]) in by_key
                  and by_key[item["scope"], item["name"]]["passed"]
                  and by_key[item["scope"], item["name"]]["file_identity"] == item.get("file_identity")
                  and by_key[item["scope"], item["name"]]["file_size"] == item.get("file_size") for item in checks)
    return matches, observed


def perform(conn, job, spec, record, *, settle=False, reopen=False, system_retries=0):
    rid = request_id.get()
    if not rid or not conn.in_transaction or not available(conn):
        raise state.StateError("revalidation requires an atomic bound request and schema 13")
    if type(system_retries) is not int or not 0 <= system_retries <= 2 or (reopen and not settle):
        raise ValueError("system-retries must be 0..2; reopen requires settle")
    source_binding(conn, job, spec, record)
    batch = state.get_batch(conn, job["batch_id"])
    if batch["mode"] != "mix" or batch["status"] == "discarded":
        raise ValueError("historical strict/discarded batches cannot be revalidated")
    reason = authority_reason(conn, job, spec, record)
    # A completed inspection is an audited business result even if validation
    # fails; request code 0 means the event committed, NOT job success.
    attempts = [] if reason else observe(spec, system_retries=system_retries)
    passed = bool(attempts and attempts[-1]["passed"])
    if reason is None:
        reason = provenance_reason(record["payload"]["checks"], attempts[-1]["checks"])
    if reason is None and not passed:
        reason = "artifact_rules_failed"
    # Recheck active groups/uncertain facts after reads, before publication.
    if reason is None:
        reason = authority_reason(conn, job, spec, record)
    metadata = []
    if reason is None:
        unchanged, metadata = publication_metadata(spec, attempts[-1]["checks"])
        if not unchanged:
            reason = "publication_file_identity_changed_or_unknown"
    settled = bool(settle and passed and reason is None)
    previous = {k: job[k] for k in ("status", "rc", "failure", "finished_at", "retries", "started_at", "kill_reason")}
    before_revision = batch["revision"]
    if settled:
        # Leave the original execution time and raw wait untouched. History
        # carries both the first failure and this explicitly requested decision.
        state.update_job(conn, job["id"], status="done", failure=None)
    reopened = False
    if settled and reopen and batch["status"] == "blocked":
        latest = conn.execute("SELECT j.status FROM jobs j WHERE batch_id=? AND version=(SELECT MAX(version) FROM jobs v WHERE v.batch_id=j.batch_id AND v.task_id=j.task_id)", (job["batch_id"],)).fetchall()
        if not any(j["status"] in ("failed", "blocked", "cancelled", "timed_out", "interrupted") for j in latest) and any(j["status"] in ("pending", "waiting_quota", "waiting_dep", "running") for j in latest):
            conn.execute("UPDATE batches SET status='active' WHERE id=?", (job["batch_id"],))
            reopened = True
    payload = {"schema_version": 1, "request_id": rid, "instance_id": instance_id(conn),
               "initial_validation_id": record["validation_id"], "job_id": job["id"],
               "batch_id": job["batch_id"], "task_id": job["task_id"], "version": job["version"],
               "original_spec_sha256": record["spec_sha256"], "original_rules_sha256": record["rules_sha256"],
               "original_wait_sha256": record["wait_sha256"], "original_evidence_sha256": record["evidence_sha256"],
               "previous": previous, "batch_revision_before": before_revision,
               "batch_revision_after": state.get_batch(conn, job["batch_id"])["revision"],
               "policy": {"system_retries": system_retries, "budget_sec": BUDGET_SEC, "max_rules": MAX_RULES},
               "attempts": attempts, "publication_metadata": metadata, "passed": passed, "settle_requested": settle,
               "settled": settled, "reopen_requested": reopen, "batch_reopened": reopened,
               "reason": reason or ("settled" if settled else "validated_without_settlement"), "observed_at": state.now()}
    encoded = canonical(payload)
    if len(encoded.encode()) > initial.MAX_RECORD_BYTES:
        raise state.StateError("revalidation record exceeds bounded size; no settlement committed")
    event = digest(payload)
    conn.execute("INSERT INTO artifact_revalidations VALUES (?,?,?,?,?,?,?,?,?,?)",
                 (event, rid, record["validation_id"], job["id"], job["version"], int(passed), int(settled),
                  payload["reason"], encoded, payload["observed_at"]))
    return {"event_id": event, **payload}


def decode(row, full=False):
    result = dict(row)
    result["passed"], result["settled"] = bool(result["passed"]), bool(result["settled"])
    if full:
        try:
            payload = json.loads(result["payload"])
            if digest(payload) != result["event_id"] or any(result[key] != payload[key] for key in
                    ("request_id", "initial_validation_id", "job_id", "passed", "settled", "reason", "observed_at")) or result["job_version"] != payload["version"]:
                raise ValueError("event binding mismatch")
        except (ValueError, TypeError, KeyError, RecursionError) as error:
            raise state.StateError("revalidation evidence is invalid") from error
        result["payload"] = payload
    else:
        result.pop("payload", None)
    return result


def list_records(conn, batch, task, *, version=None, limit=20, cursor=None, event_id=None):
    if type(limit) is not int or not 1 <= limit <= 100 or (version is not None and (type(version) is not int or version < 1)):
        raise ValueError("limit must be 1..100 and version positive")
    if any(value is not None and (not isinstance(value, str) or not re.fullmatch("[0-9a-f]{64}", value)) for value in (cursor, event_id)) or (cursor and event_id):
        raise ValueError("event ID/cursor must be SHA-256; detail and cursor are exclusive")
    if not available(conn):
        return {"available": False, "reason": "migration_required", "events": [], "truncated": False, "next_cursor": None, "effect": "none"}
    clauses, params = ["j.batch_id=?", "j.task_id=?"], [batch, task]
    for column, value in (("job_version", version), ("event_id", event_id)):
        if value is not None:
            clauses.append("e." + column + "=?")
            params.append(value)
    if cursor:
        clauses.append("e.event_id>?")
        params.append(cursor)
    fields = "e.*" if event_id else ",".join("e." + key for key in ("event_id", "request_id", "initial_validation_id", "job_id", "job_version", "passed", "settled", "reason", "observed_at"))
    rows = conn.execute("SELECT " + fields + " FROM artifact_revalidations e JOIN jobs j ON j.id=e.job_id WHERE " + " AND ".join(clauses) + " ORDER BY e.event_id LIMIT ?", (*params, limit + 1)).fetchall()
    if event_id and not rows:
        raise ValueError("event not found for exact task/version")
    truncated = len(rows) > limit
    values = [decode(row, full=bool(event_id)) for row in rows[:limit]]
    return {"available": True, "reason": None, "events": values, "truncated": truncated,
            "next_cursor": values[-1]["event_id"] if truncated else None, "effect": "none",
            "ordering": "event_id", "pagination": "live_keyset_not_complete_snapshot"}
