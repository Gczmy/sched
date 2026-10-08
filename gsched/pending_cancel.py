"""Bounded exact task facts and atomic, non-signalling pending-only cancellation."""
from __future__ import annotations

import hashlib
import json
import os
import re
from contextvars import ContextVar

from . import state
from .execution_policy import digest, IDENTITY_SCHEMA, INTERFACE_VERSION
from .integration import canonical
from ._legacy_execution import NATIVE_EXEC_ALL_INTERNAL_FIELDS

MAX_TASKS = 100
MAX_GENERATIONS = 1000
MAX_RECORDS = 10_000
MAX_BYTES = 4 * 1024 * 1024
MAX_INLINE = 64 * 1024
PENDING = {"pending", "waiting_dep", "waiting_quota"}
request_id = ContextVar("pending_cancel_request_id", default=None)
METADATA = {
    "execution_attempts": "job_id", "execution_owners": "job_id", "execution_owner_operations": "job_id",
    "native_sessions": "job_id", "gpu_jobs": "job_id", "control_requests": "job_id",
    "recovery_settlements": "job_id", "recovery_queue": "job_id", "recovery_watch": "root_job_id",
    "artifact_validations": "job_id", "artifact_revalidations": "job_id",
}


def normalize(raw, *, bindings=False):
    if not isinstance(raw, str) or len(raw.encode()) > MAX_INLINE:
        raise ValueError("tasks-json 必须是最多 64 KiB 的内联 JSON")
    items = json.loads(raw)
    if not isinstance(items, list) or not 1 <= len(items) <= MAX_TASKS:
        raise ValueError("tasks-json 必须显式列出 1..100 个任务，不接受通配或 all")
    fields = {"task_id", "version"}
    if bindings:
        fields |= {"job_id", "spec_sha256", "fingerprint", "history_sha256"}
    seen = set()
    for item in items:
        if (not isinstance(item, dict) or set(item) != fields or not isinstance(item["task_id"], str)
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", item["task_id"])
                or type(item["version"]) is not int or not 1 <= item["version"] <= 2**63 - 1
                or item["task_id"] in seen):
            raise ValueError("每个任务必须唯一并显式指定安全 task_id 和正整数 version")
        seen.add(item["task_id"])
        if bindings and (not isinstance(item["job_id"], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,511}", item["job_id"])
                or any(not isinstance(item[key], str) or not re.fullmatch("[0-9a-f]{64}", item[key]) for key in ("spec_sha256", "history_sha256"))
                or item["fingerprint"] is not None and (not isinstance(item["fingerprint"], str) or len(item["fingerprint"]) > 4096)):
            raise ValueError("取消清单必须冻结完整 job/spec/fingerprint/history 绑定")
    return items


def exact_batch(conn, batch_id):
    if not isinstance(batch_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}", batch_id):
        raise ValueError("需要完整安全 batch ID，不能按名称解析")
    batch = state.get_batch(conn, batch_id)
    if batch is None:
        raise ValueError("完整 batch ID 不存在")
    return batch


def _not_started(job, attempt, owner, operations):
    if attempt is None or attempt["phase"] != "not_started" or attempt["job_version"] != job["version"]:
        return False
    try:
        if any(len((attempt[key] or "").encode()) > MAX_INLINE for key in ("identity", "observation")):
            return False
        identity, observation = json.loads(attempt["identity"]), json.loads(attempt["observation"] or "null")
        if not isinstance(identity, dict) or not isinstance(observation, dict):
            return False
        if (identity.get("schema") != IDENTITY_SCHEMA or not isinstance(identity.get("interface_version"), str)
                or identity["interface_version"] != INTERFACE_VERSION or type(identity.get("version")) is not int
                or identity.get("node") != state.hostname()):
            return False
        if any(identity.get(key) != value for key, value in {
                "job_id": job["id"], "batch_id": job["batch_id"], "task_id": job["task_id"],
                "version": job["version"], "attempt_id": attempt["attempt_id"], "backend_id": attempt["backend_id"],
                "backend_config_sha256": attempt["backend_config_sha256"]}.items()):
            return False
        if (observation.get("status") != "not_started" or observation.get("group_clean") is not True
                or observation.get("pid") is not None or observation.get("returncode") is not None
                or attempt["cancel_reason"] or job["pgid"] is not None or job["rc"] is not None):
            return False
        if owner is not None and (operations is None or operations["cleanup_state"] != "acknowledged"
                                  or operations["acknowledgement"] != "closed"):
            return False
        return True
    except (ValueError, TypeError, KeyError, RecursionError):
        return False


def _unknown_requests(conn, batch, task, *, ignore_request_id=None):
    rows = conn.execute("SELECT * FROM operation_requests WHERE status='started' ORDER BY request_id LIMIT ?", (MAX_RECORDS + 1,)).fetchall()
    if len(rows) > MAX_RECORDS:
        raise ValueError("unknown 请求检查超过有界容量")
    relevant = []
    for row in rows:
        if row["request_id"] == ignore_request_id:
            continue
        if len((row["argv"] or "").encode()) > MAX_BYTES:
            raise ValueError("unknown 请求绑定超过字节容量")
        try:
            envelope = json.loads(row["argv"])
            expected = envelope["expect"]
            if not isinstance(expected, dict):
                raise ValueError("unknown expectation")
            kind, target = expected.get("kind"), expected.get("id")
            matched = ((kind == "batch" and target in {batch["id"], batch["name"]})
                       or (kind == "task" and target in {batch["id"] + ":" + task, batch["name"] + ":" + task})
                       or kind not in {"batch", "task", "none", "gpu"})
        except (ValueError, TypeError, KeyError, RecursionError):
            matched = True  # Unscoped historical intent cannot prove absence.
        if matched:
            relevant.append(dict(row))
    return relevant


def facts(conn, batch, selectors, *, ignore_request_id=None):
    """Private DB snapshot only; never probe a service, marker, PID or file."""
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    schema_version = conn.execute("PRAGMA user_version").fetchone()[0]
    complete_schema = schema_version >= 10 and state._schema_is_complete(conn, schema_version)
    total_records, total_bytes, result = 0, 0, []
    for selected in selectors:
        params = (batch["id"], selected["task_id"])
        jobs = [dict(row) for row in conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id=? ORDER BY version LIMIT ?", (*params, MAX_GENERATIONS + 1))]
        specs = [dict(row) for row in conn.execute("SELECT * FROM tasks WHERE batch_id=? AND id=? ORDER BY version LIMIT ?", (*params, MAX_GENERATIONS + 1))]
        if not jobs or len(jobs) > MAX_GENERATIONS or len(specs) > MAX_GENERATIONS:
            raise ValueError("任务不存在或代际记录超过 1000 上限，不能视为完整历史")
        found = next((job for job in jobs if job["version"] == selected["version"]), None)
        selected_spec = next((spec for spec in specs if spec["version"] == selected["version"]), None)
        if found is None or selected_spec is None:
            raise ValueError("精确 task/version 不存在")
        metadata = {}
        for table, key in METADATA.items():
            metadata[table] = [dict(row) for row in conn.execute(
                f"SELECT t.* FROM {table} t JOIN jobs j ON j.id=t.{key} WHERE j.batch_id=? AND j.task_id=? ORDER BY t.rowid LIMIT ?",
                (*params, MAX_RECORDS + 1))] if table in tables else []
        metadata["unknown_operation_requests"] = _unknown_requests(conn, batch, selected["task_id"], ignore_request_id=ignore_request_id) if "operation_requests" in tables else []
        total_records += len(jobs) + len(specs) + sum(len(rows) for rows in metadata.values())
        if total_records > MAX_RECORDS:
            raise ValueError("精确事实记录超过 10000 上限，不能视为完整历史")
        footprint = {"jobs": jobs, "tasks": specs, "metadata": metadata}
        encoded = canonical(footprint)
        total_bytes += len(encoded.encode())
        if total_bytes > MAX_BYTES:
            raise ValueError("精确事实超过 4 MiB 上限")
        spec_by_version = {spec["version"]: json.loads(spec["spec"]) for spec in specs}
        if any(not isinstance(spec, dict) for spec in spec_by_version.values()):
            raise ValueError("历史任务 spec 非法")
        attempts = {row["job_id"]: row for row in metadata["execution_attempts"]}
        owners = {row["job_id"]: row for row in metadata["execution_owners"]}
        operations = {row["job_id"]: row for row in metadata["execution_owner_operations"]}
        known_not_started, generations, reasons = set(), [], []
        if not complete_schema:
            reasons.append("historical_schema_without_complete_identity_contract")
        if batch["mode"] != "mix":
            reasons.append("legacy_execution_batch")
        if any(job["version"] != index + 1 for index, job in enumerate(jobs)) or set(spec_by_version) != {job["version"] for job in jobs}:
            reasons.append("generation_history_incomplete")
        for job in jobs:
            attempt = attempts.get(job["id"])
            proof = _not_started(job, attempt, owners.get(job["id"]), operations.get(job["id"]))
            if proof:
                known_not_started.add(job["id"])
            if job["status"] in {"running", "interrupted", "done", "skip", "timed_out"}:
                reasons.append("historical_execution_state:" + job["id"])
            if attempt is not None and not proof:
                reasons.append("execution_attempt_not_proven_never_started:" + job["id"])
            if not proof and (job["started_at"] is not None or job["pgid"] is not None or job["rc"] is not None or job["retries"]):
                reasons.append("historical_start_or_wait:" + job["id"])
            if job["status"] in {"failed", "blocked"} and not proof:
                reasons.append("historical_failure_without_not_started_authority:" + job["id"])
            if NATIVE_EXEC_ALL_INTERNAL_FIELDS.intersection(spec_by_version.get(job["version"], {})):
                reasons.append("legacy_execution_metadata:" + job["id"])
            generations.append({"job_id": job["id"], "version": job["version"], "status": job["status"],
                                "started_at": job["started_at"], "pgid": job["pgid"], "rc": job["rc"], "retries": job["retries"],
                                "execution_phase": attempt["phase"] if attempt else None,
                                "launch_intent_at": attempt["launch_intent_at"] if attempt else None,
                                "original_not_started_verified": proof,
                                "spec_sha256": digest(spec_by_version.get(job["version"]))})
        for table in ("native_sessions", "gpu_jobs"):
            if metadata[table]:
                reasons.append(table + "_present")
        if any(row["status"] == "pending" for row in metadata["control_requests"]):
            reasons.append("control_request_pending")
        if metadata["unknown_operation_requests"]:
            reasons.append("historical_operation_result_unknown")
        if set(owners) - known_not_started:
            reasons.append("owner_without_not_started_and_closed_authority")
        if any(row["job_id"] not in known_not_started for row in metadata["artifact_validations"]):
            reasons.append("prior_artifact_validation")
        if metadata["artifact_revalidations"]:
            reasons.append("prior_artifact_revalidation")
        if any(row["authority"] != "not_started" or row["job_id"] not in known_not_started or row["decision"] == "pending" for row in metadata["recovery_settlements"]):
            reasons.append("recovery_authority_or_intent_unresolved")
        if any(row["predecessor_job_id"] not in known_not_started for row in metadata["recovery_queue"]):
            reasons.append("recovery_predecessor_not_proven_never_started")
        if any(row["root_job_id"] not in known_not_started for row in metadata["recovery_watch"]):
            reasons.append("recovery_root_not_proven_never_started")
        if found["version"] != jobs[-1]["version"]:
            reasons.append("selected_version_not_latest")
        if found["status"] not in PENDING:
            reasons.append("selected_status_not_pending")
        binding = {"task_id": selected["task_id"], "version": selected["version"], "job_id": found["id"],
                   "spec_sha256": digest(spec_by_version[selected["version"]]), "fingerprint": found["fingerprint"],
                   "history_sha256": digest(footprint)}
        result.append({"binding": binding, "status": found["status"], "latest_version": jobs[-1]["version"],
                       "generations": generations, "metadata_counts": {table: len(rows) for table, rows in metadata.items()},
                       "recorded_never_started": not reasons, "refusal_reasons": sorted(set(reasons))})
    return result


def check_local_files(records, batch_id):
    """Presence is a refusal, never an inference of wait or successful cleanup."""
    prefixes = set()
    for record in records:
        for job in record["generations"]:
            prefix = hashlib.sha256(job["job_id"].encode()).hexdigest()[:24]
            prefixes.add(prefix)
            paths = [state.launch_marker_path(job["job_id"]),
                     os.path.join(state.host_dir(), "profiles", job["job_id"] + ".json")]
            if not job["original_not_started_verified"]:
                paths.append(os.path.join(state.host_dir(), "logs", batch_id, f"{record['binding']['task_id']}-v{job['version']}.log"))
            for path in paths:
                try:
                    os.lstat(path)
                except FileNotFoundError:
                    continue
                except OSError as error:
                    raise ValueError("startup_file_state_unknown:" + job["job_id"]) from error
                else:
                    raise ValueError("startup_file_present:" + job["job_id"])
    try:
        with os.scandir(os.path.join(state.host_dir(), "rc")) as entries:
            for index, entry in enumerate(entries):
                if index >= 100_000:
                    raise ValueError("rc_scan_incomplete")
                if entry.name[:24] in prefixes and entry.name[24:25] == "-" and entry.name.endswith(".rc"):
                    raise ValueError("historical_rc_file_present")
    except FileNotFoundError:
        pass
    except OSError as error:
        raise ValueError("rc_directory_unknown") from error


def perform(conn, batch_id, selectors):
    if not conn.in_transaction or request_id.get() is None:
        raise state.StateError("pending-only cancellation requires the original CAS request transaction")
    batch = exact_batch(conn, batch_id)
    if batch["status"] not in {"queued", "active", "blocked"}:
        raise ValueError("终态或退役批次不能成组取消")
    records = facts(conn, batch, selectors, ignore_request_id=request_id.get())
    for selected, record in zip(selectors, records):
        if selected != record["binding"]:
            raise ValueError("task_spec_or_history_binding_changed:" + selected["task_id"])
        if not record["recorded_never_started"]:
            raise ValueError("task_not_proven_never_started:" + selected["task_id"] + ":" + ",".join(record["refusal_reasons"]))
    check_local_files(records, batch_id)
    # All validation precedes the first DML; no control requests, signals,
    # artifact deletion, version creation or release calls are involved.
    for selected in selectors:
        changed = conn.execute("UPDATE jobs SET status='cancelled',kill_reason='cancelled',finished_at=?"
                               " WHERE id=? AND batch_id=? AND task_id=? AND version=?"
                               " AND status IN ('pending','waiting_dep','waiting_quota')",
                               (state.now(), selected["job_id"], batch_id, selected["task_id"], selected["version"])).rowcount
        if changed != 1:
            raise ValueError("pending_cancel_race")
    return {"cancelled_tasks": selectors, "task_binding_sha256": digest(selectors), "count": len(selectors),
            "running_tasks_touched": False, "signals_sent": False, "artifacts_deleted": False}
