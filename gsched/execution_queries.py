"""Bounded, passive execution browsing; pagination is explicitly live."""
from __future__ import annotations

import base64
import hashlib
import json
import time

from . import execution_diagnostics, execution_state, state

PHASES = ("prepared", "launching", "running", "exited", "not_started", "unresolved", "reserved", "log_bound")
OWNER_STATES = ("unknown", "responsive", "unreachable", "lost", "not_applicable")


def _decode(value, filters, scope):
    if not isinstance(value, str) or not 1 <= len(value) <= 4096:
        raise ValueError("execution cursor must be a bounded string")
    try:
        raw = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
        cursor = json.loads(raw)
        if (set(cursor) != {"v", "filters", "scope", "after", "upper", "anchor"}
                or type(cursor["v"]) is not int or cursor["v"] != 1
                or cursor["filters"] != filters or cursor["scope"] != scope
                or type(cursor["after"]) is not int or type(cursor["upper"]) is not int
                or not 0 < cursor["after"] <= cursor["upper"] < 2**63
                or not isinstance(cursor["anchor"], str) or not cursor["anchor"]):
            raise ValueError("cursor binding mismatch")
        return cursor
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError) as error:
        raise ValueError("invalid execution cursor or changed filters/node/state") from error


def list_executions(conn, *, project=None, batch=None, backend=None, phase=None,
                    owner_status=None, limit=50, cursor=None):
    if type(limit) is not int or not 1 <= limit <= 500:
        raise ValueError("limit must be 1..500")
    filters = {"project": project, "batch_id": batch, "backend": backend,
               "phase": phase, "owner_status": owner_status}
    for key in ("project", "batch_id", "backend"):
        value = filters[key]
        if value is not None and (not isinstance(value, str) or not 1 <= len(value) <= 256):
            raise ValueError(key + " must be a nonempty bounded string")
    if phase is not None and phase not in PHASES:
        raise ValueError("invalid execution phase")
    if owner_status is not None and owner_status not in OWNER_STATES:
        raise ValueError("invalid owner status")
    scope = hashlib.sha256(state.db_path().encode("utf-8")).hexdigest()
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    schema = conn.execute("PRAGMA user_version").fetchone()[0]
    anchor = conn.execute("SELECT rowid,id FROM jobs ORDER BY rowid DESC LIMIT 1").fetchone()
    continuation = cursor is not None
    if continuation:
        binding = _decode(cursor, filters, scope)
        retained = conn.execute("SELECT id FROM jobs WHERE rowid=?", (binding["upper"],)).fetchone()
        if retained is None or retained[0] != binding["anchor"]:
            raise ValueError("execution cursor anchor disappeared; restart pagination")
    else:
        binding = {"v": 1, "filters": filters, "scope": scope, "after": 0,
                   "upper": anchor[0] if anchor else 0, "anchor": anchor[1] if anchor else ""}
    joins, fields = [], ["j.*", "j.rowid AS query_rowid", "b.name AS batch_name", "b.revision AS batch_revision", "b.mode AS batch_mode", "t.spec"]
    aliases = (("execution_attempts", "a", "job_id"), ("execution_owners", "o", "job_id"),
               ("execution_owner_operations", "h", "job_id"), ("native_sessions", "s", "job_id"))
    for table, alias, key in aliases:
        if table in tables:
            joins.append(f"LEFT JOIN {table} {alias} ON {alias}.{key}=j.id")
            # Keep schemas with missing optional historical columns readable.
            columns = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
            fields.extend(f"{alias}.{c} AS {alias}_{c}" for c in columns)
    where, params = ["j.rowid>?", "j.rowid<=?"], [binding["after"], binding["upper"]]
    for column, value in (("j.project", project), ("j.batch_id", batch)):
        if value is not None:
            where.append(column + "=?")
            params.append(value)
    if backend is not None:
        where.append("a.backend_id=?" if "execution_attempts" in tables else "0")
        if "execution_attempts" in tables:
            params.append(backend)
    phase_columns = [alias + ".phase" for table, alias in (("execution_attempts", "a"), ("native_sessions", "s")) if table in tables]
    if phase is not None:
        where.append("COALESCE(" + ",".join(phase_columns + ["NULL"]) + ")=?" if phase_columns else "0")
        if phase_columns:
            params.append(phase)
    health_column = "COALESCE(h.connection_status,'unknown')" if "execution_owner_operations" in tables else "'unknown'"
    owner_column = ("CASE WHEN o.job_id IS NULL THEN 'not_applicable' ELSE " + health_column + " END"
                    if "execution_owners" in tables else "'not_applicable'")
    fields.append(owner_column + " AS query_owner_status")
    if owner_status is not None:
        where.append(owner_column + "=?")
        params.append(owner_status)
    sql = ("SELECT " + ",".join(fields) + " FROM jobs j JOIN batches b ON b.id=j.batch_id"
           " LEFT JOIN tasks t ON t.batch_id=j.batch_id AND t.id=j.task_id AND t.version=j.version "
           + " ".join(joins) + " WHERE " + " AND ".join(where) + " ORDER BY j.rowid LIMIT ?")
    rows = conn.execute(sql, (*params, limit + 1)).fetchall()
    truncated = len(rows) > limit
    items = []
    for row in rows[:limit]:
        value = dict(row)
        def record(alias):
            return {k[len(alias) + 1:]: v for k, v in value.items() if k.startswith(alias + "_")}
        attempt = record("a")
        owner = record("o")
        health = record("h")
        legacy = record("s")
        if attempt.get("job_id") is not None:
            health_record = ({"source": "recorded", **{k: v for k, v in health.items() if k != "job_id"}}
                             if health.get("job_id") is not None else
                             {"source": "recorded", "connection_status": "unknown", "cleanup_state": "unknown",
                              **{k: None for k in ("last_observed_at", "cleanup_attempts", "retry_after", "last_cleanup_at",
                                                  "cleanup_error", "acknowledged_at", "acknowledgement")}})
            attempt = execution_state.public(attempt,
                owner_binding=json.loads(owner["binding"]) if owner.get("job_id") is not None else None,
                owner_health_record=health_record)
        else:
            attempt = None
        legacy = ({k: legacy.get(k) for k in ("session_id", "job_id", "job_version", "phase", "owner_kind", "created_at",
                                              "log_attempted_at", "log_bound_at", "monitor_launch_attempted_at")}
                  if legacy.get("job_id") is not None else None)
        items.append({"batch_id": row["batch_id"], "batch_name": row["batch_name"],
                      "batch_revision": row["batch_revision"], "project": row["project"],
                      "task_id": row["task_id"], "job_id": row["id"], "job_version": row["version"],
                      "owner_status": row["query_owner_status"], "attempt": attempt, "legacy_session": legacy,
                      "diagnostics": execution_diagnostics.summarize(row, row["spec"], row["batch_mode"], attempt, legacy)})
    next_cursor = None
    if truncated:
        binding["after"] = rows[limit - 1]["query_rowid"]
        next_cursor = base64.urlsafe_b64encode(json.dumps(binding, separators=(",", ":")).encode()).decode().rstrip("=")
    return {"schema_version": 1, "query": "execution_list", "database_schema": schema,
            "observed_at": time.time(), "consistency": "live", "complete": not continuation and not truncated,
            "filters": filters, "limit": limit, "items": items, "truncated": truncated, "next_cursor": next_cursor}
