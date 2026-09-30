"""Read-only summaries of recorded facts; never reconstruct execution authority."""
from __future__ import annotations

import json

from ._legacy_execution import NATIVE_EXEC_ALL_INTERNAL_FIELDS
from .execution_policy import INTERNAL_FIELD


def legacy_session(conn, job_id: str) -> dict | None:
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='native_sessions'"
    ).fetchone()
    if not exists:
        return None
    row = conn.execute("SELECT * FROM native_sessions WHERE job_id=?", (job_id,)).fetchone()
    if row is None:
        return None
    # Older read-only snapshots lack the launch-intent columns. Do not migrate.
    value = dict(row)
    fields = ("session_id", "job_id", "job_version", "phase", "owner_kind",
              "created_at", "log_attempted_at", "log_bound_at", "monitor_launch_attempted_at")
    return {key: value.get(key) for key in fields}


def summarize(job, spec_text, batch_mode, attempt, legacy) -> dict:
    try:
        spec = json.loads(spec_text)
        if not isinstance(spec, dict):
            raise ValueError("invalid task spec")
    except (ValueError, TypeError):
        spec = None
    retired = (legacy is not None or batch_mode == "strict"
               or bool(NATIVE_EXEC_ALL_INTERNAL_FIELDS.intersection(spec or {})))
    kind = ("generic" if attempt is not None else "legacy" if retired else
            "unknown" if spec is None else "generic" if
            INTERNAL_FIELD in spec or "execution" in spec else "subprocess")
    observation = (attempt or {}).get("observation") or {}
    observed_status = observation.get("status")
    wait_available = (observed_status in ("exited", "cleanup_pending")
                      and type(observation.get("returncode")) is int)
    clean = observation.get("group_clean")
    cleanup = "confirmed" if clean is True else "pending" if clean is False else "unknown"
    phase = ((attempt or {}).get("phase") if attempt else
             legacy["phase"] if legacy else "retired" if retired else
             "not_reserved" if kind == "generic" else None)
    uncertainty = ("legacy_wait_unavailable" if retired else
                   "record_invalid" if spec is None else
                   "owner_unreachable" if observation.get("owner_unreachable") is True else
                   "owner_authority_lost" if phase == "unresolved" else None)
    return {
        "job_id": job["id"], "job_version": job["version"], "job_status": job["status"],
        "execution_kind": kind, "phase": phase,
        "attempt_id": (attempt or {}).get("attempt_id"),
        "legacy_session_id": (legacy or {}).get("session_id"),
        "cancel_reason": (attempt or {}).get("cancel_reason"),
        "job_kill_reason": job["kill_reason"], "job_failure": job["failure"],
        "observation_status": observed_status,
        "wait_result_available": wait_available,
        "returncode": observation.get("returncode") if wait_available else None,
        "rusage": observation.get("rusage") if wait_available else None,
        "launch_error": observation.get("launch_error"),
        "cleanup_state": cleanup, "uncertainty_reason": uncertainty,
        "replay_blocked": attempt is not None or retired or spec is None,
    }
