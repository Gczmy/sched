"""Versioned, read-only integration facts; no customer or tracking dependencies."""
from __future__ import annotations

import contextlib
import copy
import hashlib
import io
import secrets
import json
import os
import re
import socket

from . import state

CONTRACTS = {
    "identity": "sched-identity-v1",
    "task": "sched-task-v1",
    "request_status": "sched-request-status-v1",
    "submit": "sched-submit-v1",
    "request_status_many": "sched-request-status-many-v1",
    "request_validation": "sched-request-validation-v1",
    "request_result": "sched-request-result-v1",
    "artifact_check": "sched-artifact-check-v1",
    "artifact_rules": "sched-artifact-rules-v2",
    "batch_policy": "sched-batch-policy-v1",
    "artifact_validations": "sched-artifact-validations-v1",
    "artifact_revalidations": "sched-artifact-revalidations-v1",
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False)


def request_identity(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", value):
        raise ValueError("invalid request ID")
    return value


def instance_id(conn):
    if conn.execute("PRAGMA user_version").fetchone()[0] < 10:
        return None
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='scheduler_identity'").fetchone() is None:
        raise state.StateError("scheduler identity is incomplete")
    row = conn.execute("SELECT instance_id FROM scheduler_identity WHERE singleton=1").fetchone()
    if row is None or not re.fullmatch(r"[0-9a-f]{32}", row[0]):
        raise state.StateError("scheduler identity is incomplete")
    return row[0]


def _database_present():
    try:
        os.stat(state.db_path(), follow_symlinks=False)
    except FileNotFoundError:
        return False
    return True


def identity():
    output = {"schema_version": 1, "query": "identity", "contract": CONTRACTS["identity"],
              "node": state.hostname(), "query_host": socket.gethostname(),
              "instance_id": None, "available": False, "reason": "state_unavailable"}
    if _database_present():
        with state.connect() as conn:
            output["instance_id"] = instance_id(conn)
        output["available"] = output["instance_id"] is not None
        output["reason"] = None if output["available"] else "migration_required"
    return output


def mutation_result(conn, command, code, kind, target, request_id=None):
    # Metadata from the same mutation transaction; never infer process effects.
    result = {"outcome": "acknowledged" if code == 0 else "rejected", "effect": None}
    if code == 0 and kind == "task":
        batch, task = target.split(":", 1)
        row = conn.execute("SELECT j.version, j.status, b.project, b.revision FROM jobs j JOIN batches b ON b.id=j.batch_id WHERE j.batch_id=? AND j.task_id=? ORDER BY j.version DESC LIMIT 1", (batch, task)).fetchone()
        if row:
            result["effect"] = {"batch_id": batch, "task": task, "version": row[0],
                                "status": row[1], "project": row[2], "batch_revision": row[3]}
            if command[0] == "artifact-revalidate":
                event = conn.execute("SELECT event_id,passed,settled,reason FROM artifact_revalidations WHERE request_id=?", (request_id,)).fetchone()
                if event is None:
                    raise state.StateError("revalidation request has no durable event")
                result["effect"].update(revalidation_id=event["event_id"], artifact_rules_passed=bool(event["passed"]),
                                        settled=bool(event["settled"]), reason=event["reason"])
    elif code == 0 and kind == "batch":
        row = conn.execute("SELECT project, revision, status FROM batches WHERE id=?", (target,)).fetchone()
        if row:
            result["effect"] = {"batch_id": target, "project": row[0], "batch_revision": row[1], "status": row[2]}
            if command[0] == "batch-policy":
                result["effect"]["failure_policy"] = conn.execute(
                    "SELECT failure_policy FROM batches WHERE id=?", (target,)).fetchone()[0]
    return canonical(result)


def _empty_request_status(request_id):
    return {"schema_version": 1, "query": "request_status", "contract": CONTRACTS["request_status"],
            "request_id": request_id, "instance_id": None, "found": False,
            "request_kind": None, "phase": "not_found", "code": None, "result": None,
            "output_compacted": False, "binding_sha256": None,
            "receipt_source": None, "receipt_persisted": False, "delivery_confirmed": None,
            "batch_persisted": None, "created_at": None, "finished_at": None,
            "observed_at": state.now(), "reason_code": "receipt_not_found"}


def request_status(request_id):
    return request_status_many([request_id])["requests"][0]


def request_status_many(request_ids):
    """One private DB/WAL snapshot; ticket fallback is explicitly separate."""
    if not isinstance(request_ids, list) or not 1 <= len(request_ids) <= 100:
        raise ValueError("request IDs must be a list of 1..100 items")
    for request_id in request_ids:
        request_identity(request_id)
    if len(set(request_ids)) != len(request_ids):
        raise ValueError("duplicate request IDs")
    outputs = [_empty_request_status(value) for value in request_ids]
    if _database_present():
        with state.connect() as conn:
            conn.execute("BEGIN")
            current_instance = instance_id(conn)
            columns = {row[1] for row in conn.execute("PRAGMA table_info(operation_requests)")}
            extra = "result_json" if "result_json" in columns else "NULL AS result_json"
            compacted = "output_compacted" if "output_compacted" in columns else "0 AS output_compacted"
            has_submissions = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='submission_requests'").fetchone()
            for output in outputs:
                request_id = output["request_id"]
                output["instance_id"] = current_instance
                row = None
                if columns:
                    row = conn.execute("SELECT status, code, " + compacted + ", argv, " + extra + ", created_at, finished_at FROM operation_requests WHERE request_id=?", (request_id,)).fetchone()
                if row:
                    output.update(found=True, request_kind="operation", phase="done" if row[0] == "done" else "unknown", code=row[1],
                                  output_compacted=bool(row[2]), binding_sha256=hashlib.sha256(row[3].encode()).hexdigest(),
                                  result=json.loads(row[4]) if row[4] is not None else None,
                                  receipt_source="database", receipt_persisted=True, created_at=row[5], finished_at=row[6],
                                  reason_code="request_settled" if row[0] == "done" else "outcome_unknown")
                if has_submissions:
                    submission = conn.execute("SELECT binding, code, result_json, created_at, finished_at FROM submission_requests WHERE request_id=?", (request_id,)).fetchone()
                    if submission:
                        if row:
                            raise state.StateError("request ID has conflicting namespaces")
                        binding = json.loads(submission[0])
                        result = json.loads(submission[2])
                        output.update(found=True, request_kind="submission", phase="done", code=submission[1],
                                      binding_sha256=hashlib.sha256(submission[0].encode()).hexdigest(),
                                      payload_sha256=binding["payload_sha256"], result=result,
                                      receipt_source="database", receipt_persisted=True,
                                      delivery_confirmed=True if result.get("persisted") is True else None,
                                      batch_persisted=result.get("persisted"), created_at=submission[3], finished_at=submission[4],
                                      reason_code="request_settled")
    for output in outputs:
        if not output["found"]:
            file_receipt(output["request_id"], output)
    return {"schema_version": 1, "query": "request_status_many", "contract": CONTRACTS["request_status_many"],
            "instance_id": outputs[0]["instance_id"], "requests": outputs, "observed_at": state.now(),
            "ticket_fallback_atomic": False}


def ticket_path(request_id):
    return os.path.join(state.host_dir(), "submission_requests", hashlib.sha256(request_identity(request_id).encode()).hexdigest() + ".json")


def load_ticket(request_id):
    path = ticket_path(request_id)
    try:
        os.stat(path, follow_symlinks=False)
    except FileNotFoundError:
        return None
    # This opens only scheduler-owned metadata, never DB/WAL/SHM files.
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0))
    import stat
    with os.fdopen(fd, "r", encoding="utf-8") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > 65536:
            raise state.StateError("invalid submission ticket file")
        ticket = json.load(stream)
    if ticket.get("request_id") != request_id:
        raise state.StateError("submission ticket identity changed")
    return ticket


def save_ticket(ticket):
    path = ticket_path(ticket["request_id"])
    parent = os.path.dirname(path)
    state.ensure_private_directory(parent)
    temp = path + "." + secrets.token_hex(12) + ".tmp"
    try:
        with state.open_private_text(temp, "x") as stream:
            stream.write(canonical(ticket))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if os.path.lexists(temp):
            os.unlink(temp)


def file_receipt(request_id, output):
    ticket = load_ticket(request_id)
    if ticket is None:
        return output
    binding = ticket["binding"]
    phase = ticket.get("phase", "intent")
    output.update(found=True, request_kind="submission", phase="unknown" if phase == "intent" else phase,
                  code=ticket.get("code"), result=ticket.get("result"),
                  binding_sha256=hashlib.sha256(canonical(binding).encode()).hexdigest(),
                  payload_sha256=binding["payload_sha256"], receipt_source="ticket", receipt_persisted=True,
                  delivery_confirmed=True if phase == "delivered" else None,
                  batch_persisted=(ticket.get("result") or {}).get("persisted"),
                  created_at=ticket.get("created_at"), finished_at=ticket.get("finished_at"),
                  reason_code="awaiting_receipt" if phase == "delivered" else "outcome_unknown" if phase == "intent" else "request_settled")
    return output


def readonly_status(request_id):
    before = state.read_only()
    state.set_read_only(True)
    try:
        return request_status(request_id)
    finally:
        state.set_read_only(before)


def validate_ticket(ticket, spec):
    if not isinstance(ticket, dict) or set(ticket) != {"request_id", "binding", "batch_id", "created_at", "phase"}:
        raise ValueError("invalid submission envelope")
    request_identity(ticket["request_id"])
    binding = ticket["binding"]
    if not isinstance(binding, dict) or set(binding) != {"payload_sha256", "project", "expect_instance", "expect_project"}:
        raise ValueError("invalid submission binding")
    if binding["payload_sha256"] != hashlib.sha256(canonical(spec).encode()).hexdigest():
        raise ValueError("submission payload binding differs")
    if not isinstance(ticket["batch_id"], str) or len(ticket["batch_id"]) > 512:
        raise ValueError("invalid submission batch identity")
    for key in ("project", "expect_instance", "expect_project"):
        value = binding[key]
        if value is not None and (not isinstance(value, str) or not value or value.startswith("-") or any(c.isspace() for c in value)):
            raise ValueError("invalid submission scope")
    return binding


def check_submission(conn, ticket, spec, project):
    binding = validate_ticket(ticket, spec)
    if binding["project"] != project or (binding["expect_project"] is not None and binding["expect_project"] != project):
        raise ValueError("submission project changed")
    if binding["expect_instance"] is not None and instance_id(conn) != binding["expect_instance"]:
        raise ValueError("submission scheduler instance changed")
    if conn.execute("SELECT 1 FROM operation_requests WHERE request_id=?", (ticket["request_id"],)).fetchone():
        raise ValueError("request ID is bound to another mutation")
    previous = conn.execute("SELECT binding, batch_id FROM submission_requests WHERE request_id=?", (ticket["request_id"],)).fetchone()
    if previous and (previous[0] != canonical(binding) or previous[1] != ticket["batch_id"]):
        raise ValueError("request ID is bound to another submission")


def complete_submission(conn, ticket, result, code=0):
    if conn.execute("SELECT 1 FROM operation_requests WHERE request_id=?", (ticket["request_id"],)).fetchone():
        raise ValueError("request ID is bound to another mutation")
    binding = canonical(ticket["binding"])
    previous = conn.execute("SELECT binding, batch_id FROM submission_requests WHERE request_id=?", (ticket["request_id"],)).fetchone()
    if previous:
        if previous[0] != binding or previous[1] != ticket["batch_id"]:
            raise ValueError("request ID is bound to another submission")
        return
    conn.execute("INSERT INTO submission_requests VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                 (ticket["request_id"], binding, ticket["batch_id"], ticket["binding"]["project"],
                  code, canonical(result), ticket["created_at"], state.now()))


def rejected_submission(conn, ticket, spec):
    if ticket is None:
        return
    try:
        validate_ticket(ticket, spec)
        complete_submission(conn, ticket, {"outcome": "rejected", "batch_id": ticket["batch_id"],
                            "project": ticket["binding"]["project"], "persisted": False}, 1)
    except (ValueError, TypeError, KeyError):
        # Invalid or conflicting envelopes cannot overwrite an original receipt.
        return


def run_submission(args, cfg, spec, invoke):
    import sys
    request_id = request_identity(args.request_id)
    if args.dry_run or state._bound_connection.get() is not None:
        print("错误: idempotent submit cannot be previewed or nested in request", file=sys.stderr)
        return 64
    if not isinstance(spec, dict):
        raise ValueError("submission must be a JSON object")
    binding = {"payload_sha256": hashlib.sha256(canonical(spec).encode()).hexdigest(),
               "project": spec.get("project") or cfg.get("default_project"),
               "expect_instance": args.expect_instance, "expect_project": args.expect_project}
    digest = hashlib.sha256(canonical(binding).encode()).hexdigest()
    with state.submission_lock():
        receipt = readonly_status(request_id)
        if receipt["found"]:
            if receipt["request_kind"] != "submission" or receipt["binding_sha256"] != digest:
                print("错误: request ID is bound to another submission", file=sys.stderr)
                return 64
            if receipt["phase"] == "unknown":
                print("错误: prior submission outcome unknown; retain original request", file=sys.stderr)
                return 75
            result = receipt["result"]
            print(json.dumps({"schema_version": 1, "request_id": request_id, "replayed": True,
                              "phase": receipt["phase"], **(result or {})}))
            return receipt["code"] or 0
        name = spec.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError("submission requires a batch name")
        from datetime import datetime
        ticket = {"request_id": request_id, "binding": binding,
                  "batch_id": name + "-" + datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3] + "-" + secrets.token_hex(6),
                  "created_at": state.now(), "phase": "intent"}
        validate_ticket(ticket, spec)
        save_ticket(ticket)  # Durable intent before any delivery side effect.
    try:
        call = copy.copy(args)
        call.json, call._submission_ticket, call._submission_spec = True, ticket, spec
        capture = io.StringIO()
        try:
            with contextlib.redirect_stdout(capture):
                code = invoke(call)
        except ValueError as error:
            print("错误: " + str(error), file=sys.stderr)
            code = 65
        if code == 2:
            # An inbox rename may have succeeded before directory fsync failed.
            # Keep the durable intent unknown, never call this a rejection.
            print("错误: submission delivery unconfirmed; retain original request", file=sys.stderr)
            return 75
        # A DB receipt proves the atomic publication even if final output is lost.
        receipt = readonly_status(request_id)
        if receipt["phase"] == "done":
            result = receipt["result"]
            code = receipt["code"]
            phase = "done"
        elif code == 0:
            result = json.loads(capture.getvalue())
            phase = "delivered" if not result["persisted"] else "done"
        else:
            result = {"outcome": "rejected", "batch_id": ticket["batch_id"],
                      "project": binding["project"], "persisted": False}
            phase = "done"
        save_ticket(dict(ticket, phase=phase, code=code, result=result))
        print(json.dumps({"schema_version": 1, "request_id": request_id, "phase": phase, **result}))
        return code
    except Exception as error:
        # After durable intent, a protocol/transaction/transport failure cannot
        # be reported as a definite argument rejection. Never dispatch again.
        print("错误: submission outcome unconfirmed: " + type(error).__name__, file=sys.stderr)
        return 75
