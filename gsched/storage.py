"""Shared storage admission and passive evidence. No artifact deletion or wait authority."""
from __future__ import annotations

import json
import logging
import math
import os
import subprocess
import sys
import time

from . import resources, state
from .execution_policy import digest
from .integration import instance_id

DEFAULTS = {"enabled": False, "reserve_gib": 1, "reserve_inodes": 1024,
            "control_reserve_gib": 0.25, "control_reserve_inodes": 256, "require_user_quota": False}
GIB = 1024 ** 3
MAX_SAMPLE_AGE = 30
_pending_probe = None


def policy(cfg):
    supplied = cfg.get("storage_admission", {})
    if not isinstance(supplied, dict) or set(supplied) - DEFAULTS.keys():
        raise ValueError("storage_admission 必须是仅含受支持字段的对象")
    value = {**DEFAULTS, **supplied}
    for key in ("enabled", "require_user_quota"):
        if type(value[key]) is not bool:
            raise ValueError("storage_admission." + key + " 必须为布尔")
    for key in ("reserve_gib", "control_reserve_gib"):
        if not resources.finite_number(value[key]) or value[key] > 2 ** 30:
            raise ValueError("storage_admission." + key + " 必须为有界有限非负数")
    for key in ("reserve_inodes", "control_reserve_inodes"):
        if type(value[key]) is not int or not 0 <= value[key] <= 2 ** 63 - 1:
            raise ValueError("storage_admission." + key + " 必须为有界非负整数")
    return value


def request(spec):
    value = spec.get("resources") or {}
    if not isinstance(value, dict):
        raise ValueError("resources 必须为对象")
    amount, inodes = value.get("disk_gib", 0), value.get("disk_inodes", 0)
    if not resources.finite_number(amount) or amount > 2 ** 30 or type(inodes) is not int or not 0 <= inodes <= 2 ** 63 - 1:
        raise ValueError("resources.disk_gib/disk_inodes 必须为有界非负预留")
    return {"bytes": math.ceil(amount * GIB), "inodes": inodes}


def targets(cfg, spec, project):
    from .templates import resolve_template
    cwd = spec.get("cwd_abs")
    if not isinstance(cwd, str) or not os.path.isabs(cwd):
        raise ValueError("storage requires frozen absolute cwd")
    paths = {os.path.normpath(state.host_dir()): {"control"}, os.path.normpath(cwd): {"task"}}
    root = resolve_template(cfg["projects"][project]["root"], cfg)
    paths.setdefault(os.path.normpath(os.path.abspath(os.path.expanduser(root))), set()).add("task")
    for group in [spec, *(spec.get("stages") or [])]:
        for rule in (group.get("artifacts") or {}).values():
            path = rule.get("path") if isinstance(rule, dict) else rule
            if not isinstance(path, str):
                raise ValueError("invalid frozen artifact path")
            path = os.path.normpath(path if os.path.isabs(path) else os.path.join(cwd, path))
            paths.setdefault(os.path.dirname(path), set()).add("task")
    if len(paths) > 128 or sum(len(p.encode()) for p in paths) > 60 * 1024:
        raise ValueError("storage target evidence exceeds 128 paths / 60 KiB")
    return {p: sorted(roles) for p, roles in sorted(paths.items())}


def probe(paths):
    global _pending_probe
    process = None
    try:
        if _pending_probe is not None:
            if _pending_probe.poll() is None:
                raise ValueError("previous filesystem probe still pending")
            for stream in (_pending_probe.stdin, _pending_probe.stdout, _pending_probe.stderr):
                if stream is not None:
                    stream.close()
            _pending_probe = None
        process = subprocess.Popen([sys.executable, "-m", "gsched.storage_probe"],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, start_new_session=True)
        output, _ = process.communicate(json.dumps(list(paths)), timeout=5)
        if process.returncode or len(output.encode()) > 1024 * 1024:
            raise ValueError("probe failure or output bound")
        value = json.loads(output)
        if not isinstance(value, dict) or value.get("schema_version") != 1 or set(value.get("paths", {})) != set(paths):
            raise ValueError("incomplete probe")
        if not resources.finite_number(value.get("observed_at")) or not 0 <= time.time() - value["observed_at"] <= 5:
            raise ValueError("stale or future probe")
        return value
    except (OSError, subprocess.SubprocessError, ValueError, TypeError, RecursionError):
        if process is not None and process.poll() is None:
            try:
                process.kill()
            except OSError:
                pass
            # An uninterruptible mount read must not block the daemon's wait,
            # or create another helper every tick while the old one persists.
            _pending_probe = process
        return {"schema_version": 1, "observed_at": time.time(), "paths": {}, "reason": "probe_unavailable_or_timeout"}


def running_reservations(conn):
    rows = conn.execute("SELECT j.id,j.allocation_id,t.spec FROM jobs j LEFT JOIN tasks t ON t.batch_id=j.batch_id AND t.id=j.task_id AND t.version=j.version WHERE j.status='running' LIMIT 10001").fetchall()
    if len(rows) > 10000 or sum(len((row["spec"] or "").encode()) for row in rows) > 4 * 1024 * 1024:
        raise ValueError("storage running facts exceed bound")
    used, unknown = {}, []
    from .allocation import _allocation
    for row in rows:
        try:
            requested = request(json.loads(row["spec"]))
            if not any(requested.values()):
                continue
            source = conn.execute("SELECT * FROM allocations WHERE allocation_id=? AND job_id=?", (row["allocation_id"], row["id"])).fetchone()
            bindings = _allocation(source).get("storage_filesystems") if source is not None else None
            if (not isinstance(bindings, list) or not bindings or len(bindings) > 128
                    or any(not isinstance(key, str) or not key.isdecimal() or len(key) > 32 for key in bindings)
                    or len(set(bindings)) != len(bindings)):
                raise ValueError("running storage allocation unknown")
            for key in bindings:
                prior = used.setdefault(key, {"bytes": 0, "inodes": 0})
                for name in requested:
                    prior[name] += requested[name]
        except (TypeError, ValueError, state.StateError, KeyError):
            unknown.append(row["id"])
    return used, unknown


def decide(settings, requested, paths, sample, used, unresolved):
    """Pure decision used by actual dispatch and the recorded explanation."""
    grouped, unknown = {}, []
    if unresolved:
        unknown.append("running_storage_reservations_unknown")
    for path, roles in paths.items():
        observation = sample.get("paths", {}).get(path)
        if (not isinstance(observation, dict) or not isinstance(observation.get("filesystem_id"), str)
                or not observation["filesystem_id"].isdecimal() or len(observation["filesystem_id"]) > 32):
            unknown.append("filesystem_sample_unknown:" + path)
            continue
        key = observation["filesystem_id"]
        item = grouped.setdefault(key, {"filesystem_id": key, "paths": [], "roles": set(), "observations": []})
        item["paths"].append(path)
        item["roles"].update(roles)
        item["observations"].append(observation)
    checks, reasons = [], []
    for key, item in grouped.items():
        control, task = "control" in item["roles"], "task" in item["roles"]
        reserve = {"bytes": math.ceil(max(settings["control_reserve_gib"] if control else 0, settings["reserve_gib"] if task else 0) * GIB),
                   "inodes": max(settings["control_reserve_inodes"] if control else 0, settings["reserve_inodes"] if task else 0)}
        need = {kind: (requested[kind] if task else 0) + used.get(key, {}).get(kind, 0) + reserve[kind] for kind in reserve}
        data = {"filesystem_id": key, "paths": item["paths"], "roles": sorted(item["roles"]), "needed": need,
                "reserved_running": used.get(key, {}), "floor": reserve, "reasons": [], "unknown": [], "quota": []}
        observations = item["observations"]
        if any(type(o.get("readonly")) is not bool for o in observations):
            data["unknown"].append("filesystem_readonly_unknown")
        if any(o.get("readonly") is True for o in observations):
            data["reasons"].append("filesystem_readonly")
        for kind in ("bytes", "inodes"):
            available = [o.get(kind + "_available") for o in observations]
            known = all(type(v) is int and v >= 0 for v in available)
            data[kind + "_available"] = min(available) if known else None
            if not known:
                data["unknown"].append(kind + "_availability_unknown")
            elif min(available) < need[kind]:
                data["reasons"].append("disk_space" if kind == "bytes" else "inode_space")
            for observation in observations:
                quota = observation.get("quota")
                value = quota.get(kind) if isinstance(quota, dict) else None
                value = value if isinstance(value, dict) else {"known": False, "status": "unknown"}
                data["quota"].append({"dimension": kind, "scope": "current_uid_only", **value})
                if value.get("known") is not True:
                    if settings["require_user_quota"]:
                        data["unknown"].append("user_quota_" + kind + "_unknown")
                elif value.get("status") == "bounded":
                    headroom = value.get("headroom")
                    if type(headroom) is not int or headroom < 0:
                        data["unknown"].append("user_quota_" + kind + "_unknown")
                    elif headroom < need[kind]:
                        data["reasons"].append("user_quota_" + kind)
                elif value.get("status") != "no_user_limit":
                    data["unknown"].append("user_quota_" + kind + "_unknown")
        reasons.extend(data["reasons"])
        unknown.extend(data["unknown"])
        checks.append(data)
    return {"allowed": not reasons and not unknown, "reasons": sorted(set(reasons)), "unknown": sorted(set(unknown)),
            "filesystems": checks, "unresolved_running_count": len(unresolved), "hard_isolation": False,
            "quota_scope": "current_uid_only", "other_quota_scopes": "group_project_remote_not_verified"}


def capture(conn, cfg, job, spec, *, cache=None):
    from .admission import running_snapshot
    settings = policy(cfg)
    requested = request(spec)
    if not settings["enabled"]:
        return {"allowed": True, "enabled": False, "reasons": [], "unknown": [], "filesystems": []}
    inputs = None
    project = job["project"] or state.get_batch(conn, job["batch_id"])["project"]
    try:
        paths = targets(cfg, spec, project)
        used, unresolved = running_reservations(conn)
        key = digest(paths)
        sample = cache.get(key) if cache is not None else None
        if sample is None or not 0 <= time.time() - sample["observed_at"] <= 5:
            if cache is not None and time.monotonic() >= cache.get("_deadline", math.inf):
                sample = {"schema_version": 1, "observed_at": time.time(), "paths": {}, "reason": "tick_probe_budget_exhausted"}
            else:
                sample = probe(paths)
            if cache is not None and len(cache) < 256:
                cache[key] = sample
        decision = decide(settings, requested, paths, sample, used, unresolved)
        inputs = {"paths": paths, "sample": sample, "used": used, "unresolved": unresolved}
    except (ValueError, TypeError, KeyError, RecursionError):
        sample = {"observed_at": time.time()}
        decision = {"allowed": False, "reasons": [], "unknown": ["storage_evidence_bound_or_invalid"], "filesystems": []}
    return {**decision, "enabled": True, "settings": settings, "requested": requested,
            "spec_sha256": digest(spec), "config_sha256": digest(cfg), "job_id": job["id"], "inputs": inputs,
            "instance_id": instance_id(conn), "node": state.hostname(), "observed_at": sample["observed_at"],
            "running_sha256": running_snapshot(conn)}


def publish(reports):
    values = dict(reports)
    while values and len(json.dumps(values, allow_nan=False).encode()) > 1024 * 1024:
        values.pop(next(iter(values)))
    try:
        resources.write_private_json("daemon.storage.json", {"schema_version": 1, "reports": values})
    except (OSError, ValueError):
        logging.getLogger(__name__).warning("storage observation retention unavailable")


def explain(conn, cfg, job, spec):
    from .admission import running_snapshot
    settings = policy(cfg)
    requested = request(spec)
    result = {"enabled": settings["enabled"], "settings": settings, "requested": requested,
              "allowed": True if not settings["enabled"] else None, "reasons": [], "unknown": [], "filesystems": []}
    if not settings["enabled"]:
        return result
    if conn.execute("PRAGMA user_version").fetchone()[0] < 16:
        result["allowed"] = None
        result["unknown"] = ["storage_allocation_schema_unavailable"]
        return result
    try:
        report = resources.read_private_json("daemon.storage.json")["reports"][job["id"]]
        age = time.time() - report["observed_at"]
        if not 0 <= age <= MAX_SAMPLE_AGE:
            raise ValueError("storage_observation_stale_or_future")
        if (report["instance_id"] != instance_id(conn) or report["node"] != state.hostname()
                or report["job_id"] != job["id"]):
            raise ValueError("storage_identity_mismatch")
        if report["spec_sha256"] != digest(spec) or report["config_sha256"] != digest(cfg):
            raise ValueError("storage_configuration_or_spec_lag")
        if report["running_sha256"] != running_snapshot(conn):
            raise ValueError("storage_running_reservations_changed")
        if (type(report.get("allowed")) is not bool or report.get("settings") != settings
                or report.get("requested") != requested or not isinstance(report.get("filesystems"), list)):
            raise ValueError("storage_observation_invalid")
        inputs = report.get("inputs")
        if inputs is None and report.get("unknown") == ["storage_evidence_bound_or_invalid"]:
            return {**report, "age_s": age, "expires_after_s": MAX_SAMPLE_AGE}
        paths = targets(cfg, spec, job["project"] or state.get_batch(conn, job["batch_id"])["project"])
        used, unresolved = running_reservations(conn)
        if (not isinstance(inputs, dict) or inputs.get("paths") != paths or inputs.get("used") != used
                or inputs.get("unresolved") != unresolved or not isinstance(inputs.get("sample"), dict)
                or inputs["sample"].get("observed_at") != report["observed_at"]
                or not isinstance(inputs["sample"].get("paths"), dict)):
            raise ValueError("storage_observation_invalid")
        decision = decide(settings, requested, paths, inputs["sample"], used, unresolved)
        if any(report.get(key) != value for key, value in decision.items()):
            raise ValueError("storage_observation_decision_mismatch")
        return {**report, **decision, "age_s": age, "expires_after_s": MAX_SAMPLE_AGE}
    except ValueError as error:
        result["unknown"] = [str(error)]
    except (OSError, KeyError, TypeError, RecursionError):
        result["unknown"] = ["storage_observation_missing_or_unreadable"]
    return result
