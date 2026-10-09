"""Conservative CPU admission counts; no affinity mutation or hard isolation."""
from __future__ import annotations

import json
import re
import time

from . import cluster_lease, resources, state
from .execution_policy import digest

MAX_CPUS = 2 ** 20
MAX_AGE = cluster_lease.MAX_AGE


class CpuReservationUnknown(ValueError):
    pass


def policy(cfg):
    configured = cfg.get("cpus_total", 0)
    configured = 0 if configured is None else configured
    if configured != "auto" and (type(configured) is not int or configured < 0):
        raise ValueError('cpus_total 必须为非负整数或 "auto"')
    maximum = cfg.get("cpus_auto_max")
    if maximum is not None and (type(maximum) is not int or not 1 <= maximum <= MAX_CPUS or configured != "auto"):
        raise ValueError('cpus_auto_max 仅用于 auto，必须为 1..1048576 整数或 null')
    return {"configured": configured, "auto_max": maximum,
            "lease_policy": cluster_lease.policy(cfg)}


def positive(value):
    if isinstance(value, str) and re.fullmatch(r"[1-9][0-9]{0,6}", value):
        value = int(value)
    return value if type(value) is int and 1 <= value <= MAX_CPUS else None


def resolve(cfg, *, origin=None, current=None, slurm=None, frozen_binding=None, lease_decision=None):
    """Pure resolution from the original daemon's recorded/probed facts only."""
    settings = policy(cfg)
    configured = settings["configured"]
    mode = "auto" if configured == "auto" else "fixed" if configured else "unlimited"
    sources, warnings, unknown = [], [], []
    current = current or {}
    origin = origin or {}
    env = origin.get("slurm_environment", {})
    counts = current.get("affinity")
    affinity = (len(counts) if isinstance(counts, list) and 0 < len(counts) <= MAX_CPUS
                and all(type(c) is int and 0 <= c < MAX_CPUS for c in counts) and len(set(counts)) == len(counts) else None)
    sources.append({"name": "sched_getaffinity", "cpus": affinity, "status": "known" if affinity else "unknown"})
    if affinity is None:
        unknown.append("affinity_capacity_unknown")
    decision = lease_decision
    if origin and current and slurm is not None:
        # Never trust a supplied summary over its immutable binding/context.
        decision = cluster_lease.decide(origin, current, slurm, frozen_binding=frozen_binding,
            invalid_latched=bool((lease_decision or {}).get("invalid_latched")))
    leased = "SLURM_JOB_ID" in env
    if leased:
        job = (slurm or {}).get("job")
        verified = (decision is not None and not decision["invalid_latched"] and not decision["reasons"]
                    and not any(reason.startswith("kernel_") or reason.startswith("slurm_") or reason == "slurm_origin_absent"
                                for reason in decision["unknown"])
                    and bool((slurm or {}).get("known")) and isinstance(job, dict)
                    and job.get("states") == ["RUNNING"] and job.get("job_id") == env.get("SLURM_JOB_ID"))
        if not verified:
            unknown.append("original_slurm_capacity_unverified")
        # A job-wide CPU count on multiple nodes is not a node allocation.
        single_node = verified and len(job["hosts"]) == 1
        controller = positive(job["cpus"]) if single_node else None
        sources.append({"name": "slurm_single_node_allocation", "cpus": controller,
                        "status": "known" if controller else "multi_node_not_node_capacity" if verified and not single_node else "unknown"})
        declared = []
        for name in ("SLURM_CPUS_ON_NODE", "SLURM_CPUS_PER_TASK"):
            if name in env:
                count = positive(env[name]) if verified else None
                sources.append({"name": name, "cpus": count, "status": "known" if count else "unknown"})
                if count:
                    declared.append(count)
                elif verified:
                    unknown.append("slurm_cpu_declaration_invalid:" + name)
        if verified and controller is None and not declared:
            unknown.append("slurm_node_capacity_unknown")
        if verified and single_node and controller is None:
            unknown.append("slurm_cpu_count_invalid")
        if decision and "job_cgroup_membership_not_verified" in decision["unknown"]:
            warnings.append("job_cgroup_membership_not_verified")
    values = [s["cpus"] for s in sources if s["cpus"] is not None]
    observed_limit = min(values) if values else None
    if len(set(values)) > 1:
        warnings.append("cpu_capacity_sources_disagree")
    if settings["auto_max"] is not None:
        sources.append({"name": "cpus_auto_max", "cpus": settings["auto_max"], "status": "configured_cap"})
    if mode == "auto":
        if decision is None:
            unknown.append("daemon_origin_not_recorded")
        elif decision["invalid_latched"] or not decision["dispatch_allowed"]:
            unknown.append("original_lease_dispatch_paused")
        elif decision["reasons"] or any(u.startswith("kernel_") for u in decision["unknown"]):
            unknown.append("original_kernel_context_unverified")
        effective = min(values + ([settings["auto_max"]] if settings["auto_max"] is not None else [])) if values else None
        available = not unknown and effective is not None
    else:
        effective, available = configured, True
        if mode == "fixed" and observed_limit is not None and configured > observed_limit:
            warnings.append("fixed_cpu_total_exceeds_observed_capacity")
    return {"schema_version": 1, "configured": configured, "mode": mode, "auto_max": settings["auto_max"],
            "effective_total": effective, "available": available, "sources": sources, "observed_upper_bound": observed_limit,
            "warnings": sorted(set(warnings)), "unknown": sorted(set(unknown)),
            "allocation_state": decision["allocation_state"] if decision else "unknown",
            "lease_dispatch_allowed": decision["dispatch_allowed"] if decision else None,
            "invalid_latched": decision["invalid_latched"] if decision else None,
            "zero_means": "unlimited_reservation_budget", "unit": "logical_cpu_reservations",
            "hard_isolation": False, "admission_granted": False}


def capture(dispatcher, *, cfg=None):
    cfg = dispatcher.cfg if cfg is None else cfg
    monitor = getattr(dispatcher, "_cluster_lease", None)
    result = resolve(cfg, origin=monitor.origin if monitor is not None else None,
        current=monitor.current_context if monitor is not None else None,
        slurm=monitor.sample if monitor is not None else None,
        frozen_binding=monitor.frozen_binding if monitor is not None else None,
        lease_decision=monitor.decision if monitor is not None else None)
    owner = monitor.owner if monitor is not None else None
    body = {"schema_version": 1, "node": state.hostname(), "instance_id": monitor.origin["instance_id"] if monitor else None,
            "owner": owner, "captured_at": time.time(), "policy_sha256": digest(policy(cfg)), "capacity": result}
    dispatcher._cpu_capacity = result
    try:
        resources.write_private_json("daemon.cpu-capacity.json", {**body, "sha256": digest(body)})
    except (OSError, ValueError, TypeError):
        dispatcher.log_line("CPU capacity observation unavailable; dispatch uses fresh in-memory decision")
    signature = (result["mode"], result["effective_total"], result["available"], tuple(result["warnings"]), tuple(result["unknown"]))
    if signature != getattr(dispatcher, "_cpu_capacity_signature", None):
        if result["warnings"] or (result["mode"] == "auto" and not result["available"]):
            dispatcher.log_line(f"CPU capacity configured={result['configured']} effective={result['effective_total']} "
                                f"available={result['available']} warnings={result['warnings']} unknown={result['unknown']}")
        dispatcher._cpu_capacity_signature = signature
    return result


def recorded(conn, cfg):
    """Private passive read; no query-host affinity, Slurm or owner connection."""
    from .daemon import _read_lease_owner, health_snapshot
    from .integration import instance_id
    error = None
    age = None
    body = None
    try:
        raw = resources.read_private_json("daemon.cpu-capacity.json")
        if len(json.dumps(raw, allow_nan=False).encode()) > 64 * 1024:
            raise ValueError("CPU observation bound")
        claimed = raw.pop("sha256")
        age = time.time() - raw["captured_at"]
        owner = _read_lease_owner()
        health = health_snapshot()
        if claimed != digest(raw) or raw.get("schema_version") != 1:
            error = "cpu_observation_invalid"
        elif raw["node"] != state.hostname() or raw["instance_id"] != instance_id(conn):
            error = "cpu_observation_identity_mismatch"
        elif raw["policy_sha256"] != digest(policy(cfg)):
            error = "cpu_observation_configuration_lag"
        elif not 0 <= age <= MAX_AGE:
            error = "cpu_observation_stale_or_future"
        elif (owner is None or raw["owner"] != owner or _read_lease_owner() != owner
              or health["process_state"] == "stopped"
              or (owner["physical_host"] == health["query_host"] and health["process_state"] != "running")):
            error = "cpu_observation_owner_not_current"
        else:
            value = raw["capacity"]
            expected = policy(cfg)
            if (not isinstance(value, dict) or value["configured"] != expected["configured"]
                    or type(value["available"]) is not bool or value["hard_isolation"] is not False
                    or value["admission_granted"] is not False
                    or value["mode"] != ("auto" if expected["configured"] == "auto" else "fixed" if expected["configured"] else "unlimited")
                    or value["auto_max"] != expected["auto_max"]
                    or not isinstance(value.get("sources"), list) or len(value["sources"]) > 8
                    or any(not isinstance(s, dict) or not isinstance(s.get("name"), str)
                           or not isinstance(s.get("status"), str) or (s.get("cpus") is not None and positive(s["cpus"]) is None)
                           for s in value["sources"])
                    or any(not isinstance(value.get(key), list) or len(value[key]) > 32
                           or any(not isinstance(reason, str) or len(reason) > 256 for reason in value[key])
                           for key in ("warnings", "unknown"))
                    or (value["mode"] != "auto" and (value["effective_total"] != expected["configured"] or not value["available"]))
                    or (value["mode"] == "auto" and value["available"] and positive(value["effective_total"]) is None)
                    or (value["effective_total"] is not None and (type(value["effective_total"]) is not int or value["effective_total"] < 0))
                    or (value["mode"] == "auto" and expected["auto_max"] is not None and value["effective_total"] is not None and value["effective_total"] > expected["auto_max"])):
                raise ValueError("CPU observation decision invalid")
            body = raw
    except FileNotFoundError:
        error = "cpu_observation_missing"
    except (OSError, ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
        error = "cpu_observation_unreadable"
    result = body["capacity"] if body else resolve(cfg)
    if error and result["mode"] == "auto":
        result["effective_total"] = None
        result["available"] = False
        result["unknown"] = sorted(set(result["unknown"] + [error]))
    return {**result, "observation": {"captured_at": body["captured_at"] if body else None, "age_s": age,
             "expires_after_s": MAX_AGE, "error": error}, "lease_id": body["owner"]["lease_id"] if body else None}


def permits(cfg, capacity, *, cpus, used, cpu_only, cpu_jobs):
    if used is None:
        return False
    configured = policy(cfg)["configured"]
    if configured == "auto":
        return bool(capacity and capacity["available"] and positive(capacity["effective_total"])
                    and used + cpus <= capacity["effective_total"])
    if configured:
        return used + cpus <= configured
    return not cpu_only or cpu_jobs < int(cfg.get("max_cpu_jobs", 2))


def reserved(conn, cfg, fallback):
    """Use immutable launch CPU reservations despite later default hot updates."""
    total = 0
    has_allocations = conn.execute("PRAGMA user_version").fetchone()[0] >= 16
    for job in conn.execute("SELECT * FROM jobs WHERE status='running'"):
        identifier = dict(job).get("allocation_id") if has_allocations else None
        if identifier:
            from .allocation import _allocation
            row = conn.execute("SELECT * FROM allocations WHERE allocation_id=? AND job_id=?", (identifier, job["id"])).fetchone()
            if row is None:
                raise state.StateError("running CPU allocation binding missing")
            value = _allocation(row).get("cpu_reservation")
            if type(value) is not int or value <= 0:
                raise state.StateError("running CPU allocation reservation invalid")
            total += value
        else:
            try:
                raw = conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?", (job["batch_id"], job["task_id"], job["version"])).fetchone()
                spec = json.loads(raw[0]) if raw else None
                if policy(cfg)["configured"] == "auto":
                    declaration = spec.get("resources", {}) if isinstance(spec, dict) else {}
                    cpus = declaration.get("cpus") if isinstance(declaration, dict) else None
                    if not ((type(cpus) is int and cpus > 0) or (cpus is None and type(declaration.get("gpu")) is int and declaration["gpu"] == 0)):
                        raise CpuReservationUnknown("legacy_running_cpu_reservation_unknown")
                total += fallback(spec) if spec else int(cfg.get("gpu_job_cpus", 8))
            except CpuReservationUnknown:
                raise
            except (AttributeError, TypeError, ValueError, KeyError):
                if policy(cfg)["configured"] == "auto":
                    raise CpuReservationUnknown("legacy_running_cpu_reservation_unknown") from None
                total += int(cfg.get("gpu_job_cpus", 8))
    return total
