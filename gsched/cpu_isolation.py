"""Opt-in original CPU claims, affinity and explicitly delegated cpuset launch."""
from __future__ import annotations

import os
import sys

from . import cluster_lease, cpu_capacity, state
from .allocation import _allocation, record
from .execution import LaunchConstraints
from .execution.constraints import MAX_AFFINITY_COUNT, MAX_CPU_INDEX
from .execution_policy import digest

SCHEMA = """
CREATE TABLE IF NOT EXISTS cpu_assignments (
 cpu INTEGER PRIMARY KEY CHECK(cpu >= 0 AND cpu < 1048576),
 allocation_id TEXT NOT NULL REFERENCES allocations(allocation_id),
 job_id TEXT NOT NULL REFERENCES jobs(id)
);
CREATE INDEX IF NOT EXISTS cpu_assignment_allocation ON cpu_assignments(allocation_id,cpu);
CREATE TRIGGER IF NOT EXISTS cpu_assignment_immutable BEFORE UPDATE ON cpu_assignments
 BEGIN SELECT RAISE(ABORT,'CPU claims cannot migrate'); END;
"""
MAX_POOL = 65536


def policy(cfg):
    raw = cfg.get("cpu_isolation", {})
    if type(raw) is not dict or set(raw) - {"mode", "delegated_root"} or raw.get("mode", "off") not in ("off", "affinity", "cgroup"):
        raise ValueError("cpu_isolation 仅接受 mode=off|affinity|cgroup 和显式 delegated_root")
    if raw.get("mode") == "cgroup":
        from pathlib import PurePosixPath
        path = raw.get("delegated_root")
        if (type(path) is not str or not path.startswith("/") or path.startswith("//") or path == "/"
                or "\0" in path or len(path) > 4096 or str(PurePosixPath(path)) != path or ".." in PurePosixPath(path).parts):
            raise ValueError("cgroup 模式必须显式配置规范绝对 delegated_root")
        return {"mode": "cgroup", "delegated_root": path}
    if "delegated_root" in raw:
        raise ValueError("delegated_root 只用于显式 cgroup 模式")
    return {"mode": raw.get("mode", "off")}


def _cpus(value, *, maximum=MAX_AFFINITY_COUNT):
    if (type(value) is not list or not 0 < len(value) <= maximum
            or any(type(cpu) is not int or not 0 <= cpu <= MAX_CPU_INDEX for cpu in value)
            or sorted(set(value)) != value):
        raise state.StateError("CPU binding must be an ordered unique bounded list")
    return value


def allocation_binding(conn, identifier, job_id):
    row = conn.execute("SELECT * FROM allocations WHERE allocation_id=? AND job_id=?", (identifier, job_id)).fetchone()
    if row is None:
        raise state.StateError("CPU allocation binding missing")
    value = _allocation(row)
    binding = value.get("cpu_binding")
    if (type(binding) is not dict or binding.get("mode") not in ("affinity", "cgroup")
            or binding.get("hard_isolation") is not False or binding.get("schema_version") != 1
            or len(_cpus(binding["cpus"])) != value["cpu_reservation"]):
        raise state.StateError("CPU allocation binding invalid")
    if binding["mode"] == "cgroup":
        from .execution.scopes import ScopeParent
        try:
            ScopeParent(**binding["scope_parent"])
            _cpus(binding["scope_mems"], maximum=4096)
            pool = _cpus(binding["scope_pool"], maximum=MAX_POOL)
            if type(binding.get("scope_authority")) is not dict or not set(binding["cpus"]) <= set(pool):
                raise ValueError("CPU scope authority/pool binding missing")
        except (KeyError, TypeError, ValueError) as error:
            raise state.StateError("CPU scope authority binding invalid") from error
    return value, binding


def claims(conn):
    rows = conn.execute("SELECT * FROM cpu_assignments ORDER BY allocation_id,cpu LIMIT ?", (MAX_POOL + 1,)).fetchall()
    if len(rows) > MAX_POOL:
        raise state.StateError("active CPU claims exceed bound")
    groups = {}
    for row in rows:
        groups.setdefault((row["allocation_id"], row["job_id"]), []).append(row["cpu"])
    for (identifier, job_id), cpus in groups.items():
        _, binding = allocation_binding(conn, identifier, job_id)
        if binding["cpus"] != cpus:
            raise state.StateError("CPU claim set differs from immutable allocation")
    return groups


def pool_from_facts(cfg, origin, current, slurm, frozen_binding, decision):
    """Pure pool resolution, shared by launch and passive explanations."""
    # Affinity is a CPU-ID binding, not just a count. Keep the original exact
    # pool and kernel owner context even under an observational lease policy.
    keys = ("pid", "start_token", "physical_host", "uid", "cgroups", "affinity")
    if any(current.get(key) is None or current.get(key) != origin.get(key) for key in keys):
        return {"allowed": False, "reason": "original_cpu_kernel_context_changed_or_unknown"}
    capacity = cpu_capacity.resolve({**cfg, "cpus_total": "auto", "cpus_auto_max": None},
        origin=origin, current=current, slurm=slurm,
        frozen_binding=frozen_binding, lease_decision=decision)
    if not capacity["available"]:
        return {"allowed": False, "reason": "original_cpu_capacity_unavailable", "capacity": capacity}
    return {"allowed": True, "pool": _cpus(current["affinity"], maximum=MAX_POOL)[:capacity["effective_total"]],
            "kernel_context_sha256": digest({key: current[key] for key in keys})}


def choose(conn, count, pool, *, job_id=None):
    """One snapshot's claims, never a reservation or promise of dispatch."""
    if type(count) is not int or not 1 <= count <= MAX_AFFINITY_COUNT:
        return {"allowed": False, "reason": "cpu_request_exceeds_affinity_bound"}
    _cpus(pool, maximum=MAX_POOL)
    groups = claims(conn)
    if job_id is not None and any(holder == job_id for _, holder in groups):
        return {"allowed": False, "reason": "prior_cpu_claim_not_released"}
    for job in conn.execute("SELECT id,allocation_id FROM jobs WHERE status='running'"):
        if not job["allocation_id"] or (job["allocation_id"], job["id"]) not in groups:
            return {"allowed": False, "reason": "legacy_running_cpu_binding_unknown"}
    occupied = {cpu for cpus in groups.values() for cpu in cpus}
    selected = [cpu for cpu in pool if cpu not in occupied][:count]
    if len(selected) != count:
        return {"allowed": False, "reason": "cpu_pool_exhausted"}
    return {"allowed": True, "cpus": selected}


def select(dispatcher, conn, count, *, job_id=None):
    """Fresh kernel facts, cached original Slurm evidence; never migrate claims."""
    if policy(dispatcher.cfg)["mode"] == "off":
        if conn.execute("PRAGMA user_version").fetchone()[0] >= 19:
            from .cpu_scope_state import unresolved
            if unresolved(conn):
                return {"allowed": False, "reason": "unresolved_cpu_scope_retained"}
        return None
    monitor = getattr(dispatcher, "_cluster_lease", None)
    if sys.platform != "linux" or monitor is None or not hasattr(os, "memfd_create"):
        return {"allowed": False, "reason": "cpu_affinity_primitives_unavailable"}
    pool = pool_from_facts(dispatcher.cfg, monitor.origin, cluster_lease.kernel_context(),
                          monitor.sample, monitor.frozen_binding, monitor.decision)
    if not pool["allowed"]:
        return pool
    controller = None
    if policy(dispatcher.cfg)["mode"] == "cgroup":
        controller = getattr(dispatcher, "_cpu_scopes", None)
        if controller is None or not controller.admission_current():
            return {"allowed": False, "reason": "original_cgroup_admission_unavailable"}
        available = set(controller.cpus)
        pool["pool"] = [cpu for cpu in monitor.origin["affinity"] if cpu in available][:len(pool["pool"])]
        if not pool["pool"]:
            return {"allowed": False, "reason": "original_cgroup_cpu_pool_empty"}
    selected = choose(conn, count, pool["pool"], job_id=job_id)
    if not selected["allowed"]:
        return selected
    value = {"schema_version": 1, "mode": policy(dispatcher.cfg)["mode"], "cpus": selected["cpus"],
        "lease_id": monitor.owner["lease_id"], "kernel_context_sha256": pool["kernel_context_sha256"],
        "hard_isolation": False, "semantics": "launch_affinity_not_nonwidenable_cpuset"}
    if value["mode"] == "cgroup":
        from .cpu_scope_controller import active, MAX_ACTIVE
        if len(active(conn)) >= MAX_ACTIVE:
            return {"allowed": False, "reason": "cpu_scope_bound_exhausted"}
        value.update(controller.binding_fields())
        value["semantics"] = "cpuset_launch_intent_not_join_proof"
    return {"allowed": True, "binding": value}


def recorded_selection(conn, cfg, count, *, job_id=None):
    """Exact live owner's recorded origin; no query-host kernel/Slurm probes."""
    from .daemon import _read_lease_owner
    base = {"mode": policy(cfg)["mode"], "hard_isolation": False, "runtime_probed": False, "admission_granted": False}
    try:
        if conn.execute("PRAGMA user_version").fetchone()[0] < 18:
            return {**base, "allowed": None, "reason": "cpu_claims_migration_required"}
        owner = _read_lease_owner()
        if owner is None:
            return {**base, "allowed": None, "reason": "cpu_pool_owner_missing"}
        item = cluster_lease.query(conn, lease_id=owner["lease_id"], limit=1)["leases"][0]
        age = item["observation_age_s"]
        if (not item["current_owner_binding"] or item["recorded_exit"] or age is None
                or not 0 <= age <= cluster_lease.MAX_AGE or _read_lease_owner() != owner):
            return {**base, "allowed": None, "reason": "cpu_pool_observation_not_current"}
        check = item["recorded_check"]["data"]
        pool = pool_from_facts(cfg, item["origin"], check["current_context"], check["slurm_observation"],
                               check["slurm_binding"], check)
        if not pool["allowed"]:
            return {**base, **pool, "allowed": None}
        if base["mode"] == "cgroup":
            # A lease observation is not a current observation of the delegated
            # parent, original inode, cpuset configuration or unresolved scopes.
            # Never probe from this read-only query or promise cgroup admission.
            return {**base, "allowed": None, "reason": "original_cgroup_observation_unavailable",
                    "lease_id": owner["lease_id"], "observation_age_s": age,
                    "expires_after_s": cluster_lease.MAX_AGE}
        return {**base, **choose(conn, count, pool["pool"], job_id=job_id),
                "lease_id": owner["lease_id"], "observation_age_s": age,
                "expires_after_s": cluster_lease.MAX_AGE}
    except (OSError, ValueError, TypeError, KeyError, state.StateError):
        return {**base, "allowed": None, "reason": "cpu_pool_observation_unreadable"}


def reserve(conn, allocation_id, job_id, binding):
    if not conn.in_transaction:
        raise state.StateError("CPU claims require the launch writer transaction")
    value, persisted = allocation_binding(conn, allocation_id, job_id)
    if persisted != binding:
        raise state.StateError("CPU claim differs from immutable binding")
    conn.executemany("INSERT INTO cpu_assignments VALUES(?,?,?)", ((cpu, allocation_id, job_id) for cpu in binding["cpus"]))
    record(conn, job_id, "resource", {"event": "cpu_affinity_reserved", "cpus": binding["cpus"],
           "hard_isolation": False, "physical_boundary_verified": False}, allocation_id=allocation_id)


def launch_constraints(conn, job):
    identifier = dict(job).get("allocation_id")
    if not identifier:
        return None
    row = conn.execute("SELECT * FROM allocations WHERE allocation_id=? AND job_id=?", (identifier, job["id"])).fetchone()
    if row is None:
        raise state.StateError("launch allocation missing")
    if "cpu_binding" not in _allocation(row):
        return None
    if conn.execute("PRAGMA user_version").fetchone()[0] >= 19:
        if conn.execute("SELECT 1 FROM cpu_scopes WHERE allocation_id=?", (identifier,)).fetchone() is not None:
            raise state.StateError("CPU scope launch requires its original retained cgroup FD; affinity fallback refused")
    rows = conn.execute("SELECT cpu FROM cpu_assignments WHERE allocation_id=? AND job_id=? ORDER BY cpu", (identifier, job["id"])).fetchall()
    _, binding = allocation_binding(conn, identifier, job["id"])
    if binding["mode"] == "cgroup":
        raise state.StateError("cgroup allocation cannot use affinity-only launch even if scope intent is missing")
    if [row["cpu"] for row in rows] != binding["cpus"]:
        raise state.StateError("launch CPU claims differ from immutable allocation")
    return LaunchConstraints(cpu_affinity=tuple(binding["cpus"]))


def release(conn, job, *, cleanup_source):
    """Called only after the dispatcher's existing no-child/clean-group guard.

    Cleanup permits release without an exit code; it never creates wait facts.
    The explicit original allocation prevents retry or a newer claim migrating.
    """
    identifier = dict(job).get("allocation_id")
    if not identifier:
        return
    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")
    if cleanup_source not in ("ordinary_group_gone", "adoption_group_gone", "stop_group_gone",
                              "configured_group_clean", "configured_not_started", "launch_not_started"):
        raise ValueError("CPU release requires an identified cleanup source")
    rows = conn.execute("SELECT cpu FROM cpu_assignments WHERE allocation_id=? AND job_id=? ORDER BY cpu", (identifier, job["id"])).fetchall()
    if not rows:
        return
    if conn.execute("PRAGMA user_version").fetchone()[0] >= 19:
        from .cpu_scope_state import release_allowed
        if not release_allowed(conn, identifier, job["id"]):
            return  # Scope uncertainty survives job terminal/retry pointer changes.
    _, binding = allocation_binding(conn, identifier, job["id"])
    if [row["cpu"] for row in rows] != binding["cpus"]:
        raise state.StateError("CPU release binding differs")
    record(conn, job["id"], "resource", {"event": "cpu_affinity_released", "cpus": binding["cpus"],
           "cleanup_source": cleanup_source, "wait_authority_granted": False}, allocation_id=identifier)
    conn.execute("DELETE FROM cpu_assignments WHERE allocation_id=? AND job_id=?", (identifier, job["id"]))


def query(conn, cfg, *, limit=50, cursor=None):
    import re
    from .integration import instance_id
    if type(limit) is not int or not 1 <= limit <= 100 or (cursor is not None and re.fullmatch("[0-9a-f]{32}", cursor) is None):
        raise ValueError("CPU claims limit/cursor invalid")
    base = {"mode": policy(cfg)["mode"], "hard_isolation": False, "runtime_probed": False,
            "admission_granted": False, "semantics": "recorded_active_cpu_claims"}
    if conn.execute("PRAGMA user_version").fetchone()[0] < 18:
        return {**base, "available": False, "reason": "migration_required", "claims": [], "truncated": False, "next_cursor": None}
    if not state._schema_is_complete(conn, 18):
        raise state.StateError("CPU claims schema incomplete")
    rows = conn.execute("SELECT allocation_id,job_id FROM cpu_assignments WHERE allocation_id>? GROUP BY allocation_id,job_id ORDER BY allocation_id LIMIT ?", (cursor or "", limit + 1)).fetchall()
    values = []
    for row in rows[:limit]:
        value, binding = allocation_binding(conn, row["allocation_id"], row["job_id"])
        cpus = [r["cpu"] for r in conn.execute("SELECT cpu FROM cpu_assignments WHERE allocation_id=? ORDER BY cpu", (row["allocation_id"],))]
        if cpus != binding["cpus"] or value["instance_id"] != instance_id(conn):
            raise state.StateError("CPU query claim/instance differs")
        values.append({"allocation_id": value["allocation_id"], "job_id": value["job_id"],
                       "version": value["version"], **binding})
    return {**base, "available": True, "reason": None, "claims": values, "truncated": len(rows) > limit,
            "next_cursor": values[-1]["allocation_id"] if len(rows) > limit else None,
            "pagination": "live_keyset_not_complete_snapshot"}
