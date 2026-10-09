"""Recorded original delegation observations, never a launch capability.

Only the owning compute controller samples. Queries use a private snapshot and
exact daemon lease; they never open a cgroup, query BPF or contact Slurm.
"""
from __future__ import annotations

import math
import time

from . import cluster_lease, state
from .execution.scopes import ScopeParent
from .execution_policy import digest
from .integration import instance_id

VERSION = "sched-scope-health/v1"
MAX_AGE = 30  # Diagnostics span the ten-second tick, not launch admission.


def policies(cfg):
    from .cpu_isolation import policy as cpu
    from .device_scope_controller import policy as device
    return {"cpu": cpu(cfg), "device": device(cfg)}


def _indices(values, bound):
    return (type(values) is list and 0 < len(values) <= bound
            and all(type(n) is int and 0 <= n < 2 ** 20 for n in values)
            and sorted(set(values)) == values)


def _facts(value):
    if (type(value) is not dict or set(value) != {"policies", "parent", "authority", "cpus", "mems", "device_query"}
            or policies({"cpu_isolation": value["policies"]["cpu"], "device_isolation": value["policies"]["device"]}) != value["policies"]
            or value["policies"]["cpu"]["mode"] != "cgroup"
            or type(value["parent"]) is not dict
            or ScopeParent(**value["parent"]).path != value["policies"]["cpu"]["delegated_root"]
            or not _indices(value["cpus"], 65536) or not _indices(value["mems"], 4096)
            or type(value["authority"]) is not dict
            or set(value["authority"]) != {"schema_version", "mountpoint", "mount_root", "parent_cgroup", "original_daemon_cgroup", "original_boundary", "slurm_job_id", "kernel_context_sha256"}):
        raise state.StateError("original scope health evidence invalid")
    authority = value["authority"]
    from .cpu_scope_controller import _path
    if (type(authority["schema_version"]) is not int or authority["schema_version"] != 1
            or authority["mount_root"] != "/"
            or any(_path(authority[k]) != authority[k] for k in ("mountpoint", "parent_cgroup", "original_daemon_cgroup", "original_boundary"))):
        raise state.StateError("original scope health authority invalid")
    query = value["device_query"]
    if value["policies"]["device"]["mode"] == "off":
        if query is not None:
            raise state.StateError("disabled device health must not contain a probe")
    else:
        from .device_scope_controller import validate_parent_query
        validate_parent_query(query)
    cluster_lease.encode(value)
    return value


def _original_context(facts, origin):
    from .cpu_scope_controller import authority_from_facts
    authority = facts["authority"]
    verified = authority_from_facts(facts["parent"]["path"], origin, origin,
        {"allocation_state": "valid", "invalid_latched": False, "dispatch_allowed": True},
        [{"root": authority["mount_root"], "mountpoint": authority["mountpoint"]}])
    if verified != authority or facts["parent"]["uid"] != origin["uid"]:
        raise state.StateError("scope health origin kernel/hierarchy binding differs")


def record(controller, *, facts=None, reason=None, observed_at=None):
    """Record after probes; never inside a caller's writer or request."""
    if state._bound_connection.get() is not None:
        raise state.StateError("scope health cannot sample inside a bound writer")
    now = time.time()
    if (type(observed_at) not in (int, float) or not math.isfinite(observed_at)
            or not 0 <= now - observed_at <= 5):
        raise state.StateError("scope health probe expired before recording")
    if facts is not None:
        _facts(facts)
    if reason is not None and reason not in {"root_preflight_unavailable", "cold_policy_changed", "active_scopes_unresolved"}:
        raise state.StateError("scope health reason invalid")
    if facts is None and reason is None:
        raise state.StateError("scope health requires observations or an unknown reason")
    monitor = controller.dispatcher._cluster_lease
    with state.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        if not 0 <= time.time() - observed_at <= 5:
            raise state.StateError("scope health expired while acquiring its writer")
        if conn.execute("PRAGMA user_version").fetchone()[0] < 25:
            raise state.StateError("scope health evidence requires schema 25")
        row = conn.execute("SELECT * FROM daemon_leases WHERE lease_id=?", (monitor.owner["lease_id"],)).fetchone()
        if row is None or cluster_lease.decode(row) != monitor.origin or monitor.origin["instance_id"] != instance_id(conn):
            raise state.StateError("scope health requires its exact persisted daemon origin")
        if facts is not None:
            _original_context(facts, monitor.origin)
        lease_id = row["lease_id"]
        if conn.execute("SELECT 1 FROM daemon_lease_events WHERE lease_id=? AND kind='exit'", (lease_id,)).fetchone():
            raise state.StateError("exited daemon cannot record new scope health")
        row = conn.execute("SELECT * FROM daemon_lease_events WHERE lease_id=? AND kind='scope_origin' ORDER BY seq LIMIT 1", (lease_id,)).fetchone()
        original = cluster_lease.decode(row) if row else None
        if original is None and facts is not None:
            original = cluster_lease.event(conn, lease_id, "scope_origin", {"interface_version": VERSION, "facts": facts})
        if original is not None:
            _facts(original["data"]["facts"])
        changed = original is not None and facts is not None and facts != original["data"]["facts"]
        status = "invalid" if changed else "unknown" if facts is None else "unavailable" if reason else "ready"
        data = {"interface_version": VERSION, "observed_at": observed_at,
                "origin_sha256": digest(original) if original else None,
                "status": status, "reason": "original_root_changed" if changed else reason,
                "facts": facts, "admission_granted": False, "wait_authority_granted": False,
                "physical_boundary_verified": False}
        cluster_lease.event(conn, lease_id, "scope_check", data)
        if not 0 <= time.time() - observed_at <= 5:
            raise state.StateError("scope health expired before its evidence commit")
    return not changed


def query(conn, cfg):
    """No runtime probes or migrations, even from the compute node."""
    from .daemon import _read_lease_owner
    base = {"interface_version": VERSION, "policies": policies(cfg), "available": False,
            "status": "unknown", "reason": "original_cgroup_observation_unavailable",
            "recorded_origin": None, "recorded_check": None, "lease_id": None,
            "observation_age_s": None, "expires_after_s": MAX_AGE,
            "runtime_probed": False, "admission_granted": False,
            "wait_authority_granted": False, "physical_boundary_verified": False}
    if conn.execute("PRAGMA user_version").fetchone()[0] < 25:
        return {**base, "reason": "migration_required"}
    owner = _read_lease_owner()
    if owner is None:
        return {**base, "reason": "daemon_owner_missing"}
    item = cluster_lease.query(conn, lease_id=owner["lease_id"], limit=1)["leases"][0]
    lease_id = owner["lease_id"]
    rows = conn.execute("SELECT * FROM daemon_lease_events WHERE lease_id=? AND kind IN ('scope_origin','scope_check') AND seq IN (SELECT MIN(seq) FROM daemon_lease_events WHERE lease_id=? AND kind='scope_origin' UNION SELECT MAX(seq) FROM daemon_lease_events WHERE lease_id=? AND kind='scope_check') ORDER BY seq", (lease_id, lease_id, lease_id)).fetchall()
    events = {row["kind"]: cluster_lease.decode(row) for row in rows}
    original, check = events.get("scope_origin"), events.get("scope_check")
    if original:
        if set(original["data"]) != {"interface_version", "facts"} or original["data"]["interface_version"] != VERSION:
            raise state.StateError("scope health origin interface invalid")
        _facts(original["data"]["facts"])
        _original_context(original["data"]["facts"], item["origin"])
    if check:
        data = check["data"]
        if (set(data) != {"interface_version", "observed_at", "origin_sha256", "status", "reason", "facts", "admission_granted", "wait_authority_granted", "physical_boundary_verified"}
                or data["interface_version"] != VERSION or data["origin_sha256"] != (digest(original) if original else None)
                or data["status"] not in {"ready", "invalid", "unknown", "unavailable"}
                or data["reason"] not in {None, "root_preflight_unavailable", "cold_policy_changed", "active_scopes_unresolved", "original_root_changed"}
                or any(data[k] is not False for k in ("admission_granted", "wait_authority_granted", "physical_boundary_verified"))
                or type(data["observed_at"]) not in (int, float) or not math.isfinite(data["observed_at"])
                or not 0 <= check["recorded_at"] - data["observed_at"] <= 5
                or original is not None and check["seq"] <= original["seq"]):
            raise state.StateError("scope health check binding invalid")
        if data["facts"] is not None:
            _facts(data["facts"])
            _original_context(data["facts"], item["origin"])
        if data["status"] == "ready" and (not original or data["facts"] != original["data"]["facts"] or data["reason"] is not None):
            raise state.StateError("scope health ready evidence differs from original")
        if ((data["status"] == "unknown" and (data["facts"] is not None or data["reason"] is None))
                or (data["status"] == "unavailable" and (not original or data["facts"] != original["data"]["facts"] or data["reason"] != "active_scopes_unresolved"))
                or (data["status"] == "invalid" and (not original or data["facts"] is None or data["facts"] == original["data"]["facts"] or data["reason"] != "original_root_changed"))):
            raise state.StateError("scope health classification contradicts its evidence")
    age = time.time() - check["data"]["observed_at"] if check else None
    base.update(lease_id=lease_id, recorded_origin=original, recorded_check=check, observation_age_s=age)
    if (not item["current_owner_binding"] or item["recorded_exit"] or _read_lease_owner() != owner):
        return {**base, "reason": "daemon_owner_not_current"}
    if original and original["data"]["facts"]["policies"] != base["policies"]:
        return {**base, "reason": "cold_policy_changed"}
    if base["policies"]["cpu"]["mode"] != "cgroup":
        return {**base, "status": "disabled", "reason": "cgroup_not_enabled"}
    if item["allocation_state"] == "invalid":
        return {**base, "reason": "original_lease_invalid"}
    if item["origin"]["slurm_environment"].get("SLURM_JOB_ID") and item["allocation_state"] != "valid":
        return {**base, "reason": "original_slurm_lease_not_current"}
    if check is None or age is None or not 0 <= age <= MAX_AGE:
        return {**base, "reason": "root_observation_missing_or_stale"}
    return {**base, "available": True, "status": check["data"]["status"], "reason": check["data"]["reason"]}


def fit(conn, cfg, count, *, job_id=None):
    """Recorded explanation only; use the same bounded pool/claim decisions."""
    from . import cpu_isolation
    observed = query(conn, cfg)
    base = {"allowed": None, "reason": observed["reason"], "lease_id": observed["lease_id"],
            "observation_age_s": observed["observation_age_s"], "expires_after_s": MAX_AGE}
    if not observed["available"] or observed["status"] != "ready":
        return base
    item = cluster_lease.query(conn, lease_id=observed["lease_id"], limit=1)["leases"][0]
    if (not item["current_owner_binding"] or item["recorded_exit"] or item["observation_age_s"] is None
            or not 0 <= item["observation_age_s"] <= cluster_lease.MAX_AGE):
        return {**base, "reason": "original_lease_observation_not_current"}
    check = item["recorded_check"]["data"]
    pool = cpu_isolation.pool_from_facts(cfg, item["origin"], check["current_context"], check["slurm_observation"], check["slurm_binding"], check)
    if not pool["allowed"]:
        return {**base, "reason": pool["reason"]}
    from .cpu_scope_controller import active, MAX_ACTIVE
    if len(active(conn)) >= MAX_ACTIVE:
        return {**base, "allowed": False, "reason": "cpu_scope_bound_exhausted"}
    available = set(observed["recorded_check"]["data"]["facts"]["cpus"])
    pool = [cpu for cpu in item["origin"]["affinity"] if cpu in available][:len(pool["pool"])]
    if not pool:
        return {**base, "allowed": False, "reason": "original_cgroup_cpu_pool_empty"}
    return {**base, **cpu_isolation.choose(conn, count, pool, job_id=job_id)}
