"""Opt-in fresh VRAM admission; external occupancy never supplies wait authority."""
from __future__ import annotations

import json
import math
import os
import subprocess
import time

DEFAULT_MIN_FREE_GIB = 12.0
MAX_SAMPLE_AGE_SEC = 5.0


def normalize(raw):
    if type(raw) is not dict or set(raw) - {"min_free_gib", "allow_external_occupancy"}:
        raise ValueError("gpu_admission requires known fields")
    result = {"min_free_gib": DEFAULT_MIN_FREE_GIB, "allow_external_occupancy": False, **raw}
    value = result["min_free_gib"]
    if type(value) not in (int, float) or not 0 < value <= 4096 or not math.isfinite(value):
        raise ValueError("gpu_admission.min_free_gib must be finite and within 0..4096")
    if type(result["allow_external_occupancy"]) is not bool:
        raise ValueError("gpu_admission.allow_external_occupancy requires a boolean")
    return result


def policy(cfg, project):
    entry = cfg.get("projects", {}).get(project, {})
    return normalize(entry["gpu_admission"]) if "gpu_admission" in entry else None


def sample(allocator):
    """Bracket memory, utilization and compute attribution with complete topology."""
    try:
        before = allocator._uuid_to_idx()
        if before is None:
            return {}
        compute = allocator._compute_pids_by_card()
        if compute is None:
            return {}
        result = {}
        fake_free = json.loads(os.environ.get("SCHED_FAKE_FREE_GIB", "{}")) if allocator.fake else {}
        for idx in allocator.gpu_list:
            stamp = time.monotonic()
            if allocator.fake:
                total = allocator.mem_total(idx)
                free = fake_free.get(str(idx), total)
            else:
                value = subprocess.run(["nvidia-smi", "--query-gpu=memory.total,memory.free", "--format=csv,noheader,nounits", "-i", str(idx)], capture_output=True, text=True, timeout=10)
                if value.returncode != 0:
                    continue
                fields = value.stdout.strip().split(",")
                if len(fields) != 2:
                    continue
                total, free = (float(part.strip()) / 1024 for part in fields)
            util = allocator._util_opt(idx)
            if any(type(v) not in (int, float) or not math.isfinite(v) for v in (total, free)) or not 0 <= free <= total or total <= 0 or type(util) is not int or not 0 <= util <= 100:
                continue
            pids = compute.get(idx, [])
            if any(type(pid) is not int or pid <= 0 for pid in pids):
                continue
            pgids = {allocator._pgid_of(pid) for pid in pids}
            if None in pgids or (not pids and util > 0):
                continue  # Utilization without attributable processes is indeterminate.
            result[idx] = {"total_gib": float(total), "free_gib": float(free), "pgids": pgids, "sampled_at": stamp}
        after = allocator._uuid_to_idx()
        if after is None or after != before:
            return {}
        return result
    except (OSError, ValueError, TypeError, OverflowError, subprocess.SubprocessError):
        return {}


def minimum(conn, job_id, spec, configured):
    minimum_free = configured["min_free_gib"]
    retry = (spec.get("recovery") or {}).get("retry") or {}
    if not retry:
        return minimum_free
    tiers = retry.get("min_free_gib_by_round", [12])
    row = conn.execute("SELECT round FROM recovery_queue WHERE job_id=?", (job_id,)).fetchone()
    return max(minimum_free, tiers[min(row[0] if row else 0, len(tiers) - 1)])


def reservation(spec, minimum_free, conn=None, project=None):
    resources = spec.get("resources") or {}
    peak = resources.get("vram_gib", 0)
    budget = max(minimum_free, float(peak or 0))
    profile = resources.get("profile_key")
    if conn is not None and profile:
        from .dispatcher import _profile_cache_key
        cached = conn.execute("SELECT peak_gib FROM profile_cache WHERE profile_key=?", (_profile_cache_key(project, profile),)).fetchone()
        if cached:
            budget = max(budget, cached[0])
    return budget


def permits(conn, cfg, idx, job_id, spec, project, snapshot):
    configured = policy(cfg, project)
    if configured is None:
        return False
    row = conn.execute("SELECT * FROM gpus WHERE idx=?", (idx,)).fetchone()
    value = snapshot.get(idx)
    if row is None or row["quarantined"] or row["ignore_until"] or row["status"] not in ("free", "unmanaged", "assigned") or value is None or not 0 <= time.monotonic() - value["sampled_at"] <= MAX_SAMPLE_AGE_SEC:
        return False
    if row["status"] == "assigned" and conn.execute("SELECT 1 FROM gpu_jobs WHERE gpu_id=?", (idx,)).fetchone() is None:
        return False
    assignments = list(conn.execute("SELECT gj.job_id,gj.vram_gib,j.pgid,j.status,j.project,t.spec FROM gpu_jobs gj LEFT JOIN jobs j ON j.id=gj.job_id LEFT JOIN tasks t ON t.batch_id=j.batch_id AND t.id=j.task_id AND t.version=j.version WHERE gj.gpu_id=? AND gj.job_id!=?", (idx, job_id)))
    share = bool((spec.get("resources") or {}).get("gpu_share")) and cfg.get("co_locate") is True and cfg.get("projects", {}).get(project, {}).get("colocate") is not False
    if assignments and not share:
        return False
    active = {r["pgid"] for r in assignments if r["status"] == "running" and r["pgid"] is not None}
    known = {r[0] for r in conn.execute("SELECT pgid FROM jobs WHERE pgid IS NOT NULL")}
    if (value["pgids"] & known) - active:
        return False  # Terminal, releasing, unbound or unresolved own processes.
    external = value["pgids"] - active
    if external and not configured["allow_external_occupancy"]:
        return False
    if row["status"] == "unmanaged" and not external:
        return False  # A fresh empty sample does not erase an unresolved registry state.
    required = minimum(conn, job_id, spec, configured)
    outstanding = 0.0
    for bound in assignments:
        if bound["status"] != "running" or bound["pgid"] is None or bound["vram_gib"] is None or bound["spec"] is None:
            return False
        other = json.loads(bound["spec"])
        other_policy = policy(cfg, bound["project"])
        outstanding += max(float(bound["vram_gib"]), reservation(other, minimum(conn, bound["job_id"], other, other_policy) if other_policy else DEFAULT_MIN_FREE_GIB, conn, bound["project"]))
    # Deliberately reserve the full peak of every bound launch, including those
    # already reflected in memory.free. Conservative double accounting avoids
    # admitting against memory that a newly started child has not allocated yet.
    budget = reservation(spec, required, conn, project)
    configured_capacity = row["mem_total_gib"]
    capacity = min(value["total_gib"], float(configured_capacity)) if configured_capacity is not None else value["total_gib"]
    if not math.isfinite(capacity) or capacity <= 0 or outstanding + budget > capacity:
        return False
    if share and outstanding + budget > cfg.get("co_locate_safety", 0.7) * capacity:
        return False
    return value["free_gib"] - outstanding >= budget


def assign(conn, dispatcher, job_id, spec, project, snapshot):
    affinity = dispatcher._project_affinity(project)
    hard = dispatcher._project_affinity_hard(project)
    valid = set(dispatcher.allocator.gpu_list)
    order = [idx for idx in affinity if idx in valid]
    if not hard:
        order += [idx for idx in dispatcher.allocator.gpu_list if idx not in order]
    configured = policy(dispatcher.cfg, project)
    share = bool((spec.get("resources") or {}).get("gpu_share")) and dispatcher.cfg.get("co_locate") is True and dispatcher._projects.get(project, {}).get("colocate") is not False
    existing = conn.execute("SELECT gpu_id FROM gpu_jobs WHERE job_id=?", (job_id,)).fetchone()
    if existing:
        idx = existing[0]
        return idx if idx in order and idx not in dispatcher._frozen_gpus and permits(conn, dispatcher.cfg, idx, job_id, spec, project, snapshot) else None
    for idx in order:
        if idx in dispatcher._frozen_gpus or not permits(conn, dispatcher.cfg, idx, job_id, spec, project, snapshot):
            continue
        count = conn.execute("SELECT COUNT(*) FROM gpu_jobs WHERE gpu_id=?", (idx,)).fetchone()[0]
        cap = min(dispatcher.cfg.get("co_locate_max_jobs", 3), dispatcher._gpu_max_jobs.get(idx, 64))
        pcap = dispatcher._projects.get(project, {}).get("max_jobs", cap)
        project_count = conn.execute("SELECT COUNT(*) FROM gpu_jobs gj JOIN jobs j ON j.id=gj.job_id WHERE gj.gpu_id=? AND j.project=?", (idx, project)).fetchone()[0]
        if count >= cap or project_count >= pcap:
            continue
        peak = reservation(spec, minimum(conn, job_id, spec, configured), conn, project)
        if share:
            packed = conn.execute("SELECT COALESCE(SUM(vram_gib),0) FROM gpu_jobs WHERE gpu_id=?", (idx,)).fetchone()[0]
            if packed + peak > dispatcher.cfg.get("co_locate_safety", 0.7) * min(snapshot[idx]["total_gib"], dispatcher.allocator.mem_total(idx) or snapshot[idx]["total_gib"]):
                continue
        conn.execute("UPDATE gpus SET status='assigned',job_id=COALESCE(job_id,?),updated_at=? WHERE idx=?", (job_id, dispatcher._admission_now(), idx))
        conn.execute("INSERT INTO gpu_jobs(gpu_id,job_id,vram_gib,updated_at) VALUES(?,?,?,?)", (idx, job_id, peak if share else None, dispatcher._admission_now()))
        return idx
    return None
