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


def decision(conn, cfg, idx, job_id, spec, project, snapshot):
    """All fresh-VRAM reasons; the same decision also gates real launch."""
    configured = policy(cfg, project)
    reasons, unknown = [], []
    if configured is None:
        return {"allowed": False, "reasons": ["fresh_vram_policy_missing"], "unknown": [], "idx": idx}
    row = conn.execute("SELECT * FROM gpus WHERE idx=?", (idx,)).fetchone()
    value = snapshot.get(idx)
    share = bool((spec.get("resources") or {}).get("gpu_share")) and cfg.get("co_locate") is True and cfg.get("projects", {}).get(project, {}).get("colocate") is not False
    if row is None:
        reasons.append("gpu_registry_missing")
    else:
        if row["quarantined"]:
            reasons.append("quarantined")
        if row["ignore_until"]:
            reasons.append("ignored")
        if row["status"] not in ("free", "unmanaged", "assigned"):
            reasons.append("gpu_state_not_admissible")
        if row["status"] == "assigned" and conn.execute("SELECT 1 FROM gpu_jobs WHERE gpu_id=?", (idx,)).fetchone() is None:
            reasons.append("orphan_registry_assignment")
    valid_sample = (isinstance(value, dict) and type(value.get("pgids")) is set
                    and all(type(pid) is int and pid > 0 for pid in value["pgids"])
                    and all(type(value.get(key)) in (int, float) and math.isfinite(value[key]) for key in ("total_gib", "free_gib", "sampled_at"))
                    and 0 <= value["free_gib"] <= value["total_gib"] and value["total_gib"] > 0)
    age = time.monotonic() - value["sampled_at"] if valid_sample else None
    if age is None or not 0 <= age <= MAX_SAMPLE_AGE_SEC:
        reasons.append("gpu_sample_missing_or_stale")
        unknown.append("fresh_vram_sample")
    assignments = list(conn.execute("SELECT gj.job_id,gj.vram_gib,j.pgid,j.status,j.project,t.spec FROM gpu_jobs gj LEFT JOIN jobs j ON j.id=gj.job_id LEFT JOIN tasks t ON t.batch_id=j.batch_id AND t.id=j.task_id AND t.version=j.version WHERE gj.gpu_id=? AND gj.job_id!=?", (idx, job_id)))
    if assignments and not share:
        reasons.append("exclusive_assignment_conflict")
    active = {r["pgid"] for r in assignments if r["status"] == "running" and r["pgid"] is not None}
    known = {r[0] for r in conn.execute("SELECT pgid FROM jobs WHERE pgid IS NOT NULL")}
    external = None
    if valid_sample:
        if (value["pgids"] & known) - active:
            reasons.append("unresolved_owned_processes")
        external = value["pgids"] - active
        if external and not configured["allow_external_occupancy"]:
            reasons.append("external_occupancy_forbidden")
        if row is not None and row["status"] == "unmanaged" and not external:
            reasons.append("unmanaged_without_external_attribution")
    required = minimum(conn, job_id, spec, configured)
    budget = reservation(spec, required, conn, project)
    outstanding = 0.0
    outstanding_known = True
    for bound in assignments:
        if bound["status"] != "running" or bound["pgid"] is None or bound["vram_gib"] is None or bound["spec"] is None:
            reasons.append("assignment_identity_or_reservation_unknown")
            outstanding_known = False
            continue
        try:
            other = json.loads(bound["spec"])
            other_policy = policy(cfg, bound["project"])
            outstanding += max(float(bound["vram_gib"]), reservation(other, minimum(conn, bound["job_id"], other, other_policy) if other_policy else DEFAULT_MIN_FREE_GIB, conn, bound["project"]))
        except (ValueError, TypeError, AttributeError, OverflowError):
            reasons.append("assignment_spec_unknown")
            outstanding_known = False
    capacity, headroom = None, None
    if valid_sample and row is not None:
        configured_capacity = row["mem_total_gib"]
        capacity = min(value["total_gib"], float(configured_capacity)) if configured_capacity is not None else value["total_gib"]
        if not math.isfinite(capacity) or capacity <= 0:
            reasons.append("capacity_unknown")
        elif outstanding_known:
            if outstanding + budget > capacity:
                reasons.append("reserved_vram_exceeds_capacity")
            if share and outstanding + budget > cfg.get("co_locate_safety", 0.7) * capacity:
                reasons.append("packing_vram_budget")
        if outstanding_known:
            headroom = value["free_gib"] - outstanding
            if headroom < budget:
                reasons.append("fresh_free_vram_headroom")
    return {"idx": idx, "allowed": not reasons, "reasons": list(dict.fromkeys(reasons)), "unknown": unknown,
            "status": row["status"] if row else None, "capacity_gib": capacity, "sample_age_s": age,
            "sample_free_gib": value["free_gib"] if valid_sample else None, "minimum_free_gib": required,
            "reservation_gib": budget, "outstanding_gib": outstanding if outstanding_known else None,
            "physical_headroom_gib": headroom, "external_process_count": len(external) if external is not None else None,
            "allow_external_occupancy": configured["allow_external_occupancy"], "share": share, "hard_isolation": False}


def permits(conn, cfg, idx, job_id, spec, project, snapshot):
    return decision(conn, cfg, idx, job_id, spec, project, snapshot)["allowed"]


def plan(conn, dispatcher, job_id, spec, project, snapshot):
    """Pure existing fresh-VRAM ordering and all per-card constraints."""
    affinity = dispatcher._project_affinity(project)
    hard = dispatcher._project_affinity_hard(project)
    pool = list(dispatcher.allocator.gpu_list)
    order = [idx for idx in affinity if idx in set(pool)]
    if not hard:
        order += [idx for idx in pool if idx not in order]
    share = bool((spec.get("resources") or {}).get("gpu_share")) and dispatcher.cfg.get("co_locate") is True and dispatcher._projects.get(project, {}).get("colocate") is not False
    existing = conn.execute("SELECT gpu_id FROM gpu_jobs WHERE job_id=?", (job_id,)).fetchone()
    candidates, selected = [], None
    eligible_order = set(order)
    for idx in order + [idx for idx in pool if idx not in eligible_order]:
        card = decision(conn, dispatcher.cfg, idx, job_id, spec, project, snapshot)
        if idx not in eligible_order:
            card["reasons"].append("hard_affinity_excluded")
        if idx in dispatcher._frozen_gpus:
            card["reasons"].append("frozen")
        count = conn.execute("SELECT COUNT(*) FROM gpu_jobs WHERE gpu_id=?", (idx,)).fetchone()[0]
        cap = min(dispatcher.cfg.get("co_locate_max_jobs", 3), dispatcher._gpu_max_jobs.get(idx, 64))
        pcap = dispatcher._projects.get(project, {}).get("max_jobs", cap)
        project_count = conn.execute("SELECT COUNT(*) FROM gpu_jobs gj JOIN jobs j ON j.id=gj.job_id WHERE gj.gpu_id=? AND j.project=?", (idx, project)).fetchone()[0]
        if existing is None:
            if count >= cap:
                card["reasons"].append("global_or_card_pack_limit")
            if project_count >= pcap:
                card["reasons"].append("project_pack_limit")
            if share and isinstance(snapshot.get(idx), dict) and snapshot[idx].get("total_gib"):
                packed = conn.execute("SELECT COALESCE(SUM(vram_gib),0) FROM gpu_jobs WHERE gpu_id=?", (idx,)).fetchone()[0]
                packing_capacity = min(snapshot[idx]["total_gib"], dispatcher.allocator.mem_total(idx) or snapshot[idx]["total_gib"])
                if packed + card["reservation_gib"] > dispatcher.cfg.get("co_locate_safety", 0.7) * packing_capacity:
                    card["reasons"].append("packing_vram_budget")
        elif existing[0] != idx:
            card["reasons"].append("existing_binding_other_card")
        card.update(allowed=not card["reasons"], job_count=count, job_limit=cap,
                    project_job_count=project_count, project_job_limit=pcap, preferred=idx in affinity)
        card["reasons"] = list(dict.fromkeys(card["reasons"]))
        candidates.append(card)
        if selected is None and card["allowed"]:
            selected = idx
    peak = next((card["reservation_gib"] for card in candidates if card["idx"] == selected), None)
    return {"policy": "fresh_vram", "selected": selected, "allowed": selected is not None,
            "share": share, "assignment_vram_gib": peak if share else None, "existing_binding": existing is not None,
            "affinity": affinity, "affinity_hard": hard, "pool": pool, "candidates": candidates,
            "reject_scope": "request", "warnings": [], "reasons": [] if selected is not None else ["gpu"],
            "hard_isolation": False}


def apply_plan(conn, dispatcher, job_id, selection):
    idx = selection["selected"]
    if idx is None or selection["existing_binding"]:
        return idx
    conn.execute("UPDATE gpus SET status='assigned',job_id=COALESCE(job_id,?),updated_at=? WHERE idx=?", (job_id, dispatcher._admission_now(), idx))
    conn.execute("INSERT INTO gpu_jobs(gpu_id,job_id,vram_gib,updated_at) VALUES(?,?,?,?)", (idx, job_id, selection["assignment_vram_gib"], dispatcher._admission_now()))
    return idx


def assign(conn, dispatcher, job_id, spec, project, snapshot):
    return apply_plan(conn, dispatcher, job_id, plan(conn, dispatcher, job_id, spec, project, snapshot))
