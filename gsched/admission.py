"""Shared, observational resource decisions; no allocation, probes or signals.

Dispatch consumes these decisions before its existing allocation/launch CAS.
Explanation uses the same functions with recorded compute-node observations.
Reservations describe scheduler accounting, never OS-enforced resource limits.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from types import SimpleNamespace

from . import gpu_admission, resources, state
from .config import project_gpu_enabled
from .execution_policy import digest

MAX_GPUS = 512
MAX_RUNNING = 10_000
MAX_READY = 10_000
MAX_SAMPLE_AGE = 90.0


def running_snapshot(conn):
    rows = [dict(row) for row in conn.execute("SELECT j.id,j.version,j.pgid,j.gpu,j.project,t.spec FROM jobs j LEFT JOIN tasks t ON t.batch_id=j.batch_id AND t.id=j.task_id AND t.version=j.version WHERE j.status='running' ORDER BY j.id LIMIT ?", (MAX_RUNNING + 1,))]
    if len(rows) > MAX_RUNNING or sum(len((row["spec"] or "").encode()) for row in rows) > 4 * 1024 * 1024:
        raise ValueError("running reservation facts exceed 10000 records / 4 MiB")
    return digest(rows)


def policy_context(cfg):
    defaults = {"cpus_total": 0, "max_cpu_jobs": 2, "gpu_job_cpus": 8, "co_locate": False,
                "co_locate_safety": 0.7, "co_locate_max_jobs": 3, "host_mem_total_gib": 0,
                "host_mem_reserve_gib": 16, "host_mem_default_gib": 8, "gpus": []}
    result = {key: cfg.get(key, default) for key, default in defaults.items()}
    fields = {"gpu_quota": 0, "priority": 0, "gpu_affinity": [], "gpu_affinity_hard": False,
              "gpu_enabled": True, "colocate": True}
    result["projects"] = {name: {**{key: item.get(key, default) for key, default in fields.items()},
                                      **{key: item[key] for key in ("max_jobs", "gpu_admission") if key in item}}
                          for name, item in cfg.get("projects", {}).items()}
    return result


def capture_runtime(dispatcher, conn, policy_cfg, memory_sample, outstanding_memory, sampled_at):
    """Called only by the compute daemon, not an explanation reader."""
    allocator = getattr(dispatcher, "allocator", None)
    pool = getattr(allocator, "gpu_list", [])
    if allocator is None or not isinstance(pool, list) or len(pool) > MAX_GPUS:
        return None  # Narrow mock fixtures and excessive observations stay unknown.
    capacities, suppressed = {}, []
    for idx in pool:
        capacity = allocator.mem_total(idx)
        if not resources.finite_number(capacity):
            return None
        capacities[str(idx)] = capacity
        if dispatcher._gpu_dispatch_suppressed(idx):
            suppressed.append(idx)
    samples = {}
    stamp = time.monotonic()
    for idx, value in getattr(dispatcher, "_gpu_admission_samples", {}).items():
        samples[str(idx)] = {**value, "pgids": sorted(value["pgids"]),
                             "age_at_capture_s": stamp - value["sampled_at"]}
        del samples[str(idx)]["sampled_at"]
    from .integration import instance_id
    iid = instance_id(conn)
    try:
        running_hash = running_snapshot(conn)
    except ValueError:
        return None
    return {"schema_version": 1, "node": state.hostname(), "instance_id": iid,
            "captured_at": time.time(), "memory_sampled_at": sampled_at,
            "running_sha256": running_hash,
            "memory_sample": memory_sample, "outstanding_memory_gib": outstanding_memory,
            "cfg": policy_context(dispatcher.cfg), "policy_cfg": policy_context(policy_cfg),
            "pool": pool, "capacities_gib": capacities,
            "gpu_max_jobs": {str(idx): value for idx, value in getattr(dispatcher, "_gpu_max_jobs", {}).items()},
            "suppressed": suppressed, "frozen": sorted(getattr(dispatcher, "_frozen_gpus", set())),
            "gpu_samples": samples}


def publish_runtime(value):
    if value is not None:
        try:
            if len(json.dumps(value, allow_nan=False).encode()) > 2 * 1024 * 1024:
                raise ValueError("admission observation exceeds capacity")
            resources.write_private_json("daemon.admission.json", value)
        except (OSError, ValueError, TypeError):
            # Diagnostic retention is not a new dispatch fence. The actual
            # allocator decisions still run; readers see missing/stale evidence.
            logging.getLogger(__name__).warning("admission observation unavailable")


def recorded_runtime(cfg, iid):
    """Do not hide missing, stale, foreign or config-lagging observations."""
    try:
        value = resources.read_private_json("daemon.admission.json")
        age = time.time() - float(value["captured_at"])
        if not 0 <= age <= MAX_SAMPLE_AGE:
            return None, "runtime_observation_stale_or_future", age
        if value["node"] != state.hostname() or value["instance_id"] != iid:
            return None, "runtime_identity_mismatch", age
        if policy_context(cfg) != value["cfg"] or policy_context(cfg) != value["policy_cfg"]:
            return None, "runtime_configuration_lag", age
        if (value.get("schema_version") != 1 or not isinstance(value.get("pool"), list)
                or len(value["pool"]) > MAX_GPUS or any(type(idx) is not int or idx < 0 for idx in value["pool"])
                or len(set(value["pool"])) != len(value["pool"])
                or not isinstance(value.get("capacities_gib"), dict)
                or any(not resources.finite_number(v) for v in value["capacities_gib"].values())
                or not resources.finite_number(value.get("outstanding_memory_gib"))):
            raise ValueError("invalid admission observation")
        pool = set(value["pool"])
        if set(value["capacities_gib"]) != {str(idx) for idx in pool}:
            raise ValueError("incomplete capacity observation")
        for field in ("suppressed", "frozen"):
            if (not isinstance(value.get(field), list) or len(value[field]) > MAX_GPUS
                    or any(type(idx) is not int or idx not in pool for idx in value[field])):
                raise ValueError("invalid transient GPU gate")
        if (not isinstance(value.get("gpu_max_jobs"), dict) or len(value["gpu_max_jobs"]) > MAX_GPUS
                or any(type(cap) is not int or cap <= 0 for cap in value["gpu_max_jobs"].values())):
            raise ValueError("invalid GPU count limits")
        if not isinstance(value.get("gpu_samples"), dict) or len(value["gpu_samples"]) > MAX_GPUS:
            raise ValueError("invalid GPU observations")
        for key, observation in value["gpu_samples"].items():
            if (key not in value["capacities_gib"] or not isinstance(observation, dict)
                    or not isinstance(observation.get("pgids"), list) or len(observation["pgids"]) > MAX_RUNNING
                    or any(type(pid) is not int or pid <= 0 for pid in observation["pgids"])
                    or any(not resources.finite_number(observation.get(field)) for field in ("total_gib", "free_gib", "age_at_capture_s"))
                    or not 0 <= observation["free_gib"] <= observation["total_gib"] or observation["total_gib"] <= 0):
                raise ValueError("invalid per-card observation")
        if not resources.finite_number(value.get("memory_sampled_at")):
            raise ValueError("invalid memory timestamp")
        sample = value.get("memory_sample")
        if sample is not None and (not isinstance(sample, dict) or any(not resources.finite_number(sample.get(key)) for key in ("MemTotal", "MemAvailable"))):
            raise ValueError("invalid host memory observation")
        return value, None, age
    except FileNotFoundError:
        return None, "runtime_observation_missing", None
    except (OSError, KeyError, ValueError, TypeError, OverflowError, RecursionError):
        return None, "runtime_observation_unreadable", None


def explain(conn, cfg, batch, job, spec):
    """One DB snapshot plus separately timestamped daemon observations, not a lock."""
    from .dispatcher import Dispatcher
    from .integration import instance_id
    from .config import parse_gpus
    iid = instance_id(conn)
    runtime, error, age = recorded_runtime(cfg, iid)
    running_hash = running_snapshot(conn)
    usage_matches = bool(runtime and runtime.get("running_sha256") == running_hash)
    adapter = Dispatcher.__new__(Dispatcher)  # No constructor, probes, logs, owner or writer.
    adapter.cfg = cfg
    adapter._projects = cfg.get("projects", {})
    pool, _, caps = parse_gpus(cfg)
    if runtime is None and not pool:
        pool = [row[0] for row in conn.execute("SELECT idx FROM gpus ORDER BY idx LIMIT ?", (MAX_GPUS + 1,))]
    pool = runtime["pool"] if runtime else pool
    if len(pool) > MAX_GPUS:
        raise ValueError("GPU registry exceeds 512-card explanation bound")
    capacity = runtime["capacities_gib"] if runtime else {str(row["idx"]): row["mem_total_gib"] or 0 for row in conn.execute("SELECT idx,mem_total_gib FROM gpus LIMIT ?", (MAX_GPUS,))}
    adapter.allocator = SimpleNamespace(gpu_list=pool, _mem_cache={int(key): value for key, value in capacity.items()},
                                        mem_total=lambda idx: capacity.get(str(idx), 0),
                                        vram_used=lambda db, idx: float(db.execute("SELECT COALESCE(SUM(vram_gib),0) FROM gpu_jobs WHERE gpu_id=?", (idx,)).fetchone()[0]),
                                        job_count=lambda db, idx: db.execute("SELECT COUNT(*) FROM gpu_jobs WHERE gpu_id=?", (idx,)).fetchone()[0])
    adapter._gpu_max_jobs = {int(key): value for key, value in runtime["gpu_max_jobs"].items()} if runtime else caps
    adapter._frozen_gpus = set(runtime["frozen"]) if runtime else set()
    adapter._gpu_dispatch_suppressed = lambda idx: runtime is None or idx in runtime["suppressed"]
    adapter._gpu_admission_samples = {}
    if runtime:
        for key, value in runtime["gpu_samples"].items():
            adapter._gpu_admission_samples[int(key)] = {**value, "pgids": set(value["pgids"]),
                                                       "sampled_at": time.monotonic() - age - value["age_at_capture_s"]}
    request = spec.get("resources") or {}
    if not isinstance(request, dict) or isinstance(request.get("gpu", 1), bool) or request.get("gpu", 1) not in (0, 1):
        raise ValueError("resources 必须是对象且 gpu 必须为 0 或 1")
    cpu_only = request.get("gpu", 1) == 0
    parallel = spec.get("max_parallel")
    if parallel is not None:
        parallel = int(parallel)
        if parallel <= 0:
            raise ValueError("max_parallel 必须为正整数")
    used_cpu = adapter._cpu_in_use(conn)
    project = job["project"] or batch["project"]
    project_running = conn.execute("SELECT COUNT(*) FROM jobs j JOIN batches b ON b.id=j.batch_id WHERE j.status='running' AND j.gpu IS NOT NULL AND COALESCE(j.project,b.project)=?", (project,)).fetchone()[0]
    memory_age = time.time() - float(runtime["memory_sampled_at"]) if runtime else None
    memory_sample = runtime["memory_sample"] if usage_matches and memory_age is not None and 0 <= memory_age <= MAX_SAMPLE_AGE else None
    budget = budgets(cfg, cfg, project, cpu_only=cpu_only, cpus=adapter._task_cpus(spec),
                     memory=resources.host_mem_gib(spec, cfg), parallel=parallel,
                     used_cpu=used_cpu, cpu_jobs=conn.execute("SELECT COUNT(*) FROM jobs WHERE status='running' AND gpu IS NULL").fetchone()[0],
                     used_memory=resources.memory_usage(conn, cfg), memory_sample=memory_sample,
                     outstanding_memory=runtime["outstanding_memory_gib"] if runtime else 0, launched_memory=0,
                     batch_running=conn.execute("SELECT COUNT(*) FROM jobs WHERE batch_id=? AND status='running'", (batch["id"],)).fetchone()[0],
                     project_running=project_running)
    gpu = gpu_plan(conn, adapter, job["id"], spec, project) if not cpu_only else None
    reasons = list(budget["reasons"])
    unknown = list(budget["unknown"])
    from . import storage
    storage_check = storage.explain(conn, cfg, job, spec)
    reasons.extend(storage_check["reasons"])
    unknown.extend(storage_check["unknown"])
    if gpu:
        reasons += gpu["reasons"]
        unknown += sorted({key for card in gpu["candidates"] for key in card.get("unknown", [])})
    if error and (not cpu_only or cfg.get("host_mem_total_gib", 0)):
        unknown.append(error)
    if runtime and not usage_matches and cfg.get("host_mem_total_gib", 0):
        unknown.append("running_reservations_changed_since_memory_sample")
    latest = conn.execute("SELECT MAX(version) FROM jobs WHERE batch_id=? AND task_id=?", (batch["id"], job["task_id"])).fetchone()[0]
    if job["version"] != latest:
        reasons.append("not_latest_version")
    if job["status"] not in {"pending", "waiting_dep", "waiting_quota"}:
        reasons.append("not_pending")
    if batch["status"] != "active":
        reasons.append("batch_" + batch["status"])
    if resources.drain_state() is not None:
        reasons.append("draining")
    if job["status"] == "waiting_dep":
        reasons.append("dependency")
    retry_backoff = 0.0
    if job["retries"] and job["finished_at"]:
        try:
            finished = datetime.strptime(job["finished_at"], "%Y-%m-%d %H:%M:%S")
            retry_backoff = max(0.0, 30 - (datetime.now() - finished).total_seconds())
        except (ValueError, TypeError):
            unknown.append("retry_backoff_timestamp")
    if retry_backoff:
        reasons.append("retry_backoff")
    # These gates require actual compute-node dispatch work; a recorded
    # resource fit never asserts a successful artifact/marker/owner/lease check.
    external = ["dependency_artifacts", "launch_marker_and_execution_identity", "recovery_smoke_and_retry", "final_launch_CAS_and_kernel_permissions"]
    ready = conn.execute("SELECT j.id,j.rowid AS position,b.priority,b.project AS batch_project,j.project FROM jobs j JOIN batches b ON b.id=j.batch_id WHERE j.status IN ('pending','waiting_quota') AND b.status='active' AND j.version=(SELECT MAX(k.version) FROM jobs k WHERE k.batch_id=j.batch_id AND k.task_id=j.task_id) ORDER BY j.rowid LIMIT ?", (MAX_READY + 1,)).fetchall()
    order_truncated = len(ready) > MAX_READY
    ordered = sorted(ready[:MAX_READY], key=lambda row: (-adapter._project_priority(row["project"] or row["batch_project"]), -row["priority"], row["position"]))
    rank = next((index + 1 for index, row in enumerate(ordered) if row["id"] == job["id"]), None) if not order_truncated else None
    return {"schema_version": 1, "contract": "sched-admission-explain-v1", "query": "admission_explain",
            "instance_id": iid, "batch_id": batch["id"], "task_id": job["task_id"], "job_id": job["id"],
            "version": job["version"], "spec_sha256": digest(spec), "batch_revision": dict(batch).get("revision"),
            "status": job["status"], "batch_status": batch["status"], "observed_at": time.time(),
            "retry_backoff_remaining_s": retry_backoff,
            "runtime_observation": {"captured_at": runtime["captured_at"] if runtime else None, "age_s": age,
                                    "host_memory_age_s": memory_age, "error": error, "expires_after_s": MAX_SAMPLE_AGE,
                                    "usage_snapshot_matches": usage_matches,
                                    "fresh_vram_expires_after_s": gpu_admission.MAX_SAMPLE_AGE_SEC},
            "budget": budget, "gpu": gpu, "reasons": list(dict.fromkeys(reasons)), "unknown": sorted(set(unknown)),
            "order": {"rank_before_gates": rank, "truncated": order_truncated, "limit": MAX_READY,
                      "policy": "project_priority_desc_batch_priority_desc_fifo", "preemption": False, "smaller_jobs_may_backfill": True},
            "resource_fit": None if unknown else budget["allowed"] and (gpu is None or gpu["allowed"]) and storage_check["allowed"] is True,
            "unchecked_dispatch_gates": external, "admission_granted": False, "effect": "none",
            "source": "private_database_snapshot_and_separately_recorded_daemon_observations",
            "hard_isolation": False, "storage": storage_check,
            "disk_inode_quota_admission": "enabled" if storage_check["enabled"] else "disabled",
            "continuous_lease_validation": "not_implemented"}


def budgets(cfg, policy_cfg, project, *, cpu_only, cpus, memory, parallel,
            used_cpu, cpu_jobs, used_memory, memory_sample, outstanding_memory,
            launched_memory, batch_running, project_running):
    """Return every budget rejection in the historical gate order."""
    quota = int((cfg.get("projects", {}).get(project) or {}).get("gpu_quota", 0) or 0)
    total = int(cfg.get("cpus_total", 0) or 0)
    max_cpu = int(cfg.get("max_cpu_jobs", 2))
    mem_limit = float(policy_cfg.get("host_mem_total_gib", 0))
    reserve = float(policy_cfg.get("host_mem_reserve_gib", 16))
    enabled = project_gpu_enabled(policy_cfg, project)
    checks = {
        "project_gpu_enabled": {"applies": not cpu_only, "allowed": cpu_only or enabled, "enabled": enabled},
        "project_gpu_quota": {"applies": not cpu_only and quota > 0, "allowed": cpu_only or quota <= 0 or project_running < quota,
                              "limit": quota, "used": project_running, "unit": "running_gpu_jobs", "zero_means": "unlimited"},
        "batch_parallel": {"applies": parallel is not None, "allowed": not parallel or batch_running < parallel,
                           "limit": parallel, "used": batch_running},
        "cpu_reservation": {"applies": total > 0, "allowed": total <= 0 or used_cpu + cpus <= total,
                            "requested": cpus, "used": used_cpu, "limit": total, "hard_isolation": False},
        "host_memory": {"applies": mem_limit > 0,
                        "allowed": resources.memory_available(policy_cfg, used_memory, memory, memory_sample, launched_memory, outstanding_memory),
                        "requested_gib": memory, "reserved_gib": used_memory, "configured_limit_gib": mem_limit,
                        "control_reserve_gib": reserve, "sample": memory_sample,
                        "outstanding_gib": outstanding_memory, "launched_since_sample_gib": launched_memory,
                        "effective_budget_gib": min(mem_limit, max(0, memory_sample["MemTotal"] - reserve)) if memory_sample and mem_limit > 0 else None,
                        "physical_headroom_gib": memory_sample["MemAvailable"] - launched_memory - outstanding_memory - reserve if memory_sample else None,
                        "hard_isolation": False},
        "cpu_only_concurrency": {"applies": cpu_only and total <= 0,
                                 "allowed": not cpu_only or total > 0 or cpu_jobs < max_cpu,
                                 "limit": max_cpu, "used": cpu_jobs},
    }
    codes = {"project_gpu_enabled": "project_gpu_disabled", "project_gpu_quota": "quota",
             "batch_parallel": "parallel", "cpu_reservation": "cpu", "host_memory": "host_memory",
             "cpu_only_concurrency": "cpu"}
    reasons = [codes[key] for key, check in checks.items() if not check["allowed"]]
    unknown = ["host_memory_sample"] if mem_limit > 0 and memory_sample is None else []
    return {"allowed": not reasons, "reasons": list(dict.fromkeys(reasons)), "unknown": unknown, "checks": checks}


def gpu_plan(conn, dispatcher, job_id, spec, project):
    """Pure selection used by both allocator mutation and explanation.

    Preserve legacy free-card unknown-capacity and hard-empty-affinity semantics;
    do not silently turn observational explanation into a new scheduling policy.
    """
    if gpu_admission.policy(dispatcher.cfg, project) is not None:
        return gpu_admission.plan(conn, dispatcher, job_id, spec, project,
                                  getattr(dispatcher, "_gpu_admission_samples", {}))
    request = spec.get("resources") or {}
    requested_share = bool(request.get("gpu_share"))
    share = requested_share and bool(dispatcher.cfg.get("co_locate", False))
    project_cfg = dispatcher._projects.get(project or "") or {}
    share = share and project_cfg.get("colocate") is not False
    warnings = []
    if requested_share and not share:
        warnings.append("sharing_downgraded_to_exclusive")
    task_vram = None
    cached_peak = None
    if share:
        task_vram = float(request.get("vram_gib", 0.0) or 0.0)
        if request.get("profile_key"):
            from .dispatcher import _profile_cache_key
            row = conn.execute("SELECT peak_gib FROM profile_cache WHERE profile_key=?",
                               (_profile_cache_key(project, request["profile_key"]),)).fetchone()
            if row and row["peak_gib"]:
                cached_peak = float(row["peak_gib"])
                task_vram = max(task_vram, cached_peak)
    else:
        try:
            task_vram_declared = request.get("vram_gib")
            task_vram_declared = float(task_vram_declared) if task_vram_declared is not None else None
        except (TypeError, ValueError):
            task_vram_declared = None
    affinity = dispatcher._project_affinity(project) if project else []
    hard = dispatcher._project_affinity_hard(project) if project else False
    pool = list(dispatcher.allocator.gpu_list)
    if hard and affinity:
        order = [idx for idx in affinity if idx in set(pool)]
    else:
        order = list(affinity) + [idx for idx in pool if idx not in affinity]
    safety = float(dispatcher.cfg.get("co_locate_safety", 0.7))
    all_cap = int(dispatcher.cfg.get("co_locate_max_jobs", 3))
    candidates, selected, cap_skipped = [], None, False
    eligible_order = set(order)
    for position, idx in enumerate(order + [idx for idx in pool if idx not in eligible_order]):
        row = conn.execute("SELECT status, quarantined, ignore_until FROM gpus WHERE idx=?", (idx,)).fetchone()
        suppressed = dispatcher._gpu_dispatch_suppressed(idx)
        reasons = []
        if idx not in eligible_order:
            reasons.append("hard_affinity_excluded")
        if suppressed:
            reasons.append("utilization_debounce_or_probe_uncertain")
        if row is None:
            reasons.append("gpu_registry_missing")
        elif row["quarantined"]:
            reasons.append("quarantined")
        status = row["status"] if row else None
        cache = getattr(dispatcher.allocator, "_mem_cache", {})
        cap = cache.get(idx) if isinstance(cache, dict) else None
        used, count, project_count = 0.0, 0, 0
        card_cap = dispatcher._gpu_max_jobs.get(idx) if share else None
        count_cap = min(all_cap, int(card_cap)) if card_cap is not None else all_cap
        project_cap = project_cfg.get("max_jobs")
        frozen = idx in dispatcher._frozen_gpus if share else False
        if not share:
            if status != "free":
                reasons.append("exclusive_requires_free_card")
            if task_vram_declared is not None and status == "free":
                cap = dispatcher.allocator.mem_total(idx)
                if cap > 0 and task_vram_declared > cap:
                    reasons.append("declared_vram_exceeds_capacity")
        else:
            if status not in {"free", "assigned"}:
                reasons.append("gpu_state_not_packable")
            if status in {"free", "assigned"}:
                cap = dispatcher.allocator.mem_total(idx)
                if status == "assigned":
                    used = dispatcher.allocator.vram_used(conn, idx)
                    count = dispatcher.allocator.job_count(conn, idx)
                    project_count = conn.execute("SELECT COUNT(*) FROM gpu_jobs gj JOIN jobs j ON j.id=gj.job_id WHERE gj.gpu_id=? AND j.project=?", (idx, project)).fetchone()[0]
                    if frozen:
                        reasons.append("frozen")
                    if conn.execute("SELECT 1 FROM gpu_jobs WHERE gpu_id=? AND vram_gib IS NULL LIMIT 1", (idx,)).fetchone():
                        reasons.append("exclusive_assignment_present")
                    if cap <= 0:
                        reasons.append("capacity_unknown_for_existing_pack")
                    if count >= count_cap:
                        reasons.append("global_or_card_pack_limit")
                        cap_skipped = True
                    if project_cap is not None and project_count >= int(project_cap):
                        reasons.append("project_pack_limit")
                        cap_skipped = True
                if cap > 0 and used + task_vram > safety * cap:
                    reasons.append("packing_vram_budget")
        load = ((used + task_vram) / cap if cap and cap > 0 else 0.0) if share else None
        candidates.append({"idx": idx, "position": position, "status": status, "quarantined": bool(row and row["quarantined"]),
                           "ignore_until": row["ignore_until"] if row else None, "suppressed": suppressed, "frozen": frozen,
                           "preferred": idx in affinity, "capacity_gib": cap, "reserved_gib": used, "load_after": load,
                           "job_count": count, "job_limit": count_cap if share else None,
                           "project_job_count": project_count, "project_job_limit": project_cap if share else None,
                           "allowed": not reasons, "reasons": reasons})
    valid = [card for card in candidates if card["allowed"]]
    if valid:
        if share:
            preferred = [card for card in valid if card["preferred"]]
            selected = min(preferred or valid, key=lambda card: (card["load_after"], card["position"]))["idx"]
        else:
            selected = valid[0]["idx"]
        chosen = next(card for card in candidates if card["idx"] == selected)
        if chosen["status"] == "free" and not chosen["capacity_gib"]:
            warnings.append("legacy_free_unknown_capacity_is_not_a_hard_capacity_check")
    scope = ("project" if cap_skipped else "all") if share else ("project" if hard and affinity else "all")
    return {"policy": "legacy", "selected": selected, "allowed": selected is not None,
            "share": share, "requested_share": requested_share, "assignment_vram_gib": task_vram,
            "declared_vram_gib": request.get("vram_gib"), "cached_peak_gib": cached_peak,
            "safety": safety if share else None, "affinity": affinity, "affinity_hard": hard,
            "pool": pool, "candidates": candidates, "reject_scope": scope, "warnings": warnings,
            "reasons": [] if selected is not None else ["gpu"], "hard_isolation": False}


def allocate(conn, dispatcher, job_id, spec, project):
    """Apply the selected plan in the caller's existing transaction."""
    plan = gpu_plan(conn, dispatcher, job_id, spec, project)
    dispatcher._last_gpu_plan = plan
    if plan["policy"] == "fresh_vram":
        return gpu_admission.apply_plan(conn, dispatcher, job_id, plan)
    idx = plan["selected"]
    dispatcher._assign_reject_scope = plan["reject_scope"]
    if "sharing_downgraded_to_exclusive" in plan["warnings"]:
        dispatcher.log_line(f"job {job_id}: gpu_share 请求未获全局/项目许可，按独占运行")
    if idx is None:
        if plan["share"] and plan["reject_scope"] == "project" and job_id not in dispatcher._cap_warned:
            dispatcher._cap_warned.add(job_id)
            dispatcher.log_line(f"job {job_id}: 暂无余量卡 (打包上限)，等待自然排水")
        return None
    if plan["share"]:
        dispatcher._cap_warned.discard(job_id)
    row = conn.execute("SELECT status FROM gpus WHERE idx=?", (idx,)).fetchone()
    if not plan["share"] or row["status"] == "free":
        conn.execute("UPDATE gpus SET status='assigned',job_id=?,updated_at=? WHERE idx=?", (job_id, state.now(), idx))
    conn.execute("INSERT OR REPLACE INTO gpu_jobs(gpu_id,job_id,vram_gib,updated_at) VALUES(?,?,?,?)",
                 (idx, job_id, plan["assignment_vram_gib"], state.now()))
    return idx
