"""Bounded launch ancestry evidence, not Slurm containment or worker identity."""
from __future__ import annotations

import os
from pathlib import Path
import re

MAX_DEPTH = 64
STEP = re.compile(r"(?:[0-9]{1,10}|batch|extern|interactive)")
IDENTITY = ("pid", "ppid", "pgrp", "session", "start_ticks", "uid", "comm")


def text(path, bound):
    with Path(path).open("rb") as stream:
        raw = stream.read(bound + 1)
    if len(raw) > bound:
        raise ValueError("ancestry evidence exceeds bound")
    return raw.decode("utf-8").strip()


def boot_id():
    value = text("/proc/sys/kernel/random/boot_id", 64)
    if not re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", value):
        raise ValueError("boot identity unavailable")
    return value


def process(pid):
    if type(pid) is not int or not 1 <= pid < 2 ** 31:
        raise ValueError("process identity invalid")
    directory = Path(f"/proc/{pid}")
    raw = text(directory / "stat", 8192)
    prefix = str(pid) + " ("
    end = raw.rfind(")")
    if not raw.startswith(prefix) or end < len(prefix):
        raise ValueError("process stat identity invalid")
    fields = raw[end + 1:].split()
    if len(fields) < 20 or fields[0] in ("Z", "X", "x"):
        raise ValueError("process stat unavailable or exited")
    values = [int(fields[i]) for i in (1, 2, 3, 19)]
    if any(v < 0 for v in values) or values[-1] <= 0:
        raise ValueError("process stat binding invalid")
    return dict(zip(IDENTITY, (pid, *values[:3], values[3], directory.stat().st_uid, raw[len(prefix):end])))


def anchor_context(pid):
    value = process(pid)
    cpus = sorted(os.sched_getaffinity(pid))
    if not cpus or len(cpus) > 65536 or any(type(c) is not int or not 0 <= c < 2 ** 20 for c in cpus):
        raise ValueError("anchor affinity invalid")
    groups = [line.split(":", 2) for line in text(f"/proc/{pid}/cgroup", 8192).splitlines()]
    if not 1 <= len(groups) <= 32 or any(len(g) != 3 or not g[0].isdecimal() or not g[2].startswith("/") for g in groups):
        raise ValueError("anchor cgroup invalid")
    value.update(affinity=cpus, cgroups=[dict(zip(("hierarchy", "controllers", "path"), g)) for g in groups])
    if process(pid) != {key: value[key] for key in IDENTITY}:
        raise ValueError("anchor changed during observation")
    return value


def capture(context, env):
    """Capture before readiness, while daemon.start still owns its CLI child."""
    result = {"known": False, "reason": "launch_ancestry_unavailable"}
    try:
        job, step = env.get("SLURM_JOB_ID"), env.get("SLURM_STEP_ID")
        if not isinstance(job, str) or not re.fullmatch(r"[1-9][0-9]{0,18}", job) or not isinstance(step, str) or not STEP.fullmatch(step):
            raise ValueError("explicit original job and step required")
        boot = boot_id()
        chain, seen, pid = [], set(), context["pid"]
        for _ in range(MAX_DEPTH):
            if pid in seen:
                raise ValueError("ancestry cycle")
            seen.add(pid)
            item = process(pid)
            chain.append(item)
            if len(chain) > 1 and item["comm"] == "slurmstepd" and item["uid"] == 0:
                anchor = anchor_context(chain[-2]["pid"])
                if (anchor["uid"] != context["uid"] or anchor["ppid"] != item["pid"]
                        or not set(context["affinity"]).issubset(anchor["affinity"])
                        or context["cgroups"] != anchor["cgroups"]):
                    raise ValueError("launch anchor context mismatch")
                # Re-read every edge to avoid adopting a reused/reparented PID.
                if any(process(p["pid"]) != p for p in chain) or boot_id() != boot:
                    raise ValueError("launch ancestry changed during capture")
                return {"known": True, "job_id": job, "step_id": step, "boot_id": boot,
                        "chain": chain, "anchor": anchor, "stepd": item,
                        "semantics": "verified_launch_lineage_not_current_daemon_membership"}
            if item["uid"] != context["uid"] or item["ppid"] <= 1:
                break
            pid = item["ppid"]
    except (OSError, ValueError, TypeError, KeyError, AttributeError, UnicodeError):
        pass
    return result


def observe(origin):
    """Inspect only the immutable original anchor; never adopt a new shell."""
    if not origin.get("known"):
        return {"known": False, "reason": "launch_origin_not_captured"}
    try:
        boot = boot_id()
    except (OSError, ValueError, UnicodeError):
        return {"known": False, "reason": "original_anchor_boot_unreadable"}
    if boot != origin["boot_id"]:
        return {"known": True, "invalid": True, "reason": "original_anchor_boot_changed"}
    try:
        anchor = anchor_context(origin["anchor"]["pid"])
        stepd = process(origin["stepd"]["pid"])
        if anchor != origin["anchor"] or stepd != origin["stepd"] or boot_id() != boot:
            return {"known": True, "invalid": True, "reason": "original_launch_anchor_changed"}
        return {"known": True, "invalid": False, "boot_id": boot, "anchor": anchor, "stepd": stepd}
    except FileNotFoundError:
        return {"known": True, "invalid": True, "reason": "original_launch_anchor_absent"}
    except (OSError, ValueError, TypeError, KeyError, AttributeError, UnicodeError):
        return {"known": False, "reason": "original_launch_anchor_unreadable"}


def decide(origin, current, sample, context):
    """Pure verification of frozen kernel and local Slurm tracking evidence."""
    reasons, unknown = [], []
    if not origin.get("known"):
        unknown.append("launch_origin_not_captured")
    elif not current.get("known"):
        unknown.append("original_launch_anchor_unreadable")
    elif current.get("invalid"):
        reasons.append(current["reason"])
    elif (current.get("boot_id") != origin.get("boot_id") or current.get("anchor") != origin.get("anchor")
            or current.get("stepd") != origin.get("stepd")
            or not set(context.get("affinity") or []).issubset(origin["anchor"]["affinity"])
            or context.get("cgroups") != origin["anchor"]["cgroups"]):
        reasons.append("original_launch_anchor_changed")
    tracking = sample.get("launch_tracking", {})
    if not origin.get("known") or not tracking.get("known"):
        unknown.append("slurm_launch_tracking_unknown")
    elif any(tracking.get(k) != origin.get(k) for k in ("job_id", "step_id")) or tracking.get("anchor_pid") != origin["anchor"]["pid"]:
        unknown.append("slurm_launch_tracking_binding_invalid")
    elif type(tracking.get("present")) is not bool:
        unknown.append("slurm_launch_tracking_binding_invalid")
    elif not tracking["present"]:
        reasons.append("original_slurm_anchor_no_longer_tracked")
    return reasons, unknown
