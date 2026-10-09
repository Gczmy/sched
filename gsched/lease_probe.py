"""Compute-only bounded Slurm JSON reader. Never issues a Slurm mutation."""
from __future__ import annotations

import json
import re
import subprocess
import sys
import time


def number(value):
    if isinstance(value, dict):
        if value.get("set") is not True or value.get("infinite") is not False:
            return None
        value = value.get("number")
    return value if type(value) is int and 0 <= value <= 2 ** 63 - 1 else None


def normalize(raw, job_id, hosts):
    jobs = raw.get("jobs")
    if not isinstance(jobs, list) or len(jobs) != 1:
        raise ValueError("exact Slurm job unavailable")
    job = jobs[0]
    if not isinstance(job, dict) or str(job.get("job_id")) != job_id:
        raise ValueError("Slurm job identity mismatch")
    states = job.get("job_state")
    if isinstance(states, str):
        states = [states]
    if not isinstance(states, list) or not states or any(not isinstance(s, str) or not re.fullmatch(r"[A-Z_]{1,64}", s) for s in states):
        raise ValueError("Slurm state unavailable")
    nodes = job.get("nodes")
    if (not isinstance(nodes, str) or len(nodes) > 4096 or not isinstance(hosts, list)
            or len(hosts) > 16384 or any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,255}", h) for h in hosts)):
        raise ValueError("Slurm nodes unavailable")
    result = {"job_id": job_id, "states": states, "nodes": nodes, "hosts": hosts,
              "user_id": number(job.get("user_id")), "start_time": number(job.get("start_time")),
              "end_time": number(job.get("end_time")), "cpus": number(job.get("cpus")),
              "restart_count": number(job.get("restart_cnt"))}
    if any(result[k] is None for k in ("user_id", "start_time", "cpus", "restart_count")):
        raise ValueError("Slurm binding fields incomplete")
    return result


def command(arguments):
    result = subprocess.run(["scontrol", *arguments], capture_output=True, text=True, timeout=2)
    if len(result.stdout.encode()) > 1024 * 1024:
        raise ValueError("Slurm output bound")
    # Error text is neither saved nor interpreted as job termination.
    if result.returncode:
        raise ValueError("Slurm query failed")
    return result.stdout


def sample(job_id):
    started = time.time()
    try:
        if not isinstance(job_id, str) or not re.fullmatch(r"[1-9][0-9]{0,18}", job_id):
            raise ValueError("Slurm job ID absent or unsupported")
        raw = json.loads(command(["--json", "show", "job", job_id]))
        if not isinstance(raw, dict):
            raise ValueError("Slurm JSON unavailable")
        if raw.get("errors", []) or raw.get("warnings", []):
            raise ValueError("Slurm JSON contains diagnostics")
        jobs = raw.get("jobs")
        if jobs == [] and raw.get("errors", []) == [] and raw.get("warnings", []) == []:
            return {"observed_at": started, "known": True, "missing": True, "job_id": job_id}
        nodes = jobs[0].get("nodes") if isinstance(jobs, list) and len(jobs) == 1 and isinstance(jobs[0], dict) else None
        if nodes is None and isinstance(jobs, list) and len(jobs) == 1:
            nodes = ""
            jobs[0]["nodes"] = nodes
        if not isinstance(nodes, str):
            raise ValueError("Slurm nodes unavailable")
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,255}", nodes):
            hosts = [nodes]
        elif re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.,\[\]-]{0,4095}", nodes):
            hosts = command(["show", "hostnames", nodes]).splitlines()
        elif nodes in ("", "(null)", "None"):
            hosts = []
        else:
            raise ValueError("Slurm host list unavailable")
        return {"observed_at": started, "known": True, "job": normalize(raw, job_id, hosts)}
    except (OSError, ValueError, TypeError, RecursionError, subprocess.SubprocessError):
        return {"observed_at": started, "known": False, "reason": "slurm_query_unavailable_or_unsupported"}


def main():
    if sys.platform != "linux":
        raise SystemExit("lease helper requires Linux compute context")
    raw = sys.stdin.read(1025)
    value = json.loads(raw)
    if len(raw) > 1024 or not isinstance(value, dict) or set(value) != {"job_id"}:
        raise SystemExit("invalid lease helper input")
    print(json.dumps(sample(value["job_id"]), allow_nan=False))


if __name__ == "__main__":
    main()
