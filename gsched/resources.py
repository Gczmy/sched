"""Host-memory admission and durable drain controls (no worker-side waiting)."""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import stat
import time
import uuid

from . import state

GIB = 1024 ** 3
WAIT_REASONS = {"cpu", "host_memory", "gpu", "parallel"}


def finite_number(value, *, positive=False):
    try:
        return (isinstance(value, (int, float)) and not isinstance(value, bool)
                and math.isfinite(value) and (value > 0 if positive else value >= 0))
    except OverflowError:
        return False


def host_mem_gib(spec, cfg):
    value = (spec.get("resources") or {}).get(
        "host_mem_gib", cfg.get("host_mem_default_gib", 8))
    if not finite_number(value, positive=True):
        raise ValueError("resources.host_mem_gib 必须为有限正数")
    return float(value)


def memory_usage(conn, cfg):
    used = 0.0
    for row in conn.execute(
        "SELECT t.spec FROM jobs j LEFT JOIN tasks t ON t.batch_id=j.batch_id"
        " AND t.id=j.task_id AND t.version=j.version WHERE j.status='running'"
    ):
        try:
            used += host_mem_gib(json.loads(row["spec"] or "{}"), cfg)
        except (TypeError, ValueError, AttributeError):
            # Unknown running reservations must not manufacture headroom.
            used += float(cfg.get("host_mem_total_gib", 0) or
                          cfg.get("host_mem_default_gib", 8))
    return used


def host_memory():
    """Physical-node sample. Never call this on a gateway to describe its node."""
    try:
        values = {}
        for line in Path("/proc/meminfo").read_text().splitlines():
            key, _, rest = line.partition(":")
            if key in {"MemTotal", "MemAvailable"}:
                values[key] = int(rest.split()[0]) * 1024 / GIB
        if set(values) != {"MemTotal", "MemAvailable"}:
            return None
        return values
    except (OSError, ValueError, IndexError):
        return None


def memory_outstanding(conn, cfg):
    """Unfaulted reservations, subtracting only readable PSS of owned trees.

    RSS double-counts DataLoader/shared pages. Missing /proc records keep the
    full reservation outstanding; process-tree sampling is observational only.
    """
    rows = conn.execute(
        "SELECT j.pgid, t.spec FROM jobs j LEFT JOIN tasks t ON t.batch_id=j.batch_id"
        " AND t.id=j.task_id AND t.version=j.version WHERE j.status='running'"
    ).fetchall()
    if not rows:
        return 0.0
    parents = {}
    try:
        for path in Path('/proc').iterdir():
            if not path.name.isdecimal():
                continue
            try:
                fields = (path / 'stat').read_text().rsplit(')', 1)[1].split()
                parents[int(path.name)] = int(fields[1])
            except (OSError, ValueError, IndexError):
                continue
    except OSError:
        pass
    outstanding = 0.0
    counted = set()
    for row in rows:
        try:
            reserved = host_mem_gib(json.loads(row['spec'] or '{}'), cfg)
        except (TypeError, ValueError, AttributeError):
            reserved = float(cfg.get('host_mem_total_gib', 0) or 8)
        owned = {row['pgid']} if row['pgid'] else set()
        while True:
            extra = {pid for pid, parent in parents.items() if parent in owned} - owned
            if not extra:
                break
            owned.update(extra)
        measured = 0.0
        for pid in owned - counted:
            try:
                for line in (Path('/proc') / str(pid) / 'smaps_rollup').read_text().splitlines():
                    if line.startswith('Pss:'):
                        measured += int(line.split()[1]) * 1024 / GIB
                        break
            except (OSError, ValueError, IndexError):
                continue
        counted.update(owned)
        outstanding += max(0.0, reserved - measured)
    return outstanding


def memory_available(cfg, used, requested, sample, launched_gib=0, outstanding_gib=0):
    """Check reservations and free memory before allocating a CPU/GPU slot.

    Subtract launches in this tick from the sample: the child may not have
    faulted its pages yet. The static budget bounds all running reservations.
    """
    limit = float(cfg.get("host_mem_total_gib", 0))
    if limit <= 0:
        return True
    if sample is None:
        return False
    reserve = float(cfg.get("host_mem_reserve_gib", 16))
    budget = min(limit, max(0, sample["MemTotal"] - reserve))
    return (used + requested <= budget
            and sample["MemAvailable"] - launched_gib - outstanding_gib >= reserve + requested)


def _path(name):
    return os.path.join(state.host_dir(), name)


def read_private_json(name):
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    fd = os.open(_path(name), flags)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_size > 2 * 1024 * 1024
                or info.st_nlink != 1
                or (hasattr(os, "getuid") and info.st_uid != os.getuid())):
            raise ValueError("invalid resource control file")
        with os.fdopen(fd, "r", encoding="utf-8") as stream:
            fd = -1
            value = json.load(stream)
        if not isinstance(value, dict):
            raise ValueError("resource control must be an object")
        return value
    finally:
        if fd != -1:
            os.close(fd)


def write_private_json(name, value):
    path = _path(name)
    temporary = path + "." + uuid.uuid4().hex + ".tmp"
    try:
        with state.open_private_text(temporary, "x") as stream:
            json.dump(value, stream, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def drain_state():
    try:
        value = read_private_json("daemon.drain.json")
        if not isinstance(value.get("stop"), bool):
            raise ValueError("invalid drain state")
        return value
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        # A corrupt request must pause dispatch, but never authorize shutdown.
        return {"stop": False, "invalid": True}


def set_drain(*, stop=False):
    with state.submission_lock():
        write_private_json("daemon.drain.json", {"stop": bool(stop), "requested_at": time.time()})


def resume():
    with state.submission_lock():
        path = _path("daemon.drain.json")
        try:
            info = os.lstat(path)
        except FileNotFoundError:
            return
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or (hasattr(os, "getuid") and info.st_uid != os.getuid())):
            raise ValueError("drain control is not a regular private file")
        os.unlink(path)


def publish_admission(sample, waits):
    write_private_json("daemon.resources.json", {
        "updated_at": time.time(), "sample": sample, "waits": waits,
    })


def admission_snapshot():
    try:
        value = read_private_json("daemon.resources.json")
        age = time.time() - float(value["updated_at"])
        if not 0 <= age <= 90 or not isinstance(value.get("waits"), dict):
            return {}
        sample = value.get("sample")
        if sample is not None and (not isinstance(sample, dict) or not all(
            finite_number(sample.get(key)) for key in ("MemTotal", "MemAvailable")
        )):
            return {}
        return value
    except (OSError, ValueError, KeyError, TypeError):
        return {}
