"""Administrator-owned, project-independent execution admission and FD inputs."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import PurePosixPath
import re
import stat
from typing import Any

INTERFACE_VERSION = "sched-execution/v1"
IDENTITY_SCHEMA = "sched_execution_identity/v1"
INTERNAL_FIELD = "_execution_binding"
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
MAX_EXECUTABLE_BYTES = 256 * 1024 * 1024
OWNER_DEFAULTS = {"prepare_timeout_sec": 30, "terminal_retention_sec": 3600}
OWNER_LIMITS = {"prepare_timeout_sec": (1, 300), "terminal_retention_sec": (60, 604800)}


class ExecutionPolicyError(ValueError):
    pass


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _text(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value or "\0" in value or len(value) > 4096:
        raise ExecutionPolicyError(f"{where}: requires a bounded nonempty string")
    return value


def _sha(value: Any, where: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ExecutionPolicyError(f"{where}: requires a lowercase SHA-256")
    return value


def validate_backends(cfg: dict) -> dict[str, dict]:
    raw = cfg.get("execution_backends", {})
    if not isinstance(raw, dict):
        raise ExecutionPolicyError("execution_backends: requires an object")
    result = {}
    for backend_id, profile in raw.items():
        if not isinstance(backend_id, str) or _IDENTIFIER.fullmatch(backend_id) is None:
            raise ExecutionPolicyError("execution backend id is invalid")
        required = {"kind", "executable", "sha256", "argv", "env", "projects", "input_slots"}
        if not isinstance(profile, dict) or not required <= set(profile) or set(profile) - required - {"owner"}:
            raise ExecutionPolicyError(f"execution_backends.{backend_id}: requires keys {sorted(required)} with optional owner")
        if profile["kind"] not in ("linux_fd", "linux_fd_owner"):
            raise ExecutionPolicyError("only built-in linux_fd and linux_fd_owner backends are supported")
        if "owner" in profile:
            owner = profile["owner"]
            if profile["kind"] != "linux_fd_owner" or type(owner) is not dict or set(owner) - set(OWNER_LIMITS):
                raise ExecutionPolicyError("owner settings require linux_fd_owner and known retention keys")
            for key, value in owner.items():
                minimum, maximum = OWNER_LIMITS[key]
                if type(value) not in (int, float) or not minimum <= value <= maximum or not math.isfinite(value):
                    raise ExecutionPolicyError(f"owner.{key} must be finite and within {minimum}..{maximum} seconds")
        executable = _text(profile["executable"], "executable")
        if not os.path.isabs(executable) or os.path.normpath(executable) != executable:
            raise ExecutionPolicyError("backend executable must be a normalized absolute path")
        _sha(profile["sha256"], "backend sha256")
        argv = profile["argv"]
        if not isinstance(argv, list) or not argv or len(argv) > 128:
            raise ExecutionPolicyError("backend argv must be a bounded nonempty list")
        for token in argv:
            _text(token, "argv token")
        env = profile["env"]
        if not isinstance(env, dict) or len(env) > 128:
            raise ExecutionPolicyError("backend env must be an object")
        for key, value in env.items():
            if not isinstance(key, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) is None:
                raise ExecutionPolicyError("backend environment key is invalid")
            if key.startswith("SCHED_") or key == "CUDA_VISIBLE_DEVICES":
                raise ExecutionPolicyError("backend env cannot override scheduler-owned fields")
            if not isinstance(value, str) or "\0" in value or len(value) > 4096:
                raise ExecutionPolicyError("backend environment value is invalid")
        projects = profile["projects"]
        if (not isinstance(projects, list) or not projects
                or any(not isinstance(project, str) for project in projects)
                or len(set(projects)) != len(projects)):
            raise ExecutionPolicyError("backend projects must be unique registered project names")
        if any(not isinstance(p, str) or p not in cfg.get("projects", {}) for p in projects):
            raise ExecutionPolicyError("backend references an unregistered project")
        slots = profile["input_slots"]
        if not isinstance(slots, dict) or len(slots) > 16:
            raise ExecutionPolicyError("input_slots must be a bounded object")
        for slot, rule in slots.items():
            if not isinstance(slot, str) or not slot.isascii() or not slot.isdecimal() or str(int(slot)) != slot:
                raise ExecutionPolicyError("input slot must be a canonical decimal descriptor")
            if int(slot) < 3 or int(slot) > 63 or int(slot) == 4:
                raise ExecutionPolicyError("input slots must be 3 or 5..63; FD4 is scheduler identity")
            if not isinstance(rule, dict) or set(rule) != {"max_bytes"}:
                raise ExecutionPolicyError("input slot requires only max_bytes")
            limit = rule["max_bytes"]
            if type(limit) is not int or not 1 <= limit <= 16 * 1024 * 1024:
                raise ExecutionPolicyError("input max_bytes must be 1..16777216")
        result[backend_id] = json.loads(canonical_bytes(profile))
    return result


def project_roots(cfg: dict) -> dict[str, str]:
    return {project: os.path.realpath(os.path.expanduser(cfg["projects"][project]["root"]))
            for backend in validate_backends(cfg).values() for project in backend["projects"]}


def normalize_execution(task: dict, cfg: dict, project: str, batch_env: dict) -> dict | None:
    if INTERNAL_FIELD in task:
        raise ExecutionPolicyError("task cannot supply internal execution binding")
    if "execution" not in task:
        return None
    declaration = task["execution"]
    if not isinstance(declaration, dict) or set(declaration) != {"backend", "inputs"}:
        raise ExecutionPolicyError("execution requires exactly backend and inputs")
    backend_id = declaration["backend"]
    if not isinstance(backend_id, str) or backend_id not in validate_backends(cfg):
        raise ExecutionPolicyError("execution backend is not configured")
    profile = validate_backends(cfg)[backend_id]
    if project not in profile["projects"]:
        raise ExecutionPolicyError("execution backend is not enabled for this project")
    if task.get("cmd") != profile["argv"] or task.get("stages") is not None:
        raise ExecutionPolicyError("execution command must exactly match administrator argv")
    if task.get("env") or batch_env or task.get("runtime") or task.get("runtime_prefix"):
        raise ExecutionPolicyError("configured execution cannot inherit task, batch or runtime environment")
    if type(task.get("max_retry")) is not int or task["max_retry"] != 0:
        raise ExecutionPolicyError("configured execution requires explicit max_retry=0")
    root = os.path.realpath(os.path.expanduser(cfg["projects"][project]["root"]))
    if task["cwd_abs"] != root:
        raise ExecutionPolicyError("configured execution cwd must be the registered project root")
    inputs = declaration["inputs"]
    if not isinstance(inputs, dict) or set(inputs) != set(profile["input_slots"]):
        raise ExecutionPolicyError("execution inputs must exactly match administrator input slots")
    normalized = {}
    for slot, entry in inputs.items():
        if not isinstance(entry, dict) or set(entry) != {"path", "sha256"}:
            raise ExecutionPolicyError("execution input requires path and sha256")
        path = _text(entry["path"], "input path")
        parsed = PurePosixPath(path)
        if parsed.is_absolute() or "\\" in path or ":" in path or ".." in parsed.parts or str(parsed) != path or path == ".":
            raise ExecutionPolicyError("input path must be a normalized project-relative POSIX path")
        normalized[slot] = {"path": path, "sha256": _sha(entry["sha256"], "input sha256")}
    return {"interface_version": INTERFACE_VERSION, "backend_id": backend_id,
            "backend_config_sha256": digest(profile), "project_root": root,
            "inputs": normalized}


def revalidate_binding(spec: dict, cfg: dict, project: str, batch_env: dict) -> dict:
    binding = spec.get(INTERNAL_FIELD)
    if not isinstance(binding, dict):
        raise ExecutionPolicyError("missing persisted execution binding")
    public = dict(spec)
    public.pop(INTERNAL_FIELD, None)
    actual = normalize_execution(public, cfg, project, batch_env)
    if actual != binding:
        raise ExecutionPolicyError("execution binding differs from the administrator policy")
    return validate_backends(cfg)[binding["backend_id"]]


def sealed_bytes(data: bytes, name: str) -> int:
    import fcntl
    if not hasattr(os, "memfd_create"):
        raise ExecutionPolicyError("sealed descriptor inputs require Linux memfd")
    fd = os.memfd_create(name, os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    try:
        view = memoryview(data)
        while view:
            view = view[os.write(fd, view):]
        os.lseek(fd, 0, os.SEEK_SET)
        fcntl.fcntl(fd, fcntl.F_ADD_SEALS,
                    fcntl.F_SEAL_WRITE | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_SEAL)
        readonly = os.open(f"/proc/self/fd/{fd}", os.O_RDONLY | os.O_CLOEXEC)
        os.close(fd)
        return readonly
    except BaseException:
        os.close(fd)
        raise


def snapshot_file(path: str, expected_sha256: str, maximum: int, *, root_fd: int | None = None) -> int:
    """Read regular bytes through no-symlink traversal, verify, then seal a copy."""
    opened_dirs = []
    fd = None
    try:
        if root_fd is None:
            fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
        else:
            parts = PurePosixPath(path).parts
            parent = root_fd
            for component in parts[:-1]:
                parent = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
                opened_dirs.append(parent)
            fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=parent)
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size > maximum:
            raise ExecutionPolicyError("execution input is not a bounded regular file")
        chunks, remaining = [], maximum + 1
        while remaining:
            chunk = os.read(fd, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        after = os.fstat(fd)
        if len(data) > maximum or (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ExecutionPolicyError("execution input changed while reading")
        if hashlib.sha256(data).hexdigest() != expected_sha256:
            raise ExecutionPolicyError("execution file SHA-256 mismatch")
        return sealed_bytes(data, "sched-execution-input")
    finally:
        if fd is not None:
            os.close(fd)
        for directory in reversed(opened_dirs):
            os.close(directory)
