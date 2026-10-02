"""Opt-in application checkpoint and smoke protocol; never supplies wait authority."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import PurePosixPath
import re
import stat

from .execution_policy import canonical_bytes, digest

PROTOCOL = "sched-recovery/v1"
ENVIRONMENT = "SCHED_RECOVERY_CONTEXT"
INTERNAL_FIELDS = ("_recovery_binding", "_recovery_fingerprint")
MAX_CHECKPOINT_BYTES = 1024 * 1024
MAX_INPUT_BYTES = 256 * 1024 * 1024
_SHA = re.compile(r"[0-9a-f]{64}\Z")


class RecoveryError(ValueError):
    pass


def _sha(value):
    if not isinstance(value, str) or not _SHA.fullmatch(value):
        raise RecoveryError("recovery requires a lowercase SHA-256")
    return value


def _relative(value):
    if not isinstance(value, str) or not value or len(value) > 4096 or "\0" in value:
        raise RecoveryError("recovery input path is invalid")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or "\\" in value or ":" in value or str(path) != value or value == ".":
        raise RecoveryError("recovery input must be a normalized project-relative path")
    return value


def normalize(task):
    if any(key in task for key in (*INTERNAL_FIELDS, "_recovery_context")):
        raise RecoveryError("task cannot supply internal recovery fields")
    if "recovery" not in task:
        return None
    raw = task["recovery"]
    required = {"protocol", "mode", "code", "config", "inputs"}
    if type(raw) is not dict or not required <= raw.keys() or raw.keys() - required - {"smoke_job_id"}:
        raise RecoveryError("recovery requires protocol, mode, code, config and inputs")
    if raw["protocol"] != PROTOCOL or raw["mode"] not in ("smoke", "run"):
        raise RecoveryError("unsupported recovery protocol or mode")
    if type(task.get("max_retry")) is not int or task["max_retry"] != 0:
        raise RecoveryError("recovery requires explicit max_retry=0; new attempts use a separate policy")
    if task.get("stages") is not None:
        raise RecoveryError("recovery groups must be independent single-command tasks")
    result = {"protocol": PROTOCOL, "mode": raw["mode"]}
    count = 0
    seen = set()
    for kind in ("code", "config", "inputs"):
        entries = raw[kind]
        if type(entries) is not dict or (kind == "code" and not entries):
            raise RecoveryError("recovery code must be nonempty; input sets must be objects")
        result[kind] = {}
        for path, sha in entries.items():
            path = _relative(path)
            if path in seen:
                raise RecoveryError("recovery input is declared more than once")
            seen.add(path)
            result[kind][path] = _sha(sha)
        count += len(entries)
    if count > 64:
        raise RecoveryError("recovery permits at most 64 declared files")
    smoke = raw.get("smoke_job_id")
    if raw["mode"] == "run":
        if not isinstance(smoke, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,511}", smoke):
            raise RecoveryError("run recovery requires an exact smoke_job_id")
        result["smoke_job_id"] = smoke
    elif "smoke_job_id" in raw:
        raise RecoveryError("a smoke task cannot depend on another smoke receipt")
    return result


def verify_inputs(spec):
    declaration = spec.get("recovery")
    if declaration is None:
        return
    try:
        root = os.open(spec["cwd_abs"], os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as error:
        raise RecoveryError("recovery project root cannot be verified") from error
    try:
        for kind in ("code", "config", "inputs"):
            for path, expected in declaration[kind].items():
                opened = []
                parent = root
                try:
                    parts = PurePosixPath(_relative(path)).parts
                    for part in parts[:-1]:
                        parent = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
                        opened.append(parent)
                    fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=parent)
                    opened.append(fd)
                    before = os.fstat(fd)
                    if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_INPUT_BYTES:
                        raise RecoveryError("recovery input is not a bounded regular file")
                    sha = hashlib.sha256()
                    total = 0
                    while True:
                        chunk = os.read(fd, min(65536, MAX_INPUT_BYTES + 1 - total))
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > MAX_INPUT_BYTES:
                            raise RecoveryError("recovery input exceeds the size limit")
                        sha.update(chunk)
                    after = os.fstat(fd)
                    if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                        raise RecoveryError("recovery input changed while reading")
                    if sha.hexdigest() != _sha(expected):
                        raise RecoveryError("recovery input SHA-256 mismatch")
                finally:
                    for fd in reversed(opened):
                        os.close(fd)
    except OSError as error:
        raise RecoveryError("recovery input cannot be verified") from error
    finally:
        os.close(root)


def freeze(spec, fingerprint):
    if spec.get("recovery") is None:
        return
    verify_inputs(spec)
    _sha(fingerprint)
    payload = {"protocol": PROTOCOL, "task_id": spec["id"], "fingerprint": fingerprint,
               **{kind: spec["recovery"][kind] for kind in ("code", "config", "inputs")}}
    spec["_recovery_binding"] = digest(payload)
    spec["_recovery_fingerprint"] = fingerprint


def verify_binding(spec, fingerprint):
    if spec.get("recovery") is None:
        return
    candidate = dict(spec)
    freeze(candidate, fingerprint)
    if any(candidate[key] != spec.get(key) for key in INTERNAL_FIELDS):
        raise RecoveryError("recovery code, configuration or input binding changed; run smoke again")


def copy_fields(source, destination):
    if source.get("recovery") is not None:
        for key in ("recovery", *INTERNAL_FIELDS):
            if key not in source:
                raise RecoveryError("missing persisted recovery binding")
            destination[key] = source[key]


def context(host_dir, job, spec, *, create=True):
    if spec.get("recovery") is None:
        return None
    from . import state
    checkpoint_dir = os.path.join(host_dir, "recovery", job["batch_id"], job["task_id"])
    report_dir = os.path.join(checkpoint_dir, "reports")
    if create:
        state.ensure_private_directory(report_dir)
    value = {"protocol": PROTOCOL, "binding_sha256": _sha(spec["_recovery_binding"]),
             "mode": spec["recovery"]["mode"], "job_id": job["id"],
             "checkpoint_dir": checkpoint_dir, "report_dir": report_dir}
    return canonical_bytes(value).decode("ascii")


def _read(directory, name, limit=MAX_CHECKPOINT_BYTES):
    directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    fd = None
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=directory_fd)
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
            raise RecoveryError("recovery record is not a bounded regular file")
        data = bytearray()
        while len(data) <= limit:
            part = os.read(fd, min(65536, limit + 1 - len(data)))
            if not part:
                break
            data.extend(part)
        after = os.fstat(fd)
        if len(data) > limit or (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise RecoveryError("recovery record changed while reading")
        try:
            return json.loads(data)
        except (ValueError, UnicodeError, RecursionError) as error:
            raise RecoveryError("invalid recovery JSON") from error
    finally:
        if fd is not None:
            os.close(fd)
        os.close(directory_fd)


def _atomic(directory, name, value):
    data = canonical_bytes(value)
    if len(data) > MAX_CHECKPOINT_BYTES:
        raise RecoveryError("recovery record exceeds the size limit")
    directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    temporary = ".pending-" + os.urandom(16).hex()
    fd = None
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory_fd)
        remaining = memoryview(data)
        while remaining:
            remaining = remaining[os.write(fd, remaining):]
        os.fsync(fd)
        os.close(fd)
        fd = None
        os.replace(temporary, name, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        os.fsync(directory_fd)
    finally:
        if fd is not None:
            os.close(fd)
        try:
            os.unlink(temporary, dir_fd=directory_fd)
        except FileNotFoundError:
            pass
        os.close(directory_fd)


class CheckpointStore:
    """Application helper for bounded JSON progress; application owns large artifacts."""
    def __init__(self, value):
        required = {"protocol", "binding_sha256", "mode", "job_id", "checkpoint_dir", "report_dir"}
        if type(value) is not dict or set(value) != required or value["protocol"] != PROTOCOL:
            raise RecoveryError("invalid recovery context")
        _sha(value["binding_sha256"])
        if value["mode"] not in ("smoke", "run") or not isinstance(value["job_id"], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,511}", value["job_id"]):
            raise RecoveryError("invalid recovery job identity")
        for key in ("checkpoint_dir", "report_dir"):
            if not isinstance(value[key], str) or not os.path.isabs(value[key]) or "\0" in value[key]:
                raise RecoveryError("invalid recovery directory")
        self.value = dict(value)

    @classmethod
    def from_environment(cls):
        try:
            return cls(json.loads(os.environ[ENVIRONMENT]))
        except (KeyError, ValueError) as error:
            raise RecoveryError("scheduler recovery context is unavailable") from error

    def load(self):
        try:
            value = _read(self.value["checkpoint_dir"], "checkpoint.json")
        except FileNotFoundError:
            return None
        required = {"protocol", "binding_sha256", "producer_job_id", "payload", "payload_sha256"}
        if type(value) is not dict or set(value) != required or value["protocol"] != PROTOCOL or value["binding_sha256"] != self.value["binding_sha256"]:
            raise RecoveryError("checkpoint binding mismatch")
        if not isinstance(value["producer_job_id"], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,511}", value["producer_job_id"]):
            raise RecoveryError("checkpoint producer identity is invalid")
        if digest(value["payload"]) != value["payload_sha256"]:
            raise RecoveryError("checkpoint payload checksum mismatch")
        return value["payload"]

    def save(self, payload):
        if payload is None:
            raise RecoveryError("checkpoint payload must be non-null JSON progress")
        value = {"protocol": PROTOCOL, "binding_sha256": self.value["binding_sha256"],
                 "producer_job_id": self.value["job_id"], "payload": payload, "payload_sha256": digest(payload)}
        _atomic(self.value["checkpoint_dir"], "checkpoint.json", value)

    def report(self, outcome):
        if outcome not in ("oom", "smoke_ok") or (outcome == "smoke_ok" and self.value["mode"] != "smoke"):
            raise RecoveryError("invalid recovery outcome")
        payload = self.load()
        if payload is None:
            raise RecoveryError("a durable checkpoint is required before reporting recovery")
        value = {"protocol": PROTOCOL, "binding_sha256": self.value["binding_sha256"],
                 "job_id": self.value["job_id"], "outcome": outcome, "checkpoint_sha256": digest(payload)}
        _atomic(self.value["report_dir"], self.value["job_id"] + ".json", value)


def report(host_dir, job, spec):
    value = json.loads(context(host_dir, job, spec, create=False))
    try:
        record = _read(value["report_dir"], job["id"] + ".json", 4096)
    except FileNotFoundError:
        return None
    required = {"protocol", "binding_sha256", "job_id", "outcome", "checkpoint_sha256"}
    if type(record) is not dict or set(record) != required or record["protocol"] != PROTOCOL or record["binding_sha256"] != spec["_recovery_binding"] or record["job_id"] != job["id"] or record["outcome"] not in ("oom", "smoke_ok"):
        raise RecoveryError("recovery report binding mismatch")
    _sha(record["checkpoint_sha256"])
    return record


def smoke_gate(conn, host_dir, job, spec):
    declaration = spec.get("recovery")
    if declaration is None or declaration["mode"] == "smoke":
        return True
    smoke = conn.execute("SELECT * FROM jobs WHERE id=?", (declaration["smoke_job_id"],)).fetchone()
    if smoke is None or smoke["status"] != "done" or smoke["rc"] != 0:
        return False
    task = conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?", (smoke["batch_id"], smoke["task_id"], smoke["version"])).fetchone()
    if task is None:
        return False
    try:
        candidate = json.loads(task["spec"])
    except (ValueError, TypeError, RecursionError):
        return False
    if type(candidate) is not dict:
        return False
    if candidate.get("recovery", {}).get("mode") != "smoke" or candidate.get("_recovery_binding") != spec.get("_recovery_binding"):
        return False
    try:
        receipt = report(host_dir, smoke, candidate)
    except (ValueError, OSError):
        return False
    return receipt is not None and receipt["outcome"] == "smoke_ok"
