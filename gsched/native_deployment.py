"""Explicit, immutable cold bindings for experimental native protocols.

Only a trusted bootstrap may load this file and supply its independently pinned
digest. Never take either the file or its digest from a job, request or anchor.
Nothing is loaded from the environment, daemon configuration or current directory.
These bindings do not grant native execution or external verifier authority.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re


SCHEMA = "sched_native_deployment/v1"
PHASES = ("preparation", "raw_collection", "aggregation")
MAX_BYTES = 65536
_AUTHORITY = object()
_KEYS = frozenset(("schema", "project", "batch_name_template", "task_id_template",
                   "logical_argv_profiles"))


class NativeDeploymentError(ValueError):
    """Invalid or unpinned administrator bindings; contains no input values."""


def _require(condition: bool, reason: str = "native_deployment_invalid") -> None:
    if not condition:
        raise NativeDeploymentError(reason)


def _text(value: object, *, maximum: int = 4096, empty: bool = False) -> str:
    _require(type(value) is str and "\x00" not in value)
    try:
        size = len(value.encode("utf-8"))
    except UnicodeError as exc:
        raise NativeDeploymentError("native_deployment_invalid") from exc
    _require((0 if empty else 1) <= size <= maximum)
    return value


def _path(value: object) -> str:
    value = _text(value, maximum=4095)
    _require(value.startswith("/") and "\\" not in value)
    _require(all(part not in ("", ".", "..") and len(part.encode("utf-8")) <= 255
                 for part in value[1:].split("/")))
    return value


def _template(value: object) -> str:
    value = _text(value)
    _require(value.count("{phase}") == 1)
    _require(re.fullmatch(r"[A-Za-z0-9_.-]+", value.replace("{phase}", "phase")) is not None)
    return value


def _pairs(items):
    value = {}
    for key, item in items:
        _require(key not in value)
        value[key] = item
    return value


@dataclass(frozen=True, slots=True, init=False, repr=False)
class NativeDeployment:
    project: str
    batch_name_template: str
    task_id_template: str
    _argv: tuple[tuple[str, ...], ...]
    file_sha256: str

    def __init__(self, authority, *, project, batch_name_template,
                 task_id_template, argv, file_sha256):
        _require(authority is _AUTHORITY)
        for key, value in (("project", project), ("batch_name_template", batch_name_template),
                           ("task_id_template", task_id_template), ("_argv", argv),
                           ("file_sha256", file_sha256)):
            object.__setattr__(self, key, value)

    def argv(self, phase: str) -> tuple[str, ...]:
        _require(type(phase) is str and phase in PHASES)
        return self._argv[PHASES.index(phase)]

    def batch_name(self, phase: str) -> str:
        self.argv(phase)
        return self.batch_name_template.replace("{phase}", phase)

    def task_id(self, phase: str) -> str:
        self.argv(phase)
        return self.task_id_template.replace("{phase}", phase)


def require_deployment(value: NativeDeployment) -> NativeDeployment:
    _require(type(value) is NativeDeployment, "native_deployment_required")
    return value


def parse_deployment(payload: bytes, *, expected_sha256: str) -> NativeDeployment:
    """Check an independently supplied file digest before parsing its values."""
    _require(type(expected_sha256) is str
             and re.fullmatch(r"[0-9a-f]{64}", expected_sha256) is not None,
             "native_deployment_digest_required")
    _require(type(payload) is bytes and 0 < len(payload) <= MAX_BYTES)
    _require(hashlib.sha256(payload).hexdigest() == expected_sha256,
             "native_deployment_digest_mismatch")
    try:
        value = json.loads(payload.decode("utf-8"), object_pairs_hook=_pairs)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise NativeDeploymentError("native_deployment_invalid") from exc
    _require(type(value) is dict and value.keys() == _KEYS and value["schema"] == SCHEMA)
    project = _text(value["project"])
    _require(re.fullmatch(r"[A-Za-z0-9_.-]+", project) is not None)
    batch = _template(value["batch_name_template"])
    task = _template(value["task_id_template"])
    profiles = value["logical_argv_profiles"]
    _require(type(profiles) is dict and set(profiles) == set(PHASES))
    argv = []
    for phase in PHASES:
        items = profiles[phase]
        _require(type(items) is list and 4 <= len(items) <= 256)
        items = tuple(_text(item, empty=True) for item in items)
        _path(items[0])
        # Keep the isolated Python stop-only profile boundary; no shell/defaults.
        _require(items[1:3] == ("-I", "-S"))
        _text(items[3])
        argv.append(items)
    return NativeDeployment(_AUTHORITY, project=project, batch_name_template=batch,
                            task_id_template=task, argv=tuple(argv), file_sha256=expected_sha256)


def load_deployment(path: str | Path, *, expected_sha256: str) -> NativeDeployment:
    """Load one bounded snapshot, never rereading bindings during an exchange."""
    with Path(path).open("rb") as stream:
        payload = stream.read(MAX_BYTES + 1)
    return parse_deployment(payload, expected_sha256=expected_sha256)
