"""Cold-configured exact native execution profiles.

Native execution is deliberately opt-in.  A profile is an administrator-owned
config entry that binds one strict batch/task command.  Submission data can
select a profile only by matching every public field; it cannot name a profile
or provide any of the persisted internal attestation fields itself.

V2 is intentionally a validation-only frozen-batch compatibility schema.  Its
contract marker exists only in the ephemeral normalized result: both local and
inbox submission paths reject it before fingerprinting or durable batch writes.
It therefore makes no persisted-contract or launch-time authority claim until
the retained/bootstrap launcher is implemented separately.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
from typing import Any, Mapping


NATIVE_EXEC_PROFILE_KEYS = frozenset(
    {"mode", "project", "batch_name", "task_id", "submitted_argv"}
)
NATIVE_EXEC_PROFILE_V2_SCHEMA = "sched_native_exec_profile_v2"
NATIVE_EXEC_PROFILE_V2_KEYS = frozenset(
    {
        "schema",
        "mode",
        "project",
        "batch_name",
        "task_id",
        "submitted_argv",
        "cwd",
        "depends_on",
        "_protocol",
        "batch_env",
        "task_env",
        "runtime",
        "duration_min",
        "max_retry",
        "resources",
        "artifacts",
    }
)
NATIVE_EXEC_V2_ROOT_KEYS = frozenset(
    {"name", "project", "mode", "cwd", "depends_on", "_protocol", "env", "tasks"}
)
NATIVE_EXEC_V2_TASK_KEYS = frozenset(
    {
        "id",
        "cmd",
        "env",
        "runtime",
        "duration_min",
        "max_retry",
        "artifacts",
        "resources",
    }
)
NATIVE_EXEC_V2_CONTRACT_FIELD = "_native_exec_contract_v2"
NATIVE_EXEC_INTERNAL_FIELDS = frozenset(
    {
        "_native_exec_profile_id",
        "_native_exec_profile_sha256",
        "_native_exec_project_root_identity_sha256",
        "_native_exec_submitted_argv",
    }
)
NATIVE_EXEC_ALL_INTERNAL_FIELDS = (
    NATIVE_EXEC_INTERNAL_FIELDS | {NATIVE_EXEC_V2_CONTRACT_FIELD}
)
_SAFE_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_LOWER_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


class NativeExecProfileError(ValueError):
    """A native-exec profile or persisted binding is invalid."""


def _identifier(value: Any, where: str) -> str:
    if not isinstance(value, str) or _SAFE_IDENTIFIER_RE.fullmatch(value) is None:
        raise NativeExecProfileError(
            f"{where}: must be a 1..128 character safe ASCII identifier"
        )
    return value


def _submitted_argv(value: Any, where: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise NativeExecProfileError(f"{where}: must be a non-empty argv array")
    detached: list[str] = []
    for index, token in enumerate(value):
        if not isinstance(token, str) or not token or "\0" in token:
            raise NativeExecProfileError(
                f"{where}[{index}]: must be a non-empty string without NUL"
            )
        detached.append(token)
    if not os.path.isabs(detached[0]) or os.path.normpath(detached[0]) != detached[0]:
        raise NativeExecProfileError(
            f"{where}[0]: must be a normalized absolute executable path"
        )
    return detached


def _string_map(value: Any, where: str, *, require_nonempty: bool) -> dict[str, str]:
    if not isinstance(value, dict) or (require_nonempty and not value):
        qualifier = "non-empty " if require_nonempty else ""
        raise NativeExecProfileError(f"{where}: must be a {qualifier}string map")
    detached: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not key or "\0" in key:
            raise NativeExecProfileError(f"{where}: keys must be non-empty strings")
        if not isinstance(item, str) or "\0" in item:
            raise NativeExecProfileError(f"{where}.{key}: must be a string without NUL")
        detached[key] = item
    return detached


def _canonical_v2_contract(value: Any, where: str, project: str) -> dict[str, Any]:
    """Validate the frozen public batch/task values bound by a V2 profile."""
    if not isinstance(value, Mapping):
        raise NativeExecProfileError(f"{where}: must be an object")
    expected = NATIVE_EXEC_PROFILE_V2_KEYS - {
        "schema", "mode", "project", "batch_name", "task_id", "submitted_argv"
    }
    actual = frozenset(value)
    if actual != expected:
        raise NativeExecProfileError(
            f"{where}: contract keys must be exact; "
            f"missing={sorted(expected - actual, key=repr)}, "
            f"extra={sorted(actual - expected, key=repr)}"
        )
    cwd = value["cwd"]
    expected_cwd = f"{{PROJECT:{project}}}"
    if cwd != expected_cwd:
        raise NativeExecProfileError(f"{where}.cwd: must be exactly {expected_cwd!r}")
    if value["depends_on"] != []:
        raise NativeExecProfileError(f"{where}.depends_on: must be exactly []")
    protocol = value["_protocol"]
    if not isinstance(protocol, str) or not protocol or "\0" in protocol:
        raise NativeExecProfileError(f"{where}._protocol: must be a non-empty string")
    batch_env = _string_map(value["batch_env"], f"{where}.batch_env", require_nonempty=True)
    task_env = _string_map(value["task_env"], f"{where}.task_env", require_nonempty=False)
    if task_env:
        raise NativeExecProfileError(f"{where}.task_env: must be exactly empty")
    runtime = value["runtime"]
    if not isinstance(runtime, dict) or frozenset(runtime) != {"prefix"}:
        raise NativeExecProfileError(f"{where}.runtime: must be exact prefix-only object")
    prefix = runtime["prefix"]
    if (
        not isinstance(prefix, str)
        or not os.path.isabs(prefix)
        or os.path.normpath(prefix) != prefix
        or not os.path.isdir(prefix)
    ):
        raise NativeExecProfileError(
            f"{where}.runtime.prefix: must be a normalized existing absolute directory"
        )
    duration = value["duration_min"]
    if type(duration) is not int or duration <= 0:
        raise NativeExecProfileError(
            f"{where}.duration_min: must be a positive integer"
        )
    if type(value["max_retry"]) is not int or value["max_retry"] != 0:
        raise NativeExecProfileError(f"{where}.max_retry: must be exactly zero")
    resources = value["resources"]
    if (
        not isinstance(resources, dict)
        or frozenset(resources) != {"gpu", "cpus"}
        or type(resources["gpu"]) is not int
        or resources["gpu"] != 0
        or type(resources["cpus"]) is not int
        or resources["cpus"] != 1
    ):
        raise NativeExecProfileError(
            f"{where}.resources: must be exactly gpu=0, cpus=1"
        )
    if value["artifacts"] != {}:
        raise NativeExecProfileError(f"{where}.artifacts: must be exactly empty")
    return {
        "cwd": cwd,
        "depends_on": [],
        "_protocol": protocol,
        "batch_env": batch_env,
        "task_env": {},
        "runtime": {"prefix": prefix},
        "duration_min": duration,
        "max_retry": 0,
        "resources": {"gpu": 0, "cpus": 1},
        "artifacts": {},
    }


def _configured_project_root(
    project: str,
    projects: Mapping[str, Any],
    where: str,
) -> str:
    entry = projects.get(project)
    root = entry.get("root") if isinstance(entry, Mapping) else entry
    if not isinstance(root, str) or not root:
        raise NativeExecProfileError(f"{where}: configured project root is missing")
    expanded = os.path.expanduser(root)
    if not os.path.isabs(expanded) or os.path.normpath(expanded) != expanded:
        raise NativeExecProfileError(
            f"{where}: native-exec project root must be a normalized absolute path"
        )
    canonical = os.path.realpath(expanded)
    if os.path.islink(expanded):
        raise NativeExecProfileError(
            f"{where}: native-exec project root itself must not be a symlink"
        )
    try:
        root_stat = os.stat(canonical)
    except OSError as exc:
        raise NativeExecProfileError(
            f"{where}: native-exec project root cannot be statted"
        ) from exc
    if not stat.S_ISDIR(root_stat.st_mode):
        raise NativeExecProfileError(
            f"{where}: native-exec project root must be an existing directory"
        )
    return canonical


def _canonical_profile(
    profile_id: Any,
    value: Any,
    *,
    where: str,
    projects: Mapping[str, Any] | None,
) -> dict[str, Any]:
    canonical_id = _identifier(profile_id, f"{where}.profile_id")
    if not isinstance(value, dict):
        raise NativeExecProfileError(f"{where}: profile must be an object")
    actual_keys = frozenset(value)
    is_v2 = value.get("schema") == NATIVE_EXEC_PROFILE_V2_SCHEMA
    expected_keys = NATIVE_EXEC_PROFILE_V2_KEYS if is_v2 else NATIVE_EXEC_PROFILE_KEYS
    if actual_keys != expected_keys:
        missing = sorted(expected_keys - actual_keys, key=repr)
        extra = sorted(actual_keys - expected_keys, key=repr)
        raise NativeExecProfileError(
            f"{where}: profile keys must be exact; missing={missing}, extra={extra}"
        )
    if value["mode"] != "strict":
        raise NativeExecProfileError(f"{where}.mode: must be exactly 'strict'")
    project = _identifier(value["project"], f"{where}.project")
    if projects is not None and project not in projects:
        raise NativeExecProfileError(
            f"{where}.project: unknown configured project {project!r}"
        )
    if projects is not None:
        _configured_project_root(
            project,
            projects,
            f"{where}.project",
        )
    profile = {
        "mode": "strict",
        "project": project,
        "batch_name": _identifier(
            value["batch_name"], f"{where}.batch_name"
        ),
        "task_id": _identifier(value["task_id"], f"{where}.task_id"),
        "submitted_argv": _submitted_argv(
            value["submitted_argv"], f"{where}.submitted_argv"
        ),
    }
    if is_v2:
        profile["schema"] = NATIVE_EXEC_PROFILE_V2_SCHEMA
        profile["contract"] = _canonical_v2_contract(
            {key: value[key] for key in NATIVE_EXEC_PROFILE_V2_KEYS - {
                "schema", "mode", "project", "batch_name", "task_id", "submitted_argv"
            }},
            f"{where}.contract",
            project,
        )
    profile["profile_id"] = canonical_id
    profile["profile_sha256"] = native_exec_profile_sha256(
        canonical_id, profile
    )
    return profile


def native_exec_profile_sha256(
    profile_id: str, profile: Mapping[str, Any]
) -> str:
    """Return the domain-separated canonical digest for one exact profile."""
    canonical_id = _identifier(profile_id, "profile_id")
    if not isinstance(profile, Mapping):
        raise NativeExecProfileError("profile: must be an object")
    if profile.get("mode") != "strict":
        raise NativeExecProfileError("profile.mode: must be exactly 'strict'")
    project = _identifier(profile.get("project"), "profile.project")
    batch_name = _identifier(profile.get("batch_name"), "profile.batch_name")
    task_id = _identifier(profile.get("task_id"), "profile.task_id")
    argv = _submitted_argv(profile.get("submitted_argv"), "profile.submitted_argv")
    if profile.get("schema") == NATIVE_EXEC_PROFILE_V2_SCHEMA:
        contract = _canonical_v2_contract(
            profile.get("contract", {
                key: profile.get(key) for key in NATIVE_EXEC_PROFILE_V2_KEYS - {
                    "schema", "mode", "project", "batch_name", "task_id", "submitted_argv"
                }
            }),
            "profile.contract",
            project,
        )
        preimage = {
            "schema": NATIVE_EXEC_PROFILE_V2_SCHEMA,
            "profile_id": canonical_id,
            "mode": "strict",
            "project": project,
            "batch_name": batch_name,
            "task_id": task_id,
            "submitted_argv": argv,
            **contract,
        }
    else:
        preimage = {
            "schema": "sched_native_exec_profile_v1",
            "profile_id": canonical_id,
            "mode": "strict",
            "project": project,
            "batch_name": batch_name,
            "task_id": task_id,
            "submitted_argv": argv,
        }
    encoded = json.dumps(
        preimage,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_native_exec_profiles(
    raw: Any,
    *,
    where: str = "native_exec_profiles",
    projects: Mapping[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    """Validate and detach a cold profile registry.

    Duplicate match tuples and batch names are rejected so an exact submitted
    command resolves to one administrator-owned, one-shot profile identity.
    """
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise NativeExecProfileError(f"{where}: must be an object map")
    resolved: dict[str, dict[str, Any]] = {}
    matches: dict[tuple[Any, ...], str] = {}
    batch_names: dict[str, str] = {}
    for profile_id, value in raw.items():
        profile_where = f"{where}.{profile_id}"
        profile = _canonical_profile(
            profile_id,
            value,
            where=profile_where,
            projects=projects,
        )
        match_key = (
            profile["mode"],
            profile["project"],
            profile["batch_name"],
            profile["task_id"],
            tuple(profile["submitted_argv"]),
        )
        prior = matches.get(match_key)
        if prior is not None:
            raise NativeExecProfileError(
                f"{profile_where}: duplicates exact match owned by profile {prior!r}"
            )
        matches[match_key] = profile["profile_id"]
        prior_batch = batch_names.get(profile["batch_name"])
        if prior_batch is not None:
            raise NativeExecProfileError(
                f"{profile_where}: batch_name is already owned by profile "
                f"{prior_batch!r}"
            )
        batch_names[profile["batch_name"]] = profile["profile_id"]
        resolved[profile["profile_id"]] = profile
    return resolved


def native_exec_project_roots(cfg: Mapping[str, Any]) -> dict[str, str]:
    """Return the canonical roots whose meaning is frozen by native profiles."""
    projects = cfg.get("projects")
    if not isinstance(projects, Mapping):
        raise NativeExecProfileError("projects: must be an object map")
    profiles = validate_native_exec_profiles(
        cfg.get("native_exec_profiles"),
        projects=projects,
    )
    roots: dict[str, str] = {}
    for profile in profiles.values():
        project = profile["project"]
        roots[project] = _configured_project_root(
            project,
            projects,
            f"projects.{project}.root",
        )
    return roots


def native_exec_reserved_batch_names(
    cfg: Mapping[str, Any],
) -> frozenset[str]:
    """Return every batch name reserved by the cold native registry."""
    projects = cfg.get("projects")
    if not isinstance(projects, Mapping):
        raise NativeExecProfileError("projects: must be an object map")
    profiles = validate_native_exec_profiles(
        cfg.get("native_exec_profiles"),
        projects=projects,
    )
    return frozenset(profile["batch_name"] for profile in profiles.values())


def native_exec_profile_schema_for_batch(
    cfg: Mapping[str, Any], batch_name: str
) -> str | None:
    """Return the cold profile schema owning a reserved batch name."""
    profiles = validate_native_exec_profiles(
        cfg.get("native_exec_profiles"), projects=cfg.get("projects")
    )
    for profile in profiles.values():
        if profile["batch_name"] == batch_name:
            return profile.get("schema", "sched_native_exec_profile_v1")
    return None


def native_exec_project_root_identity_sha256(root: str) -> str:
    """Bind one canonical project-root path to its current directory inode."""
    if not isinstance(root, str) or not os.path.isabs(root):
        raise NativeExecProfileError(
            "native project root identity requires an absolute path"
        )
    canonical = os.path.realpath(root)
    try:
        root_stat = os.stat(canonical)
    except OSError as exc:
        raise NativeExecProfileError(
            "native project root identity cannot be statted"
        ) from exc
    if not stat.S_ISDIR(root_stat.st_mode):
        raise NativeExecProfileError(
            "native project root identity requires a directory"
        )
    preimage = {
        "schema": "sched_native_exec_project_root_identity_v1",
        "canonical_path": canonical,
        "st_dev": root_stat.st_dev,
        "st_ino": root_stat.st_ino,
    }
    encoded = json.dumps(
        preimage,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def resolve_native_exec_profile(
    cfg: Mapping[str, Any],
    *,
    mode: Any,
    project: Any,
    batch_name: Any,
    task_id: Any,
    submitted_argv: Any,
    batch_contract: Any = None,
) -> dict[str, Any] | None:
    """Resolve an exact profile match from the current cold config."""
    profiles = validate_native_exec_profiles(
        cfg.get("native_exec_profiles"),
        projects=cfg.get("projects"),
    )
    try:
        argv = _submitted_argv(submitted_argv, "submitted_argv")
    except NativeExecProfileError:
        return None
    for profile in profiles.values():
        is_v2 = profile.get("schema") == NATIVE_EXEC_PROFILE_V2_SCHEMA
        if is_v2:
            try:
                contract = _canonical_v2_contract(
                    batch_contract, "batch_contract", str(project)
                )
            except NativeExecProfileError:
                continue
            if contract != profile["contract"]:
                continue
        elif batch_contract is not None:
            continue
        if (
            profile["mode"] == mode
            and profile["project"] == project
            and profile["batch_name"] == batch_name
            and profile["task_id"] == task_id
            and profile["submitted_argv"] == argv
        ):
            resolved = {
                "mode": profile["mode"],
                "project": profile["project"],
                "batch_name": profile["batch_name"],
                "task_id": profile["task_id"],
                "submitted_argv": list(profile["submitted_argv"]),
                "profile_id": profile["profile_id"],
                "profile_sha256": profile["profile_sha256"],
            }
            if is_v2:
                resolved["schema"] = NATIVE_EXEC_PROFILE_V2_SCHEMA
                resolved["contract"] = json.loads(json.dumps(profile["contract"]))
            return resolved
    return None


def reattest_native_exec_profile(
    cfg: Mapping[str, Any],
    *,
    mode: Any,
    project: Any,
    batch_name: Any,
    task_id: Any,
    profile_id: Any,
    profile_sha256: Any,
    submitted_argv: Any,
    batch_contract: Any = None,
) -> dict[str, Any]:
    """Re-attest persisted native binding against the current cold config."""
    if not isinstance(profile_id, str) or not profile_id:
        raise NativeExecProfileError("persisted native profile id is missing")
    if (
        not isinstance(profile_sha256, str)
        or _LOWER_SHA256_RE.fullmatch(profile_sha256) is None
    ):
        raise NativeExecProfileError("persisted native profile sha256 is invalid")
    argv = _submitted_argv(submitted_argv, "persisted submitted_argv")
    resolved = resolve_native_exec_profile(
        cfg,
        mode=mode,
        project=project,
        batch_name=batch_name,
        task_id=task_id,
        submitted_argv=argv,
        batch_contract=batch_contract,
    )
    if resolved is None:
        raise NativeExecProfileError(
            "persisted native execution tuple no longer matches cold config"
        )
    if not hmac.compare_digest(resolved["profile_id"], profile_id):
        raise NativeExecProfileError("persisted native profile id drifted")
    if not hmac.compare_digest(resolved["profile_sha256"], profile_sha256):
        raise NativeExecProfileError("persisted native profile digest drifted")
    return resolved
