"""Cold-configured exact native execution profiles.

Native execution is deliberately opt-in.  A profile is an administrator-owned
config entry that binds one strict batch/task command.  Submission data can
select a profile only by matching every public field; it cannot name a profile
or provide any of the persisted internal attestation fields itself.
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
NATIVE_EXEC_INTERNAL_FIELDS = frozenset(
    {
        "_native_exec_profile_id",
        "_native_exec_profile_sha256",
        "_native_exec_project_root_identity_sha256",
        "_native_exec_submitted_argv",
    }
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
    if actual_keys != NATIVE_EXEC_PROFILE_KEYS:
        missing = sorted(NATIVE_EXEC_PROFILE_KEYS - actual_keys, key=repr)
        extra = sorted(actual_keys - NATIVE_EXEC_PROFILE_KEYS, key=repr)
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
        if (
            profile["mode"] == mode
            and profile["project"] == project
            and profile["batch_name"] == batch_name
            and profile["task_id"] == task_id
            and profile["submitted_argv"] == argv
        ):
            return {
                "mode": profile["mode"],
                "project": profile["project"],
                "batch_name": profile["batch_name"],
                "task_id": profile["task_id"],
                "submitted_argv": list(profile["submitted_argv"]),
                "profile_id": profile["profile_id"],
                "profile_sha256": profile["profile_sha256"],
            }
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
