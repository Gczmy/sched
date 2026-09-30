"""Historical execution metadata and replay guards; no launch implementation.

These keys remain readable so upgrading never turns a consumed historical task
into an ordinary runnable task. New execution uses the public generic backend.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Mapping

NATIVE_EXEC_PROFILE_V2_SCHEMA = "sched_native_exec_profile_v2"
NATIVE_EXEC_V2_CONTRACT_FIELD = "_native_exec_contract_v2"
NATIVE_EXEC_ALL_INTERNAL_FIELDS = frozenset({
    "_native_exec_profile_id", "_native_exec_profile_sha256",
    "_native_exec_project_root_identity_sha256", "_native_exec_submitted_argv",
    NATIVE_EXEC_V2_CONTRACT_FIELD,
})


class NativeExecProfileError(ValueError):
    pass


def validate_native_exec_profiles(raw: Any, *, where: str = "native_exec_profiles",
                                  projects: Mapping | None = None) -> dict:
    """Recognize only identity fields required to preserve historical guards."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise NativeExecProfileError(f"{where}: must be an object map")
    result, names = {}, set()
    for key, value in raw.items():
        if not isinstance(key, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", key) is None:
            raise NativeExecProfileError(f"{where}: invalid historical profile id")
        if not isinstance(value, dict):
            raise NativeExecProfileError(f"{where}.{key}: must be an object")
        project, name = value.get("project"), value.get("batch_name")
        if not isinstance(project, str) or projects is None or project not in projects:
            raise NativeExecProfileError(f"{where}.{key}: unregistered project")
        if not isinstance(name, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", name) is None or name in names:
            raise NativeExecProfileError(f"{where}.{key}: invalid or duplicate reserved name")
        names.add(name)
        try:
            result[key] = json.loads(json.dumps(value, allow_nan=False))
        except (ValueError, TypeError, RecursionError) as error:
            raise NativeExecProfileError(f"{where}.{key}: invalid historical JSON") from error
    return result


def native_exec_project_roots(cfg: Mapping) -> dict[str, str]:
    profiles = validate_native_exec_profiles(cfg.get("native_exec_profiles"), projects=cfg.get("projects"))
    return {entry["project"]: os.path.realpath(os.path.expanduser(cfg["projects"][entry["project"]]["root"]))
            for entry in profiles.values()}


def native_exec_reserved_batch_names(cfg: Mapping) -> frozenset[str]:
    return frozenset(entry["batch_name"] for entry in validate_native_exec_profiles(
        cfg.get("native_exec_profiles"), projects=cfg.get("projects")).values())


def retired_error() -> NativeExecProfileError:
    return NativeExecProfileError("legacy strict execution is retired; history remains readable and cannot be replayed")
