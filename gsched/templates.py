"""Command template expansion shared by direct and inbox submission paths."""

from __future__ import annotations

import os

from .config import resolve_template
from .schema import SchemaError


def _expand_venv(token: str, cfg: dict) -> str:
    if isinstance(token, str) and token.startswith("{VENV:") and token.endswith("}"):
        name = token[len("{VENV:"):-1]
        path = cfg.get("venvs", {}).get(name)
        if not path:
            raise SchemaError(f"venv 未定义: {name}")
        return str(path)
    return token


def _expand_root(token: str, cfg: dict) -> str:
    if isinstance(token, str) and "{ROOT}" in token:
        return resolve_template(token, cfg)
    return token


def expand_cmd(
    cmd_list: list[str],
    cfg: dict,
    stage_artifacts: dict[int, dict] | None = None,
    cwd_abs: str | None = None,
) -> list[str]:
    """Expand VENV, ROOT, and prior-stage artifact references in one command."""
    out = []
    for token in cmd_list:
        token = _expand_venv(token, cfg)
        token = _expand_root(token, cfg)
        if isinstance(token, str) and token.startswith("{stage") and token.endswith("}"):
            inner = token[1:-1]
            parts = inner.split("_", 1)
            if len(parts) == 2 and parts[0].startswith("stage") and parts[0][5:].isdigit():
                stage_idx = int(parts[0][5:])
                key = parts[1]
                artifacts = (stage_artifacts or {}).get(stage_idx)
                if artifacts is None:
                    raise SchemaError(f"{token}: 引用不存在的 stage {stage_idx} (N7)")
                artifact = artifacts.get(key)
                if not artifact or not artifact.get("path"):
                    raise SchemaError(
                        f"{token}: stage{stage_idx} 未声明产物 key '{key}' (N7)"
                    )
                path = artifact["path"]
                if not os.path.isabs(path) and cwd_abs:
                    path = os.path.normpath(os.path.join(cwd_abs, path))
                token = path
        out.append(token)
    return out
