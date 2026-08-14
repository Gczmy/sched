"""产物指纹 (文档 §3.2 A2).

指纹 = cmdline hash + git rev + venv 路径. 代码版本必须入指纹:
代码更新后同一 cmdline 的旧产物视为过期必须重跑 (darf_da 实踩).

git rev 按任务 cwd 向上找最近 .git (J 类); 非 git 目录 (git:false)
不做代码版本指纹, 指纹 = cmdline + venv.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from typing import Any


def _git_rev(cwd: str) -> str | None:
    """在任务 cwd 向上找最近 .git 并返回 rev; 非 git 仓库返回 None."""
    try:
        out = subprocess.run(
            ["git", "-C", cwd, "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if out.returncode == 0:
            return out.stdout.strip()
    except (subprocess.SubprocessError, FileNotFoundError):
        pass
    return None


def compute_fingerprint(
    cmd: list[str] | None,
    stages: list[dict] | None,
    cwd: str,
    git: bool | None,
    venv_paths: dict[str, str],
) -> tuple[str | None, dict | None, str | None]:
    """计算任务指纹.

    返回 (task_fingerprint, stage_fingerprints, git_rev):
      - task_fingerprint: None 表示"不做代码版本指纹" (git:false 且无法求 rev)
      - stage_fingerprints: stage 级指纹 dict {stage_idx: fp} (O4), 单 cmd 任务为 None
      - git_rev: 启动时审计用 (B7)
    """
    # venv 路径: 把 cmd 里的 {VENV:name} 解析为实际解释器路径入指纹
    def resolve_venv(tok: str) -> str:
        if tok.startswith("{VENV:") and tok.endswith("}"):
            name = tok[len("{VENV:"):-1]
            return venv_paths.get(name, tok)
        return tok

    rev = _git_rev(cwd) if git is not False else None
    use_code = git is not False  # git:false 强制不做; 自动探测时看能否求到 rev
    if use_code and rev is None and git is None:
        # 自动探测: 非 git 目录 -> 不做代码版本指纹 (§3.2 约定)
        use_code = False

    def fp_for(cmd_list: list[str]) -> str:
        resolved = [resolve_venv(t) for t in cmd_list]
        payload = json.dumps({"cmd": resolved, "rev": rev if use_code else None})
        return hashlib.sha256(payload.encode()).hexdigest()

    if stages is not None:
        stage_fps = {str(i): fp_for(s["cmd"]) for i, s in enumerate(stages)}
        # 任务级指纹 = 全部 stage 指纹串联 (代码/venv 任一变化即变)
        task_fp = hashlib.sha256(
            json.dumps(stage_fps, sort_keys=True).encode()
        ).hexdigest()
        return task_fp, stage_fps, rev
    else:
        return fp_for(cmd or []), None, rev
