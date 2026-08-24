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
    runtime_prefix: str | None = None,
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

    # B13-§4: dirty-tree hash —— 工作区有未提交改动时并入指纹.
    # 只在"脏"时才入哈希: 干净树的指纹与旧版完全一致, 升级不触发全量重跑;
    # 脏树指纹必变 -> "改了代码没 commit 就 resubmit 还 SKIP"的隐蔽陷阱消除.
    dirty_hash = None
    if use_code and rev is not None:
        try:
            # -uno: 忽略未跟踪文件 —— 任务产物/临时文件写进仓库不该使指纹变脏;
            # 只感知已跟踪文件的修改 (真正的代码改动)
            dout = subprocess.run(
                ["git", "-C", cwd, "status", "--porcelain", "-uno"],
                capture_output=True, text=True, timeout=10,
            )
            if dout.returncode == 0 and dout.stdout.strip():
                dirty_hash = hashlib.sha256(dout.stdout.encode()).hexdigest()[:16]
        except (subprocess.SubprocessError, FileNotFoundError):
            pass

    def fp_for(cmd_list: list[str]) -> str:
        resolved = [resolve_venv(t) for t in cmd_list]
        payload = json.dumps({
            "cmd": resolved,
            "rev": rev if use_code else None,
            "dirty": dirty_hash,   # None = 干净树 (与历史指纹兼容)
            "runtime": runtime_prefix,   # B15: 声明了才参与哈希 (环境漂移可审计)
        })
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
