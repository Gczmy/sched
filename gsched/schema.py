"""batch.json 校验 (文档 §4.1 + B10 E4 + B13 H1 + §3.4e 依赖环检测).

校验失败报 SchemaError (明确错误, 不污染状态).
"""

from __future__ import annotations

import json
import os
import shlex
from typing import Any

from .config import ConfigError, expand_path, resolve_template

SUDO_TOKENS = {"sudo", "su", "runuser"}


class SchemaError(Exception):
    pass


def _require_type(v: Any, t: type, field: str, where: str) -> None:
    if not isinstance(v, t):
        raise SchemaError(f"{where}.{field}: 期望 {t.__name__}, 实际 {type(v).__name__}")


def _check_sudo_tokens(tokens: list[str], where: str) -> None:
    """H1: cmd 数组/sched run 的 shell token 含 sudo/su/runuser 直接拒绝."""
    for tok in tokens:
        if tok in SUDO_TOKENS:
            raise SchemaError(
                f"{where}: 含特权命令 '{tok}' —— 框架无 sudo 硬约束 (H1),"
                " 请重新设计为无 sudo 方案"
            )


def _check_path_in_cwd(path: str, cwd_abs: str, where: str) -> None:
    """E4: 产物/日志路径必须 realpath 归一化后在任务 cwd 内."""
    real = os.path.realpath(path)
    real_cwd = os.path.realpath(cwd_abs)
    if real != real_cwd and not real.startswith(real_cwd + os.sep):
        raise SchemaError(
            f"{where}: 路径 {path} 逃出任务 cwd ({real_cwd}) ——"
            " 跨 cwd 写产物需显式声明 paths_escape: true (E4)"
        )


def validate_batch(spec: dict, cfg: dict) -> dict:
    """校验整个 batch.json, 返回规范化后的 spec (模板已展开, cwd 已归一化).

    通过后调用方持 spec 入队 (insert_batch + insert_task + insert_job).
    """
    if not isinstance(spec, dict):
        raise SchemaError("batch.json 顶层必须是 JSON 对象")

    name = spec.get("name")
    if not name or not isinstance(name, str):
        raise SchemaError("缺少 name (批次名, 依赖按 name 引用)")

    mode = spec.get("mode", "mix")
    if mode not in ("mix", "strict"):
        raise SchemaError(f"mode 必须是 mix|strict, 实际 {mode}")

    depends_on = spec.get("depends_on", [])
    if not isinstance(depends_on, list) or not all(
        isinstance(d, str) for d in depends_on
    ):
        raise SchemaError("depends_on 必须是 [batch_name] 字符串数组")

    gpus = spec.get("gpus")
    if gpus is not None and (
        not isinstance(gpus, list) or not all(isinstance(g, int) for g in gpus)
    ):
        raise SchemaError("gpus 必须是卡号整数数组 (如 [0,1,2,3])")

    env = spec.get("env", {})
    if not isinstance(env, dict):
        raise SchemaError("env 必须是对象")

    # 批次 cwd: 模板展开
    batch_cwd = resolve_template(spec.get("cwd", "{ROOT}"), cfg)
    batch_cwd_abs = os.path.realpath(
        os.path.expanduser(batch_cwd)
    )

    tasks = spec.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise SchemaError("tasks 必须是至少一个任务的对象数组")

    norm_tasks = []
    seen_ids: set[str] = set()
    for i, t in enumerate(tasks):
        norm_tasks.append(
            _validate_task(t, cfg, batch_cwd_abs, f"tasks[{i}]", seen_ids)
        )

    return {
        "name": name,
        "mode": mode,
        "depends_on": depends_on,
        "gpus": gpus,
        "cwd": spec.get("cwd", "{ROOT}"),
        "cwd_abs": batch_cwd_abs,
        "env": env,
        "tasks": norm_tasks,
    }


def _validate_task(
    t: Any, cfg: dict, batch_cwd_abs: str, where: str, seen_ids: set[str]
) -> dict:
    if not isinstance(t, dict):
        raise SchemaError(f"{where}: 任务必须是对象")

    tid = t.get("id")
    if not tid or not isinstance(tid, str):
        raise SchemaError(f"{where}: 缺少 id")
    if tid in seen_ids:
        raise SchemaError(f"{where}: 重复任务 id '{tid}'")
    seen_ids.add(tid)

    # cwd: 任务级覆盖 (任意目录, J 类)
    t_cwd_abs = batch_cwd_abs
    if t.get("cwd"):
        t_cwd_abs = os.path.realpath(
            os.path.expanduser(resolve_template(t["cwd"], cfg))
        )

    # cmd 数组
    stages = t.get("stages")
    if stages is not None:
        if not isinstance(stages, list) or not stages:
            raise SchemaError(f"{where}: stages 必须是非空数组")
        norm_stages = []
        for si, s in enumerate(stages):
            norm_stages.append(
                _validate_stage(s, cfg, t_cwd_abs, f"{where}.stages[{si}]")
            )
        cmd = None
        stage_specs = norm_stages
    else:
        cmd = t.get("cmd")
        if not isinstance(cmd, list) or not cmd:
            raise SchemaError(f"{where}: 必须提供 cmd 数组 (或 stages 数组)")
        _check_sudo_tokens([str(c) for c in cmd], f"{where}.cmd")
        # 解释器必须是 {VENV:...} 模板 (venv 不单设字段)
        if not str(cmd[0]).startswith("{VENV:"):
            raise SchemaError(
                f"{where}.cmd[0]: 解释器必须用 {{VENV:<name>}} 模板"
                " (I 类, venv 不单设字段)"
            )
        stage_specs = None

    # 产物路径 E4 校验
    artifacts = t.get("artifacts", {})
    if not isinstance(artifacts, dict):
        raise SchemaError(f"{where}.artifacts: 必须是对象")
    for key, a in artifacts.items():
        if not isinstance(a, dict) or not a.get("path"):
            raise SchemaError(f"{where}.artifacts.{key}: 缺 path")
        if not t.get("paths_escape"):
            p = expand_path(a["path"], cfg, t_cwd_abs)
            _check_path_in_cwd(p, t_cwd_abs, f"{where}.artifacts.{key}")

    # resources: {cpus: N, gpu: 0|1} —— gpu: 0 = CPU-only 任务 (不占 GPU 槽位, §5b B4)
    resources = t.get("resources", {})
    if not isinstance(resources, dict):
        raise SchemaError(f"{where}.resources: 必须是对象")
    cpus = resources.get("cpus")
    if cpus is not None:
        if not isinstance(cpus, int) or isinstance(cpus, bool) or cpus < 1:
            raise SchemaError(f"{where}.resources.cpus: 必须是正整数 (声明 CPU 配额)")
    gpu_req = resources.get("gpu", 1)  # 缺省 gpu=1 (向后兼容: 每卡一任务)
    if gpu_req not in (0, 1):
        raise SchemaError(f"{where}.resources.gpu: 必须是 0 (CPU-only) 或 1 (占 1 GPU)")
    resources = dict(resources)
    resources["gpu"] = gpu_req
    if gpu_req == 0 and "cpus" not in resources:
        resources["cpus"] = 1  # CPU-only 缺省 1 核 (配额制调度用)
    # co-location (§3.2e 待定项 2, 定案 39): gpu_share 必须声明 vram_gib (GiB,
    # 装箱必须有名数, 缺省 = 无法装箱); vram_gib 非法值拒绝.
    gpu_share = resources.get("gpu_share", False)
    if gpu_share not in (True, False):
        raise SchemaError(f"{where}.resources.gpu_share: 必须是布尔")
    vram = resources.get("vram_gib")
    if gpu_share:
        if vram is None:
            raise SchemaError(
                f"{where}.resources: gpu_share=true 必须声明 vram_gib (GiB 峰值)"
            )
        if not isinstance(vram, (int, float)) or isinstance(vram, bool) or vram <= 0:
            raise SchemaError(f"{where}.resources.vram_gib: 必须是正数 (GiB)")
    elif vram is not None and (
        not isinstance(vram, (int, float)) or isinstance(vram, bool) or vram <= 0
    ):
        raise SchemaError(f"{where}.resources.vram_gib: 必须是正数 (GiB)")
    resources["gpu_share"] = gpu_share
    if vram is not None:
        resources["vram_gib"] = float(vram)
    profile_key = resources.get("profile_key")
    if profile_key is not None and not isinstance(profile_key, str):
        raise SchemaError(f"{where}.resources.profile_key: 必须是字符串")

    # retry_transform / probes 透传
    retry_transform = t.get("retry_transform")
    probes = t.get("probes")
    duration_min = t.get("duration_min")
    if duration_min is not None and not isinstance(duration_min, (int, float)):
        raise SchemaError(f"{where}.duration_min: 必须是数字 (分钟)")

    return {
        "id": tid,
        "cmd": cmd,
        "stages": stage_specs,
        "cwd_abs": t_cwd_abs,
        "git": t.get("git"),  # None=自动探测
        "env": t.get("env", {}),
        "resources": resources,
        "duration_min": duration_min,
        "max_retry": t.get("max_retry", 1),
        "artifacts": artifacts,
        "retry_transform": retry_transform,
        "probes": probes,
        "paths_escape": t.get("paths_escape", False),
    }


def _validate_stage(s: Any, cfg: dict, t_cwd_abs: str, where: str) -> dict:
    if not isinstance(s, dict):
        raise SchemaError(f"{where}: stage 必须是对象")
    cmd = s.get("cmd")
    if not isinstance(cmd, list) or not cmd:
        raise SchemaError(f"{where}: 缺 cmd 数组")
    _check_sudo_tokens([str(c) for c in cmd], f"{where}.cmd")
    if not str(cmd[0]).startswith("{VENV:"):
        raise SchemaError(f"{where}.cmd[0]: 解释器必须用 {{VENV:<name>}} 模板")

    artifacts = s.get("artifacts", {})
    if not isinstance(artifacts, dict):
        raise SchemaError(f"{where}.artifacts: 必须是对象")
    for key, a in artifacts.items():
        if not isinstance(a, dict) or not a.get("path"):
            raise SchemaError(f"{where}.artifacts.{key}: 缺 path")
        if not s.get("paths_escape"):
            p = expand_path(a["path"], cfg, t_cwd_abs)
            _check_path_in_cwd(p, t_cwd_abs, f"{where}.artifacts.{key}")

    return {
        "cmd": cmd,
        "artifacts": artifacts,
        "probes": s.get("probes"),
        "retry_transform": s.get("retry_transform"),
        "paths_escape": s.get("paths_escape", False),
    }


def check_dependency_cycle(depends_on: list[str], cfg: dict) -> None:
    """§3.4e: depends_on 引用不存在的 name 报错; 环检测由提交时按 name 拓扑 DFS."""
    # 引用存在性: config 不持有历史批次, 存在性由 daemon 提交时查 state (见 cli.submit)
    # 环检测: 提交链 (当前批次 + depends_on 链) 由 daemon 校验, 这里做语法级检查
    for d in depends_on:
        if not isinstance(d, str) or not d.strip():
            raise SchemaError(f"depends_on 含非法 name: {d}")


def parse_shell_cmd(shell_str: str, where: str) -> list[str]:
    """Q1: sched run 的 shell 字符串 shlex.split 后做 sudo 词法检查."""
    try:
        tokens = shlex.split(shell_str)
    except ValueError as e:
        raise SchemaError(f"{where}: shell 字符串解析失败: {e}") from e
    _check_sudo_tokens(tokens, where)
    return tokens
