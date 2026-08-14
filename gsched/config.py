"""config.json 加载与模板变量解析 (I/J 类: 账户/节点/目录/任务解耦).

配置源: {STATE}/config.json (可用环境变量 SCHED_CONFIG 覆盖路径).
config.json 是唯一权威配置, 框架代码零写死账户/节点/路径/项目名.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

# 默认 state 目录 (与文档 §6 决策 3 一致)
DEFAULT_STATE_DIR = os.path.expanduser("~/.sched")

# 模板变量: 解析目标见 resolve_template
TEMPLATE_PREFIX = "{"
TEMPLATE_SUFFIX = "}"


class ConfigError(Exception):
    """配置缺失/非法."""


def default_state_dir() -> str:
    """state 目录: SCHED_STATE 环境变量 > 默认 ~/.sched."""
    return os.environ.get("SCHED_STATE", DEFAULT_STATE_DIR)


def config_path() -> str:
    """config.json 路径: SCHED_CONFIG 覆盖 > {STATE}/config.json."""
    if os.environ.get("SCHED_CONFIG"):
        return os.environ["SCHED_CONFIG"]
    return os.path.join(default_state_dir(), "config.json")


def load_config(path: str | None = None) -> dict[str, Any]:
    """加载并校验 config.json. 缺失/非法报 ConfigError."""
    p = path or config_path()
    if not os.path.isfile(p):
        raise ConfigError(
            f"config.json 不存在: {p}\n"
            f"  请先运行 `sched init` 生成 (或设置 SCHED_CONFIG 指向正确路径)"
        )
    try:
        with open(p, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except json.JSONDecodeError as e:
        raise ConfigError(f"config.json 解析失败: {e}") from e

    _validate(cfg, p)
    return cfg


def _validate(cfg: dict[str, Any], p: str) -> None:
    """基本结构校验 (字段缺失即报错, 快速失败)."""
    if not isinstance(cfg, dict):
        raise ConfigError(f"{p}: 顶层必须是 JSON 对象")
    if not cfg.get("user"):
        raise ConfigError(f"{p}: 缺少 user (运行身份账户名)")
    if not cfg.get("node"):
        raise ConfigError(f"{p}: 缺少 node (daemon 所在计算节点名)")
    if "projects" not in cfg or not isinstance(cfg["projects"], dict):
        raise ConfigError(f"{p}: 缺少 projects (多项目 root 映射)")
    if "venvs" not in cfg or not isinstance(cfg["venvs"], dict):
        raise ConfigError(f"{p}: 缺少 venvs (语义名 -> 解释器路径)")
    # CPU 配额制 (可选): cpus_total 节点总核数, gpu_job_cpus GPU 任务默认 CPU 占用
    for k in ("cpus_total", "gpu_job_cpus", "max_cpu_jobs"):
        v = cfg.get(k)
        if v is not None and (not isinstance(v, int) or isinstance(v, bool) or v < 1):
            raise ConfigError(f"{p}: {k} 必须是正整数")


def resolve_template(value: str, cfg: dict[str, Any], cwd: str | None = None) -> str:
    """解析模板变量 {ROOT} / {PROJECT:name} / {VENV:name} / {STATE}.

    嵌套路径保留: "{ROOT}/nn/data" -> "/home/.../veighna-trade/nn/data".
    未识别变量 -> 保持原样 (由调用方决定是否报错).
    """
    if not isinstance(value, str) or not value.startswith(TEMPLATE_PREFIX):
        return value

    name = value[1:-1] if value.endswith(TEMPLATE_SUFFIX) else value[1:]
    if name == "ROOT":
        return _project_root(cfg, cfg.get("default_project", "a_share"))
    if name.startswith("PROJECT:"):
        return _project_root(cfg, name.split(":", 1)[1])
    if name.startswith("VENV:"):
        venv = cfg.get("venvs", {}).get(name[len("VENV:"):])
        if not venv:
            raise ConfigError(f"venv 未在 config.venvs 中定义: {name}")
        return str(venv)
    if name == "STATE":
        return default_state_dir()
    return value  # 未知模板: 保持原样


def _project_root(cfg: dict[str, Any], proj: str) -> str:
    entry = cfg.get("projects", {}).get(proj)
    if not entry:
        raise ConfigError(f"project 未在 config.projects 中定义: {proj}")
    # projects 值是对象 {root, git} (J 类), 兼容纯字符串
    if isinstance(entry, dict):
        root = entry.get("root")
        if not root:
            raise ConfigError(f"project {proj} 缺少 root 字段")
    else:
        root = entry
    return str(root)


def expand_path(value: str, cfg: dict[str, Any], base_cwd: str | None = None) -> str:
    """模板展开 + 相对路径相对 base_cwd 归一化为绝对路径 (供 E4 校验/executor)."""
    expanded = resolve_template(value, cfg)
    if os.path.isabs(expanded):
        return os.path.normpath(expanded)
    base = base_cwd or resolve_template(
        cfg.get("default_project", "{ROOT}"), cfg
    )
    return os.path.normpath(os.path.join(base, expanded))
