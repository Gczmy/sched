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


def parse_gpus(cfg: dict[str, Any]) -> tuple[list[int], dict[int, float]]:
    """归一化 config.gpus -> (卡号列表, 显存覆盖表 idx->GiB).

    支持两种形态 (向后兼容):
      [0, 1, 2, 3]                       纯卡号 (显存自动探测)
      [{"idx": 0, "mem_gib": 24}, ...]  带显存覆盖 (不写 mem_gib 或缺省 -> 自动探测)
    mem_gib 语义 = 该卡总容量 (GiB), 覆盖 daemon 启动 nvidia-smi 探测值
    (手动配置异构卡容量 / 无 nvidia-smi 的环境用).
    """
    raw = cfg.get("gpus") or []
    idxs: list[int] = []
    mem: dict[int, float] = {}
    for g in raw:
        if isinstance(g, dict):
            idx = g.get("idx")
            if idx is None or not isinstance(idx, int) or isinstance(idx, bool):
                raise ConfigError(f"gpus 对象形态必须有整数 idx: {g}")
            idxs.append(idx)
            m = g.get("mem_gib")
            if m is not None:
                if not isinstance(m, (int, float)) or isinstance(m, bool) or m <= 0:
                    raise ConfigError(f"gpus[{idx}].mem_gib 必须 > 0 的数字 (GiB): {g}")
                mem[idx] = float(m)
        else:
            if not isinstance(g, int) or isinstance(g, bool):
                # M17: 原消息引用只在 dict 分支赋值的 idx -> NameError
                raise ConfigError(f"gpus 必须是卡号整数数组或对象数组: {g}")
            idxs.append(g)
    # 去重保序 (同一卡重复声明 -> 后者覆盖显存, 卡号只留一个)
    seen: set[int] = set()
    uniq: list[int] = []
    for i in idxs:
        if i not in seen:
            seen.add(i)
            uniq.append(i)
    return uniq, mem


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
    # CPU 配额制 (可选): cpus_total 节点总核数 (0=不限制), gpu_job_cpus GPU 任务默认 CPU 占用
    for k, min_v in (("cpus_total", 0), ("gpu_job_cpus", 1), ("max_cpu_jobs", 1)):
        v = cfg.get(k)
        if v is not None and (not isinstance(v, int) or isinstance(v, bool) or v < min_v):
            raise ConfigError(f"{p}: {k} 必须是整数且 >= {min_v}")
    # gpus 归一化校验 (两种形态, 2026-08-17): 纯卡号数组向后兼容;
    # 对象数组 {idx, mem_gib} 支持显存覆盖 (异构卡容量/无 nvidia-smi 环境)
    if "gpus" in cfg and cfg["gpus"] is not None:
        parse_gpus(cfg)  # 抛 ConfigError = 非法
    # 通知 (设计 docs/sched_notify_design.md §3, 可选; 缺省 = 功能关闭)
    nf = cfg.get("notify")
    if nf is not None:
        if not isinstance(nf, dict):
            raise ConfigError(f"{p}: notify 必须是对象")
        on = nf.get("on")
        if on is not None and (
            not isinstance(on, list)
            or not all(e in ("batch_done", "batch_blocked") for e in on)
        ):
            raise ConfigError(
                f"{p}: notify.on 必须是 batch_done/batch_blocked 子集数组"
            )
        em = nf.get("email")
        if em is not None:
            if not isinstance(em, dict):
                raise ConfigError(f"{p}: notify.email 必须是对象")
            for k in ("smtp_host", "from"):
                if not em.get(k) or not isinstance(em[k], str):
                    raise ConfigError(f"{p}: notify.email 缺少 {k}")
            to = em.get("to")
            if not isinstance(to, list) or not to or not all(
                isinstance(x, str) for x in to
            ):
                raise ConfigError(f"{p}: notify.email.to 必须是非空邮箱数组")
            port = em.get("smtp_port", 465)
            if not isinstance(port, int) or isinstance(port, bool) or not (1 <= port <= 65535):
                raise ConfigError(f"{p}: notify.email.smtp_port 非法: {port}")
            # password_env 可选 (决策 3): 缺省 = 无认证内网 relay
            if em.get("password_env") is not None and not isinstance(
                em["password_env"], str
            ):
                raise ConfigError(f"{p}: notify.email.password_env 必须是环境变量名")
        for ch in ("file", "webhook"):
            if nf.get(ch) is not None and not isinstance(nf[ch], dict):
                raise ConfigError(f"{p}: notify.{ch} 必须是对象")
        if nf.get("command") is not None and (
            not isinstance(nf["command"], list)
            or not all(isinstance(x, str) for x in nf["command"])
        ):
            raise ConfigError(f"{p}: notify.command 必须是命令数组")

    # co-location (定案 39 待定项 4, 实验性默认关):
    #   co_locate: bool 全局开关; co_locate_safety 安全系数 [0.5,0.85] 默认 0.7;
    #   co_locate_max_jobs 每卡任务数上限 [2,8] 默认 3; co_locate_freeze_pct L3 冻结阈值 (0,100) 默认 85
    if cfg.get("co_locate") is not None and not isinstance(cfg["co_locate"], bool):
        raise ConfigError(f"{p}: co_locate 必须是布尔 (全局开关)")
    for k, lo, hi, dfl in (
        ("co_locate_safety", 0.5, 0.85, 0.7),
        ("co_locate_max_jobs", 2, 8, 3),
        ("co_locate_freeze_pct", 1, 100, 85),
    ):
        v = cfg.get(k)
        if v is None:
            continue
        if not isinstance(v, (int, float)) or isinstance(v, bool) or not (lo <= v <= hi):
            raise ConfigError(f"{p}: {k} 必须在 [{lo}, {hi}] 范围 (默认 {dfl})")


def resolve_template(value: str, cfg: dict[str, Any], cwd: str | None = None) -> str:
    """解析模板变量 {ROOT} / {PROJECT:name} / {VENV:name} / {STATE}.

    嵌套路径保留: "{ROOT}/nn/data" -> "/home/.../veighna-trade/nn/data".
    未识别变量 -> 保持原样 (由调用方决定是否报错).
    """
    if not isinstance(value, str) or not value.startswith(TEMPLATE_PREFIX):
        return value

    # H7 修复: 取首个 {...} 段做替换并保留余量; 旧逻辑 value[1:] 会把
    # "{ROOT}/nn/data" 当成名为 "ROOT}/nn/data" 的未知模板原样返回
    end = value.find(TEMPLATE_SUFFIX)
    if end == -1:
        return value  # 无闭合括号: 非模板, 保持原样
    name = value[1:end]
    rest = value[end + 1:]
    if name == "ROOT":
        return _project_root(cfg, cfg.get("default_project", "a_share")) + rest
    if name.startswith("PROJECT:"):
        return _project_root(cfg, name.split(":", 1)[1]) + rest
    if name.startswith("VENV:"):
        venv = cfg.get("venvs", {}).get(name[len("VENV:"):])
        if not venv:
            raise ConfigError(f"venv 未在 config.venvs 中定义: {name}")
        return str(venv) + rest
    if name == "STATE":
        return default_state_dir() + rest
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
