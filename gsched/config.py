"""config.json 加载与模板变量解析 (I/J 类: 账户/节点/目录/任务解耦).

配置源: {STATE}/config.json (可用环境变量 SCHED_CONFIG 覆盖路径).
config.json 是唯一权威配置, 框架代码零写死账户/节点/路径/项目名.
"""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any

# 默认 state 目录 (与文档 §6 决策 3 一致)
DEFAULT_STATE_DIR = os.path.expanduser("~/.sched")

_runtime_state_dir: str | None = None

# 模板变量: 解析目标见 resolve_template
TEMPLATE_PREFIX = "{"
TEMPLATE_SUFFIX = "}"


class ConfigError(Exception):
    """配置缺失/非法."""


def default_state_dir() -> str:
    """Return the normalized runtime data root."""
    if "SCHED_STATE" in os.environ:
        root = os.environ["SCHED_STATE"]
    else:
        root = _runtime_state_dir or DEFAULT_STATE_DIR
    return os.path.normpath(os.path.abspath(os.path.expanduser(root)))


def parse_gpus(cfg: dict[str, Any]) -> tuple[list[int], dict[int, float], dict[int, int]]:
    """归一化 config.gpus -> (卡号列表, 显存覆盖表 idx->GiB, 单卡打包上限 idx->N).

    支持两种形态 (向后兼容):
      [0, 1, 2, 3]                       纯卡号 (显存自动探测)
      [{"idx": 0, "mem_gib": 24}, ...]  带显存覆盖 (不写 mem_gib 或缺省 -> 自动探测)
    mem_gib 语义 = 该卡总容量 (GiB), 覆盖 daemon 启动 nvidia-smi 探测值
    (手动配置异构卡容量 / 无 nvidia-smi 的环境用).
    B12-c: 对象形态可选 max_jobs (正整数) = 该卡共享装箱任务数上限
    (异构卡差异化密度; 缺省跟随全局 co_locate_max_jobs). 热键 —— 可热更新.
    """
    raw = cfg.get("gpus")
    if raw is None:
        raw = []
    if not isinstance(raw, list):
        raise ConfigError("gpus 必须是卡号整数数组或对象数组")
    idxs: list[int] = []
    mem: dict[int, float] = {}
    mj: dict[int, int] = {}
    for g in raw:
        if isinstance(g, dict):
            idx = g.get("idx")
            if not isinstance(idx, int) or isinstance(idx, bool) or idx < 0:
                raise ConfigError(f"gpus 对象形态必须有非负整数 idx: {g}")
            idxs.append(idx)
            m = g.get("mem_gib")
            if m is not None:
                try:
                    capacity = float(m)
                except (TypeError, ValueError, OverflowError):
                    capacity = math.nan
                if (
                    not isinstance(m, (int, float))
                    or isinstance(m, bool)
                    or not math.isfinite(capacity)
                    or capacity <= 0
                ):
                    raise ConfigError(f"gpus[{idx}].mem_gib 必须是有限正数 (GiB): {g}")
                mem[idx] = capacity
            mjv = g.get("max_jobs")
            if mjv is not None:
                if not isinstance(mjv, int) or isinstance(mjv, bool) or mjv < 1:
                    raise ConfigError(f"gpus[{idx}].max_jobs 必须是正整数: {g}")
                mj[idx] = mjv
        else:
            if not isinstance(g, int) or isinstance(g, bool) or g < 0:
                # M17: 原消息引用只在 dict 分支赋值的 idx -> NameError
                raise ConfigError(f"gpus 必须是非负卡号整数数组或对象数组: {g}")
            idxs.append(g)
    # 去重保序 (同一卡重复声明 -> 后者覆盖显存, 卡号只留一个)
    seen: set[int] = set()
    uniq: list[int] = []
    for i in idxs:
        if i not in seen:
            seen.add(i)
            uniq.append(i)
    return uniq, mem, mj


def config_path() -> str:
    """Bootstrap config path, independent of the installed runtime data root."""
    if os.environ.get("SCHED_CONFIG"):
        return os.environ["SCHED_CONFIG"]
    bootstrap_root = os.environ.get("SCHED_STATE", DEFAULT_STATE_DIR)
    return os.path.join(os.path.expanduser(bootstrap_root), "config.json")


def load_config(
    path: str | None = None, *, apply_runtime_state: bool = True
) -> dict[str, Any]:
    """Load and validate config, optionally installing its runtime state root."""
    p = os.path.normpath(os.path.abspath(os.path.expanduser(path or config_path())))
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
    global _runtime_state_dir
    if apply_runtime_state and "SCHED_STATE" not in os.environ:
        configured_root = cfg.get("state_dir")
        if configured_root:
            configured_root = os.path.expanduser(str(configured_root))
            if not os.path.isabs(configured_root):
                configured_root = os.path.join(os.path.dirname(p), configured_root)
            _runtime_state_dir = os.path.normpath(os.path.abspath(configured_root))
        else:
            _runtime_state_dir = None
    return cfg


def _validate(cfg: dict[str, Any], p: str) -> None:
    """基本结构校验 (字段缺失即报错, 快速失败)."""
    if not isinstance(cfg, dict):
        raise ConfigError(f"{p}: 顶层必须是 JSON 对象")
    if not cfg.get("user"):
        raise ConfigError(f"{p}: 缺少 user (运行身份账户名)")
    if not cfg.get("node"):
        raise ConfigError(f"{p}: 缺少 node (daemon 所在计算节点名)")
    node = cfg["node"]
    if (
        not isinstance(node, str)
        or not node.strip()
        or node != node.strip()
        or node in (".", "..")
        or "/" in node
        or "\\" in node
        or "\x00" in node
    ):
        raise ConfigError(f"{p}: node 必须是安全的单一路径组件")
    if "projects" not in cfg or not isinstance(cfg["projects"], dict):
        raise ConfigError(f"{p}: 缺少 projects (多项目 root 映射)")
    state_dir = cfg.get("state_dir")
    if state_dir is not None and (
        not isinstance(state_dir, str) or not state_dir.strip()
    ):
        raise ConfigError(f"{p}: state_dir 必须是非空字符串")

    # B11c: projects schema validation (multi-project quota/priority/affinity)
    for proj_name, proj_cfg in cfg["projects"].items():
        if not isinstance(proj_cfg, dict):
            raise ConfigError(f"{p}: projects.{proj_name} 必须是对象")
        if "root" not in proj_cfg:
            raise ConfigError(f"{p}: projects.{proj_name} 缺少 root 字段")
        if not isinstance(proj_cfg["root"], str) or not proj_cfg["root"]:
            raise ConfigError(f"{p}: projects.{proj_name}.root 必须是非空字符串")
        if not isinstance(proj_cfg.get("gpu_enabled", True), bool):
            raise ConfigError(f"{p}: projects.{proj_name}.gpu_enabled 必须是布尔 (缺省=true)")
        gq = proj_cfg.get("gpu_quota")
        if gq is not None:
            if not isinstance(gq, int) or isinstance(gq, bool) or gq < 0:
                raise ConfigError(f"{p}: projects.{proj_name}.gpu_quota 必须是非负整数 (0=无限制)")
        pr = proj_cfg.get("priority")
        if pr is not None:
            if not isinstance(pr, int) or isinstance(pr, bool):
                raise ConfigError(f"{p}: projects.{proj_name}.priority 必须是整数")
        ga = proj_cfg.get("gpu_affinity")
        if ga is not None:
            if not isinstance(ga, list) or not all(isinstance(x, int) and not isinstance(x, bool) and x >= 0 for x in ga):
                raise ConfigError(f"{p}: projects.{proj_name}.gpu_affinity 必须是非负卡号整数数组")
        hard_affinity = proj_cfg.get("gpu_affinity_hard", False)
        if not isinstance(hard_affinity, bool):
            raise ConfigError(
                f"{p}: projects.{proj_name}.gpu_affinity_hard 必须是布尔"
            )
        if hard_affinity and not ga:
            raise ConfigError(
                f"{p}: projects.{proj_name}.gpu_affinity_hard=true 必须同时声明非空 gpu_affinity"
            )
        # B12-b: 项目级 colocate 开关 (可选布尔; 缺省=中立跟随全局, 与门模型)
        col = proj_cfg.get("colocate")
        if col is not None and not isinstance(col, bool):
            raise ConfigError(f"{p}: projects.{proj_name}.colocate 必须是布尔 (缺省=跟随全局)")
        pmj = proj_cfg.get("max_jobs")
        if pmj is not None and (
            not isinstance(pmj, int) or isinstance(pmj, bool) or pmj < 1
        ):
            raise ConfigError(f"{p}: projects.{proj_name}.max_jobs 必须是正整数"
                              f" (该项目任务在单卡上的打包数上限)")
    # 全局 co_locate_max_jobs 的范围校验在下方定案 39 范围表 ([2,64]), 不在此重复
    tde = cfg.get("task_default_env")
    if tde is not None:
        if not isinstance(tde, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in tde.items()
        ):
            raise ConfigError(
                f"{p}: task_default_env 必须是 字符串->字符串 的对象"
                " (部署级任务环境缺省值, batch/task env 可覆盖)")
    ced = cfg.get("conda_envs_dirs")
    if ced is not None:
        if not isinstance(ced, list) or not all(
            isinstance(x, str) and x.strip() for x in ced
        ):
            raise ConfigError(f"{p}: conda_envs_dirs 必须是非空字符串数组")
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
    # 通知 (现行配置见 docs/reference.md; 可选, 缺省 = 功能关闭)
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
    #   co_locate_max_jobs 每卡任务数上限 [2,64] 默认 3; co_locate_freeze_pct L3 冻结阈值 (0,100) 默认 85
    if cfg.get("co_locate") is not None and not isinstance(cfg["co_locate"], bool):
        raise ConfigError(f"{p}: co_locate 必须是布尔 (全局开关)")
    for k, lo, hi, dfl in (
        ("co_locate_safety", 0.5, 0.85, 0.7),
        ("co_locate_max_jobs", 2, 64, 3),   # B13-§3: 轻任务场景放宽; 细粒度走卡级/项目级 max_jobs
        ("co_locate_freeze_pct", 1, 100, 85),
    ):
        v = cfg.get(k)
        if v is None:
            continue
        if k == "co_locate_max_jobs" and (not isinstance(v, int) or isinstance(v, bool)):
            raise ConfigError(f"{p}: {k} 必须是 [{lo}, {hi}] 范围内的整数")
        if not isinstance(v, (int, float)) or isinstance(v, bool) or not (lo <= v <= hi):
            raise ConfigError(f"{p}: {k} 必须在 [{lo}, {hi}] 范围 (默认 {dfl})")


def project_gpu_enabled(cfg: dict, project: str | None) -> bool:
    """Unknown projects and invalid switches cannot grant GPU access."""
    entry = cfg.get("projects", {}).get(project)
    return isinstance(entry, dict) and entry.get("gpu_enabled", True) is True


def task_environment(cfg: dict, batch_env: dict | None, task_env: dict | None) -> dict[str, str]:
    """Merge declared environment identically for fingerprinting and launch."""
    return {
        str(key): str(value)
        for layer in (cfg.get("task_default_env") or {}, batch_env or {}, task_env or {})
        for key, value in layer.items()
    }


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


def resolve_runtime(rt: Any, cfg: dict[str, Any]) -> str:
    """B15: 解析任务级 runtime 声明 -> 环境前缀目录. 三通道恰选其一.

      {"venv_alias": "k"}       查 config.venvs 注册表, 解释器路径上两级为前缀
      {"conda_env": "timerxl"}  依次查 conda_envs_dirs 下同名目录
      {"prefix": "/abs/path"}   直接前缀
    """
    import os as _os

    if not isinstance(rt, dict):
        raise ConfigError("runtime 必须是对象")
    channels = {k: rt[k] for k in ("venv_alias", "conda_env", "prefix") if rt.get(k)}
    if len(channels) != 1:
        raise ConfigError(
            "runtime 需要且仅需一个通道: venv_alias / conda_env / prefix"
            f" (收到: {sorted(rt.keys())})"
        )
    if "venv_alias" in channels:
        name = str(channels["venv_alias"])
        py = (cfg.get("venvs") or {}).get(name)
        if not py:
            known = ", ".join(sorted((cfg.get("venvs") or {}).keys())) or "(无)"
            raise ConfigError(f"runtime.venv_alias '{name}' 未在 config.venvs 中定义 (可选: {known})")
        return _os.path.dirname(_os.path.dirname(str(py)))
    if "conda_env" in channels:
        name = str(channels["conda_env"])
        dirs = list(cfg.get("conda_envs_dirs") or ["~/miniconda3/envs"])
        for d in dirs:
            cand = _os.path.expanduser(_os.path.join(d, name))
            if _os.path.isdir(cand):
                return cand
        raise ConfigError(
            f"runtime.conda_env '{name}' 在 conda_envs_dirs {dirs} 中未找到"
        )
    pref = _os.path.expanduser(str(channels["prefix"]))
    if not _os.path.isdir(pref):
        raise ConfigError(f"runtime.prefix 目录不存在: {pref}")
    return pref
