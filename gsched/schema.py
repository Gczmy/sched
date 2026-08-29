"""batch.json 校验 (文档 §4.1 + B10 E4 + B13 H1 + §3.4e 依赖环检测).

校验失败报 SchemaError (明确错误, 不污染状态).
"""

from __future__ import annotations

import json
import os
import re
import shlex
from typing import Any

from .config import ConfigError, expand_path, resolve_template

SUDO_TOKENS = {"sudo", "su", "runuser"}
MAX_NESTED_SHELL_STATES = 1024
SHELL_TOKENS = {"sh", "bash", "dash", "zsh", "ksh", "fish"}


class SchemaError(Exception):
    pass


def _require_type(v: Any, t: type, field: str, where: str) -> None:
    if not isinstance(v, t):
        raise SchemaError(f"{where}.{field}: 期望 {t.__name__}, 实际 {type(v).__name__}")

SHELL_WRAPPERS = {"env", "command", "nohup", "eval", "builtin"}
COMMAND_LAUNCHERS = {
    "find", "xargs", "busybox", "nice", "timeout", "setsid",
    "stdbuf", "chrt", "taskset", "command", "nohup", "time", "!",
}
SCRIPT_INTERPRETERS = {
    "python", "python3", "perl", "ruby", "node", "nodejs", "php", "lua",
}
SHELL_CONTROL_WORDS = {"if", "then", "else", "elif", "while", "until", "for", "do", "case"}


def _shell_command_arg(tokens: list[str], idx: int) -> str | None:
    next_idx = idx + 1
    if next_idx < len(tokens) and tokens[next_idx] in ("--", "-", "+"):
        next_idx += 1
    return tokens[next_idx] if next_idx < len(tokens) else None


def _wrapper_command_tokens(tokens: list[str], idx: int) -> list[str]:
    """Return argv after a command wrapper's own options/assignments."""
    base = os.path.basename(tokens[idx])
    tail = tokens[idx + 1:]
    pos = 0
    while pos < len(tail):
        token = tail[pos]
        if base == "env":
            if re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", token):
                pos += 1
                continue
            if token == "--":
                return tail[pos + 1:]
            if token in ("-S", "--split-string") or token.startswith(("--split-string=", "-S")):
                raise SchemaError("env -S/--split-string 无法安全解析, 拒绝命令")
            if token in ("-u", "--unset", "-C", "--chdir"):
                pos += 2
                continue
            if token.startswith(("--unset=", "--chdir=")):
                pos += 1
                continue
            if token.startswith("-"):
                pos += 1
                continue
            return tail[pos:]
        if token == "--":
            return tail[pos + 1:]
        if token in ("-a", "--argv0"):
            pos += 2
            continue
        if token.startswith("-"):
            pos += 1
            continue
        return tail[pos:]
    return []


def _launcher_shell_payloads(tokens: list[str], launcher_idx: int) -> tuple[bool, list[str]]:
    base = os.path.basename(tokens[launcher_idx])
    candidates: list[int] = []
    if base == "find":
        for idx in range(launcher_idx + 1, len(tokens)):
            if tokens[idx] not in ("-exec", "-execdir"):
                continue
            for candidate in range(idx + 1, len(tokens)):
                if tokens[candidate] == ";":
                    break
                if os.path.basename(tokens[candidate]) in SHELL_TOKENS:
                    candidates.append(candidate)
    elif base in COMMAND_LAUNCHERS - {"find"}:
        candidates = [
            idx for idx in range(launcher_idx + 1, len(tokens))
            if os.path.basename(tokens[idx]) in SHELL_TOKENS
        ]
    payloads: list[str] = []
    for shell_idx in candidates:
        payloads.extend(_nested_shell_commands(tokens, shell_idx))
    return bool(candidates), payloads

def _launcher_argument_tokens(tokens: list[str], launcher_idx: int) -> list[str]:
    base = os.path.basename(tokens[launcher_idx])
    if base != "find":
        return tokens[launcher_idx + 1:]
    arguments: list[str] = []
    for idx in range(launcher_idx + 1, len(tokens)):
        if tokens[idx] not in ("-exec", "-execdir"):
            continue
        for candidate in range(idx + 1, len(tokens)):
            if tokens[candidate] == ";":
                break
            arguments.append(tokens[candidate])
    return arguments


def _cluster_command_index(flags: str, *, fish: bool = False) -> int:
    for pos, char in enumerate(flags):
        if char in ("o", "O"):
            return -1
        if char == "C" and fish:
            return pos
        if char == "c":
            return pos
    return -1


def _nested_shell_commands(tokens: list[str], shell_idx: int) -> list[str]:
    """Return every possible shell command-string payload."""
    commands: list[str] = []
    shell_name = os.path.basename(tokens[shell_idx])
    for idx in range(shell_idx + 1, len(tokens)):
        option = tokens[idx]
        if shell_name == "fish":
            if option in ("--command", "--init-command", "--init-cmd", "-C", "-c", "+c"):
                command = _shell_command_arg(tokens, idx)
                if command is not None:
                    commands.append(command)
                continue
            if option.startswith(("--command=", "--init-command=", "--init-cmd=", "-C=", "-c=", "+c=")):
                commands.append(option.partition("=")[2])
                continue
            if option.startswith(("-", "+")):
                flags = option[1:]
                c_index = _cluster_command_index(flags, fish=True)
                if c_index >= 0:
                    attached = flags[c_index + 1:]
                    if attached:
                        commands.append(attached)
                    else:
                        command = _shell_command_arg(tokens, idx)
                        if command is not None:
                            commands.append(command)
                    continue
            continue
        if option == "--command":
            command = _shell_command_arg(tokens, idx)
            if command is not None:
                commands.append(command)
            break
        if option.startswith("--command="):
            commands.append(option.partition("=")[2])
            break
        if option in ("-c", "+c"):
            command = _shell_command_arg(tokens, idx)
            if command is not None:
                commands.append(command)
            break
        if option.startswith("-c=") or option.startswith("+c="):
            commands.append(option[3:])
            break
        if option.startswith(("-", "+")):
            flags = option[1:]
            c_index = _cluster_command_index(flags)
            if c_index < 0:
                continue
            attached = flags[c_index + 1:]
            if attached:
                commands.append(attached)
            command = _shell_command_arg(tokens, idx)
            if command is not None:
                commands.append(command)
            break
    return commands


def _ends_shell_separator(tok: str) -> bool:
    return bool(re.search(r"[;|&]$", tok))


def _token_has_sudo(tok: str) -> bool:
    """Recognize privilege helpers embedded in shell punctuation/code."""
    candidates = re.findall(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", str(tok))
    return any(os.path.basename(candidate).lower() in SUDO_TOKENS for candidate in candidates)
def _check_sudo_tokens(
    tokens: list[str],
    where: str,
    cfg: dict[str, Any] | None = None,
    shell_payload: bool = False,
) -> None:
    """Reject privilege helpers and inspect nested shell payloads."""
    pending = [(tokens, where, shell_payload)]
    seen: set[tuple[bool, tuple[str, ...]]] = set()
    while pending:
        current, current_where, is_shell_payload = pending.pop()
        expanded: list[str] = []
        for token in current:
            if cfg is None or not isinstance(token, str):
                expanded.append(token)
                continue
            try:
                expanded.append(resolve_template(token, cfg))
            except ConfigError as e:
                raise SchemaError(str(e)) from e
        current_key = (is_shell_payload, tuple(expanded))
        if current_key in seen:
            continue
        if len(seen) >= MAX_NESTED_SHELL_STATES:
            raise SchemaError(
                f"{where}: nested shell/template expansion exceeds "
                f"{MAX_NESTED_SHELL_STATES} states"
            )
        seen.add(current_key)
        command_position = True
        for idx, tok in enumerate(expanded):
            base = os.path.basename(tok)
            if is_shell_payload and re.search(r"[$`]", tok):
                if command_position or re.search(r"(?i)(?:sudo|runuser|\bsu\b)", tok):
                    raise SchemaError(
                        f"{current_where}: 动态命令展开被拒绝 ('{tok}') —— "
                        "无法安全确认特权命令"
                    )
            if command_position and base == "eval":
                raise SchemaError(
                    f"{current_where}: eval 动态执行被拒绝 —— 无法安全检查特权命令"
                )
            if (
                (command_position and base in SUDO_TOKENS)
                or (is_shell_payload and _token_has_sudo(tok))
            ):
                raise SchemaError(
                    f"{current_where}: 含特权命令 '{tok}' —— 框架无 sudo 硬约束 (H1),"
                    " 请重新设计为无 sudo 方案"
                )
            if command_position and base == "exec":
                raise SchemaError(
                    f"{current_where}: exec 会绕过退出码封装, 拒绝命令"
                )
            if command_position and base in SCRIPT_INTERPRETERS:
                for option_idx in range(idx + 1, len(expanded)):
                    option = expanded[option_idx]
                    script = None
                    payload_options = ("-c", "--command", "-e", "--eval", "-r")
                    if base == "perl":
                        payload_options += ("-E", "--execute")
                    if base in {"node", "nodejs"}:
                        payload_options += ("-p", "--print")
                    if option in payload_options:
                        script = _shell_command_arg(expanded, option_idx)
                    elif option.startswith(("--command=", "--eval=", "--execute=", "--print=")):
                        script = option.partition("=")[2]
                    elif len(option) > 2 and option[:2] in ("-c", "-e", "-r", "-E", "-p"):
                        script = option[2:]
                    if script is None:
                        continue
                    pending.append(
                        ([script], f"{current_where} interpreter payload", True)
                    )
            if command_position and base in COMMAND_LAUNCHERS:
                for launcher_arg in _launcher_argument_tokens(expanded, idx):
                    if (
                        os.path.basename(launcher_arg) in SUDO_TOKENS
                        or _token_has_sudo(launcher_arg)
                    ):
                        raise SchemaError(
                            f"{current_where}: 命令启动器包含特权命令 '{launcher_arg}'"
                        )
                shell_found, launcher_payloads = _launcher_shell_payloads(expanded, idx)
                if shell_found and not launcher_payloads:
                    raise SchemaError(
                        f"{current_where}: 命令启动器中的 shell 缺少可检查命令"
                    )
                for command in launcher_payloads:
                    try:
                        nested = shlex.split(command)
                    except ValueError as e:
                        raise SchemaError(
                            f"{current_where}: launcher shell 解析失败: {e}"
                        ) from e
                    pending.append((nested, f"{current_where} command launcher", True))
            if command_position and base in SHELL_WRAPPERS:
                nested = _wrapper_command_tokens(expanded, idx)
                if nested:
                    pending.append((nested, f"{current_where} command wrapper", is_shell_payload))
            if (
                command_position
                or (
                    is_shell_payload
                    and any(token in SHELL_CONTROL_WORDS for token in expanded[:idx])
                )
            ) and base in {"source", "."}:
                raise SchemaError(
                    f"{current_where}: source 动态脚本执行被拒绝"
                )
            if command_position and base in SHELL_TOKENS:
                nested_commands = _nested_shell_commands(expanded, idx)
                if not nested_commands:
                    raise SchemaError(
                        f"{current_where}: 未提供可检查的嵌套 shell 命令"
                    )
                for command in nested_commands:
                    try:
                        nested = shlex.split(command)
                    except ValueError as e:
                        raise SchemaError(
                            f"{current_where}: nested shell 解析失败: {e}"
                        ) from e
                    pending.append((nested, f"{current_where} nested shell", True))
            if command_position and re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", tok):
                continue
            command_position = (
                _ends_shell_separator(tok) if is_shell_payload else False
            )


def check_sudo_tokens(
    tokens: list[str], where: str, cfg: dict[str, Any] | None = None
) -> None:
    """Public wrapper for validating commands after template expansion."""
    _check_sudo_tokens(tokens, where, cfg)
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
    if mode != "mix":
        raise SchemaError(f"mode 目前仅支持 mix (实际 {mode})")

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

    # 批次级通知覆盖 (设计 §3): false = 本批不通知; {"email_to": [...]} 改收件人;
    # 缺省/true = 跟随全局 config.notify
    bnotify = spec.get("notify")
    if bnotify is not None:
        if isinstance(bnotify, bool):
            pass
        elif isinstance(bnotify, dict):
            et = bnotify.get("email_to")
            if et is not None and (
                not isinstance(et, list)
                or not all(isinstance(x, str) for x in et)
            ):
                raise SchemaError("notify.email_to 必须是邮箱字符串数组")
        else:
            raise SchemaError('notify 必须是布尔或对象 (如 {"email_to": [...]})')

    # B11c 项目隔离 (强制): 批次必须声明所属项目, 且项目必须在 config.projects
    # 中已定义 -- 无 project 的提交会绕过配额/亲和隔离, 直接拒绝 (用户决策 2026-08-23)
    project = spec.get("project")
    if not isinstance(project, str) or not project.strip():
        raise SchemaError(
            "缺少 project 字段 (B11c 项目隔离): 必须为 config.projects 中已定义的项目名"
        )
    if project not in cfg.get("projects", {}):
        known = ", ".join(sorted(cfg.get("projects", {}).keys())) or "(无)"
        raise SchemaError(f"project '{project}' 未在 config.projects 中定义 (可选: {known})")

    # 批次 cwd: 模板展开。B11c 修复 (2026-08-26): 默认跟随批次声明的项目根
    # ({PROJECT:<project>})，而非 {ROOT}(=default_project) —— 否则 selfdist
    # 批次没写 cwd 时会在 veighna 仓库里跑、指纹也取错仓库 (GPU 隔离了但
    # cwd/指纹没隔离)。显式声明 cwd/{ROOT} 仍可覆盖。
    batch_cwd = resolve_template(
        spec.get("cwd", "{PROJECT:" + str(project) + "}"), cfg
    )
    batch_cwd_abs = os.path.realpath(
        os.path.expanduser(batch_cwd)
    )

    tasks = spec.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise SchemaError("tasks 必须是至少一个任务的对象数组")

    # B14 L4: sweep.matrix 同质任务矩阵展开。
    #   sweep: {matrix: {参数名: [取值...]}, max_parallel: N}
    #   任务 cmd/cwd/env值/artifacts路径/id 中 {参数名} 占位符按笛卡尔积逐组合
    #   替换; id 未含占位符时自动追加 "_v1_v2" 后缀。max_parallel 由 dispatcher
    #   按 batch 内 running 数限制并发 (存入每个任务 spec)。
    sweep_spec = spec.get("sweep")
    if sweep_spec is not None:
        tasks, batch_max_parallel = _expand_sweep(tasks, sweep_spec)
    else:
        batch_max_parallel = None

    norm_tasks = []
    seen_ids: set[str] = set()
    force_rerun = spec.get("force_rerun", False)
    if not isinstance(force_rerun, bool):
        raise SchemaError("force_rerun: 必须是布尔")
    for i, t in enumerate(tasks):
        nt = _validate_task(t, cfg, batch_cwd_abs, f"tasks[{i}]", seen_ids)
        if batch_max_parallel is not None:
            nt["max_parallel"] = batch_max_parallel
        if force_rerun:
            nt["_force_rerun"] = True   # B13-§4: 强制重跑 (跳过 SKIP 判定)
        # B15: runtime 三通道解析 (提交期即校验存在性)
        rt = t.get("runtime")
        if rt is not None:
            from .config import ConfigError as _CE, resolve_runtime as _rr

            try:
                prefix = _rr(rt, cfg)
            except _CE as e:
                raise SchemaError(f"tasks[{i}].runtime: {e}")
            nt["runtime"] = rt
            nt["runtime_prefix"] = prefix
        # B13-§5: 进度正则 (daemon 周期从日志尾部提取, status 可视化)
        prx = t.get("progress_regex")
        if prx is not None:
            if not isinstance(prx, str) or not prx.strip():
                raise SchemaError(f"tasks[{i}].progress_regex: 必须是非空字符串")
            nt["progress_regex"] = prx
        norm_tasks.append(nt)

    project = spec.get("project")
    if project is not None and not isinstance(project, str):
        raise SchemaError("project: 必须是字符串")

    batch_priority = spec.get("priority", 0)
    if not isinstance(batch_priority, int) or isinstance(batch_priority, bool):
        raise SchemaError("priority: 必须是整数 (批次级优先级, 数值大者先派)")

    return {
        "name": name,
        "mode": mode,
        "depends_on": depends_on,
        "gpus": gpus,
        "cwd": spec.get("cwd", "{ROOT}"),
        "cwd_abs": batch_cwd_abs,
        "env": env,
        "notify": bnotify,
        "tasks": norm_tasks,
        "project": project,
        "priority": batch_priority,
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
        _check_sudo_tokens([str(c) for c in cmd], f"{where}.cmd", cfg)
        # B15: I 类规则放宽 —— cmd[0] 自由格式。环境声明走可选 runtime 字段
        # (三通道) 或保留 {VENV:x} 语法糖；两者皆无 -> 放行, 由调用方打印警告
        # 并在指纹中省略环境分量 (git rev 仍锚定代码版本)。定案 Q1。
        stage_specs = None

    # 产物路径 E4 校验
    artifacts = t.get("artifacts", {})
    if not isinstance(artifacts, dict):
        raise SchemaError(f"{where}.artifacts: 必须是对象")
    for key, a in artifacts.items():
        if not isinstance(a, dict) or not a.get("path"):
            raise SchemaError(f"{where}.artifacts.{key}: 缺 path")
        # M14: min_bytes 未校验会在 reap 路径 int() ValueError 炸整轮
        mb = a.get("min_bytes")
        if mb is not None and (
            not isinstance(mb, int) or isinstance(mb, bool) or mb < 0
        ):
            raise SchemaError(f"{where}.artifacts.{key}.min_bytes: 必须是非负整数")
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
    # M14: bool 穿透 —— JSON true/false == 1/0, 与 cpus/vram 的排除保持一致
    if isinstance(gpu_req, bool) or gpu_req not in (0, 1):
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
    if duration_min is not None and (
        not isinstance(duration_min, (int, float)) or isinstance(duration_min, bool)
    ):
        raise SchemaError(f"{where}.duration_min: 必须是数字 (分钟)")
    # M14: max_retry 原先完全未校验 —— 字符串/负数会在 dispatcher 比较处
    # TypeError 炸掉整轮 reap
    max_retry = t.get("max_retry", 1)
    if not isinstance(max_retry, int) or isinstance(max_retry, bool) or max_retry < 0:
        raise SchemaError(f"{where}.max_retry: 必须是非负整数")

    project = t.get("project")
    if project is not None and not isinstance(project, str):
        raise SchemaError(f"{where}.project: 必须是字符串")

    return {
        "id": tid,
        "cmd": cmd,
        "stages": stage_specs,
        "cwd_abs": t_cwd_abs,
        "git": t.get("git"),  # None=自动探测
        "env": t.get("env", {}),
        "resources": resources,
        "duration_min": duration_min,
        "max_retry": max_retry,
        "artifacts": artifacts,
        "retry_transform": retry_transform,
        "probes": probes,
        "paths_escape": t.get("paths_escape", False),
        "project": project,
    }


def _validate_stage(s: Any, cfg: dict, t_cwd_abs: str, where: str) -> dict:
    if not isinstance(s, dict):
        raise SchemaError(f"{where}: stage 必须是对象")
    cmd = s.get("cmd")
    if not isinstance(cmd, list) or not cmd:
        raise SchemaError(f"{where}: 缺 cmd 数组")
    _check_sudo_tokens([str(c) for c in cmd], f"{where}.cmd", cfg)
    # B15: stage 级同样放开 cmd[0] (runtime 为任务级声明, stages 继承)

    artifacts = s.get("artifacts", {})
    if not isinstance(artifacts, dict):
        raise SchemaError(f"{where}.artifacts: 必须是对象")
    for key, a in artifacts.items():
        if not isinstance(a, dict) or not a.get("path"):
            raise SchemaError(f"{where}.artifacts.{key}: 缺 path")
        mb = a.get("min_bytes")
        if mb is not None and (
            not isinstance(mb, int) or isinstance(mb, bool) or mb < 0
        ):
            raise SchemaError(f"{where}.artifacts.{key}.min_bytes: 必须是非负整数")
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


def _expand_sweep(tasks: list[dict], sweep: Any) -> tuple[list[dict], int | None]:
    """B14 L4: 按笛卡尔积展开矩阵组合, 返回 (展开后任务列表, max_parallel)."""
    import copy
    import itertools

    if not isinstance(sweep, dict):
        raise SchemaError("sweep: 必须是对象")
    matrix = sweep.get("matrix")
    if not isinstance(matrix, dict) or not matrix:
        raise SchemaError('sweep.matrix: 必须是非空对象 (如 {"seed": [42, 2024]})')

    keys = sorted(matrix.keys())
    val_sets: list[list[str]] = []
    for k in keys:
        vs = matrix[k]
        if not isinstance(vs, list) or not vs:
            raise SchemaError(f"sweep.matrix.{k}: 必须是非空数组")
        val_sets.append([str(v) for v in vs])

    mp = sweep.get("max_parallel")
    if mp is not None and (
        not isinstance(mp, int) or isinstance(mp, bool) or mp < 1
    ):
        raise SchemaError("sweep.max_parallel: 必须是正整数")

    def _sub(obj: Any, subs: dict[str, str]) -> Any:
        """递归字符串替换 (cmd 数组/env 对象/artifacts 路径等任意嵌套结构)."""
        if isinstance(obj, str):
            for k, v in subs.items():
                obj = obj.replace("{" + k + "}", v)
            return obj
        if isinstance(obj, list):
            return [_sub(x, subs) for x in obj]
        if isinstance(obj, dict):
            return {k: _sub(v, subs) for k, v in obj.items()}
        return obj

    out: list[dict] = []
    for combo in itertools.product(*val_sets):
        subs = dict(zip(keys, combo))
        suffix = "_" + "_".join(combo)
        for t in tasks:
            tc = copy.deepcopy(t)
            tid = str(tc.get("id", ""))
            if "{" in tid:
                for k, v in subs.items():
                    tid = tid.replace("{" + k + "}", v)
                if "{" in tid or "}" in tid:
                    raise SchemaError(
                        f"task id '{tid}' 含未在 sweep.matrix 中定义的占位符"
                    )
            else:
                tid = tid + suffix
            tc["id"] = tid
            out.append(_sub(tc, subs))
    return out, mp


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
    _check_sudo_tokens(tokens, where, shell_payload=True)
    return tokens
