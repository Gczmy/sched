"""batch.json 校验 (文档 §4.1 + B10 E4 + B13 H1 + §3.4e 依赖环检测).

校验失败报 SchemaError (明确错误, 不污染状态).
"""

from __future__ import annotations

import json
import math
import os
import re
import shlex
from typing import Any

from .config import ConfigError, expand_path, resolve_template, project_gpu_enabled
from .native_exec import (
    NATIVE_EXEC_ALL_INTERNAL_FIELDS,
    NATIVE_EXEC_PROFILE_V2_SCHEMA,
    NATIVE_EXEC_V2_CONTRACT_FIELD,
    NATIVE_EXEC_V2_ROOT_KEYS,
    NATIVE_EXEC_V2_TASK_KEYS,
    NativeExecProfileError,
    native_exec_profile_schema_for_batch,
    native_exec_project_root_identity_sha256,
    native_exec_reserved_batch_names,
    resolve_native_exec_profile,
)

SUDO_TOKENS = {"sudo", "su", "runuser"}
MAX_NESTED_SHELL_STATES = 1024
SHELL_TOKENS = {"sh", "bash", "dash", "zsh", "ksh", "fish"}


class SchemaError(Exception):
    pass

MAX_IDENTIFIER_LENGTH = 128
MAX_PATTERN_LENGTH = 4096
MAX_PATH_LENGTH = 4096
MAX_ARTIFACTS_PER_GROUP = 64
MAX_ARTIFACT_RULE_BYTES = 64 * 1024
MAX_NORMALIZED_TASKS = 10_000
ARTIFACT_RULE_KEYS = frozenset({"path", "min_bytes", "check", "has_key", "regex"})
SAFE_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
SAFE_ENV_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
DANGEROUS_ENV_NAMES = {
    "BASH_ENV",
    "ENV",
    "LD_PRELOAD",
    "LD_AUDIT",
    "LD_LIBRARY_PATH",
    "DYLD_INSERT_LIBRARIES",
    "DYLD_LIBRARY_PATH",
    "DYLD_FRAMEWORK_PATH",
}


def _check_normalized_task_count(count: int) -> None:
    if count > MAX_NORMALIZED_TASKS:
        raise SchemaError(
            f"tasks: 规范化后任务数 {count} 超过硬上限 {MAX_NORMALIZED_TASKS}"
        )


def _validate_identifier(value: Any, where: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) > MAX_IDENTIFIER_LENGTH
        or value in {".", ".."}
        or SAFE_IDENTIFIER_RE.fullmatch(value) is None
    ):
        raise SchemaError(
            f"{where}: 必须是 1..{MAX_IDENTIFIER_LENGTH} 位 ASCII 安全标识符"
            " ([A-Za-z0-9][A-Za-z0-9._-]*)"
        )
    return value


def _validate_env(value: Any, where: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise SchemaError(f"{where}: 必须是字符串键值对象")
    for key, item in value.items():
        if (
            not isinstance(key, str)
            or SAFE_ENV_NAME_RE.fullmatch(key) is None
            or key in DANGEROUS_ENV_NAMES
            or key.startswith(("LD_", "DYLD_"))
        ):
            raise SchemaError(f"{where}: 包含非法或危险的环境变量名")
        if not isinstance(item, str) or "\0" in item:
            raise SchemaError(f"{where}.{key}: 必须是无 NUL 字符串")
    return value


def _validate_command(value: Any, where: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise SchemaError(f"{where}: 必须是非空字符串数组")
    for index, token in enumerate(value):
        if not isinstance(token, str) or not token or "\0" in token:
            raise SchemaError(f"{where}[{index}]: 必须是非空无 NUL 字符串")
    return value


def _is_finite_positive_number(value: Any) -> bool:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    try:
        return value > 0 and math.isfinite(value)
    except (OverflowError, TypeError, ValueError):
        return False


def _validate_regex(value: Any, where: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\0" in value
        or len(value) > MAX_PATTERN_LENGTH
    ):
        raise SchemaError(
            f"{where}: 必须是 1..{MAX_PATTERN_LENGTH} 位正则字符串"
        )
    try:
        re.compile(value)
    except (re.error, OverflowError, RecursionError) as exc:
        raise SchemaError(f"{where}: 无效正则: {exc}") from exc
    return value


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
SHELL_CONTROL_WORDS = {"if", "then", "else", "elif", "while", "until", "for", "do", "case", "coproc"}


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
            if re.match(r"^[A-Za-z_][A-Za-z0-9_]*(?:\[[^]]+\])?\+?=", token):
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
    return bool(re.search(r"[;|&({\n]$", tok))


def _token_has_sudo(tok: str) -> bool:
    """Recognize privilege helpers embedded in shell punctuation/code."""
    candidates = re.findall(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", str(tok))
    return any(os.path.basename(candidate).lower() in SUDO_TOKENS for candidate in candidates)

def _reject_shell_source(command: str, where: str) -> None:
    text = str(command)
    source_token = (
        r"(?:(?<![\w.])source(?!\w)"
        r"|(?<![\w.])\.(?=$|[\s;|&`(){}<>]))"
    )
    if re.search(
        rf"(?:^|[;\n|&(){{}}])\s*{source_token}",
        text,
    ):
        raise SchemaError(f"{where}: source 动态脚本执行被拒绝")
    if re.search(
        rf"(?:\$\(|`)[^`)]*{source_token}",
        text,
    ):
        raise SchemaError(f"{where}: source 命令替换被拒绝")
    if re.search(
        rf"\b(?:if|then|else|elif|!|command|builtin|time|coproc)\s+"
        rf"(?:[A-Za-z_][A-Za-z0-9_]*=\S+\s+)*{source_token}",
        text,
    ):
        raise SchemaError(f"{where}: source 控制前缀被拒绝")
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
        if is_shell_payload and re.search(
            r"(?:^|[;\n|&`(){}])\s*(?:source|\.)"
            r"(?=$|[\s;|&`(){}])",
            " ".join(str(token) for token in expanded),
        ):
            raise SchemaError(
                f"{current_where}: source 动态脚本执行被拒绝"
            )
        command_position = True
        control_pending = False
        for idx, tok in enumerate(expanded):
            base = os.path.basename(tok)
            effective_command_position = (
                command_position or (is_shell_payload and control_pending)
            )
            if is_shell_payload and re.search(r"[$`]", tok):
                if effective_command_position or re.search(r"(?i)(?:sudo|runuser|\bsu\b)", tok):
                    raise SchemaError(
                        f"{current_where}: 动态命令展开被拒绝 ('{tok}') —— "
                        "无法安全确认特权命令"
                    )
            if effective_command_position and base == "eval":
                raise SchemaError(
                    f"{current_where}: eval 动态执行被拒绝 —— 无法安全检查特权命令"
                )
            if (
                (effective_command_position and base in SUDO_TOKENS)
                or (is_shell_payload and _token_has_sudo(tok))
            ):
                raise SchemaError(
                    f"{current_where}: 含特权命令 '{tok}' —— 框架无 sudo 硬约束 (H1),"
                    " 请重新设计为无 sudo 方案"
                )
            if effective_command_position and base == "exec":
                raise SchemaError(
                    f"{current_where}: exec 会绕过退出码封装, 拒绝命令"
                )
            if effective_command_position and base in SCRIPT_INTERPRETERS:
                for option_idx in range(idx + 1, len(expanded)):
                    option = expanded[option_idx]
                    script = None
                    payload_options = ("-c", "--command", "-e", "--eval", "-r")
                    if base == "perl":
                        payload_options += ("-E", "--execute")
                    if base in {"node", "nodejs"}:
                        payload_options += ("-p", "--print")
                    if base == "php":
                        payload_options += (
                            "-R", "--process-code", "-B", "--process-begin",
                            "-E", "--process-end",
                        )
                    if option in payload_options:
                        script = _shell_command_arg(expanded, option_idx)
                    elif option.startswith(("--command=", "--eval=", "--execute=", "--print=", "--process-code=", "--process-begin=", "--process-end=")):
                        script = option.partition("=")[2]
                    elif base in {"node", "nodejs"} and option.startswith("-p="):
                        script = option.partition("=")[2]
                    elif base in {"node", "nodejs"} and option.startswith("-p") and len(option) > 2:
                        script = _shell_command_arg(expanded, option_idx)
                    elif len(option) > 2 and option[:2] in ("-c", "-e", "-r", "-E", "-p", "-R", "-B"):
                        script = option[2:]
                    if script is None:
                        continue
                    _reject_shell_source(script, current_where)
                    pending.append(
                        ([script], f"{current_where} interpreter payload", True)
                    )
            if effective_command_position and base in COMMAND_LAUNCHERS:
                for launcher_arg in _launcher_argument_tokens(expanded, idx):
                    if (
                        os.path.basename(launcher_arg) in SUDO_TOKENS
                        or os.path.basename(launcher_arg) in {"source", ".", "eval", "exec"}
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
                    _reject_shell_source(command, current_where)
                    try:
                        nested = shlex.split(command)
                    except ValueError as e:
                        raise SchemaError(
                            f"{current_where}: launcher shell 解析失败: {e}"
                        ) from e
                    pending.append((nested, f"{current_where} command launcher", True))
            if effective_command_position and base in SHELL_WRAPPERS:
                nested = _wrapper_command_tokens(expanded, idx)
                if nested:
                    pending.append((nested, f"{current_where} command wrapper", is_shell_payload))
            if (
                effective_command_position
                or (
                    is_shell_payload
                    and (control_pending or any(
                        token in SHELL_CONTROL_WORDS for token in expanded[:idx]
                    ))
                )
            ) and base in {"source", "."}:
                raise SchemaError(
                    f"{current_where}: source 动态脚本执行被拒绝"
                )
            if effective_command_position and base in SHELL_TOKENS:
                nested_commands = _nested_shell_commands(expanded, idx)
                if not nested_commands:
                    raise SchemaError(
                        f"{current_where}: 未提供可检查的嵌套 shell 命令"
                    )
                for command in nested_commands:
                    _reject_shell_source(command, current_where)
                    try:
                        nested = shlex.split(command)
                    except ValueError as e:
                        raise SchemaError(
                            f"{current_where}: nested shell 解析失败: {e}"
                        ) from e
                    pending.append((nested, f"{current_where} nested shell", True))
            control_pending = bool(
                is_shell_payload
                and (effective_command_position or control_pending)
                and (tok in SHELL_CONTROL_WORDS or tok == "!")
            )
            if effective_command_position and re.match(r"^[A-Za-z_][A-Za-z0-9_]*(?:\[[^]]+\])?\+?=", tok):
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


def _validate_artifacts(
    artifacts: Any,
    cfg: dict,
    cwd_abs: str,
    where: str,
    *,
    paths_escape: bool,
) -> dict[str, dict[str, Any]]:
    if not isinstance(artifacts, dict):
        raise SchemaError(f"{where}: 必须是对象")
    if len(artifacts) > MAX_ARTIFACTS_PER_GROUP:
        raise SchemaError(
            f"{where}: 最多声明 {MAX_ARTIFACTS_PER_GROUP} 个产物"
        )
    for key, rule in artifacts.items():
        rule_where = f"{where}.{key}"
        if not isinstance(key, str) or not isinstance(rule, dict):
            raise SchemaError(f"{rule_where}: 必须是对象")
        _validate_identifier(key, f"{rule_where} 名称")
        unknown_keys = set(rule) - ARTIFACT_RULE_KEYS
        if unknown_keys:
            raise SchemaError(
                f"{rule_where}: 未知规则 "
                f"{sorted((repr(key) for key in unknown_keys))!r}"
            )
        path = rule.get("path")
        if (
            not isinstance(path, str)
            or not path
            or "\0" in path
            or len(path) > MAX_PATH_LENGTH
        ):
            raise SchemaError(
                f"{rule_where}.path: 必须是 1..{MAX_PATH_LENGTH} 位有效路径字符串"
            )
        min_bytes = rule.get("min_bytes")
        if min_bytes is not None and (
            not isinstance(min_bytes, int)
            or isinstance(min_bytes, bool)
            or min_bytes < 0
        ):
            raise SchemaError(f"{rule_where}.min_bytes: 必须是非负整数")
        check = rule.get("check")
        if check is not None and check != "json":
            raise SchemaError(f"{rule_where}.check: 仅支持 'json'")
        has_key = rule.get("has_key")
        if has_key is not None and (
            not isinstance(has_key, str)
            or not has_key
            or "\0" in has_key
            or len(has_key) > MAX_PATTERN_LENGTH
        ):
            raise SchemaError(f"{rule_where}.has_key: 必须是非空字符串")
        regex = rule.get("regex")
        if regex is not None:
            _validate_regex(regex, f"{rule_where}.regex")
        if not paths_escape:
            expanded_path = expand_path(path, cfg, cwd_abs)
            _check_path_in_cwd(expanded_path, cwd_abs, rule_where)
    try:
        encoded_size = len(
            json.dumps(
                artifacts,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise SchemaError(f"{where}: 无法序列化产物规则: {exc}") from exc
    if encoded_size > MAX_ARTIFACT_RULE_BYTES:
        raise SchemaError(
            f"{where}: 规则总大小超过 {MAX_ARTIFACT_RULE_BYTES} bytes"
        )
    return artifacts


def validate_project_gpu_access(cfg: dict, project: str | None, tasks) -> None:
    """Validate admission without changing queued or running task state."""
    for task in tasks:
        resources = task.get("resources") or {}
        if not isinstance(resources, dict):
            raise SchemaError("resources 必须是对象")
        gpu = resources.get("gpu", 1)
        if isinstance(gpu, bool) or not isinstance(gpu, int) or gpu not in (0, 1):
            raise SchemaError("resources.gpu 必须是 0 或 1")
        if gpu and not project_gpu_enabled(cfg, project):
            raise SchemaError(
                f"project '{project}' 禁止 GPU (gpu_enabled=false 或项目未注册):"
                f" 任务 '{task.get('id', '?')}' 申请了 GPU；CPU-only 任务仍允许"
            )


def validate_batch(spec: dict, cfg: dict, *, check_gpu_access: bool = True) -> dict:
    """校验整个 batch.json, 返回规范化后的 spec (模板已展开, cwd 已归一化).

    通过后调用方持 spec 入队 (insert_batch + insert_task + insert_job).
    """
    if not isinstance(spec, dict):
        raise SchemaError("batch.json 顶层必须是 JSON 对象")

    supplied_native_fields = sorted(NATIVE_EXEC_ALL_INTERNAL_FIELDS.intersection(spec))
    if supplied_native_fields:
        raise SchemaError(
            "batch.json 不得提供 scheduler 内部 native-exec 字段: "
            f"{supplied_native_fields}"
        )

    name = _validate_identifier(spec.get("name"), "name")

    mode = spec.get("mode", "mix")
    if mode not in ("mix", "strict"):
        raise SchemaError(f"mode 仅支持 mix 或冷配置精确授权的 strict (实际 {mode})")
    try:
        reserved_native_names = native_exec_reserved_batch_names(cfg)
        strict_native_profile_schema = native_exec_profile_schema_for_batch(cfg, name)
    except NativeExecProfileError as exc:
        raise SchemaError(f"native_exec_profiles 配置非法: {exc}") from exc
    if name in reserved_native_names and mode != "strict":
        raise SchemaError(
            f"批次名 '{name}' 已由 admin native_exec_profile 保留，"
            "只能通过 exact mode=strict 提交"
        )
    if mode == "strict" and strict_native_profile_schema == NATIVE_EXEC_PROFILE_V2_SCHEMA:
        actual_root_keys = frozenset(spec)
        if actual_root_keys != NATIVE_EXEC_V2_ROOT_KEYS:
            raise SchemaError(
                "mode=strict V2 batch keys 必须精确匹配；"
                f"missing={sorted(NATIVE_EXEC_V2_ROOT_KEYS - actual_root_keys, key=repr)}, "
                f"extra={sorted(actual_root_keys - NATIVE_EXEC_V2_ROOT_KEYS, key=repr)}"
            )

    depends_on = spec.get("depends_on", [])
    if not isinstance(depends_on, list):
        raise SchemaError("depends_on 必须是 [batch_name] 字符串数组")
    for index, dependency in enumerate(depends_on):
        _validate_identifier(dependency, f"depends_on[{index}]")

    if "gpus" in spec:
        raise SchemaError("gpus: 批次级选卡未实现, 请使用 task.resources")

    env = _validate_env(spec.get("env", {}), "env")
    if mode == "strict" and env and strict_native_profile_schema != NATIVE_EXEC_PROFILE_V2_SCHEMA:
        raise SchemaError(
            "mode=strict 不允许 batch env；原生执行环境只由 scheduler 控制字段构造"
        )

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
    # ({PROJECT:<project>})，而非 {ROOT}(=default_project)，避免未声明 cwd
    # 的批次在另一个项目根目录运行并使用错误的代码指纹。
    # 显式声明 cwd/{ROOT} 仍可覆盖。
    batch_cwd = spec.get("cwd", "{PROJECT:" + str(project) + "}")
    if (
        not isinstance(batch_cwd, str)
        or not batch_cwd
        or "\0" in batch_cwd
        or len(batch_cwd) > MAX_PATH_LENGTH
    ):
        raise SchemaError("cwd: 必须是有效且有界的路径字符串")
    batch_cwd = resolve_template(batch_cwd, cfg)
    batch_cwd_abs = os.path.realpath(os.path.expanduser(batch_cwd))

    tasks = spec.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise SchemaError("tasks 必须是至少一个任务的对象数组")
    if mode == "strict":
        if len(tasks) != 1:
            raise SchemaError("mode=strict 仅允许一个精确授权的 cmd 任务")
        if "sweep" in spec:
            raise SchemaError("mode=strict 不允许 sweep 展开")
        sole_task = tasks[0]
        if not isinstance(sole_task, dict) or "stages" in sole_task:
            raise SchemaError("mode=strict 仅允许单一 cmd，不能声明 stages")
        if strict_native_profile_schema == NATIVE_EXEC_PROFILE_V2_SCHEMA:
            actual_task_keys = frozenset(sole_task)
            if actual_task_keys != NATIVE_EXEC_V2_TASK_KEYS:
                raise SchemaError(
                    "mode=strict V2 task keys 必须精确匹配；"
                    f"missing={sorted(NATIVE_EXEC_V2_TASK_KEYS - actual_task_keys, key=repr)}, "
                    f"extra={sorted(actual_task_keys - NATIVE_EXEC_V2_TASK_KEYS, key=repr)}"
                )

    # B14 L4: sweep.matrix 同质任务矩阵展开。
    #   sweep: {matrix: {参数名: [取值...]}, max_parallel: N}
    #   任务 cmd/cwd/env值/artifacts路径/id 中 {参数名} 占位符按笛卡尔积逐组合
    #   替换; id 未含占位符时自动追加 "_v1_v2" 后缀。max_parallel 由 dispatcher
    #   按 batch 内 running 数限制并发 (存入每个任务 spec)。
    sweep_spec = spec.get("sweep")
    if sweep_spec is not None:
        tasks, batch_max_parallel = _expand_sweep(tasks, sweep_spec)
    else:
        _check_normalized_task_count(len(tasks))
        batch_max_parallel = None

    norm_tasks = []
    seen_ids: set[str] = set()
    strict_project_root = None
    strict_project_root_identity_sha256 = None
    if mode == "strict":
        strict_project_root = os.path.realpath(
            os.path.expanduser(resolve_template(f"{{PROJECT:{project}}}", cfg))
        )
        try:
            strict_project_root_identity_sha256 = (
                native_exec_project_root_identity_sha256(
                    strict_project_root
                )
            )
        except NativeExecProfileError as exc:
            raise SchemaError(str(exc)) from exc
    force_rerun = spec.get("force_rerun", False)
    if not isinstance(force_rerun, bool):
        raise SchemaError("force_rerun: 必须是布尔")
    for i, t in enumerate(tasks):
        nt = _validate_task(t, cfg, batch_cwd_abs, f"tasks[{i}]", seen_ids)
        if mode == "strict":
            from .templates import expand_cmd

            assert nt["cmd"] is not None
            assert strict_project_root is not None
            if nt["cwd_abs"] != strict_project_root:
                raise SchemaError(
                    "mode=strict 的 effective cwd 必须等于当前 config project root"
                )
            is_native_v2 = strict_native_profile_schema == NATIVE_EXEC_PROFILE_V2_SCHEMA
            if "runtime" in t and not is_native_v2:
                raise SchemaError(
                    "mode=strict 不允许另行声明 runtime；执行环境必须由精确 argv 决定"
                )
            if not is_native_v2 and ("git" not in t or t["git"] is not False):
                raise SchemaError(
                    "mode=strict 必须显式声明 git=false，禁止 native verifier "
                    "之前执行 PATH 解析的 git 指纹子进程"
                )
            if nt["env"]:
                raise SchemaError(
                    "mode=strict 不允许 task env；原生执行环境只由 scheduler 控制字段构造"
                )
            if nt["resources"] != {
                "gpu": 0,
                "cpus": 1,
                "gpu_share": False,
            }:
                raise SchemaError(
                    "mode=strict 当前只允许精确 CPU-only resources: "
                    "gpu=0, cpus=1, gpu_share=false"
                )
            if nt["artifacts"] or nt["paths_escape"]:
                raise SchemaError(
                    "mode=strict 不允许 scheduler artifact skip/cleanup 规则"
                )
            if nt["probes"] is not None:
                raise SchemaError(
                    "mode=strict 不允许 scheduler 日志 probe 改写任务终态"
                )
            if nt["max_retry"] != 0:
                raise SchemaError(
                    "mode=strict 必须显式声明 max_retry=0，不能自动重放原生执行"
                )
            effective_argv = expand_cmd(nt["cmd"], cfg, None, nt["cwd_abs"])
            if effective_argv != nt["cmd"]:
                raise SchemaError(
                    "mode=strict 的 submitted argv 必须已是最终 argv，不能依赖模板展开"
                )
            batch_contract = None
            if is_native_v2:
                batch_contract = {
                    "cwd": spec["cwd"],
                    "depends_on": spec["depends_on"],
                    "_protocol": spec["_protocol"],
                    "batch_env": spec["env"],
                    "task_env": t["env"],
                    "runtime": t["runtime"],
                    "duration_min": t["duration_min"],
                    "max_retry": t["max_retry"],
                    "resources": t["resources"],
                    "artifacts": t["artifacts"],
                }
            try:
                native_profile = resolve_native_exec_profile(
                    cfg,
                    mode=mode,
                    project=project,
                    batch_name=name,
                    task_id=nt["id"],
                    submitted_argv=nt["cmd"],
                    batch_contract=batch_contract,
                )
            except NativeExecProfileError as exc:
                raise SchemaError(f"native_exec_profiles 配置非法: {exc}") from exc
            if native_profile is None:
                raise SchemaError(
                    "mode=strict 未精确匹配 admin cold native_exec_profile"
                )
            nt["_native_exec_profile_id"] = native_profile["profile_id"]
            nt["_native_exec_profile_sha256"] = native_profile[
                "profile_sha256"
            ]
            assert strict_project_root_identity_sha256 is not None
            nt["_native_exec_project_root_identity_sha256"] = (
                strict_project_root_identity_sha256
            )
            nt["_native_exec_submitted_argv"] = list(
                native_profile["submitted_argv"]
            )
            if is_native_v2:
                nt["git"] = False
                nt[NATIVE_EXEC_V2_CONTRACT_FIELD] = native_profile["contract"]
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
            nt["progress_regex"] = _validate_regex(
                prx, f"tasks[{i}].progress_regex"
            )
        norm_tasks.append(nt)

    if check_gpu_access:
        validate_project_gpu_access(cfg, project, norm_tasks)

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

    supplied_native_fields = sorted(NATIVE_EXEC_ALL_INTERNAL_FIELDS.intersection(t))
    if supplied_native_fields:
        raise SchemaError(
            f"{where}: 不得提供 scheduler 内部 native-exec 字段: "
            f"{supplied_native_fields}"
        )

    tid = _validate_identifier(t.get("id"), f"{where}.id")
    if tid in seen_ids:
        raise SchemaError(f"{where}: 重复任务 id '{tid}'")
    seen_ids.add(tid)

    # cwd: 任务级覆盖 (任意目录, J 类)
    t_cwd_abs = batch_cwd_abs
    if "cwd" in t:
        task_cwd = t["cwd"]
        if (
            not isinstance(task_cwd, str)
            or not task_cwd
            or "\0" in task_cwd
            or len(task_cwd) > MAX_PATH_LENGTH
        ):
            raise SchemaError(f"{where}.cwd: 必须是有效且有界的路径字符串")
        t_cwd_abs = os.path.realpath(
            os.path.expanduser(resolve_template(task_cwd, cfg))
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
        cmd = _validate_command(t.get("cmd"), f"{where}.cmd")
        _check_sudo_tokens(cmd, f"{where}.cmd", cfg)
        # B15: I 类规则放宽 —— cmd[0] 自由格式。环境声明走可选 runtime 字段
        # (三通道) 或保留 {VENV:x} 语法糖；两者皆无 -> 放行, 由调用方打印警告
        # 并在指纹中省略环境分量 (git rev 仍锚定代码版本)。定案 Q1。
        stage_specs = None

    paths_escape = t.get("paths_escape", False)
    if not isinstance(paths_escape, bool):
        raise SchemaError(f"{where}.paths_escape: 必须是布尔")
    artifacts = _validate_artifacts(
        t.get("artifacts", {}),
        cfg,
        t_cwd_abs,
        f"{where}.artifacts",
        paths_escape=paths_escape,
    )

    # resources: {cpus: N, gpu: 0|1} —— gpu: 0 = CPU-only 任务 (不占 GPU 槽位, §5b B4)
    resources = t.get("resources", {})
    if not isinstance(resources, dict):
        raise SchemaError(f"{where}.resources: 必须是对象")
    cpus = resources.get("cpus")
    if "host_mem_gib" in resources and not _is_finite_positive_number(resources["host_mem_gib"]):
        raise SchemaError(f"{where}.resources.host_mem_gib: 必须是有限正数 (GiB 主机内存预留)")
    if cpus is not None:
        if not isinstance(cpus, int) or isinstance(cpus, bool) or cpus < 1:
            raise SchemaError(f"{where}.resources.cpus: 必须是正整数 (声明 CPU 配额)")
    gpu_req = resources.get("gpu", 1)  # 缺省 gpu=1 (向后兼容: 每卡一任务)
    # M14: bool 穿透 —— JSON true/false == 1/0, 与 cpus/vram 的排除保持一致
    if (
        not isinstance(gpu_req, int)
        or isinstance(gpu_req, bool)
        or gpu_req not in (0, 1)
    ):
        raise SchemaError(f"{where}.resources.gpu: 必须是 0 (CPU-only) 或 1 (占 1 GPU)")
    resources = dict(resources)
    resources["gpu"] = gpu_req
    if gpu_req == 0 and "cpus" not in resources:
        resources["cpus"] = 1  # CPU-only 缺省 1 核 (配额制调度用)
    # co-location (§3.2e 待定项 2, 定案 39): gpu_share 必须声明 vram_gib (GiB,
    # 装箱必须有名数, 缺省 = 无法装箱); vram_gib 非法值拒绝.
    gpu_share = resources.get("gpu_share", False)
    if not isinstance(gpu_share, bool):
        raise SchemaError(f"{where}.resources.gpu_share: 必须是布尔")
    vram = resources.get("vram_gib")
    if gpu_share and vram is None:
        raise SchemaError(
            f"{where}.resources: gpu_share=true 必须声明 vram_gib (GiB 峰值)"
        )
    if vram is not None and not _is_finite_positive_number(vram):
        raise SchemaError(f"{where}.resources.vram_gib: 必须是有限正数 (GiB)")
    resources["gpu_share"] = gpu_share
    if vram is not None:
        resources["vram_gib"] = float(vram)
    profile_key = resources.get("profile_key")
    if profile_key is not None and (
        not isinstance(profile_key, str)
        or not profile_key
        or len(profile_key) > MAX_PATTERN_LENGTH
    ):
        raise SchemaError(f"{where}.resources.profile_key: 必须是非空字符串")

    if "retry_transform" in t:
        raise SchemaError(f"{where}.retry_transform: 未实现, 不接受该字段")
    probes = t.get("probes")
    if probes is not None:
        if not isinstance(probes, dict):
            raise SchemaError(f"{where}.probes: 必须是对象")
        unknown_probes = set(probes) - {"fail_on_log", "ready_on_log"}
        if unknown_probes:
            raise SchemaError(
                f"{where}.probes: 未知规则 {sorted(unknown_probes)!r}"
            )
        for probe_name, pattern in probes.items():
            _validate_regex(pattern, f"{where}.probes.{probe_name}")

    duration_min = t.get("duration_min")
    if duration_min is not None and not _is_finite_positive_number(duration_min):
        raise SchemaError(f"{where}.duration_min: 必须是有限正数 (分钟)")
    # M14: max_retry 原先完全未校验 —— 字符串/负数会在 dispatcher 比较处
    # TypeError 炸掉整轮 reap
    max_retry = t.get("max_retry", 1)
    if not isinstance(max_retry, int) or isinstance(max_retry, bool) or max_retry < 0:
        raise SchemaError(f"{where}.max_retry: 必须是非负整数")

    if "project" in t:
        raise SchemaError(
            f"{where}.project: 未实现任务级覆盖; 项目必须在 batch.project 声明"
        )

    task_git = t.get("git")
    if task_git is not None and not isinstance(task_git, bool):
        raise SchemaError(f"{where}.git: 必须是布尔")
    task_env = _validate_env(t.get("env", {}), f"{where}.env")

    return {
        "id": tid,
        "cmd": cmd,
        "stages": stage_specs,
        "cwd_abs": t_cwd_abs,
        "git": task_git,  # None=自动探测
        "env": task_env,
        "resources": resources,
        "duration_min": duration_min,
        "max_retry": max_retry,
        "artifacts": artifacts,
        "probes": probes,
        "paths_escape": paths_escape,
    }


def _validate_stage(s: Any, cfg: dict, t_cwd_abs: str, where: str) -> dict:
    if not isinstance(s, dict):
        raise SchemaError(f"{where}: stage 必须是对象")
    cmd = _validate_command(s.get("cmd"), f"{where}.cmd")
    _check_sudo_tokens(cmd, f"{where}.cmd", cfg)
    # B15: stage 级同样放开 cmd[0] (runtime 为任务级声明, stages 继承)

    if "retry_transform" in s:
        raise SchemaError(f"{where}.retry_transform: 未实现, 不接受该字段")
    if "probes" in s:
        raise SchemaError(f"{where}.probes: stage 级探测未实现, 不接受该字段")
    if "env" in s:
        raise SchemaError(f"{where}.env: stage 级环境变量未实现, 不接受该字段")
    paths_escape = s.get("paths_escape", False)
    if not isinstance(paths_escape, bool):
        raise SchemaError(f"{where}.paths_escape: 必须是布尔")
    artifacts = _validate_artifacts(
        s.get("artifacts", {}),
        cfg,
        t_cwd_abs,
        f"{where}.artifacts",
        paths_escape=paths_escape,
    )

    return {
        "cmd": cmd,
        "artifacts": artifacts,
        "paths_escape": paths_escape,
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

    combination_count = 1
    for values in val_sets:
        combination_count *= len(values)
    _check_normalized_task_count(len(tasks) * combination_count)

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


def validate_persisted_dependencies(
    conn: Any,
    batch_name: str,
    depends_on: list[str],
) -> None:
    """Validate one proposed latest-name dependency graph on ``conn``.

    Callers that publish a batch must run this check again while holding the
    submission gate.  The earlier schema/preview check is intentionally not a
    commit-time authority: another same-name generation may have been inserted
    while fingerprints were being computed.
    """
    _validate_identifier(batch_name, "name")
    for index, dependency in enumerate(depends_on):
        _validate_identifier(dependency, f"depends_on[{index}]")
        row = conn.execute(
            "SELECT id FROM batches WHERE name=?"
            " ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (dependency,),
        ).fetchone()
        if not row:
            raise SchemaError(
                f"depends_on 引用的批次不存在: '{dependency}' (O1)"
            )

    graph: dict[str, list[str]] = {batch_name: list(depends_on)}
    latest: dict[str, tuple[str, int, Any]] = {}
    rows = conn.execute(
        "SELECT rowid, name, depends_on, created_at FROM batches"
    ).fetchall()
    for row in rows:
        key = (row["created_at"] or "", row["rowid"])
        current = latest.get(row["name"])
        if current is not None and key <= current[:2]:
            continue
        latest[row["name"]] = (key[0], key[1], row["depends_on"])
    for name, (_created_at, _rowid, raw_dependencies) in latest.items():
        try:
            stored_dependencies = json.loads(raw_dependencies or "[]")
        except (
            json.JSONDecodeError,
            UnicodeDecodeError,
            TypeError,
            RecursionError,
        ) as error:
            raise SchemaError(
                f"批次 '{name}' 的 depends_on 状态无效"
            ) from error
        if (
            not isinstance(stored_dependencies, list)
            or any(
                not isinstance(item, str) or not item
                for item in stored_dependencies
            )
        ):
            raise SchemaError(
                f"批次 '{name}' 的 depends_on 状态无效"
            )
        try:
            for index, item in enumerate(stored_dependencies):
                _validate_identifier(item, f"depends_on[{index}]")
        except SchemaError as error:
            raise SchemaError(
                f"批次 '{name}' 的 depends_on 状态无效"
            ) from error
        graph.setdefault(name, stored_dependencies)

    # Iterative DFS avoids turning a long but valid historical dependency chain
    # into a Python recursion failure.  ``active`` also preserves a useful cycle
    # path for the existing B3 diagnostic.
    visited: set[str] = set()
    active: dict[str, int] = {}
    path: list[str] = []
    stack: list[tuple[str, int]] = [(batch_name, 0)]
    while stack:
        name, child_index = stack[-1]
        if child_index == 0 and name not in active:
            active[name] = len(path)
            path.append(name)
        children = graph.get(name, [])
        if child_index >= len(children):
            stack.pop()
            active.pop(name, None)
            if path and path[-1] == name:
                path.pop()
            visited.add(name)
            continue
        dependency = children[child_index]
        stack[-1] = (name, child_index + 1)
        if dependency in active:
            cycle = " -> ".join(path[active[dependency]:] + [dependency])
            raise SchemaError(f"依赖成环: {cycle} (B3 拒绝提交)")
        if dependency not in visited:
            stack.append((dependency, 0))


def parse_shell_cmd(shell_str: str, where: str) -> list[str]:
    """Q1: sched run 的 shell 字符串 shlex.split 后做 sudo 词法检查."""
    try:
        tokens = shlex.split(shell_str)
    except ValueError as e:
        raise SchemaError(f"{where}: shell 字符串解析失败: {e}") from e
    _check_sudo_tokens(tokens, where, shell_payload=True)
    return tokens
