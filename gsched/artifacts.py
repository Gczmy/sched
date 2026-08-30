"""产物校验 (文档 §3.4d D8).

默认校验 = 存在; 可加规则: check:json (可解析) / min_bytes: N (非空).
done 判定 = rc=0 且所有 artifacts 校验通过.
指纹 (A2) 与内容校验正交: 指纹管"代码版本匹配", 规则管"产物有效".
"""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import sys
from typing import Any


class ArtifactError(Exception):
    pass

ARTIFACT_RULE_KEYS = frozenset({"path", "min_bytes", "check", "has_key", "regex"})

def _isolated_child_env() -> dict[str, str]:
    child_env = dict(os.environ)
    for key in tuple(child_env):
        if key in {"BASH_ENV", "ENV"} or key.startswith(("LD_", "DYLD_")):
            child_env.pop(key, None)
    return child_env


def bounded_regex_last_match(
    pattern: str,
    content: bytes | str,
    *,
    timeout: float = 0.25,
    max_match_chars: int = 120,
) -> str | None:
    """Return the last match in an isolated, time-bounded interpreter."""
    if (
        not isinstance(pattern, str)
        or not pattern
        or "\0" in pattern
        or len(pattern) > 4096
        or not isinstance(max_match_chars, int)
        or max_match_chars < 0
    ):
        return None
    try:
        re.compile(pattern)
    except (re.error, OverflowError, RecursionError):
        return None
    payload = content.encode("utf-8", "replace") if isinstance(content, str) else content
    if not isinstance(payload, bytes) or len(payload) > 1024 * 1024:
        return None
    program = (
        "import json,re,sys\n"
        "last=None\n"
        "text=sys.stdin.buffer.read().decode('utf-8','replace')\n"
        "for match in re.finditer(sys.argv[1],text):\n"
        " last=match.group(0)[:int(sys.argv[2])]\n"
        "sys.stdout.write(json.dumps(last))\n"
    )
    try:
        completed = subprocess.run(
            [
                sys.executable,
                "-I",
                "-c",
                program,
                pattern,
                str(max_match_chars),
            ],
            input=payload,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
            env=_isolated_child_env(),
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0 or len(completed.stdout) > max_match_chars * 6 + 16:
        return None
    try:
        result = json.loads(completed.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        return None
    return result if isinstance(result, str) else None

def check_artifact(
    path: str,
    rule: dict[str, Any] | None,
    *,
    dir_fd: int | None = None,
) -> str | None:
    """Validate one regular-file artifact through an already-open descriptor."""
    if not isinstance(path, str) or not path or "\0" in path:
        return "路径无效"
    if rule is None:
        rule = {}
    elif not isinstance(rule, dict):
        return "规则必须是对象"
    unknown_keys = set(rule) - ARTIFACT_RULE_KEYS
    if unknown_keys:
        return "未知规则: " + ", ".join(
            sorted((repr(key) for key in unknown_keys))
        )

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags, dir_fd=dir_fd)
    except OSError as exc:
        return f"不存在或不可安全读取: {exc}"

    try:
        try:
            file_stat = os.fstat(fd)
        except OSError as exc:
            return f"读取属性失败: {exc}"
        if not stat.S_ISREG(file_stat.st_mode):
            return "不是普通文件"

        min_bytes = rule.get("min_bytes")
        if min_bytes is not None:
            if (
                not isinstance(min_bytes, int)
                or isinstance(min_bytes, bool)
                or min_bytes < 0
            ):
                return "min_bytes 必须是非负整数"
            if file_stat.st_size < min_bytes:
                return f"过小 ({file_stat.st_size} < {min_bytes} bytes)"

        needs_json = rule.get("check") == "json" or rule.get("has_key") is not None
        rx = rule.get("regex")
        if rx is not None and (
            not isinstance(rx, str)
            or not rx
            or "\0" in rx
            or len(rx) > 4096
        ):
            return "无效正则: 必须是非空无 NUL 字符串"
        if not needs_json and rx is None:
            return None

        max_content_bytes = 1024 * 1024
        if file_stat.st_size > max_content_bytes:
            return f"内容过大 ({file_stat.st_size} > {max_content_bytes} bytes)"
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            chunks: list[bytes] = []
            remaining = max_content_bytes + 1
            while remaining:
                chunk = os.read(fd, min(64 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            content = b"".join(chunks)
        except OSError as exc:
            return f"读取失败: {exc}"
        if len(content) > max_content_bytes:
            return f"内容过大 (> {max_content_bytes} bytes)"

        parsed: Any = None
        if needs_json:
            try:
                parsed = json.loads(content.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
                prefix = "has_key 前置: " if rule.get("has_key") is not None else ""
                return f"{prefix}JSON 解析失败: {exc}"

        hk = rule.get("has_key")
        if hk is not None:
            if (
                not isinstance(hk, str)
                or not hk
                or "\0" in hk
                or len(hk) > 4096
            ):
                return "has_key 必须是 1..4096 位无 NUL 字符串"
            current = parsed
            for part in hk.split("."):
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return f"缺少键: {hk}"

        if rx is not None:
            try:
                re.compile(rx)
            except (re.error, OverflowError, RecursionError) as exc:
                return f"无效正则: {exc}"
            if bounded_regex_last_match(
                rx,
                content,
                timeout=1,
                max_match_chars=0,
            ) is None:
                return f"未匹配正则: {rx}"
        return None
    finally:
        os.close(fd)


def _beneath_parts(cwd: str, path: str) -> tuple[str, list[str]] | None:
    if (
        not isinstance(cwd, str)
        or not cwd
        or "\0" in cwd
        or not isinstance(path, str)
        or not path
        or "\0" in path
    ):
        return None
    root = os.path.abspath(cwd)
    if os.path.isabs(path):
        candidate = os.path.normpath(path)
        try:
            if os.path.commonpath((root, candidate)) != root:
                return None
        except ValueError:
            return None
        relative = os.path.relpath(candidate, root)
    else:
        relative = os.path.normpath(path)
    parts = relative.split(os.sep)
    if (
        relative in ("", ".")
        or any(part in ("", ".", "..") for part in parts)
    ):
        return None
    return root, parts


def _check_artifact_beneath(
    cwd: str,
    path: str,
    rule: dict[str, Any],
) -> str | None:
    """Open every component below cwd without following intermediate links."""
    location = _beneath_parts(cwd, path)
    if location is None:
        return "路径无效或逃出任务 cwd"
    root, parts = location

    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    opened: list[int] = []
    try:
        root_fd = os.open(root, directory_flags)
        opened.append(root_fd)
        parent_fd = root_fd
        for component in parts[:-1]:
            child_fd = os.open(
                component,
                directory_flags | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            opened.append(child_fd)
            parent_fd = child_fd
        return check_artifact(parts[-1], rule, dir_fd=parent_fd)
    except OSError as exc:
        return f"不存在或不可安全读取: {exc}"
    finally:
        for opened_fd in reversed(opened):
            os.close(opened_fd)

def unlink_artifact(
    cwd: str,
    path: str,
    *,
    paths_escape: bool = False,
) -> bool:
    """Unlink one artifact without traversing symlinks below a confined cwd."""
    if not isinstance(paths_escape, bool):
        return False
    if paths_escape:
        target = path
        if not os.path.isabs(target):
            target = os.path.normpath(os.path.join(cwd, target))
        try:
            os.unlink(target)
            return True
        except OSError:
            return False

    location = _beneath_parts(cwd, path)
    if location is None:
        return False
    root, parts = location
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    opened: list[int] = []
    try:
        root_fd = os.open(root, directory_flags)
        opened.append(root_fd)
        parent_fd = root_fd
        for component in parts[:-1]:
            child_fd = os.open(
                component,
                directory_flags | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            opened.append(child_fd)
            parent_fd = child_fd
        os.unlink(parts[-1], dir_fd=parent_fd)
        return True
    except OSError:
        return False
    finally:
        for opened_fd in reversed(opened):
            os.close(opened_fd)





def check_artifacts(
    artifacts: dict[str, dict],
    cwd: str | None = None,
    *,
    paths_escape: bool = False,
) -> dict[str, str | None]:
    """Validate an artifact mapping, resolving relative paths against ``cwd``."""
    if not isinstance(artifacts, dict):
        return {"<artifacts>": "必须是对象"}
    if not isinstance(paths_escape, bool):
        return {"<paths_escape>": "必须是布尔"}
    result: dict[str, str | None] = {}
    for key, rule in artifacts.items():
        if not isinstance(key, str) or not isinstance(rule, dict):
            result[str(key)] = "规则必须是对象"
            continue
        raw_path = rule.get("path")
        if not isinstance(raw_path, str) or not raw_path:
            result[key] = "路径无效"
            continue
        if paths_escape:
            path = raw_path
            if cwd is not None and not os.path.isabs(path):
                path = os.path.normpath(os.path.join(cwd, path))
            result[key] = check_artifact(path, rule)
        elif cwd is None:
            result[key] = "缺少任务 cwd"
        else:
            result[key] = _check_artifact_beneath(cwd, raw_path, rule)
    return result


def all_pass(result: dict[str, str | None]) -> bool:
    return all(v is None for v in result.values())


def check_declared_artifacts(spec: dict[str, Any], cwd: str) -> bool:
    """Validate task-level and every stage-level declared artifact."""
    if not isinstance(spec, dict) or not isinstance(cwd, str) or not cwd:
        return False
    groups: list[tuple[Any, Any]] = [
        (spec.get("artifacts", {}), spec.get("paths_escape", False))
    ]
    stages = spec.get("stages")
    if stages is not None:
        if not isinstance(stages, list):
            return False
        for stage in stages:
            if not isinstance(stage, dict):
                return False
            groups.append(
                (
                    stage.get("artifacts", {}),
                    stage.get("paths_escape", False),
                )
            )
    for artifacts, paths_escape in groups:
        if not isinstance(paths_escape, bool):
            return False
        result = check_artifacts(
            artifacts,
            cwd,
            paths_escape=paths_escape,
        )
        if not all_pass(result):
            return False
    return True


def validate_json_artifact(path: str) -> dict | None:
    """读取 json 产物 (如 optuna best.json), 失败返回 None."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError, RecursionError):
        return None
