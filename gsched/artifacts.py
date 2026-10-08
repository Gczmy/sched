"""产物校验 (文档 §3.4d D8).

默认校验 = 存在; 可加规则: check:json (可解析) / min_bytes: N (非空).
done 判定 = rc=0 且所有 artifacts 校验通过.
指纹 (A2) 与内容校验正交: 指纹管"代码版本匹配", 规则管"产物有效".
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import time
from typing import Any


class ArtifactError(Exception):
    pass

ARTIFACT_RULE_KEYS = frozenset({"path", "min_bytes", "check", "has_key", "json_equals", "regex"})


def validate_json_equals(value: Any) -> None:
    """A bounded mapping from dotted object keys to finite JSON values."""
    if not isinstance(value, dict) or not value or len(value) > 64:
        raise ValueError("json_equals 必须是含 1..64 项的对象")
    for key in value:
        if not isinstance(key, str) or not key or "\0" in key or len(key) > 4096:
            raise ValueError("json_equals 键必须是 1..4096 位无 NUL 字符串")
    try:
        encoded = json.dumps(value, allow_nan=False, ensure_ascii=True)
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        raise ValueError("json_equals 必须是有限 JSON 值") from exc
    if len(encoded.encode("utf-8")) > 64 * 1024:
        raise ValueError("json_equals 总大小超过 65536 bytes")


def _json_equal(actual: Any, expected: Any) -> bool:
    # JSON numbers share a type, but true must never compare equal to 1.
    if isinstance(actual, bool) or isinstance(expected, bool):
        return type(actual) is type(expected) and actual == expected
    if isinstance(actual, dict) and isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(
            _json_equal(actual[key], expected[key]) for key in actual
        )
    if isinstance(actual, list) and isinstance(expected, list):
        return len(actual) == len(expected) and all(
            _json_equal(a, b) for a, b in zip(actual, expected)
        )
    return actual == expected

def _isolated_child_env() -> dict[str, str]:
    child_env = dict(os.environ)
    for key in tuple(child_env):
        if key in {"BASH_ENV", "ENV"} or key.startswith(("LD_", "DYLD_")):
            child_env.pop(key, None)
    return child_env


def bounded_regex_result(
    pattern: str,
    content: bytes | str,
    *,
    timeout: float = 0.25,
    max_match_chars: int = 120,
) -> dict[str, Any]:
    """Isolated regex evidence; no-match, timeout and child errors are distinct."""
    started = time.monotonic()

    def finish(reason: str, **extra: Any) -> dict[str, Any]:
        return {"reason_code": reason, "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
                "match": None, "returncode": None, "errno": None, "stderr": None, **extra}

    if (
        not isinstance(pattern, str)
        or not pattern
        or "\0" in pattern
        or len(pattern) > 4096
        or not isinstance(max_match_chars, int)
        or max_match_chars < 0
    ):
        return finish("invalid_regex")
    try:
        re.compile(pattern)
    except (re.error, OverflowError, RecursionError):
        return finish("invalid_regex")
    payload = content.encode("utf-8", "replace") if isinstance(content, str) else content
    if not isinstance(payload, bytes) or len(payload) > 1024 * 1024:
        return finish("content_too_large")
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
            stderr=subprocess.PIPE,
            timeout=timeout,
            env=_isolated_child_env(),
            check=False,
        )
    except subprocess.TimeoutExpired:
        return finish("regex_timeout")
    except OSError as exc:
        return finish("regex_child_start_error", errno=exc.errno)
    except subprocess.SubprocessError:
        return finish("regex_child_error")
    stderr = (completed.stderr or b"")[:2048].decode("utf-8", "replace")
    if completed.returncode != 0 or len(completed.stdout) > max_match_chars * 6 + 16:
        return finish("regex_child_error", returncode=completed.returncode, stderr=stderr)
    try:
        result = json.loads(completed.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        return finish("regex_child_output_invalid", returncode=completed.returncode, stderr=stderr)
    if result is None:
        return finish("regex_no_match", returncode=0)
    if not isinstance(result, str):
        return finish("regex_child_output_invalid", returncode=completed.returncode, stderr=stderr)
    return finish("passed", match=result, returncode=0)


def bounded_regex_last_match(
    pattern: str, content: bytes | str, *, timeout: float = 0.25,
    max_match_chars: int = 120,
) -> str | None:
    """Compatibility interface used by progress sampling."""
    return bounded_regex_result(pattern, content, timeout=timeout,
                                max_match_chars=max_match_chars)["match"]

def inspect_artifact(
    path: str,
    rule: dict[str, Any] | None,
    *,
    dir_fd: int | None = None,
) -> dict[str, Any]:
    """Validate one regular-file artifact through an already-open descriptor."""
    started = time.monotonic()
    evidence: dict[str, Any] = {"file_size": None, "sha256": None, "rule_sha256": None,
                                "errno": None, "regex": None}

    def finish(reason: str, message: str | None = None, **extra: Any) -> dict[str, Any]:
        return {**evidence, "passed": reason == "passed", "reason_code": reason,
                "message": message, "elapsed_ms": round((time.monotonic() - started) * 1000, 3), **extra}

    def io_failure(exc: OSError, prefix: str) -> dict[str, Any]:
        reason = {errno.ENOENT: "missing_file", errno.EACCES: "permission_denied",
                  errno.EPERM: "permission_denied", errno.ELOOP: "unsafe_path"}.get(exc.errno, "io_error")
        return finish(reason, f"{prefix}: {exc}", errno=exc.errno)

    if not isinstance(path, str) or not path or "\0" in path:
        return finish("invalid_path", "路径无效")
    if rule is None:
        rule = {}
    elif not isinstance(rule, dict):
        return finish("invalid_rule", "规则必须是对象")
    unknown_keys = set(rule) - ARTIFACT_RULE_KEYS
    if unknown_keys:
        return finish("invalid_rule", "未知规则: " + ", ".join(
            sorted((repr(key) for key in unknown_keys))
        ))
    try:
        evidence["rule_sha256"] = hashlib.sha256(json.dumps(
            rule, sort_keys=True, ensure_ascii=True, allow_nan=False,
            separators=(",", ":"),
        ).encode()).hexdigest()
        if "json_equals" in rule:
            validate_json_equals(rule["json_equals"])
    except (TypeError, ValueError, OverflowError, RecursionError) as exc:
        return finish("invalid_rule", str(exc))
    if rule.get("check") not in (None, "json"):
        return finish("invalid_rule", "check: 仅支持 'json'")

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags, dir_fd=dir_fd)
    except OSError as exc:
        return io_failure(exc, "不存在或不可安全读取")

    try:
        try:
            file_stat = os.fstat(fd)
        except OSError as exc:
            return io_failure(exc, "读取属性失败")
        evidence["file_size"] = file_stat.st_size
        if not stat.S_ISREG(file_stat.st_mode):
            return finish("not_regular_file", "不是普通文件")

        min_bytes = rule.get("min_bytes")
        if min_bytes is not None:
            if (
                not isinstance(min_bytes, int)
                or isinstance(min_bytes, bool)
                or min_bytes < 0
            ):
                return finish("invalid_rule", "min_bytes 必须是非负整数")
            if file_stat.st_size < min_bytes:
                return finish("too_small", f"过小 ({file_stat.st_size} < {min_bytes} bytes)")

        needs_json = rule.get("check") == "json" or rule.get("has_key") is not None or "json_equals" in rule
        rx = rule.get("regex")
        if rx is not None and (
            not isinstance(rx, str)
            or not rx
            or "\0" in rx
            or len(rx) > 4096
        ):
            return finish("invalid_regex", "无效正则: 必须是非空无 NUL 字符串")
        if not needs_json and rx is None:
            return finish("passed")

        max_content_bytes = 1024 * 1024
        if file_stat.st_size > max_content_bytes:
            return finish("content_too_large", f"内容过大 ({file_stat.st_size} > {max_content_bytes} bytes)")
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
            return io_failure(exc, "读取失败")
        if len(content) > max_content_bytes:
            return finish("content_too_large", f"内容过大 (> {max_content_bytes} bytes)")
        evidence["sha256"] = hashlib.sha256(content).hexdigest()
        after = os.fstat(fd)
        if (file_stat.st_size, file_stat.st_mtime_ns, file_stat.st_ctime_ns) != (
            after.st_size, after.st_mtime_ns, after.st_ctime_ns
        ):
            return finish("file_changed", "检查期间文件发生变化")

        parsed: Any = None
        if needs_json:
            try:
                parsed = json.loads(content.decode("utf-8"))
            except (UnicodeDecodeError, ValueError, RecursionError) as exc:
                prefix = "has_key 前置: " if rule.get("has_key") is not None else ""
                return finish("invalid_json", f"{prefix}JSON 解析失败: {exc}")

        hk = rule.get("has_key")
        if hk is not None:
            if (
                not isinstance(hk, str)
                or not hk
                or "\0" in hk
                or len(hk) > 4096
            ):
                return finish("invalid_rule", "has_key 必须是 1..4096 位无 NUL 字符串")
            current = parsed
            for part in hk.split("."):
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return finish("missing_json_key", f"缺少键: {hk}")

        for key, expected in rule.get("json_equals", {}).items():
            current = parsed
            for part in key.split("."):
                if not isinstance(current, dict) or part not in current:
                    return finish("missing_json_key", f"缺少键: {key}")
                current = current[part]
            try:
                equal = _json_equal(current, expected)
            except RecursionError:
                return finish("invalid_json", "JSON 嵌套过深")
            if not equal:
                return finish("json_value_mismatch", f"JSON 值不匹配: {key}")

        if rx is not None:
            try:
                re.compile(rx)
            except (re.error, OverflowError, RecursionError) as exc:
                return finish("invalid_regex", f"无效正则: {exc}")
            regex_result = bounded_regex_result(
                rx,
                content,
                timeout=1,
                max_match_chars=0,
            )
            evidence["regex"] = {k: v for k, v in regex_result.items() if k != "match"}
            if regex_result["reason_code"] != "passed":
                reason = regex_result["reason_code"]
                message = f"未匹配正则: {rx}" if reason == "regex_no_match" else f"正则检查失败: {reason}"
                return finish(reason, message)
        final_stat = os.fstat(fd)
        if (file_stat.st_size, file_stat.st_mtime_ns, file_stat.st_ctime_ns) != (
            final_stat.st_size, final_stat.st_mtime_ns, final_stat.st_ctime_ns
        ):
            return finish("file_changed", "检查期间文件发生变化")
        return finish("passed")
    except OSError as exc:
        return io_failure(exc, "读取属性失败")
    finally:
        os.close(fd)


def check_artifact(path: str, rule: dict[str, Any] | None, *, dir_fd: int | None = None) -> str | None:
    """Retain the legacy message-or-None contract."""
    return inspect_artifact(path, rule, dir_fd=dir_fd)["message"]


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


def _inspection_error(reason: str, message: str, error_number: int | None = None) -> dict[str, Any]:
    return {"passed": False, "reason_code": reason, "message": message,
            "elapsed_ms": 0.0, "file_size": None, "sha256": None,
            "rule_sha256": None, "errno": error_number, "regex": None}


def _inspect_artifact_beneath(
    cwd: str,
    path: str,
    rule: dict[str, Any],
) -> dict[str, Any]:
    """Open every component below cwd without following intermediate links."""
    location = _beneath_parts(cwd, path)
    if location is None:
        return _inspection_error("unsafe_path", "路径无效或逃出任务 cwd")
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
        return inspect_artifact(parts[-1], rule, dir_fd=parent_fd)
    except OSError as exc:
        reason = {errno.ENOENT: "missing_file", errno.EACCES: "permission_denied",
                  errno.EPERM: "permission_denied", errno.ELOOP: "unsafe_path",
                  errno.ENOTDIR: "unsafe_path"}.get(exc.errno, "io_error")
        return _inspection_error(reason, f"不存在或不可安全读取: {exc}", exc.errno)
    finally:
        for opened_fd in reversed(opened):
            os.close(opened_fd)


def _check_artifact_beneath(cwd: str, path: str, rule: dict[str, Any]) -> str | None:
    return _inspect_artifact_beneath(cwd, path, rule)["message"]

def unlink_artifact(
    cwd: str,
    path: str,
    *,
    paths_escape: bool = False,
    raise_on_error: bool = False,
) -> bool:
    """Unlink one artifact without traversing symlinks below a confined cwd.

    The default remains best-effort for runtime cleanup.  Strict callers may
    set ``raise_on_error``: ``False`` then means only that the target is
    already absent, while invalid paths and non-ENOENT I/O failures raise
    :class:`ArtifactError`.
    """

    def rejected(message: str, exc: BaseException | None = None) -> bool:
        if raise_on_error:
            error = ArtifactError(message)
            if exc is not None:
                raise error from exc
            raise error
        return False

    if (
        not isinstance(cwd, str)
        or not cwd
        or "\0" in cwd
        or not isinstance(path, str)
        or not path
        or "\0" in path
    ):
        return rejected("产物路径必须是非空、无 NUL 的字符串")
    if not isinstance(paths_escape, bool):
        return rejected("paths_escape 必须是布尔值")
    if paths_escape:
        target = path
        if not os.path.isabs(target):
            target = os.path.normpath(os.path.join(cwd, target))
        try:
            os.unlink(target)
            return True
        except FileNotFoundError:
            return False
        except OSError as exc:
            return rejected(f"无法删除产物 {target!r}: {exc}", exc)

    location = _beneath_parts(cwd, path)
    if location is None:
        return rejected(f"产物路径无效或逃出任务 cwd: {path!r}")
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
    except FileNotFoundError:
        return False
    except OSError as exc:
        return rejected(f"无法安全删除产物 {path!r}: {exc}", exc)
    finally:
        for opened_fd in reversed(opened):
            os.close(opened_fd)





def inspect_artifacts(
    artifacts: dict[str, dict],
    cwd: str | None = None,
    *,
    paths_escape: bool = False,
) -> dict[str, dict[str, Any]]:
    """Validate an artifact mapping, resolving relative paths against ``cwd``."""
    if not isinstance(artifacts, dict):
        return {"<artifacts>": _inspection_error("invalid_rule", "必须是对象")}
    if not isinstance(paths_escape, bool):
        return {"<paths_escape>": _inspection_error("invalid_rule", "必须是布尔")}
    result: dict[str, dict[str, Any]] = {}
    for key, rule in artifacts.items():
        if not isinstance(key, str) or not isinstance(rule, dict):
            result[str(key)] = _inspection_error("invalid_rule", "规则必须是对象")
            continue
        raw_path = rule.get("path")
        if not isinstance(raw_path, str) or not raw_path:
            result[key] = _inspection_error("invalid_path", "路径无效")
            continue
        if paths_escape:
            path = raw_path
            if cwd is not None and not os.path.isabs(path):
                path = os.path.normpath(os.path.join(cwd, path))
            result[key] = inspect_artifact(path, rule)
        elif cwd is None:
            result[key] = _inspection_error("invalid_path", "缺少任务 cwd")
        else:
            result[key] = _inspect_artifact_beneath(cwd, raw_path, rule)
    return result


def check_artifacts(
    artifacts: dict[str, dict], cwd: str | None = None, *, paths_escape: bool = False,
) -> dict[str, str | None]:
    return {key: detail["message"] for key, detail in inspect_artifacts(
        artifacts, cwd, paths_escape=paths_escape,
    ).items()}


def all_pass(result: dict[str, str | None]) -> bool:
    return all(v is None for v in result.values())


def inspect_declared_artifacts(spec: dict[str, Any], cwd: str, *, stop_on_failure: bool = False) -> list[dict[str, Any]]:
    """Inspect task and stage declarations; never infer execution success."""
    if not isinstance(spec, dict) or not isinstance(cwd, str) or not cwd:
        return [{"scope": "task", "name": "<spec>", **_inspection_error("invalid_spec", "任务 spec/cwd 无效")}]
    groups: list[tuple[str, Any, Any]] = [
        ("task", spec.get("artifacts", {}), spec.get("paths_escape", False))
    ]
    stages = spec.get("stages")
    if stages is not None:
        if not isinstance(stages, list):
            return [{"scope": "task", "name": "<stages>", **_inspection_error("invalid_spec", "stages 必须是数组")}]
        for index, stage in enumerate(stages):
            if not isinstance(stage, dict):
                return [{"scope": f"stage:{index}", "name": "<stage>", **_inspection_error("invalid_spec", "stage 必须是对象")}]
            groups.append(
                (
                    f"stage:{index}",
                    stage.get("artifacts", {}),
                    stage.get("paths_escape", False),
                )
            )
    details = []
    for scope, artifacts, paths_escape in groups:
        group = inspect_artifacts(artifacts, cwd, paths_escape=paths_escape)
        details.extend({"scope": scope, "name": key, **detail} for key, detail in group.items())
        if stop_on_failure and any(not detail["passed"] for detail in group.values()):
            break
    return details


def check_declared_artifacts(spec: dict[str, Any], cwd: str) -> bool:
    """Legacy predicate, sharing the same detailed validator."""
    return all(detail["passed"] for detail in inspect_declared_artifacts(spec, cwd, stop_on_failure=True))


def validate_json_artifact(path: str) -> dict | None:
    """读取 json 产物 (如 optuna best.json), 失败返回 None."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError, RecursionError):
        return None
