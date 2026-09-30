"""产物指纹 (文档 §3.2 A2).

指纹覆盖命令、工作目录、声明的合并环境、产物规则、git 内容与 runtime/venv 路径。
代码版本必须入指纹:
代码更新后同一 cmdline 的旧产物视为过期必须重跑 (darf_da 实踩).

git rev 按任务 cwd 向上找最近 .git (J 类); 非 git 目录 (git:false)
不做代码版本指纹，其余执行输入仍参与计算。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import threading
from typing import Any


GIT_TIMEOUT_SECONDS = 5
GIT_MAX_OUTPUT_BYTES = 16 * 1024 * 1024
GIT_CONFIG_OVERRIDES = (
    "-c",
    "core.fsmonitor=false",
    "-c",
    "core.untrackedCache=false",
    "-c",
    f"core.hooksPath={os.devnull}",
    "-c",
    "diff.external=",
    "-c",
    "diff.trustExitCode=false",
)


def _git_env() -> dict[str, str]:
    env = dict(os.environ)
    exact = {
        "GIT_CONFIG",
        "GIT_CONFIG_PARAMETERS",
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_EXTERNAL_DIFF",
        "GIT_DIFF_OPTS",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    }
    for key in tuple(env):
        if (
            key in exact
            or key.startswith("GIT_CONFIG_KEY_")
            or key.startswith("GIT_CONFIG_VALUE_")
        ):
            env.pop(key, None)
    env.update(
        {
            "GIT_CONFIG_COUNT": "0",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_PAGER": "cat",
            "GIT_TERMINAL_PROMPT": "0",
        }
    )
    return env


def _bounded_pipe_reader(
    fd: int, limit: int, output: bytearray, exceeded: list[bool]
) -> None:
    try:
        while True:
            chunk = os.read(fd, min(64 * 1024, limit - len(output) + 1))
            if not chunk:
                return
            remaining = limit - len(output)
            output.extend(chunk[:remaining])
            if len(chunk) > remaining:
                exceeded.append(True)
                return
    except OSError:
        return
    finally:
        os.close(fd)


def _git_bytes(
    cwd: str, args: list[str], *, max_bytes: int = GIT_MAX_OUTPUT_BYTES
) -> bytes | None:
    argv = ["git", *GIT_CONFIG_OVERRIDES, "-C", cwd, *args]
    stdout_data = bytearray()
    stderr_data = bytearray()
    stdout_exceeded: list[bool] = []
    stderr_exceeded: list[bool] = []
    stdout_read, stdout_write = os.pipe()
    stderr_read, stderr_write = os.pipe()
    readers = (
        threading.Thread(
            target=_bounded_pipe_reader,
            args=(stdout_read, max_bytes, stdout_data, stdout_exceeded),
            daemon=True,
        ),
        threading.Thread(
            target=_bounded_pipe_reader,
            args=(stderr_read, max_bytes, stderr_data, stderr_exceeded),
            daemon=True,
        ),
    )
    for reader in readers:
        reader.start()
    try:
        result = subprocess.run(
            argv,
            stdout=stdout_write,
            stderr=stderr_write,
            timeout=GIT_TIMEOUT_SECONDS,
            env=_git_env(),
            check=False,
        )
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return None
    finally:
        for fd in (stdout_write, stderr_write):
            try:
                os.close(fd)
            except OSError:
                pass
        for reader in readers:
            reader.join(timeout=1)
    if any(reader.is_alive() for reader in readers):
        return None
    if result.returncode != 0 or stdout_exceeded or stderr_exceeded:
        return None

    mocked_stdout = getattr(result, "stdout", None)
    mocked_stderr = getattr(result, "stderr", None)
    data = (
        mocked_stdout.encode()
        if isinstance(mocked_stdout, str)
        else bytes(mocked_stdout)
        if mocked_stdout is not None
        else bytes(stdout_data)
    )
    error_data = (
        mocked_stderr.encode()
        if isinstance(mocked_stderr, str)
        else bytes(mocked_stderr)
        if mocked_stderr is not None
        else bytes(stderr_data)
    )
    if len(data) > max_bytes or len(error_data) > max_bytes:
        return None
    return data


def _has_git_marker(cwd: str) -> bool | None:
    try:
        current = os.path.abspath(cwd)
        if not os.path.isdir(current):
            return None
        while True:
            if os.path.lexists(os.path.join(current, ".git")):
                return True
            parent = os.path.dirname(current)
            if parent == current:
                return False
            current = parent
    except OSError:
        return None


def _git_worktree_state(cwd: str) -> bool | None:
    """Return True for a worktree, False outside Git, and None on broken Git."""
    probe = _git_bytes(
        cwd, ["rev-parse", "--is-inside-work-tree"], max_bytes=1024
    )
    if probe is not None:
        normalized = probe.strip()
        if normalized == b"true":
            return True
        if normalized == b"false":
            return False
        return None
    marker = _has_git_marker(cwd)
    return None if marker else marker


def _git_rev(cwd: str) -> str | None:
    revision = _git_bytes(cwd, ["rev-parse", "--verify", "HEAD"], max_bytes=1024)
    if revision is None:
        return None
    try:
        text = revision.decode("ascii", "strict").strip()
    except UnicodeDecodeError:
        return None
    if re.fullmatch(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})", text) is None:
        return None
    return text.lower()


def _add_hash_record(digest: Any, label: bytes, data: bytes) -> None:
    digest.update(len(label).to_bytes(4, "big"))
    digest.update(label)
    digest.update(len(data).to_bytes(8, "big"))
    digest.update(data)


def _dirty_tree_hash(cwd: str) -> tuple[bool, str | None]:
    """Hash bounded staged and unstaged tracked content, excluding untracked files."""
    status_bytes = _git_bytes(
        cwd,
        [
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=no",
            "--ignore-submodules=untracked",
        ],
    )
    if status_bytes is None:
        return False, None
    if not status_bytes:
        return True, None

    digest = hashlib.sha256()
    _add_hash_record(digest, b"status", status_bytes)
    diff_queries = (
        [
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--binary",
            "--cached",
            "HEAD",
            "--",
        ],
        [
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--binary",
            "--",
        ],
    )
    for index, args in enumerate(diff_queries):
        diff = _git_bytes(cwd, args)
        if diff is None:
            return False, None
        _add_hash_record(digest, f"diff-{index}".encode(), diff)
    return True, digest.hexdigest()


def compute_fingerprint(
    cmd: list[str] | None,
    stages: list[dict] | None,
    cwd: str,
    git: bool | None,
    venv_paths: dict[str, str],
    runtime_prefix: str | None = None,
    native_exec_profile_sha256: str | None = None,
    native_exec_project_root_identity_sha256: str | None = None,
    *,
    execution_binding_sha256: str | None = None,
    execution_env: dict[str, str] | None = None,
    artifacts: dict | None = None,
) -> tuple[str | None, dict | None, str | None]:
    """Compute task and per-stage producer fingerprints.

    Git-enabled projects fail closed with a ``None`` task fingerprint whenever
    the revision or any dirty-content query cannot be established.
    """
    if native_exec_profile_sha256 not in (None, "") and (
        not isinstance(native_exec_profile_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", native_exec_profile_sha256) is None
    ):
        raise ValueError(
            "native_exec_profile_sha256 must be a 64-hex digest"
        )
    if native_exec_project_root_identity_sha256 not in (None, "") and (
        not isinstance(native_exec_project_root_identity_sha256, str)
        or re.fullmatch(
            r"[0-9a-f]{64}",
            native_exec_project_root_identity_sha256,
        )
        is None
    ):
        raise ValueError(
            "native_exec_project_root_identity_sha256 must be a 64-hex digest"
        )

    if execution_binding_sha256 is not None and (not isinstance(execution_binding_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", execution_binding_sha256) is None):
        raise ValueError("execution_binding_sha256 must be a SHA-256")

    # venv 路径: 把 cmd 里的 {VENV:name} 解析为实际解释器路径入指纹
    def resolve_venv(tok: str) -> str:
        if tok.startswith("{VENV:") and tok.endswith("}"):
            name = tok[len("{VENV:"):-1]
            return venv_paths.get(name, tok)
        return tok

    if git is False:
        rev = None
        use_code = False
    else:
        worktree_state = _git_worktree_state(cwd)
        if worktree_state is None:
            return None, None, None
        if not worktree_state:
            if git is True:
                return None, None, None
            rev = None
            use_code = False
        else:
            rev = _git_rev(cwd)
            if rev is None:
                return None, None, None
            use_code = True

    dirty_hash = None
    if use_code:
        dirty_ok, dirty_hash = _dirty_tree_hash(cwd)
        if not dirty_ok:
            return None, None, rev

    def fp_for(cmd_list: list[str], output_rules: dict | None = None) -> str:
        resolved = [resolve_venv(t) for t in cmd_list]
        fingerprint_payload = {
            "schema": 2,
            "cmd": resolved,
            "cwd": os.path.realpath(cwd),
            "env": execution_env or {},
            "artifacts": output_rules or {},
            "task_artifacts": artifacts or {},
            "rev": rev if use_code else None,
            "dirty": dirty_hash,   # None = 干净树
            "runtime": runtime_prefix,   # B15: 声明了才参与哈希 (环境漂移可审计)
        }
        if native_exec_profile_sha256 not in (None, ""):
            fingerprint_payload["native_exec_profile_sha256"] = (
                native_exec_profile_sha256
            )
        if native_exec_project_root_identity_sha256 not in (None, ""):
            fingerprint_payload[
                "native_exec_project_root_identity_sha256"
            ] = native_exec_project_root_identity_sha256
        if execution_binding_sha256 is not None:
            fingerprint_payload["execution_binding_sha256"] = execution_binding_sha256
        payload = json.dumps(fingerprint_payload, sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    if stages is not None:
        stage_fps = {str(i): fp_for(s["cmd"], s.get("artifacts")) for i, s in enumerate(stages)}
        # 任务级指纹 = 全部 stage 指纹串联 (代码/venv 任一变化即变)
        task_fp = hashlib.sha256(
            json.dumps({"stages": stage_fps, "artifacts": artifacts or {}}, sort_keys=True).encode()
        ).hexdigest()
        return task_fp, stage_fps, rev
    else:
        return fp_for(cmd or [], artifacts), None, rev
