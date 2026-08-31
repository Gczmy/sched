"""executor (文档 §3.3 / §3.3b / R2 / B9).

- 进程组执行: Popen(start_new_session=True), 组级 killpg 全灭 (防孤儿)
- 多 stage: wrapper 进程作唯一 pgid 锚点, 串行执行各 stage (R2)
- kill_reason 机制 (N2): 组级 kill 前写 reason, reap 优先读 reason 定终态
- 日志: stdout/stderr 直接重定向到文件 (无管道阻塞风险, 根治排雷 #3);
  子进程 env 注入 PYTHONUNBUFFERED=1 (python 侧行缓冲, 日志即时性 B9 第 2 层)
- 进度解析 (第 3 层, best-effort): 从日志文件 tail 解析, 失败不影响状态
"""

from __future__ import annotations

import ctypes
import json
import os
import re
import secrets
import shlex
import signal
import subprocess
import sys
from typing import Any, Callable
from . import state

from .artifacts import all_pass, check_artifacts
from .native_launch import NativeLaunchPlan, NativeLaunchUnavailable

# 常见进度行: "Epoch 5/30", "epoch: 5, loss: 0.12", "trial 3/20"
PROGRESS_RE = re.compile(
    r"(?i)(?:epoch|trial|iter|step)\s*[:/#= ]\s*(\d+)\s*(?:/|of\s+)?\s*(\d+)?"
)

NATIVE_EXEC_ALLOWED_ENV_KEYS = frozenset(
    {
        "SCHED_PROFILE_OUT",
        "SCHED_BATCH_ID",
        "SCHED_TASK_ID",
        "SCHED_RUN_ID",
        "SCHED_PROJECT",
        "SCHED_RC_DIR",
        "SCHED_RC_PREFIX",
        "SCHED_LAUNCH_MARKER",
    }
)


class _DarwinProcBsdInfo(ctypes.Structure):
    _fields_ = [
        ("pbi_flags", ctypes.c_uint32),
        ("pbi_status", ctypes.c_uint32),
        ("pbi_xstatus", ctypes.c_uint32),
        ("pbi_pid", ctypes.c_uint32),
        ("pbi_ppid", ctypes.c_uint32),
        ("pbi_uid", ctypes.c_uint32),
        ("pbi_gid", ctypes.c_uint32),
        ("pbi_ruid", ctypes.c_uint32),
        ("pbi_rgid", ctypes.c_uint32),
        ("pbi_svuid", ctypes.c_uint32),
        ("pbi_svgid", ctypes.c_uint32),
        ("rfu_1", ctypes.c_uint32),
        ("pbi_comm", ctypes.c_char * 16),
        ("pbi_name", ctypes.c_char * 32),
        ("pbi_nfiles", ctypes.c_uint32),
        ("pbi_pgid", ctypes.c_uint32),
        ("pbi_pjobc", ctypes.c_uint32),
        ("e_tdev", ctypes.c_uint32),
        ("e_tpgid", ctypes.c_uint32),
        ("pbi_nice", ctypes.c_int32),
        ("pbi_start_tvsec", ctypes.c_uint64),
        ("pbi_start_tvusec", ctypes.c_uint64),
    ]


_darwin_proc_pidinfo: Any = None
_darwin_libproc_unavailable = False


def _darwin_start_token(pid: int) -> str | None:
    """Read Darwin's kernel process birth timeval through libproc."""
    global _darwin_proc_pidinfo, _darwin_libproc_unavailable
    if _darwin_libproc_unavailable:
        return None
    if _darwin_proc_pidinfo is None:
        try:
            libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
            proc_pidinfo = libproc.proc_pidinfo
            proc_pidinfo.argtypes = [
                ctypes.c_int,
                ctypes.c_int,
                ctypes.c_uint64,
                ctypes.c_void_p,
                ctypes.c_int,
            ]
            proc_pidinfo.restype = ctypes.c_int
            _darwin_proc_pidinfo = proc_pidinfo
        except (AttributeError, OSError):
            _darwin_libproc_unavailable = True
            return None

    info = _DarwinProcBsdInfo()
    try:
        result = _darwin_proc_pidinfo(
            pid,
            3,  # PROC_PIDTBSDINFO
            0,
            ctypes.byref(info),
            ctypes.sizeof(info),
        )
    except (OSError, TypeError, ValueError):
        return None
    if (
        result != ctypes.sizeof(info)
        or info.pbi_pid != pid
        or info.pbi_start_tvsec <= 0
        or info.pbi_start_tvusec >= 1_000_000
    ):
        return None
    return f"darwin:{info.pbi_start_tvsec}:{info.pbi_start_tvusec}"


def _is_strong_start_token(token: object) -> bool:
    if not isinstance(token, str):
        return False
    if token.startswith("proc:"):
        ticks = token[5:]
        return bool(ticks) and ticks.isdigit()
    if not token.startswith("darwin:"):
        return False
    fields = token.split(":")
    if len(fields) != 3 or not fields[1].isdigit() or not fields[2].isdigit():
        return False
    return int(fields[1]) > 0 and int(fields[2]) < 1_000_000


def process_start_token(pid: int) -> str | None:
    """Return an immutable OS process-start token, or None when unprovable."""
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return None
    if sys.platform == "darwin":
        return _darwin_start_token(pid)
    if not sys.platform.startswith("linux"):
        return None
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as proc_stat:
            fields = proc_stat.read().rsplit(")", 1)[1].split()
        token = f"proc:{fields[19]}" if len(fields) > 19 else None
        return token if _is_strong_start_token(token) else None
    except (OSError, IndexError):
        return None


def pid_cmdline_matches(pid: int, needle: str) -> bool:
    """Return whether a readable process command line contains ``needle``."""
    if not isinstance(needle, str) or not needle:
        return False
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as stream:
            cmd = stream.read().replace(b"\0", b" ").decode("utf-8", "replace")
    except OSError:
        try:
            result = subprocess.run(
                ["ps", "-p", str(pid), "-o", "command="],
                capture_output=True,
                text=True,
                timeout=1,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        if result.returncode != 0:
            return False
        cmd = result.stdout
    return needle in cmd


def read_tail(path: str, max_bytes: int = 1024 * 1024) -> str:
    """尾部读取 (P2): seek 到文件末尾前 max_bytes, 不整读 GB 级日志进内存.

    文件不存在/不可读 -> 抛 OSError (调用方按现状捕获处理)。
    首行可能是半行 (块边界切开), 子串/正则匹配场景无害。
    """
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        f.seek(max(0, size - max_bytes))
        return f.read().decode("utf-8", "replace")


def stage_checkpoint_valid(
    stage: dict[str, Any],
    cwd: str,
    fingerprint: str | None,
    checkpoint_dir: str | None,
    stage_index: int,
    *,
    force_rerun: bool = False,
) -> bool:
    """Return whether an exact producer checkpoint can skip one stage."""
    if force_rerun or not fingerprint or not checkpoint_dir:
        return False
    sidecar_path = os.path.join(checkpoint_dir, f"stage-{stage_index}.json")
    try:
        with open(sidecar_path, "r", encoding="utf-8") as sidecar_file:
            sidecar = json.load(sidecar_file)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return False
    if sidecar != {"schema_version": 1, "fingerprint": fingerprint}:
        return False

    artifacts = stage.get("artifacts", {})
    paths_escape = stage.get("paths_escape", False)
    if not isinstance(artifacts, dict) or not isinstance(paths_escape, bool):
        return False
    return all_pass(
        check_artifacts(
            artifacts,
            cwd,
            paths_escape=paths_escape,
        )
    )


def _checkpoint_commit_command(sidecar_path: str, fingerprint: str) -> str:
    payload = json.dumps(
        {"schema_version": 1, "fingerprint": fingerprint},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    sidecar = shlex.quote(sidecar_path)
    tmp_base = shlex.quote(f"{sidecar_path}.tmp")
    return (
        f"(umask 077; __sched_stage_tmp={tmp_base}.$$; "
        f"printf '%s\\n' {shlex.quote(payload)} > \"$__sched_stage_tmp\" && "
        f"/bin/chmod 0600 \"$__sched_stage_tmp\" && "
        f"/bin/mv -f \"$__sched_stage_tmp\" {sidecar})"
    )


def _artifact_validation_command(
    artifacts: Any,
    cwd: str,
    *,
    paths_escape: bool,
) -> str:
    """Build a shell-safe post-stage validator command."""
    package_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    program = (
        "import json,sys;"
        "sys.path.insert(0,sys.argv[1]);"
        "from gsched.artifacts import all_pass,check_artifacts;"
        "rules=json.loads(sys.argv[2]);"
        "sys.exit(0 if all_pass(check_artifacts("
        "rules,sys.argv[3],paths_escape=sys.argv[4]=='1')) else 1)"
    )
    argv = [
        sys.executable,
        "-I",
        "-c",
        program,
        package_root,
        json.dumps(artifacts, ensure_ascii=True, separators=(",", ":")),
        cwd,
        "1" if paths_escape else "0",
    ]
    return " ".join(shlex.quote(argument) for argument in argv)

def _launch_marker_command(marker_path: str) -> str:
    """Build the wrapper-side strong-identity marker publisher."""
    package_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    program = (
        "import os,sys\n"
        "sys.path.insert(0,sys.argv[1])\n"
        "from gsched import state\n"
        "from gsched.executor import process_start_token\n"
        "path=sys.argv[2]\n"
        "pid=int(sys.argv[3])\n"
        "token=process_start_token(pid)\n"
        "if token is None: raise SystemExit(1)\n"
        "tmp=f'{path}.child.{pid}.{os.getpid()}.tmp'\n"
        "try:\n"
        " with state.open_private_text(tmp,'x') as stream:\n"
        "  stream.write(f'{pid} {token}\\n')\n"
        "  stream.flush()\n"
        "  os.fsync(stream.fileno())\n"
        " os.replace(tmp,path)\n"
        "finally:\n"
        " try: os.unlink(tmp)\n"
        " except OSError: pass\n"
    )
    argv = [
        sys.executable,
        "-I",
        "-c",
        program,
        package_root,
        marker_path,
    ]
    return " ".join(shlex.quote(argument) for argument in argv) + ' "$$"'


class Executor:
    def __init__(
        self,
        on_progress: Callable[[str], None] | None = None,
        sanitize_env: bool = True,
    ):
        self.on_progress = on_progress  # 第 3 层: 从日志行解析进度 (best-effort)
        # B13-§1: conda 环境净化开关 (daemon 自身激活的 env 会经 os.environ
        # 泄漏给子任务 —— LD_LIBRARY_PATH/CONDA_PREFIX 抢载导致跨 env import 冲突)
        self.sanitize_env = sanitize_env
        self._procs: dict[int, subprocess.Popen] = {}  # pgid -> proc
        self._dead_pgroups: set[int] = set()
        # D2: _rces 死字段已删 (全仓无读写, rc 读取走 _procs[pgid].poll())

    def has_process(self, pgid: int) -> bool:
        """Whether this executor still owns the Popen handle for a process group."""
        return pgid in self._procs

    def local_supervisor_completed(self, pgid: int) -> bool | None:
        """Prove the local wrapper completed its normal group-drain path."""
        proc = self._procs.get(pgid)
        if proc is None:
            return None
        returncode = proc.poll()
        return returncode is not None and returncode >= 0
    @staticmethod
    def _detect_conda_env(cmd: list[str] | None, stages: list[dict] | None):
        """从 cmd/stages 探测 conda env 解释器 (/envs/<name>/bin/python) -> env 根目录."""
        tokens = [str(t) for t in (cmd or [])]
        for st in stages or []:
            tokens.extend(str(t) for t in (st.get("cmd") or []))
        for t in tokens:
            if "/envs/" in t:
                head, tail = t.split("/envs/", 1)
                seg = tail.split("/")
                if seg and seg[0]:
                    return os.path.join(head, "envs", seg[0])
        return None

    def _sanitize_conda_env(
        self, merged_env: dict, cmd: list[str] | None, stages: list[dict] | None,
        explicit: str | None = None,
    ) -> None:
        """explicit = 任务 runtime 声明解析出的前缀 (B15), 优先于特征探测."""
        """B13-§1: 剥离父进程(daemon)的 conda 污染键, 按 VENV 路径注入等效 activate.

        daemon 在某 conda env 下启动时, 其 CONDA_PREFIX/LD_LIBRARY_PATH 会经
        dict(os.environ) 泄漏给所有子任务 —— {VENV:txl} 的 python 加载到别的
        env 的 libstdc++/MKL 即 import 冲突。此处剥离污染键并按任务自己的
        env 注入关键子集 (CONDA_PREFIX/bin/lib), 等效 activate 免去 source
        conda.sh。batch.json 的 env 字段仍可最终覆盖 (合并顺序在前, 本方法
        只在键缺失时补 PATH/LD_LIBRARY_PATH 前缀, CONDA_PREFIX 直接覆盖).
        """
        env_dir = explicit or self._detect_conda_env(cmd, stages)
        for k in (
            "CONDA_PREFIX", "CONDA_DEFAULT_ENV", "CONDA_PROMPT_MODIFIER",
            "CONDA_SHLVL", "CONDA_PYTHON_EXE",
        ):
            merged_env.pop(k, None)
        if not env_dir:
            return
        merged_env["CONDA_PREFIX"] = env_dir
        merged_env["PATH"] = env_dir + "/bin:" + merged_env.get("PATH", "")
        lib = env_dir + "/lib"
        ldl = merged_env.get("LD_LIBRARY_PATH", "")
        merged_env["LD_LIBRARY_PATH"] = f"{lib}:{ldl}" if ldl else lib

    def launch_native(self, plan: NativeLaunchPlan) -> int:
        """Consume one retained native plan or fail without a fallback.

        The actual entry argv and empty environment are properties of the
        scheduler-owned plan.  This interface intentionally accepts no public
        ``cmd``, pathname executable, environment, cwd, or shell input.  The
        reviewed Linux FD-exec backend is a later step, so the current method
        closes every plan-owned descriptor and raises before process creation.
        """
        if not isinstance(plan, NativeLaunchPlan):
            raise TypeError("launch_native requires a NativeLaunchPlan")
        try:
            plan.validate_live_fds()
            raise NativeLaunchUnavailable(
                "native FD-exec backend is not connected; no pathname or logical-argv "
                "fallback is allowed"
            )
        finally:
            plan.close()

    def launch(
        self,
        cmd: list[str] | None,
        stages: list[dict] | None,
        cwd: str,
        env: dict[str, str],
        gpu: int | None,
        log_path: str,
        on_stage_start: Callable[[int], None] | None = None,
        conda_env_dir: str | None = None,
        stage_fingerprints: dict[str, str] | None = None,
        stage_checkpoint_dir: str | None = None,
        force_rerun: bool = False,
        native_exec_profile_id: str | None = None,
        native_exec_profile_sha256: str | None = None,
        native_exec_submitted_argv: list[str] | None = None,
    ) -> int:
        """启动任务. 返回 wrapper 进程 PID (pgid 锚点).

        - 单 cmd: wrapper = 该 cmd 直接 Popen
        - 多 stage: wrapper 是 bash 串行执行各 stage (R2, killpg 一次全灭)
        - GPU 任务: 注入 CUDA_VISIBLE_DEVICES=<gpu> (不可覆盖, §4.1)
        - CPU-only 任务 (gpu=None): 注入 CUDA_VISIBLE_DEVICES="" 禁 GPU ——
          XGB 等库启动时会初始化 CUDA context (即使 CPU 训练), 空串禁用
        - 一律注入 PYTHONUNBUFFERED=1 (日志即时性)
        - legacy V1 native-exec 三字段必须同时缺席或同时有效；启用时只允许
          单一 exact argv 并直接 Popen，不经 bash supervisor/RC shell。该路径
          仍是非正式 foundation；V2 retained-FD 接口只允许走 launch_native。
        """
        native_values = (
            native_exec_profile_id,
            native_exec_profile_sha256,
            native_exec_submitted_argv,
        )
        native_exec = any(value is not None for value in native_values)
        if native_exec:
            if any(value is None for value in native_values):
                raise ValueError("native-exec metadata must be all present or all absent")
            if (
                not isinstance(native_exec_profile_id, str)
                or re.fullmatch(
                    r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}",
                    native_exec_profile_id,
                )
                is None
            ):
                raise ValueError("native-exec profile id is invalid")
            if (
                not isinstance(native_exec_profile_sha256, str)
                or re.fullmatch(r"[0-9a-f]{64}", native_exec_profile_sha256)
                is None
            ):
                raise ValueError("native-exec profile digest is invalid")
            if (
                not isinstance(native_exec_submitted_argv, list)
                or not native_exec_submitted_argv
                or any(
                    not isinstance(token, str) or not token or "\0" in token
                    for token in native_exec_submitted_argv
                )
            ):
                raise ValueError("native-exec submitted argv is invalid")
            if (
                not os.path.isabs(native_exec_submitted_argv[0])
                or os.path.normpath(native_exec_submitted_argv[0])
                != native_exec_submitted_argv[0]
            ):
                raise ValueError(
                    "native-exec executable must be a normalized absolute path"
                )
            if conda_env_dir is not None:
                raise ValueError("native-exec launch forbids an explicit runtime")
            if gpu is not None:
                raise ValueError("native-exec launch is CPU-only")
            if stages is not None:
                raise ValueError("native-exec launch forbids stages")
            if cmd != native_exec_submitted_argv:
                raise ValueError("native-exec command differs from submitted argv")
            unexpected_env = sorted(set(env) - NATIVE_EXEC_ALLOWED_ENV_KEYS)
            if unexpected_env:
                raise ValueError(
                    "native-exec environment contains non-scheduler keys: "
                    f"{unexpected_env}"
                )
        if stages is not None and stage_checkpoint_dir:
            state.ensure_private_directory(stage_checkpoint_dir)
        state.ensure_private_directory(os.path.dirname(log_path))
        log_f = state.open_private_text(log_path, "a")

        # A native verifier is the first reviewed process.  It must not inherit
        # daemon/PATH/loader/Python startup state; only dispatcher-owned control
        # values are copied into an otherwise empty execve environment.
        merged_env = {} if native_exec else dict(os.environ)
        for k, v in env.items():
            merged_env[k] = str(v)
        # H8 修复: 钉卡/fake 剥离放在任务 env 合并**之后** (§4.1 不可覆盖);
        # 否则任务 env 里的 CUDA_VISIBLE_DEVICES/SCHED_FAKE_GPUS 静默覆盖钉卡
        merged_env["CUDA_VISIBLE_DEVICES"] = str(gpu) if gpu is not None else ""
        if not native_exec:
            merged_env.setdefault("PYTHONUNBUFFERED", "1")
        merged_env.pop("SCHED_FAKE_GPUS", None)  # fake-gpu 不传染给子进程
        rc_dir = merged_env.get("SCHED_RC_DIR")
        rc_prefix = merged_env.get("SCHED_RC_PREFIX")
        launch_marker = merged_env.get("SCHED_LAUNCH_MARKER")
        launch_script = ""
        if launch_marker:
            launch_marker = str(launch_marker)
            state.ensure_private_directory(os.path.dirname(launch_marker) or ".")
            launch_script = _launch_marker_command(launch_marker) + " && "
        if self.sanitize_env and not native_exec:
            self._sanitize_conda_env(
                merged_env,
                cmd,
                stages,
                explicit=conda_env_dir,
            )

        rc_script = ""
        if rc_dir and rc_prefix:
            rc_script = (
                "; __sched_rc=$?; umask 077; "
                "__sched_rc_path=\"$SCHED_RC_DIR/$SCHED_RC_PREFIX-$$.rc\"; "
                "__sched_rc_tmp=\"${__sched_rc_path}.tmp.$$\"; "
                "printf '%s\\n' \"$__sched_rc\" > \"$__sched_rc_tmp\" && "
                "/bin/mv -f \"$__sched_rc_tmp\" \"$__sched_rc_path\"; "
                "exit \"$__sched_rc\""
            )
        supervisor_script = (
            "trap '' TERM; "
            "__sched_group_empty_once() { "
            "__sched_members=$(/bin/ps -axo pid=,pgid= 2>/dev/null | "
            "/usr/bin/awk -v group=\"$$\" '$2 == group { print $1 }') || return 1; "
            "__sched_saw_leader=0; "
            "for __sched_member in $__sched_members; do "
            "if [ \"$__sched_member\" = \"$$\" ]; then "
            "__sched_saw_leader=1; "
            "elif kill -0 \"$__sched_member\" 2>/dev/null; then return 1; fi; "
            "done; "
            "[ \"$__sched_saw_leader\" -eq 1 ]; "
            "}; "
            "__sched_wait_group() { "
            "__sched_empty_samples=0; "
            "while [ \"$__sched_empty_samples\" -lt 2 ]; do "
            "if __sched_group_empty_once; then "
            "__sched_empty_samples=$((__sched_empty_samples + 1)); "
            "else __sched_empty_samples=0; fi; "
            "if [ \"$__sched_empty_samples\" -lt 2 ]; then /bin/sleep 0.05; fi; "
            "done; "
            "}; "
            "__sched_run() { "
            "(trap - TERM; exec \"$@\") & "
            "__sched_child=$!; wait \"$__sched_child\"; "
            "__sched_foreground_rc=$?; "
            "__sched_wait_group; "
            "return \"$__sched_foreground_rc\"; "
            "}; "
        )
        if native_exec:
            # The first process must be the externally reviewed native verifier
            # itself.  A shell wrapper would create an unreviewed execution
            # boundary and could rewrite argv or environment before execve.
            wrapper_cmd = [str(token) for token in (cmd or [])]
        elif stages is not None:
            parts = []
            stale_sidecars = []
            fingerprints = stage_fingerprints or {}
            rerun_downstream = force_rerun
            for i, stage in enumerate(stages):
                fingerprint = fingerprints.get(str(i))
                if on_stage_start:
                    on_stage_start(i)
                can_skip = not rerun_downstream and stage_checkpoint_valid(
                    stage,
                    cwd,
                    fingerprint,
                    stage_checkpoint_dir,
                    i,
                )
                if can_skip:
                    parts.append(f"echo [sched] stage{i} checkpoint 匹配, 跳过")
                    continue

                rerun_downstream = True
                if stage_checkpoint_dir:
                    stale_sidecars.append(
                        os.path.join(stage_checkpoint_dir, f"stage-{i}.json")
                    )
                stage_command = "__sched_run " + " ".join(
                    shlex.quote(token) for token in stage["cmd"]
                )
                artifacts = stage.get("artifacts", {})
                stage_command += " && __sched_run " + _artifact_validation_command(
                    artifacts,
                    cwd,
                    paths_escape=stage.get("paths_escape", False),
                )
                if fingerprint and stage_checkpoint_dir:
                    sidecar_path = os.path.join(
                        stage_checkpoint_dir, f"stage-{i}.json"
                    )
                    stage_command += (
                        " && "
                        + _checkpoint_commit_command(sidecar_path, fingerprint)
                    )
                parts.append(stage_command)
            if stale_sidecars:
                parts.insert(
                    0,
                    "/bin/rm -f "
                    + " ".join(shlex.quote(path) for path in stale_sidecars),
                )
            wrapper_cmd = [
                "/bin/bash",
                "--noprofile",
                "--norc",
                "-c",
                supervisor_script + launch_script + " && ".join(parts) + rc_script,
            ]
        elif rc_script or launch_script:
            command = launch_script + "__sched_run " + " ".join(
                shlex.quote(str(t)) for t in (cmd or [])
            )
            wrapper_cmd = [
                "/bin/bash",
                "--noprofile",
                "--norc",
                "-c",
                supervisor_script + command + rc_script,
            ]
        else:
            wrapper_cmd = [str(t) for t in (cmd or [])]

        try:
            proc = subprocess.Popen(
                wrapper_cmd,
                cwd=cwd,
                env=merged_env,
                stdout=log_f,
                stderr=subprocess.STDOUT,
                start_new_session=True,  # 新进程组, pgid = proc.pid
            )
        except Exception:
            try:
                log_f.close()  # Popen 失败 (cwd/cmd 非法等)
            except Exception:
                pass
            raise
        self._dead_pgroups.discard(proc.pid)
        try:
            if launch_marker:
                marker_path = str(launch_marker)
                marker_token = process_start_token(proc.pid)
                marker_tmp = (
                    f"{marker_path}.parent.{os.getpid()}.{proc.pid}."
                    f"{secrets.token_hex(8)}.tmp"
                )
                try:
                    with state.open_private_text(marker_tmp, "x") as marker:
                        if not _is_strong_start_token(marker_token):
                            raise OSError(
                                "strong process identity unavailable for launch marker"
                            )
                        marker.write(f"{proc.pid} {marker_token}\n")
                        marker.flush()
                        os.fsync(marker.fileno())
                    os.replace(marker_tmp, marker_path)
                finally:
                    try:
                        os.unlink(marker_tmp)
                    except OSError:
                        pass
            log_f.close()
            self._procs[proc.pid] = proc
        except Exception:
            try:
                log_f.close()
            except Exception:
                pass
            marker_token = locals().get("marker_token")
            if (
                proc.poll() is None
                and _is_strong_start_token(marker_token)
                and process_start_token(proc.pid) == marker_token
            ):
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except (OSError, ProcessLookupError):
                    pass
            try:
                proc.wait(timeout=1)
            except Exception:
                pass
            raise
        return proc.pid

    def poll_rc(self, pgid: int) -> int | None:
        """查询进程退出码. 未退出返回 None; 已退出返回 rc (并清理记录)."""
        proc = self._procs.get(pgid)
        if proc is None:
            # daemon 重启后接管: 无 proc 对象, 用 killpg 存活判断
            return None if self.alive(pgid) else 137
        rc = proc.poll()
        if rc is None:
            return None
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            self._procs.pop(pgid, None)
            return rc
        except PermissionError:
            if len(self._dead_pgroups) >= 1024:
                self._dead_pgroups.clear()
            self._dead_pgroups.add(pgid)
            self._procs.pop(pgid, None)
            return rc
        except OSError:
            return None
        return None

    # ---------- 组级 kill ----------

    def kill_pgid(self, pgid: int, sig: int = signal.SIGTERM) -> bool:
        """组级杀 (进程组全灭). pgid 即 wrapper PID (start_new_session 锚点)."""
        kill_sent = True
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            kill_sent = False
            if sig == signal.SIGKILL:
                proc = self._procs.pop(pgid, None)
                if proc is not None:
                    try:
                        proc.wait(timeout=1)
                    except Exception:
                        pass
                if len(self._dead_pgroups) >= 1024:
                    self._dead_pgroups.clear()
                self._dead_pgroups.add(pgid)
        except PermissionError:
            # A local Popen may already be a zombie on Darwin: killpg reports
            # EPERM even though poll() proves the wrapper has exited.
            proc = self._procs.get(pgid)
            if sig == signal.SIGKILL and proc is not None and proc.poll() is not None:
                if len(self._dead_pgroups) >= 1024:
                    self._dead_pgroups.clear()
                self._dead_pgroups.add(pgid)
                kill_sent = True
            else:
                # No proof of death: retain bookkeeping so callers can retry.
                kill_sent = False
        finally:
            # A successful SIGKILL is terminal only after this executor reaps
            # the exact local wrapper. Cache that proof so a rapidly reused
            # process-group number cannot make the old job appear live again.
            if sig == signal.SIGKILL and kill_sent:
                proc = self._procs.get(pgid)
                if proc is not None:
                    try:
                        proc.wait(timeout=1)
                    except (OSError, subprocess.SubprocessError):
                        pass
                    else:
                        if len(self._dead_pgroups) >= 1024:
                            self._dead_pgroups.clear()
                        self._dead_pgroups.add(pgid)
                        self._procs.pop(pgid, None)
        return kill_sent
    def alive(self, pgid: int) -> bool:
        if pgid in self._dead_pgroups:
            return False
        try:
            os.killpg(pgid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            # 决策 6A: 进程存在但无权探测 (跨用户) -> 判活 (保守: 不误标 rc=137
            # 失败、不参与 SIGKILL 升级误杀), 与 dispatcher._pid_exists 对齐
            return True

    # ---------- 日志 ----------

    def tail(self, log_path: str, n: int = 20) -> str:
        try:
            lines = read_tail(log_path, 256 * 1024).splitlines(keepends=True)
            return "".join(lines[-n:])
        except OSError:
            return "(日志不存在)"

    def parse_progress(self, log_path: str) -> str | None:
        """第 3 层进度: 从日志尾部解析 epoch/trial 进度 (best-effort)."""
        try:
            lines = read_tail(log_path, 256 * 1024).splitlines()[-200:]
        except OSError:
            return None
        for line in reversed(lines):
            m = PROGRESS_RE.search(line)
            if m:
                cur = m.group(1)
                total = m.group(2) or "?"
                return f"{cur}/{total}"
        return None

    def failed_classify(self, log_path: str) -> tuple[str, str | None]:
        """B2 失败分类: 从日志找 OOM / gpu_fault / perm / error.

        P2: 读尾部 4MiB (失败特征一般在尾部), 不再整读 GB 级日志进内存。
        """
        try:
            text = read_tail(log_path, 4 * 1024 * 1024)
        except OSError:
            return "error", None
        if "CUDA out of memory" in text or "OutOfMemoryError" in text:
            return "oom", "CUDA OOM"
        if re.search(r"\bXid\b|\bECC\b", text):
            return "gpu_fault", "驱动级硬件错误 (Xid/ECC)"
        if "PermissionError" in text or "Operation not permitted" in text or "EACCES" in text:
            return "perm", "权限错误"
        return "error", None
