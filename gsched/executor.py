"""executor (文档 §3.3 / §3.3b / R2 / B9).

- 进程组执行: Popen(start_new_session=True), 组级 killpg 全灭 (防孤儿)
- 多 stage: wrapper 进程作唯一 pgid 锚点, 串行执行各 stage (R2)
- kill_reason 机制 (N2): 组级 kill 前写 reason, reap 优先读 reason 定终态
- 日志: stdout/stderr 直接重定向到文件 (无管道阻塞风险, 根治排雷 #3);
  子进程 env 注入 PYTHONUNBUFFERED=1 (python 侧行缓冲, 日志即时性 B9 第 2 层)
- 进度解析 (第 3 层, best-effort): 从日志文件 tail 解析, 失败不影响状态
"""

from __future__ import annotations

import os
import re
import shlex
import signal
import subprocess
from typing import Any, Callable

# 常见进度行: "Epoch 5/30", "epoch: 5, loss: 0.12", "trial 3/20"
PROGRESS_RE = re.compile(
    r"(?i)(?:epoch|trial|iter|step)\s*[:/#= ]\s*(\d+)\s*(?:/|of\s+)?\s*(\d+)?"
)


def pid_cmdline_matches(pid: int, needle: str) -> bool:
    """kill 前身份校验 (审查 M4): /proc/<pid>/cmdline 含 needle 才认作目标进程.

    防 PID 复用误杀: pid 文件残留 + OS 复用该 PID 时, 只凭数字发信号会杀错
    无关用户进程。进程不存在 -> False; /proc 不可用 (macOS 开发) -> True
    (无法校验, 回退现状行为)。
    """
    if not os.path.isdir("/proc"):
        return True  # /proc 不可用 (macOS 等): 无法校验, 回退现状
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            cmd = f.read().replace(b"\0", b" ").decode("utf-8", "replace")
    except FileNotFoundError:
        return False  # 进程已不存在
    except OSError:
        return True  # 读取失败: 无法校验, 回退现状
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
    ) -> int:
        """启动任务. 返回 wrapper 进程 PID (pgid 锚点).

        - 单 cmd: wrapper = 该 cmd 直接 Popen
        - 多 stage: wrapper 是 bash 串行执行各 stage (R2, killpg 一次全灭)
        - GPU 任务: 注入 CUDA_VISIBLE_DEVICES=<gpu> (不可覆盖, §4.1)
        - CPU-only 任务 (gpu=None): 注入 CUDA_VISIBLE_DEVICES="" 禁 GPU ——
          XGB 等库启动时会初始化 CUDA context (即使 CPU 训练), 空串禁用
        - 一律注入 PYTHONUNBUFFERED=1 (日志即时性)
        """
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        log_f = open(log_path, "a", encoding="utf-8")

        merged_env = dict(os.environ)
        for k, v in env.items():
            merged_env[k] = str(v)
        # H8 修复: 钉卡/fake 剥离放在任务 env 合并**之后** (§4.1 不可覆盖);
        # 否则任务 env 里的 CUDA_VISIBLE_DEVICES/SCHED_FAKE_GPUS 静默覆盖钉卡
        merged_env["CUDA_VISIBLE_DEVICES"] = str(gpu) if gpu is not None else ""
        merged_env.setdefault("PYTHONUNBUFFERED", "1")
        merged_env.pop("SCHED_FAKE_GPUS", None)  # fake-gpu 不传染给子进程
        rc_dir = merged_env.get("SCHED_RC_DIR")
        rc_prefix = merged_env.get("SCHED_RC_PREFIX")
        launch_marker = merged_env.get("SCHED_LAUNCH_MARKER")
        launch_script = ""
        if launch_marker:
            marker_path = shlex.quote(str(launch_marker))
            marker_tmp = shlex.quote(f"{launch_marker}.tmp")
            launch_script = (
                f"(umask 077; launch_tmp={marker_tmp}.$$; "
                f"if [ -r /proc/$$/stat ] && "
                f"/usr/bin/awk '{{print $1 \" \" $22}}' /proc/$$/stat > \"$launch_tmp\"; "
                f"then :; else printf '%s\\n' \"$$\" > \"$launch_tmp\"; fi && "
                f"/bin/mv -f \"$launch_tmp\" {marker_path}) && "
            )
        if self.sanitize_env:
            self._sanitize_conda_env(merged_env, cmd, stages, explicit=conda_env_dir)

        rc_script = ""
        if rc_dir and rc_prefix:
            rc_script = (
                "; __sched_rc=$?; __sched_rc_path=\"$SCHED_RC_DIR/"
                "$SCHED_RC_PREFIX-$$.rc\"; __sched_rc_tmp=\"${__sched_rc_path}.tmp.$$\"; "
                "printf '%s\\n' \"$__sched_rc\" > \"$__sched_rc_tmp\" && "
                "/bin/mv -f \"$__sched_rc_tmp\" \"$__sched_rc_path\"; "
                "exit \"$__sched_rc\""
            )
        if stages is not None:
            # §3.4c 断点续跑: 已成功的 stage (产物已存在) 跳过, 只从失败 stage 起重跑
            # M9 修复: 产物相对路径必须拼任务 cwd —— 否则相对 daemon 进程 cwd 判定,
            # 找不到 -> 已成功 stage 全部重跑 (断点续跑静默失效)
            parts = []
            for i, s in enumerate(stages):
                arts = s.get("artifacts", {})
                done = bool(arts) and all(
                    os.path.isfile(
                        str(a.get("path", ""))
                        if os.path.isabs(str(a.get("path", "")))
                        else os.path.join(cwd, str(a.get("path", "")))
                    )
                    for a in arts.values()
                )
                if on_stage_start:
                    on_stage_start(i)
                if done:
                    # stage 产物已存在: 跳过执行 (echo 标记)
                    parts.append(f"echo [sched] stage{i} 产物已存在, 跳过")
                    continue
                parts.append(" ".join(shlex.quote(str(t)) for t in s["cmd"]))
            wrapper_cmd = [
                "/bin/bash",
                "--noprofile",
                "--norc",
                "-c",
                launch_script + " && ".join(parts) + rc_script,
            ]
        elif rc_script or launch_script:
            command = launch_script + " ".join(shlex.quote(str(t)) for t in (cmd or []))
            wrapper_cmd = ["/bin/bash", "--noprofile", "--norc", "-c", command + rc_script]
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
                marker_value = str(proc.pid)
                try:
                    with open(f"/proc/{proc.pid}/stat", encoding="utf-8") as proc_stat:
                        fields = proc_stat.read().rsplit(")", 1)[1].split()
                    if len(fields) > 19:
                        marker_value += f" {fields[19]}"
                except (OSError, IndexError):
                    try:
                        with open(marker_path, encoding="utf-8") as existing:
                            existing_fields = existing.read().split()
                        if len(existing_fields) >= 2 and existing_fields[0] == str(proc.pid):
                            marker_value = " ".join(existing_fields[:2])
                    except (OSError, IndexError):
                        pass
                marker_tmp = f"{marker_path}.parent.{proc.pid}.tmp"
                with open(marker_tmp, "w", encoding="utf-8") as marker:
                    marker.write(marker_value + "\n")
                os.replace(marker_tmp, marker_path)
            log_f.close()
            self._procs[proc.pid] = proc
        except Exception:
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
            pass
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
            # SIGKILL is terminal for a successfully signalled local Popen.
            # Wait for the group leader before dropping its handle so a
            # rollback cannot retain an unreaped zombie forever.
            if sig == signal.SIGKILL and kill_sent:
                proc = self._procs.get(pgid)
                if proc is not None:
                    try:
                        proc.wait(timeout=1)
                    except Exception:
                        pass
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
