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


class Executor:
    def __init__(
        self,
        on_progress: Callable[[str], None] | None = None,
    ):
        self.on_progress = on_progress  # 第 3 层: 从日志行解析进度 (best-effort)
        self._procs: dict[int, subprocess.Popen] = {}  # pgid -> proc
        self._rces: dict[int, int | None] = {}  # pgid -> rc (进程退出后)

    def launch(
        self,
        cmd: list[str] | None,
        stages: list[dict] | None,
        cwd: str,
        env: dict[str, str],
        gpu: int,
        log_path: str,
        on_stage_start: Callable[[int], None] | None = None,
    ) -> int:
        """启动任务. 返回 wrapper 进程 PID (pgid 锚点).

        - 单 cmd: wrapper = 该 cmd 直接 Popen
        - 多 stage: wrapper 是 bash 串行执行各 stage (R2, killpg 一次全灭)
        - 注入 CUDA_VISIBLE_DEVICES (不可覆盖, §4.1) + PYTHONUNBUFFERED=1
        """
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        log_f = open(log_path, "a", encoding="utf-8")

        merged_env = dict(os.environ)
        merged_env["CUDA_VISIBLE_DEVICES"] = str(gpu)
        merged_env.setdefault("PYTHONUNBUFFERED", "1")
        merged_env.pop("SCHED_FAKE_GPUS", None)  # fake-gpu 不传染给子进程
        for k, v in env.items():
            merged_env[k] = str(v)

        if stages is not None:
            parts = []
            for i, s in enumerate(stages):
                if on_stage_start:
                    on_stage_start(i)
                parts.append(" ".join(shlex.quote(str(t)) for t in s["cmd"]))
            wrapper_cmd = ["bash", "-lc", " && ".join(parts)]
        else:
            wrapper_cmd = [str(t) for t in (cmd or [])]

        proc = subprocess.Popen(
            wrapper_cmd,
            cwd=cwd,
            env=merged_env,
            stdout=log_f,
            stderr=subprocess.STDOUT,
            start_new_session=True,  # 新进程组, pgid = proc.pid
        )
        log_f.close()
        self._procs[proc.pid] = proc
        return proc.pid

    def poll_rc(self, pgid: int) -> int | None:
        """查询进程退出码. 未退出返回 None; 已退出返回 rc (并清理记录)."""
        proc = self._procs.get(pgid)
        if proc is None:
            # daemon 重启后接管: 无 proc 对象, 用 killpg 存活判断
            return None if self.alive(pgid) else 137
        rc = proc.poll()
        if rc is not None:
            self._procs.pop(pgid, None)
        return rc

    # ---------- 组级 kill ----------

    def kill_pgid(self, pgid: int, sig: int = signal.SIGTERM) -> None:
        """组级杀 (进程组全灭). pgid 即 wrapper PID (start_new_session 锚点)."""
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            pass

    def alive(self, pgid: int) -> bool:
        try:
            os.killpg(pgid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False

    # ---------- 日志 ----------

    def tail(self, log_path: str, n: int = 20) -> str:
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
            return "".join(lines[-n:])
        except OSError:
            return "(日志不存在)"

    def parse_progress(self, log_path: str) -> str | None:
        """第 3 层进度: 从日志尾部解析 epoch/trial 进度 (best-effort)."""
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()[-200:]
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
        """B2 失败分类: 从日志找 OOM / gpu_fault / perm / error."""
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                text = f.read()
        except OSError:
            return "error", None
        if "CUDA out of memory" in text or "OutOfMemoryError" in text:
            return "oom", "CUDA OOM"
        if re.search(r"\bXid\b|\bECC\b", text):
            return "gpu_fault", "驱动级硬件错误 (Xid/ECC)"
        if "PermissionError" in text or "Operation not permitted" in text or "EACCES" in text:
            return "perm", "权限错误"
        return "error", None
