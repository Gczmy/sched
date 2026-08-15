"""allocator (文档 §5b B5 / M7 / M8 / §3.4b2 D3).

GPU 状态机 (注册表内维护, 唯一权威):
  free -> assigned(job) -> releasing -> free
                  └-> unmanaged (物理抽查发现外部占用/孤儿, 不派发)

物理校验只做两个边界 (releasing 归零确认 + free 的 unmanaged 抽查),
正常路径零 nvidia-smi 查询. fake-gpu 模式 (SCHED_FAKE_GPUS) 全部短路.
"""

from __future__ import annotations

import os
import subprocess
from typing import Any

from .state import connect, get_gpu, now


class Allocator:
    def __init__(self, gpu_list: list[int], fake: bool = False):
        self.gpu_list = gpu_list  # 配置集 (D3: 实际可用集 = 配置集 - quarantine)
        self.fake = fake or bool(os.environ.get("SCHED_FAKE_GPUS"))
        if self.fake:
            # 模拟 GPU 数 (0,1,2,3 语义)
            n = len(os.environ.get("SCHED_FAKE_GPUS", "").split(","))
            self.gpu_list = list(range(n))

    # ---------- 物理探测 (仅边界校验) ----------

    def _compute_pids(self) -> list[int]:
        """nvidia-smi compute 进程列表 (M8 主判据). fake 模式返回空."""
        if self.fake:
            return []
        try:
            out = subprocess.run(
                ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if out.returncode != 0:
                return []
            return [int(l.strip()) for l in out.stdout.splitlines() if l.strip()]
        except (subprocess.SubprocessError, ValueError, FileNotFoundError):
            return []

    def _util(self, idx: int) -> int:
        """单卡 util (仅 unmanaged 抽查用). fake 返回 0."""
        if self.fake:
            return 0
        try:
            out = subprocess.run(
                [
                    "nvidia-smi",
                    f"--query-gpu=utilization.gpu",
                    "--format=csv,noheader,nounits",
                    "-i",
                    str(idx),
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            return int(out.stdout.strip().splitlines()[0])
        except (subprocess.SubprocessError, ValueError, IndexError, FileNotFoundError):
            return 0

    # ---------- 注册表状态机 (唯一权威) ----------

    def assign(self, job_id: str, want_idx: int | None = None) -> int | None:
        """派发: free 卡原子转 assigned. 返回分配到的卡号或 None."""
        with connect() as conn:
            # 可用集 = 配置集 - quarantine
            available = [
                i
                for i in self.gpu_list
                if not (
                    conn.execute(
                        "SELECT quarantined FROM gpus WHERE idx=?", (i,)
                    ).fetchone()
                    or [0]
                )[0]
            ]
            if want_idx is not None:
                cands = [want_idx] if want_idx in available else []
            else:
                cands = available
            for idx in cands:
                row = conn.execute(
                    "SELECT status FROM gpus WHERE idx=?", (idx,)
                ).fetchone()
                if row and row["status"] == "free":
                    conn.execute(
                        "UPDATE gpus SET status='assigned', job_id=?, updated_at=? WHERE idx=?",
                        (job_id, now(), idx),
                    )
                    return idx
        return None

    def release(self, job_id: str) -> None:
        """任务 reap 时: assigned -> releasing (立即, 不等物理)."""
        with connect() as conn:
            conn.execute(
                "UPDATE gpus SET status='releasing', updated_at=? WHERE job_id=?",
                (now(), job_id),
            )

    def settle_releasing(self) -> list[int]:
        """releasing 卡: compute 进程消失 -> free (连续 2 次采样, M7/M8).

        返回本轮转 free 的卡号. 5 分钟未释放 -> unmanaged (冷却上限).
        """
        import time as _t

        freed: list[int] = []
        with connect() as conn:
            rows = conn.execute(
                "SELECT idx, updated_at FROM gpus WHERE status='releasing'"
            ).fetchall()
            for row in rows:
                idx = row["idx"]
                has_proc = self._card_has_compute(idx)
                # 冷却起点 = updated_at (R6: daemon 重启不重置)
                try:
                    elapsed = _t.time() - _t.mktime(
                        _t.strptime(row["updated_at"], "%Y-%m-%d %H:%M:%S")
                    )
                except ValueError:
                    elapsed = 0
                if not has_proc:
                    if self._confirm_release(idx):
                        conn.execute(
                            "UPDATE gpus SET status='free', job_id=NULL, updated_at=? WHERE idx=?",
                            (now(), idx),
                        )
                        freed.append(idx)
                else:
                    self._reset_confirm(idx)  # 中间不干净: 中断连续计数
                    if elapsed > 300:
                        conn.execute(
                            "UPDATE gpus SET status='unmanaged', job_id=NULL, updated_at=? WHERE idx=?",
                            (now(), idx),
                        )
        return freed

    def _card_has_compute(self, idx: int) -> bool:
        """该卡是否有 compute 进程 (releasing 主判据, M8). fake 返回 False."""
        if self.fake:
            return False
        return self._util(idx) > 0

    def _confirm_release(self, idx: int) -> bool:
        """M7 冷却确认: 连续 2 次采样均无进程才转 free.

        计数文件存在 = 上次采样干净 -> 本次再干净 -> 连续 2 次成立.
        """
        if self.fake:
            return True
        marker = os.path.join(
            os.environ.get("SCHED_STATE", os.path.expanduser("~/.sched")),
            "release_confirm",
        )
        os.makedirs(marker, exist_ok=True)
        flag = os.path.join(marker, f"gpu{idx}")
        if os.path.exists(flag):
            os.unlink(flag)
            return True
        open(flag, "w").close()
        return False

    def _reset_confirm(self, idx: int) -> None:
        """中断连续计数 (采样到进程)."""
        if self.fake:
            return
        flag = os.path.join(
            os.environ.get("SCHED_STATE", os.path.expanduser("~/.sched")),
            "release_confirm",
            f"gpu{idx}",
        )
        if os.path.exists(flag):
            os.unlink(flag)

    def _confirm_occupied(self, idx: int) -> bool:
        """unmanaged 探测连续 2 次 (M7)."""
        if self.fake:
            return True
        marker = os.path.join(
            os.environ.get("SCHED_STATE", os.path.expanduser("~/.sched")),
            "occupied_confirm",
        )
        os.makedirs(marker, exist_ok=True)
        flag = os.path.join(marker, f"gpu{idx}")
        if os.path.exists(flag):
            os.unlink(flag)
            return True
        open(flag, "w").close()
        return False

    def probe_unmanaged(self) -> list[int]:
        """unmanaged 卡周期复查 (Q3 扩展): 物理真实空闲 (util==0) 连续 2 次采样
        -> 自动回 free, 无需人工 gpu-free.

        背景 (2026-08-15 排雷): 非 sched 外部进程占卡触发孤儿防线 (probe_free)
        误判为 unmanaged 后, 外部进程退出但状态不恢复, GPU 永久空置 -> 曾需人工
        sched gpu-free. 本函数让 unmanaged 卡在真实空闲后自动回到派发池.
        边界: quarantined 卡不自动恢复 (P2 用户显式标记, 需 gpu-ok 解除).
        """
        moved: list[int] = []
        with connect() as conn:
            rows = conn.execute(
                "SELECT idx FROM gpus WHERE status='unmanaged' AND quarantined=0"
            ).fetchall()
            for row in rows:
                idx = row["idx"]
                # 物理判据 (M8): util==0 视为无 compute 进程; fake 模式恒空闲 (验收用)
                if not self.fake and self._util(idx) != 0:
                    self._reset_confirm(idx)
                    continue
                # 与 settle_releasing 同确认 (M7): 连续 2 次采样才回 free (防抖动)
                if self._confirm_release(idx):
                    conn.execute(
                        "UPDATE gpus SET status='free', job_id=NULL, updated_at=? WHERE idx=?",
                        (now(), idx),
                    )
                    moved.append(idx)
        return moved

    def probe_free(self) -> list[int]:
        """free 卡抽查: 有 compute 进程 或 util>0 -> unmanaged (孤儿防线)."""
        if self.fake:
            return []
        moved: list[int] = []
        with connect() as conn:
            rows = conn.execute(
                "SELECT idx FROM gpus WHERE status='free'"
            ).fetchall()
            for row in rows:
                idx = row["idx"]
                if self._util(idx) > 0 or self._card_has_compute(idx):
                    # 连续 2 次 (M7): 这里用标记确认
                    if self._confirm_occupied(idx):
                        conn.execute(
                            "UPDATE gpus SET status='unmanaged', updated_at=? WHERE idx=?",
                            (now(), idx),
                        )
                        moved.append(idx)
        return moved

    def _confirm_occupied(self, idx: int) -> bool:
        marker = os.path.join(
            os.environ.get("SCHED_STATE", os.path.expanduser("~/.sched")),
            "occupied_confirm",
        )
        os.makedirs(marker, exist_ok=True)
        flag = os.path.join(marker, f"gpu{idx}")
        if os.path.exists(flag):
            os.unlink(flag)
            return True
        open(flag, "w").close()
        return False

    def available_gpus(self) -> list[int]:
        """当前 free 卡 (派发候选). 注册表权威."""
        with connect() as conn:
            rows = conn.execute(
                "SELECT idx FROM gpus WHERE status='free' AND quarantined=0"
            ).fetchall()
            return [r["idx"] for r in rows]
