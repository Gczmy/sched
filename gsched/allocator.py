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

from .state import connect, get_gpu, now, release_gpu


class Allocator:
    def __init__(
        self,
        gpu_list: list[int],
        fake: bool = False,
        mem_overrides: dict[int, float] | None = None,
    ):
        self.gpu_list = gpu_list  # 配置集 (D3: 实际可用集 = 配置集 - quarantine)
        self.mem_overrides = mem_overrides or {}  # config.gpus[{idx,mem_gib}] 手动覆盖
        self.fake = fake or bool(os.environ.get("SCHED_FAKE_GPUS"))
        self._uuid_map: dict[str, int] | None = None  # gpu_uuid->idx 缓存 (M8, 建一次)
        if self.fake:
            # 模拟 GPU 数 (0,1,2,3 语义); 支持 "idx:mem" 形式带容量 (GiB, 验收用)
            parts = [p for p in os.environ.get("SCHED_FAKE_GPUS", "").split(",") if p]
            idxs, self._fake_mem = [], {}
            for p in parts:
                if ":" in p:
                    i, m = p.split(":", 1)
                    idxs.append(int(i))
                    self._fake_mem[int(i)] = float(m)
                else:
                    idxs.append(int(p))
            if not idxs:
                idxs = [0]
            self.gpu_list = idxs
        elif not self.gpu_list:
            # 缺口 2 (2026-08-17): config 未配 gpus -> 自动探测全卡
            # (定案 1 第三级回退, 之前文档写了但代码没实现)
            self.gpu_list = self._detect_gpus()

    def _detect_gpus(self) -> list[int]:
        """自动探测全卡 (nvidia-smi -L). 无 nvidia-smi/失败 -> 空 (纯 CPU)."""
        try:
            out = subprocess.run(
                ["nvidia-smi", "-L"], capture_output=True, text=True, timeout=10,
            )
            idxs = []
            for line in out.stdout.splitlines():
                head = line.split(":", 1)[0].strip()
                if head.startswith("GPU ") and head[4:].isdigit():
                    idxs.append(int(head[4:]))
            return idxs
        except (subprocess.SubprocessError, ValueError, FileNotFoundError):
            return []

    # ---------- 容量 (定案 39 待定项 4: 容量来源 daemon 启动探测) ----------

    def probe_capacity(self) -> None:
        """daemon 启动时探测每卡总容量 (GiB) 缓存进 gpus.mem_total_gib.

        优先级 (2026-08-17 缺口 1): config.gpus[{idx,mem_gib}] 手动覆盖 >
        fake SCHED_FAKE_GPUS "idx:mem" > nvidia-smi 探测 (MiB/1024 = GiB).
        覆盖场景: 异构卡容量手动指定 / 无 nvidia-smi 环境 (容量进 DB 供装箱).
        """
        with connect() as conn:
            if self.mem_overrides:
                for idx, mem in self.mem_overrides.items():
                    conn.execute(
                        "UPDATE gpus SET mem_total_gib=? WHERE idx=?", (mem, idx)
                    )
            if self.fake:
                for idx in self.gpu_list:
                    mem = self.mem_overrides.get(idx, self._fake_mem.get(idx, 24.0))
                    conn.execute(
                        "UPDATE gpus SET mem_total_gib=? WHERE idx=?", (mem, idx)
                    )
                return
            try:
                out = subprocess.run(
                    ["nvidia-smi", "--query-gpu=index,memory.total",
                     "--format=csv,noheader,nounits"],
                    capture_output=True, text=True, timeout=10,
                )
                for line in out.stdout.splitlines():
                    parts = line.split(",")
                    if len(parts) != 2:
                        continue
                    idx, mi = int(parts[0].strip()), int(parts[1].strip())
                    if idx in self.gpu_list and idx not in self.mem_overrides:
                        conn.execute(
                            "UPDATE gpus SET mem_total_gib=? WHERE idx=?",
                            (round(mi / 1024.0, 1), idx),
                        )
            except (subprocess.SubprocessError, ValueError, FileNotFoundError):
                pass

    def mem_total(self, idx: int) -> float:
        """该卡总容量 (GiB); 未探测/缺失 -> 0 (调用方按无容量处理)."""
        with connect() as conn:
            row = conn.execute(
                "SELECT mem_total_gib FROM gpus WHERE idx=?", (idx,)
            ).fetchone()
        if row and row["mem_total_gib"]:
            return float(row["mem_total_gib"])
        return self._fake_mem.get(idx, 0.0) if self.fake else 0.0

    def vram_used(self, conn, idx: int) -> float:
        """该卡已装箱显存 (SUM gpu_jobs.vram_gib, 定案 39 L2). 共享装箱用."""
        row = conn.execute(
            "SELECT COALESCE(SUM(vram_gib), 0.0) AS s FROM gpu_jobs WHERE gpu_id=?",
            (idx,),
        ).fetchone()
        return float(row["s"])

    def job_count(self, conn, idx: int) -> int:
        """该卡当前 job 数 (co_locate_max_jobs 上限用)."""
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM gpu_jobs WHERE gpu_id=?", (idx,)
        ).fetchone()
        return int(row["n"])

    # ---------- 物理探测 (仅边界校验) ----------

    def _compute_pids_by_card(self) -> dict[int, list[int]] | None:
        """compute-apps 按卡列 pid (M8 主判据, 新建路径 §3.2e C).

        返回 {idx: [pid,...]}; None = 查询失败 (调用方回退 util==0 兜底).
        fake: SCHED_FAKE_COMPUTE_APPS="idx:pid1,pid2;idx:pid3" 模拟
        (空/未设置 -> {}, 即无进程).
        """
        if self.fake:
            out: dict[int, list[int]] = {}
            raw = os.environ.get("SCHED_FAKE_COMPUTE_APPS", "")
            for part in raw.split(";"):
                if not part.strip() or ":" not in part:
                    continue
                idx_s, pids_s = part.split(":", 1)
                try:
                    idx = int(idx_s.strip())
                except ValueError:
                    continue
                out[idx] = [
                    int(p.strip())
                    for p in pids_s.split(",")
                    if p.strip()
                ]
            return out
        try:
            out = subprocess.run(
                ["nvidia-smi", "--query-compute-apps=pid,gpu_uuid",
                 "--format=csv,noheader"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if out.returncode != 0:
                return None
            uuid_map = self._uuid_to_idx()
            by_card: dict[int, list[int]] = {}
            for line in out.stdout.splitlines():
                parts = line.split(",")
                if len(parts) != 2:
                    continue
                pid_s, uuid = parts[0].strip(), parts[1].strip()
                idx = uuid_map.get(uuid)
                if idx is not None:
                    by_card.setdefault(idx, []).append(int(pid_s))
            return by_card
        except (subprocess.SubprocessError, ValueError, FileNotFoundError):
            return None

    def _uuid_to_idx(self) -> dict[str, int]:
        """gpu_uuid -> idx 映射 (§3.2e C: compute-apps 返回 UUID 非 idx).

        --query-gpu=index,uuid 建一次 (daemon 启动时首次调用缓存).
        fake 返回空 (fake 路径不走 UUID).
        """
        if self.fake:
            return {}
        if self._uuid_map is None:
            self._uuid_map = {}
            try:
                out = subprocess.run(
                    ["nvidia-smi", "--query-gpu=index,uuid",
                     "--format=csv,noheader"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                for line in out.stdout.splitlines():
                    parts = line.split(",")
                    if len(parts) == 2:
                        try:
                            self._uuid_map[parts[1].strip()] = int(
                                parts[0].strip()
                            )
                        except ValueError:
                            pass
            except (subprocess.SubprocessError, ValueError, FileNotFoundError):
                pass
        return self._uuid_map

    def _pgid_of(self, pid: int) -> int | None:
        """pid -> pgid (os.getpgid, stdlib 同用户无 sudo 可行). 已死/权限 -> None.

        fake: pid 本身即 pgid (验收模拟, 与 SCHED_FAKE_COMPUTE_APPS 配合).
        """
        if self.fake:
            return pid
        try:
            return os.getpgid(pid)
        except (ProcessLookupError, PermissionError, OSError):
            return None

    def _known_job_pgids(self) -> set[int]:
        """jobs 表已知 pgid 集 (M8 归属判定: 该卡已知 job 的 pgid).

        计数释放保证 releasing 时卡上无框架 job, 残留进程来自刚结束的 job
        (jobs.pgid 保留, retry/resubmit 才清空).
        """
        with connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT pgid FROM jobs WHERE pgid IS NOT NULL"
            ).fetchall()
        return {int(r["pgid"]) for r in rows}

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
                    # 多归属: 写 gpu_jobs 行 (独占 = 每卡 1 行; gpus.job_id 镜像双写)
                    conn.execute(
                        "INSERT OR REPLACE INTO gpu_jobs (gpu_id, job_id, updated_at)"
                        " VALUES (?,?,?)",
                        (idx, job_id, now()),
                    )
                    return idx
        return None

    def release(self, job_id: str) -> None:
        """任务 reap 时: 多归属计数释放 (§3.2e B). 复用 state.release_gpu."""
        with connect() as conn:
            release_gpu(conn, job_id)

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
                        # 防御: releasing 卡应已无 gpu_jobs 行 (计数释放), 清残留
                        conn.execute(
                            "DELETE FROM gpu_jobs WHERE gpu_id=?", (idx,)
                        )
                        freed.append(idx)
                else:
                    self._reset_confirm(idx)  # 中间不干净: 中断连续计数
                    if elapsed > 300:
                        conn.execute(
                            "UPDATE gpus SET status='unmanaged', job_id=NULL, updated_at=? WHERE idx=?",
                            (now(), idx),
                        )
                        # 防御: unmanaged 卡不应有 gpu_jobs 行 (任务已离场)
                        conn.execute(
                            "DELETE FROM gpu_jobs WHERE gpu_id=?", (idx,)
                        )
        return freed

    def _card_has_compute(self, idx: int) -> bool:
        """M8 主判据: 该卡是否有未离场的 compute 进程 (§3.2e C).

        层次:
          1. compute-apps 按卡列 pid -> 逐 os.getpgid(pid) 对照 jobs.pgid:
             - 任一 pid 属于已知 job pgid -> 残留框架进程 -> True (等离场)
             - 无 pid / 全外部进程 -> False (干净)
          2. 兜底: compute-apps 查询失败 -> util==0 (现状语义)

        行为路径变化 (评审确认): 外部进程判干净 -> 回 free -> 同 tick 的
        probe_free 立即抓回 unmanaged (util>0), 无 free 窗口可派发 (dispatch
        在 probe_free 之后).
        """
        by_card = self._compute_pids_by_card()
        if by_card is None:
            return self._util(idx) > 0  # 兜底: 查询失败回退 util 判据
        pids = by_card.get(idx, [])
        if not pids:
            return False
        known = self._known_job_pgids()
        for pid in pids:
            pgid = self._pgid_of(pid)
            if pgid is not None and pgid in known:
                return True  # 残留框架进程: 等离场
        return False  # 全外部进程: 干净

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
                    # 防御: unmanaged 卡不应有 gpu_jobs 行
                    conn.execute(
                        "DELETE FROM gpu_jobs WHERE gpu_id=?", (idx,)
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
