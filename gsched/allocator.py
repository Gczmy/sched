"""allocator (文档 §5b B5 / M7 / M8 / §3.4b2 D3).

GPU 状态机 (注册表内维护, 唯一权威):
  free -> assigned(job) -> releasing -> free
                  └-> unmanaged (物理抽查发现外部占用/孤儿, 不派发)

物理校验只做两个边界 (releasing 归零确认 + free 的 unmanaged 抽查),
正常路径零 nvidia-smi 查询. fake-gpu 模式 (SCHED_FAKE_GPUS) 全部短路.
"""

from __future__ import annotations

import os
import json
import subprocess
from typing import Any

from .state import (
    connect,
    default_state_dir,
    ensure_private_directory,
    get_gpu,
    hostname,
    now,
    open_private_text,
    release_gpu,
)

_UNSET = object()  # P3: by_card 预取参数哨兵 (区分"未传"与"查询失败返回 None")

RELEASE_TIMEOUT_SEC = 300  # releasing 冷却上限 5 分钟 (B5, 原 dispatcher.py:28 死常量迁此)

# A compute-apps hit or an indeterminate physical probe is always fail-closed.
# Only a positive utilization sample with a complete, empty compute-apps probe
# is noisy enough to debounce.  Three daemon ticks (~20s from first to third at
# the default cadence) filters the observed idle-L4 1-2% spikes without making
# a sustained non-compute workload invisible forever.
FREE_UTIL_CONFIRM_SAMPLES = 3


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
        self._uuid_map: dict[str, int] | None = None  # 上次完整拓扑; 每次归属前重探
        # B26: GPU 健康自愈 —— nvidia-smi 连续异常计数 -> 自动熔断 (2026-08-26 幽灵卡事故)
        self._probe_fail_streak: dict[int, int] = {}
        self._auto_quarantine_threshold = 5  # 连续 5 次探测失败 (~50s) 即熔断
        # Util-only debounce is intentionally not a persistent GPU status.
        # While a streak is below threshold, suppress this card in every
        # allocation path until a clean physical sample clears it.
        self._dispatch_suppressed: set[int] = set()
        self._mem_cache: dict[int, float] = {}  # 容量进程内缓存 (P3: 静态值, 不重复开 DB 连接)
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
                    self._mem_cache[idx] = float(mem)
            if self.fake:
                for idx in self.gpu_list:
                    mem = self.mem_overrides.get(idx, self._fake_mem.get(idx, 24.0))
                    conn.execute(
                        "UPDATE gpus SET mem_total_gib=? WHERE idx=?", (mem, idx)
                    )
                    self._mem_cache[idx] = float(mem)
                return
            try:
                out = subprocess.run(
                    ["nvidia-smi", "--query-gpu=index,memory.total",
                     "--format=csv,noheader,nounits"],
                    capture_output=True, text=True, timeout=10,
                )
                if out.returncode != 0 or not (out.stdout or "").strip():
                    import sys as _sys
                    print(
                        f"[allocator] H3 nvidia-smi 容量探测失败 (rc={out.returncode}),"
                        " 保留现有显存/隔离状态, 跳过幽灵卡判定",
                        file=_sys.stderr,
                    )
                    return
                seen: set[int] = set()
                for line in out.stdout.splitlines():
                    parts = line.split(",")
                    if len(parts) != 2:
                        continue
                    idx, mi = int(parts[0].strip()), int(parts[1].strip())
                    seen.add(idx)
                    if idx in self.gpu_list and idx not in self.mem_overrides:
                        gib = round(mi / 1024.0, 1)
                        conn.execute(
                            "UPDATE gpus SET mem_total_gib=? WHERE idx=?",
                            (gib, idx),
                        )
                        self._mem_cache[idx] = gib
                # B26: 幽灵卡检测 —— config 声明但 nvidia-smi 未见 = 物理缺失,
                # 立即熔断防派发 (2026-08-26 GPU3 掉线仍显示 free 的事故)
                for ghost in set(self.gpu_list) - seen:
                    conn.execute(
                        "UPDATE gpus SET quarantined=1, updated_at=? WHERE idx=? AND quarantined=0",
                        (__import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"), ghost),
                    )
                    conn.execute(
                        "INSERT INTO incidents (ts, kind, gpu_idx, job_id, batch_id, payload)"
                        " VALUES (?, 'gpu_ghost', ?, NULL, NULL, ?)",
                        (
                            __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            ghost,
                            json.dumps({
                                "verdicts": [
                                    "config.gpus 声明该卡但 nvidia-smi 未列出 —— 物理掉线/驱动异常;",
                                    "已自动熔断禁止派发; 硬件恢复后 sched gpu-ok 解除",
                                ],
                            }, ensure_ascii=False),
                        ),
                    )
            except (subprocess.SubprocessError, ValueError, FileNotFoundError):
                pass

    def mem_total(self, idx: int) -> float:
        """该卡总容量 (GiB); 未探测/缺失 -> 0 (调用方按无容量处理).

        P3: 进程内缓存 —— 容量是启动探测的静态值, 原实现在装箱热路径
        (_assign_in_tx 逐卡逐任务) 每次新开独立 DB 连接。
        gpu-set-mem 的外部修改需重启 daemon 生效 (与探测覆盖语义一致)。
        """
        if idx in self._mem_cache:
            return self._mem_cache[idx]
        with connect() as conn:
            row = conn.execute(
                "SELECT mem_total_gib FROM gpus WHERE idx=?", (idx,)
            ).fetchone()
        if row and row["mem_total_gib"]:
            val = float(row["mem_total_gib"])
        else:
            val = self._fake_mem.get(idx, 0.0) if self.fake else 0.0
        self._mem_cache[idx] = val
        return val

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

    def phys_mem_used_gib(self, idx: int) -> float | None:
        """该卡物理已用显存 (GiB); OOM 快照用. 查询失败/fake -> None."""
        if self.fake:
            return None
        try:
            out = subprocess.run(
                ["nvidia-smi", "--query-gpu=index,memory.used",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=10,
            )
            if out.returncode != 0:
                return None
            for line in out.stdout.splitlines():
                parts = line.split(",")
                if len(parts) != 2:
                    continue
                if int(parts[0].strip()) == idx:
                    return round(int(parts[1].strip()) / 1024.0, 2)
        except (subprocess.SubprocessError, ValueError, FileNotFoundError):
            pass
        return None

    def incident_external_pids(self, idx: int) -> tuple[list[dict], bool]:
        """OOM 快照用: 该卡上不属于框架已知进程组的 compute 进程 (调研 F2 §2.3).

        判定: pid 的 pgid 不在已知集合 (running + 近 10min 终态, 见
        _known_job_pgids) -> 外部. 已知近似: 同用户子进程若独立成组会被误判,
        文档标注, 不追求完美.
        返回 ([{pid, mem_mib?}, ...], degraded) —— degraded=True 表示查询失败
        (此时外部进程可能存在但不可见).
        """
        by_card = self._compute_pids_by_card()
        if by_card is None:
            return [], True  # 查询失败 -> degraded
        try:
            known = self._known_job_pgids()
        except Exception:  # noqa: BLE001 — 无表新库等边缘: 视为无已知进程
            known = set()
        # per-pid 显存: 一次查询建 {pid: mem_mib}; 失败不致命 (mem 缺省)
        mem_by_pid: dict[int, int] = {}
        if not self.fake:
            try:
                out = subprocess.run(
                    ["nvidia-smi", "--query-compute-apps=pid,used_memory,gpu_uuid",
                     "--format=csv,noheader,nounits"],
                    capture_output=True, text=True, timeout=10,
                )
                uuid_map = self._uuid_to_idx() if out.returncode == 0 else None
                if uuid_map is not None:
                    for line in out.stdout.splitlines():
                        parts = [x.strip() for x in line.split(",")]
                        if len(parts) != 3:
                            continue
                        pid_i = int(parts[0])
                        if uuid_map.get(parts[2]) == idx:
                            mem_by_pid[pid_i] = int(parts[1])
            except (subprocess.SubprocessError, ValueError, FileNotFoundError):
                pass
        ext: list[dict] = []
        for pid in by_card.get(idx, []):
            pgid = self._pgid_of(pid)
            if pgid is not None and pgid in known:
                continue  # 我们自己的 (含近 10min 终态残留)
            d: dict = {"pid": pid}
            if pid in mem_by_pid:
                d["mem_mib"] = mem_by_pid[pid]
            ext.append(d)
        return ext, False

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
            topology_before = self._uuid_to_idx()
            if topology_before is None:
                return None
            out = subprocess.run(
                ["nvidia-smi", "--query-compute-apps=pid,gpu_uuid",
                 "--format=csv,noheader"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if out.returncode != 0:
                return None
            topology_after = self._uuid_to_idx()
            if topology_after is None or topology_after != topology_before:
                return None
            lines = [line for line in out.stdout.splitlines() if line.strip()]
            if not lines:
                return {}
            by_card: dict[int, list[int]] = {}
            for line in lines:
                parts = line.split(",")
                if len(parts) != 2:
                    return None
                pid_s, uuid = parts[0].strip(), parts[1].strip()
                idx = topology_before.get(uuid)
                if idx is None:
                    # A complete topology was bracketed around the compute
                    # sample. Any other UUID makes attribution indeterminate.
                    return None
                by_card.setdefault(idx, []).append(int(pid_s))
            return by_card
        except (subprocess.SubprocessError, ValueError, FileNotFoundError):
            return None

    def _uuid_to_idx(self) -> dict[str, int] | None:
        """Re-probe a complete topology before attributing any compute PID.

        A successful probe replaces the cached map as one unit. If topology
        changed since the preceding sample, discard this attribution cycle:
        the compute-app and topology queries may straddle a reconfiguration.
        """
        if self.fake:
            return {}
        try:
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=index,uuid",
                 "--format=csv,noheader"],
                capture_output=True,
                text=True,
                timeout=10,
            )
        except (subprocess.SubprocessError, FileNotFoundError):
            return None
        if result.returncode != 0:
            return None

        mapping: dict[str, int] = {}
        indices: set[int] = set()
        try:
            lines = [line for line in result.stdout.splitlines() if line.strip()]
            if not lines:
                return None
            for line in lines:
                parts = line.split(",")
                if len(parts) != 2:
                    return None
                idx = int(parts[0].strip())
                uuid = parts[1].strip()
                if not uuid or uuid in mapping or idx in indices:
                    return None
                mapping[uuid] = idx
                indices.add(idx)
        except ValueError:
            return None
        configured = set(getattr(self, "gpu_list", ()) or ())
        if configured and not configured.issubset(indices):
            return None

        previous = getattr(self, "_uuid_map", None)
        self._uuid_map = mapping
        if previous is not None and previous != mapping:
            return None
        return mapping

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
        (jobs.pgid 保留, retry/resubmit 才清空) —— 因此必须覆盖近期终态 job。

        决策 3A 落地变体: 原决策字面"只查 running"会让 settle_releasing 把
        刚结束 job 的残留进程误判为外部 -> 回 free -> probe_free 立即抓回
        unmanaged (正常退出路径抖动)。改为 "running + 近 10 分钟终态" 有界
        窗口: 覆盖进程优雅退出期 (远小于 releasing 5min 冷却), 又避免全表
        DISTINCT 永久累积导致 OS 复用旧 pgid 后的误判 (原审查问题)。
        """
        import time as _t

        cutoff = _t.strftime(
            "%Y-%m-%d %H:%M:%S", _t.localtime(_t.time() - 600)
        )
        with connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT pgid FROM jobs WHERE pgid IS NOT NULL"
                " AND (status='running' OR finished_at >= ?)",
                (cutoff,),
            ).fetchall()
        return {int(r["pgid"]) for r in rows}

    def _util(self, idx: int) -> int:
        """单卡 util. fake 返回 0; 查询失败返回 0 (旧语义, settle_releasing 兜底用)."""
        u = self._util_opt(idx)
        return u if u is not None else 0

    def _util_opt(self, idx: int) -> int | None:
        """单卡 util; 查询失败 -> None (审查 M8: probe 路径 fail-closed 用)."""
        if self.fake:
            return 0
        try:
            out = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=utilization.gpu",   # D2: 无占位 f-string -> 普通字符串
                    "--format=csv,noheader,nounits",
                    "-i",
                    str(idx),
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if out.returncode != 0:
                self._note_probe_fail(idx)
                return None
            val = int(out.stdout.strip().splitlines()[0])
            self._note_probe_ok(idx)
            return val
        except (subprocess.SubprocessError, ValueError, IndexError, FileNotFoundError):
            self._note_probe_fail(idx)
            return None

    # ---------- 注册表状态机 (唯一权威) ----------

    def is_dispatch_suppressed(self, idx: int) -> bool:
        """Whether a noisy free-card sample suppresses allocation for now.

        ``vars`` keeps this safe for narrow ``Allocator.__new__`` test doubles
        created before all runtime fields are initialized.
        """
        return idx in vars(self).get("_dispatch_suppressed", ())

    def _suppress_dispatch(self, idx: int) -> None:
        vars(self).setdefault("_dispatch_suppressed", set()).add(idx)

    def _allow_dispatch(self, idx: int) -> None:
        vars(self).setdefault("_dispatch_suppressed", set()).discard(idx)

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
                if self.is_dispatch_suppressed(idx):
                    continue
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

    def settle_releasing(self) -> tuple[list[int], list[int]]:
        """releasing 卡: compute 进程消失 -> free (连续 2 次采样, M7/M8).

        返回 (freed, timeout) 两列表: freed = 本轮转 free 的卡号;
        timeout = 冷却上限 (5min) 到期仍被占的卡号 (已转 unmanaged) ——
        调用方 (dispatcher) 据此输出 nvidia-smi 进程列表诊断, 引导人工清理
        (事故记录 4 修复建议 3).
        """
        import time as _t

        freed: list[int] = []
        timeout: list[int] = []
        with connect() as conn:
            rows = conn.execute(
                "SELECT idx, updated_at FROM gpus WHERE status='releasing'"
            ).fetchall()
            # P3: 循环外预取一次 compute-apps 和已知 pgid, 逐卡分发
            # (每卡各查一次 = 2N 次子进程/全表扫描, N=卡数)
            by_card = self._compute_pids_by_card() if rows else {}
            try:
                known = self._known_job_pgids() if rows else set()
            except Exception:
                known = None
            for row in rows:
                idx = row["idx"]
                pids = None if by_card is None else by_card.get(idx, [])
                try:
                    elapsed = _t.time() - _t.mktime(
                        _t.strptime(row["updated_at"], "%Y-%m-%d %H:%M:%S")
                    )
                except (TypeError, ValueError):
                    elapsed = 0

                if pids is None:
                    # An indeterminate physical probe is not a clean sample.
                    self._reset_release_confirm(idx)
                    if elapsed > RELEASE_TIMEOUT_SEC:
                        self._set_unmanaged(conn, idx)
                        timeout.append(idx)
                    continue
                if pids:
                    self._reset_release_confirm(idx)
                    external_or_unknown = known is None or any(
                        (pgid := self._pgid_of(pid)) is None or pgid not in known
                        for pid in pids
                    )
                    if external_or_unknown:
                        # Scheduler ownership has ended; an external/unknown
                        # compute process makes this card unmanaged immediately.
                        self._set_unmanaged(conn, idx)
                    elif elapsed > RELEASE_TIMEOUT_SEC:
                        self._set_unmanaged(conn, idx)
                        timeout.append(idx)
                    continue
                if self._confirm_release(idx):
                    conn.execute(
                        "UPDATE gpus SET status='free', job_id=NULL, updated_at=? WHERE idx=?",
                        (now(), idx),
                    )
                    conn.execute("DELETE FROM gpu_jobs WHERE gpu_id=?", (idx,))
                    freed.append(idx)
        return freed, timeout

    def _set_unmanaged(self, conn, idx: int) -> None:
        conn.execute(
            "UPDATE gpus SET status='unmanaged', job_id=NULL, updated_at=? WHERE idx=?",
            (now(), idx),
        )
        conn.execute("DELETE FROM gpu_jobs WHERE gpu_id=?", (idx,))

    def _card_any_occupied(self, idx: int, by_card: Any = _UNSET) -> bool | None:
        """probe 占用判据 (审查 M7): 有 compute 进程 (不分归属) 或 util>0 -> True.

        与 _card_has_compute 的区别: 不做 pgid 归属判定 —— 外部进程驻留显存
        即使 util=0 也判占用, 否则 probe_free 永远抓不出 "驻留但空闲" 的外部
        进程, 派发新任务上卡会显存冲突 (归属判定只用于 settle_releasing 等离场)。
        None = 查询失败 (M8 fail-closed: releasing 不计干净样本，free 卡转 unmanaged).

        by_card 可传入本轮预取结果；None 表示查询失败。
        """
        sample = self._card_occupancy_sample(idx, by_card)
        if sample == "indeterminate":
            return None
        return sample != "clean"

    def _card_occupancy_sample(
        self, idx: int, by_card: Any = _UNSET
    ) -> str:
        """Classify one physical occupancy sample without losing its cause.

        Results are ``compute``, ``util``, ``clean``, or ``indeterminate``.
        A complete compute-apps probe is evaluated before utilization so probe
        uncertainty can never be mistaken for a debouncable utilization-only
        sample.  ``util`` therefore means exactly: topology/compute probe was
        complete, no compute PID was present, and utilization was positive.
        """
        if by_card is _UNSET:
            by_card = self._compute_pids_by_card()
        if by_card is None:
            return "indeterminate"
        if by_card.get(idx):
            return "compute"
        if self.fake:
            return "clean"
        util = self._util_opt(idx)
        if util is None:
            return "indeterminate"
        return "util" if util > 0 else "clean"

    def _card_has_compute(
        self, idx: int, by_card: Any = _UNSET, known: set[int] | None = None
    ) -> bool | None:
        """Return physical compute occupancy; None means the probe is indeterminate."""
        if by_card is _UNSET:
            by_card = self._compute_pids_by_card()
        if by_card is None:
            return None
        return bool(by_card.get(idx))

    def _confirm_flag(self, kind: str, idx: int) -> str:
        """连续采样确认标记路径 (决策 5B: 按节点隔离 + 统一走 state 路径函数,
        不再手写 os.environ["SCHED_STATE"] 默认值)."""
        return os.path.join(
            default_state_dir(), hostname(), kind, f"gpu{idx}"
        )

    def _confirm_release(self, idx: int) -> bool:
        """M7 冷却确认: 连续 2 次采样均无进程才转 free.

        计数文件存在 = 上次采样干净 -> 本次再干净 -> 连续 2 次成立.
        """
        if self.fake:
            return True
        flag = self._confirm_flag("release_confirm", idx)
        ensure_private_directory(os.path.dirname(flag))
        if os.path.exists(flag):
            os.unlink(flag)
            return True
        open_private_text(flag, "w").close()
        return False

    def _reset_release_confirm(self, idx: int) -> None:
        """中断连续计数 (采样到进程)."""
        if self.fake:
            return
        flag = self._confirm_flag("release_confirm", idx)
        if os.path.exists(flag):
            os.unlink(flag)

    def _confirm_unmanaged(self, idx: int) -> bool:
        """unmanaged 卡连续 2 次干净采样确认."""
        if self.fake:
            return True
        flag = self._confirm_flag("unmanaged_confirm", idx)
        ensure_private_directory(os.path.dirname(flag))
        if os.path.exists(flag):
            os.unlink(flag)
            return True
        open_private_text(flag, "w").close()
        return False

    def _reset_unmanaged_confirm(self, idx: int) -> None:
        """中断 unmanaged 连续干净采样计数."""
        if self.fake:
            return
        flag = self._confirm_flag("unmanaged_confirm", idx)
        if os.path.exists(flag):
            os.unlink(flag)

    def _confirm_free_util_occupied(self, idx: int) -> bool:
        """Confirm consecutive utilization-only positives for a free card.

        One private ``O_EXCL`` marker represents each pre-confirmation sample.
        The markers are deliberately left saturated once the threshold is met:
        if the following DB update cannot commit, the next positive sample is
        still confirmed instead of reopening a dispatchable window.  A clean
        physical sample resets the sequence.
        """
        base = self._confirm_flag("free_util_confirm", idx)
        ensure_private_directory(os.path.dirname(base))
        for sample_no in range(1, FREE_UTIL_CONFIRM_SAMPLES):
            flag = f"{base}.{sample_no}"
            try:
                open_private_text(flag, "x").close()
            except FileExistsError:
                continue
            return False
        return True

    def _reset_free_util_confirm(self, idx: int) -> None:
        """Reset the utilization-only streak after any clean sample."""
        base = self._confirm_flag("free_util_confirm", idx)
        for sample_no in range(1, FREE_UTIL_CONFIRM_SAMPLES):
            try:
                os.unlink(f"{base}.{sample_no}")
            except FileNotFoundError:
                pass

    # ── B26: GPU 健康自愈 (2026-08-26 幽灵卡事故: nvidia-smi 对掉线卡挂起) ──

    def _note_probe_fail(self, idx: int) -> None:
        """记录一次探测失败; 连续超阈值 -> 自动熔断该卡 (写 quarantined)."""
        if self.fake:
            return
        try:
            streak = self._probe_fail_streak.get(idx, 0) + 1
            self._probe_fail_streak[idx] = streak
            if streak < self._auto_quarantine_threshold:
                return
            self._probe_fail_streak[idx] = 0  # 只熔断一次; 恢复走 gpu-ok 人工通道
            with connect() as conn:
                conn.execute(
                    "UPDATE gpus SET quarantined=1, updated_at=? WHERE idx=? AND quarantined=0",
                    (__import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"), idx),
                )
                conn.execute(
                    "INSERT INTO incidents (ts, kind, gpu_idx, job_id, batch_id, payload)"
                    " VALUES (?, 'gpu_probe_failed', ?, NULL, NULL, ?)",
                    (
                        __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        idx,
                        json.dumps({
                            "streak": self._auto_quarantine_threshold,
                            "verdicts": ["nvidia-smi 连续异常 -> 自动隔离; 排查硬件后 sched gpu-ok 解除"],
                        }, ensure_ascii=False),
                    ),
                )
                import sys as _sys
                print(f"[allocator] B26 GPU{idx} 连续 {self._auto_quarantine_threshold} 次探测失败 -> 自动熔断",
                      file=_sys.stderr)
        except Exception:
            pass  # 自愈路径绝不反噬主循环

    def _note_probe_ok(self, idx: int) -> None:
        """采样成功 -> 清零失败计数."""
        self._probe_fail_streak.pop(idx, None)


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
            # P3: 循环外预取一次 compute-apps 逐卡分发
            by_card = self._compute_pids_by_card() if rows else None
            for row in rows:
                idx = row["idx"]
                # 物理判据 (审查 M7/M8): 有 compute 进程 (不分归属) 或 util>0
                # 判占用 —— 外部进程驻留显存但 util=0 也不能回 free;
                # occ=None (nvidia-smi 故障) -> fail-closed 保持 unmanaged
                occ = self._card_any_occupied(idx, by_card)
                if occ is None or occ:
                    self._reset_unmanaged_confirm(idx)
                    continue
                # A genuinely clean sample also invalidates any saturated
                # util-only sequence which originally moved the card here.
                self._allow_dispatch(idx)
                self._reset_free_util_confirm(idx)
                # 与 settle_releasing 同确认 (M7): 连续 2 次采样才回 free (防抖动)
                if self._confirm_unmanaged(idx):
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
        """Remove physically occupied or indeterminate cards from the free pool.

        Compute PIDs and indeterminate probes move immediately (fail-closed).
        Only utilization without a compute PID is debounced, because idle GPUs
        can report short 1-2% utilization spikes.  On the confirming sample the
        DB transition is completed before this method returns, and dispatcher
        calls this method before selecting any GPU for launch.
        """
        if self.fake:
            return []
        moved: list[int] = []
        with connect() as conn:
            rows = conn.execute(
                "SELECT idx FROM gpus WHERE status='free'"
            ).fetchall()
            by_card = self._compute_pids_by_card() if rows else {}
            for row in rows:
                idx = row["idx"]
                sample = self._card_occupancy_sample(idx, by_card)
                if sample == "clean":
                    self._allow_dispatch(idx)
                    self._reset_free_util_confirm(idx)
                    continue
                self._suppress_dispatch(idx)
                if sample == "util":
                    if not self._confirm_free_util_occupied(idx):
                        continue
                # There must never be a dispatchable confirmation window:
                # compute occupancy and probe uncertainty fail closed on their
                # first sample; sustained util-only occupancy does so on its
                # confirming sample, before dispatcher selects a card.
                if sample != "util":
                    self._reset_free_util_confirm(idx)
                if sample in {"compute", "indeterminate", "util"}:
                    self._set_unmanaged(conn, idx)
                    moved.append(idx)
        return moved

    def available_gpus(self) -> list[int]:
        """当前 free 卡 (派发候选). 注册表权威."""
        with connect() as conn:
            rows = conn.execute(
                "SELECT idx FROM gpus WHERE status='free' AND quarantined=0"
            ).fetchall()
            return [
                r["idx"]
                for r in rows
                if not self.is_dispatch_suppressed(r["idx"])
            ]
