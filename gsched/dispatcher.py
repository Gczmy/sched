"""dispatcher (文档 §3.1 主循环 / §3.2b 崩溃接管 / §3.2c 节点重启 / §2.4 依赖解锁).

每轮固定 sleep (POLL_SEC=10s, 零忙轮询):
  reap_finished_jobs -> settle_releasing -> probe_free_gpus
  -> unlock_dependent_batches -> 贪心派发 (只派 free 卡)

单实例: PID 文件 + 心跳 mtime 双校验 (B11 F3/F4).
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from datetime import datetime

from . import state
from .allocator import Allocator
from .executor import Executor, pid_cmdline_matches
from .fingerprint import compute_fingerprint
from .config import resolve_template

POLL_SEC = 10
HEARTBEAT_SEC = 30
# RELEASE_TIMEOUT_SEC 迁至 allocator.py (原处为死常量, 消费方在 settle_releasing)
DEFAULT_MAX_RETRY = 1
RETRY_BACKOFF_SEC = 30  # 失败重试退避 (M2): 防秒级崩溃任务紧密崩溃循环
DEFAULT_GPU_JOB_CPUS = 8  # GPU 任务默认 CPU 占用 (NN 训练数据加载也要 CPU, config gpu_job_cpus 可覆盖)
DEFAULT_MAX_CPU_JOBS = 2  # cpus_total 未配置时回退: CPU-only 并发上限 (定案 7 旧语义)
DEFAULT_IDLE_TIMEOUT_MIN = 360  # 空转自动退出 (定案 38): 默认 6h, 0 = 禁用


class Dispatcher:
    def __init__(self, cfg: dict, fake: bool = False):
        self.cfg = cfg
        self.fake = fake
        self.state_dir = state.default_state_dir()
        self.host_dir = os.path.join(self.state_dir, state.hostname())
        os.makedirs(self.host_dir, exist_ok=True)
        self.pid_file = os.path.join(self.host_dir, "daemon.pid")
        self.heartbeat_file = os.path.join(self.host_dir, "daemon.heartbeat")
        self._prev_hb_ts: float | None = None  # acquire_lock 触心跳前采样 (D4 用)
        self._probe_offsets: dict[str, int] = {}  # job_id -> 日志已扫字节偏移 (P2)
        self.lock_dir = os.path.join(self.host_dir, "dispatcher.lock")
        self.log = open(
            os.path.join(self.host_dir, "scheduler.log"), "a", encoding="utf-8"
        )
        self.executor = Executor()
        # gpus 归一化 (2026-08-17 缺口 1/2): 纯卡号数组或 {idx,mem_gib} 对象数组;
        # config 未配 -> Allocator 自动探测全卡 (定案 1 第三级回退)
        from .config import parse_gpus

        gpu_list, mem_overrides = parse_gpus(cfg)
        self.allocator = Allocator(
            gpu_list=gpu_list, fake=fake, mem_overrides=mem_overrides,
        )
        # venv 路径映射 (指纹用)
        self.venv_paths = cfg.get("venvs", {})
        # 空转自动退出 (定案 38): 默认 360min (6h), 0 = 禁用; last_activity 内存态,
        # 重启重置; 崩溃循环 (反复拉起又立即崩) 永不 idle 退出为已知取舍
        self.idle_timeout_min = int(cfg.get("idle_timeout_min", DEFAULT_IDLE_TIMEOUT_MIN))
        self.last_activity = time.time()
        # co-location L3 冻结 (定案 39): 卡显存 > freeze_pct 冻结不再 pack.
        # 内存态 (仅运行时保护, 重启重置); 仅 co_locate 开启时启用, 60s 采样一次.
        self._frozen_gpus: set[int] = set()
        self._last_freeze_sample = 0.0
        self._co_locate = bool(cfg.get("co_locate", False))
        # 审查 B1: 优雅停止请求标志 (信号处理器只置此标志, 主循环 tick 边界消费).
        # 内存态 (无跨进程语义), SIGTERM/SIGINT handler 调用 request_stop().
        self._stop_requested = False

    def request_stop(self) -> None:
        """信号处理器入口: 只置标志 (绝不在 handler 里开 DB 连接)."""
        self._stop_requested = True

    def log_line(self, msg: str) -> None:
        line = f"[{state.now()}] {msg}"
        print(line, flush=True)
        self.log.write(line + "\n")
        self.log.flush()

    # ---------- 单实例锁 ----------

    def acquire_lock(self) -> bool:
        """B11 F3/F4: PID 文件 + 心跳 mtime 双校验; 崩溃残留先杀旧进程再清锁."""
        if self._is_running():
            return False
        # 清理残留锁
        if os.path.isdir(self.lock_dir):
            pid = self._read_pid()
            if pid and self._pid_exists(pid) and not self._heartbeat_fresh():
                # M4: kill 前身份校验 —— pid 文件残留 + PID 复用时凭数字发
                # 信号会误杀无关进程; cmdline 不含 gsched 则只清锁不杀
                if not pid_cmdline_matches(pid, "gsched"):
                    self.log_line(
                        f"F4: 残留 pid={pid} cmdline 非 gsched (PID 复用?), 只清锁不 kill"
                    )
                else:
                    # 卡死场景 (O2): 先杀再清锁
                    self.log_line(f"F4: 检测到卡死 daemon pid={pid}, SIGTERM -> 5s -> SIGKILL")
                    try:
                        os.kill(pid, signal.SIGTERM)
                        time.sleep(5)
                        if self._pid_exists(pid):
                            os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            self._cleanup_lock()
        try:
            os.makedirs(self.lock_dir)
        except FileExistsError:
            return False
        with open(self.pid_file, "w") as f:
            f.write(str(os.getpid()))
        # H2 修复: 触心跳前采样旧 mtime 供 _check_node_restart 用;
        # 否则 touch 后 hb_ts≈now > boot_ts, D4 节点重启检测恒不触发
        try:
            self._prev_hb_ts = os.path.getmtime(self.heartbeat_file)
        except OSError:
            self._prev_hb_ts = None
        self._touch_heartbeat()
        return True

    def _is_running(self) -> bool:
        return self._pid_exists(self._read_pid()) and self._heartbeat_fresh()

    def _read_pid(self) -> int | None:
        try:
            with open(self.pid_file) as f:
                return int(f.read().strip())
        except (OSError, ValueError):
            return None

    def _pid_exists(self, pid: int | None) -> bool:
        if not pid:
            return False
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    def _heartbeat_fresh(self) -> bool:
        try:
            age = time.time() - os.path.getmtime(self.heartbeat_file)
            return age < 2 * HEARTBEAT_SEC  # 阈值必须 > 心跳间隔 (F3)
        except OSError:
            return False

    def _touch_heartbeat(self) -> None:
        open(self.heartbeat_file, "a").close()
        os.utime(self.heartbeat_file, None)

    def _cleanup_lock(self) -> None:
        if os.path.isdir(self.lock_dir):
            try:
                os.rmdir(self.lock_dir)
            except OSError:
                pass
        if os.path.exists(self.pid_file):
            try:
                os.unlink(self.pid_file)
            except OSError:
                pass
        # 定案 38 "优雅退出 = 停心跳+清锁" (2026-08-16 修): idle 退出/stop 必须删
        # heartbeat 文件, 否则 is_running() 看 mtime<60s 仍判 alive -> submit 不
        # 触发拉起 (6b 场景: 任务 pending 无人派发). 崩溃路径不删 (60s 自然过期).
        if os.path.exists(self.heartbeat_file):
            try:
                os.unlink(self.heartbeat_file)
            except OSError:
                pass

    def stop(self) -> None:
        """daemon stop: 未完成任务标 cancelled 收尾 (N11).

        审查 B1: 主循环 tick 边界调用 (任何 connect() 块之外), 不再由信号
        handler 嵌套调用; 锁冲突 (database is locked) 短暂重试兜底, 防与
        同轮其他连接竞争。幂等: 重复调用无害 (无 running 任务则空转)。
        """
        import sqlite3 as _sq

        for attempt in range(3):
            try:
                with state.connect() as conn:
                    rows = conn.execute(
                        "SELECT * FROM jobs WHERE status='running'"
                    ).fetchall()
                    for j in rows:
                        if j["pgid"]:
                            self.executor.kill_pgid(j["pgid"])
                        state.update_job(
                            conn, j["id"], status="cancelled", kill_reason="cancelled",
                            finished_at=state.now(),
                        )
                        # N11 收尾 bug 修复 (2026-08-15 排雷): kill 后必须释放占用卡
                        # (assigned -> releasing), 否则 cancelled 任务残留 assigned
                        # 卡 -> daemon 重启后 GPU 永久不可用 (本次事故根因之一)
                        # 多归属 (定案 36 + §3.2e B): 计数释放, co-tenant 不误杀
                        if j["gpu"] is not None:
                            self._release_in_tx(conn, j["id"])
                break
            except _sq.OperationalError as e:
                if attempt == 2 or "locked" not in str(e):
                    self.log_line(f"stop 收尾失败: {e}")
                    break
                time.sleep(1)
        self._cleanup_lock()

    # ---------- 主循环 ----------

    def run(self, once: bool = False) -> None:
        # GPU 表初始化 (配置集 -> free; 幂等)
        with state.connect() as conn:
            state.init_gpus(conn, self.allocator.gpu_list)
        # 容量探测 (定案 39: daemon 启动时缓存每卡 GiB 容量, 装箱用)
        self.allocator.probe_capacity()
        if not self.fake:
            self._check_node_restart()
        self.log_line(
            f"dispatcher 启动 (pid={os.getpid()}, fake={self.fake}, gpus={self.allocator.gpu_list})"
        )
        # 接管: running 任务 pgid 存活则继续等 (A3/3.2b)
        self._adopt_running()

        tick_failures = 0
        while True:
            try:
                self._heartbeat()
                if self._stop_requested:
                    self.log_line("收到停止请求 (tick 边界), 收尾未完成任务")
                    self.stop()
                    break
                if self._idle_check():
                    break
                self._tick()
                tick_failures = 0
            except KeyboardInterrupt:
                break
            except Exception as e:
                tick_failures += 1
                self.log_line(f"tick 异常 (连续 {tick_failures} 次): {e}")
                # M11: tick 持续异常 (DB 损坏/磁盘满等) 时心跳照更会骗过看门狗
                # —— status 显示运行中但调度停摆。连续 5 次退出并清锁/停心跳,
                # 让外部判死并可重新拉起
                if tick_failures >= 5:
                    self.log_line("tick 连续 5 次异常, 退出 (停心跳让外部判死)")
                    break
            if once:
                break
            # B1: 可中断 sleep —— time.sleep(POLL_SEC) 被信号打断后 PEP 475 自动
            # 续睡, SIGTERM 要等满整轮才被响应 (daemon stop CLI 10s 超时 SIGKILL,
            # 任务没收尾). 拆成 1s 片逐片查停止标志, 停止延迟 ≤1s.
            for _ in range(POLL_SEC):
                if self._stop_requested:
                    break
                time.sleep(1)
        self._cleanup_lock()

    def _idle_check(self) -> bool:
        """定案 38: 连续 idle 超时优雅退出.

        idle = jobs 表无 pending/running/waiting_dep 任务 (blocked/failed/cancelled
        等人工态不计 activity —— 批次 blocked 时 daemon 不派发, 空转无意义).
        返回 True = 触发退出 (主循环 break, 随后 _cleanup_lock).
        """
        if self.idle_timeout_min <= 0:
            return False  # 0 = 禁用
        with state.connect() as conn:
            n = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE status IN ('pending','running','waiting_dep')"
            ).fetchone()[0]
        now = time.time()
        if n > 0:
            self.last_activity = now
            return False
        if now - self.last_activity >= self.idle_timeout_min * 60:
            self.log_line(
                f"连续 {self.idle_timeout_min}min 无任务 (idle_timeout_min), 自动退出"
            )
            return True
        return False

    def _l3_freeze_sample(self) -> None:
        """L3 运行时保护 (定案 39): 每 60s 采样 assigned 卡显存, > freeze_pct 冻结.

        fake 模式: 用 gpu_jobs SUM(vram_gib) 模拟已占显存 (真实环境测不到).
        冻结只影响共享装箱 (pack), 独占任务不受影响 (free 卡照派).
        """
        if not self._co_locate:
            return
        now_t = time.time()
        if now_t - self._last_freeze_sample < 60:
            return
        self._last_freeze_sample = now_t
        freeze_pct = float(self.cfg.get("co_locate_freeze_pct", 85))
        with state.connect() as conn:
            rows = conn.execute(
                "SELECT idx FROM gpus WHERE status='assigned'"
            ).fetchall()
            for r in rows:
                idx = r["idx"]
                cap = self.allocator.mem_total(idx)
                if cap <= 0:
                    continue
                used = self.allocator.vram_used(conn, idx)
                if used > freeze_pct / 100.0 * cap:
                    if idx not in self._frozen_gpus:
                        self._frozen_gpus.add(idx)
                        self.log_line(f"L3 冻结: GPU{idx} 装箱显存 {used:.1f}/{cap:.0f} GiB")
                else:
                    self._frozen_gpus.discard(idx)

    def _tick(self) -> None:
        self._process_control_requests()  # 事故记录 4: cancel 转发 daemon, kill 前处理
        self._check_timeouts()  # H6: duration_min 超时看门狗, kill 后交 reap 收尾
        self._check_probes()  # L6: 日志门控 (fail_on_log/ready_on_log), kill 后交 reap 收尾
        self._reap_finished_jobs()
        _freed, _to = self.allocator.settle_releasing()
        for g in _to:
            self._diag_unreleased(g)  # 事故记录 4 建议 3: 超时未释放 -> 诊断输出
        self._l3_freeze_sample()
        moved = self.allocator.probe_free()
        for g in moved:
            if self._gpu_ignored(g):
                continue  # C2: gpu-ignore 人工确认, 静默 unmanaged 告警 (卡仍不派发)
            self.log_line(f"unmanaged: GPU{g} 被外部占用/孤儿, 不派发")
        restored = self.allocator.probe_unmanaged()
        for g in restored:
            self._clear_gpu_ignore(g)  # 恢复 free 自动复位 ignore 标记 (下次占用重新告警)
            self.log_line(f"unmanaged 自动恢复: GPU{g} 真实空闲 -> free")
        self._unlock_dependent_batches()
        self._settle_batch_status()  # P1: 批次终态收敛
        self._dispatch_ready_jobs()

    def _gpu_ignored(self, idx: int) -> bool:
        """gpu-ignore 人工确认标记 (C2 修复): ignore_until 非 NULL = 静默告警.

        列名沿用 schema (ignore_until), 语义为"人工确认于该时刻"——unmanaged
        是持久状态, 用 NULL/非 NULL 表达是否已确认, 恢复 free 时自动清 NULL.
        """
        with state.connect() as conn:
            row = state.get_gpu(conn, idx)
            return bool(row and row["ignore_until"])

    def _clear_gpu_ignore(self, idx: int) -> None:
        with state.connect() as conn:
            conn.execute(
                "UPDATE gpus SET ignore_until=NULL WHERE idx=?", (idx,)
            )

    def _settle_batch_status(self) -> None:
        """P1: 批次终态. done = 全部任务成功终态 (done/skip);
        任一 failed/blocked/cancelled/timed_out -> blocked (interrupted 除外 R4).
        定案 37 (2026-08-15): blocked 批次人工 retry/resubmit 解除失败终态后
        -> 自动回 active 继续派发 (本次事故需手动 UPDATE 的 gap)."""
        with state.connect() as conn:
            batches = conn.execute(
                "SELECT * FROM batches WHERE status IN ('active','blocked')"
            ).fetchall()
            for b in batches:
                jobs = conn.execute(
                    "SELECT status FROM jobs WHERE batch_id=?", (b["id"],)
                ).fetchall()
                if not jobs:
                    continue
                statuses = [j["status"] for j in jobs]
                if all(s in ("done", "skip") for s in statuses):
                    conn.execute(
                        "UPDATE batches SET status='done' WHERE id=?", (b["id"],)
                    )
                    self._write_marker(
                        b["name"], "done",
                        f"{len(statuses)} 任务全部成功终态 (done/skip)",
                    )
                    self.log_line(f"批次 {b['name']} done (全部任务成功终态)")
                elif any(
                    s in ("failed", "blocked", "cancelled", "timed_out")
                    for s in statuses
                ):
                    if b["status"] == "active":
                        conn.execute(
                            "UPDATE batches SET status='blocked' WHERE id=?", (b["id"],)
                        )
                        fails = [
                            r["task_id"] for r in conn.execute(
                                "SELECT task_id FROM jobs WHERE batch_id=? AND status IN"
                                " ('failed','blocked','cancelled','timed_out')",
                                (b["id"],),
                            ).fetchall()
                        ]
                        self._write_marker(
                            b["name"], "blocked",
                            f"失败任务: {','.join(fails) if fails else '-'}",
                        )
                        self.log_line(f"批次 {b['name']} blocked (有失败任务, 等人工)")
                elif b["status"] == "blocked":
                    # 人工 retry/resubmit 已解除全部失败终态 (只剩 pending/running 等)
                    conn.execute(
                        "UPDATE batches SET status='active' WHERE id=?", (b["id"],)
                    )
                    self._remove_marker(b["name"], "blocked")  # P7: 解除阻塞删除 marker
                    self.log_line(f"批次 {b['name']} 失败终态解除 -> active (人工 retry 生效)")

    # ---------- P7: 批次终态 marker (2026-08-15) ----------

    def _marker_dir(self) -> str:
        # 决策 5B: 按节点隔离 ({STATE}/<hostname>/markers) —— 共享 NFS 多节点
        # 时同名批次 marker 不再互相覆盖 (与 state.db/logs/profiles 一致)
        d = os.path.join(self.host_dir, "markers")
        os.makedirs(d, exist_ok=True)
        return d

    def _write_marker(self, name: str, kind: str, detail: str) -> None:
        """P7: 批次进入终态 (done/blocked) 写 marker 文件, 供一行查看 (sched markers).

        文件: {STATE}/<hostname>/markers/{name}.{kind} (决策 5B 按节点隔离; 按名覆盖幂等).
        """
        p = os.path.join(self._marker_dir(), f"{name}.{kind}")
        try:
            with open(p, "w", encoding="utf-8") as f:
                f.write(f"{state.now()} | {detail}\n")
        except OSError:
            pass  # marker 非关键路径, 写失败不影响调度

    def _remove_marker(self, name: str, kind: str) -> None:
        p = os.path.join(self._marker_dir(), f"{name}.{kind}")
        try:
            os.remove(p)
        except OSError:
            pass

    # ---------- 节点重启恢复 (D4) ----------

    def _check_node_restart(self) -> None:
        """心跳在但 uptime < daemon 启动时间 -> 节点重启 -> interrupted (D4).

        H2 修复: 必须用 acquire_lock 触心跳**之前**采样的 _prev_hb_ts,
        否则 hb_ts≈now > boot_ts 恒为 False, 检测永不触发.
        """
        hb_ts = getattr(self, "_prev_hb_ts", None)
        if hb_ts is None:
            return
        with open("/proc/uptime") as f:
            uptime = float(f.read().split()[0])
        try:
            boot_ts = time.time() - uptime
            if boot_ts > hb_ts:
                self.log_line("D4: 检测到节点重启, running 任务标 interrupted (不计 retries)")
                with state.connect() as conn:
                    rows = conn.execute(
                        "SELECT * FROM jobs WHERE status='running'"
                    ).fetchall()
                    for j in rows:
                        state.update_job(
                            conn, j["id"], status="interrupted",
                            kill_reason=None,
                        )
                        self._requeue_for_retry(conn, j)
        except (OSError, ValueError):
            pass

    # ---------- 接管 (A3) ----------

    def _adopt_running(self) -> None:
        with state.connect() as conn:
            # P1: SQL 层过滤 running
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status='running'"
            ).fetchall()
            for j in rows:
                if j["pgid"] and self.executor.alive(j["pgid"]):
                    self.log_line(f"A3: 接管 running job {j['id']} (pgid={j['pgid']})")
                    continue
                # M6: 成功任务恰在 reap 前 daemon 重启 -> pgid 已死但产物
                # 齐全; 先查产物/指纹, 有效判 done, 避免白跑一遍
                spec = json.loads(self._get_task_spec(conn, j) or "{}")
                if self._should_skip(spec, j):
                    state.update_job(
                        conn, j["id"], status="done",
                        finished_at=state.now(),
                    )
                    self.log_line(f"A3: job {j['id']} pgid 已死但产物/指纹有效 -> done")
                else:
                    self.log_line(f"A3: job {j['id']} pgid 已死, 标 failed")
                    state.update_job(
                        conn, j["id"], status="failed",
                        finished_at=state.now(),
                    )
                self._release_gpu_for_job(conn, j)
                # M3 (决策 2A): 与 reap 路径对齐 —— 接管标 failed 后也走 retry,
                # 不再直接堵批次; 重试满 -> blocked 语义与正常运行路径一致
                if state.get_job(conn, j["id"])["status"] == "failed":
                    self._maybe_retry(conn, j)

    # ---------- reap ----------

    def _process_control_requests(self) -> None:
        """事故记录 4 (2026-08-17): 处理 cancel 转发请求 — 在**计算节点本地**执行 kill.

        CLI (登录节点) 看不到计算节点进程组 (PID namespace 跨节点, 定案 44 同类),
        本地 killpg 恒失败曾致孤儿占卡 13 分钟。现在 CLI 只写 control_requests 队列,
        本方法每轮 tick 拉取并在本地完成:
          1. alive 预检 (O5): 进程已自然结束 -> 清 kill_reason 让 reap 按 rc 判
          2. 写 kill_reason=cancelled (N2: 先写 reason 再 killpg)
          3. killpg SIGTERM; 下一轮仍存活 -> SIGKILL 升级 (绝不静默, 修复建议 2)
        GPU 释放交给 reap (_handle_job_done cancelled 分支), 与正常路径一致.
        """
        with state.connect() as conn:
            reqs = state.pending_control_requests(conn)
            if not reqs:
                return
            for r in reqs:
                j = state.get_job(conn, r["job_id"])
                if j is None or j["status"] != "running" or not j["pgid"]:
                    # 任务已不在 running (已 done/failed/cancelled 或 pgid 丢失)
                    state.finish_control_request(conn, r["id"], "job 非 running, 无需 kill")
                    self.log_line(f"cancel req {r['id']}: job {r['job_id']} 非 running, 跳过")
                    continue
                pgid = j["pgid"]
                if not self.executor.alive(pgid):
                    # 已死。区分: 若是我们上一轮 SIGTERM 杀死的 -> 保留 reason,
                    # reap 判 cancelled; 若从未 kill (自然结束) -> 清 reason 按 rc 判 (O5)
                    if j["kill_reason"] == "cancelled":
                        state.finish_control_request(conn, r["id"], "SIGTERM 生效, 已退出")
                        self.log_line(f"cancel req {r['id']}: job {j['id']} SIGTERM 生效 (等 reap 收敛 cancelled)")
                    else:
                        state.update_job(conn, j["id"], kill_reason=None)
                        state.finish_control_request(conn, r["id"], "进程已自然结束 (O5)")
                        self.log_line(f"cancel req {r['id']}: job {j['id']} 进程已自然结束 (O5), 清 reason")
                    continue
                if j["kill_reason"] == "cancelled":
                    # 上一轮已 SIGTERM 但仍存活 -> SIGKILL 升级
                    self.executor.kill_pgid(pgid, signal.SIGKILL)
                    state.finish_control_request(conn, r["id"], "SIGKILL 升级")
                    self.log_line(f"⚠️ cancel req {r['id']}: job {j['id']} SIGTERM 未生效 -> SIGKILL (pgid={pgid})")
                    continue
                state.update_job(conn, j["id"], kill_reason="cancelled")
                self.executor.kill_pgid(pgid)  # SIGTERM
                self.log_line(f"cancel req {r['id']}: job {j['id']} killpg SIGTERM (pgid={pgid})")
                # 请求本轮不 finish: 下轮 tick 复查, 仍存活则 SIGKILL 升级

    def _check_timeouts(self) -> None:
        """H6 任务级超时看门狗: running 超 duration_min (schema 已校验) -> timed_out.

        与 cancel 同结构 (自愈升级, 零同 tick 竞态):
          1. kill_reason='timed_out' 的 running job: 仍存活 -> SIGKILL 升级
             (这些 job 是**上一轮** tick SIGTERM 的, 有 10s 优雅退出窗口)
          2. 其余 running job: 超 duration_min -> 先写 kill_reason 再 SIGTERM,
             reap 按 reason 收尾 (_handle_job_done timed_out 分支, 不 retry ——
             超时任务重跑大概率再超时)
        """
        with state.connect() as conn:
            escal = conn.execute(
                "SELECT * FROM jobs WHERE status='running'"
                " AND kill_reason='timed_out' AND pgid IS NOT NULL"
            ).fetchall()
            for j in escal:
                if self.executor.alive(j["pgid"]):
                    self.log_line(
                        f"⚠️ job {j['id']} 超时 SIGTERM 未生效 -> SIGKILL (pgid={j['pgid']})"
                    )
                    self.executor.kill_pgid(j["pgid"], signal.SIGKILL)
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status='running'"
                " AND kill_reason IS NULL AND started_at IS NOT NULL"
                " AND pgid IS NOT NULL"
            ).fetchall()
            for j in rows:
                spec = json.loads(self._get_task_spec(conn, j) or "{}")
                dur = spec.get("duration_min")
                if not dur:
                    continue
                try:
                    started = time.mktime(
                        time.strptime(j["started_at"], "%Y-%m-%d %H:%M:%S")
                    )
                except (TypeError, ValueError):
                    continue
                if time.time() - started <= float(dur) * 60:
                    continue
                state.update_job(conn, j["id"], kill_reason="timed_out")
                self.executor.kill_pgid(j["pgid"])  # SIGTERM
                self.log_line(
                    f"job {j['id']} 超时 (duration_min={dur}) -> killpg SIGTERM"
                    f" (pgid={j['pgid']})"
                )

    def _check_probes(self) -> None:
        """L6 probes 日志门控 (§3.4d R3): 运行中任务按声明匹配日志模式.

        - fail_on_log 命中: 组级 kill -> 直接 blocked (failure='probe', 不 retry)
          (probe 命中视为确定失败)
        - ready_on_log 命中: 组级 kill -> done (产物校验仍执行, 失败降级
          failed —— probe 只是"看起来成功", 产物才是最终裁判)
        - kill 用 killpg (组级), 与 cancel 同机制; 状态先行写入使 reap 按
          kill_reason/终态收尾, 不误判为 rc 失败
        - SIGKILL 升级 (审查 L3): 已触发 kill 的 job (kill_reason='probe') 若
          pgid 仍存活 -> 逐轮 SIGKILL, 与 cancel 的升级语义对齐 (防进程忽略
          SIGTERM 占卡直至 releasing 超时)
        """
        with state.connect() as conn:
            # P1: SQL 层过滤, 不再每 tick 全表扫描历史 job
            running = conn.execute(
                "SELECT * FROM jobs WHERE status='running' AND pgid IS NOT NULL"
            ).fetchall()
            for j in running:
                spec = json.loads(self._get_task_spec(conn, j) or "{}")
                probes = spec.get("probes") or {}
                fail_pat = probes.get("fail_on_log")
                ready_pat = probes.get("ready_on_log")
                if not fail_pat and not ready_pat:
                    continue
                log_path = self._job_log_path(j)
                try:
                    # P2: 增量扫描 —— 记录已扫偏移只读新增字节 (重叠回退最长
                    # 模式长度防跨块切断); 日志截断/轮转则从头重扫
                    off = self._probe_offsets.get(j["id"], 0)
                    size = os.path.getsize(log_path)
                    if size < off:
                        off = 0
                    longest = max(len(fail_pat or ""), len(ready_pat or ""))
                    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                        f.seek(max(0, off - longest))
                        text = f.read()
                    self._probe_offsets[j["id"]] = size
                except OSError:
                    continue  # 日志未就绪, 下轮再查
                if fail_pat and fail_pat in text:
                    self.log_line(
                        f"probe fail_on_log 命中: job {j['id']} ({fail_pat!r}) -> kill + blocked"
                    )
                    self.executor.kill_pgid(j["pgid"])
                    # probe 命中视为确定失败: 不 retry, 直接 blocked (等人工)
                    state.update_job(
                        conn, j["id"], status="blocked", failure="probe",
                        kill_reason="probe", finished_at=state.now(),
                    )
                    self._release_gpu_for_job(conn, j)
                    continue
                if ready_pat and ready_pat in text:
                    self.log_line(
                        f"probe ready_on_log 命中: job {j['id']} ({ready_pat!r}) -> kill + done"
                    )
                    self.executor.kill_pgid(j["pgid"])
                    state.update_job(
                        conn, j["id"], status="done", kill_reason="probe",
                        finished_at=state.now(),
                    )
                    # ready 命中但产物校验不过 -> 降级 failed (probe 只是看起来成功)
                    artifacts = spec.get("artifacts", {})
                    if not self._check_artifacts(artifacts, spec.get("cwd_abs") or "."):
                        self.log_line(f"job {j['id']} ready probe 但产物校验失败 -> 降级 failed")
                        state.update_job(conn, j["id"], status="failed", failure="artifact")
                        self._drop_profile(j)  # 失败路径: 只删临时不 upsert (同 rc!=0)
                    else:
                        self._consume_profile(conn, j, spec)
                    self._release_gpu_for_job(conn, j)
            # L3: SIGKILL 升级 —— probe kill 已触发但进程忽略 SIGTERM 仍存活
            # (终态 done/blocked 的 job 不会再进上面的 running 循环, 在此补杀)
            # P1: SQL 层过滤, 不扫全表
            escal = conn.execute(
                "SELECT * FROM jobs WHERE kill_reason='probe' AND pgid IS NOT NULL"
            ).fetchall()
            for j in escal:
                if self.executor.alive(j["pgid"]):
                    self.log_line(f"probe kill 升级: job {j['id']} SIGTERM 未生效 -> SIGKILL")
                    self.executor.kill_pgid(j["pgid"], signal.SIGKILL)
                # H4 修复: 补杀/确认死亡后清 pgid, 解除对历史终态 job 的永久
                # 探测 —— 否则 OS 复用该 pgid 后每轮 SIGKILL 无关进程组
                state.update_job(conn, j["id"], pgid=None)
            # P2: 清理已不在 running 的 job 的偏移记录, 防内存随历史膨胀
            live = {j["id"] for j in running}
            for jid in [k for k in self._probe_offsets if k not in live]:
                del self._probe_offsets[jid]

    def _reap_finished_jobs(self) -> None:
        with state.connect() as conn:
            # P1: SQL 层过滤 running, 不再每 tick 全表扫描历史 job
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status='running' AND pgid IS NOT NULL"
            ).fetchall()
            for j in rows:
                rc = self.executor.poll_rc(j["pgid"])
                if rc is None:
                    continue  # 仍在运行
                state.update_job(conn, j["id"], rc=rc)
                self._handle_job_done(conn, j, rc)

    def _handle_job_done(self, conn, j, rc: int | None = None) -> None:
        """reap 顺序 (N2): 先读 kill_reason 定终态; 无 reason 按 rc + 日志分类."""
        reason = j["kill_reason"]
        if rc is None:
            rc = j["rc"]
        if reason == "cancelled":
            # 修复建议 2 (事故记录 4): reap 前二次校验——标 cancelled 但进程仍
            # 存活 (SIGTERM 未生效/孤儿逃逸) -> 绝不静默, SIGKILL 兜底
            if j["pgid"] and self.executor.alive(j["pgid"]):
                self.log_line(f"⚠️ 兜底: job {j['id']} 标 cancelled 但 pgid={j['pgid']} 仍存活 -> SIGKILL")
                self.executor.kill_pgid(j["pgid"], signal.SIGKILL)
            self.log_line(f"job {j['id']} cancelled (用户终止)")
            state.update_job(conn, j["id"], status="cancelled", finished_at=state.now())
            self._release_gpu_for_job(conn, j)
            self._drop_profile(j)  # 失败路径: 只删临时不 upsert
            return
        if reason == "timed_out":
            self.log_line(f"job {j['id']} timed_out (超时)")
            state.update_job(conn, j["id"], status="timed_out", finished_at=state.now())
            self._release_gpu_for_job(conn, j)
            self._drop_profile(j)
            return

        log_path = self._job_log_path(j)
        # rc 缺失 (executor 崩溃/被强杀) -> 兜底
        if rc is None:
            failure, _ = self.executor.failed_classify(log_path)
            state.update_job(
                conn, j["id"], status="failed", failure=failure,
                finished_at=state.now(),
            )
            self.log_line(f"job {j['id']} failed (rc 缺失, {failure})")
            self._release_gpu_for_job(conn, j)
            self._drop_profile(j)
            self._maybe_retry(conn, j)
            return

        if rc == 0:
            # 产物校验 (D8)
            spec = json.loads(self._get_task_spec(conn, j) or "{}")
            artifacts = spec.get("artifacts", {})
            if self._check_artifacts(artifacts, spec.get("cwd_abs") or "."):
                state.update_job(conn, j["id"], status="done", rc=rc,
                                 finished_at=state.now())
                self.log_line(f"job {j['id']} done rc=0 产物校验通过")
                # profile 消费 (定案 39): rc=0 后 upsert profile_cache + 删临时
                self._consume_profile(conn, j, spec)
            else:
                state.update_job(conn, j["id"], status="failed", rc=rc,
                                 failure="artifact",
                                 finished_at=state.now())
                self.log_line(f"job {j['id']} failed (rc=0 但产物校验失败)")
                self._drop_profile(j)  # 失败路径: 只删临时不 upsert (同 rc!=0)
        else:
            failure, _ = self.executor.failed_classify(log_path)
            state.update_job(conn, j["id"], status="failed", rc=rc, failure=failure,
                             finished_at=state.now())
            self.log_line(f"job {j['id']} failed rc={rc} ({failure})")
            self._drop_profile(j)  # 失败路径: 只删不 upsert
        self._release_gpu_for_job(conn, j)
        self._maybe_retry(conn, j)

    def _release_gpu_for_job(self, conn, j) -> None:
        """assigned -> releasing (立即, B5). 多归属计数释放 (§3.2e B)."""
        if j["gpu"] is not None:
            self._release_in_tx(conn, j["id"])

    def _diag_unreleased(self, idx: int) -> None:
        """事故记录 4 建议 3: releasing 冷却上限 (5min) 到期仍被占 -> 输出诊断.

        真实环境: nvidia-smi 列出该卡 compute 进程 + 已知 job pgid 对照, 引导
        人工清理 (kill -9). fake 模式: 只记录警告 (无真实 nvidia-smi).
        """
        if self.fake:
            self.log_line(f"⚠️ releasing 超时: GPU{idx} 5min 冷却到期仍有 compute 进程 -> 转 unmanaged (fake)")
            return
        self.log_line(
            f"⚠️ releasing 超时: GPU{idx} 5min 冷却到期仍有 compute 进程 -> 转 unmanaged. "
            f"残留进程列表 (nvidia-smi):"
        )
        try:
            out = subprocess.run(
                ["nvidia-smi", "--query-compute-apps=pid,process_name,gpu_uuid", "--format=csv,noheader", "-i", str(idx)],
                capture_output=True, text=True, timeout=10,
            )
            for line in (out.stdout or "").strip().splitlines():
                self.log_line(f"  {line.strip()}")
            self.log_line(
                f"  处置: 确认无价值进程后 kill -9 <pid> 清理, 卡会自动回 free "
                f"(probe_unmanaged); 或 sched gpu-free {idx} 强制回 free"
            )
        except (subprocess.SubprocessError, ValueError, OSError):
            self.log_line(f"  (nvidia-smi 查询失败, 请手动执行 nvidia-smi -i {idx})")

    # ---------- profile 消费 (定案 39 待定项 3, daemon 侧) ----------

    def _profile_path(self, j) -> str:
        return os.path.join(self.host_dir, "profiles", f"{j['id']}.json")

    def _consume_profile(self, conn, j, spec: dict) -> None:
        """job 成功 (rc=0 且产物校验通过) 后: 读 peak_gib -> upsert profile_cache -> 删临时.

        失败路径 (定案 39): 读取失败/JSON 畸形按"无 profile"忽略 (只删不 upsert);
        任务未声明 resources.profile_key -> 只删 (profile_cache 按 key 索引, 无 key 不入库).
        """
        import json as _json

        p = self._profile_path(j)
        profile_key = (spec.get("resources") or {}).get("profile_key")
        try:
            if not os.path.isfile(p):
                return
            data = _json.loads(open(p, encoding="utf-8").read())
            peak = data.get("peak_gib")
            if peak is None:
                return
            peak = float(peak)
        except (OSError, ValueError, TypeError, _json.JSONDecodeError):
            return
        finally:
            # 无论成败删临时 (失败只删不 upsert, 防垃圾累积)
            try:
                os.unlink(p)
            except OSError:
                pass
        if not profile_key:
            return  # 无 key 不入库 (文件已删)
        conn.execute(
            "INSERT INTO profile_cache (profile_key, peak_gib, updated_at, git_rev)"
            " VALUES (?,?,?,?) "
            "ON CONFLICT(profile_key) DO UPDATE SET peak_gib=excluded.peak_gib,"
            " updated_at=excluded.updated_at, git_rev=excluded.git_rev",
            (profile_key, peak, state.now(), j["git_rev"]),
        )
        self.log_line(f"profile upsert {profile_key} peak={peak:.2f} GiB")

    def _drop_profile(self, j) -> None:
        """失败/取消/超时路径: 只删临时文件不 upsert."""
        try:
            os.unlink(self._profile_path(j))
        except OSError:
            pass

    def _maybe_retry(self, conn, j) -> None:
        """失败重试: max_retry 内回 pending; 满 -> blocked (3.4).

        调用方传入的是更新前的 Row 快照 (status 还是 running/pending),
        必须重新取行才能看到刚写入的 failed/failure/retries.
        """
        j = state.get_job(conn, j["id"])
        if j is None or j["status"] != "failed":
            return
        if j["failure"] == "perm":
            # H4: 权限错不重试
            state.update_job(conn, j["id"], status="blocked")
            return
        spec = json.loads(self._get_task_spec(conn, j) or "{}")
        max_retry = spec.get("max_retry", DEFAULT_MAX_RETRY)
        if j["retries"] < max_retry:
            state.update_job(
                conn, j["id"], status="pending", retries=j["retries"] + 1,
                pgid=None, rc=None, kill_reason=None, gpu=None,
            )
            self.log_line(f"job {j['id']} 重试 {j['retries'] + 1}/{max_retry} (30s 退避)")
        else:
            state.update_job(conn, j["id"], status="blocked")
            self.log_line(f"job {j['id']} blocked (重试满)")

    def _requeue_for_retry(self, conn, j) -> None:
        """interrupted -> pending (D4, 不计 retries).

        H3 修复: 回队前必须释放 GPU 占用 (assigned -> releasing + 删 gpu_jobs
        行), 否则节点重启后卡仍 assigned 给已死 job, GPU 永久泄漏.
        """
        if j["gpu"] is not None:
            self._release_in_tx(conn, j["id"])
        state.update_job(
            conn, j["id"], status="pending", pgid=None, rc=None,
            kill_reason=None, gpu=None,
        )

    # ---------- 依赖解锁 ----------

    def _unlock_dependent_batches(self) -> None:
        with state.connect() as conn:
            batches = conn.execute("SELECT * FROM batches WHERE status='queued'").fetchall()
            for b in batches:
                deps = json.loads(b["depends_on"] or "[]")
                if not deps:
                    conn.execute(
                        "UPDATE batches SET status='active' WHERE id=?", (b["id"],)
                    )
                    continue
                if all(self._batch_successful(conn, d) for d in deps):
                    conn.execute(
                        "UPDATE batches SET status='active' WHERE id=?", (b["id"],)
                    )
                    self.log_line(f"批次 {b['name']} 依赖解锁 -> active")

    def _batch_successful(self, conn, batch_name: str) -> bool:
        """§2.4: depends_on 批次全部任务成功终态 (done/skip) 才解锁."""
        # M19: created_at 秒级精度可能并列, 加 rowid 次序保证锚定最新批次
        b = conn.execute(
            "SELECT id FROM batches WHERE name=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (batch_name,),
        ).fetchone()
        if not b:
            return False
        jobs = conn.execute(
            "SELECT status FROM jobs WHERE batch_id=?", (b["id"],)
        ).fetchall()
        if not jobs:
            return False
        return all(j["status"] in ("done", "skip") for j in jobs)

    # ---------- 派发 ----------

    def _dispatch_ready_jobs(self) -> None:
        with state.connect() as conn:
            # 只派发所属批次已解锁 (active/done) 的 pending job——
            # queued 批次 (依赖未解锁) 的 job 不派发 (场景 2: 下游挂起)
            ready = conn.execute(
                "SELECT j.* FROM jobs j JOIN batches b ON j.batch_id=b.id"
                " WHERE j.status='pending' AND b.status IN ('active','done')"
                " ORDER BY j.rowid"
            ).fetchall()
            # CPU 配额制 (§5b B4 v2): config.cpus_total = 节点总核数;
            # running 任务 (GPU + CPU-only) 的 CPU 占用总和 + 新任务 <= 总核数 才派发.
            # GPU 任务 CPU 占用 = resources.cpus 或 config.gpu_job_cpus (NN 训练也要 CPU).
            # cpus_total 未配置(0) -> 回退 max_cpu_jobs: CPU-only 并发上限 (定案 7 旧语义),
            # GPU 任务不受 CPU 约束 (旧版行为).
            cpus_total = int(self.cfg.get("cpus_total", 0) or 0)
            max_cpu_jobs = int(self.cfg.get("max_cpu_jobs", DEFAULT_MAX_CPU_JOBS))
            used_cpu = self._cpu_in_use(conn)
            cpu_only_running = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE status='running' AND gpu IS NULL"
            ).fetchone()[0]
            gpu_full = False
            now_ts = datetime.now()
            for j in ready:
                # M2: 重试退避 —— 失败重试 (retries>0) 的 job 等 RETRY_BACKOFF_SEC
                # 再派发, 防秒级崩溃任务同 tick 重新拉起形成紧密崩溃循环
                # (与 _maybe_retry 日志承诺的 30s 退避一致)
                if j["retries"] and j["finished_at"]:
                    try:
                        ft = datetime.strptime(j["finished_at"], "%Y-%m-%d %H:%M:%S")
                    except (ValueError, TypeError):
                        ft = None
                    if ft and (now_ts - ft).total_seconds() < RETRY_BACKOFF_SEC:
                        continue  # 退避中, 下轮再试
                spec = json.loads(self._get_task_spec(conn, j) or "{}")
                resources = spec.get("resources") or {}
                is_cpu_only = resources.get("gpu", 1) == 0
                task_cpus = self._task_cpus(spec)
                if cpus_total > 0 and used_cpu + task_cpus > cpus_total:
                    # CPU 配额不足: 本任务等下轮 (CPU 超卖禁止, 与 GPU 同纪律)
                    # 批内补位: continue 让后面的小任务可插队 (大任务等 GPU 释放同轮再试)
                    continue
                if is_cpu_only:
                    if cpus_total <= 0 and cpu_only_running >= max_cpu_jobs:
                        continue  # 回退模式: CPU-only 并发上限 (旧语义)
                    gpu = None  # CPU-only: 不占 GPU 槽位
                else:
                    if gpu_full:
                        continue  # GPU 已满: 跳过后续 GPU 任务, 继续扫 CPU-only (防饿死)
                    # 用同一事务 assign (避免嵌套 connect 的 database is locked)
                    gpu = self._assign_in_tx(conn, j["id"], spec)
                    if gpu is None:
                        gpu_full = True
                        continue  # 无空卡: 本轮不再派 GPU 任务, 但 CPU-only 仍可派 (防饿死)
                try:
                    self._launch_job(conn, j, gpu)
                    used_cpu += task_cpus
                    if is_cpu_only:
                        cpu_only_running += 1
                except Exception as e:
                    # 启动失败: 释放 GPU (如占) + 标 failed (走 retry 路径), 不中断整轮派发
                    self.log_line(f"LAUNCH FAIL job {j['id']} gpu={gpu}: {e}")
                    if gpu is not None:
                        self._release_in_tx(conn, j["id"])
                    state.update_job(
                        conn, j["id"], status="failed", failure="launch",
                        finished_at=state.now(),
                    )
                    self._maybe_retry(conn, j)

    def _task_cpus(self, spec: dict) -> int:
        """任务 CPU 占用: resources.cpus 优先; GPU 任务未声明用 config.gpu_job_cpus."""
        resources = spec.get("resources") or {}
        cpus = resources.get("cpus")
        if cpus:
            return int(cpus)
        if resources.get("gpu", 1) == 0:
            return 1  # CPU-only 缺省 1 核 (schema 已补, 双保险)
        return int(self.cfg.get("gpu_job_cpus", DEFAULT_GPU_JOB_CPUS))

    def _cpu_in_use(self, conn) -> int:
        """当前 running 任务 (GPU + CPU-only) 的 CPU 占用总和."""
        used = 0
        rows = conn.execute(
            "SELECT * FROM jobs WHERE status='running'"
        ).fetchall()
        for j in rows:
            try:
                spec = json.loads(self._get_task_spec(conn, j) or "{}")
            except (json.JSONDecodeError, TypeError):
                spec = {}
            used += self._task_cpus(spec)
        return used

    def _assign_in_tx(self, conn, job_id: str, spec: dict | None = None) -> int | None:
        """事务内 assign (定案 39 L2 共享装箱).

        独占任务 (gpu_share 缺省 false): free 卡 -> assigned (现状语义);
        声明 resources.vram_gib 时跳过容量不足的卡 (异构适配, 定案 46 B).
        共享任务 (gpu_share=true 且 config co_locate=true): free 卡 ∪ 有余量的
        assigned 卡 (SUM(vram_gib)+新任务 ≤ co_locate_safety×容量 且 未冻结 且
        每卡任务数 < co_locate_max_jobs) -> **归一化负载选卡 (min
        (used+task_vram)/cap, 定案 46 A; 非 First-Fit)**.
        组合缺格 (gpu_share=true × co_locate=false): 按独占跑 + 告警 (声明是意愿,
        全局开关是许可). 鲸鱼排除: vram_gib > safety×容量 -> 返回 None (装箱必失败,
        由调用方按无卡处理).
        """
        spec = spec or {}
        resources = spec.get("resources") or {}
        gpu_share = bool(resources.get("gpu_share"))
        co_locate = bool(self.cfg.get("co_locate", False))
        if gpu_share and not co_locate:
            self.log_line(f"job {job_id} gpu_share=true 但 co_locate 未开启 -> 按独占跑 + 告警")
            gpu_share = False
        # 装箱值 = max(声明 vram_gib, profile_cache.peak_gib) (定案 39 L1, profile 命中)
        task_vram = None
        if gpu_share:
            task_vram = float(resources.get("vram_gib", 0.0) or 0.0)
            pk = resources.get("profile_key")
            if pk:
                row = conn.execute(
                    "SELECT peak_gib FROM profile_cache WHERE profile_key=?", (pk,)
                ).fetchone()
                if row and row["peak_gib"]:
                    task_vram = max(task_vram, float(row["peak_gib"]))
            safety = float(self.cfg.get("co_locate_safety", 0.7))
        # 独占任务: 第一张能装下的 free 卡 (定案 6 每卡独占; 2026-08-17 方案 B:
        # 异构容量适配——任务声明 resources.vram_gib 时跳过容量不足的卡, 防大任务
        # 被派到小卡 OOM. 未声明维持现状 (不声明不校验).)
        excl_vram = None
        if not gpu_share:
            v = resources.get("vram_gib")
            if v is not None:
                try:
                    excl_vram = float(v)
                except (TypeError, ValueError):
                    excl_vram = None
            for idx in self.allocator.gpu_list:
                row = conn.execute(
                    "SELECT status, quarantined FROM gpus WHERE idx=?", (idx,)
                ).fetchone()
                if not row or row["quarantined"]:
                    continue
                if row["status"] == "free":
                    if excl_vram is not None:
                        cap = self.allocator.mem_total(idx)
                        if cap > 0 and excl_vram > cap:
                            continue  # 声明峰值超过该卡容量: 换下一张 (异构适配)
                    conn.execute(
                        "UPDATE gpus SET status='assigned', job_id=?, updated_at=? WHERE idx=?",
                        (job_id, state.now(), idx),
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO gpu_jobs (gpu_id, job_id, vram_gib, updated_at)"
                        " VALUES (?,?,?,?)",
                        (idx, job_id, task_vram, state.now()),
                    )
                    return idx
            return None

        # 共享任务: 归一化负载装箱 (定案 40 Least-Loaded + 2026-08-17 方案 A).
        #   动机: First-Fit 会把轻任务全堆 GPU0 (raft 峰值 0.3-0.6GiB 摸不到显存
        #   约束, 仅靠任务数上限换卡) -> GPU0 满载 GPU1/2/3 空转. 改为候选
        #   (free ∪ 有余量 assigned) 中选负载率最低的一张, 轻任务均匀分散到全部卡.
        #   2026-08-17 异构升级: 选卡标准从"绝对已用最小"(min used) 改为
        #   "负载率最低"(min used/cap)——16GB+24GB 混用时按比例均衡, 轻任务
        #   自动倾向大卡 (绝对 used 会优先堆小卡, 大卡空转). free 卡 used=0
        #   负载率 0 天然优先. 平局取最小 idx (确定性, 与定案 2 声明顺序一致).
        #   独占卡 (gpu_jobs 含 vram_gib IS NULL 行) 视为满: 独占占整卡不可再装箱,
        #   否则 SUM(NULL)=0 会骗过装箱 (First-Fit 也会放, Least-Loaded 会优先选).
        best_idx: int | None = None
        best_load = float("inf")
        for idx in self.allocator.gpu_list:
            row = conn.execute(
                "SELECT status, quarantined FROM gpus WHERE idx=?", (idx,)
            ).fetchone()
            if not row or row["quarantined"]:
                continue
            if row["status"] == "free":
                used = 0.0
                cap = self.allocator.mem_total(idx)
                # H5 修复: free 卡同样做鲸鱼排除 (docstring 承诺: vram_gib >
                # safety×容量 -> 不装箱), 否则大任务被装上小空卡启动即 OOM
                if cap > 0 and task_vram > safety * cap:
                    continue
            elif row["status"] == "assigned":
                if idx in self._frozen_gpus:
                    continue
                excl = conn.execute(
                    "SELECT COUNT(*) AS n FROM gpu_jobs WHERE gpu_id=? AND vram_gib IS NULL",
                    (idx,),
                ).fetchone()
                if excl and excl["n"]:
                    continue  # 卡上有独占任务, 占整卡
                cap = self.allocator.mem_total(idx)
                if cap <= 0:
                    continue  # 容量未知: 不冒险共享
                used = self.allocator.vram_used(conn, idx)
                if used + task_vram > safety * cap:
                    continue
                if self.allocator.job_count(conn, idx) >= int(
                    self.cfg.get("co_locate_max_jobs", 3)
                ):
                    continue
            else:
                continue
            # 归一化负载 = 放入后负载率 (used+task)/cap 最小 — 回答"放哪张最均衡"
            # (当前负载 used/cap 会误选: 16GB@4GiB(0.25) vs 24GB@8GiB(0.33),
            #  放 8GiB 任务后 16GB->0.75 反而失衡, 应选 24GB->0.67)
            if cap > 0:
                load = (used + task_vram) / cap
            else:
                load = 0.0  # free 且容量未知: 第一个任务总能放 (现状语义)
            if load < best_load:
                best_load = load
                best_idx = idx
        if best_idx is None:
            return None
        srow = conn.execute(
            "SELECT status FROM gpus WHERE idx=?", (best_idx,)
        ).fetchone()
        if srow["status"] == "free":
            conn.execute(
                "UPDATE gpus SET status='assigned', job_id=?, updated_at=? WHERE idx=?",
                (job_id, state.now(), best_idx),
            )
        # 镜像列语义 (首个 assign 为镜像): 已有镜像不动, 新 job 只加 gpu_jobs 行
        conn.execute(
            "INSERT OR REPLACE INTO gpu_jobs (gpu_id, job_id, vram_gib, updated_at)"
            " VALUES (?,?,?,?)",
            (best_idx, job_id, task_vram, state.now()),
        )
        return best_idx

    def _release_in_tx(self, conn, job_id: str) -> None:
        """事务内释放: 多归属计数释放 (§3.2e B). 复用 state.release_gpu."""
        state.release_gpu(conn, job_id)

    def _launch_job(self, conn, j, gpu: int | None) -> None:
        # M1 修复: 条件更新抢占 —— SELECT 快照到 launch 之间 (指纹计算/产物清理
        # 可达秒级) CLI 可能已把 pending 标 cancelled; 只有仍为 pending 才允许
        # 转 running, 否则放弃派发并释放本事务已 assign 的卡
        cur = conn.execute(
            "UPDATE jobs SET status='running', started_at=?"
            " WHERE id=? AND status='pending'",
            (state.now(), j["id"]),
        )
        if cur.rowcount == 0:
            self.log_line(f"job {j['id']} 派发竞态: 已非 pending (或被 cancel), 放弃启动")
            if gpu is not None:
                self._release_in_tx(conn, j["id"])
            return
        spec = json.loads(self._get_task_spec(conn, j) or "{}")
        cwd = spec.get("cwd_abs") or resolve_template(self.cfg.get("default_project", "{ROOT}"), self.cfg)
        log_path = self._job_log_path(j)
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        # 产物指纹 skip 判据 (A2 + O3): 指纹有效 且 规则校验通过 -> skip
        if self._should_skip(spec, j):
            state.update_job(conn, j["id"], status="skip", finished_at=state.now())
            self.log_line(f"job {j['id']} skip (产物指纹有效, 不执行)")
            if gpu is not None:
                self._release_in_tx(conn, j["id"])
            return

        # 半成品清理 (§3.2): 产物存在但无效 (指纹不匹配/规则不过) -> 删除后启动
        self._clean_stale_artifacts(spec, j)

        # 显存峰值回写通道 (定案 39 profile 协议, daemon 侧注入):
        #   注入 SCHED_PROFILE_OUT=<host_dir>/profiles/<job_id>.json, 训练侧写
        #   {"peak_gib": X} (GiB); job rc=0 后 _consume_profile upsert profile_cache
        #   并删临时; 失败路径只删不 upsert. 任务显式声明 SCHED_PROFILE_OUT 则尊重.
        # M15: 批次级 env 生效 —— batch.env 打底 + task.env 覆盖, 此前批次 env
        # 校验入库后无读取方被静默丢弃
        b = state.get_batch(conn, j["batch_id"])
        batch_env = json.loads(b["env"]) if b and b["env"] else {}
        task_env = {**batch_env, **dict(spec.get("env", {}))}
        task_env.setdefault(
            "SCHED_PROFILE_OUT", os.path.join(self.host_dir, "profiles", f"{j['id']}.json")
        )
        os.makedirs(os.path.dirname(task_env["SCHED_PROFILE_OUT"]), exist_ok=True)
        pgid = self.executor.launch(
            cmd=spec.get("cmd"),
            stages=spec.get("stages"),
            cwd=cwd,
            env=task_env,
            gpu=gpu,
            log_path=log_path,
        )
        git_rev = None
        try:
            _, _, git_rev = compute_fingerprint(
                spec.get("cmd"), spec.get("stages"), cwd,
                spec.get("git"), self.venv_paths,
            )
        except Exception:
            pass
        state.update_job(
            conn, j["id"], gpu=gpu, pgid=pgid,
            git_rev=git_rev, kill_reason=None,
        )
        tag = f"cpu" if gpu is None else f"gpu={gpu}"
        self.log_line(f"LAUNCH job {j['id']} {tag} pgid={pgid}")

    def _should_skip(self, spec: dict, j) -> bool:
        """A2/O3: 产物指纹有效 且 规则校验通过 -> skip."""
        artifacts = spec.get("artifacts", {})
        if not artifacts:
            return False
        if not self._fingerprint_matches(spec, j):
            return False
        return self._check_artifacts(artifacts, spec.get("cwd_abs") or ".")

    def _fingerprint_matches(self, spec: dict, j) -> bool:
        if not j["fingerprint"]:
            return False
        try:
            cur, _, _ = compute_fingerprint(
                spec.get("cmd"), spec.get("stages"),
                spec.get("cwd_abs") or ".", spec.get("git"), self.venv_paths,
            )
        except Exception:
            return False
        return cur == j["fingerprint"]

    def _clean_stale_artifacts(self, spec: dict, j) -> None:
        """§3.2 半成品: 产物存在但指纹/规则无效 -> 删除后启动."""
        from .artifacts import check_artifact

        cwd = spec.get("cwd_abs") or "."
        for key, a in spec.get("artifacts", {}).items():
            p = str(a["path"])
            if p and not os.path.isabs(p):
                p = os.path.normpath(os.path.join(cwd, p))
            if os.path.exists(p):
                ok = self._fingerprint_matches(spec, j) and (
                    check_artifact(p, a) is None
                )
                if not ok:
                    try:
                        os.unlink(p)
                        self.log_line(f"清理过期产物 {p}")
                    except OSError:
                        pass

    # ---------- 工具 ----------

    def _get_task_spec(self, conn, j) -> str | None:
        row = conn.execute(
            "SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
            (j["batch_id"], j["task_id"], j["version"]),
        ).fetchone()
        return row["spec"] if row else None

    def _job_log_path(self, j) -> str:
        # 审查 L1: 带 version —— resubmit 新版本不再覆盖旧 job 日志
        # (否则 log -f/probes/diag 读到串扰内容)。
        return os.path.join(
            self.host_dir, "logs", j["batch_id"], f"{j['task_id']}-v{j['version']}.log"
        )

    def _check_artifacts(self, artifacts: dict, cwd: str) -> bool:
        """D8 产物校验: path 相对任务 cwd 解析 (E4 已保证在 cwd 内)."""
        from .artifacts import check_artifact

        for key, a in artifacts.items():
            p = a.get("path")
            if p and not os.path.isabs(p):
                p = os.path.normpath(os.path.join(cwd, p))
            if check_artifact(p or "", a) is not None:
                return False
        return True

    def _heartbeat(self) -> None:
        self._touch_heartbeat()
