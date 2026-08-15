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
from typing import Any

from . import state
from .allocator import Allocator
from .executor import Executor
from .fingerprint import compute_fingerprint
from .config import ConfigError, load_config, resolve_template

POLL_SEC = 10
HEARTBEAT_SEC = 30
RELEASE_TIMEOUT_SEC = 300  # releasing 冷却上限 5 分钟 (B5)
DEFAULT_MAX_RETRY = 1
DEFAULT_GPU_JOB_CPUS = 8  # GPU 任务默认 CPU 占用 (NN 训练数据加载也要 CPU, config gpu_job_cpus 可覆盖)
DEFAULT_MAX_CPU_JOBS = 2  # cpus_total 未配置时回退: CPU-only 并发上限 (定案 7 旧语义)


class Dispatcher:
    def __init__(self, cfg: dict, fake: bool = False):
        self.cfg = cfg
        self.fake = fake
        self.state_dir = state.default_state_dir()
        self.host_dir = os.path.join(self.state_dir, state.hostname())
        os.makedirs(self.host_dir, exist_ok=True)
        self.pid_file = os.path.join(self.host_dir, "daemon.pid")
        self.heartbeat_file = os.path.join(self.host_dir, "daemon.heartbeat")
        self.lock_dir = os.path.join(self.host_dir, "dispatcher.lock")
        self.log = open(
            os.path.join(self.host_dir, "scheduler.log"), "a", encoding="utf-8"
        )
        self.executor = Executor()
        self.allocator = Allocator(
            gpu_list=cfg.get("gpus") or [], fake=fake
        )
        # venv 路径映射 (指纹用)
        self.venv_paths = cfg.get("venvs", {})

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

    def stop(self) -> None:
        """daemon stop: 未完成任务标 cancelled 收尾 (N11)."""
        with state.connect() as conn:
            running = state.all_jobs(conn)
            for j in running:
                if j["status"] == "running":
                    if j["pgid"]:
                        self.executor.kill_pgid(j["pgid"])
                    state.update_job(
                        conn, j["id"], status="cancelled", kill_reason="cancelled",
                        finished_at=state.now(),
                    )
        self._cleanup_lock()

    # ---------- 主循环 ----------

    def run(self, once: bool = False) -> None:
        # GPU 表初始化 (配置集 -> free; 幂等)
        with state.connect() as conn:
            state.init_gpus(conn, self.allocator.gpu_list)
        if not self.fake:
            self._check_node_restart()
        self.log_line(
            f"dispatcher 启动 (pid={os.getpid()}, fake={self.fake}, gpus={self.allocator.gpu_list})"
        )
        # 接管: running 任务 pgid 存活则继续等 (A3/3.2b)
        self._adopt_running()

        while True:
            try:
                self._heartbeat()
                self._tick()
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.log_line(f"tick 异常: {e}")
            if once:
                break
            time.sleep(POLL_SEC)
        self._cleanup_lock()

    def _tick(self) -> None:
        self._reap_finished_jobs()
        self.allocator.settle_releasing()
        moved = self.allocator.probe_free()
        for g in moved:
            self.log_line(f"unmanaged: GPU{g} 被外部占用/孤儿, 不派发")
        restored = self.allocator.probe_unmanaged()
        for g in restored:
            self.log_line(f"unmanaged 自动恢复: GPU{g} 真实空闲 -> free")
        self._unlock_dependent_batches()
        self._settle_batch_status()  # P1: 批次终态收敛
        self._dispatch_ready_jobs()

    def _settle_batch_status(self) -> None:
        """P1: 批次终态. done = 全部任务成功终态 (done/skip);
        任一 failed/blocked/cancelled/timed_out -> blocked (interrupted 除外 R4)."""
        with state.connect() as conn:
            batches = conn.execute(
                "SELECT * FROM batches WHERE status='active'"
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
                    self.log_line(f"批次 {b['name']} done (全部任务成功终态)")
                elif any(
                    s in ("failed", "blocked", "cancelled", "timed_out")
                    for s in statuses
                ):
                    conn.execute(
                        "UPDATE batches SET status='blocked' WHERE id=?", (b["id"],)
                    )
                    self.log_line(f"批次 {b['name']} blocked (有失败任务, 等人工)")

    # ---------- 节点重启恢复 (D4) ----------

    def _check_node_restart(self) -> None:
        """心跳在但 uptime < daemon 启动时间 -> 节点重启 -> interrupted (D4)."""
        if not os.path.exists(self.heartbeat_file):
            return
        with open("/proc/uptime") as f:
            uptime = float(f.read().split()[0])
        try:
            boot_ts = time.time() - uptime
            hb_ts = os.path.getmtime(self.heartbeat_file)
            if boot_ts > hb_ts:
                self.log_line("D4: 检测到节点重启, running 任务标 interrupted (不计 retries)")
                with state.connect() as conn:
                    for j in state.all_jobs(conn):
                        if j["status"] == "running":
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
            for j in state.all_jobs(conn):
                if j["status"] == "running":
                    if j["pgid"] and self.executor.alive(j["pgid"]):
                        self.log_line(f"A3: 接管 running job {j['id']} (pgid={j['pgid']})")
                    else:
                        self.log_line(f"A3: job {j['id']} pgid 已死, 标 failed")
                        state.update_job(
                            conn, j["id"], status="failed",
                            finished_at=state.now(),
                        )
                        self._release_gpu_for_job(conn, j)

    # ---------- reap ----------

    def _reap_finished_jobs(self) -> None:
        with state.connect() as conn:
            for j in state.all_jobs(conn):
                if j["status"] != "running" or not j["pgid"]:
                    continue
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
            self.log_line(f"job {j['id']} cancelled (用户终止)")
            state.update_job(conn, j["id"], status="cancelled", finished_at=state.now())
            self._release_gpu_for_job(conn, j)
            return
        if reason == "timed_out":
            self.log_line(f"job {j['id']} timed_out (超时)")
            state.update_job(conn, j["id"], status="timed_out", finished_at=state.now())
            self._release_gpu_for_job(conn, j)
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
            else:
                state.update_job(conn, j["id"], status="failed", rc=rc,
                                 failure="artifact",
                                 finished_at=state.now())
                self.log_line(f"job {j['id']} failed (rc=0 但产物校验失败)")
        else:
            failure, _ = self.executor.failed_classify(log_path)
            state.update_job(conn, j["id"], status="failed", rc=rc, failure=failure,
                             finished_at=state.now())
            self.log_line(f"job {j['id']} failed rc={rc} ({failure})")
        self._release_gpu_for_job(conn, j)
        self._maybe_retry(conn, j)

    def _release_gpu_for_job(self, conn, j) -> None:
        """assigned -> releasing (立即, B5)."""
        if j["gpu"] is not None:
            conn.execute(
                "UPDATE gpus SET status='releasing', job_id=?, updated_at=? WHERE idx=?",
                (j["id"], state.now(), j["gpu"]),
            )

    def _maybe_retry(self, conn, j) -> None:
        """失败重试: max_retry 内回 pending; 满 -> blocked (3.4)."""
        if j["status"] != "failed":
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
        """interrupted -> pending (D4, 不计 retries)."""
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
        b = conn.execute(
            "SELECT id FROM batches WHERE name=? ORDER BY created_at DESC LIMIT 1",
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
            for j in ready:
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
                    gpu = self._assign_in_tx(conn, j["id"])
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

    def _assign_in_tx(self, conn, job_id: str) -> int | None:
        """事务内 assign: 可用集 = 配置集 - quarantine, 只派 free 卡."""
        for idx in self.allocator.gpu_list:
            row = conn.execute(
                "SELECT status, quarantined FROM gpus WHERE idx=?", (idx,)
            ).fetchone()
            if row and row["status"] == "free" and not row["quarantined"]:
                conn.execute(
                    "UPDATE gpus SET status='assigned', job_id=?, updated_at=? WHERE idx=?",
                    (job_id, state.now(), idx),
                )
                return idx
        return None

    def _release_in_tx(self, conn, job_id: str) -> None:
        """事务内释放: assigned -> releasing (B5)."""
        conn.execute(
            "UPDATE gpus SET status='releasing', updated_at=? WHERE job_id=?",
            (state.now(), job_id),
        )

    def _launch_job(self, conn, j, gpu: int | None) -> None:
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

        pgid = self.executor.launch(
            cmd=spec.get("cmd"),
            stages=spec.get("stages"),
            cwd=cwd,
            env=spec.get("env", {}),
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
            conn, j["id"], status="running", gpu=gpu, pgid=pgid,
            started_at=state.now(), git_rev=git_rev, kill_reason=None,
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
        return os.path.join(
            self.host_dir, "logs", j["batch_id"], f"{j['task_id']}.log"
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
