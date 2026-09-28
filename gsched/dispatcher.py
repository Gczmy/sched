"""dispatcher (文档 §3.1 主循环 / §3.2b 崩溃接管 / §3.2c 节点重启 / §2.4 依赖解锁).

每轮固定 sleep (POLL_SEC=10s, 零忙轮询):
  reap_finished_jobs -> settle_releasing -> probe_free_gpus
  -> unlock_dependent_batches -> 贪心派发 (只派 free 卡)

单实例: PID 文件 + 心跳 mtime 双校验 (B11 F3/F4).
"""

from __future__ import annotations
from contextlib import contextmanager

import fcntl
import json
import hashlib
import sqlite3
import math
import stat
import os
import re
import secrets
import socket
import signal
import subprocess
import threading
import time
from datetime import datetime
from typing import Any

from . import notify, state
from .artifacts import (
    bounded_regex_last_match,
    check_artifacts,
    check_declared_artifacts,
    unlink_artifact,
)
from .allocator import Allocator
from .executor import (
    Executor,
    _claim_abandoned_launch_intent,
    _is_strong_start_token,
    pid_cmdline_matches,
    process_start_token,
    read_tail,
    stage_checkpoint_valid,
)
from .fingerprint import compute_fingerprint
from .config import ConfigError, config_path, default_state_dir, load_config, parse_gpus, resolve_template, task_environment, project_gpu_enabled
from .schema import SchemaError, validate_batch, validate_persisted_dependencies, validate_project_gpu_access
from .native_exec import (
    NATIVE_EXEC_PROFILE_V2_SCHEMA,
    NATIVE_EXEC_V2_CONTRACT_FIELD,
    NativeExecProfileError,
    native_exec_project_roots,
    native_exec_project_root_identity_sha256,
    reattest_native_exec_profile,
)
from .native_launch import NativeLaunchUnavailable
from .templates import expand_cmd

POLL_SEC = 10
HEARTBEAT_SEC = 30
# RELEASE_TIMEOUT_SEC 迁至 allocator.py (原处为死常量, 消费方在 settle_releasing)
DEFAULT_MAX_RETRY = 1
RETRY_BACKOFF_SEC = 30  # 失败重试退避 (M2): 防秒级崩溃任务紧密崩溃循环
DEFAULT_GPU_JOB_CPUS = 8  # GPU 任务默认 CPU 占用 (NN 训练数据加载也要 CPU, config gpu_job_cpus 可覆盖)
DEFAULT_MAX_CPU_JOBS = 2  # cpus_total 未配置时回退: CPU-only 并发上限 (定案 7 旧语义)
DEFAULT_IDLE_TIMEOUT_MIN = 360  # 空转自动退出 (定案 38): 默认 6h, 0 = 禁用
LOCK_STARTUP_GRACE_SEC = 2 * HEARTBEAT_SEC
INBOX_MAX_BYTES = 1024 * 1024
INBOX_MAX_DEPTH = 64
INBOX_MAX_NODES = 100_000
JOB_STOP_TERM_GRACE_SEC = 5.0
JOB_STOP_KILL_GRACE_SEC = 5.0
JOB_STOP_POLL_SEC = 0.1
PROBE_READ_MAX_BYTES = 1024 * 1024
PROFILE_MAX_BYTES = 64 * 1024
PROFILE_MAX_PEAK_GIB = 1024.0
RC_MAX_BYTES = 32
INBOX_BID_MAX_LENGTH = 256
_INBOX_BID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")

_SIGNAL_SENT = "sent"
_SIGNAL_DEAD = "dead_or_mismatch"
_SIGNAL_UNKNOWN = "unknown_or_error"


def _native_root_identities(
    roots: dict[str, str],
) -> dict[str, tuple[int, int]]:
    identities: dict[str, tuple[int, int]] = {}
    for project, root in roots.items():
        root_stat = os.stat(root)
        if not stat.S_ISDIR(root_stat.st_mode):
            raise NativeExecProfileError(
                f"native project root is not a directory: {project!r}"
            )
        identities[project] = (root_stat.st_dev, root_stat.st_ino)
    return identities


def _validate_inbox_bid(value: object, batch_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("inbox bid must be a non-empty string")
    if (
        len(value) > INBOX_BID_MAX_LENGTH
        or value in (".", "..")
        or _INBOX_BID_RE.fullmatch(value) is None
        or not value.startswith(batch_name + "-")
    ):
        raise ValueError(
            "inbox bid must be a bounded safe identifier prefixed by batch name"
        )
    return value


def _load_bounded_submit_json(fd: int) -> object:
    """Read and structurally bound one already-open submit payload."""
    payload_stat = os.fstat(fd)
    if not stat.S_ISREG(payload_stat.st_mode):
        raise ValueError("payload 必须是普通文件")
    if payload_stat.st_size > INBOX_MAX_BYTES:
        raise ValueError(
            f"payload 过大 ({payload_stat.st_size} > {INBOX_MAX_BYTES} bytes)"
        )
    chunks: list[bytes] = []
    remaining = INBOX_MAX_BYTES + 1
    while remaining:
        chunk = os.read(fd, min(64 * 1024, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    payload = b"".join(chunks)
    if len(payload) > INBOX_MAX_BYTES:
        raise ValueError(f"payload 过大 (> {INBOX_MAX_BYTES} bytes)")
    try:
        value = json.loads(payload.decode("utf-8"))
    except UnicodeDecodeError as error:
        raise ValueError(f"payload 不是 UTF-8: {error}") from error
    except RecursionError as error:
        raise ValueError(
            f"payload JSON 嵌套过深 (上限 {INBOX_MAX_DEPTH})"
        ) from error
    except json.JSONDecodeError as error:
        raise ValueError(f"payload JSON 无效: {error}") from error

    nodes = 0
    stack = [(value, 1)]
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > INBOX_MAX_NODES:
            raise ValueError(
                f"payload JSON 节点过多 (上限 {INBOX_MAX_NODES})"
            )
        if depth > INBOX_MAX_DEPTH:
            raise ValueError(
                f"payload JSON 嵌套过深 (上限 {INBOX_MAX_DEPTH})"
            )
        if isinstance(current, dict):
            stack.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in current)
    return value

def _profile_cache_key(project: object, profile_key: object) -> str:
    """Encode the internal cache key without cross-project collisions."""
    return json.dumps(
        [project, profile_key],
        ensure_ascii=False,
        separators=(",", ":"),
    )

_FINGERPRINT_UNSET = object()
_NATIVE_EXEC_METADATA_KEYS = (
    "_native_exec_profile_id",
    "_native_exec_profile_sha256",
    "_native_exec_project_root_identity_sha256",
    "_native_exec_submitted_argv",
)


def _native_exec_metadata(spec: dict) -> dict[str, Any] | None:
    """Return a complete persisted tuple, rejecting partial internal state."""
    present = [key for key in _NATIVE_EXEC_METADATA_KEYS if key in spec]
    if not present:
        if NATIVE_EXEC_V2_CONTRACT_FIELD in spec:
            raise NativeExecProfileError(
                "persisted native V2 contract is missing its metadata tuple"
            )
        return None
    if len(present) != len(_NATIVE_EXEC_METADATA_KEYS):
        raise NativeExecProfileError(
            "persisted native execution metadata is incomplete"
        )
    metadata = {key: spec[key] for key in _NATIVE_EXEC_METADATA_KEYS}
    if NATIVE_EXEC_V2_CONTRACT_FIELD in spec:
        metadata[NATIVE_EXEC_V2_CONTRACT_FIELD] = spec[
            NATIVE_EXEC_V2_CONTRACT_FIELD
        ]
    return metadata


def _native_exec_fingerprint_kwargs(spec: dict) -> dict[str, str]:
    """Bind complete native profile/root identity into fingerprints."""
    digest = spec.get("_native_exec_profile_sha256")
    root_digest = spec.get("_native_exec_project_root_identity_sha256")
    if digest in (None, "") or root_digest in (None, ""):
        return {}
    return {
        "native_exec_profile_sha256": digest,
        "native_exec_project_root_identity_sha256": root_digest,
    }


def _persist_native_exec_metadata(source: dict, destination: dict) -> None:
    """Copy only a complete schema-issued native tuple into task storage."""
    metadata = _native_exec_metadata(source)
    if metadata is not None:
        destination.update(metadata)


def _inbox_task_spec(
    task: dict,
    expanded_cmd: list[str] | None,
    expanded_stages: list[dict] | None,
) -> dict[str, Any]:
    """Build the exact durable task payload used by inbox insertion."""
    persisted = {
        "id": task["id"],
        "cmd": expanded_cmd,
        "stages": expanded_stages,
        "cwd_abs": task["cwd_abs"],
        "git": task["git"],
        "env": task["env"],
        "resources": task["resources"],
        "duration_min": task["duration_min"],
        "max_retry": task["max_retry"],
        "artifacts": task["artifacts"],
        "paths_escape": task.get("paths_escape", False),
        "probes": task.get("probes"),
        "max_parallel": task.get("max_parallel"),
        "_force_rerun": task.get("_force_rerun"),
        "progress_regex": task.get("progress_regex"),
        "runtime": task.get("runtime"),
        "runtime_prefix": task.get("runtime_prefix"),
    }
    _persist_native_exec_metadata(task, persisted)
    return persisted


def _strict_inbox_delivery_matches(
    conn: sqlite3.Connection,
    bid: str,
    norm: dict,
    prepared_tasks: list[tuple],
) -> bool:
    """Prove an existing strict bid is the exact same durable delivery.

    Runtime/terminal columns are intentionally ignored.  Every immutable
    batch/task/job binding is compared, so a replay can be idempotent without
    treating a same-name preclaim or persisted tamper as a successful submit.
    """
    if norm.get("mode") != "strict":
        return False
    batch = conn.execute(
        "SELECT * FROM batches WHERE id=?",
        (bid,),
    ).fetchone()
    if batch is None:
        return False
    try:
        depends_on = json.loads(batch["depends_on"] or "[]")
        batch_env = json.loads(batch["env"] or "{}")
        batch_notify = (
            json.loads(batch["notify"])
            if batch["notify"] is not None
            else None
        )
    except (json.JSONDecodeError, TypeError):
        return False
    if (
        batch["name"] != norm["name"]
        or batch["mode"] != "strict"
        or depends_on != norm["depends_on"]
        or batch["gpus"] is not None
        or batch["cwd"] != norm["cwd"]
        or batch_env != norm["env"]
        or batch_notify != norm.get("notify")
        or batch["project"] != norm.get("project")
        or batch["priority"] != norm.get("priority", 0)
    ):
        return False

    task_rows = conn.execute(
        "SELECT * FROM tasks WHERE batch_id=? ORDER BY order_idx, id, version",
        (bid,),
    ).fetchall()
    job_rows = conn.execute(
        "SELECT * FROM jobs WHERE batch_id=? ORDER BY task_id, version, id",
        (bid,),
    ).fetchall()
    if len(task_rows) != len(prepared_tasks) or len(job_rows) != len(
        prepared_tasks
    ):
        return False

    for prepared, task_row, job_row in zip(
        prepared_tasks,
        task_rows,
        job_rows,
    ):
        index, task, cmd_e, stages_e, fingerprint, stage_fingerprints = prepared
        expected_spec = _inbox_task_spec(task, cmd_e, stages_e)
        try:
            persisted_spec = json.loads(task_row["spec"])
            persisted_stage_fingerprints = (
                json.loads(job_row["stage_fingerprints"])
                if job_row["stage_fingerprints"] is not None
                else None
            )
        except (json.JSONDecodeError, TypeError):
            return False
        expected_stage_fingerprints = (
            stage_fingerprints if stage_fingerprints else None
        )
        expected_job_id = f"{bid}-{task['id']}-v1"
        if (
            task_row["id"] != task["id"]
            or task_row["version"] != 1
            or task_row["order_idx"] != index
            or task_row["project"] != norm.get("project")
            or persisted_spec != expected_spec
            or job_row["id"] != expected_job_id
            or job_row["batch_id"] != bid
            or job_row["task_id"] != task["id"]
            or job_row["version"] != 1
            or job_row["project"] != norm.get("project")
            or job_row["fingerprint"] != fingerprint
            or persisted_stage_fingerprints != expected_stage_fingerprints
        ):
            return False
    return True


# B12-a: 配置冷键 —— 变更拒绝热更新, 必须重启 daemon (调研 §2.3).


def _validate_inbox_dependencies(conn, norm: dict) -> None:
    """Recheck dependency existence and cycles on the daemon's DB connection."""
    validate_persisted_dependencies(
        conn,
        norm["name"],
        norm["depends_on"],
    )
# gpus 卡集/容量另经 parse_gpus 结构比对, 不在本列表.
CONFIG_COLD_KEYS = (
    "node",
    "state_dir",
    "user",
    "schema_version",
    "native_exec_profiles",
)


class Dispatcher:
    def __init__(self, cfg: dict, fake: bool = False):
        self.cfg = cfg
        self._native_exec_project_roots = native_exec_project_roots(cfg)
        self._native_exec_project_root_identities = _native_root_identities(
            self._native_exec_project_roots
        )
        self.fake = fake
        self.state_dir = state.default_state_dir()
        self.host_dir = state.ensure_private_directory(state.host_dir())
        self.pid_file = os.path.join(self.host_dir, "daemon.pid")
        self.heartbeat_file = os.path.join(self.host_dir, "daemon.heartbeat")
        # B26: tick_ok —— heartbeat=活着, tick_ok=主循环在正常完成调度轮

        self.tick_ok_file = os.path.join(self.host_dir, "daemon.tick_ok")

        self._prev_hb_ts: float | None = None  # acquire_lock 触心跳前采样 (D4 用)
        self._notify_threads: list[threading.Thread] = []  # 在途通知线程 (退出前 join)
        self._probe_offsets: dict[str, int] = {}  # job_id -> 日志已扫字节偏移 (P2)
        self.lock_dir = os.path.join(self.host_dir, "dispatcher.lock")
        self._lease_owner: dict | None = None
        self._lease_id: str | None = None
        self.log = state.open_private_text(
            os.path.join(self.host_dir, "scheduler.log"),
            "a",
        )
        # B13-§1: 环境净化默认开 (sanitize_env: false 可关回旧行为)
        self.executor = Executor(sanitize_env=bool(cfg.get("sanitize_env", True)))
        self._launch_inflight: dict[str, int] = {}
        # gpus 归一化 (2026-08-17 缺口 1/2): 纯卡号数组或 {idx,mem_gib} 对象数组;
        # config 未配 -> Allocator 自动探测全卡 (定案 1 第三级回退)
        from .config import parse_gpus

        gpu_list, mem_overrides, gpu_max_jobs = parse_gpus(cfg)
        self.allocator = Allocator(
            gpu_list=gpu_list, fake=fake, mem_overrides=mem_overrides,
        )
        # B12-c: 每卡共享打包上限 (热键, 热更新时刷新)
        self._gpu_max_jobs = gpu_max_jobs
        self._cap_warned: set[str] = set()
        self._last_progress_scan = 0.0   # B13-§5: 进度扫描节流   # 已告警过"等打包上限"的 job (warn-once)
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
        # F2: 每卡显存采样环形缓冲 [(epoch_sec, used_gib)], 上限 ~2h (60s/点).
        # 仅内存, daemon 重启丢失 —— 快照取"事发前时间线"用, 可接受 (定案 §2.5).
        self._mem_samples: dict[int, list[tuple[float, float]]] = {}
        self._co_locate = bool(cfg.get("co_locate", False))
        # B12-a: 钉住 host 目录名 —— node 属冷键, 文件后续变更被热更新拒绝,
        # 但 state.connect() 若仍实时解析会把 DB 路径漂移到新节点的空目录。
        # 钉住后 daemon 进程终身使用启动时目录; 重启后才接受新 node。
        state.pin_hostname(state.hostname())
        # 配置热更新 —— 基线路径与 mtime; 缓存字段刷新清单见 _reload_config_now
        self._config_path = config_path()
        try:
            self._cfg_mtime = os.stat(self._config_path).st_mtime
        except OSError:
            self._cfg_mtime = 0.0
        # 审查 B1: 优雅停止请求标志 (信号处理器只置此标志, 主循环 tick 边界消费).
        # 内存态 (无跨进程语义), SIGTERM/SIGINT handler 调用 request_stop().
        self._stop_requested = False

        # B11c: project config cache
        self._projects = cfg.get("projects", {})
        self._project_quota_used = {}

    # B11c: project config / quota / priority / affinity
    def _get_project_config(self, project):
        if not project:
            return {"gpu_quota": 0, "priority": 0, "affinity": []}
        proj = self._projects.get(project, {})
        return {
            "gpu_quota": int(proj.get("gpu_quota", 0) or 0),
            "priority": int(proj.get("priority", 0) or 0),
            "affinity": proj.get("gpu_affinity", []),
            # B11c 补充: 硬隔离 -- true 时任务只能落在 affinity 列内的卡,
            # 亲和卡全忙则排队等待(不外借其他卡)。防多项目混卡 OOM。
            "affinity_hard": bool(proj.get("gpu_affinity_hard", False)),
        }

    def _update_project_quota_used(self, conn) -> None:
        self._project_quota_used.clear()
        rows = conn.execute(
            "SELECT COALESCE(j.project, b.project) AS project, COUNT(*) AS n"
            " FROM jobs j JOIN batches b ON b.id=j.batch_id"
            " WHERE j.status='running' AND j.gpu IS NOT NULL"
            "   AND COALESCE(j.project, b.project) IS NOT NULL"
            " GROUP BY COALESCE(j.project, b.project)"
        ).fetchall()
        for row in rows:
            self._project_quota_used[row["project"]] = row["n"]

    def _project_quota_available(self, conn, project) -> bool:
        if not project:
            return True
        quota = self._get_project_config(project)["gpu_quota"]
        if quota <= 0:
            return True
        return self._project_quota_used.get(project, 0) < quota

    def _project_priority(self, project) -> int:
        if not project:
            return 0
        return self._get_project_config(project)["priority"]

    def _project_affinity(self, project):
        if not project:
            return []
        return self._get_project_config(project)["affinity"]

    def _project_affinity_hard(self, project) -> bool:
        if not project:
            return False
        return self._get_project_config(project)["affinity_hard"]


    def request_stop(self) -> None:
        """信号处理器入口: 只置标志 (绝不在 handler 里开 DB 连接)."""
        self._stop_requested = True

    def log_line(self, msg: str) -> None:
        # B13-§6c: 毫秒精度 (并发问题排查); state.now() 保持秒级 (DB 字段兼容)
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        line = f"[{ts}] {msg}"
        print(line, flush=True)
        self.log.write(line + "\n")
        self.log.flush()

    # ---------- 单实例锁 ----------

    def acquire_lock(self) -> bool:
        """Acquire an exact process lease, reclaiming only a rechecked dead owner."""
        physical_host = socket.gethostname().strip()
        if not physical_host:
            self.log_line("dispatcher physical_host 为空，拒绝发布 lease")
            return False
        lock_exists = os.path.isdir(self.lock_dir)
        observed_present = self._lock_owner_entry_present() if lock_exists else False
        observed = self._read_lock_owner() if lock_exists else None
        if lock_exists and observed is None and observed_present:
            self.log_line("dispatcher lock owner 无法验证，拒绝回收")
            return False
        if lock_exists:
            try:
                lock_age = max(0.0, time.time() - os.path.getmtime(self.lock_dir))
            except OSError:
                return False
            if lock_age < LOCK_STARTUP_GRACE_SEC:
                if observed is None:
                    self.log_line("检测到新鲜 ownerless dispatcher lock, 视为启动中")
                return False
            if observed is not None and self._lock_owner_is_live(observed):
                return False

        with self._serialized_lock_update():
            if os.path.isdir(self.lock_dir):
                current_present = self._lock_owner_entry_present()
                current = self._read_lock_owner()
                if current != observed or current_present != observed_present:
                    return False
                try:
                    lock_age = max(0.0, time.time() - os.path.getmtime(self.lock_dir))
                except OSError:
                    return False
                if lock_age < LOCK_STARTUP_GRACE_SEC:
                    return False
                if current is not None and self._lock_owner_is_live(current):
                    return False
                if not self._remove_exact_lock(current):
                    return False

            try:
                os.mkdir(self.lock_dir, 0o700)
                os.chmod(self.lock_dir, 0o700)
            except FileExistsError:
                return False

            pid = os.getpid()
            self._lease_owner = {
                "schema_version": 1,
                "lease_id": secrets.token_hex(16),
                "pid": pid,
                "start_token": self._proc_start_time(pid),
                "physical_host": physical_host,
            }
            self._lease_id = self._lease_owner["lease_id"]
            try:
                self._publish_lock_owner()
                self._atomic_write(self.pid_file, str(pid))
                # H2: sample the old heartbeat before publishing the new heartbeat.
                try:
                    self._prev_hb_ts = os.path.getmtime(self.heartbeat_file)
                except OSError:
                    self._prev_hb_ts = None
                self._touch_heartbeat()
            except OSError:
                self._remove_exact_lock(self._lease_owner)
                self._lease_owner = None
                self._lease_id = None
                raise
        return True

    def _lock_owner_file(self) -> str:
        return os.path.join(self.lock_dir, "owner.json")

    def _lock_guard_file(self) -> str:
        return f"{self.lock_dir}.guard"

    @contextmanager
    def _serialized_lock_update(self):
        """Serialize lease replacement and cleanup across dispatcher processes."""
        with state.open_private_text(self._lock_guard_file(), "a+") as guard:
            fcntl.flock(guard.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(guard.fileno(), fcntl.LOCK_UN)

    def _read_lock_owner(self) -> dict | None:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        flags |= getattr(os, "O_NONBLOCK", 0)
        try:
            fd = os.open(self._lock_owner_file(), flags)
        except OSError:
            return None
        try:
            owner_stat = os.fstat(fd)
            if (
                not stat.S_ISREG(owner_stat.st_mode)
                or owner_stat.st_uid != os.getuid()
                or owner_stat.st_size > 4096
                or owner_stat.st_nlink != 1
            ):
                return None
            raw = os.read(fd, 4097)
            if len(raw) > 4096:
                return None
            owner = json.loads(raw.decode("utf-8"))
            if not isinstance(owner, dict):
                return None
            if owner.get("schema_version") != 1:
                return None
            lease_id = owner.get("lease_id")
            if not isinstance(lease_id, str) or not lease_id:
                return None
            physical_host = owner.get("physical_host")
            if not isinstance(physical_host, str) or not physical_host.strip():
                return None
            pid = owner.get("pid")
            if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
                return None
            owner = dict(owner)
            owner["pid"] = pid
            owner["physical_host"] = physical_host.strip()
            return owner
        except (
            OSError,
            ValueError,
            TypeError,
            UnicodeDecodeError,
            json.JSONDecodeError,
            RecursionError,
        ):
            return None
        finally:
            os.close(fd)

    def _lock_owner_entry_present(self) -> bool:
        """Distinguish a missing startup owner from an invalid/unreadable entry."""
        try:
            os.lstat(self._lock_owner_file())
            return True
        except FileNotFoundError:
            return False
        except OSError:
            return True

    def _owns_current_lease(self) -> bool:
        """Return whether this process still owns the published dispatcher lease."""
        owner = getattr(self, "_lease_owner", None)
        return isinstance(owner, dict) and self._read_lock_owner() == owner

    def _lock_owner_is_live(self, owner: dict) -> bool:
        local_host = socket.gethostname().strip()
        owner_host = owner.get("physical_host")
        if (
            not local_host
            or not isinstance(owner_host, str)
            or owner_host.strip() != local_host
        ):
            return True
        pid = owner["pid"]
        if not self._pid_exists(pid):
            return False
        expected_start = owner.get("start_token")
        if not _is_strong_start_token(expected_start):
            return True
        actual_start = self._proc_start_time(pid)
        # Failure to prove PID reuse must never authorize reclaiming a live PID.
        return actual_start is None or actual_start == expected_start

    def _remove_exact_lock(self, expected_owner: dict | None) -> bool:
        """Remove the currently observed lease only if its owner is unchanged."""
        if self._read_lock_owner() != expected_owner:
            return False
        try:
            os.unlink(self._lock_owner_file())
        except FileNotFoundError:
            if expected_owner is not None:
                return False
        except OSError:
            return False
        try:
            os.rmdir(self.lock_dir)
        except OSError:
            return False
        return True

    def _atomic_write(self, path: str, content: str) -> None:
        tmp = f"{path}.{os.getpid()}.tmp"
        try:
            with state.open_private_text(tmp, "w") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(tmp, path)
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    def _publish_lock_owner(self) -> None:
        owner = getattr(self, "_lease_owner", None)
        if (
            not isinstance(owner, dict)
            or owner.get("schema_version") != 1
            or not isinstance(owner.get("physical_host"), str)
            or not owner["physical_host"].strip()
        ):
            raise OSError("dispatcher lease owner is not initialized")
        self._atomic_write(self._lock_owner_file(), json.dumps(owner))


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


    def _touch_heartbeat(self) -> None:
        state.touch_private_file(self.heartbeat_file)

    # B26: tick_ok = 调度主循环健康的真信号 (heartbeat 只是进程活性)
    _touch_tick_ok_ts: float = 0.0
    _frozen_incident_at: float = 0.0

    def _touch_tick_ok(self) -> None:
        state.touch_private_file(self.tick_ok_file)
        self._touch_tick_ok_ts = time.time()

    def tick_ok_age(self) -> float | None:
        """tick_ok 文件年龄 (秒); 无文件返回 None (daemon 尚未完成过任何 tick)."""
        try:
            return max(0.0, time.time() - os.path.getmtime(self.tick_ok_file))
        except OSError:
            return None

    def _check_frozen(self) -> None:
        """tick_ok 停更超 HEARTBEAT_SEC*3 (90s) -> incident + log (10 分钟节流).

        调用点: run() 主循环 sleep 片内 (1s 粒度), 不占 tick 预算.
        """
        try:
            age = self.tick_ok_age()
        except Exception:
            return
        if age is None or age < HEARTBEAT_SEC * 3:
            return
        now = time.time()
        if now - self._frozen_incident_at < 600:
            return
        self._frozen_incident_at = now
        msg = f"daemon 主循环停摆 {int(age)}s 未完成任何 tick (疑似 NFS/nvidia-smi 阻塞)"
        self.log_line(f"B26 冻结检测: {msg}")
        try:
            with state.connect() as conn:
                state.insert_incident(
                    conn, ts=state.now(), kind="daemon_frozen",
                    gpu_idx=None, job_id=None, batch_id=None,
                    payload_json=json.dumps({
                        "frozen_sec": int(age),
                        "verdicts": ["主循环阻塞在探测/NFS; 检查 GPU 健康与 NFS 延迟"],
                    }, ensure_ascii=False),
                )
        except Exception:
            pass

    def _cleanup_lock(self) -> None:
        # 通知线程收尾: 退出前等在途通知发完 (超时则放弃, 记 log)
        for thread in getattr(self, "_notify_threads", ()):
            thread.join(timeout=10)
            if thread.is_alive():
                self.log_line("notify 线程超时未结束, 放弃等待")
        owner = getattr(self, "_lease_owner", None)
        if not isinstance(owner, dict):
            return
        # Keep replacement serialized until all shared lease sidecars are gone;
        # otherwise an exiting owner could unlink its successor's PID/heartbeat.
        with self._serialized_lock_update():
            if not self._remove_exact_lock(owner):
                return
            try:
                os.unlink(self.pid_file)
            except OSError:
                pass
            # 优雅退出必须停心跳; 崩溃路径保留并等待自然过期。
            try:
                os.unlink(self.heartbeat_file)
            except OSError:
                pass
            self._lease_owner = None
            self._lease_id = None

    def _wait_for_job_states(self, jobs, timeout: float) -> dict[str, str]:
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            states = {
                str(job["id"]): (
                    self._job_process_state(job) if job["pgid"] else "dead"
                )
                for job in jobs
            }
            if all(
                process_state in {"dead", "mismatch"}
                for process_state in states.values()
            ):
                return states
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return states
            time.sleep(min(JOB_STOP_POLL_SEC, remaining))

    def stop(self) -> bool:
        with state.submission_lock():
            try:
                state.mark_idle_shutdown()
            except OSError as error:
                self.log_line(f"stop 无法发布 shutdown 栅栏: {error}")
                return False
            return self._stop_locked()

    def _stop_locked(self) -> bool:
        """Terminate exact owned groups, settling state only after proven exit."""
        self._recover_launch_markers()
        try:
            with state.connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM jobs WHERE status='running'"
                ).fetchall()
                native_session_ids = {
                    row["job_id"]
                    for row in conn.execute("SELECT job_id FROM native_sessions")
                }
        except sqlite3.Error as error:
            self.log_line(f"stop 读取 running jobs 失败: {error}")
            return False

        # A native session can only be claimed from pending, so no session can
        # attach to these already-running rows after this snapshot.  Recheck
        # inside the settlement writer transaction as well.
        legacy_rows = [j for j in rows if j["id"] not in native_session_ids]
        for job in legacy_rows:
            if not job["pgid"]:
                continue
            process_state = self._job_process_state(job)
            if process_state == "alive":
                self._signal_job(job, signal.SIGTERM)
            elif process_state == "group_alive":
                self.log_line(
                    f"stop job {job['id']}: leader 已退出，永不向复用风险进程组发信号"
                )
            elif process_state == "unknown":
                self.log_line(
                    f"stop 保留 running job {job['id']}: process identity 无法确认"
                )

        after_term = self._wait_for_job_states(
            legacy_rows,
            JOB_STOP_TERM_GRACE_SEC,
        )
        for job in legacy_rows:
            if after_term.get(str(job["id"])) != "alive":
                continue
            self._signal_job(job, signal.SIGKILL)
        self._wait_for_job_states(legacy_rows, JOB_STOP_KILL_GRACE_SEC)

        settled_rows = []
        settlement_ok = False
        remaining_running = 1
        unresolved_native = 1
        for attempt in range(3):
            try:
                with state.connect() as conn:
                    if not conn.in_transaction:
                        conn.execute("BEGIN IMMEDIATE")
                    settled_rows = []
                    for original in rows:
                        current = state.get_job(conn, original["id"])
                        if current is None or current["status"] != "running":
                            continue
                        if current["pgid"] != original["pgid"]:
                            self.log_line(
                                f"stop 保留 job {original['id']}: DB pgid 已变化"
                            )
                            continue
                        if self._has_native_session(conn, current["id"]):
                            self.log_line(
                                f"stop 保留 native session job {current['id']}: "
                                "无原始 owner/wait 证明"
                            )
                            continue
                        if (
                            not current["pgid"]
                            and self._prepare_launch_marker(current)
                        ):
                            self.log_line(
                                f"stop 保留 job {current['id']}: "
                                "pgid 未回写且 launch marker 无法安全收敛"
                            )
                            continue
                        process_state = (
                            self._job_process_state(current)
                            if current["pgid"]
                            else "dead"
                        )
                        if process_state not in {"dead", "mismatch"}:
                            self.log_line(
                                f"stop 保留 running job {current['id']}: "
                                f"process identity={process_state}"
                            )
                            continue
                        state.update_job(
                            conn,
                            current["id"],
                            status="cancelled",
                            kill_reason="cancelled",
                            rc=137 if current["rc"] is None else current["rc"],
                            finished_at=state.now(),
                        )
                        if current["gpu"] is not None:
                            self._release_in_tx(conn, current["id"])
                        settled_rows.append(current)
                    # A native claim can commit after the initial running
                    # snapshot.  Decide stop completion from this writer's
                    # current state, not only from the old row count.
                    remaining_running = conn.execute(
                        "SELECT COUNT(*) FROM jobs WHERE status='running'"
                    ).fetchone()[0]
                    unresolved_native = conn.execute(
                        "SELECT COUNT(*) FROM native_sessions n"
                        " JOIN jobs j ON j.id=n.job_id"
                        " WHERE j.status='running'"
                    ).fetchone()[0]
                settlement_ok = True
                break
            except sqlite3.OperationalError as error:
                if attempt == 2 or "locked" not in str(error).lower():
                    self.log_line(f"stop 收尾失败: {error}")
                    break
                time.sleep(1)
            except sqlite3.Error as error:
                self.log_line(f"stop 收尾失败: {error}")
                break

        for job in settled_rows if settlement_ok else ():
            self._drop_job_rc(job)
            self._drop_profile(job)
            self._drop_launch_marker(job)

        self._recover_launch_markers()
        completed = (
            settlement_ok
            and len(settled_rows) == len(rows)
            and remaining_running == 0
            and unresolved_native == 0
            and not self._unresolved_launch_markers()
        )
        if completed:
            self._cleanup_lock()
        else:
            self.log_line("stop 未完成: 保留 running/resources/lease 并继续重试")
        return completed

    # ---------- 主循环 ----------

    def run(self, once: bool = False) -> None:
        state.clear_idle_shutdown()
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
        # 接管 legacy running；持久 native session 留待原始 owner/wait 路径。
        self._recover_launch_markers()
        self._adopt_running()

        tick_failures = 0
        while True:
            if not self._owns_current_lease():
                self.log_line("dispatcher lease 已丢失，停止旧实例")
                break
            try:
                self._heartbeat()
                if self._stop_requested:
                    self.log_line("收到停止请求 (tick 边界), 收尾未完成任务")
                    if self.stop():
                        break
                    continue
                if self._idle_check():
                    break
                self._tick()
                self._touch_tick_ok()  # B26: tick 完成才写 —— 调度健康真信号
                if self._drain_complete():
                    break
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
                self._check_frozen()  # B26: 不占 tick 预算的调度健康看门狗 (1s 粒度)
        self._cleanup_lock()

    def _submit_inbox_pending(self) -> bool:
        state_dir = default_state_dir()
        node = str(self.cfg.get("node") or state.hostname())
        inbox_dir = os.path.join(state_dir, node, "submit_inbox")
        try:
            return any(
                name.startswith("submit-") and name.endswith(".json")
                for name in os.listdir(inbox_dir)
            )
        except FileNotFoundError:
            return False
        except OSError:
            # Failure to inspect an existing inbox must not let idle shutdown
            # strand a payload; the next tick will retry the scan.
            return True

    def _idle_check(self) -> bool:
        """定案 38: 连续 idle 超时优雅退出.

        idle = 没有 running，且 active/queued 批次没有最新代际的
        pending/waiting 任务。终态批次和旧代际遗留的 pending 不可派发，
        不能让 daemon 永久无法 idle 退出；但任一未决 launch marker 仍可能
        代表 crash-window 进程，必须全局 fail-closed。
        返回 True = 触发退出 (主循环 break, 随后 _cleanup_lock).
        """
        from .resources import drain_state
        if drain_state() is not None:
            return False  # Explicit drain controls its own shutdown boundary.
        if self.idle_timeout_min <= 0:
            return False  # 0 = 禁用
        activity_query = (
            "SELECT COUNT(*) FROM jobs j JOIN batches b ON b.id=j.batch_id"
            " WHERE j.status='running' OR ("
            "   b.status IN ('active','queued')"
            "   AND j.status IN ('pending','waiting_quota','waiting_dep')"
            "   AND j.version=(SELECT MAX(j2.version) FROM jobs j2"
            "     WHERE j2.batch_id=j.batch_id AND j2.task_id=j.task_id)"
            " )"
        )
        with state.submission_lock():
            with state.connect() as conn:
                n = conn.execute(activity_query).fetchone()[0]
                pending_control = conn.execute(
                    "SELECT COUNT(*) FROM control_requests WHERE status='pending'"
                ).fetchone()[0]
            now = time.time()
            inbox_pending = self._submit_inbox_pending()
            unresolved_launch = self._unresolved_launch_markers()
            if n > 0 or pending_control > 0 or inbox_pending or unresolved_launch:
                self.last_activity = now
                return False
            if now - self.last_activity >= self.idle_timeout_min * 60:
                # File/inbox checks stay outside the writer.  Recheck DB
                # activity under the writer and publish the stop marker before
                # releasing it so a native claim cannot appear in between.
                with state.connect() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    if (
                        conn.execute(activity_query).fetchone()[0] > 0
                        or conn.execute(
                            "SELECT 1 FROM control_requests"
                            " WHERE status='pending' LIMIT 1"
                        ).fetchone() is not None
                    ):
                        self.last_activity = now
                        return False
                    state.mark_idle_shutdown()
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
                # F2: 时间线采样 (冻结判定与快照共用同一次读数, 零额外开销)
                buf = self._mem_samples.setdefault(idx, [])
                buf.append((time.time(), round(float(used), 2)))
                if len(buf) > 120:
                    del buf[: len(buf) - 120]
                if used > freeze_pct / 100.0 * cap:
                    if idx not in self._frozen_gpus:
                        self._frozen_gpus.add(idx)
                        self.log_line(f"L3 冻结: GPU{idx} 装箱显存 {used:.1f}/{cap:.0f} GiB")
                else:
                    self._frozen_gpus.discard(idx)

    def _tick(self) -> None:
        self._maybe_reload_config()  # B12-a: 配置热更新 (mtime 变化时)
        self._drain_submit_inbox()  # C2: 单写者 inbox 文件补插请求行
        self._process_control_requests()  # 事故记录 4: cancel 转发 daemon, kill 前处理
        self._prune_control_requests()  # L11: 有界保留已完成控制请求
        self._prune_notify_threads()  # L11: 清理已结束通知线程引用
        self._check_timeouts()  # H6: duration_min 超时看门狗, kill 后交 reap 收尾
        self._check_probes()  # L6: 日志门控 (fail_on_log/ready_on_log), kill 后交 reap 收尾
        self._recover_launch_markers()
        # Revisit every running row that still lacks a pgid on every tick, not
        # only when a marker was claimed in this tick.  This also converges
        # safely after "claim succeeded, DB settlement failed": on the next
        # tick the marker is absent.  Legacy adoption then settles the
        # pre-Popen failure; a durable native session remains unresolved.
        self._adopt_running(unidentified_only=True)
        self._reap_finished_jobs()
        _freed, _to = self.allocator.settle_releasing()
        for g in _to:
            self._diag_unreleased(g)  # 事故记录 4 建议 3: 超时未释放 -> 诊断输出
        self._l3_freeze_sample()
        self._scan_progress()   # B13-§5: 进度正则解析 (30s 节流)
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
        try:
            notify.cleanup_acked()  # 顺带清理 7 天前已确认通知 (设计 §6)
        except Exception as e:
            self.log_line(f"notify 清理异常: {e}")

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
        # Terminal success for skip rows depends on external artifacts.  Share
        # clean's gate across validation, status publication, marker ownership,
        # and event snapshot so a concurrent deletion cannot invalidate a
        # success decision immediately after it is made.
        with state.submission_lock():
            self._settle_batch_status_serialized()

    def _terminal_job_successful(self, conn, job) -> bool:
        if job["status"] not in ("done", "skip"):
            return False
        if job["status"] == "skip" and (
            not isinstance(job["fingerprint"], str)
            or not job["fingerprint"]
        ):
            return False
        try:
            spec = self._load_task_spec(conn, job)
        except ValueError as error:
            self.log_line(
                f"job {job['id']} skip 成功态校验失败: {error}"
            )
            return False
        return check_declared_artifacts(
            spec,
            spec.get("cwd_abs") or ".",
        )

    def _settle_batch_status_serialized(self) -> None:
        """P1: 批次终态. done = 全部任务成功终态 (done/skip);
        任一 failed/blocked/cancelled/timed_out -> blocked (interrupted 除外 R4).

        blocked 只能由 retry/resubmit 显式重开；done 可由 resubmit，或确有
        最新 skip 重排的 clean 显式重开。daemon 不根据历史遗留的
        pending/waiting 行猜测人工意图。"""
        marker_effects: list[tuple[str, str, str, str, str | None]] = []
        with state.connect() as conn:
            # Status snapshots and terminal publication must be one writer
            # transaction.  Otherwise retry/resubmit/discard can commit after
            # our read and be overwritten by a stale done/blocked decision.
            if not conn.in_transaction:
                conn.execute("BEGIN IMMEDIATE")
            batches = conn.execute(
                "SELECT * FROM batches WHERE status IN ('active','blocked')"
            ).fetchall()
            for b in batches:
                # B17: 只统计每任务最新版本 —— 旧版本的失败终态行不应永久
                # 把批次钉在 blocked (否则 resubmit 新版本后批次无法回 active,
                # 新 pending 全部冻结)。与 C4/retry
                # 的"每 task 取最新 version"口径一致。
                jobs = conn.execute(
                    "SELECT j.* FROM jobs j"
                    " JOIN (SELECT task_id, MAX(version) AS mv FROM jobs"
                    "       WHERE batch_id=? GROUP BY task_id) t"
                    "   ON j.batch_id=? AND j.task_id=t.task_id AND j.version=t.mv",
                    (b["id"], b["id"]),
                ).fetchall()
                if not jobs:
                    continue
                statuses = [j["status"] for j in jobs]
                stale_running = conn.execute(
                    "SELECT 1 FROM jobs j"
                    " JOIN (SELECT task_id, MAX(version) AS mv FROM jobs"
                    "       WHERE batch_id=? GROUP BY task_id) t"
                    "   ON j.batch_id=? AND j.task_id=t.task_id"
                    " WHERE j.version < t.mv"
                    "   AND j.status='running'"
                    " LIMIT 1",
                    (b["id"], b["id"]),
                ).fetchone()
                nominal_terminal_success = all(
                    status in ("done", "skip") for status in statuses
                )
                terminal_success = nominal_terminal_success and all(
                    self._terminal_job_successful(conn, j) for j in jobs
                )
                if nominal_terminal_success and not terminal_success:
                    if b["status"] == "active":
                        changed = conn.execute(
                            "UPDATE batches SET status='blocked'"
                            " WHERE id=? AND status='active'",
                            (b["id"],),
                        ).rowcount
                        if changed:
                            marker_effects.append(
                                (
                                    "write",
                                    b["id"],
                                    b["name"],
                                    "blocked",
                                    "done/skip 声明产物已缺失或失效",
                                )
                            )
                    continue
                unresolved_launch = (
                    terminal_success
                    and self._batch_has_unresolved_launch_marker(conn, b["id"])
                )
                if terminal_success and (stale_running or unresolved_launch):
                    # 老版本仍在跑，或任一代际还留有未决 launch marker：
                    # 最新版本虽成功，批次也不可提前收敛。
                    continue
                if terminal_success:
                    changed = conn.execute(
                        "UPDATE batches SET status='done' WHERE id=?"
                        " AND status IN ('active','blocked')",
                        (b["id"],),
                    ).rowcount
                    if changed != 1:
                        continue
                    marker_effects.append(
                        (
                            "write",
                            b["id"],
                            b["name"],
                            "done",
                            f"{len(statuses)} 任务全部成功终态 (done/skip)",
                        )
                    )
                elif any(
                    s in ("failed", "blocked", "cancelled", "timed_out")
                    for s in statuses
                ):
                    if b["status"] == "active":
                        changed = conn.execute(
                            "UPDATE batches SET status='blocked'"
                            " WHERE id=? AND status='active'",
                            (b["id"],),
                        ).rowcount
                        if changed != 1:
                            continue
                        fails = [
                            r["task_id"] for r in conn.execute(
                                "SELECT task_id FROM jobs WHERE batch_id=? AND status IN"
                                " ('failed','blocked','cancelled','timed_out')",
                                (b["id"],),
                            ).fetchall()
                        ]
                        marker_effects.append(
                            (
                                "write",
                                b["id"],
                                b["name"],
                                "blocked",
                                f"失败任务: {','.join(fails) if fails else '-'}",
                            )
                        )

        for operation, batch_id, name, kind, detail in marker_effects:
            # Marker ownership and event snapshot share one serialization
            # interval: either this terminal generation publishes both, or a
            # retry/resubmit/new same-name generation wins and both are skipped.
            try:
                with state.submission_lock():
                    applied = self._apply_terminal_marker_effect(
                        operation,
                        batch_id,
                        name,
                        kind,
                        detail,
                    )
                    if not applied:
                        continue
                    if operation == "write":
                        self.log_line(
                            f"批次 {name} {kind} "
                            + (
                                "(全部任务成功终态)"
                                if kind == "done"
                                else "(有失败任务, 等人工)"
                            )
                        )
                        self._notify_batch(batch_id, name, kind)
            except (OSError, sqlite3.Error, state.StateError) as error:
                self.log_line(
                    f"批次 {name} terminal effect 序列化失败，保留状态: {error}"
                )

    # ---------- 批次终态通知 (现行配置见 docs/reference.md) ----------

    def _notify_batch(
        self,
        batch_id: str,
        batch_name: str,
        expected_status: str,
    ) -> bool:
        """批次进终态 -> 异步投递 (email/file 渠道). 故障只记日志, 绝不影响调度.

        - 一次性迁移点触发 (与 _write_marker 同处), 天然去重无需已发记录
        - 批次级覆盖 (设计 §3): batches.notify=false 关; {"email_to": [...]} 改收件人
        - 构造前在 submission gate 内确认仍是同名最新的预期终态；批次若已
          retry/resubmit 重开为 active，绝不能被 build_event 误编码为 blocked
        - 发送在 daemon 线程, 退出时 _cleanup_lock join 等发完 (定案 38 交互)
        """
        self._prune_notify_threads()
        try:
            if expected_status not in ("done", "blocked"):
                return False
            with state.submission_lock():
                with state.connect() as conn:
                    if not conn.in_transaction:
                        conn.execute("BEGIN")
                    latest = conn.execute(
                        "SELECT id, status FROM batches WHERE name=?"
                        " ORDER BY created_at DESC, rowid DESC LIMIT 1",
                        (batch_name,),
                    ).fetchone()
                    if (
                        latest is None
                        or latest["id"] != batch_id
                        or latest["status"] != expected_status
                    ):
                        return False
                    b = state.get_batch(conn, batch_id)
                    if b is None or b["status"] != expected_status:
                        return False
                    bnf = json.loads(b["notify"]) if b["notify"] else None
                    if bnf is False:
                        return False  # 批次级关闭
                    ncfg = self.cfg.get("notify") or {}
                    if not ncfg:
                        return False  # 全局未配置 = 功能关闭
                    cfg = self.cfg
                    if (
                        isinstance(bnf, dict)
                        and bnf.get("email_to")
                        and ncfg.get("email")
                    ):
                        # 批次级改收件人: 浅拷覆盖, 不动全局 cfg
                        cfg = dict(self.cfg)
                        em = dict(ncfg["email"])
                        em["to"] = bnf["email_to"]
                        cfg["notify"] = dict(ncfg, email=em)
                    event = notify.build_event(conn, b, self.host_dir)

            def _send() -> None:
                try:
                    for r in notify.send(event, cfg):
                        if not r.startswith("ok:"):
                            self.log_line(f"notify [{batch_name}]: {r}")
                except Exception as e:  # noqa: BLE001 — 通知绝不影响调度 (§6)
                    self.log_line(f"notify [{batch_name}] 线程异常: {e}")

            t = threading.Thread(target=_send, daemon=True)
            t.start()
            self._notify_threads.append(t)
            return True
        except Exception as e:  # noqa: BLE001 — 构造事件失败也不影响批次收敛
            self.log_line(f"notify [{batch_name}] 构造失败: {e}")
            return False

    # ---------- P7: 批次终态 marker (2026-08-15) ----------
    def _marker_dir(self) -> str:
        # 决策 5B: 按节点隔离 ({STATE}/<hostname>/markers) —— 共享 NFS 多节点
        # 时同名批次 marker 不再互相覆盖 (与 state.db/logs/profiles 一致)
        d = os.path.join(self.host_dir, "markers")
        return state.ensure_private_directory(d)

    def _apply_terminal_marker_effect(
        self,
        operation: str,
        batch_id: str,
        name: str,
        kind: str,
        detail: str | None,
    ) -> bool:
        """Apply a marker effect only while its batch owns the shared name.

        Submission mutations use the same short lock, so validation and file
        replacement cannot race a newer same-name batch becoming authoritative.
        """
        try:
            with state.submission_lock():
                with state.connect() as conn:
                    latest = conn.execute(
                        "SELECT id, status FROM batches WHERE name=?"
                        " ORDER BY created_at DESC, rowid DESC LIMIT 1",
                        (name,),
                    ).fetchone()
                if not latest or latest["id"] != batch_id:
                    return False
                if operation == "write":
                    if latest["status"] != kind or detail is None:
                        return False
                    opposite = "blocked" if kind == "done" else "done"
                    self._remove_marker(name, opposite)
                    self._write_marker(name, kind, detail)
                    return True
                if (
                    operation == "remove"
                    and latest["status"] in ("active", "queued")
                ):
                    self._remove_marker(name, kind)
                    return True
                return False
        except (OSError, sqlite3.Error, state.StateError) as error:
            self.log_line(
                f"批次 {name} marker 所有权校验失败，保留现状: {error}"
            )
            return False

    def _write_marker(self, name: str, kind: str, detail: str) -> None:
        """P7: 批次进入终态 (done/blocked) 写 marker 文件, 供一行查看 (sched markers).

        文件: {STATE}/<hostname>/markers/{name}.{kind} (决策 5B 按节点隔离; 按名覆盖幂等).
        """
        p = os.path.join(self._marker_dir(), f"{name}.{kind}")
        try:
            with state.open_private_text(p, "w") as f:
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
        try:
            with open("/proc/uptime") as f:
                uptime = float(f.read().split()[0])
            boot_ts = time.time() - uptime
            if boot_ts > hb_ts:
                self.log_line("D4: 检测到节点重启, legacy running 任务标 interrupted (不计 retries)")
                cleanup_jobs: list[dict] = []
                with state.connect() as conn:
                    # A cancel command takes the same SQLite writer claim before
                    # classifying jobs.  Whichever writer wins determines a
                    # complete state transition; no stale running snapshot may
                    # be republished as pending below.
                    if not conn.in_transaction:
                        conn.execute("BEGIN IMMEDIATE")
                    rows = conn.execute(
                        "SELECT * FROM jobs WHERE status='running'"
                    ).fetchall()
                    for j in rows:
                        if self._has_native_session(conn, j["id"]):
                            self.log_line(
                                f"D4: native session job {j['id']} 无原始 owner/wait 证明; "
                                "保留 running 与取消意图"
                            )
                            continue
                        state.update_job(
                            conn, j["id"], status="interrupted",
                        )
                        self._requeue_for_retry(conn, j)
                        cleanup_jobs.append(dict(j))
                for job in cleanup_jobs:
                    self._drop_launch_marker(job)
        except (OSError, ValueError):
            pass

    @staticmethod
    def _has_native_session(conn, job_id: str) -> bool:
        """Legacy Popen recovery cannot settle a durable native session."""
        return conn.execute(
            "SELECT 1 FROM native_sessions WHERE job_id=?",
            (job_id,),
        ).fetchone() is not None

    def _has_native_v2_contract(self, conn, job) -> bool:
        """A V2 candidate must never be handled through a legacy marker."""
        try:
            return NATIVE_EXEC_V2_CONTRACT_FIELD in self._load_task_spec(conn, job)
        except ValueError:
            return False

    def _job_rc_prefix(self, job) -> str:
        return hashlib.sha256(str(job["id"]).encode("utf-8")).hexdigest()[:24]

    def _job_rc_path(self, job, pgid: int | None = None) -> str | None:
        pgid = job["pgid"] if pgid is None else pgid
        if not pgid:
            return None
        return os.path.join(
            self.host_dir, "rc", f"{self._job_rc_prefix(job)}-{pgid}.rc"
        )
    def _launch_marker_path(self, job) -> str:
        return os.path.join(
            self.host_dir, "launch", f"{self._job_rc_prefix(job)}.launch"
        )

    def _launch_marker_unresolved(self, job) -> bool:
        """Fail closed while any marker object still exists for a job."""
        try:
            os.lstat(self._launch_marker_path(job))
            return True
        except FileNotFoundError:
            return False
        except OSError:
            return True

    def _batch_has_unresolved_launch_marker(self, conn, batch_id: str) -> bool:
        jobs = conn.execute(
            "SELECT id FROM jobs WHERE batch_id=?",
            (batch_id,),
        ).fetchall()
        return any(self._launch_marker_unresolved(job) for job in jobs)

    def _task_has_unresolved_launch_marker(
        self,
        conn,
        batch_id: str,
        task_id: str,
        *,
        exclude_job_id: str | None = None,
    ) -> bool:
        jobs = conn.execute(
            "SELECT id FROM jobs WHERE batch_id=? AND task_id=?",
            (batch_id, task_id),
        ).fetchall()
        return any(
            job["id"] != exclude_job_id
            and self._launch_marker_unresolved(job)
            for job in jobs
        )

    def _drop_launch_marker(self, job) -> None:
        try:
            os.unlink(self._launch_marker_path(job))
        except OSError:
            pass

    def _proc_start_time(self, pgid: int) -> str | None:
        return process_start_token(pgid)

    def _read_launch_marker_identity(self, job) -> tuple[int, str] | None:
        """Read one owned, bounded marker containing a strong identity."""
        marker_path = self._launch_marker_path(job)
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        flags |= getattr(os, "O_NONBLOCK", 0)
        try:
            fd = os.open(marker_path, flags)
        except OSError:
            return None
        try:
            marker_stat = os.fstat(fd)
            if (
                not stat.S_ISREG(marker_stat.st_mode)
                or marker_stat.st_uid != os.getuid()
                or marker_stat.st_nlink != 1
                or marker_stat.st_size > 4096
            ):
                return None
            raw = os.read(fd, 4097)
            if len(raw) > 4096:
                return None
            fields = raw.decode("utf-8").split()
        except (OSError, UnicodeDecodeError):
            return None
        finally:
            os.close(fd)
        if len(fields) != 2:
            return None
        try:
            pgid = int(fields[0])
        except (TypeError, ValueError):
            return None
        marker_start = fields[1]
        if (
            pgid <= 0
            or pgid > 2**31 - 1
            or not _is_strong_start_token(marker_start)
        ):
            return None
        return pgid, marker_start

    def _read_launch_identity(self, job) -> tuple[int, str] | None:
        identity = self._read_launch_marker_identity(job)
        if identity is None:
            return None
        try:
            row_pgid = job["pgid"]
        except (KeyError, TypeError):
            return None
        return identity if identity[0] == row_pgid else None

    def _launch_identity_state(self, identity: tuple[int, str]) -> str:
        pgid, expected_start = identity
        actual_start = self._proc_start_time(pgid)
        if actual_start is None:
            try:
                local_supervisor_completed = (
                    self.executor.local_supervisor_completed(pgid)
                )
            except (AttributeError, OSError, OverflowError, TypeError):
                local_supervisor_completed = None
            try:
                group_alive = self.executor.alive(pgid)
            except (OSError, OverflowError, TypeError):
                return "unknown"
            if local_supervisor_completed is True and group_alive:
                # The persistent local supervisor exited only after its original
                # group emptied; a live group with the same number is a reuse.
                return "mismatch"
            return "group_alive" if group_alive else "dead"
        if actual_start != expected_start:
            return "mismatch"
        try:
            return "alive" if self.executor.alive(pgid) else "dead"
        except (OSError, OverflowError, TypeError):
            return "unknown"

    def _job_process_state(self, job) -> str:
        """Return identity-aware process-group state without trusting a bare PID."""
        identity = self._read_launch_identity(job)
        if identity is not None:
            return self._launch_identity_state(identity)
        try:
            pgid = job["pgid"]
            return "unknown" if self.executor.alive(pgid) else "dead"
        except (KeyError, OSError, OverflowError, TypeError):
            return "unknown"

    def _signal_job_result(
        self,
        job,
        sig: int = signal.SIGTERM,
    ) -> str:
        """Signal only a twice-attested exact leader and classify the result."""
        for attempt in range(2):
            process_state = self._job_process_state(job)
            if process_state in {"dead", "mismatch"}:
                return _SIGNAL_DEAD
            if process_state != "alive":
                phase = "re-attest" if attempt else "attest"
                self.log_line(
                    f"job {job['id']} signal 拒绝 ({phase}): "
                    f"process identity={process_state}"
                )
                return _SIGNAL_UNKNOWN
        try:
            if self.executor.kill_pgid(job["pgid"], sig):
                return _SIGNAL_SENT
        except Exception as error:
            try:
                self.log_line(
                    f"job {job['id']} signal 传递失败 sig={sig}: {error}"
                )
            except Exception:
                pass
            return _SIGNAL_UNKNOWN
        final_state = self._job_process_state(job)
        if final_state in {"dead", "mismatch"}:
            return _SIGNAL_DEAD
        return _SIGNAL_UNKNOWN

    def _signal_job(
        self,
        job,
        sig: int = signal.SIGTERM,
    ) -> bool:
        return self._signal_job_result(job, sig) == _SIGNAL_SENT

    def _signal_launch_identity(
        self,
        job_id: str,
        identity: tuple[int, str],
        sig: int,
    ) -> bool:
        for attempt in range(2):
            process_state = self._launch_identity_state(identity)
            if process_state != "alive":
                phase = "re-attest" if attempt else "attest"
                self.log_line(
                    f"job {job_id} marker signal 拒绝 ({phase}): "
                    f"process identity={process_state}"
                )
                return False
        return bool(self.executor.kill_pgid(identity[0], sig))

    def _prepare_launch_marker(self, job) -> bool:
        """Return True while a prior attempt's marker must block relaunch."""
        marker_path = self._launch_marker_path(job)
        try:
            marker_stat = os.lstat(marker_path)
        except FileNotFoundError:
            return False
        except OSError as error:
            self.log_line(
                f"job {job['id']} launch marker 无法检查; 阻止派发: {error}"
            )
            return True
        if not stat.S_ISREG(marker_stat.st_mode):
            self.log_line(
                f"job {job['id']} launch marker 非普通文件; 阻止派发"
            )
            return True
        intent_claim = _claim_abandoned_launch_intent(marker_path)
        if intent_claim is True:
            self.log_line(
                f"job {job['id']} 清理未进入 Popen 的 abandoned launch intent"
            )
            return False
        if intent_claim is False:
            self.log_line(
                f"job {job['id']} launch intent 仍被 launcher/wrapper 持有; "
                "保留 running/资源并阻止派发"
            )
            return True
        identity = self._read_launch_marker_identity(job)
        if identity is None:
            self.log_line(
                f"job {job['id']} launch marker identity=unknown; 阻止派发"
            )
            return True
        process_state = self._launch_identity_state(identity)
        if process_state in {"dead", "mismatch"}:
            self._drop_rc_path(self._job_rc_path(job, identity[0]))
            self._drop_rc_path(marker_path)
            self.log_line(
                f"job {job['id']} 清理旧 launch marker "
                f"(process identity={process_state})"
            )
            return False
        if process_state == "alive":
            sent = self._signal_launch_identity(
                str(job["id"]),
                identity,
                signal.SIGKILL,
            )
            self.log_line(
                f"job {job['id']} crash-window orphan "
                + ("已发送 SIGKILL; 保留 marker 等待死亡"
                   if sent else "SIGKILL 未发送; 保留 marker 阻止派发")
            )
        else:
            self.log_line(
                f"job {job['id']} launch identity={process_state}; "
                "保留 marker 阻止派发"
            )
        return True

    def _launch_marker_alive(self, job) -> bool:
        return self._job_process_state(job) in {"alive", "group_alive"}



    def _recover_launch_markers(self) -> bool:
        recovered_abandoned_intent = False
        marker_dir = os.path.join(self.host_dir, "launch")
        try:
            names = sorted(
                name for name in os.listdir(marker_dir)
                if name.endswith(".launch")
            )
        except FileNotFoundError:
            return False
        except OSError:
            return False
        with state.connect() as conn:
            rows = conn.execute("SELECT * FROM jobs").fetchall()
            by_prefix = {self._job_rc_prefix(row): row for row in rows}
            for name in names:
                path = os.path.join(marker_dir, name)
                row = by_prefix.get(name[:-len(".launch")])
                if row is None:
                    self.log_line(
                        f"launch marker 无对应 job, 拒绝信号并保留: {path}"
                    )
                    continue
                # A pending V2 job may be claimed concurrently after the job
                # snapshot.  Its persisted spec already identifies it as V2;
                # no legacy marker may be cleaned or signalled for that job.
                if (
                    self._has_native_session(conn, row["id"])
                    or self._has_native_v2_contract(conn, row)
                ):
                    self.log_line(
                        f"job {row['id']} native V2/session 已入库; "
                        "忽略 legacy launch marker 恢复"
                    )
                    continue
                if row["status"] != "running":
                    self._prepare_launch_marker(row)
                    continue
                intent_claim = _claim_abandoned_launch_intent(path)
                if intent_claim is True:
                    recovered_abandoned_intent = True
                    self.log_line(
                        f"job {row['id']} 周期恢复已清理 unlocked launch intent; "
                        "立即重新执行 adoption 收敛"
                    )
                    continue
                if intent_claim is False:
                    self.log_line(
                        f"job {row['id']} launch intent 仍被 launcher/wrapper 持有; "
                        "保留 running/资源"
                    )
                    continue
                identity = self._read_launch_marker_identity(row)
                process_state = (
                    "unknown"
                    if identity is None
                    else self._launch_identity_state(identity)
                )
                if identity is not None and not row["pgid"]:
                    state.update_job(conn, row["id"], pgid=identity[0])
                    self.log_line(
                        f"job {row['id']} 从 launch marker 恢复 pgid={identity[0]}"
                    )
                if process_state == "unknown":
                    self.log_line(
                        f"job {row['id']} launch identity=unknown; "
                        "保留 running/资源与 marker"
                    )
        return recovered_abandoned_intent


    def _unresolved_launch_markers(self) -> bool:
        marker_dir = os.path.join(self.host_dir, "launch")
        try:
            return any(
                name.endswith(".launch") for name in os.listdir(marker_dir)
            )
        except FileNotFoundError:
            return False
        except OSError:
            return True

    def _read_job_rc(self, job) -> int | None:
        path = self._job_rc_path(job)
        if path is None:
            return None
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        flags |= getattr(os, "O_NONBLOCK", 0)
        try:
            fd = os.open(path, flags)
        except OSError:
            return None
        try:
            rc_stat = os.fstat(fd)
            if (
                not stat.S_ISREG(rc_stat.st_mode)
                or rc_stat.st_uid != os.getuid()
                or rc_stat.st_nlink != 1
                or rc_stat.st_size > RC_MAX_BYTES
            ):
                return None
            raw = os.read(fd, RC_MAX_BYTES + 1)
            if len(raw) > RC_MAX_BYTES:
                return None
            text = raw.decode("utf-8").strip()
            if not text or not text.lstrip("-").isdigit():
                return None
            return int(text)
        except (OSError, UnicodeDecodeError, ValueError):
            return None
        finally:
            os.close(fd)

    def _job_uses_native_exec(self, conn, job) -> bool:
        """Conservatively identify strict/native jobs from durable state."""
        batch = state.get_batch(conn, job["batch_id"])
        if batch is not None and batch["mode"] == "strict":
            return True
        try:
            spec = self._load_task_spec(conn, job)
        except (TypeError, ValueError):
            return False
        return any(key in spec for key in _NATIVE_EXEC_METADATA_KEYS)

    def _drop_rc_path(self, path: str | None) -> None:
        if path is None:
            return
        try:
            os.unlink(path)
        except OSError:
            pass

    def _drop_job_rc(self, job) -> None:
        self._drop_rc_path(self._job_rc_path(job))

    def _adopt_running(self, *, unidentified_only: bool = False) -> None:
        drop_paths: list[str] = []
        cleanup_jobs: list[tuple[str, dict]] = []
        with state.connect() as conn:
            # P1: SQL 层过滤 running
            query = "SELECT * FROM jobs WHERE status='running'"
            if unidentified_only:
                query += " AND pgid IS NULL"
            rows = conn.execute(query).fetchall()
            for j in rows:
                if self._has_native_session(conn, j["id"]):
                    self.log_line(
                        f"A3: native session job {j['id']} 无原始 owner/wait 证明; "
                        "保留 running、资源与取消意图"
                    )
                    continue
                native_exec = self._job_uses_native_exec(conn, j)
                if not j["pgid"] and self._prepare_launch_marker(j):
                    self.log_line(
                        f"A3: job {j['id']} pgid 未回写且 launch marker 未决; "
                        "保留 running/资源"
                    )
                    continue
                if j["pgid"]:
                    process_state = self._job_process_state(j)
                    if process_state in {"alive", "group_alive"}:
                        self.log_line(
                            f"A3: 接管 running job {j['id']} (pgid={j['pgid']})"
                        )
                        continue
                    if process_state == "unknown":
                        self.log_line(
                            f"A3: job {j['id']} process identity=unknown; "
                            "保留 running/资源且拒绝接管与信号"
                        )
                        continue
                    if process_state == "mismatch":
                        self.log_line(
                            f"A3: job {j['id']} 原 process identity 已退出; "
                            "检测到 PGID 复用，绝不信号新进程"
                        )
                # Serialize terminal/adoption publication with cmd_cancel.  If
                # cancel committed first, consume its durable intent; if this
                # writer wins, a later cancel observes the published terminal.
                if not conn.in_transaction:
                    conn.execute("BEGIN IMMEDIATE")
                rc = None if native_exec else self._read_job_rc(j)
                if rc is not None:
                    # Preserve the wrapper's durable exit evidence even when a
                    # concurrent cancel intent wins the terminal status.
                    state.update_job(conn, j["id"], rc=rc)
                if self._consume_pending_cancel_before_requeue(
                    conn,
                    j,
                    context="daemon adoption settlement",
                    rc_if_missing=137,
                ):
                    cleanup_jobs.extend(
                        (
                            ("launch", dict(j)),
                            ("profile", dict(j)),
                        )
                    )
                    rc_path = self._job_rc_path(j)
                    if rc_path is not None:
                        drop_paths.append(rc_path)
                    continue
                if native_exec:
                    rc_path = self._job_rc_path(j)
                    if j["kill_reason"] in ("cancelled", "timed_out"):
                        state.update_job(conn, j["id"], rc=137)
                        cleanup_jobs.extend(
                            self._handle_job_done(conn, j, 137)
                        )
                    else:
                        # A restarted daemon no longer owns the exact Popen and
                        # a same-uid child can forge the legacy Bash RC sidecar.
                        # Preserve the one-shot evidence and fail closed.
                        state.update_job(
                            conn,
                            j["id"],
                            status="failed",
                            rc=137,
                            failure="native_rc_authority_lost",
                            finished_at=state.now(),
                        )
                        self._release_gpu_for_job(conn, j)
                        self._maybe_retry(conn, j)
                        cleanup_jobs.extend(
                            (
                                ("launch", dict(j)),
                                ("profile", dict(j)),
                            )
                        )
                        self.log_line(
                            f"A3: native job {j['id']} 已失去 Popen "
                            "退出码权威 -> blocked"
                        )
                    if rc_path is not None:
                        drop_paths.append(rc_path)
                    continue
                if rc is not None:
                    rc_path = self._job_rc_path(j)
                    self.log_line(
                        f"A3: job {j['id']} pgid 已死, 读取持久退出码 rc={rc}"
                    )
                    cleanup_jobs.extend(self._handle_job_done(conn, j, rc))
                    if rc_path is not None:
                        drop_paths.append(rc_path)
                    continue
                if j["kill_reason"] in ("cancelled", "timed_out"):
                    rc_path = self._job_rc_path(j)
                    state.update_job(conn, j["id"], rc=137)
                    cleanup_jobs.extend(self._handle_job_done(conn, j, 137))
                    if rc_path is not None:
                        drop_paths.append(rc_path)
                    continue
                # M6: 成功任务恰在 reap 前 daemon 重启 -> pgid 已死但产物
                # 齐全; 先查产物/指纹, 有效判 done, 避免白跑一遍
                try:
                    spec = self._load_task_spec(conn, j)
                except ValueError as exc:
                    self.log_line(f"A3: job {j['id']} 存量 spec 非法: {exc}")
                    state.update_job(
                        conn, j["id"], status="failed", failure="invalid_spec",
                        finished_at=state.now(),
                    )
                    spec = None
                if spec is not None and self._should_skip(conn, spec, j):
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
                cleanup_jobs.extend(
                    (
                        ("launch", dict(j)),
                        ("profile", dict(j)),
                    )
                )
                rc_path = self._job_rc_path(j)
                if rc_path is not None:
                    drop_paths.append(rc_path)
        for kind, job in cleanup_jobs:
            if kind == "launch":
                self._drop_launch_marker(job)
            else:
                self._drop_profile(job)
        for path in drop_paths:
            self._drop_rc_path(path)

    # ---------- B12-a: 配置热更新 (现行配置见 docs/reference.md) ----------

    def _scan_progress(self) -> None:
        """B13-§5: 对声明 progress_regex 的运行任务, 从日志尾部提取最新进度."""
        now_t = time.time()
        if now_t - self._last_progress_scan < 10:
            return
        self._last_progress_scan = now_t

        with state.connect() as conn:
            rows = conn.execute(
                "SELECT j.id, j.batch_id, j.task_id, j.version, j.progress, t.spec"
                " FROM jobs j JOIN tasks t"
                "   ON t.batch_id=j.batch_id AND t.id=j.task_id AND t.version=j.version"
                " WHERE j.status='running'"
            ).fetchall()
            for r in rows:
                try:
                    spec = json.loads(r["spec"] or "{}")
                except (json.JSONDecodeError, TypeError):
                    continue
                if not isinstance(spec, dict):
                    self.log_line(f"job {r['id']} 存量 spec 顶层非对象, 跳过进度规则")
                    continue
                rx = spec.get("progress_regex")
                if not isinstance(rx, str) or not rx:
                    continue
                log_path = os.path.join(
                    self.host_dir, "logs", r["batch_id"],
                    f"{r['task_id']}-v{r['version']}.log",
                )
                try:
                    text = read_tail(log_path, 4096)
                except OSError:
                    continue
                val = bounded_regex_last_match(
                    str(rx),
                    text,
                    timeout=0.25,
                    max_match_chars=120,
                )
                if val is None:
                    continue
                if r["progress"] != val:
                    state.update_job(conn, r["id"], progress=val)

    def _maybe_reload_config(self) -> None:
        """每 tick stat 一次 config.json, mtime 变化则重载.

        失败(半写/非法 JSON/冷键变更)一律保留旧配置并告警; 基线 mtime 无论
        成败都更新 —— 防止坏文件触发每 tick 重试风暴 (用户修复保存后新 mtime
        自然再次触发).
        """
        try:
            mtime = os.stat(self._config_path).st_mtime
        except OSError:
            return  # 文件暂时不可见 (NFS 抖动): 下轮再看
        if mtime == self._cfg_mtime:
            return
        self._cfg_mtime = mtime
        ok = self._reload_config_now()
        self.log_line("✅ 配置已热更新" if ok else "⚠️ 配置热更新未生效 (保留旧配置)")

    def _reload_config_now(self) -> bool:
        """加载并校验新配置, 通过冷键检查后原子换引用 + 刷新缓存字段. 幂等."""
        try:
            new_cfg = load_config(
                self._config_path, apply_runtime_state=False
            )
            old_gpu_list, old_mem, _ = parse_gpus(self.cfg)
            new_gpu_list, new_mem, new_mj = parse_gpus(new_cfg)
            old_native_roots = getattr(
                self, "_native_exec_project_roots", None
            )
            if old_native_roots is None:
                old_native_roots = native_exec_project_roots(self.cfg)
            new_native_roots = native_exec_project_roots(new_cfg)
            new_native_identities = _native_root_identities(new_native_roots)
        except Exception as e:  # ConfigError/json/OSError — 半写或非法
            self.log_line(f"⚠️ 配置热更新失败 (保留旧配置): {e}")
            return False
        cold_diff = [k for k in CONFIG_COLD_KEYS
                     if self.cfg.get(k) != new_cfg.get(k)]
        if (old_gpu_list, old_mem) != (new_gpu_list, new_mem):
            cold_diff.append("gpus(卡集或容量覆盖)")  # max_jobs 是热键, 不参与冷键比对
        if old_native_roots != new_native_roots:
            cold_diff.append("native_exec_project_roots")
        else:
            frozen_identities = getattr(
                self,
                "_native_exec_project_root_identities",
                {},
            )
            if (
                frozen_identities
                and frozen_identities != new_native_identities
            ):
                cold_diff.append("native_exec_project_root_identity")
        if cold_diff:
            self.log_line(
                f"⚠️ 配置含冷键变更 {cold_diff} —— 拒绝热更新, 请重启 daemon 生效"
            )
            return False
        self.cfg = new_cfg
        # 缓存字段刷新清单 (其余键均实时读 self.cfg, 换引用即生效):
        self._co_locate = bool(new_cfg.get("co_locate", False))
        self._projects = new_cfg.get("projects", {})   # B12-b: 项目配置(含 colocate/max_jobs)
        self._gpu_max_jobs = new_mj                    # B12-c: 每卡打包上限 (热键)
        self.venv_paths = new_cfg.get("venvs", {})
        self.idle_timeout_min = int(
            new_cfg.get("idle_timeout_min", DEFAULT_IDLE_TIMEOUT_MIN))
        return True

    def _drain_submit_inbox(self) -> None:
        """C2: 将网关投递的 payload 文件收编为本地 pending 请求行.

        网关与 daemon 共享 NFS 时禁止网关写 state.db; payload 文件是跨主机
        唯一投递通道。daemon 是 control_requests 的唯一写者, 因此每轮先以
        job_id=payload path 全状态去重, 再本地插入 pending 行交给统一消费者。
        """
        state_dir = default_state_dir()
        node = str(self.cfg.get("node") or state.hostname())
        inbox_dir = os.path.join(state_dir, node, "submit_inbox")
        if not os.path.isdir(inbox_dir):
            return
        try:
            names = os.listdir(inbox_dir)
        except OSError as e:
            self.log_line(f"submit_inbox 扫描失败 (保留下轮重试): {e}")
            return
        payloads = []
        for name in names:
            if not name.startswith("submit-") or not name.endswith(".json"):
                continue
            path = os.path.join(inbox_dir, name)
            try:
                if not stat.S_ISREG(os.lstat(path).st_mode):
                    self.log_line(f"submit_inbox 拒绝非普通文件: {path}")
                    os.unlink(path)
                    continue
            except FileNotFoundError:
                continue
            except OSError as error:
                self.log_line(f"submit_inbox 文件检查失败 (保留下轮重试): {error}")
                continue
            payloads.append(path)
        payloads.sort()
        if not payloads:
            return
        with state.connect() as conn:
            known = {
                row["job_id"]: row["status"]
                for row in conn.execute(
                    "SELECT job_id, status FROM control_requests WHERE op='batch_submit'"
                ).fetchall()
            }
            for payload_path in payloads:
                status = known.get(payload_path)
                if status:
                    if status == "done":
                        try:
                            os.unlink(payload_path)
                        except OSError:
                            pass
                    continue
                conn.execute(
                    "INSERT INTO control_requests (job_id, op, status, created_at)"
                    " VALUES (?, 'batch_submit', 'pending', ?)",
                    (payload_path, state.now()),
                )
                known[payload_path] = "pending"
                self.log_line(f"submit_inbox 收编: {payload_path}")

    def _prune_control_requests(self) -> None:
        """L11: 删除过期/超上限的 done 控制请求, 保留 pending 请求."""
        cutoff = datetime.fromtimestamp(
            datetime.now().timestamp() - 30 * 86400
        ).strftime("%Y-%m-%d %H:%M:%S")
        with state.connect() as conn:
            conn.execute(
                "DELETE FROM control_requests"
                " WHERE status='done' AND processed_at IS NOT NULL AND processed_at < ?",
                (cutoff,),
            )
            keep_min = conn.execute(
                "SELECT MIN(id) FROM ("
                " SELECT id FROM control_requests WHERE status='done'"
                " ORDER BY id DESC LIMIT 1000"
                ")"
            ).fetchone()[0]
            if keep_min is not None:
                conn.execute(
                    "DELETE FROM control_requests WHERE status='done' AND id < ?",
                    (keep_min,),
                )

    def _prune_notify_threads(self) -> None:
        """L11: 通知线程只保留仍在执行的引用."""
        self._notify_threads[:] = [
            thread for thread in self._notify_threads if thread.is_alive()
        ]

    def _process_control_requests(self) -> None:
        """事故记录 4 (2026-08-17): 处理 cancel 转发请求 — 在**计算节点本地**执行 kill.
        CLI (登录节点) 看不到计算节点进程组 (PID namespace 跨节点, 定案 44 同类),
        本地 killpg 恒失败曾致孤儿占卡 13 分钟。现在 CLI 只写 control_requests 队列,
        本方法每轮 tick 拉取并在本地完成:
          1. alive 预检 (O5): 即使进程刚自然退出也先持久化 cancel intent，
             防止随后 reap/adopt 将任务成功结算或自动重试
          2. 写 kill_reason=cancelled (N2: 先写 reason 再 killpg)
          3. killpg SIGTERM; 下一轮仍存活 -> SIGKILL 升级 (绝不静默, 修复建议 2)
        GPU 释放交给 reap (_handle_job_done cancelled 分支), 与正常路径一致.
        """
        cleanup_payloads: list[str] = []
        signal_intents: list[tuple[dict, int, int]] = []
        with state.connect() as conn:
            reqs = state.pending_control_requests(conn)
            if not reqs:
                return
            seen_cancel_jobs: set[str] = set()
            for r in reqs:
                if r["op"] == "cancel":
                    # Claim the writer before observing job/session state.
                    # An owner settlement must not race between that read and
                    # publication of a native cancellation intent.
                    if not conn.in_transaction:
                        conn.execute("BEGIN IMMEDIATE")
                    else:
                        # in_transaction may mean DEFERRED read-only so far.
                        conn.execute("UPDATE jobs SET status=status WHERE 0")
                    current_req = conn.execute(
                        "SELECT status FROM control_requests WHERE id=?",
                        (r["id"],),
                    ).fetchone()
                    if current_req is None or current_req["status"] != "pending":
                        continue
                    if r["job_id"] in seen_cancel_jobs:
                        current = state.get_job(conn, r["job_id"])
                        if (
                            current is not None
                            and current["status"] == "running"
                            and self._has_native_session(conn, current["id"])
                        ):
                            self.log_line(
                                f"cancel req {r['id']}: native session job "
                                f"{r['job_id']} 保留重复请求与取消意图"
                            )
                            continue
                        state.finish_control_request(conn, r["id"], "同轮重复取消, 已跳过")
                        self.log_line(f"cancel req {r['id']}: job {r['job_id']} 同轮重复, 跳过")
                        continue
                    seen_cancel_jobs.add(r["job_id"])
                if r["op"] == "batch_submit":
                    # Release any earlier request's write transaction before
                    # doing file validation, git probes, and fingerprinting.
                    conn.commit()
                    # B27: consume the file-only submit inbox on the daemon.
                    # Validate and fingerprint before taking the global
                    # submission gate so git/NFS work never serializes CLI
                    # mutations.  The final dependency/name checks, inserts,
                    # request completion, and commit use a fresh connection
                    # *inside* that gate; clean/retry/resubmit therefore cannot
                    # reopen an old same-name batch between our check and commit.
                    payload_path = r["job_id"]

                    def safe_log(message: str) -> None:
                        try:
                            self.log_line(message)
                        except Exception:
                            pass

                    def discard_payload() -> None:
                        if payload_path not in cleanup_payloads:
                            cleanup_payloads.append(payload_path)
                    bid = None
                    try:
                        payload_fd = None
                        try:
                            payload_fd = os.open(
                                payload_path,
                                os.O_RDONLY
                                | os.O_NONBLOCK
                                | getattr(os, "O_CLOEXEC", 0)
                                | getattr(os, "O_NOFOLLOW", 0),
                            )
                            envelope = _load_bounded_submit_json(payload_fd)
                        finally:
                            if payload_fd is not None:
                                os.close(payload_fd)
                        if not isinstance(envelope, dict):
                            raise ValueError("payload 顶层必须是对象")
                        spec = envelope.get("spec") or {}
                        if not isinstance(spec, dict) or "name" not in spec:
                            raise ValueError("payload 缺少合法 spec")
                        cfg_now = self.cfg
                        # Admission is checked under the submission gate below,
                        # after duplicate detection and against the latest policy.
                        norm = validate_batch(spec, cfg_now, check_gpu_access=False)
                        if any(
                            NATIVE_EXEC_V2_CONTRACT_FIELD in task
                            for task in norm["tasks"]
                        ):
                            raise NativeExecProfileError(
                                "native exec profile V2 is validation-only until "
                                "the retained/bootstrap launcher is connected"
                            )
                        bid = envelope.get("bid")
                        if not isinstance(bid, str) or not bid:
                            bid = f"{norm['name']}-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}"
                        bid = _validate_inbox_bid(bid, norm["name"])

                        prepared_tasks = []
                        for i2, t in enumerate(norm["tasks"]):
                            cmd_e = expand_cmd(t["cmd"], cfg_now) if t["cmd"] else None
                            stages_e = None
                            if t["stages"]:
                                stage_art: dict[int, dict] = {}
                                stages_e = []
                                for stage_idx, stage in enumerate(t["stages"]):
                                    stage_art[stage_idx] = stage["artifacts"]
                                    stages_e.append(
                                        {
                                            "cmd": expand_cmd(
                                                stage["cmd"], cfg_now, stage_art, t["cwd_abs"]
                                            ),
                                            "artifacts": stage["artifacts"],
                                            "paths_escape": stage.get("paths_escape", False),
                                        }
                                    )
                            native_metadata = _native_exec_metadata(t)
                            if native_metadata is not None:
                                if stages_e is not None:
                                    raise NativeExecProfileError(
                                        "native execution profile forbids stages"
                                    )
                                if (
                                    native_metadata[
                                        "_native_exec_submitted_argv"
                                    ]
                                    != cmd_e
                                ):
                                    raise NativeExecProfileError(
                                        "native submitted argv differs from expanded command"
                                    )
                            fp, stage_fps, _rev = compute_fingerprint(
                                cmd_e,
                                stages_e,
                                t["cwd_abs"],
                                t["git"],
                                cfg_now.get("venvs", {}),
                                runtime_prefix=t.get("runtime_prefix"),
                                execution_env=task_environment(cfg_now, norm.get("env"), t.get("env")),
                                artifacts=t.get("artifacts"),
                                **_native_exec_fingerprint_kwargs(t),
                            )
                            prepared_tasks.append((i2, t, cmd_e, stages_e, fp, stage_fps))

                        duplicate = False
                        with state.submission_connect() as submit_conn:
                            # submission_connect checks the daemon-shutdown
                            # fence and commits before releasing the global
                            # gate.  Do not replace this with the outer
                            # long-lived control-request connection.
                            existing_bid = submit_conn.execute(
                                "SELECT name, mode FROM batches WHERE id=?", (bid,)
                            ).fetchone()
                            if existing_bid:
                                if existing_bid["name"] != norm["name"]:
                                    raise SchemaError(
                                        f"批次 id 已存在且名称不同: '{bid}'"
                                    )
                                if norm["mode"] == "strict" and not _strict_inbox_delivery_matches(
                                    submit_conn, bid, norm, prepared_tasks,
                                ):
                                    raise SchemaError("strict 重复批次 id 的耐久绑定与当前 exact delivery 不一致")
                                state.finish_control_request(
                                    submit_conn,
                                    r["id"],
                                    f"已入队 {bid} (重复投递, 已存在)",
                                )
                                duplicate = True
                            else:
                                validate_project_gpu_access(
                                    self._read_gpu_policy(), norm.get("project"), norm["tasks"],
                                )
                                _validate_inbox_dependencies(submit_conn, norm)
                                existing = submit_conn.execute(
                                    "SELECT status FROM batches WHERE name=?",
                                    (norm["name"],),
                                ).fetchall()
                                if norm["mode"] == "strict" and existing:
                                    raise SchemaError(
                                        f"strict 批次名 '{norm['name']}' 已消费；"
                                        "必须配置新的 native profile/batch name 并重启 daemon"
                                    )
                                if any(
                                    x["status"]
                                    not in ("done", "blocked", "discarded")
                                    for x in existing
                                ):
                                    raise SchemaError(
                                        f"同名批次 '{norm['name']}' 已有未终态批次 "
                                        "(定案 6), 未入队"
                                    )
                                state.insert_batch(
                                    submit_conn,
                                    bid,
                                    norm["name"],
                                    norm["mode"],
                                    norm["depends_on"],
                                    None,
                                    norm["cwd"],
                                    norm["env"],
                                    norm.get("notify"),
                                    norm.get("project"),
                                    norm.get("priority", 0),
                                )
                                for (
                                    i2,
                                    t,
                                    cmd_e,
                                    stages_e,
                                    fp,
                                    stage_fps,
                                ) in prepared_tasks:
                                    spec_json = _inbox_task_spec(
                                        t,
                                        cmd_e,
                                        stages_e,
                                    )
                                    state.insert_task(
                                        submit_conn,
                                        bid,
                                        t["id"],
                                        1,
                                        spec_json,
                                        i2,
                                        norm.get("project"),
                                    )
                                    state.insert_job(
                                        submit_conn,
                                        f"{bid}-{t['id']}-v1",
                                        bid,
                                        t["id"],
                                        1,
                                        fp,
                                        stage_fps,
                                        norm.get("project"),
                                    )
                                state.finish_control_request(
                                    submit_conn, r["id"], f"已入队 {bid}"
                                )
                        discard_payload()
                        if duplicate:
                            safe_log(
                                f"batch_submit req {r['id']}: "
                                f"重复投递 {bid}, 已跳过"
                            )
                        else:
                            safe_log(
                                f"batch_submit req {r['id']}: 已入队 {bid} "
                                f"({len(norm['tasks'])} 任务)"
                            )
                    except (SchemaError, ConfigError, ValueError, TypeError, KeyError, FileNotFoundError, sqlite3.IntegrityError) as error:
                        prefix = f"submit {json.dumps(bid, ensure_ascii=True)} => " if bid else ""
                        state.finish_control_request(conn, r["id"], f"{prefix}失败: {error}")
                        discard_payload()
                        safe_log(f"⚠️ batch_submit req {r['id']} 拒绝: {error}")
                    except Exception as error:
                        safe_log(
                            f"⚠️ batch_submit req {r['id']} 暂未完成，将重试: {error}"
                        )
                    continue
                if r["op"] == "config_reload":
                    # B12-a: CLI `sched config reload` 的强制重载路径
                    # (mtime 未变也执行; CLI 已本地预校验过语法)
                    ok = self._reload_config_now()
                    state.finish_control_request(
                        conn, r["id"], "已生效" if ok else "失败 (见 scheduler.log)")
                    self.log_line(
                        f"config_reload req {r['id']}: "
                        + ("✅ 配置已热更新" if ok else "⚠️ 未生效 (保留旧配置)"))
                    continue
                j = state.get_job(conn, r["job_id"])
                if j is None:
                    state.finish_control_request(conn, r["id"], "job 非 running, 无需 kill")
                    self.log_line(f"cancel req {r['id']}: job {r['job_id']} 非 running, 跳过")
                    continue
                if j["status"] == "running" and self._has_native_session(conn, j["id"]):
                    state.update_job(conn, j["id"], kill_reason="cancelled")
                    self.log_line(
                        f"cancel req {r['id']}: native session job {j['id']} "
                        "保留请求与 cancel intent，等待原始 owner/wait"
                    )
                    continue
                if j["status"] != "running" or not j["pgid"]:
                    if j["status"] in (
                        "pending",
                        "waiting_quota",
                        "waiting_dep",
                        "interrupted",
                        "failed",
                        "blocked",
                    ):
                        self._consume_pending_cancel_before_requeue(
                            conn,
                            j,
                            context="control request observed after state transition",
                        )
                        continue
                    if j["status"] == "running":
                        # A durable launch intent may represent a child between
                        # Popen and pgid writeback.  Record cancellation but keep
                        # the request pending until identity is recoverable.
                        state.update_job(conn, j["id"], kill_reason="cancelled")
                        self.log_line(
                            f"cancel req {r['id']}: job {j['id']} running/pgid 未决; "
                            "保留请求与 cancel intent"
                        )
                        continue
                    # 已 done/skip/cancelled/timed_out 等成功或失败终态，不会重跑。
                    state.finish_control_request(conn, r["id"], "job 非 running, 无需 kill")
                    self.log_line(f"cancel req {r['id']}: job {r['job_id']} 非 running, 跳过")
                    continue
                process_state = self._job_process_state(j)
                if process_state in {"dead", "mismatch"}:
                    # Cancellation still wins when the process exited just
                    # before this scan: reap must not auto-retry it afterwards.
                    state.update_job(conn, j["id"], kill_reason="cancelled")
                    state.finish_control_request(
                        conn,
                        r["id"],
                        f"进程已退出，取消意图已记录 (identity={process_state})",
                    )
                    continue
                if j["kill_reason"] == "cancelled":
                    if process_state == "alive":
                        signal_intents.append(
                            (dict(j), signal.SIGKILL, int(r["id"]))
                        )
                    else:
                        self.log_line(
                            f"cancel req {r['id']}: job {j['id']} identity="
                            f"{process_state}, 保留请求与 cancel intent"
                        )
                    continue
                if process_state != "alive":
                    self.log_line(
                        f"cancel req {r['id']}: job {j['id']} identity="
                        f"{process_state}, 保留请求等待重试"
                    )
                    continue
                state.update_job(conn, j["id"], kill_reason="cancelled")
                signal_intents.append(
                    (dict(j), signal.SIGTERM, int(r["id"]))
                )

        for job, sig, request_id in signal_intents:
            result = self._signal_job_result(job, sig)
            if result == _SIGNAL_UNKNOWN:
                self.log_line(
                    f"cancel req {request_id}: job {job['id']} signal 未确认; "
                    "保留请求与 cancel intent"
                )
                continue
            if sig == signal.SIGTERM and result == _SIGNAL_SENT:
                self.log_line(
                    f"cancel req {request_id}: job {job['id']} "
                    f"killpg SIGTERM (pgid={job['pgid']})"
                )
                continue
            if result == _SIGNAL_SENT:
                detail = "SIGKILL 升级"
            else:
                detail = "进程已退出 (signal re-attest)"
            with state.connect() as conn:
                state.finish_control_request(conn, request_id, detail)

        for payload_path in cleanup_payloads:
            try:
                os.unlink(payload_path)
            except OSError as error:
                self.log_line(f"⚠️ submit payload cleanup failed (保留下轮重试): {error}")
    def _check_timeouts(self) -> None:
        """H6 任务级超时看门狗: running 超 duration_min (schema 已校验) -> timed_out.

        与 cancel 同结构 (自愈升级, 零同 tick 竞态):
          1. kill_reason='timed_out' 的 running job: 仍存活 -> SIGKILL 升级
             (这些 job 是**上一轮** tick SIGTERM 的, 有 10s 优雅退出窗口)
          2. 其余 running job: 超 duration_min -> 先写 kill_reason 再 SIGTERM,
             reap 按 reason 收尾 (_handle_job_done timed_out 分支, 不 retry ——
             超时任务重跑大概率再超时)
        """
        signal_intents: list[tuple[dict, int, object]] = []
        with state.connect() as conn:
            escal = conn.execute(
                "SELECT * FROM jobs WHERE status='running'"
                " AND kill_reason='timed_out' AND pgid IS NOT NULL"
            ).fetchall()
            for j in escal:
                if self._has_native_session(conn, j["id"]):
                    continue
                if self._job_process_state(j) == "alive":
                    signal_intents.append(
                        (dict(j), signal.SIGKILL, None)
                    )
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status='running'"
                " AND kill_reason IS NULL AND started_at IS NOT NULL"
                " AND pgid IS NOT NULL"
            ).fetchall()
            for j in rows:
                if self._has_native_session(conn, j["id"]):
                    continue
                try:
                    spec = self._load_task_spec(conn, j)
                    dur = spec.get("duration_min")
                    if dur is None:
                        continue
                    duration = float(dur)
                    if not math.isfinite(duration) or duration <= 0:
                        raise ValueError("duration_min 必须为有限正数")
                except (TypeError, ValueError) as exc:
                    self.log_line(f"job {j['id']} 存量 duration 规则非法, 已忽略: {exc}")
                    continue
                try:
                    started = time.mktime(
                        time.strptime(j["started_at"], "%Y-%m-%d %H:%M:%S")
                    )
                except (TypeError, ValueError):
                    continue
                if time.time() - started <= duration * 60:
                    continue
                state.update_job(conn, j["id"], kill_reason="timed_out")
                signal_intents.append(
                    (dict(j), signal.SIGTERM, dur)
                )

        for job, sig, duration_min in signal_intents:
            if sig == signal.SIGKILL:
                self.log_line(
                    f"⚠️ job {job['id']} 超时 SIGTERM 未生效 -> "
                    f"SIGKILL (pgid={job['pgid']})"
                )
            result = self._signal_job_result(job, sig)
            if sig == signal.SIGTERM and result == _SIGNAL_SENT:
                self.log_line(
                    f"job {job['id']} 超时 (duration_min={duration_min}) "
                    f"-> killpg SIGTERM (pgid={job['pgid']})"
                )
            elif result == _SIGNAL_UNKNOWN:
                self.log_line(
                    f"job {job['id']} 超时 signal 未确认; "
                    "保留 timed_out intent 等待重试"
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
        signal_intents: list[tuple[dict, int, str | None]] = []
        skipped_ranges: list[tuple[str, int]] = []
        offset_updates: dict[str, int] = {}
        with state.connect() as conn:
            # Capture only pre-existing durable probe intents for escalation.
            # Intents discovered below receive TERM after this commit and are
            # not eligible for KILL until the next tick.
            escal = conn.execute(
                "SELECT * FROM jobs WHERE status='running'"
                " AND kill_reason IN"
                " ('probe_invalid','probe_failed','probe_ready')"
                " AND pgid IS NOT NULL"
            ).fetchall()
            escal_ids = {j["id"] for j in escal}
            for j in escal:
                if self._has_native_session(conn, j["id"]):
                    continue
                if self._job_process_state(j) == "alive":
                    signal_intents.append((dict(j), signal.SIGKILL, None))

            # P1: SQL 层过滤, 不再每 tick 全表扫描历史 job
            running = conn.execute(
                "SELECT * FROM jobs WHERE status='running' AND pgid IS NOT NULL"
            ).fetchall()
            for j in running:
                if self._has_native_session(conn, j["id"]):
                    continue
                if j["id"] in escal_ids:
                    continue
                try:
                    spec = self._load_task_spec(conn, j)
                    probes = spec.get("probes") or {}
                    if not isinstance(probes, dict):
                        raise ValueError("probes 必须是对象")
                    fail_pat = probes.get("fail_on_log")
                    ready_pat = probes.get("ready_on_log")
                    if any(
                        pattern is not None and not isinstance(pattern, str)
                        for pattern in (fail_pat, ready_pat)
                    ):
                        raise ValueError("probe 模式必须是字符串")
                except (TypeError, ValueError) as exc:
                    self.log_line(
                        f"job {j['id']} 存量 probe 规则非法, 终止后隔离: {exc}"
                    )
                    state.update_job(
                        conn,
                        j["id"],
                        failure="invalid_spec",
                        kill_reason="probe_invalid",
                    )
                    signal_intents.append(
                        (dict(j), signal.SIGTERM, "probe_invalid")
                    )
                    continue
                if not fail_pat and not ready_pat:
                    continue
                log_path = self._job_log_path(j)
                try:
                    # Read a bounded tail of the newly visible range. Preserve
                    # enough overlap for a pattern split at the prior offset;
                    # when an append exceeds the cap, prefer the current tail.
                    off = self._probe_offsets.get(j["id"], 0)
                    size = os.path.getsize(log_path)
                    if size < off:
                        off = 0
                    longest = max(
                        len((fail_pat or "").encode("utf-8")),
                        len((ready_pat or "").encode("utf-8")),
                    )
                    overlap_start = max(0, off - longest)
                    start = max(
                        overlap_start,
                        size - PROBE_READ_MAX_BYTES,
                    )
                    if start > overlap_start:
                        skipped_ranges.append(
                            (str(j["id"]), start - overlap_start)
                        )
                    with open(log_path, "rb") as f:
                        f.seek(start)
                        raw = f.read(PROBE_READ_MAX_BYTES)
                    text = raw.decode("utf-8", "replace")
                    text = text.replace("\r\n", "\n").replace("\r", "\n")
                    offset_updates[str(j["id"])] = size
                except OSError:
                    continue  # 日志未就绪, 下轮再查
                if fail_pat and fail_pat in text:
                    self.log_line(
                        f"probe fail_on_log 命中: job {j['id']} "
                        f"({fail_pat!r}) -> 终止后 blocked"
                    )
                    state.update_job(
                        conn,
                        j["id"],
                        failure="probe",
                        kill_reason="probe_failed",
                    )
                    signal_intents.append(
                        (dict(j), signal.SIGTERM, "probe_failed")
                    )
                    continue
                if ready_pat and ready_pat in text:
                    self.log_line(
                        f"probe ready_on_log 命中: job {j['id']} "
                        f"({ready_pat!r}) -> 终止并 reap 后结算"
                    )
                    state.update_job(
                        conn,
                        j["id"],
                        kill_reason="probe_ready",
                    )
                    signal_intents.append(
                        (dict(j), signal.SIGTERM, "probe_ready")
                    )

            live = {str(j["id"]) for j in running}

        self._probe_offsets.update(offset_updates)
        for job_id in [key for key in self._probe_offsets if key not in live]:
            del self._probe_offsets[job_id]

        for job_id, skipped in skipped_ranges:
            self.log_line(
                f"probe 增量过大: job {job_id} 跳过中间 {skipped} bytes, "
                f"只扫描末尾 {PROBE_READ_MAX_BYTES} bytes"
            )
        for job, sig, _expected_reason in signal_intents:
            if sig == signal.SIGKILL:
                self.log_line(
                    f"probe kill 升级: job {job['id']} "
                    "SIGTERM 未生效 -> SIGKILL"
                )
            result = self._signal_job_result(job, sig)
            if result == _SIGNAL_UNKNOWN:
                self.log_line(
                    f"probe signal 未确认: job {job['id']}; "
                    "保留 durable intent 等待重试"
                )

    def _reap_finished_jobs(self) -> None:
        drop_paths: list[str] = []
        cleanup_jobs: list[tuple[str, dict]] = []
        with state.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status='running' AND pgid IS NOT NULL"
            ).fetchall()
            for j in rows:
                if self._has_native_session(conn, j["id"]):
                    continue
                native_exec = self._job_uses_native_exec(conn, j)
                known_proc = self.executor.has_process(j["pgid"])
                if native_exec and known_proc:
                    # Native execution never writes a trusted Bash RC sidecar.
                    # poll_rc is authoritative only while this daemon retains
                    # the exact Popen object and proves the group is empty.
                    rc = self.executor.poll_rc(j["pgid"])
                    if rc is None:
                        continue
                else:
                    process_state = self._job_process_state(j)
                    if process_state == "unknown":
                        self.log_line(
                            f"reap 保留 job {j['id']}: "
                            "process identity 无法确认"
                        )
                        continue
                    if native_exec:
                        if process_state in {"alive", "group_alive"}:
                            continue
                        if j["kill_reason"] in ("cancelled", "timed_out"):
                            rc = 137
                        else:
                            # After daemon restart there is no trusted Popen.
                            # Ignore any child-authored sidecar and block the
                            # consumed one-shot execution.
                            rc_path = self._job_rc_path(j)
                            state.update_job(
                                conn,
                                j["id"],
                                status="failed",
                                rc=137,
                                failure="native_rc_authority_lost",
                                finished_at=state.now(),
                            )
                            self._release_gpu_for_job(conn, j)
                            self._maybe_retry(conn, j)
                            cleanup_jobs.extend(
                                (
                                    ("launch", dict(j)),
                                    ("profile", dict(j)),
                                )
                            )
                            if rc_path is not None:
                                drop_paths.append(rc_path)
                            self.log_line(
                                f"reap native job {j['id']}: "
                                "Popen authority lost -> blocked"
                            )
                            continue
                    else:
                        marker_rc = self._read_job_rc(j)
                        if process_state == "mismatch":
                            # The immutable leader identity proves our process
                            # exited. Never poll or signal the replacement.
                            rc = marker_rc if marker_rc is not None else 137
                        else:
                            rc = self.executor.poll_rc(j["pgid"])
                            if rc is None:
                                continue
                            final_state = self._job_process_state(j)
                            if final_state in {
                                "alive",
                                "group_alive",
                                "unknown",
                            }:
                                self.log_line(
                                    f"reap 延后 job {j['id']}: "
                                    f"post-poll identity={final_state}"
                                )
                                continue
                            if marker_rc is not None:
                                rc = marker_rc
                            elif not known_proc and process_state == "dead":
                                rc = 137
                rc_path = self._job_rc_path(j)
                state.update_job(conn, j["id"], rc=rc)
                cleanup_jobs.extend(
                    self._handle_job_done(
                        conn,
                        j,
                        rc,
                        process_exit_authoritative=(
                            native_exec and known_proc
                        ),
                    )
                )
                if rc_path is not None:
                    drop_paths.append(rc_path)
        for kind, job in cleanup_jobs:
            if kind == "launch":
                self._drop_launch_marker(job)
            else:
                self._drop_profile(job)
        for path in drop_paths:
            self._drop_rc_path(path)

    def _handle_job_done(
        self,
        conn,
        j,
        rc: int | None = None,
        *,
        process_exit_authoritative: bool = False,
    ) -> list[tuple[str, dict]]:
        """Settle after exact exit and return cleanup actions for commit."""
        if j["pgid"] and not process_exit_authoritative:
            process_state = self._job_process_state(j)
            if process_state in {"alive", "group_alive", "unknown"}:
                self.log_line(
                    f"job {j['id']} settlement 延后: "
                    f"process identity={process_state}"
                )
                return []

        job_snapshot = dict(j)
        cleanup_paths = [
            ("launch", job_snapshot),
            ("profile", job_snapshot),
        ]
        if self._consume_pending_cancel_before_requeue(
            conn,
            j,
            context="finished job settlement",
        ):
            return cleanup_paths
        current = state.get_job(conn, j["id"])
        reason = current["kill_reason"] if current is not None else j["kill_reason"]
        if rc is None:
            rc = j["rc"]
        if reason == "cancelled":
            self.log_line(f"job {j['id']} cancelled (用户终止)")
            state.update_job(
                conn,
                j["id"],
                status="cancelled",
                finished_at=state.now(),
            )
            self._release_gpu_for_job(conn, j)
            return cleanup_paths
        if reason == "timed_out":
            self.log_line(f"job {j['id']} timed_out (超时)")
            state.update_job(conn, j["id"], status="timed_out", finished_at=state.now())
            self._release_gpu_for_job(conn, j)
            return cleanup_paths
        if reason in {"probe_invalid", "probe_failed", "probe_ready"}:
            if reason == "probe_ready":
                try:
                    spec = self._load_task_spec(conn, j)
                except ValueError as exc:
                    spec = None
                    self.log_line(
                        f"job {j['id']} ready probe 存量 spec 非法: {exc}"
                    )
                if spec is not None and check_declared_artifacts(
                    spec, spec.get("cwd_abs") or "."
                ):
                    state.update_job(
                        conn,
                        j["id"],
                        status="done",
                        rc=rc,
                        failure=None,
                        finished_at=state.now(),
                    )
                    self._consume_profile(conn, j, spec)
                    self.log_line(
                        f"job {j['id']} ready probe 进程已退出且产物有效 -> done"
                    )
                else:
                    state.update_job(
                        conn,
                        j["id"],
                        status="failed",
                        rc=rc,
                        failure="artifact",
                        finished_at=state.now(),
                    )
                    self.log_line(
                        f"job {j['id']} ready probe 进程已退出但产物无效 -> failed"
                    )
            else:
                failure = (
                    "invalid_spec" if reason == "probe_invalid" else "probe"
                )
                state.update_job(
                    conn,
                    j["id"],
                    status="blocked",
                    rc=rc,
                    failure=failure,
                    finished_at=state.now(),
                )
                self.log_line(
                    f"job {j['id']} probe 终止完成 -> blocked ({failure})"
                )
            self._release_gpu_for_job(conn, j)
            return cleanup_paths

        # rc 缺失 (executor 崩溃/被强杀) -> 兜底
        if rc is None:
            log_path = self._job_log_path(j)
            failure, _ = self.executor.failed_classify(log_path)
            state.update_job(
                conn, j["id"], status="failed", failure=failure,
                finished_at=state.now(),
            )
            self.log_line(f"job {j['id']} failed (rc 缺失, {failure})")
            if failure in ("oom", "gpu_fault"):
                self._capture_incident(conn, j, failure, log_path)
            self._release_gpu_for_job(conn, j)
            self._maybe_retry(conn, j)
            return cleanup_paths

        if rc == 0:
            # 产物校验 (D8)
            try:
                spec = self._load_task_spec(conn, j)
            except ValueError as exc:
                spec = None
                self.log_line(f"job {j['id']} 存量 spec 非法, 产物验证失败: {exc}")
            if spec is not None and check_declared_artifacts(
                spec, spec.get("cwd_abs") or "."
            ):
                state.update_job(conn, j["id"], status="done", rc=rc,
                                 finished_at=state.now())
                self.log_line(f"job {j['id']} done rc=0 产物校验通过")
                self._consume_profile(conn, j, spec)
            else:
                state.update_job(conn, j["id"], status="failed", rc=rc,
                                 failure="artifact",
                                 finished_at=state.now())
                self.log_line(f"job {j['id']} failed (rc=0 但产物校验失败)")
        else:
            log_path = self._job_log_path(j)
            failure, _ = self.executor.failed_classify(log_path)
            state.update_job(conn, j["id"], status="failed", rc=rc, failure=failure,
                             finished_at=state.now())
            self.log_line(f"job {j['id']} failed rc={rc} ({failure})")
            if failure in ("oom", "gpu_fault"):
                self._capture_incident(conn, j, failure, log_path)
        self._release_gpu_for_job(conn, j)
        self._maybe_retry(conn, j)
        return cleanup_paths

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
        """Read and upsert a successful profile without deleting it.

        The caller records the profile path and unlinks it only after the
        surrounding job-settlement transaction commits.
        """
        p = self._profile_path(j)
        resources = spec.get("resources") or {}
        profile_key = (
            resources.get("profile_key")
            if isinstance(resources, dict)
            else None
        )
        flags = os.O_RDONLY | os.O_NONBLOCK
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        fd = None
        try:
            fd = os.open(p, flags)
            profile_stat = os.fstat(fd)
            if (
                not stat.S_ISREG(profile_stat.st_mode)
                or profile_stat.st_uid != os.getuid()
                or profile_stat.st_nlink != 1
                or profile_stat.st_size > PROFILE_MAX_BYTES
            ):
                return
            chunks = []
            remaining = PROFILE_MAX_BYTES + 1
            while remaining:
                chunk = os.read(fd, min(8192, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            raw = b"".join(chunks)
            if len(raw) > PROFILE_MAX_BYTES:
                return
            data = json.loads(raw.decode("utf-8"))
            if (
                not isinstance(data, dict)
                or set(data) != {"peak_gib"}
            ):
                return
            peak_value = data.get("peak_gib")
            if isinstance(peak_value, bool) or not isinstance(
                peak_value, (int, float)
            ):
                return
            peak = float(peak_value)
            if (
                not math.isfinite(peak)
                or peak < 0
                or peak > PROFILE_MAX_PEAK_GIB
            ):
                return
        except (
            OSError,
            UnicodeDecodeError,
            ValueError,
            TypeError,
            RecursionError,
        ):
            return
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
        if not profile_key:
            return  # 无 key 不入库; caller 仍在 commit 后清理文件
        try:
            project = j["project"]
        except (KeyError, TypeError):
            project = None
        internal_key = _profile_cache_key(project, profile_key)
        conn.execute(
            "INSERT INTO profile_cache (profile_key, peak_gib, updated_at, git_rev)"
            " VALUES (?,?,?,?) "
            "ON CONFLICT(profile_key) DO UPDATE SET peak_gib=excluded.peak_gib,"
            " updated_at=excluded.updated_at, git_rev=excluded.git_rev",
            (internal_key, peak, state.now(), j["git_rev"]),
        )
        self.log_line(f"profile upsert job={j['id']} peak={peak:.2f} GiB")

    def _drop_profile(self, j) -> None:
        """失败/取消/超时路径: 只删临时文件不 upsert."""
        try:
            os.unlink(self._profile_path(j))
        except OSError:
            pass

    # ---------- F2: OOM 事故快照 (oom_rescheduling_research.md §2) ----------

    def _capture_incident(self, conn, j, failure: str, log_path: str) -> None:
        """failure ∈ {oom, gpu_fault} 时采集事故快照入 incidents 表.

        必须在 _release_gpu_for_job 之前调用 (gpu_jobs 同卡邻居信息释放即失).
        整体 try/except 兜底: 快照失败绝不阻塞 reap 主流程, 只 log 告警.
        """
        try:
            self._capture_incident_impl(conn, j, failure, log_path)
        except Exception as e:  # noqa: BLE001 — 旁路记录, 任何异常不外溢
            self.log_line(f"⚠️ incident 快照失败 job={j['id']}: {e!r}")

    def _capture_incident_impl(self, conn, j, failure: str, log_path: str) -> None:
        gpu_idx = j["gpu"]
        # --- 肇事任务声明值 / 历史实测峰值 ---
        declared_vram = profile_peak = None
        spec: dict = {}
        row = conn.execute(
            "SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
            (j["batch_id"], j["task_id"], j["version"]),
        ).fetchone()
        if row:
            try:
                spec = json.loads(row["spec"])
            except (json.JSONDecodeError, TypeError):
                spec = {}
        res = spec.get("resources") or {}
        if res.get("vram_gib") is not None:
            declared_vram = float(res["vram_gib"])
        pk = res.get("profile_key")
        if pk:
            prow = conn.execute(
                "SELECT peak_gib FROM profile_cache WHERE profile_key=?",
                (_profile_cache_key(j["project"], pk),),
            ).fetchone()
            if prow and prow["peak_gib"]:
                profile_peak = float(prow["peak_gib"])
        # --- 派发密度 (释放前判定): 同卡 >1 个框架任务 = shared ---
        n_on_card = conn.execute(
            "SELECT COUNT(*) AS n FROM gpu_jobs WHERE gpu_id=?", (gpu_idx,)
        ).fetchone()["n"]
        dispatch_mode = "shared" if n_on_card > 1 else "exclusive"
        # --- 同卡邻居 (gpu_jobs JOIN jobs, 排除肇事者) ---
        co_runners = []
        for r in conn.execute(
            "SELECT gj.job_id, gj.vram_gib, j.batch_id, j.task_id,"
            " j.status, j.started_at, j.project"
            " FROM gpu_jobs gj JOIN jobs j ON j.id = gj.job_id"
            " WHERE gj.gpu_id=? AND gj.job_id != ?", (gpu_idx, j["id"]),
        ).fetchall():
            cpk_row = None
            crow_spec = conn.execute(
                "SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=("
                " SELECT MAX(version) FROM tasks WHERE batch_id=? AND id=?)",
                (r["batch_id"], r["task_id"], r["batch_id"], r["task_id"]),
            ).fetchone()
            cpk = None
            if crow_spec:
                try:
                    cres = (json.loads(crow_spec["spec"]).get("resources") or {})
                    cpk = cres.get("profile_key")
                except (json.JSONDecodeError, TypeError):
                    pass
            if cpk:
                cpk_row = conn.execute(
                    "SELECT peak_gib FROM profile_cache WHERE profile_key=?",
                    (_profile_cache_key(r["project"], cpk),),
                ).fetchone()
            runtime_sec = None
            if r["started_at"]:
                try:
                    t0 = datetime.strptime(
                        r["started_at"], "%Y-%m-%d %H:%M:%S").timestamp()
                    runtime_sec = int(time.time() - t0)
                except ValueError:
                    pass
            co_runners.append({
                "job_id": r["job_id"], "batch": r["batch_id"],
                "task": r["task_id"], "status": r["status"],
                "declared_vram_gib": r["vram_gib"],
                "profile_peak_gib": (float(cpk_row["peak_gib"]) if cpk_row and cpk_row["peak_gib"] else None),
                "runtime_sec": runtime_sec,
            })
        # --- 物理事实 (fake/查询失败 -> degraded) ---
        cap_gib = self.allocator.mem_total(gpu_idx) or None
        actual_used = self.allocator.phys_mem_used_gib(gpu_idx)
        external_pids, ext_degraded = self.allocator.incident_external_pids(gpu_idx)
        packed_sum = float(self.allocator.vram_used(conn, gpu_idx))
        # --- 时间线: 最近 ~10 点 (相对秒) ---
        buf = self._mem_samples.get(gpu_idx, [])[-10:]
        now_t = time.time()
        timeline = [
            {"t_rel_sec": int(t - now_t), "used_gib": u} for (t, u) in buf
        ]
        # --- 日志摘录: 首个 OOM/Xid 特征 ±3 行, 封顶 4KiB ---
        excerpt = self._incident_log_excerpt(log_path)
        payload = {
            "failed": {
                "declared_vram_gib": declared_vram,
                "profile_peak_gib": profile_peak,
                "dispatch_mode": dispatch_mode,
                "retries": j["retries"],
                "git_rev": j["git_rev"],
                "pgid": j["pgid"],
                "rc": j["rc"],
            },
            "co_runners": co_runners,
            "memory": {
                "cap_gib": cap_gib,
                "packed_sum_gib": round(packed_sum, 2),
                "actual_used_gib": actual_used,
                "external_pids": external_pids,
                "note": "actual 含肇事进程异步销毁未释放部分; packed 为框架记账值",
            },
            "timeline": timeline,
            "log_excerpt": excerpt,
            "degraded": bool(ext_degraded or actual_used is None),
            "failure_detail": failure,
        }
        state.insert_incident(
            conn, state.now(), failure, gpu_idx, j["id"], j["batch_id"],
            json.dumps(payload, ensure_ascii=False),
        )
        state.prune_incidents(
            conn,
            max_rows=int(self.cfg.get("incidents_max_rows", 200)),
            ttl_days=int(self.cfg.get("incidents_ttl_days", 30)),
        )
        self.log_line(
            f"📸 incident 快照: job {j['id']} {failure}@GPU{gpu_idx}"
            f" mode={dispatch_mode} 邻居={len(co_runners)}"
            f" 外部进程={len(external_pids)}"
        )

    def _incident_log_excerpt(self, log_path: str, ctx_lines: int = 3,
                              max_bytes: int = 4096) -> str | None:
        """日志中首个 OOM/Xid 特征行 ±ctx_lines, 超长截尾."""
        import re as _re
        try:
            text = read_tail(log_path, 4 * 1024 * 1024)
        except OSError:
            return None
        lines = text.splitlines()
        hit = None
        for i, l in enumerate(lines):
            if ("CUDA out of memory" in l or "OutOfMemoryError" in l
                    or _re.search(r"\bXid\b|\bECC\b", l)):
                hit = i
                break
        if hit is None:
            return None
        lo = max(0, hit - ctx_lines)
        seg = "\n".join(lines[lo : hit + ctx_lines + 1])
        if len(seg) > max_bytes:
            seg = seg[-max_bytes:]
        return seg

    def _maybe_retry(self, conn, j) -> None:
        """失败重试: max_retry 内回 pending; 满 -> blocked (3.4).

        调用方传入的是更新前的 Row 快照 (status 还是 running/pending),
        必须重新取行才能看到刚写入的 failed/failure/retries.
        """
        j = state.get_job(conn, j["id"])
        if j is None or j["status"] != "failed":
            return
        if self._consume_pending_cancel_before_requeue(
            conn,
            j,
            context="failed job retry",
        ):
            return
        if j["failure"] == "perm":
            # H4: 权限错不重试
            state.update_job(conn, j["id"], status="blocked")
            return
        try:
            spec = self._load_task_spec(conn, j)
            batch = state.get_batch(conn, j["batch_id"])
            if (
                (batch is not None and batch["mode"] == "strict")
                or any(key in spec for key in _NATIVE_EXEC_METADATA_KEYS)
            ):
                state.update_job(conn, j["id"], status="blocked")
                self.log_line(
                    f"job {j['id']} native execution failed; replay denied"
                )
                return
            max_retry = int(spec.get("max_retry", DEFAULT_MAX_RETRY))
            if max_retry < 0:
                raise ValueError("max_retry 必须非负")
        except (TypeError, ValueError) as exc:
            state.update_job(conn, j["id"], status="blocked", failure="invalid_spec")
            self.log_line(f"job {j['id']} 存量 retry 规则非法, 已隔离: {exc}")
            return
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
        batch = state.get_batch(conn, j["batch_id"])
        native_exec = batch is not None and batch["mode"] == "strict"
        if not native_exec:
            try:
                persisted_spec = self._load_task_spec(conn, j)
            except ValueError:
                persisted_spec = {}
            native_exec = any(
                key in persisted_spec for key in _NATIVE_EXEC_METADATA_KEYS
            )
        # Launch marker cleanup is owned by _check_node_restart after commit.
        if self._consume_pending_cancel_before_requeue(
            conn,
            j,
            context="interrupted job recovery",
        ):
            return
        if j["gpu"] is not None:
            self._release_in_tx(conn, j["id"])
        if native_exec:
            state.update_job(
                conn,
                j["id"],
                status="blocked",
                failure="interrupted_native_no_retry",
                finished_at=state.now(),
                pgid=None,
                rc=None,
                kill_reason=None,
                gpu=None,
            )
            self.log_line(
                f"job {j['id']} native execution interrupted; automatic replay denied"
            )
            return
        state.update_job(
            conn, j["id"], status="pending", pgid=None, rc=None,
            kill_reason=None, gpu=None,
        )

    def _consume_pending_cancel_before_requeue(
        self,
        conn,
        j,
        *,
        context: str,
        rc_if_missing: int | None = None,
    ) -> bool:
        """Consume durable cancellation before terminal/requeue publication.

        Intent is represented by either ``kill_reason=cancelled`` (the daemon
        may already have acknowledged the request) or a still-pending request.
        Callers invoke this only after the launched process is known absent (or
        before any process was launched).  The surrounding writer transaction
        makes request consumption, GPU release, and the cancelled state atomic
        against CLI classification and dispatch.
        """
        requests = conn.execute(
            "SELECT id FROM control_requests"
            " WHERE job_id=? AND op='cancel' AND status='pending'"
            " ORDER BY id",
            (j["id"],),
        ).fetchall()
        current = state.get_job(conn, j["id"])
        if current is None:
            for request in requests:
                state.finish_control_request(
                    conn,
                    request["id"],
                    "job 已不存在，取消请求结束",
                )
            return bool(requests)
        if current["kill_reason"] != "cancelled" and not requests:
            return False
        if current["gpu"] is not None:
            self._release_in_tx(conn, current["id"])
        fields = {
            "status": "cancelled",
            "gpu": None,
            "pgid": None,
            "kill_reason": "cancelled",
            "finished_at": state.now(),
        }
        if current["rc"] is None and rc_if_missing is not None:
            fields["rc"] = rc_if_missing
        state.update_job(conn, current["id"], **fields)
        for request in requests:
            state.finish_control_request(
                conn,
                request["id"],
                f"取消意图优先于重新排队 ({context})",
            )
        self.log_line(
            f"job {current['id']} cancelled: pending cancel intent "
            f"阻止重新排队 ({context})"
        )
        return True

    # ---------- 依赖解锁 ----------

    def _unlock_dependent_batches(self) -> None:
        # clean keeps a terminal upstream visible while deleting its artifacts,
        # then atomically reopens it.  Dependency unlock must take the same gate
        # before its SQLite writer so it cannot publish a downstream active in
        # that external-deletion window.  This also standardizes lock ordering
        # with submit/retry/resubmit/discard: submission gate -> DB writer.
        with state.submission_lock():
            with state.connect() as conn:
                # Dependency snapshots and queued->active publication must have
                # one serialization point against every upstream reopen/retire.
                if not conn.in_transaction:
                    conn.execute("BEGIN IMMEDIATE")
                batches = conn.execute(
                    "SELECT * FROM batches WHERE status='queued'"
                ).fetchall()
                for b in batches:
                    try:
                        deps = json.loads(b["depends_on"] or "[]")
                        if (
                            not isinstance(deps, list)
                            or any(
                                not isinstance(dep, str) or not dep
                                for dep in deps
                            )
                        ):
                            raise ValueError("depends_on 必须是非空字符串数组")
                    except (json.JSONDecodeError, TypeError, ValueError) as exc:
                        conn.execute(
                            "UPDATE batches SET status='blocked'"
                            " WHERE id=? AND status='queued'",
                            (b["id"],),
                        )
                        self.log_line(
                            f"批次 {b['name']} 存量依赖规则非法, 已隔离: {exc}"
                        )
                        continue
                    if not deps:
                        conn.execute(
                            "UPDATE batches SET status='active'"
                            " WHERE id=? AND status='queued'",
                            (b["id"],),
                        )
                        continue
                    if all(self._batch_successful(conn, d) for d in deps):
                        changed = conn.execute(
                            "UPDATE batches SET status='active'"
                            " WHERE id=? AND status='queued'",
                            (b["id"],),
                        ).rowcount
                        if changed:
                            self.log_line(
                                f"批次 {b['name']} 依赖解锁 -> active"
                            )

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
            "SELECT j.* FROM jobs j"
            " JOIN (SELECT task_id, MAX(version) AS mv FROM jobs"
            "       WHERE batch_id=? GROUP BY task_id) latest"
            "   ON j.batch_id=? AND j.task_id=latest.task_id AND j.version=latest.mv",
            (b["id"], b["id"]),
        ).fetchall()
        if not jobs:
            return False
        if not all(job["status"] in ("done", "skip") for job in jobs):
            return False
        if not all(self._terminal_job_successful(conn, j) for j in jobs):
            return False
        stale_running = conn.execute(
            "SELECT 1 FROM jobs j"
            " JOIN (SELECT task_id, MAX(version) AS mv FROM jobs"
            "       WHERE batch_id=? GROUP BY task_id) latest"
            "   ON j.batch_id=? AND j.task_id=latest.task_id"
            " WHERE j.version < latest.mv"
            "   AND j.status='running'"
            " LIMIT 1",
            (b["id"], b["id"]),
        ).fetchone()
        if stale_running:
            return False
        if self._batch_has_unresolved_launch_marker(conn, b["id"]):
            return False
        return True

    # ---------- 派发 ----------

    @contextmanager
    def _dispatch_connection(self):
        self._launch_inflight = {}
        try:
            with state.connect() as conn:
                yield conn
        except BaseException:
            for job_id, pgid in list(self._launch_inflight.items()):
                try:
                    self._signal_job(
                        {"id": job_id, "pgid": pgid},
                        signal.SIGKILL,
                    )
                except Exception:
                    pass
                try:
                    still_alive = self.executor.alive(pgid)
                except (OSError, OverflowError):
                    still_alive = True
                if not still_alive:
                    self._drop_rc_path(
                        self._job_rc_path({"id": job_id, "pgid": pgid})
                    )
                    self._drop_launch_marker({"id": job_id})
            raise
        finally:
            self._launch_inflight.clear()
            self._ready_task_specs = {}
            self._ready_fingerprint_snapshots = {}

    def _reconcile_ready_batch_markers(self, conn, ready) -> None:
        """Remove stale terminal markers only for dispatchable latest batches.

        This heals a CLI death after its active/pending commit without scanning
        unbounded batch history or touching a marker owned by a newer same-name
        batch.
        """
        checked: set[str] = set()
        for job in ready:
            batch_id = str(job["batch_id"])
            if batch_id in checked:
                continue
            checked.add(batch_id)
            latest = conn.execute(
                "SELECT id FROM batches WHERE name=?"
                " ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (job["batch_name"],),
            ).fetchone()
            if latest and latest["id"] == batch_id:
                self._remove_marker(job["batch_name"], "done")
                self._remove_marker(job["batch_name"], "blocked")

    def _read_gpu_policy(self) -> dict:
        """Read at the gate; mtime and a queued reload are not admission fences."""
        try:
            return load_config(getattr(self, "_config_path", None), apply_runtime_state=False)
        except (ConfigError, OSError, ValueError) as error:
            # Keep an inbox request pending when configuration cannot be read.
            raise RuntimeError(f"GPU 策略配置不可用: {error}") from error

    def _dispatch_ready_jobs(self) -> None:
        # clean performs a two-phase database mutation around external artifact
        # deletion.  The final skip/cleanup/Popen decision must share its gate:
        # otherwise dispatch can validate a producer just before clean deletes
        # that producer's output, or start a writer whose output clean removes.
        # Keep the conservative whole-dispatch critical section for now; it
        # also guarantees the global lock order is gate -> SQLite writer.
        with state.submission_lock():
            self._dispatch_ready_jobs_serialized()

    def _dispatch_ready_jobs_serialized(self) -> None:
        from . import resources as admission
        if admission.drain_state() is not None:
            return
        try:
            gpu_policy = self._read_gpu_policy()
        except RuntimeError as error:
            self.log_line(f"本轮暂停 GPU 派发: {error}")
            if self.cfg.get("host_mem_total_gib", 0):
                return  # Never admit from stale memory limits after a failed reload.
            gpu_policy = {}  # CPU-only work continues using the last valid config.
        admission_cfg = dict(self.cfg)
        if gpu_policy:
            for key, default in (("host_mem_total_gib", 0), ("host_mem_reserve_gib", 16),
                                 ("host_mem_default_gib", 8)):
                admission_cfg[key] = gpu_policy.get(key, default)
        with self._dispatch_connection() as conn:
            # 只派发 active 批次中每个 task 的最新版本 pending job。
            # queued 批次 (依赖未解锁) 与终态批次一律不派发；done/blocked
            # 的 retry/resubmit 会在提交事务内先重开为 active。旧 state 可能
            # 遗留非最新 pending，绝不能因 daemon 重启而把旧代际重新执行。
            # B11c: 显式带出 rowid 与两处 project; 排序在 Python 层做双键
            ready = conn.execute(
                "SELECT j.*, j.rowid AS rid, b.project AS batch_project,"
                " b.priority AS batch_priority, b.name AS batch_name, b.env AS batch_env"
                " FROM jobs j JOIN batches b ON j.batch_id=b.id"
                " WHERE j.status IN ('pending','waiting_quota')"
                " AND b.status='active'"
                " AND j.version=(SELECT MAX(j2.version) FROM jobs j2"
                "   WHERE j2.batch_id=j.batch_id AND j2.task_id=j.task_id)"
                " ORDER BY j.rowid"
            ).fetchall()
            # 双键优先级排序: (-project_priority, -batch_priority, rid)。
            # project_priority 来自 config (Python 层); batch_priority 来自 DB 列。
            # 注意: sqlite3.Row 无 .get(), 键必须显式存在于 SELECT 中。
            proj_prio = self._project_priority
            ready = sorted(ready, key=lambda r: (
                -proj_prio(r["project"] or r["batch_project"]),
                -r["batch_priority"],
                r["rid"],
            ))
            marker_safe_ready = []
            for job in ready:
                if self._task_has_unresolved_launch_marker(
                    conn,
                    job["batch_id"],
                    job["task_id"],
                ):
                    self.log_line(
                        f"job {job['id']} 同 task 任一代际仍有未决 launch marker;"
                        " 本轮不派发"
                    )
                    continue
                marker_safe_ready.append(job)
            ready = marker_safe_ready
            self._reconcile_ready_batch_markers(conn, ready)
            # Snapshot every ready job before this connection performs any DML.
            # Git/NFS probes must never hold a SQLite writer transaction, and
            # the exact same snapshot feeds skip, cleanup, and launch.
            self._ready_task_specs: dict[str, dict] = {}
            self._ready_fingerprint_snapshots: dict[str, tuple] = {}
            for job in ready:
                try:
                    task_spec = self._load_task_spec(conn, job)
                except ValueError:
                    continue
                task_cwd = task_spec.get("cwd_abs") or resolve_template(
                    self.cfg.get("default_project", "{ROOT}"), self.cfg
                )
                self._ready_task_specs[job["id"]] = task_spec
                self._ready_fingerprint_snapshots[job["id"]] = (
                    self._snapshot_fingerprint(task_spec, task_cwd, job["id"], json.loads(job["batch_env"] or "{}"))
                )
            # B11c: waiting_quota 只是"配额不足被跳过"的可见标记, 不是终态;
            # 重新入候选前归一化回 pending, 否则 _launch_job 的 pending 条件
            # 更新 (M1 竞态防护) 会永远拒绝启动
            waiting_ids = [r["id"] for r in ready if r["status"] == "waiting_quota"]
            if waiting_ids:
                conn.executemany(
                    "UPDATE jobs SET status='pending'"
                    " WHERE id=? AND status='waiting_quota'"
                    " AND EXISTS (SELECT 1 FROM batches b"
                    "   WHERE b.id=jobs.batch_id AND b.status='active')"
                    " AND jobs.version=(SELECT MAX(j2.version) FROM jobs j2"
                    "   WHERE j2.batch_id=jobs.batch_id"
                    "     AND j2.task_id=jobs.task_id)",
                    ((job_id,) for job_id in waiting_ids),
                )
            # CPU 配额制 (§5b B4 v2): config.cpus_total = 节点总核数;
            # running 任务 (GPU + CPU-only) 的 CPU 占用总和 + 新任务 <= 总核数 才派发.
            # GPU 任务 CPU 占用 = resources.cpus 或 config.gpu_job_cpus (NN 训练也要 CPU).
            # cpus_total 未配置(0) -> 回退 max_cpu_jobs: CPU-only 并发上限 (定案 7 旧语义),
            # GPU 任务不受 CPU 约束 (旧版行为).
            cpus_total = int(self.cfg.get("cpus_total", 0) or 0)
            max_cpu_jobs = int(self.cfg.get("max_cpu_jobs", DEFAULT_MAX_CPU_JOBS))
            used_cpu = self._cpu_in_use(conn)
            used_memory = admission.memory_usage(conn, admission_cfg)
            memory_sample = admission.host_memory() if admission_cfg.get("host_mem_total_gib", 0) else None
            outstanding_memory = admission.memory_outstanding(conn, admission_cfg) if memory_sample else 0
            launched_memory = 0.0
            resource_waits = {}
            cpu_only_running = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE status='running' AND gpu IS NULL"
            ).fetchone()[0]
            now_ts = datetime.now()
            # B14 L4: batch 内并发上限 (sweep.max_parallel)
            running_per_batch: dict[str, int] = {}
            for r in conn.execute(
                "SELECT batch_id, COUNT(*) AS n FROM jobs"
                " WHERE status='running' GROUP BY batch_id"
            ):
                running_per_batch[r["batch_id"]] = r["n"]
            # B11c: refresh per-project running GPU counts
            self._update_project_quota_used(conn)

            for j in ready:
                if self._launch_marker_alive(j):
                    # A previous launch survived transaction rollback; keep its
                    # marker and wait for recovery instead of double-starting.
                    continue
                project = j["project"] or j["batch_project"]

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
                try:
                    spec = self._ready_task_specs.get(j["id"])
                    if spec is None:
                        spec = self._load_task_spec(conn, j)
                    resources = spec.get("resources") or {}
                    if not isinstance(resources, dict):
                        raise ValueError("resources 必须是对象")
                    gpu_request = resources.get("gpu", 1)
                    if isinstance(gpu_request, bool) or gpu_request not in (0, 1):
                        raise ValueError("resources.gpu 必须是 0 或 1")
                    is_cpu_only = gpu_request == 0
                    task_cpus = self._task_cpus(spec)
                    task_memory = admission.host_mem_gib(spec, admission_cfg)
                    if task_cpus <= 0:
                        raise ValueError("resources.cpus 必须为正整数")
                    mp = spec.get("max_parallel")
                    if mp is not None:
                        mp = int(mp)
                        if mp <= 0:
                            raise ValueError("max_parallel 必须为正整数")
                except (TypeError, ValueError) as exc:
                    isolated = conn.execute(
                        "UPDATE jobs SET status='blocked', failure='invalid_spec',"
                        " finished_at=? WHERE id=?"
                        " AND status IN ('pending','waiting_quota')"
                        " AND EXISTS (SELECT 1 FROM batches b"
                        "   WHERE b.id=jobs.batch_id AND b.status='active')"
                        " AND jobs.version=(SELECT MAX(j2.version) FROM jobs j2"
                        "   WHERE j2.batch_id=jobs.batch_id"
                        "     AND j2.task_id=jobs.task_id)",
                        (state.now(), j["id"]),
                    )
                    if isolated.rowcount:
                        self.log_line(
                            f"job {j['id']} 存量 spec 非法, 已隔离: {exc}"
                        )
                    continue
                if not is_cpu_only and not project_gpu_enabled(gpu_policy, project):
                    continue
                # GPU quota does not apply to CPU-only work.
                if not is_cpu_only and not self._project_quota_available(conn, project):
                    continue
                # B14 L4: sweep.max_parallel -- 同批 running 达上限则等下轮
                if mp and running_per_batch.get(j["batch_id"], 0) >= mp:
                    resource_waits[j["id"]] = "parallel"
                    continue
                if cpus_total > 0 and used_cpu + task_cpus > cpus_total:
                    resource_waits[j["id"]] = "cpu"
                    # CPU 配额不足: 本任务等下轮 (CPU 超卖禁止, 与 GPU 同纪律)
                    # 批内补位: continue 让后面的小任务可插队 (大任务等 GPU 释放同轮再试)
                    continue
                if not admission.memory_available(admission_cfg, used_memory, task_memory,
                                                  memory_sample, launched_memory, outstanding_memory):
                    resource_waits[j["id"]] = "host_memory"
                    continue
                if is_cpu_only:
                    if cpus_total <= 0 and cpu_only_running >= max_cpu_jobs:
                        resource_waits[j["id"]] = "cpu"
                        continue  # 回退模式: CPU-only 并发上限 (旧语义)
                    gpu = None  # CPU-only: 不占 GPU 槽位
                else:
                    # A rejection is request-specific unless the allocator can
                    # prove a task-independent global exhaustion condition.
                    self._assign_reject_scope = "request"
                    gpu = self._assign_in_tx(conn, j["id"], spec, project)
                    if gpu is None:
                        resource_waits[j["id"]] = "gpu"
                        continue
                try:
                    launched = self._launch_job(conn, j, gpu)
                    # D1: 竞态放弃/skip 不占 CPU 配额 (skip 密集批次不再人为压低并发)
                    if launched:
                        used_cpu += task_cpus
                        used_memory += task_memory
                        launched_memory += task_memory
                        if is_cpu_only:
                            cpu_only_running += 1
                        else:
                            self._project_quota_used[project] = (
                                self._project_quota_used.get(project, 0) + 1
                            )
                        running_per_batch[j["batch_id"]] = (
                            running_per_batch.get(j["batch_id"], 0) + 1
                        )
                except Exception as e:
                    self.log_line(f"LAUNCH FAIL job {j['id']} gpu={gpu}: {e}")
                    if not conn.in_transaction:
                        conn.execute("BEGIN IMMEDIATE")
                    else:
                        conn.execute("UPDATE jobs SET status=status WHERE 0")
                    current = state.get_job(conn, j["id"])
                    eligible = conn.execute(
                        "SELECT 1 FROM jobs j JOIN batches b ON b.id=j.batch_id"
                        " WHERE j.id=? AND j.status IN ('pending','waiting_quota','running')"
                        " AND b.status='active'"
                        " AND j.version=(SELECT MAX(j2.version) FROM jobs j2"
                        "   WHERE j2.batch_id=j.batch_id AND j2.task_id=j.task_id)",
                        (j["id"],),
                    ).fetchone()
                    if (
                        current is None
                        or self._has_native_session(conn, current["id"])
                        or self._has_native_v2_contract(conn, current)
                        or eligible is None
                    ):
                        self.log_line(
                            f"LAUNCH FAIL job {j['id']}: 状态/代际已变化; "
                            "保留当前 job/resource/marker"
                        )
                        continue
                    if self._prepare_launch_marker(current):
                        self.log_line(
                            f"LAUNCH FAIL job {j['id']}: orphan identity 未决; "
                            "保留 running/resource/marker"
                        )
                        continue
                    if gpu is not None:
                        self._release_in_tx(conn, j["id"])
                    state.update_job(
                        conn,
                        j["id"],
                        status="failed",
                        failure="launch",
                        finished_at=state.now(),
                    )
                    self._maybe_retry(conn, j)

            admission.publish_admission(memory_sample, resource_waits)

    def _drain_complete(self) -> bool:
        """Stop after all owned work exits, preserving pending jobs and inbox."""
        from . import resources as admission
        with state.submission_lock():
            request = admission.drain_state()
            if not request or not request.get("stop") or request.get("invalid"):
                return False
            if self._unresolved_launch_markers():
                return False
            with state.connect() as conn:
                # Hold the SQLite writer through marker publication.  A T1
                # claim that wins first is visible here; a later claim sees
                # the marker after its own running CAS and rolls back.
                conn.execute("BEGIN IMMEDIATE")
                running = conn.execute("SELECT 1 FROM jobs WHERE status='running' LIMIT 1").fetchone()
                if running:
                    return False
                state.mark_idle_shutdown()
            self.log_line("排空完成，停止 daemon；保留 pending 与排空请求，resume 后恢复派发")
            return True

    def _task_cpus(self, spec: dict) -> int:
        """Return a validated CPU reservation for one task."""
        resources = spec.get("resources") or {}
        if not isinstance(resources, dict):
            raise ValueError("resources 必须是对象")
        cpus = resources.get("cpus")
        if cpus is not None:
            if isinstance(cpus, bool) or not isinstance(cpus, int) or cpus <= 0:
                raise ValueError("resources.cpus 必须为正整数")
            return cpus
        if resources.get("gpu", 1) == 0:
            return 1
        return int(self.cfg.get("gpu_job_cpus", DEFAULT_GPU_JOB_CPUS))

    def _cpu_in_use(self, conn) -> int:
        """当前 running 任务 (GPU + CPU-only) 的 CPU 占用总和."""
        used = 0
        rows = conn.execute(
            "SELECT * FROM jobs WHERE status='running'"
        ).fetchall()
        for j in rows:
            try:
                spec = self._load_task_spec(conn, j)
                used += self._task_cpus(spec)
            except (AttributeError, TypeError, ValueError):
                # Fail closed for quota accounting: reserve the default GPU
                # job CPU share until the malformed record is isolated.
                used += int(self.cfg.get("gpu_job_cpus", DEFAULT_GPU_JOB_CPUS))
        return used

    def _gpu_dispatch_suppressed(self, idx: int) -> bool:
        """Share Allocator's util-debounce gate across every packing path."""
        # Use the concrete implementation as an unbound method so narrow Mock
        # allocators in state-machine tests default to an empty suppression set
        # instead of fabricating a truthy ``is_dispatch_suppressed`` method.
        return Allocator.is_dispatch_suppressed(self.allocator, idx)

    def _assign_in_tx(self, conn, job_id: str, spec: dict | None = None, project: str | None = None) -> int | None:
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
        # B12-b: 项目级开关 (与门第三项): 项目 colocate=false -> 降级独占。
        # 独占占整卡后他人本就不可 pack (S7), 天然零跨项目干扰, 无需额外隔离。
        if gpu_share and project:
            if (self._projects.get(project) or {}).get("colocate") is False:
                self.log_line(
                    f"job {job_id}: 项目 {project} 已禁用 colocate -> gpu_share 降级独占"
                )
                gpu_share = False
        # 装箱值 = max(声明 vram_gib, profile_cache.peak_gib) (定案 39 L1, profile 命中)
        task_vram = None
        if gpu_share:
            task_vram = float(resources.get("vram_gib", 0.0) or 0.0)
            pk = resources.get("profile_key")
            if pk:
                row = conn.execute(
                    "SELECT peak_gib FROM profile_cache WHERE profile_key=?",
                    (_profile_cache_key(project, pk),),
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
            # B11c: project card selection.
            # affinity_hard=true: 只允许 affinity 列内的卡 (硬隔离, 防跨项目混卡 OOM);
            #   全忙 -> 返回 None 由调用方按无卡处理 (下轮重试)。
            # 默认(软): 亲和卡优先, 不满足可借其他卡。
            affinity = self._project_affinity(project) if project else []
            hard = self._project_affinity_hard(project) if project else False
            valid = set(self.allocator.gpu_list)
            if hard and affinity:
                gpu_order = [i for i in affinity if i in valid]
            else:
                gpu_order = list(affinity) + [i for i in self.allocator.gpu_list if i not in affinity]
            for idx in gpu_order:
                if self._gpu_dispatch_suppressed(idx):
                    continue
                row = conn.execute(
                    "SELECT status, quarantined FROM gpus WHERE idx=?", (idx,)
                ).fetchone()
                if not row or row["quarantined"]:
                    continue
                if row["status"] == "free":
                    if excl_vram is not None:
                        cap = self.allocator.mem_total(idx)
                        if cap > 0 and excl_vram > cap:
                            continue  # declared peak exceeds card capacity
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
            self._assign_reject_scope = "project" if hard and affinity else "all"
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
        preferred_idx: int | None = None
        preferred_load = float("inf")
        cap_skipped = False   # B12-c: 候选卡中是否有因项目级上限被跳过
        self._assign_reject_scope = "all"   # 默认: 无卡对所有 GPU 任务一视同仁
        # B11c: shared packing honors the same hard/soft affinity semantics
        affinity_s = self._project_affinity(project) if project else []
        hard_s = self._project_affinity_hard(project) if project else False
        valid_s = set(self.allocator.gpu_list)
        affinity_set = set(affinity_s)
        if hard_s and affinity_s:
            gpu_order_s = [i for i in affinity_s if i in valid_s]
        else:
            gpu_order_s = list(affinity_s) + [i for i in self.allocator.gpu_list if i not in affinity_s]
        for idx in gpu_order_s:
            if self._gpu_dispatch_suppressed(idx):
                continue
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
                # B12-c: 三级打包上限. 全局/卡级是物理容量属性 -> 约束卡上
                # 总任务数; 项目级是策略属性 -> 只数该项目在此卡的 任务
                # (不同计数器, 不能折叠进一个 min 数). 超额只挡新 pack 不驱逐
                # (定案 Q5: 自然排水).
                cap_all = int(self.cfg.get("co_locate_max_jobs", 3))
                gcap = self._gpu_max_jobs.get(idx)
                if gcap is not None:
                    cap_all = min(cap_all, int(gcap))
                if self.allocator.job_count(conn, idx) >= cap_all:
                    cap_skipped = True
                    continue
                pmax = (self._projects.get(project or "") or {}).get("max_jobs")
                if pmax is not None:
                    proj_n = conn.execute(
                        "SELECT COUNT(*) AS n FROM gpu_jobs gj"
                        " JOIN jobs j ON j.id=gj.job_id"
                        " WHERE gj.gpu_id=? AND j.project=?",
                        (idx, project),
                    ).fetchone()["n"]
                    if proj_n >= int(pmax):
                        cap_skipped = True
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
            if idx in affinity_set:
                if load < preferred_load:
                    preferred_load = load
                    preferred_idx = idx
            elif load < best_load:
                best_load = load
                best_idx = idx
        if preferred_idx is not None:
            best_idx = preferred_idx
        if best_idx is None:
            # B12-c: 项目级上限挡住 ≠ 全卡满。scope=project 时调用方不置
            # gpu_full, 同 tick 后续其他项目的任务仍可尝试该卡。
            self._assign_reject_scope = "project" if cap_skipped else "all"
            if cap_skipped and job_id not in self._cap_warned:
                self._cap_warned.add(job_id)
                self.log_line(
                    f"job {job_id}: 暂无余量卡 (打包上限"
                    f" 全局={self.cfg.get('co_locate_max_jobs', 3)}"
                    f" 卡级={self._gpu_max_jobs or '-'}"
                    f" 项目级={(self._projects.get(project or '') or {}).get('max_jobs', '-')}"
                    ") —— 等待自然排水"
                )
            return None
        self._cap_warned.discard(job_id)
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
    def _snapshot_fingerprint(
        self, spec: dict, cwd: str, job_id: str, batch_env: dict | None = None
    ) -> tuple[str | None, dict, str | None]:
        try:
            current_fp, stage_fingerprints, git_rev = compute_fingerprint(
                spec.get("cmd"),
                spec.get("stages"),
                cwd,
                spec.get("git"),
                getattr(self, "venv_paths", {}),
                runtime_prefix=spec.get("runtime_prefix"),
                execution_env=task_environment(getattr(self, "cfg", {}), batch_env, spec.get("env")),
                artifacts=spec.get("artifacts"),
                **_native_exec_fingerprint_kwargs(spec),
            )
        except Exception as exc:
            self.log_line(
                f"job {job_id} 指纹探测失败, 禁止复用 checkpoint: {exc}"
            )
            return None, {}, None
        if not isinstance(stage_fingerprints, dict):
            stage_fingerprints = {}
        return current_fp, stage_fingerprints, git_rev

    def _reattest_native_launch(
        self,
        batch,
        job,
        spec: dict,
        task_project: object,
    ) -> dict[str, Any] | None:
        """Re-attest one persisted native tuple before the running claim."""
        metadata = _native_exec_metadata(spec)
        if metadata is None:
            if batch["mode"] == "strict":
                raise NativeExecProfileError(
                    "strict task is missing persisted native execution metadata"
                )
            return None
        if spec.get("stages") is not None:
            raise NativeExecProfileError("native execution profile forbids stages")
        try:
            batch_env = json.loads(batch["env"] or "{}")
        except (json.JSONDecodeError, TypeError) as exc:
            raise NativeExecProfileError(
                "persisted native batch environment is invalid"
            ) from exc
        v2_contract = metadata.get(NATIVE_EXEC_V2_CONTRACT_FIELD)
        is_v2 = NATIVE_EXEC_V2_CONTRACT_FIELD in metadata
        if is_v2:
            if not isinstance(v2_contract, dict):
                raise NativeExecProfileError(
                    "persisted native V2 contract must be an object"
                )
            runtime = v2_contract.get("runtime")
            resources = spec.get("resources")
            if not isinstance(runtime, dict) or not isinstance(resources, dict):
                raise NativeExecProfileError(
                    "persisted native V2 runtime or resources are invalid"
                )
            try:
                depends_on = json.loads(batch["depends_on"])
            except (json.JSONDecodeError, TypeError) as exc:
                raise NativeExecProfileError(
                    "persisted native V2 batch dependencies are invalid"
                ) from exc
            if (
                batch["cwd"] != v2_contract.get("cwd")
                or depends_on != v2_contract.get("depends_on")
                or batch_env != v2_contract.get("batch_env")
                or spec.get("env") != v2_contract.get("task_env")
                or spec.get("runtime") != v2_contract.get("runtime")
                or spec.get("runtime_prefix") != runtime.get("prefix")
                or type(spec.get("duration_min")) is not int
                or spec.get("duration_min") != v2_contract.get("duration_min")
                or spec.get("max_retry") != v2_contract.get("max_retry")
                or spec.get("artifacts") != v2_contract.get("artifacts")
                or spec.get("id") != job["task_id"]
                or spec.get("_force_rerun") is not None
                or spec.get("max_parallel") is not None
                or spec.get("progress_regex") is not None
                or spec.get("paths_escape") is not False
                or not isinstance(v2_contract.get("resources"), dict)
                or {"gpu": resources.get("gpu"), "cpus": resources.get("cpus")}
                != v2_contract["resources"]
                or batch.get("gpus") is not None
                or batch["notify"] is not None
                or type(batch["priority"]) is not int
                or batch["priority"] != 0
            ):
                raise NativeExecProfileError(
                    "persisted native V2 batch/task fields differ from frozen contract"
                )
        elif batch_env or spec.get("env"):
            raise NativeExecProfileError(
                "persisted native execution must have empty batch/task env"
            )
        resources = spec.get("resources")
        if (
            not isinstance(resources, dict)
            or frozenset(resources) != {"gpu", "cpus", "gpu_share"}
            or type(resources["gpu"]) is not int
            or type(resources["cpus"]) is not int
            or type(resources["gpu_share"]) is not bool
            or resources != {"gpu": 0, "cpus": 1, "gpu_share": False}
        ):
            raise NativeExecProfileError(
                "persisted native execution resources must remain CPU-only"
            )
        configured_root = os.path.realpath(
            os.path.expanduser(
                resolve_template(f"{{PROJECT:{batch['project']}}}", self.cfg)
            )
        )
        frozen_roots = getattr(self, "_native_exec_project_roots", None)
        frozen_identities = getattr(
            self, "_native_exec_project_root_identities", None
        )
        if not isinstance(frozen_roots, dict) or not isinstance(
            frozen_identities, dict
        ):
            raise NativeExecProfileError(
                "native project-root cold snapshot is unavailable"
            )
        frozen_root = frozen_roots.get(batch["project"])
        frozen_identity = frozen_identities.get(batch["project"])
        try:
            current_stat = os.stat(configured_root)
            current_identity = (current_stat.st_dev, current_stat.st_ino)
        except OSError as exc:
            raise NativeExecProfileError(
                "current native project root cannot be attested"
            ) from exc
        if (
            configured_root != frozen_root
            or current_identity != frozen_identity
            or spec.get("cwd_abs") != frozen_root
        ):
            raise NativeExecProfileError(
                "persisted native cwd differs from frozen project-root identity"
            )
        current_root_identity_sha256 = (
            native_exec_project_root_identity_sha256(configured_root)
        )
        if (
            metadata["_native_exec_project_root_identity_sha256"]
            != current_root_identity_sha256
        ):
            raise NativeExecProfileError(
                "persisted native project-root identity digest drifted"
            )
        if not is_v2 and (
            spec.get("runtime") is not None
            or spec.get("runtime_prefix") is not None
        ):
            raise NativeExecProfileError(
                "persisted native execution unexpectedly declares a runtime"
            )
        if spec.get("git") is not False:
            raise NativeExecProfileError(
                "persisted native execution git binding must remain exactly false"
            )
        if spec.get("artifacts") or spec.get("paths_escape"):
            raise NativeExecProfileError(
                "persisted native execution declares artifact cleanup rules"
            )
        if spec.get("probes") is not None:
            raise NativeExecProfileError(
                "persisted native execution declares scheduler log probes"
            )
        if type(spec.get("max_retry")) is not int or spec["max_retry"] != 0:
            raise NativeExecProfileError(
                "persisted native execution max_retry must remain exactly zero"
            )
        submitted_argv = metadata["_native_exec_submitted_argv"]
        if spec.get("cmd") != submitted_argv:
            raise NativeExecProfileError(
                "persisted native command differs from submitted argv"
            )
        if (
            job["project"] != batch["project"]
            or task_project != batch["project"]
        ):
            raise NativeExecProfileError(
                "persisted native project binding is inconsistent"
            )
        resolved = reattest_native_exec_profile(
            self.cfg,
            mode=batch["mode"],
            project=batch["project"],
            batch_name=batch["name"],
            task_id=job["task_id"],
            profile_id=metadata["_native_exec_profile_id"],
            profile_sha256=metadata["_native_exec_profile_sha256"],
            submitted_argv=submitted_argv,
            batch_contract=v2_contract if is_v2 else None,
        )
        if is_v2 != (resolved.get("schema") == NATIVE_EXEC_PROFILE_V2_SCHEMA):
            raise NativeExecProfileError(
                "persisted native contract schema differs from cold profile"
            )
        return resolved


    def _launch_job(self, conn, j, gpu: int | None) -> bool:
        """启动任务; 返回是否真正启动 (调用方据此计 CPU/并发配额, D1)。

        未启动的正常返回路径: M1 竞态 (已非 pending) 与产物指纹 skip ——
        二者都不该占用 CPU 配额 (skip 密集批次会人为压低并发)。
        """
        if self._task_has_unresolved_launch_marker(
            conn,
            j["batch_id"],
            j["task_id"],
            exclude_job_id=j["id"],
        ):
            self.log_line(
                f"job {j['id']} 同 task 旧代际 launch marker 未决; 放弃启动"
            )
            if gpu is not None:
                self._release_in_tx(conn, j["id"])
            return False
        live_job = state.get_job(conn, j["id"])
        if live_job is None:
            raise ValueError(f"job {j['id']} no longer exists")
        for identity_key in ("batch_id", "task_id", "version"):
            if j[identity_key] != live_job[identity_key]:
                raise ValueError(
                    f"job {j['id']} persisted {identity_key} drifted"
                )
        j = live_job
        b = state.get_batch(conn, j["batch_id"])
        if b is None:
            raise ValueError(f"job {j['id']} batch no longer exists")

        persisted_spec, task_project = self._load_task_launch_binding(conn, j)
        spec_cache = getattr(self, "_ready_task_specs", {})
        cached_spec = spec_cache.pop(j["id"], None)
        native_candidate = (
            b["mode"] == "strict"
            or any(key in persisted_spec for key in _NATIVE_EXEC_METADATA_KEYS)
            or (
                cached_spec is not None
                and any(
                    key in cached_spec for key in _NATIVE_EXEC_METADATA_KEYS
                )
            )
        )
        if (
            native_candidate
            and cached_spec is not None
            and cached_spec != persisted_spec
        ):
            raise NativeExecProfileError(
                "persisted native task spec drifted after fingerprint snapshot"
            )
        spec = persisted_spec if native_candidate else (cached_spec or persisted_spec)
        native_binding = self._reattest_native_launch(
            b,
            j,
            spec,
            task_project,
        )
        if (
            native_binding is not None
            and native_binding.get("schema") == NATIVE_EXEC_PROFILE_V2_SCHEMA
        ):
            raise NativeLaunchUnavailable(
                "native V2 formal launcher and lifecycle are not connected"
            )
        if native_binding is not None and gpu is not None:
            raise NativeExecProfileError(
                "native CPU-only execution received a GPU assignment"
            )

        if self._prepare_launch_marker(j):
            self.log_line(
                f"job {j['id']} 存在未决 launch marker; 本轮不派发"
            )
            if gpu is not None:
                self._release_in_tx(conn, j["id"])
            return False
        cwd = spec.get("cwd_abs") or resolve_template(
            self.cfg.get("default_project", "{ROOT}"), self.cfg
        )
        snapshot_cache = getattr(self, "_ready_fingerprint_snapshots", {})
        fingerprint_snapshot = snapshot_cache.pop(j["id"], _FINGERPRINT_UNSET)
        if fingerprint_snapshot is _FINGERPRINT_UNSET:
            # Direct callers receive the same no-writer-transaction guarantee.
            if conn.in_transaction:
                conn.commit()
            batch = state.get_batch(conn, j["batch_id"])
            fingerprint_snapshot = self._snapshot_fingerprint(
                spec, cwd, j["id"], json.loads(batch["env"] or "{}") if batch else {},
            )
        current_fp, stage_fingerprints, git_rev = fingerprint_snapshot

        # M1 修复: 条件更新抢占 —— SELECT/指纹快照到 launch 之间可能已被
        # cancel、批次终止或 resubmit 成旧代际；最终写必须再次原子校验
        # pending + active batch + latest task generation。
        cur = conn.execute(
            "UPDATE jobs SET status='running', started_at=?"
            " WHERE id=? AND status='pending'"
            " AND EXISTS (SELECT 1 FROM batches b"
            "   WHERE b.id=jobs.batch_id AND b.status='active')"
            " AND jobs.version=(SELECT MAX(j2.version) FROM jobs j2"
            "   WHERE j2.batch_id=jobs.batch_id AND j2.task_id=jobs.task_id)",
            (state.now(), j["id"]),
        )
        if cur.rowcount == 0:
            self.log_line(
                f"job {j['id']} 派发竞态: 已非 active/latest pending, 放弃启动"
            )
            if gpu is not None:
                self._release_in_tx(conn, j["id"])
            return False
        log_path = self._job_log_path(j)
        state.ensure_private_directory(os.path.dirname(log_path))
        should_skip = self._should_skip(
            conn, spec, j, current_fingerprint=current_fp
        )
        state.update_job(
            conn,
            j["id"],
            fingerprint=current_fp,
            stage_fingerprints=(
                json.dumps(stage_fingerprints) if stage_fingerprints else None
            ),
            git_rev=git_rev,
        )
        if should_skip:
            state.update_job(conn, j["id"], status="skip", finished_at=state.now())
            self.log_line(
                f"job {j['id']} skip (产物指纹有效, 不执行)"
                f" 匹配版本 git_rev={str(git_rev or '-')[:8]}"
                " —— 若为改码后误 SKIP: 确认已 commit, 或 submit 加 \"force_rerun\": true"
            )
            if gpu is not None:
                self._release_in_tx(conn, j["id"])
            return False
        # Destructive launch preparation is safe only after the reservation and
        # running claim can be recovered independently of this outer dispatch
        # transaction. A skip remains part of the caller's transaction.
        state.update_job(
            conn,
            j["id"],
            gpu=gpu,
            pgid=None,
            kill_reason=None,
        )
        conn.commit()
        prior_inflight = getattr(self, "_launch_inflight", None)
        if isinstance(prior_inflight, dict):
            # This commit also made any earlier launch pgid writebacks durable.
            prior_inflight.clear()
        # H2: retry/previous daemon attempts may leave a stale RC for this job.
        self._drop_job_rc(j)
        # 半成品清理 (§3.2): 产物存在但无效 (指纹不匹配/规则不过) -> 删除后启动
        self._clean_stale_artifacts(
            conn,
            spec,
            j,
            current_fingerprint=current_fp,
            stage_fingerprints=stage_fingerprints,
        )

        # The scheduler-owned profile path is a reserved control channel.
        # Batch, task, and deployment defaults may not redirect it.
        batch_env = json.loads(b["env"]) if b and b["env"] else {}
        if native_binding is not None:
            task_env = {}
        else:
            task_env = task_environment(self.cfg, batch_env, spec.get("env"))
        task_env["SCHED_PROFILE_OUT"] = self._profile_path(j)
        # Dispatcher-owned live identity: these values are derived from the
        # persisted batch/job records and must override batch/task/default env.
        task_env["SCHED_BATCH_ID"] = str(b["name"])
        task_env["SCHED_TASK_ID"] = str(j["task_id"])
        task_env["SCHED_RUN_ID"] = str(j["id"])
        task_env["SCHED_PROJECT"] = str(b["project"])
        rc_dir = os.path.join(self.host_dir, "rc")
        state.ensure_private_directory(rc_dir)
        task_env["SCHED_RC_DIR"] = rc_dir
        task_env["SCHED_RC_PREFIX"] = self._job_rc_prefix(j)
        launch_marker = self._launch_marker_path(j)
        state.ensure_private_directory(os.path.dirname(launch_marker))
        task_env["SCHED_LAUNCH_MARKER"] = launch_marker
        state.ensure_private_directory(
            os.path.dirname(task_env["SCHED_PROFILE_OUT"])
        )
        launch_kwargs = {
            "cmd": spec.get("cmd"),
            "stages": spec.get("stages"),
            "cwd": cwd,
            "env": task_env,
            "gpu": gpu,
            "log_path": log_path,
            "conda_env_dir": spec.get("runtime_prefix"),
            "stage_fingerprints": stage_fingerprints,
            "stage_checkpoint_dir": os.path.join(
                self.host_dir, "stage_checkpoints", j["id"]
            ),
            "force_rerun": bool(spec.get("_force_rerun")),
        }
        if native_binding is not None:
            launch_kwargs.update(
                {
                    "native_exec_profile_id": native_binding["profile_id"],
                    "native_exec_profile_sha256": native_binding[
                        "profile_sha256"
                    ],
                    "native_exec_submitted_argv": list(
                        native_binding["submitted_argv"]
                    ),
                }
            )
        pgid = self.executor.launch(**launch_kwargs)
        inflight = getattr(self, "_launch_inflight", None)
        if isinstance(inflight, dict):
            inflight[j["id"]] = pgid
        def abort_launch() -> None:
            try:
                self._signal_job(
                    {"id": j["id"], "pgid": pgid},
                    signal.SIGKILL,
                )
            except Exception:
                pass
            try:
                still_alive = self.executor.alive(pgid)
            except (OSError, OverflowError):
                still_alive = True
            if not still_alive:
                self._drop_rc_path(self._job_rc_path(j, pgid))
                self._drop_launch_marker(j)
            if isinstance(inflight, dict):
                inflight.pop(j["id"], None)
        try:
            state.update_job(
                conn, j["id"], gpu=gpu, pgid=pgid,
                kill_reason=None,
            )
        except Exception:
            try:
                self.log_line(
                    f"LAUNCH 回写失败: job {j['id']} pgid={pgid} -> SIGKILL 防止孤儿进程"
                )
            except Exception:
                pass
            abort_launch()
            raise
        tag = f"cpu" if gpu is None else f"gpu={gpu}"
        try:
            self.log_line(f"LAUNCH job {j['id']} {tag} pgid={pgid}")
        except Exception:
            # Logging is not part of launch correctness.  At this point Popen
            # succeeded and the durable running row has its exact pgid; rolling
            # the job back for a log sink failure can detach a still-live group
            # from its GPU lease and make the outer abort path leak gpu_jobs.
            pass
        return True

    def _should_skip(
        self,
        conn,
        spec: dict,
        j,
        current_fingerprint=_FINGERPRINT_UNSET,
    ) -> bool:
        """Return whether valid artifacts were produced by the current code."""
        if spec.get("_force_rerun"):
            return False
        artifact_groups = [spec.get("artifacts")]
        stages = spec.get("stages")
        if isinstance(stages, list):
            artifact_groups.extend(
                stage.get("artifacts") for stage in stages
                if isinstance(stage, dict)
            )
        if not any(
            isinstance(artifacts, dict) and artifacts
            for artifacts in artifact_groups
        ):
            return False
        if not self._fingerprint_matches(
            conn, spec, j, current_fingerprint=current_fingerprint
        ):
            return False
        return check_declared_artifacts(spec, spec.get("cwd_abs") or ".")

    def _fingerprint_matches(
        self,
        conn,
        spec: dict,
        j,
        current_fingerprint=_FINGERPRINT_UNSET,
    ) -> bool:
        """B13-§4 语义修正: 对照"产物生产者"的指纹, 而非本行自比.

        旧实现 cur == j["fingerprint"] 是提交时/派发时两次对同一树状态采样,
        永远自洽 —— 改码后 resubmit 照样 SKIP.
        正确语义: 磁盘上的产物必须有同 task 的可信 done/skip 生产者;
        当前态指纹与其不同时必须重跑。首跑没有可信生产者，绝不复用磁盘上
        预先存在的产物。
        """
        if current_fingerprint is _FINGERPRINT_UNSET:
            try:
                batch = state.get_batch(conn, j["batch_id"])
                current_fingerprint, _, _ = compute_fingerprint(
                    spec.get("cmd"), spec.get("stages"),
                    spec.get("cwd_abs") or ".", spec.get("git"), self.venv_paths,
                    runtime_prefix=spec.get("runtime_prefix"),
                    execution_env=task_environment(
                        getattr(self, "cfg", {}),
                        json.loads(batch["env"] or "{}") if batch else {},
                        spec.get("env"),
                    ),
                    artifacts=spec.get("artifacts"),
                    **_native_exec_fingerprint_kwargs(spec),
                )
            except Exception:
                return False
        if not current_fingerprint:
            return False
        # 产物生产者 = 同项目同任务名最近一个终态 job (跨批次实例:
        # 每次 submit 都生成新 batch id, 同 batch 内永远没有"前序版本")
        prev = conn.execute(
            "SELECT j.fingerprint FROM jobs j"
            " WHERE j.project=? AND j.task_id=?"
            "   AND j.status IN ('done','skip') AND j.fingerprint IS NOT NULL"
            "   AND j.id != ?"
            " ORDER BY j.finished_at DESC, j.rowid DESC LIMIT 1",
            (j["project"] if "project" in j.keys() else None,
             j["task_id"], j["id"]),
        ).fetchone()
        if not prev or not prev["fingerprint"]:
            return False
        return current_fingerprint == prev["fingerprint"]

    def _clean_stale_artifacts(
        self,
        conn,
        spec: dict,
        j,
        current_fingerprint=_FINGERPRINT_UNSET,
        stage_fingerprints: dict | None = None,
    ) -> None:
        """Remove outputs without valid task or job-scoped stage provenance."""

        cwd = spec.get("cwd_abs") or "."
        force_rerun = bool(spec.get("_force_rerun"))
        checkpoint_dir = os.path.join(
            self.host_dir, "stage_checkpoints", j["id"]
        )
        fingerprints = stage_fingerprints or {}
        stages = spec.get("stages")
        stage_paths: set[str] = set()

        def resolved_path(rule) -> str | None:
            if not isinstance(rule, dict):
                return None
            raw = rule.get("path")
            if not isinstance(raw, str) or not raw:
                return None
            return raw if os.path.isabs(raw) else os.path.normpath(
                os.path.join(cwd, raw)
            )

        def remove_rule(rule, *, paths_escape: bool) -> None:
            path = resolved_path(rule)
            if path is None or not isinstance(rule, dict):
                return
            raw = rule.get("path")
            if not isinstance(raw, str):
                return
            if unlink_artifact(
                cwd,
                raw,
                paths_escape=paths_escape,
            ):
                self.log_line(f"清理过期产物 {path}")

        rerun_downstream = force_rerun
        if isinstance(stages, list):
            for index, stage in enumerate(stages):
                artifacts = (
                    stage.get("artifacts", {})
                    if isinstance(stage, dict)
                    else {}
                )
                if isinstance(artifacts, dict):
                    for rule in artifacts.values():
                        path = resolved_path(rule)
                        if path is not None:
                            stage_paths.add(path)
                valid = (
                    not rerun_downstream
                    and isinstance(stage, dict)
                    and stage_checkpoint_valid(
                        stage,
                        cwd,
                        fingerprints.get(str(index)),
                        checkpoint_dir,
                        index,
                    )
                )
                if valid:
                    continue
                rerun_downstream = True
                if isinstance(artifacts, dict):
                    stage_paths_escape = (
                        stage.get("paths_escape", False)
                        if isinstance(stage, dict)
                        else False
                    )
                    for rule in artifacts.values():
                        remove_rule(
                            rule,
                            paths_escape=stage_paths_escape,
                        )
                try:
                    os.unlink(
                        os.path.join(checkpoint_dir, f"stage-{index}.json")
                    )
                except OSError:
                    pass

        fingerprint_matches = (
            not force_rerun
            and self._fingerprint_matches(
                conn, spec, j, current_fingerprint=current_fingerprint
            )
        )
        task_artifacts = spec.get("artifacts", {})
        if not isinstance(task_artifacts, dict):
            return
        task_paths_escape = spec.get("paths_escape", False)
        for rule in task_artifacts.values():
            path = resolved_path(rule)
            if path is None or path in stage_paths:
                continue
            valid = (
                fingerprint_matches
                and isinstance(rule, dict)
                and check_artifacts(
                    {"artifact": rule},
                    cwd,
                    paths_escape=task_paths_escape,
                ).get("artifact")
                is None
            )
            if not valid:
                remove_rule(
                    rule,
                    paths_escape=task_paths_escape,
                )

    # ---------- 工具 ----------

    def _get_task_spec(self, conn, j) -> str | None:
        row = conn.execute(
            "SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
            (j["batch_id"], j["task_id"], j["version"]),
        ).fetchone()
        return row["spec"] if row else None

    def _load_task_launch_binding(self, conn, job) -> tuple[dict, object]:
        """Load the exact persisted task row used for a launch decision."""
        row = conn.execute(
            "SELECT spec, project FROM tasks"
            " WHERE batch_id=? AND id=? AND version=?",
            (job["batch_id"], job["task_id"], job["version"]),
        ).fetchone()
        if row is None:
            raise ValueError("任务 spec 不存在")
        try:
            spec = json.loads(row["spec"])
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError("任务 spec 不是合法 JSON") from exc
        if not isinstance(spec, dict):
            raise ValueError("任务 spec 顶层必须是对象")
        return spec, row["project"]

    def _load_task_spec(self, conn, job) -> dict:
        raw = self._get_task_spec(conn, job)
        if raw is None:
            raise ValueError("任务 spec 不存在")
        try:
            spec = json.loads(raw)
        except (json.JSONDecodeError, TypeError) as exc:
            raise ValueError("任务 spec 不是合法 JSON") from exc
        if not isinstance(spec, dict):
            raise ValueError("任务 spec 顶层必须是对象")
        return spec

    def _job_log_path(self, j) -> str:
        # 审查 L1: 带 version —— resubmit 新版本不再覆盖旧 job 日志
        # (否则 log -f/probes/diag 读到串扰内容)。
        return os.path.join(
            self.host_dir, "logs", j["batch_id"], f"{j['task_id']}-v{j['version']}.log"
        )

    def _check_artifacts(self, artifacts: dict, cwd: str) -> bool:
        """Validate a task-level artifact mapping through the shared checker."""
        return check_declared_artifacts({"artifacts": artifacts}, cwd)

    def _heartbeat(self) -> None:
        self._touch_heartbeat()
