"""SQLite state store (文档 §3.4g, M1 照此建表).

单文件 {STATE}/<hostname>/state.db, WAL 模式, busy_timeout=5000ms
(CLI 写操作与 dispatcher 并发安全). 状态是唯一权威 (B9 第 1 层).
"""

from __future__ import annotations

import os
import hashlib
import sqlite3
import shutil
import stat
import tempfile
import secrets
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterator

from .config import default_state_dir

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
  id          TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  mode        TEXT NOT NULL DEFAULT 'mix',
  depends_on  TEXT NOT NULL DEFAULT '[]',
  gpus        TEXT,
  cwd         TEXT,
  env         TEXT,
  notify      TEXT,
  status      TEXT NOT NULL DEFAULT 'queued',
  created_at  TEXT NOT NULL,
  project     TEXT,
  priority    INTEGER NOT NULL DEFAULT 0,
  revision    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_batches_name_created
  ON batches(name, created_at DESC);

CREATE TABLE IF NOT EXISTS tasks (
  batch_id  TEXT NOT NULL REFERENCES batches(id),
  id        TEXT NOT NULL,
  version   INTEGER NOT NULL DEFAULT 1,
  spec      TEXT NOT NULL,
  order_idx INTEGER NOT NULL,
  PRIMARY KEY (batch_id, id, version)
);

CREATE TABLE IF NOT EXISTS jobs (
  id          TEXT PRIMARY KEY,
  batch_id    TEXT NOT NULL,
  task_id     TEXT NOT NULL,
  version     INTEGER NOT NULL,
  status      TEXT NOT NULL DEFAULT 'pending',
  gpu         INTEGER,
  pgid        INTEGER,
  kill_reason TEXT,
  rc          INTEGER,
  failure     TEXT,
  retries     INTEGER NOT NULL DEFAULT 0,
  fingerprint TEXT,
  stage_fingerprints TEXT,
  git_rev     TEXT,
  submitted_at TEXT, started_at TEXT, finished_at TEXT,
  UNIQUE (batch_id, task_id, version)
);

CREATE TABLE IF NOT EXISTS gpus (
  idx      INTEGER PRIMARY KEY,
  status   TEXT NOT NULL,
  job_id   TEXT,
  quarantined INTEGER NOT NULL DEFAULT 0,
  ignore_until TEXT,
  updated_at TEXT,
  mem_total_gib REAL,
  revision INTEGER NOT NULL DEFAULT 0
);

-- gpu_jobs 关联表 (co-location 多归属, §3.2e A2): gpu_id <-> job_id 多对一.
-- 独占模式 = 每卡至多 1 行; gpus.job_id 保留作"主 job 镜像" (兼容/回滚, 镜像语义
-- = 首个 assign 的 job). vram_gib = 装箱值 (max(声明, profile 实测), 单位 GiB).
CREATE TABLE IF NOT EXISTS gpu_jobs (
  gpu_id   INTEGER NOT NULL,
  job_id   TEXT PRIMARY KEY,
  vram_gib REAL,
  updated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_gpu_jobs_gpu ON gpu_jobs(gpu_id);

-- profile_cache 显存峰值库 (定案 39 待定项 3): profile_key 显式声明 (D1 不解析 cmd),
-- 命中取 max(声明, 实测) 保守装箱. 独立表不复用 jobs (任务实例 vs 累积数据生命周期不同).
-- 写入: daemon 注入 SCHED_PROFILE_OUT env -> 训练侧写 {"peak_gib": X} -> job rc=0
-- 后 daemon upsert 本表 + 删临时 (失败只删不 upsert). 列名 peak_gib 与 JSON key 一致 (GiB).
CREATE TABLE IF NOT EXISTS profile_cache (
  profile_key TEXT PRIMARY KEY,
  peak_gib    REAL NOT NULL,
  updated_at  TEXT,
  git_rev     TEXT
);

-- cancel 控制队列 (事故记录 4, 2026-08-17): CLI (登录节点) 看不到计算节点
-- 进程组 (PID namespace 跨节点, 定案 44 同类) -> 不本地 killpg, 改为写控制
-- 请求落库, daemon (计算节点) 每轮 tick 拉取处理: 本地 alive 预检 (O5) +
-- 写 kill_reason + killpg + reap 释放 GPU. pending 任务无进程, CLI 直标不需请求.
CREATE TABLE IF NOT EXISTS incidents (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  ts         TEXT NOT NULL,
  kind       TEXT NOT NULL,
  gpu_idx    INTEGER,
  job_id     TEXT,
  batch_id   TEXT,
  payload    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS control_requests (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id      TEXT NOT NULL,
  op          TEXT NOT NULL DEFAULT 'cancel',
  status      TEXT NOT NULL DEFAULT 'pending',  -- pending / done
  created_at  TEXT NOT NULL,
  processed_at TEXT,
  result      TEXT
);

CREATE TABLE IF NOT EXISTS operation_requests (
  request_id  TEXT PRIMARY KEY,
  argv        TEXT NOT NULL,
  status      TEXT NOT NULL,  -- started / done
  code        INTEGER,
  stdout      TEXT,
  stderr      TEXT,
  output_compacted INTEGER NOT NULL DEFAULT 0,
  created_at  TEXT NOT NULL,
  finished_at TEXT
);
"""


class StateError(Exception):
    pass
class SubmissionBlocked(StateError):
    pass

def ensure_private_directory(path: str) -> str:
    """Create/repair a scheduler directory as 0700 without accepting symlinks."""
    absolute = os.path.normpath(os.path.abspath(path))
    os.makedirs(absolute, mode=0o700, exist_ok=True)
    state_root = os.path.normpath(os.path.abspath(default_state_dir()))
    try:
        inside_state = os.path.commonpath((state_root, absolute)) == state_root
    except ValueError:
        inside_state = False
    targets = [absolute]
    if inside_state:
        relative = os.path.relpath(absolute, state_root)
        targets = [state_root]
        if relative != ".":
            current = state_root
            for component in relative.split(os.sep):
                current = os.path.join(current, component)
                targets.append(current)
    for target in targets:
        entry = os.lstat(target)
        if stat.S_ISLNK(entry.st_mode) or not stat.S_ISDIR(entry.st_mode):
            raise StateError(f"state directory is not a real directory: {target}")
        os.chmod(target, 0o700)
    return absolute


def ensure_private_file(path: str) -> str:
    """Create/repair a regular scheduler file as 0600 without following links."""
    ensure_private_directory(os.path.dirname(path) or ".")
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    fd = os.open(path, flags, 0o600)
    try:
        entry = os.fstat(fd)
        if not stat.S_ISREG(entry.st_mode):
            raise StateError(f"state file is not regular: {path}")
        os.fchmod(fd, 0o600)
    finally:
        os.close(fd)
    return path


def open_private_text(
    path: str,
    mode: str,
    *,
    encoding: str = "utf-8",
):
    """Open a scheduler text file with O_NOFOLLOW and an exact 0600 mode."""
    modes = {
        "a": os.O_WRONLY | os.O_CREAT | os.O_APPEND,
        "a+": os.O_RDWR | os.O_CREAT | os.O_APPEND,
        "w": os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        "x": os.O_WRONLY | os.O_CREAT | os.O_EXCL,
    }
    if mode not in modes:
        raise ValueError(f"unsupported private file mode: {mode}")
    ensure_private_directory(os.path.dirname(path) or ".")
    flags = (
        modes[mode]
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    fd = os.open(path, flags, 0o600)
    try:
        entry = os.fstat(fd)
        if not stat.S_ISREG(entry.st_mode):
            raise StateError(f"state file is not regular: {path}")
        os.fchmod(fd, 0o600)
        return os.fdopen(fd, mode, encoding=encoding)
    except Exception:
        os.close(fd)
        raise


def touch_private_file(path: str) -> None:
    ensure_private_file(path)
    fd = os.open(
        path,
        os.O_WRONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.utime(fd)
    finally:
        os.close(fd)

_read_only = False


class _CommitNeutralConnection:
    """Delegate SQLite work while reserving transaction control to the caller."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.raw = connection

    def __getattr__(self, name: str) -> Any:
        return getattr(self.raw, name)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


_bound_connection: ContextVar[_CommitNeutralConnection | None] = ContextVar(
    "sched_bound_connection",
    default=None,
)
_bound_after_commit: ContextVar[list[Callable[[], None]] | None] = ContextVar(
    "sched_bound_after_commit",
    default=None,
)
_submission_lock_depth: ContextVar[int] = ContextVar(
    "sched_submission_lock_depth",
    default=0,
)


@contextmanager
def bind_connection(
    conn: sqlite3.Connection,
) -> Iterator[list[Callable[[], None]]]:
    """Reuse one writer transaction and collect effects for its outer commit."""
    existing = _bound_connection.get()
    if existing is not None and existing.raw is not conn:
        raise StateError("cannot replace an active bound state transaction")
    callbacks = _bound_after_commit.get()
    owns_callbacks = callbacks is None
    if callbacks is None:
        callbacks = []
    bound = existing or _CommitNeutralConnection(conn)
    connection_token = _bound_connection.set(bound)
    callbacks_token = _bound_after_commit.set(callbacks)
    try:
        yield callbacks
    except Exception:
        if owns_callbacks:
            callbacks.clear()
        raise
    finally:
        _bound_after_commit.reset(callbacks_token)
        _bound_connection.reset(connection_token)


def defer_after_commit(callback: Callable[[], None]) -> bool:
    """Queue an external effect when a durable request owns the transaction."""
    callbacks = _bound_after_commit.get()
    if callbacks is None:
        return False
    callbacks.append(callback)
    return True



def set_read_only(enabled: bool) -> None:
    """Select read-only SQLite connections for the current CLI invocation."""
    global _read_only
    _read_only = bool(enabled)


def read_only() -> bool:
    return _read_only


def hostname() -> str:
    """state 子目录名: daemon 所在计算节点名 (P6, 2026-08-15).

    优先读 config.json 的 node 字段 (daemon 常驻计算节点, 多机共享 home 时
    登录节点 CLI 也读同一 state.db); 无 config/无 node 字段 fallback 本机
    hostname. 背景: 登录节点 gethostname() = hpdc-gateway != ambiorix,
    登录节点 sched status 读到空目录 (旧坑: 只能 tmux 进计算节点查状态).

    M16: 回退仅限 config **不存在** (未 init 的环境); 存在但解析失败必须
    报错 —— 静默回退会让登录节点在 config 损坏时读到本机空目录, 正是本
    函数要根治的旧坑的复活路径.

    P3: 进程内缓存 (path, mtime) —— 原实现每次 connect() 都完整 load_config
    (读+解析+校验), daemon 每 tick 开多个连接导致 config.json 每秒被解析
    近一遍。mtime 变化自动失效, CLI 短进程与 daemon 长驻均安全。
    """
    from .config import ConfigError, config_path, load_config

    if _pinned_host:
        return _pinned_host["v"]
    p = config_path()
    if not os.path.isfile(p):
        import socket

        return socket.gethostname()
    try:
        key = (p, os.path.getmtime(p))
    except OSError:
        key = (p, None)
    if key in _hostname_cache:
        return _hostname_cache[key]
    try:
        cfg = load_config()
    except ConfigError:
        # B12-a: 热更新窗口的半写/坏文件。有最近已知好值 -> 沿用 (daemon 与
        # 长驻 CLI 进程不断链); 无历史 -> 保持 M16 语义上抛 (登录节点首查,
        # 静默回退 gethostname 会读到本机空目录 —— 旧坑复活路径, 绝不放开)
        last = _hostname_last_good.get("v")
        if last is not None:
            return str(last)
        raise
    node = cfg.get("node")
    if node:
        result = str(node)
    else:
        import socket

        result = socket.gethostname()
    _hostname_cache.clear()
    _hostname_cache[key] = result
    _hostname_last_good["v"] = result   # B12-a: 坏配置窗口的兜底值
    return result


def host_dir() -> str:
    """Return the configured host directory without permitting root escape."""
    root = os.path.normpath(os.path.abspath(default_state_dir()))
    host = hostname()
    if (
        not host
        or host in (".", "..")
        or "/" in host
        or "\\" in host
        or "\x00" in host
    ):
        raise StateError("node must be a safe single path component")
    candidate = os.path.join(root, host)
    root_real = os.path.realpath(root)
    try:
        contained = os.path.commonpath(
            (root_real, os.path.realpath(candidate))
        ) == root_real
    except ValueError:
        contained = False
    if not contained:
        raise StateError(f"node state path escapes state_dir: {host}")
    return candidate


def db_path() -> str:
    return os.path.join(host_dir(), "state.db")


def submission_inbox_dir() -> str:
    return os.path.join(host_dir(), "submit_inbox")


def launch_marker_path(job_id: str) -> str:
    prefix = hashlib.sha256(str(job_id).encode("utf-8")).hexdigest()[:24]
    return os.path.join(host_dir(), "launch", f"{prefix}.launch")


def _launch_process_start(pgid: int) -> str | None:
    from .executor import process_start_token

    return process_start_token(pgid)


def launch_marker_active(job_id: str) -> bool:
    """Conservatively report whether a strong launch identity may be active."""
    path = launch_marker_path(job_id)
    import socket

    if socket.gethostname().strip() != hostname().strip():
        try:
            os.lstat(path)
            return True
        except FileNotFoundError:
            return False
        except OSError:
            return True

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        return False
    except OSError:
        return True
    try:
        marker_stat = os.fstat(fd)
        if (
            not stat.S_ISREG(marker_stat.st_mode)
            or marker_stat.st_uid != os.getuid()
            or marker_stat.st_nlink != 1
            or marker_stat.st_size > 4096
        ):
            return True
        payload = os.read(fd, 4097)
        if len(payload) > 4096:
            return True
        try:
            fields = payload.decode("utf-8").split()
        except UnicodeDecodeError:
            return True
    except OSError:
        return True
    finally:
        os.close(fd)

    if len(fields) != 2:
        return True
    try:
        pgid = int(fields[0])
    except (TypeError, ValueError):
        return True
    from .executor import _is_strong_start_token

    marker_start = fields[1]
    if (
        pgid <= 0
        or pgid > 2**31 - 1
        or not _is_strong_start_token(marker_start)
    ):
        return True
    process_start = _launch_process_start(pgid)
    if process_start is None:
        return True
    if process_start != marker_start:
        try:
            os.unlink(path)
        except OSError:
            pass
        return False
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        try:
            os.unlink(path)
        except OSError:
            pass
        return False
    except (PermissionError, OSError):
        return True


def submission_shutdown_marker() -> str:
    return os.path.join(submission_inbox_dir(), ".daemon-stopping")


@contextmanager
def submission_lock() -> Iterator[None]:
    """Serialize submissions with one re-entrant process-local lock order."""
    depth = _submission_lock_depth.get()
    if depth:
        token = _submission_lock_depth.set(depth + 1)
        try:
            yield
        finally:
            _submission_lock_depth.reset(token)
        return

    import fcntl

    inbox_dir = submission_inbox_dir()
    ensure_private_directory(inbox_dir)
    lock_path = os.path.join(inbox_dir, ".submit.lock")
    with open_private_text(lock_path, "a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        token = _submission_lock_depth.set(1)
        try:
            yield
        finally:
            _submission_lock_depth.reset(token)
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

def submission_shutdown_active() -> bool:
    """True only while a daemon with a fresh heartbeat is stopping."""
    if not idle_shutdown_pending():
        return False
    import time

    heartbeat = os.path.join(default_state_dir(), hostname(), "daemon.heartbeat")
    try:
        fresh = time.time() - os.path.getmtime(heartbeat) <= 60
    except FileNotFoundError:
        fresh = False
    except OSError:
        return True
    if not fresh:
        clear_idle_shutdown()
    return fresh


@contextmanager
def submission_connect() -> Iterator[sqlite3.Connection]:
    """Open a submission transaction while holding the idle-shutdown lease."""
    with submission_lock():
        if submission_shutdown_active():
            raise SubmissionBlocked("daemon 正在退出, 请稍后重试")
        with connect() as conn:
            yield conn


def idle_shutdown_pending() -> bool:
    return os.path.exists(submission_shutdown_marker())


def mark_idle_shutdown() -> str:
    """Atomically publish a caller-owned shutdown marker and return its token."""
    with submission_lock():
        inbox_dir = submission_inbox_dir()
        ensure_private_directory(inbox_dir)
        marker = submission_shutdown_marker()
        token = secrets.token_hex(24)
        tmp = f"{marker}.{token}.tmp"
        with open_private_text(tmp, "x") as f:
            f.write(f"{token}\n{os.getpid()}\n")
        os.replace(tmp, marker)
        return token


def clear_idle_shutdown(token: str | None = None) -> bool:
    """Clear the marker, optionally only when it is still owned by *token*."""
    with submission_lock():
        marker = submission_shutdown_marker()
        if token is not None:
            try:
                with open(marker, encoding="utf-8") as stream:
                    current_token = stream.readline().strip()
            except OSError:
                return False
            if not secrets.compare_digest(current_token, token):
                return False
        try:
            os.unlink(marker)
        except OSError:
            return False
        return True


def _execute_sql_statements(conn: sqlite3.Connection, script: str) -> None:
    """Execute a migration script statement-by-statement without implicit commits."""
    pending: list[str] = []
    for line in script.splitlines(keepends=True):
        pending.append(line)
        statement = "".join(pending)
        if sqlite3.complete_statement(statement):
            conn.execute(statement)
            pending.clear()
    if any(part.strip() for part in pending):
        raise StateError("incomplete SQL migration statement")


def init_db() -> str:
    """建目录 + 建表 + 迁移, 返回 db 路径. 幂等."""
    if _read_only:
        raise StateError("read-only state mode cannot initialize or migrate the database")
    p = db_path()
    if _bound_connection.get() is not None:
        return p
    ensure_private_directory(os.path.dirname(p))
    with connect() as conn:
        conn.executescript("BEGIN IMMEDIATE;\n" + SCHEMA)
        migrate_gpu_jobs(conn)
        migrate_project_columns(conn)
        migrate_incidents(conn)
        migrate_job_progress(conn)
        migrate_operation_requests(conn)
        migrate_revisions(conn)
        migrate_legacy_job_statuses(conn)
    return p


def migrate_legacy_job_statuses(conn: sqlite3.Connection) -> None:
    """Normalize pre-pending waiting states in the initialization transaction."""
    conn.execute(
        "UPDATE jobs SET status='pending'"
        " WHERE status IN ('waiting_quota','waiting_dep')"
    )


def migrate_gpu_jobs(conn: sqlite3.Connection) -> None:
    """迁移 (§3.2e E): 现有 gpus.job_id 非空行 -> INSERT gpu_jobs (每卡 1 行).

    独占模式 = 每卡 1 行 = 现状语义; 远程有运行中任务 (assigned) 只迁移状态
    不动进程. 幂等 (INSERT OR IGNORE 按 job_id 主键). 迁移后独占行为必须零变化.
    """
    conn.execute(
        "INSERT OR IGNORE INTO gpu_jobs (gpu_id, job_id, updated_at) "
        "SELECT idx, job_id, updated_at FROM gpus WHERE job_id IS NOT NULL"
    )
    # 容量列迁移 (定案 39 待定项 4 容量来源): 旧库无 mem_total_gib 列 -> ALTER 补齐
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(gpus)").fetchall()]
    if "mem_total_gib" not in cols:
        conn.execute("ALTER TABLE gpus ADD COLUMN mem_total_gib REAL")
    # 批次级通知覆盖列 (设计 §3): 旧库无 notify 列 -> ALTER 补齐
    bcols = [r["name"] for r in conn.execute("PRAGMA table_info(batches)").fetchall()]
    if "notify" not in bcols:
        conn.execute("ALTER TABLE batches ADD COLUMN notify TEXT")



def migrate_project_columns(conn: sqlite3.Connection) -> None:
    """迁移: 给 tasks/batches/jobs 表添加 project 列.

    幂等: 列已存在则忽略. 旧数据 project = NULL (无项目关联, 兼容旧数据).
    """
    for table, col in [("tasks", "project"), ("batches", "project"), ("jobs", "project")]:
        cols = [r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if col not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} Text")
    # B11c: batches.priority 列迁移 (批次级优先级, 默认 0)
    bcols = [r["name"] for r in conn.execute("PRAGMA table_info(batches)").fetchall()]
    if "priority" not in bcols:
        conn.execute("ALTER TABLE batches ADD COLUMN priority INTEGER NOT NULL DEFAULT 0")

def migrate_job_progress(conn: sqlite3.Connection) -> None:
    """B13-§5: jobs.progress 列 (progress_regex 周期解析的最新进度串). 幂等."""
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
    if "progress" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN progress TEXT")


def migrate_operation_requests(conn: sqlite3.Connection) -> None:
    """Add durable-output tombstone metadata to databases created pre-ledger."""
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(operation_requests)").fetchall()
    }
    if "output_compacted" not in columns:
        conn.execute(
            "ALTER TABLE operation_requests"
            " ADD COLUMN output_compacted INTEGER NOT NULL DEFAULT 0"
        )


def migrate_revisions(conn: sqlite3.Connection) -> None:
    """Install monotonic ABA revisions after upgrading legacy table columns."""
    for table in ("batches", "gpus"):
        columns = {
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if "revision" not in columns:
            conn.execute(
                f"ALTER TABLE {table}"
                " ADD COLUMN revision INTEGER NOT NULL DEFAULT 0"
            )
    _execute_sql_statements(
        conn,
        """
        CREATE TRIGGER IF NOT EXISTS revision_batch_status
        AFTER UPDATE OF status ON batches
        WHEN OLD.status IS NOT NEW.status
        BEGIN
          UPDATE batches SET revision=revision+1 WHERE id=NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS revision_task_insert
        AFTER INSERT ON tasks
        BEGIN
          UPDATE batches SET revision=revision+1 WHERE id=NEW.batch_id;
        END;
        CREATE TRIGGER IF NOT EXISTS revision_task_delete
        AFTER DELETE ON tasks
        BEGIN
          UPDATE batches SET revision=revision+1 WHERE id=OLD.batch_id;
        END;
        CREATE TRIGGER IF NOT EXISTS revision_task_membership
        AFTER UPDATE OF batch_id, id, version ON tasks
        WHEN OLD.batch_id IS NOT NEW.batch_id
          OR OLD.id IS NOT NEW.id
          OR OLD.version IS NOT NEW.version
        BEGIN
          UPDATE batches SET revision=revision+1 WHERE id=OLD.batch_id;
          UPDATE batches SET revision=revision+1
            WHERE id=NEW.batch_id AND NEW.batch_id IS NOT OLD.batch_id;
        END;

        CREATE TRIGGER IF NOT EXISTS revision_job_insert
        AFTER INSERT ON jobs
        BEGIN
          UPDATE batches SET revision=revision+1 WHERE id=NEW.batch_id;
        END;
        CREATE TRIGGER IF NOT EXISTS revision_job_delete
        AFTER DELETE ON jobs
        BEGIN
          UPDATE batches SET revision=revision+1 WHERE id=OLD.batch_id;
        END;
        CREATE TRIGGER IF NOT EXISTS revision_job_state
        AFTER UPDATE OF status, batch_id, task_id, version ON jobs
        WHEN OLD.status IS NOT NEW.status
          OR OLD.batch_id IS NOT NEW.batch_id
          OR OLD.task_id IS NOT NEW.task_id
          OR OLD.version IS NOT NEW.version
        BEGIN
          UPDATE batches SET revision=revision+1 WHERE id=OLD.batch_id;
          UPDATE batches SET revision=revision+1
            WHERE id=NEW.batch_id AND NEW.batch_id IS NOT OLD.batch_id;
        END;

        CREATE TRIGGER IF NOT EXISTS revision_gpu_state
        AFTER UPDATE OF status, job_id, quarantined ON gpus
        WHEN OLD.status IS NOT NEW.status
          OR OLD.job_id IS NOT NEW.job_id
          OR OLD.quarantined IS NOT NEW.quarantined
        BEGIN
          UPDATE gpus SET revision=revision+1 WHERE idx=NEW.idx;
        END;
        CREATE TRIGGER IF NOT EXISTS revision_gpu_ignore
        AFTER UPDATE OF ignore_until ON gpus
        WHEN OLD.ignore_until IS NOT NEW.ignore_until
        BEGIN
          UPDATE gpus SET revision=revision+1 WHERE idx=NEW.idx;
        END;
        CREATE TRIGGER IF NOT EXISTS revision_gpu_job_insert
        AFTER INSERT ON gpu_jobs
        BEGIN
          UPDATE gpus SET revision=revision+1 WHERE idx=NEW.gpu_id;
        END;
        CREATE TRIGGER IF NOT EXISTS revision_gpu_job_delete
        AFTER DELETE ON gpu_jobs
        BEGIN
          UPDATE gpus SET revision=revision+1 WHERE idx=OLD.gpu_id;
        END;
        CREATE TRIGGER IF NOT EXISTS revision_gpu_job_update
        AFTER UPDATE OF gpu_id, job_id, vram_gib ON gpu_jobs
        WHEN OLD.gpu_id IS NOT NEW.gpu_id
          OR OLD.job_id IS NOT NEW.job_id
          OR OLD.vram_gib IS NOT NEW.vram_gib
        BEGIN
          UPDATE gpus SET revision=revision+1 WHERE idx=OLD.gpu_id;
          UPDATE gpus SET revision=revision+1
            WHERE idx=NEW.gpu_id AND NEW.gpu_id IS NOT OLD.gpu_id;
        END;
        """
    )


def compact_operation_outputs(
    conn: sqlite3.Connection,
    *,
    keep_recent: int = 1000,
    ttl_days: int = 7,
) -> int:
    """Replace old completed output with tombstones while retaining bindings."""
    cutoff = (datetime.now() - timedelta(days=max(0, ttl_days))).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    cursor = conn.execute(
        "UPDATE operation_requests"
        " SET stdout=NULL, stderr=NULL, output_compacted=1"
        " WHERE status='done' AND output_compacted=0"
        " AND (finished_at<? OR request_id IN ("
        "   SELECT request_id FROM operation_requests"
        "   WHERE status='done' ORDER BY finished_at DESC, rowid DESC"
        "   LIMIT -1 OFFSET ?"
        " ))",
        (cutoff, max(0, keep_recent)),
    )
    return cursor.rowcount


def migrate_incidents(conn: sqlite3.Connection) -> None:
    """OOM 事故快照表 (调研 F2): 幂等补建 (init_db 与旧库升级共用)."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS incidents ("
        " id         INTEGER PRIMARY KEY AUTOINCREMENT,"
        " ts         TEXT NOT NULL,"
        " kind       TEXT NOT NULL,"
        " gpu_idx    INTEGER,"
        " job_id     TEXT,"
        " batch_id   TEXT,"
        " payload    TEXT NOT NULL)"
    )


def insert_incident(conn: sqlite3.Connection, ts: str, kind: str,
                    gpu_idx: int | None, job_id: str | None,
                    batch_id: str | None, payload_json: str) -> int:
    cur = conn.execute(
        "INSERT INTO incidents (ts, kind, gpu_idx, job_id, batch_id, payload)"
        " VALUES (?,?,?,?,?,?)",
        (ts, kind, gpu_idx, job_id, batch_id, payload_json),
    )
    return int(cur.lastrowid)


def prune_incidents(conn: sqlite3.Connection, max_rows: int = 200,
                    ttl_days: int = 30) -> int:
    """裁剪事故快照 (定案 Q3): 条数 + TTL 双限, blocked 引用的条目豁免.

    返回删除行数. 豁免规则: job 当前仍处 blocked 状态的事故不裁 ——
    保证事后 sched diag 永远能拿到证据 (直到任务被 retry/resubmit 解锁).
    """
    cutoff = (
        datetime.now()
        .timestamp()
        - ttl_days * 86400
    )
    cutoff_str = datetime.fromtimestamp(cutoff).strftime("%Y-%m-%d %H:%M:%S")
    # TTL 与条数裁剪使用同一 blocked 豁免: 未完成诊断所需的事故证据不删。
    blocked_ids = {
        r["id"]
        for r in conn.execute(
            "SELECT DISTINCT id FROM jobs WHERE status='blocked'"
        ).fetchall()
    }
    if blocked_ids:
        placeholders = ",".join("?" * len(blocked_ids))
        conn.execute(
            f"DELETE FROM incidents WHERE ts < ?"
            f" AND (job_id IS NULL OR job_id NOT IN ({placeholders}))",
            (cutoff_str, *blocked_ids),
        )
    else:
        conn.execute("DELETE FROM incidents WHERE ts < ?", (cutoff_str,))
    keep_min = conn.execute(
        "SELECT MIN(id) FROM (SELECT id FROM incidents ORDER BY id DESC LIMIT ?)",
        (max_rows,),
    ).fetchone()[0]
    if keep_min is None:
        return 0
    # 注意 SQL NULL 三值逻辑: job_id 为 NULL 的行用 `IS NULL` 单独放行,
    # 否则 `NULL NOT IN (...)` 求值为 NULL -> 裁剪静默空转
    if blocked_ids:
        placeholders = ",".join("?" * len(blocked_ids))
        cur = conn.execute(
            f"DELETE FROM incidents WHERE id < ?"
            f" AND (job_id IS NULL OR job_id NOT IN ({placeholders}))",
            (keep_min, *blocked_ids),
        )
    else:
        cur = conn.execute("DELETE FROM incidents WHERE id < ?", (keep_min,))
    return cur.rowcount


def latest_incident_for_job(conn: sqlite3.Connection, job_id: str):
    return conn.execute(
        "SELECT * FROM incidents WHERE job_id=? ORDER BY id DESC LIMIT 1",
        (job_id,),
    ).fetchone()


def _snapshot_signature(path: str) -> tuple:
    """Stat the database and WAL without opening SQLite shared-memory state."""
    signature = []
    for candidate in (path, path + "-wal"):
        try:
            stat = os.stat(candidate)
        except FileNotFoundError:
            signature.append(None)
        else:
            signature.append(
                (
                    stat.st_dev,
                    stat.st_ino,
                    stat.st_size,
                    stat.st_mtime_ns,
                    stat.st_ctime_ns,
                )
            )
    return tuple(signature)


@contextmanager
def _read_only_database(path: str) -> Iterator[tuple[str, bool]]:
    """Yield a stable private copy without opening SQLite state on the source."""
    snapshot_dir = tempfile.mkdtemp(prefix="sched-state-ro-")
    snapshot_db = os.path.join(snapshot_dir, os.path.basename(path))
    snapshot_wal = snapshot_db + "-wal"
    try:
        for _ in range(4):
            before = _snapshot_signature(path)
            if before[0] is None:
                raise StateError(f"state database does not exist: {path}")
            try:
                shutil.copyfile(path, snapshot_db)
            except FileNotFoundError:
                continue
            if before[1] is None:
                try:
                    os.unlink(snapshot_wal)
                except FileNotFoundError:
                    pass
            else:
                try:
                    shutil.copyfile(path + "-wal", snapshot_wal)
                except FileNotFoundError:
                    continue
            if before == _snapshot_signature(path):
                yield snapshot_db, before[1] is None
                return
        raise StateError("state database changed while creating read-only snapshot")
    finally:
        shutil.rmtree(snapshot_dir, ignore_errors=True)


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    """Open the state database in invocation-selected read or writer mode."""
    bound = _bound_connection.get()
    if bound is not None:
        yield bound
        return
    p = db_path()
    if _read_only:
        with _read_only_database(p) as (read_path, immutable):
            uri = Path(read_path).absolute().as_uri() + "?mode=ro"
            if immutable:
                uri += "&immutable=1"
            conn = sqlite3.connect(uri, timeout=5.0, uri=True)
            conn.row_factory = sqlite3.Row
            try:
                yield conn
            finally:
                conn.close()
        return

    ensure_private_directory(os.path.dirname(p))
    ensure_private_file(p)
    conn = sqlite3.connect(p, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    for sidecar in (p + "-wal", p + "-shm"):
        if os.path.exists(sidecar):
            ensure_private_file(sidecar)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        for sidecar in (p + "-wal", p + "-shm"):
            if os.path.exists(sidecar):
                ensure_private_file(sidecar)


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


_hostname_cache: dict = {}  # P3: hostname() 进程内缓存 {(path, mtime): node}
_hostname_last_good: dict = {}  # B12-a: 最近一次成功解析的 node (ConfigError 兜底)
_pinned_host: dict = {}         # B12-a: daemon 启动时钉住 (文件后续变更不再影响本进程)


def pin_hostname(name: str) -> None:
    """daemon 启动期钉住 host 目录名 (B12-a 冷键语义的执行面).

    node 变更属冷键 —— 热更新会拒绝; 但若不钉住, state.connect() 每次仍从
    文件实时解析, 文件一改 DB 路径立即漂移到空目录 (与 daemon 的拒绝与否
    无关)。钉住后 daemon 进程终身使用启动时的目录; CLI 短进程不受影响,
    保持动态解析 (登录节点读远端目录的既有语义).
    """
    _pinned_host["v"] = str(name)


def release_gpu(conn: sqlite3.Connection, job_id: str) -> None:
    """事务内多归属计数释放 (§3.2e B). 调用方持有事务 (WAL 串行).

    DELETE gpu_jobs 行 -> 卡还有 co-tenant? 有 = 保持 assigned (不误杀);
    无 = 转 releasing (最后任务结束, 等进程离场). 幂等: 重复调用无害
    (gpu_jobs 无行 -> 回退镜像反查, 找不到也无操作)。
    """
    gpu = conn.execute(
        "SELECT gpu_id FROM gpu_jobs WHERE job_id=?", (job_id,)
    ).fetchone()
    conn.execute("DELETE FROM gpu_jobs WHERE job_id=?", (job_id,))
    if gpu is None:
        # 无 gpu_jobs 行 (历史/异常): 回退旧逻辑 (镜像列反查)
        conn.execute(
            "UPDATE gpus SET status='releasing', job_id=NULL, updated_at=? "
            "WHERE job_id=?",
            (now(), job_id),
        )
        return
    idx = gpu["gpu_id"]
    remain = conn.execute(
        "SELECT COUNT(*) AS n FROM gpu_jobs WHERE gpu_id=?", (idx,)
    ).fetchone()["n"]
    if remain > 0:
        # 还有 co-tenant: 保持 assigned, 镜像改指剩余任一 job
        other = conn.execute(
            "SELECT job_id FROM gpu_jobs WHERE gpu_id=? LIMIT 1", (idx,)
        ).fetchone()
        conn.execute(
            "UPDATE gpus SET job_id=?, updated_at=? WHERE idx=?",
            (other["job_id"], now(), idx),
        )
    else:
        conn.execute(
            "UPDATE gpus SET status='releasing', job_id=NULL, updated_at=? "
            "WHERE idx=?",
            (now(), idx),
        )


# ---------- 批次 ----------

def insert_batch(
    conn: sqlite3.Connection,
    bid: str,
    name: str,
    mode: str,
    depends_on: list[str],
    gpus: list[int] | None,
    cwd: str | None,
    env: dict | None,
    notify: Any = None,
    project: str | None = None,
    priority: int = 0,
) -> None:
    import json

    conn.execute(
        "INSERT INTO batches (id,name,mode,depends_on,gpus,cwd,env,notify,status,created_at,project,priority)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            bid,
            name,
            mode,
            json.dumps(depends_on),
            json.dumps(gpus) if gpus is not None else None,
            cwd,
            json.dumps(env) if env else None,
            json.dumps(notify) if notify is not None else None,
            "queued",
            now(),
            project,
            priority,
        ),
    )


def insert_task(
    conn: sqlite3.Connection,
    batch_id: str,
    task_id: str,
    version: int,
    spec: dict,
    order_idx: int,
    project: str | None = None,
) -> None:
    import json

    conn.execute(
        "INSERT INTO tasks (batch_id,id,version,spec,order_idx,project) VALUES (?,?,?,?,?,?)",
        (batch_id, task_id, version, json.dumps(spec), order_idx, project),
    )


def insert_job(
    conn: sqlite3.Connection,
    job_id: str,
    batch_id: str,
    task_id: str,
    version: int,
    fingerprint: str | None,
    stage_fingerprints: dict | None = None,
    project: str | None = None,
) -> None:
    import json

    conn.execute(
        "INSERT INTO jobs (id,batch_id,task_id,version,status,fingerprint,"
        "stage_fingerprints,submitted_at,project) VALUES (?,?,?,?,?,?,?,?,?)",
        (
            job_id,
            batch_id,
            task_id,
            version,
            "pending",
            fingerprint,
            json.dumps(stage_fingerprints) if stage_fingerprints else None,
            now(),
            project,
        ),
    )


def update_job(conn: sqlite3.Connection, job_id: str, **fields: Any) -> None:
    cols = ", ".join(f"{k}=?" for k in fields)
    conn.execute(f"UPDATE jobs SET {cols} WHERE id=?", (*fields.values(), job_id))


# ---------- 控制请求队列 (事故记录 4: cancel 转发 daemon) ----------

def insert_control_request(
    conn: sqlite3.Connection, job_id: str, op: str = "cancel",
) -> int:
    """CLI 写入控制请求 (cancel 转发 daemon 执行 kill, 事故记录 4).

    返回请求 id. daemon 每轮 tick 拉取 pending 请求, 在计算节点本地完成
    alive 预检 + 写 kill_reason + killpg + reap 释放 GPU——登录节点看不到
    计算节点进程组 (PID namespace, 定案 44 同类), CLI 绝不本地 killpg.
    """
    cur = conn.execute(
        "INSERT INTO control_requests (job_id, op, status, created_at)"
        " VALUES (?,?,?,?)",
        (job_id, op, "pending", now()),
    )
    return int(cur.lastrowid)


def pending_control_requests(conn: sqlite3.Connection):
    """拉取所有 pending 控制请求 (daemon tick 用)."""
    return conn.execute(
        "SELECT * FROM control_requests WHERE status='pending' ORDER BY id"
    ).fetchall()


def finish_control_request(
    conn: sqlite3.Connection, req_id: int, result: str,
) -> None:
    conn.execute(
        "UPDATE control_requests SET status='done', processed_at=?, result=?"
        " WHERE id=?",
        (now(), result, req_id),
    )


def get_job(conn: sqlite3.Connection, job_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()


def get_batch(conn: sqlite3.Connection, bid: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM batches WHERE id=?", (bid,)).fetchone()


def get_gpu(conn: sqlite3.Connection, idx: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM gpus WHERE idx=?", (idx,)).fetchone()


def init_gpus(conn: sqlite3.Connection, gpu_list: list[int]) -> None:
    """把配置集 GPU 行补齐为 free (幂等, 已有行不动)."""
    for idx in gpu_list:
        conn.execute(
            "INSERT OR IGNORE INTO gpus (idx,status,quarantined,updated_at)"
            " VALUES (?,?,0,?)",
            (idx, "free", now()),
        )


def all_jobs(conn: sqlite3.Connection, batch_id: str | None = None) -> list[sqlite3.Row]:
    if batch_id:
        return list(
            conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? ORDER BY rowid", (batch_id,)
            ).fetchall()
        )
    return list(conn.execute("SELECT * FROM jobs ORDER BY rowid").fetchall())
