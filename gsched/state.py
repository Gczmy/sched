"""SQLite state store (文档 §3.4g, M1 照此建表).

单文件 {STATE}/<hostname>/state.db, WAL 模式, busy_timeout=5000ms
(CLI 写操作与 dispatcher 并发安全). 状态是唯一权威 (B9 第 1 层).
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

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
  priority    INTEGER NOT NULL DEFAULT 0
);

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
  mem_total_gib REAL
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
"""


class StateError(Exception):
    pass


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
    from .config import config_path, load_config

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
    cfg = load_config()  # 存在但损坏: ConfigError 上抛, 不静默回退
    node = cfg.get("node")
    if node:
        result = str(node)
    else:
        import socket

        result = socket.gethostname()
    _hostname_cache.clear()
    _hostname_cache[key] = result
    return result


def db_path() -> str:
    return os.path.join(default_state_dir(), hostname(), "state.db")


def init_db() -> str:
    """建目录 + 建表 + 迁移, 返回 db 路径. 幂等."""
    p = db_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with connect() as conn:
        conn.executescript(SCHEMA)
        migrate_gpu_jobs(conn)
        migrate_project_columns(conn)
        migrate_incidents(conn)
    return p


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


@contextmanager

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
    conn.execute(
        "DELETE FROM incidents WHERE ts < ?", (cutoff_str,)
    )
    # 条数裁剪: 保留最新 max_rows 条, 但跳过 blocked 引用的行
    blocked_ids = {
        r["id"]
        for r in conn.execute(
            "SELECT DISTINCT id FROM jobs WHERE status='blocked'"
        ).fetchall()
    }
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


@contextmanager
def connect() -> Iterator[sqlite3.Connection]:
    """WAL + busy_timeout 连接. 事务由调用方 with 管理 (自动 commit/rollback)."""
    p = db_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    conn = sqlite3.connect(p, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


_hostname_cache: dict = {}  # P3: hostname() 进程内缓存 {(path, mtime): node}


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
