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
  status      TEXT NOT NULL DEFAULT 'queued',
  created_at  TEXT NOT NULL
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
"""


class StateError(Exception):
    pass


def hostname() -> str:
    """state 子目录名: daemon 所在计算节点名 (P6, 2026-08-15).

    优先读 config.json 的 node 字段 (daemon 常驻计算节点, 多机共享 home 时
    登录节点 CLI 也读同一 state.db); 无 config/无 node 字段 fallback 本机
    hostname. 背景: 登录节点 gethostname() = hpdc-gateway != ambiorix,
    登录节点 sched status 读到空目录 (旧坑: 只能 tmux 进计算节点查状态).
    """
    try:
        from .config import ConfigError, load_config

        cfg = load_config()
        node = cfg.get("node")
        if node:
            return str(node)
    except ConfigError:
        pass
    import socket

    return socket.gethostname()


def db_path() -> str:
    return os.path.join(default_state_dir(), hostname(), "state.db")


def init_db() -> str:
    """建目录 + 建表 + 迁移, 返回 db 路径. 幂等."""
    p = db_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with connect() as conn:
        conn.executescript(SCHEMA)
        migrate_gpu_jobs(conn)
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
) -> None:
    import json

    conn.execute(
        "INSERT INTO batches (id,name,mode,depends_on,gpus,cwd,env,status,created_at)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        (
            bid,
            name,
            mode,
            json.dumps(depends_on),
            json.dumps(gpus) if gpus is not None else None,
            cwd,
            json.dumps(env) if env else None,
            "queued",
            now(),
        ),
    )


def insert_task(
    conn: sqlite3.Connection,
    batch_id: str,
    task_id: str,
    version: int,
    spec: dict,
    order_idx: int,
) -> None:
    import json

    conn.execute(
        "INSERT INTO tasks (batch_id,id,version,spec,order_idx) VALUES (?,?,?,?,?)",
        (batch_id, task_id, version, json.dumps(spec), order_idx),
    )


def insert_job(
    conn: sqlite3.Connection,
    job_id: str,
    batch_id: str,
    task_id: str,
    version: int,
    fingerprint: str | None,
    stage_fingerprints: dict | None = None,
) -> None:
    import json

    conn.execute(
        "INSERT INTO jobs (id,batch_id,task_id,version,status,fingerprint,"
        "stage_fingerprints,submitted_at) VALUES (?,?,?,?,?,?,?,?)",
        (
            job_id,
            batch_id,
            task_id,
            version,
            "pending",
            fingerprint,
            json.dumps(stage_fingerprints) if stage_fingerprints else None,
            now(),
        ),
    )


def update_job(conn: sqlite3.Connection, job_id: str, **fields: Any) -> None:
    cols = ", ".join(f"{k}=?" for k in fields)
    conn.execute(f"UPDATE jobs SET {cols} WHERE id=?", (*fields.values(), job_id))


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
