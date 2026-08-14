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
  updated_at TEXT
);
"""


class StateError(Exception):
    pass


def hostname() -> str:
    import socket

    return socket.gethostname()


def db_path() -> str:
    return os.path.join(default_state_dir(), hostname(), "state.db")


def init_db() -> str:
    """建目录 + 建表, 返回 db 路径. 幂等."""
    p = db_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with connect() as conn:
        conn.executescript(SCHEMA)
    return p


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
