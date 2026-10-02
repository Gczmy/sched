"""Durable no-progress observations and opt-in notification outbox."""
from __future__ import annotations

import json
import time

from . import recovery, state

SCHEMA = """
CREATE TABLE IF NOT EXISTS recovery_watch (
 root_job_id TEXT PRIMARY KEY REFERENCES jobs(id),
 checkpoint_sha256 TEXT NOT NULL,
 last_progress_at REAL NOT NULL,
 last_notice_at REAL
);
CREATE TABLE IF NOT EXISTS recovery_notices (
 id TEXT PRIMARY KEY,
 root_job_id TEXT NOT NULL REFERENCES recovery_watch(root_job_id),
 event TEXT NOT NULL,
 delivered_at REAL
);
"""


def progress(conn, root, checkpoint_sha, timestamp):
    conn.execute("INSERT INTO recovery_watch(root_job_id,checkpoint_sha256,last_progress_at) VALUES(?,?,?) ON CONFLICT(root_job_id) DO UPDATE SET checkpoint_sha256=excluded.checkpoint_sha256,last_progress_at=excluded.last_progress_at,last_notice_at=NULL WHERE recovery_watch.checkpoint_sha256!=excluded.checkpoint_sha256", (root, checkpoint_sha, timestamp))


def public(conn, job_id, queue):
    if conn.execute("SELECT 1 FROM sqlite_master WHERE name='recovery_watch' AND type='table'").fetchone() is None:
        return {"watch": None}
    watch = conn.execute("SELECT * FROM recovery_watch WHERE root_job_id=?", (queue["root_job_id"] if queue else job_id,)).fetchone()
    return {"watch": dict(watch) if watch else None}


def inspect(conn, host_dir, job, spec, cfg, *, timestamp=None):
    now = time.time() if timestamp is None else timestamp
    entry = conn.execute("SELECT * FROM recovery_queue WHERE job_id=?", (job["id"],)).fetchone()
    if entry is None:
        return
    from .recovery_state import normalize_policy
    policy = normalize_policy(spec["recovery"]["retry"])
    if conn.execute("SELECT 1 FROM recovery_watch WHERE root_job_id=?", (entry["root_job_id"],)).fetchone() is None:
        progress(conn, entry["root_job_id"], entry["checkpoint_sha256"], entry["first_queued_at"])
    try:
        checkpoint = recovery.CheckpointStore(json.loads(recovery.context(host_dir, job, spec, create=False))).load()
        sha = recovery.digest(checkpoint)
        if checkpoint is not None or entry["checkpoint_sha256"] == recovery.digest(None):
            progress(conn, entry["root_job_id"], sha, now)
    except (ValueError, OSError):
        pass  # Invalid or missing progress cannot extend a deadline.
    watch = conn.execute("SELECT * FROM recovery_watch WHERE root_job_id=?", (entry["root_job_id"],)).fetchone()
    age = max(0, now - watch["last_progress_at"])
    stop = policy["max_no_progress_sec"] > 0 and age >= policy["max_no_progress_sec"]
    interval = policy["no_progress_sec"]
    if interval > 0 and age >= interval and (watch["last_notice_at"] is None or now - watch["last_notice_at"] >= interval):
        batch = state.get_batch(conn, job["batch_id"])
        if "recovery_no_progress" in (cfg.get("notify") or {}).get("on", []):
            event_id = f"recovery-{job['id']}-{int(now * 1000)}"
            event = {"event": "recovery_no_progress", "event_id": event_id, "batch": batch["name"], "batch_id": job["batch_id"], "project": job["project"], "node": state.hostname(), "job_id": job["id"], "root_job_id": entry["root_job_id"], "round": entry["round"], "last_progress_at": watch["last_progress_at"], "observed_at": now, "stopped": stop}
            conn.execute("INSERT OR IGNORE INTO recovery_notices(id,root_job_id,event) VALUES(?,?,?)", (event_id, entry["root_job_id"], json.dumps(event)))
        conn.execute("UPDATE recovery_watch SET last_notice_at=? WHERE root_job_id=?", (now, entry["root_job_id"]))
    if stop and job["status"] in ("pending", "waiting_quota"):
        state.update_job(conn, job["id"], status="blocked", failure="recovery_no_progress", finished_at=state.now())


def deliver(cfg, log):
    from . import notify
    with state.connect() as conn:
        events = list(conn.execute("SELECT * FROM recovery_notices WHERE delivered_at IS NULL ORDER BY rowid LIMIT 32"))
    for row in events:
        results = notify.send(json.loads(row["event"]), cfg)
        for result in results:
            if not result.startswith("ok:"):
                log(f"recovery notify: {result}")
        if results and all(result.startswith("ok:") for result in results):
            with state.connect() as conn:
                conn.execute("UPDATE recovery_notices SET delivered_at=? WHERE id=?", (time.time(), row["id"]))
