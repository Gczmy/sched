"""Version-pinned task DAG and append-only binding decisions.

No worker inference, client lineage or scientific acceptance authority.
"""
from __future__ import annotations

from contextvars import ContextVar
import hashlib
import json
import os
import re

from . import dependencies, state
from .execution_policy import digest
from .integration import instance_id

request_id = ContextVar("dependency_request_id", default=None)
MAX_GRAPH = 100_000
SCHEMA = """
CREATE TABLE IF NOT EXISTS task_dependency_events (
 seq INTEGER PRIMARY KEY AUTOINCREMENT,
 event_id TEXT NOT NULL UNIQUE,
 request_id TEXT UNIQUE,
 job_id TEXT NOT NULL,
 job_version INTEGER NOT NULL,
 instance_id TEXT NOT NULL,
 target_sha256 TEXT NOT NULL,
 previous_event_id TEXT,
 bindings TEXT NOT NULL,
 observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS task_dependency_job ON task_dependency_events(job_id,seq);
CREATE TRIGGER IF NOT EXISTS task_dependency_immutable
 BEFORE UPDATE ON task_dependency_events BEGIN SELECT RAISE(ABORT,'immutable task dependencies'); END;
CREATE TRIGGER IF NOT EXISTS task_dependency_retained
 BEFORE DELETE ON task_dependency_events BEGIN SELECT RAISE(ABORT,'retained task dependencies'); END;
CREATE TRIGGER IF NOT EXISTS revision_task_dependency
 AFTER INSERT ON task_dependency_events BEGIN
 UPDATE batches SET revision=revision+1 WHERE id=(SELECT batch_id FROM jobs WHERE id=NEW.job_id);
 END;
"""


def local_selectors(value):
    if not isinstance(value, list) or len(value) > dependencies.MAX_TASKS:
        raise ValueError("任务 depends_on 必须是显式 task_id/version 数组")
    seen = set()
    for item in value:
        if (not isinstance(item, dict) or set(item) != {"task_id", "version"}
                or not isinstance(item["task_id"], str)
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", item["task_id"])
                or type(item["version"]) is not int or not 1 <= item["version"] <= 2**63 - 1):
            raise ValueError("任务 depends_on 必须显式指定安全 task_id 与正整数 version")
        key = (item["task_id"], item["version"])
        if key in seen:
            raise ValueError("任务 depends_on 重复")
        seen.add(key)
    return value


def validate_input(tasks):
    """Forward references allowed; new tasks have exactly version one."""
    graph = {task["id"]: [] for task in tasks}
    for task in tasks:
        for item in local_selectors(task.get("depends_on", [])):
            if item["task_id"] not in graph or item["version"] != 1:
                raise ValueError("新批次任务依赖必须引用本批次存在的 task/version 1")
            graph[task["id"]].append(item["task_id"])
        dependencies.normalize(task.get("depends_on_exact", []))
    check_graph(graph)


def check_graph(graph, roots=None):
    visited, active = set(), set()
    for root in graph if roots is None else roots:
        if root in visited:
            continue
        active.add(root)
        stack = [(root, iter(graph.get(root, [])))]
        while stack:
            node, iterator = stack[-1]
            child = next(iterator, None)
            if child is None:
                stack.pop()
                active.remove(node)
                visited.add(node)
            elif child in active:
                raise ValueError("任务/批次依赖成环: " + " -> ".join(str(n) for n, _ in stack) + " -> " + str(child))
            elif child not in visited:
                active.add(child)
                stack.append((child, iter(graph.get(child, []))))


def available(conn):
    return (conn.execute("PRAGMA user_version").fetchone()[0] >= 15
            and conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='task_dependency_events'").fetchone() is not None)


def latest(conn, job):
    if not available(conn):
        return None
    return conn.execute("SELECT * FROM task_dependency_events WHERE job_id=? ORDER BY seq DESC LIMIT 1", (job["id"],)).fetchone()


def target_digest(conn, job):
    row = conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
                       (job["batch_id"], job["task_id"], job["version"])).fetchone()
    if row is None:
        raise ValueError("任务 spec 不存在")
    # Runtime/clean may legitimately replace the target's current fingerprint.
    # Its immutable declaration, not a mutable cache observation, binds edges.
    return digest({"instance_id": instance_id(conn), "job_id": job["id"], "version": job["version"],
                   "spec": json.loads(row[0])})


def stored(conn, job):
    event = latest(conn, job)
    if event is not None:
        if (event["instance_id"] != instance_id(conn) or event["job_version"] != job["version"]
                or event["target_sha256"] != target_digest(conn, job)):
            raise ValueError("任务依赖目标绑定已漂移")
        return decode_event(event)["bindings"]
    spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
                                   (job["batch_id"], job["task_id"], job["version"])).fetchone()[0])
    if spec.get("depends_on") or spec.get("depends_on_exact"):
        raise ValueError("任务依赖声明缺少已接受的冻结绑定")
    return []


def decode_event(event):
    raw = event["bindings"]
    if not isinstance(raw, str) or len(raw.encode()) > dependencies.MAX_BYTES:
        raise ValueError("任务依赖绑定超过上限")
    bindings = dependencies.normalize(json.loads(raw), stored=True)
    payload = {"job_id": event["job_id"], "version": event["job_version"], "instance_id": event["instance_id"],
               "target_sha256": event["target_sha256"], "previous_event_id": event["previous_event_id"],
               "bindings": bindings, "request_id": event["request_id"]}
    if digest(payload) != event["event_id"]:
        raise ValueError("依赖事件摘要不匹配")
    return {**dict(event), "bindings": bindings}


def append(conn, job, bindings, *, rid=None):
    bindings = dependencies.normalize(bindings, stored=True)
    previous = latest(conn, job)
    payload = {"job_id": job["id"], "version": job["version"], "instance_id": instance_id(conn),
               "target_sha256": target_digest(conn, job), "previous_event_id": previous["event_id"] if previous else None,
               "bindings": bindings, "request_id": rid}
    event_id = digest(payload)
    conn.execute("INSERT INTO task_dependency_events(event_id,request_id,job_id,job_version,instance_id,"
                 "target_sha256,previous_event_id,bindings,observed_at) VALUES (?,?,?,?,?,?,?,?,?)",
                 (event_id, rid, job["id"], job["version"], payload["instance_id"], payload["target_sha256"],
                  payload["previous_event_id"], json.dumps(bindings), state.now()))
    return event_id


def bind_new_batch(conn, batch_id):
    jobs = conn.execute("SELECT * FROM jobs WHERE batch_id=?", (batch_id,)).fetchall()
    for job in jobs:
        spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
                                       (batch_id, job["task_id"], job["version"])).fetchone()[0])
        selectors = list(spec.get("depends_on_exact", []))
        local = local_selectors(spec.get("depends_on", []))
        if local:
            selectors.append({"instance_id": instance_id(conn), "batch_id": batch_id, "tasks": local})
        if selectors:
            append(conn, job, dependencies.bind(conn, selectors))
    validate_cycles(conn, [job["id"] for job in jobs])


def inherit(conn, old, new):
    # A new version inherits the effective *frozen* selection, not latest names.
    bindings = stored(conn, old)
    if bindings or latest(conn, old) is not None:
        append(conn, new, bindings)


def validate_cycles(conn, roots):
    """Job nodes plus shared batch gates preserve whole-batch legacy barriers."""
    jobs = conn.execute("SELECT * FROM jobs LIMIT ?", (MAX_GRAPH + 1,)).fetchall()
    batches = conn.execute("SELECT rowid,* FROM batches ORDER BY created_at,rowid LIMIT ?", (MAX_GRAPH + 1,)).fetchall()
    if len(jobs) + len(batches) > MAX_GRAPH:
        raise ValueError("依赖环检查超过有界图容量")
    by_batch, graph = {}, {}
    for job in jobs:
        by_batch.setdefault(job["batch_id"], []).append(job)
    latest_names = {batch["name"]: batch["id"] for batch in batches}
    gates = {"@batch:" + batch["id"]: batch for batch in batches}
    # Parse only reachable declarations, not unrelated damaged historical rows.
    job_map = {job["id"]: job for job in jobs}
    pending, seen, edge_count = list(roots), set(), 0
    while pending:
        node = pending.pop()
        if node in seen:
            continue
        seen.add(node)
        if node in job_map:
            job = job_map[node]
            graph[node] = ["@batch:" + job["batch_id"]] + [task["job_id"] for source in stored(conn, job) for task in source["tasks"]]
        elif node in gates:
            batch = gates[node]
            edges = []
            raw = batch["depends_on"] or "[]"
            if len(raw.encode()) > dependencies.MAX_BYTES:
                raise ValueError("存量名称依赖超过上限")
            names = json.loads(raw)
            if not isinstance(names, list) or any(not isinstance(n, str) for n in names):
                raise ValueError("存量批次依赖非法")
            for name in names:
                source_batch = latest_names.get(name)
                if source_batch:
                    edges.append("@batch:" + source_batch)
                    newest = {}
                    for job in by_batch.get(source_batch, []):
                        key = job["task_id"]
                        if key not in newest or job["version"] > newest[key]["version"]:
                            newest[key] = job
                    edges.extend(job["id"] for job in newest.values())
            edges.extend(task["job_id"] for source in dependencies.stored(batch) for task in source["tasks"])
            graph[node] = edges
        edge_count += len(graph.get(node, []))
        if edge_count > MAX_GRAPH:
            raise ValueError("依赖环检查超过有界边容量")
        pending.extend(graph.get(node, []))
    check_graph(graph, roots)


def update(conn, job, selectors, *, reopen=False):
    if request_id.get() is None:
        raise ValueError("依赖更新必须绑定 request")
    if job["status"] not in {"pending", "waiting_dep", "waiting_quota"}:
        raise ValueError("只允许更新未启动的 pending 版本")
    batch = state.get_batch(conn, job["batch_id"])
    if batch["mode"] != "mix" or batch["status"] == "discarded":
        raise ValueError("历史 strict/退役批次不可更新")
    from ._legacy_execution import NATIVE_EXEC_ALL_INTERNAL_FIELDS
    for row in conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=?", (job["batch_id"], job["task_id"])):
        if NATIVE_EXEC_ALL_INTERNAL_FIELDS.intersection(json.loads(row[0])):
            raise ValueError("历史执行元数据不可授予更新权")
    if job["started_at"] is not None or job["pgid"] is not None or job["retries"] or job["rc"] is not None:
        raise ValueError("当前版本存在历史启动事实")
    probe = {"batch_id": job["batch_id"]}
    task = {"task_id": job["task_id"]}
    if not dependencies.recorded_clear(conn, probe, task):
        raise ValueError("同任务存在运行、未知或历史执行记录")
    from .artifact_revalidation import group_absent
    for row in conn.execute("SELECT pgid FROM jobs WHERE batch_id=? AND task_id=? AND pgid IS NOT NULL", (job["batch_id"], job["task_id"])):
        if not group_absent(row[0]):
            raise ValueError("同任务进程组仍活跃或不可知")
    if conn.execute("SELECT 1 FROM execution_attempts WHERE job_id=?", (job["id"],)).fetchone():
        raise ValueError("当前版本存在 execution attempt")
    for row in conn.execute("SELECT id FROM jobs WHERE batch_id=? AND task_id=?", (job["batch_id"], job["task_id"])):
        prefix = hashlib.sha256(row["id"].encode()).hexdigest()[:24]
        try:
            os.stat(os.path.join(state.host_dir(), "launch", prefix + ".launch"), follow_symlinks=False)
        except FileNotFoundError:
            pass
        except OSError as error:
            raise ValueError("同任务启动标记不可读") from error
        else:
            raise ValueError("同任务存在未决启动标记")
    if conn.execute("SELECT 1 FROM gpu_jobs WHERE job_id=?", (job["id"],)).fetchone() or conn.execute(
            "SELECT 1 FROM control_requests WHERE job_id=? AND status='pending'", (job["id"],)).fetchone():
        raise ValueError("当前版本存在分配或待处理控制请求")
    # Prevent overlay updates from concealing corruption of the prior target.
    stored(conn, job)
    frozen = dependencies.bind(conn, selectors)
    event = append(conn, job, frozen, rid=request_id.get())
    validate_cycles(conn, [job["id"]])
    if reopen:
        if batch["status"] != "blocked" or batch["failure_policy"] != "continue_independent":
            raise ValueError("reopen 仅适用于显式 continue_independent 的 blocked 批次")
        conn.execute("UPDATE batches SET status='active' WHERE id=?", (batch["id"],))
    return event


def facts(conn, job, *, limit, cursor):
    """Bounded recorded blocking paths; no file/marker checks or ready claim."""
    bindings = stored(conn, job)
    event = latest(conn, job)
    results, pending, visited = [], [(job, [], 0)], set()
    depth_truncated = False
    budget = min(MAX_GRAPH, cursor + limit + 1)
    while pending and len(results) < budget:
        current, path, depth = pending.pop()
        if current["id"] in visited:
            continue
        visited.add(current["id"])
        for source in stored(conn, current):
            for task in source["tasks"]:
                selected = dependencies.matched(conn, source, task)
                clear = dependencies.recorded_clear(conn, source, task) if selected is not None else False
                reason = ("binding_mismatch" if selected is None else "execution_uncertain" if not clear
                          else "recorded_success_requires_dispatch_recheck" if selected["status"] in {"done", "skip"}
                          else "source_" + selected["status"])
                edge = {"instance_id": source["instance_id"], "batch_id": source["batch_id"], **task}
                edge_path = path + [{"batch_id": current["batch_id"], "task_id": current["task_id"], "version": current["version"]}]
                results.append({**edge, "path": edge_path, "reason": reason,
                                "recorded_status": selected["status"] if selected is not None else None,
                                "recorded_failure": selected["failure"] if selected is not None else None,
                                "recorded_rc": selected["rc"] if selected is not None else None,
                                "binding_matches": selected is not None, "recorded_clear": clear})
                if selected is not None and selected["status"] not in {"done", "skip"} and depth < 31:
                    pending.append((selected, edge_path, depth + 1))
                elif selected is not None and selected["status"] not in {"done", "skip"}:
                    depth_truncated = True
                if len(results) >= budget:
                    break
            if len(results) >= budget:
                break
    more = len(results) > cursor + limit or bool(pending)
    next_cursor = cursor + limit if more and cursor + limit <= 10_000 else None
    return {"bindings": bindings, "event_id": event["event_id"] if event else None,
            "previous_event_id": event["previous_event_id"] if event else None,
            "binding_sha256": digest(bindings), "blocking_paths": results[cursor:cursor + limit],
            "truncated": more, "next_cursor": next_cursor, "cursor_limit_reached": more and next_cursor is None,
            "path_depth_limit": 32, "depth_truncated": depth_truncated,
            "path_semantics": "one_recorded_path_per_visited_job; batch gates queried separately",
            "external_artifacts_checked": False, "launch_markers_checked": False}
