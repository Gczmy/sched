"""Frozen scheduler selectors; legacy names stay explicitly dynamic.

No customer lineage, scientific acceptance, launch or settlement authority.
"""
from __future__ import annotations

import json
import re

from .execution_policy import digest
from .integration import instance_id

MAX_SOURCES = 256
MAX_TASKS = 10_000
MAX_BYTES = 4 * 1024 * 1024


def normalize(value, *, stored=False):
    if not isinstance(value, list) or len(value) > MAX_SOURCES:
        raise ValueError(f"depends_on_exact 必须是最多 {MAX_SOURCES} 个来源的数组")
    result, seen, count = [], set(), 0
    for source in value:
        if not isinstance(source, dict) or set(source) != {"instance_id", "batch_id", "tasks"}:
            raise ValueError("exact 来源需要 instance_id/batch_id/tasks，不能使用名称或隐式 latest")
        if not isinstance(source["instance_id"], str) or not re.fullmatch(r"[0-9a-f]{32}", source["instance_id"]):
            raise ValueError("exact instance_id 必须是 32 位小写十六进制")
        batch = source["batch_id"]
        if not isinstance(batch, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,255}", batch):
            raise ValueError("exact batch_id 必须是完整安全 ID")
        if not isinstance(source["tasks"], list) or not source["tasks"]:
            raise ValueError("exact 来源必须显式指定非空 task/version 清单")
        tasks = []
        for task in source["tasks"]:
            keys = {"task_id", "version"}
            if stored:
                keys |= {"job_id", "spec_sha256", "fingerprint"}
            if not isinstance(task, dict) or set(task) != keys:
                raise ValueError("exact task 必须指定 task_id 和 version")
            if not isinstance(task["task_id"], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", task["task_id"]):
                raise ValueError("exact task_id 必须是安全标识符")
            if type(task["version"]) is not int or not 1 <= task["version"] <= 2**63 - 1:
                raise ValueError("exact version 必须是正整数，不能使用 latest")
            key = (source["instance_id"], batch, task["task_id"], task["version"])
            if key in seen:
                raise ValueError("重复 exact task/version")
            seen.add(key)
            if stored and (not isinstance(task["job_id"], str) or not task["job_id"]
                    or not isinstance(task["spec_sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", task["spec_sha256"])
                    or task["fingerprint"] is not None and not isinstance(task["fingerprint"], str)):
                raise ValueError("存量 exact 绑定无效")
            tasks.append(dict(task))
            count += 1
        result.append({"instance_id": source["instance_id"], "batch_id": batch, "tasks": tasks})
    if count > MAX_TASKS or len(json.dumps(result, ensure_ascii=True).encode()) > MAX_BYTES:
        raise ValueError("exact 清单超过任务数/字节上限")
    return result


def stored(batch):
    # Schema migration must not reinterpret any historical name dependency.
    if "depends_on_exact" not in batch.keys():
        return []
    raw = batch["depends_on_exact"] or "[]"
    if not isinstance(raw, str) or len(raw.encode()) > MAX_BYTES:
        raise ValueError("存量 exact 清单超过字节上限或类型非法")
    try:
        return normalize(json.loads(raw), stored=True)
    except RecursionError as error:
        raise ValueError("存量 exact 清单嵌套非法") from error


def selected_job(conn, source, task):
    jobs = conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id=? AND version=?",
                        (source["batch_id"], task["task_id"], task["version"])).fetchall()
    spec = conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
                        (source["batch_id"], task["task_id"], task["version"])).fetchone()
    if len(jobs) != 1 or spec is None:
        raise ValueError("exact task/version 不存在或不唯一")
    parsed = json.loads(spec[0])
    if not isinstance(parsed, dict):
        raise ValueError("exact 来源 spec 无效")
    return jobs[0], digest(parsed)


def bind(conn, selectors):
    bound = normalize(selectors)
    current_instance = instance_id(conn)
    for source in bound:
        if current_instance is None or source["instance_id"] != current_instance:
            raise ValueError("exact instance 不匹配；不支持跨实例执行依赖")
        batch = conn.execute("SELECT mode FROM batches WHERE id=?", (source["batch_id"],)).fetchone()
        if batch is None or batch["mode"] != "mix":
            raise ValueError("exact batch ID 不存在或属于历史 strict 执行")
        for task in source["tasks"]:
            job, spec_hash = selected_job(conn, source, task)
            task.update(job_id=job["id"], spec_sha256=spec_hash, fingerprint=job["fingerprint"])
    return normalize(bound, stored=True)


def matched(conn, source, task):
    if instance_id(conn) != source["instance_id"]:
        return None
    try:
        job, spec_hash = selected_job(conn, source, task)
    except (ValueError, TypeError, json.JSONDecodeError):
        return None
    if job["id"] != task["job_id"] or spec_hash != task["spec_sha256"] or job["fingerprint"] != task["fingerprint"]:
        return None
    return job


def recorded_clear(conn, source, task):
    # Any generation of the selected source task may still mutate its files.
    # Unrelated tasks do not become a whole-batch barrier for exact subsets.
    params = (source["batch_id"], task["task_id"])
    if conn.execute("SELECT 1 FROM jobs WHERE batch_id=? AND task_id=? AND status IN ('running','interrupted') LIMIT 1", params).fetchone():
        return False
    if conn.execute("SELECT 1 FROM execution_attempts a JOIN jobs j ON j.id=a.job_id"
                    " WHERE j.batch_id=? AND j.task_id=? AND a.phase!='exited' LIMIT 1", params).fetchone():
        return False
    for attempt in conn.execute("SELECT a.observation FROM execution_attempts a JOIN jobs j ON j.id=a.job_id"
                                " WHERE j.batch_id=? AND j.task_id=?", params):
        try:
            observation = json.loads(attempt[0] or "null")
        except (ValueError, TypeError):
            return False
        if not isinstance(observation, dict) or observation.get("status") != "exited" or observation.get("group_clean") is not True:
            return False
    if conn.execute("SELECT 1 FROM native_sessions n JOIN jobs j ON j.id=n.job_id"
                    " WHERE j.batch_id=? AND j.task_id=? LIMIT 1", params).fetchone():
        return False  # Historical sessions never gain execution authority.
    return True


def validate_mixed_cycles(conn, name, legacy_names, exact):
    """Proposed latest-name node plus immutable full-ID edges, in one snapshot."""
    rows = conn.execute("SELECT rowid,* FROM batches ORDER BY created_at,rowid").fetchall()
    batches = {row["id"]: row for row in rows}
    latest = {row["name"]: row["id"] for row in rows}
    candidate = "@candidate"
    latest[name] = candidate

    def children(node):
        if node == candidate:
            names, sources = legacy_names, exact
        elif node in batches:
            row = batches[node]
            names, sources = json.loads(row["depends_on"] or "[]"), stored(row)
        else:
            return []
        if not isinstance(names, list) or any(not isinstance(n, str) for n in names):
            raise ValueError("存量名称依赖无效")
        return [latest[n] for n in names if n in latest] + [s["batch_id"] for s in sources]

    visited, active, path = set(), set(), []
    stack = [(candidate, iter(children(candidate)))]
    active.add(candidate)
    path.append(candidate)
    while stack:
        node, iterator = stack[-1]
        child = next(iterator, None)
        if child is None:
            stack.pop()
            active.remove(node)
            path.pop()
            visited.add(node)
        elif child in active:
            raise ValueError("混合 exact/名称依赖成环: " + " -> ".join(path + [child]))
        elif child not in visited:
            active.add(child)
            path.append(child)
            stack.append((child, iter(children(child))))


def facts(conn, batch):
    """Recorded facts only; never inspect external files or grant dispatch."""
    rows = []
    names = json.loads(batch["depends_on"] or "[]")
    if not isinstance(names, list) or any(not isinstance(name, str) or not name for name in names):
        raise ValueError("存量名称依赖无效")
    for name in names:
        resolved = conn.execute("SELECT id,status FROM batches WHERE name=? ORDER BY created_at DESC,rowid DESC LIMIT 1", (name,)).fetchone()
        rows.append({"kind": "legacy_name", "name": name, "dynamic_latest": True,
                     "resolved_batch_id": resolved["id"] if resolved else None,
                     "recorded_status": resolved["status"] if resolved else None})
    for source in stored(batch):
        for task in source["tasks"]:
            job = matched(conn, source, task)
            rows.append({"kind": "exact_task", "instance_id": source["instance_id"],
                         "batch_id": source["batch_id"], **task, "binding_matches": job is not None,
                         "recorded_status": job["status"] if job else None,
                         "recorded_clear": recorded_clear(conn, source, task) if job else False})
    return rows
