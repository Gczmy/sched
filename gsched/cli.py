"""CLI (文档 §4.2 / B9 / R1).

任务级命令统一 <batch>:<task> 双段引用 (R1).
本地 Mac CLI 只做 ssh 跳转; 计算节点上直接执行 (N5 双层结构).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import Any

from . import state, __version__
from .config import (
    ConfigError,
    config_path,
    default_state_dir,
    expand_path,
    load_config,
    resolve_template,
)
from .schema import (
    SchemaError,
    check_dependency_cycle,
    parse_shell_cmd,
    validate_batch,
)


def _load_cfg():
    try:
        return load_config()
    except ConfigError as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(1)


def _batch_id_from_name(name: str) -> str | None:
    """batch name -> 最新批次 id (带时间戳). 找不到返回 None."""
    with state.connect() as conn:
        row = conn.execute(
            "SELECT id FROM batches WHERE name=? ORDER BY created_at DESC LIMIT 1",
            (name,),
        ).fetchone()
    return row["id"] if row else None


def _job_id(batch_id: str, task_id: str, version: int = 0) -> str:
    """jobs.id = {batch}-{task}-v{version}; version<=0 取最新."""
    if version > 0:
        return f"{batch_id}-{task_id}-v{version}"
    with state.connect() as conn:
        row = conn.execute(
            "SELECT version FROM jobs WHERE batch_id=? AND task_id=?"
            " ORDER BY version DESC LIMIT 1",
            (batch_id, task_id),
        ).fetchone()
        v = row["version"] if row else 1
    return f"{batch_id}-{task_id}-v{v}"


def _parse_task_ref(ref: str) -> tuple[str, str]:
    """R1: <batch>:<task> 双段定位, 禁止裸 task."""
    if ":" not in ref:
        print(f"错误: 任务引用必须是 <batch>:<task> 格式 (收到 '{ref}')", file=sys.stderr)
        sys.exit(1)
    b, t = ref.split(":", 1)
    if not b or not t:
        print(f"错误: 非法任务引用 '{ref}' (需 <batch>:<task>)", file=sys.stderr)
        sys.exit(1)
    return b, t


# ---------- 命令实现 ----------

def cmd_init(args: argparse.Namespace) -> int:
    """sched init: 向导生成 config.json (M0)."""
    p = args.config or config_path()
    if os.path.exists(p):
        print(f"config.json 已存在: {p} (如需重建请先删除)", file=sys.stderr)
        return 1
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
    cfg = {
        "schema_version": 1,
        "user": input(f"运行账户 [{os.environ.get('USER','')}]: ").strip()
        or os.environ.get("USER", ""),
        "node": input("daemon 计算节点名 [ambiorix]: ").strip() or "ambiorix",
        "state_dir": input(f"state 目录 [{default_state_dir()}]: ").strip()
        or default_state_dir(),
        "ssh_chain": ["HPDC"],
        "gpus": [0, 1, 2, 3],
        "projects": {
            "a_share": {
                "root": input("a_share 项目根目录: ").strip(),
                "git": True,
            }
        },
        "default_project": "a_share",
        "venvs": {
            "kronos_ft": input("kronos_ft venv python 路径: ").strip(),
        },
    }
    with open(p, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)
    print(f"已生成 {p}")
    print("下一步: `sched daemon start --check` 跑前置检查 (M0)")
    return 0


def _dry_run_preview(norm: dict, cfg: dict) -> dict:
    """§G4 A 类 dry-run: 纯只读预览 (不写 state).

    返回 {"tasks": [{id, cmd_flat, stage_preds, skip, reason}],
          "dep_status": {name: (status, n_done, n_total)}, "git_rev"}.
    skip 预测 = 产物指纹有效 (A2) 且规则校验通过 (D8) -> skip;
    依赖就绪 = depends_on 上游当前状态 (O1 name->id 解析, 一次 SELECT).
    """
    from .artifacts import check_artifact
    from .fingerprint import compute_fingerprint

    def _expand_venv(tok: str) -> str:
        if isinstance(tok, str) and tok.startswith("{VENV:") and tok.endswith("}"):
            name = tok[len("{VENV:"):-1]
            p = cfg.get("venvs", {}).get(name)
            if not p:
                raise SchemaError(f"venv 未定义: {name}")
            return p
        return tok

    def _expand_cmd(cmd_list: list[str], stage_artifacts: dict[int, dict] | None = None,
                    cwd_abs: str | None = None) -> list[str]:
        out = []
        for tok in cmd_list:
            tok = _expand_venv(tok)
            if isinstance(tok, str) and tok.startswith("{stage") and tok.endswith("}"):
                inner = tok[1:-1]
                parts = inner.split("_", 1)
                if len(parts) == 2 and parts[0].startswith("stage") and parts[0][5:].isdigit():
                    si = int(parts[0][5:])
                    key = parts[1]
                    arts = (stage_artifacts or {}).get(si)
                    if arts is None:
                        raise SchemaError(f"{tok}: 引用不存在的 stage {si} (N7)")
                    a = arts.get(key)
                    if not a or not a.get("path"):
                        raise SchemaError(f"{tok}: stage{si} 未声明产物 key '{key}' (N7)")
                    p = a["path"]
                    if not os.path.isabs(p) and cwd_abs:
                        p = os.path.normpath(os.path.join(cwd_abs, p))
                    tok = p
            out.append(tok)
        return out

    def _pred_stage(cmd_e: list[str], arts: dict, cwd_abs: str) -> tuple[str, str]:
        """单 stage 预测: (skip/run, 原因). 产物路径相对 cwd 解析."""
        if not arts:
            return "run", "无产物声明 (必跑)"
        bad: list[str] = []
        for key, a in arts.items():
            p = a.get("path")
            if p and not os.path.isabs(p):
                p = os.path.normpath(os.path.join(cwd_abs, p))
            r = check_artifact(p or "", a)
            if r is not None:
                bad.append(f"{key}:{r}")
        if bad:
            return "run", "产物缺失/无效: " + "; ".join(bad)
        return "skip", "产物已就绪且规则通过"

    # 1) 展开命令 + skip 预测 (逐 stage, 对齐 dispatcher._should_skip 语义)
    venv_paths = cfg.get("venvs", {})
    preview_tasks = []
    git_rev: str | None = None
    n_skip = 0
    n_run = 0
    for t in norm["tasks"]:
        cwd_abs = t["cwd_abs"]
        stage_art: dict[int, dict] = {}
        if t["stages"]:
            stages_e = []
            stage_preds = []
            for j, s in enumerate(t["stages"]):
                stage_art[j] = s["artifacts"]
                cmd_e = _expand_cmd(s["cmd"], stage_art, cwd_abs)
                stages_e.append(cmd_e)
                # 指纹 (A2): 展开 cmd + git rev + venv
                fp, _, rev = compute_fingerprint(
                    None, [{"cmd": cmd_e}], cwd_abs, t["git"], venv_paths
                )
                git_rev = rev or git_rev
                st, why = _pred_stage(cmd_e, s["artifacts"], cwd_abs)
                stage_preds.append({"stage": j, "skip": st == "skip", "reason": why})
                if st == "skip":
                    n_skip += 1
                else:
                    n_run += 1
            preview_tasks.append({
                "id": t["id"],
                "cmd_flat": " && ".join(" ".join(c) for c in stages_e),
                "stages": stage_preds,
                "skip": all(p["skip"] for p in stage_preds),
            })
        else:
            cmd_e = _expand_cmd(t["cmd"], None, cwd_abs)
            fp, _, rev = compute_fingerprint(
                cmd_e, None, cwd_abs, t["git"], venv_paths
            )
            git_rev = rev or git_rev
            st, why = _pred_stage(cmd_e, t["artifacts"], cwd_abs)
            if st == "skip":
                n_skip += 1
            else:
                n_run += 1
            preview_tasks.append({
                "id": t["id"],
                "cmd_flat": " ".join(cmd_e),
                "stages": [{"stage": 0, "skip": st == "skip", "reason": why}],
                "skip": st == "skip",
            })

    # 2) 依赖就绪预览 (O1): depends_on name -> 最新批次 id + 状态
    dep_status: dict[str, str] = {}
    with state.connect() as conn:
        for dep in norm["depends_on"]:
            row = conn.execute(
                "SELECT id, status FROM batches WHERE name=?"
                " ORDER BY created_at DESC LIMIT 1",
                (dep,),
            ).fetchone()
            if row:
                dep_status[dep] = row["status"]
            else:
                dep_status[dep] = "NOT_FOUND"

    return {
        "tasks": preview_tasks,
        "dep_status": dep_status,
        "git_rev": git_rev,
        "n_skip": n_skip,
        "n_run": n_run,
    }


def cmd_submit(args: argparse.Namespace) -> int:
    """sched submit batch.json [--dry-run]: 校验 -> 预览(dry) 或 入队."""
    cfg = _load_cfg()
    path = args.batch
    try:
        with open(path, "r", encoding="utf-8") as f:
            spec = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"错误: 读取 {path} 失败: {e}", file=sys.stderr)
        return 1
    try:
        norm = validate_batch(spec, cfg)
        check_dependency_cycle(norm["depends_on"], cfg)
    except SchemaError as e:
        print(f"校验失败: {e}", file=sys.stderr)
        return 1

    # 依赖 name 存在性 (O1): 提交时解析为最新同 name 批次 id
    with state.connect() as conn:
        for dep in norm["depends_on"]:
            row = conn.execute(
                "SELECT id FROM batches WHERE name=? ORDER BY created_at DESC LIMIT 1",
                (dep,),
            ).fetchone()
            if not row:
                print(f"错误: depends_on 引用的批次不存在: '{dep}' (O1)", file=sys.stderr)
                return 1

    # 依赖环检测 (§3.4e B3): 按 name 拓扑 DFS (当前批次 + 已存在批次全图)
    def _dep_graph() -> dict[str, list[str]]:
        """name -> depends_on name 列表 (含当前批次)."""
        g: dict[str, list[str]] = {norm["name"]: list(norm["depends_on"])}
        with state.connect() as conn:
            rows = conn.execute("SELECT name, depends_on FROM batches").fetchall()
            for r in rows:
                g.setdefault(r["name"], json.loads(r["depends_on"] or "[]"))
        return g

    g = _dep_graph()
    visited: set[str] = set()
    stack: list[str] = []

    def _has_cycle(name: str) -> bool:
        if name in stack:
            cycle = " -> ".join(stack[stack.index(name):] + [name])
            raise SchemaError(f"依赖成环: {cycle} (B3 拒绝提交)")
        if name in visited:
            return False
        visited.add(name)
        stack.append(name)
        for d in g.get(name, []):
            if _has_cycle(d):
                return True
        stack.pop()
        return False

    try:
        _has_cycle(norm["name"])
    except SchemaError as e:
        print(f"校验失败: {e}", file=sys.stderr)
        return 1

    from datetime import datetime

    bid = f"{norm['name']}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    from .fingerprint import compute_fingerprint

    def _expand_venv(tok: str) -> str:
        """cmd 里的 {VENV:name} 模板展开为绝对解释器路径 (executor 直接用)."""
        if isinstance(tok, str) and tok.startswith("{VENV:") and tok.endswith("}"):
            name = tok[len("{VENV:"):-1]
            p = cfg.get("venvs", {}).get(name)
            if not p:
                raise SchemaError(f"venv 未定义: {name}")
            return p
        return tok

    def _expand_cmd(cmd_list: list[str], stage_artifacts: dict[int, dict] | None = None,
                    cwd_abs: str | None = None) -> list[str]:
        """cmd 模板展开: {VENV:name} + {stageN_<key>} (N7, 前序 stage 产物路径)."""
        out = []
        for tok in cmd_list:
            tok = _expand_venv(tok)
            if isinstance(tok, str) and tok.startswith("{stage") and tok.endswith("}"):
                # {stage0_ckpt} -> stage0 的 artifacts["ckpt"].path
                inner = tok[1:-1]  # stage0_ckpt
                parts = inner.split("_", 1)
                if len(parts) == 2 and parts[0].startswith("stage") and parts[0][5:].isdigit():
                    si = int(parts[0][5:])
                    key = parts[1]
                    arts = (stage_artifacts or {}).get(si)
                    if arts is None:
                        raise SchemaError(f"{tok}: 引用不存在的 stage {si} (N7)")
                    a = arts.get(key)
                    if not a or not a.get("path"):
                        raise SchemaError(f"{tok}: stage{si} 未声明产物 key '{key}' (N7)")
                    p = a["path"]
                    if not os.path.isabs(p) and cwd_abs:
                        p = os.path.normpath(os.path.join(cwd_abs, p))
                    tok = p
            out.append(tok)
        return out

    with state.connect() as conn:
        # 同名批次未全部终态 -> 拒绝 (定案 6)
        existing = conn.execute(
            "SELECT status FROM batches WHERE name=?", (norm["name"],)
        ).fetchall()
        if any(b["status"] not in ("done", "blocked") for b in existing):
            print(
                f"错误: 同名批次 '{norm['name']}' 已有未终态批次 (定案 6),"
                " 请改名或用 sched resubmit",
                file=sys.stderr,
            )
            return 1

        if getattr(args, "dry_run", False):
            # §G4 A 类: 纯只读预览, 不 insert
            prev = _dry_run_preview(norm, cfg)
            print(f"=== dry-run: {norm['name']} ({len(norm['tasks'])} 任务, mode={norm['mode']}) ===")
            if prev["dep_status"]:
                print("--- 依赖就绪 ---")
                for dep, st in prev["dep_status"].items():
                    mark = "✅" if st == "done" else ("⚠️" if st in ("active", "queued") else "❌")
                    note = {
                        "done": "上游已终态, 本批提交后可直接派发",
                        "blocked": "上游 blocked, 本批将挂起 waiting_dep",
                        "active": "上游运行中, 本批将挂起等解锁",
                        "queued": "上游排队中, 本批将挂起等解锁",
                        "NOT_FOUND": "上游不存在 (O1 已拒绝, 这里仅为展示)",
                    }.get(st, st)
                    print(f"  {mark} {dep}: {st} — {note}")
            print("--- 任务预览 ---")
            for pt in prev["tasks"]:
                tag = "SKIP" if pt["skip"] else "RUN "
                print(f"  [{tag}] {pt['id']}")
                for sp in pt["stages"]:
                    st = "SKIP" if sp["skip"] else "RUN "
                    print(f"      stage{sp['stage']} [{st}] {sp['reason']}")
                print(f"      cmd: {pt['cmd_flat'][:120]}{"..." if len(pt['cmd_flat']) > 120 else ""}")
            print("--- 汇总 ---")
            print(f"  将跑 {prev['n_run']} / 将 skip {prev['n_skip']} / 共 {len(norm['tasks'])} 任务")
            if prev["git_rev"]:
                print(f"  ⚠️ 预测基于当前 git rev {prev['git_rev'][:12]} (提交前若 pull 代码则预测作废, §G4)")
            if args.json:
                print("==JSON==")
                print(json.dumps(prev, ensure_ascii=False, indent=2))
            return 0

        state.insert_batch(
            conn, bid, norm["name"], norm["mode"], norm["depends_on"],
            norm["gpus"], norm["cwd"], norm["env"],
        )
        for i, t in enumerate(norm["tasks"]):
            # cmd/stages 的 {VENV:}/{stageN_<key>} 展开为绝对路径 (spec 存展开后的)
            cmd_e = _expand_cmd(t["cmd"]) if t["cmd"] else None
            stages_e = None
            if t["stages"]:
                # N7: stage j 可引用前序 stage 0..j-1 的产物
                stage_art: dict[int, dict] = {}
                stages_e = []
                for j, s in enumerate(t["stages"]):
                    stage_art[j] = s["artifacts"]
                    stages_e.append(
                        {
                            "cmd": _expand_cmd(s["cmd"], stage_art, t["cwd_abs"]),
                            "artifacts": s["artifacts"],
                            "probes": s.get("probes"),
                            "retry_transform": s.get("retry_transform"),
                            "paths_escape": s.get("paths_escape", False),
                        }
                    )
            # 任务级 spec 存规范化后的 (含 cwd_abs, 供 executor 直接用)
            spec_json = {
                "id": t["id"],
                "cmd": cmd_e,
                "stages": stages_e,
                "cwd_abs": t["cwd_abs"],
                "git": t["git"],
                "env": t["env"],
                "resources": t["resources"],
                "duration_min": t["duration_min"],
                "max_retry": t["max_retry"],
                "artifacts": t["artifacts"],
                "retry_transform": t["retry_transform"],
                "probes": t["probes"],
            }
            state.insert_task(conn, bid, t["id"], 1, spec_json, i)
            # Job 指纹 (A2): 指纹用展开后的 cmd (venv 路径入指纹)
            fp, stage_fps, rev = compute_fingerprint(
                cmd_e, stages_e, t["cwd_abs"], t["git"], cfg.get("venvs", {})
            )
            state.insert_job(
                conn, f"{bid}-{t['id']}-v1", bid, t["id"], 1,
                fp, stage_fps,
            )

    print(f"已入队: {bid} ({len(norm['tasks'])} 任务, mode={norm['mode']})")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """sched run [flags] -- <cmd>: 一行提交单任务 (B14 L1 / N8 / O9 / P8 / R5)."""
    cfg = _load_cfg()
    shell_cmd = " ".join(args.cmd)
    try:
        tokens = parse_shell_cmd(shell_cmd, "sched run")
    except SchemaError as e:
        print(f"校验失败: {e}", file=sys.stderr)
        return 1
    if not tokens:
        print("错误: 空命令", file=sys.stderr)
        return 1

    from datetime import datetime

    # venv: 无 --venv 时用 config 第一个 venv
    venvs = cfg.get("venvs", {})
    venv_name = args.venv or next(iter(venvs), None)
    if not venv_name:
        print("错误: config.venvs 为空, 无法解析解释器", file=sys.stderr)
        return 1
    interp = venvs[venv_name]
    cmd_array = [f"{{VENV:{venv_name}}}"] + tokens
    base = tokens  # 用户给的 cmd 是纯命令, 解释器由框架补

    batch_name = f"run-{tokens[0].split('/')[-1]}-{datetime.now().strftime('%H%M%S')}"
    bid = f"{batch_name}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    cwd = args.cwd or "{ROOT}"
    cwd_abs = os.path.realpath(os.path.expanduser(resolve_template(cwd, cfg)))

    task_spec = {
        "id": "run",
        "cmd": [interp] + base,  # {VENV:} 已展开为绝对路径
        "stages": None,
        "cwd_abs": cwd_abs,
        "git": None,
        "env": {},
        "resources": {},
        "duration_min": args.duration,
        "max_retry": 0,
        "artifacts": (
            {"out": {"path": args.out}} if args.out else {}
        ),
        "retry_transform": None,
        "probes": None,
    }

    from .fingerprint import compute_fingerprint

    with state.connect() as conn:
        state.insert_batch(
            conn, bid, batch_name, "mix", [], None, "{ROOT}", None
        )
        state.insert_task(conn, bid, "run", 1, task_spec, 0)
        fp, stage_fps, rev = compute_fingerprint(
            task_spec["cmd"], None, cwd_abs, None, cfg.get("venvs", {})
        )
        state.insert_job(conn, f"{bid}-run-v1", bid, "run", 1, fp, stage_fps)

    print(f"已入队: {bid} (gpus={args.gpus} 张, duration={args.duration}min)")
    print(f"  status/cancel 用批次名: {batch_name}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """sched status [batch]: 三视图总览 + --json."""
    cfg = _load_cfg()
    out: dict[str, Any] = {"batches": [], "jobs": [], "gpus": []}
    with state.connect() as conn:
        batches = conn.execute(
            "SELECT * FROM batches ORDER BY created_at DESC"
        ).fetchall()
        for b in batches:
            if args.batch and b["name"] != args.batch:
                continue
            jobs = conn.execute(
                "SELECT status FROM jobs WHERE batch_id=?", (b["id"],)
            ).fetchall()
            statuses = [j["status"] for j in jobs]
            progress = f"{statuses.count('done') + statuses.count('skip')}/{len(statuses)}"
            out["batches"].append(
                {
                    "id": b["id"], "name": b["name"], "mode": b["mode"],
                    "status": b["status"], "depends_on": json.loads(b["depends_on"] or "[]"),
                    "progress": progress,
                }
            )
        jobs = conn.execute("SELECT * FROM jobs ORDER BY rowid").fetchall()
        # batch_id -> name 映射 (显示用, 避免截断 batch_id 丢 name 首字符)
        name_by_id = {b["id"]: b["name"] for b in batches}
        for j in jobs:
            if args.batch and j["batch_id"] not in [
                b["id"] for b in conn.execute(
                    "SELECT id FROM batches WHERE name=?", (args.batch,)
                ).fetchall()
            ]:
                continue
            out["jobs"].append(
                {
                    "id": j["id"], "batch": j["batch_id"], "task": j["task_id"],
                    "status": j["status"], "gpu": j["gpu"],
                    "retries": j["retries"], "failure": j["failure"],
                }
            )
        gpus = conn.execute("SELECT * FROM gpus ORDER BY idx").fetchall()
        for g in gpus:
            out["gpus"].append(
                {
                    "idx": g["idx"], "status": g["status"], "job": g["job_id"],
                    "quarantined": g["quarantined"],
                }
            )

    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0

    print("=== 批次 ===")
    for b in out["batches"]:
        print(
            f"  {b['name']:<28} [{b['status']:<8}] {b['progress']:<6}"
            f" dep={b['depends_on']}"
        )
    print("=== 任务 ===")
    for j in out["jobs"]:
        extra = f" gpu={j['gpu']}" if j["gpu"] is not None else ""
        fail = f" ({j['failure']})" if j["failure"] else ""
        bname = name_by_id.get(j["batch"], j["batch"])
        print(f"  {bname:<22}:{j['task']:<20} [{j['status']:<10}]{extra}{fail}")
    print("=== GPU ===")
    for g in out["gpus"]:
        q = " QUARANTINED" if g["quarantined"] else ""
        print(f"  GPU{g['idx']} [{g['status']:<10}] job={g['job']}{q}")
    return 0


def cmd_task(args: argparse.Namespace) -> int:
    """sched task <batch>:<task>: 状态时间线 + 失败原因 + 产物校验."""
    batch, task = _resolve_task_ref(args.task)
    cfg = _load_cfg()
    with state.connect() as conn:
        jobs = conn.execute(
            "SELECT * FROM jobs WHERE batch_id=? AND task_id=? ORDER BY version",
            (batch, task),
        ).fetchall()
        if not jobs:
            print(f"错误: 任务不存在 {batch}:{task}", file=sys.stderr)
            return 1
        for j in jobs:
            print(f"=== {j['id']} ===")
            print(f"  status: {j['status']}")
            print(f"  submitted: {j['submitted_at']}")
            print(f"  started:   {j['started_at'] or '-'}")
            print(f"  finished:  {j['finished_at'] or '-'}")
            dur = "-"
            if j["started_at"] and j["finished_at"]:
                try:
                    t0 = datetime.fromisoformat(j["started_at"])
                    t1 = datetime.fromisoformat(j["finished_at"])
                    dur = f"{(t1 - t0).total_seconds():.0f}s"
                except (ValueError, TypeError):
                    pass
            print(f"  elapsed:   {dur}")
            print(f"  gpu: {j['gpu']}  pgid: {j['pgid']}  retries: {j['retries']}")
            print(f"  rc: {j['rc']}  failure: {j['failure'] or '-'}")
            print(f"  kill_reason: {j['kill_reason'] or '-'}")
            print(f"  git_rev: {j['git_rev'] or '-'}")
            print(f"  log: {state.default_state_dir()}/{state.hostname()}/logs/{batch}/{task}.log")
    return 0


def cmd_history(args: argparse.Namespace) -> int:
    """sched history [batch] [--limit N] [--status s1,s2]: 终态任务历史.

    展示批次名 (非截断 batch_id) + 耗时 + 失败原因; 支持按批次过滤.
    """
    limit = getattr(args, "limit", 50)
    statuses = getattr(args, "status", None)
    with state.connect() as conn:
        batches = conn.execute(
            "SELECT id, name FROM batches ORDER BY created_at DESC"
        ).fetchall()
        name_by_id = {b["id"]: b["name"] for b in batches}
        where = "WHERE status IN ('done','skip','failed','blocked','cancelled','timed_out')"
        params: list = []
        if args.batch:
            b = _batch_id_from_name(args.batch)
            if not b:
                print(f"错误: 批次不存在: {args.batch}", file=sys.stderr)
                return 1
            where += " AND batch_id=?"
            params.append(b)
        if statuses:
            sts = [s.strip() for s in statuses.split(",") if s.strip()]
            if sts:
                where += " AND status IN (%s)" % ",".join("?" * len(sts))
                params.extend(sts)
        rows = conn.execute(
            f"SELECT * FROM jobs {where} ORDER BY finished_at DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        if not rows:
            print("(无历史任务)")
            return 0
        print(f"{'批次':<20} {'任务':<16} {'状态':<10} {'rc':<4} {'耗时':<8} {'gpu':<4} {'失败原因'}")
        for j in rows:
            bname = name_by_id.get(j["batch_id"], j["batch_id"])
            dur = "-"
            if j["started_at"] and j["finished_at"]:
                try:
                    t0 = datetime.fromisoformat(j["started_at"])
                    t1 = datetime.fromisoformat(j["finished_at"])
                    dur = f"{(t1 - t0).total_seconds():.0f}s"
                except (ValueError, TypeError):
                    pass
            rc = "-" if j["rc"] is None else str(j["rc"])
            gpu = "-" if j["gpu"] is None else str(j["gpu"])
            fail = j["failure"] or (j["kill_reason"] or "-")
            print(
                f"  {bname:<18} {j['task_id']:<16} [{j['status']:<8}] {rc:<4} "
                f"{dur:<8} {gpu:<4} {fail}"
            )
    return 0


def cmd_cancel(args: argparse.Namespace) -> int:
    """sched cancel <batch>[:task]: 组级 kill -> cancelled (N2 kill_reason)."""
    ref = args.batch
    if not args.yes:
        print(f"确认取消 {ref}? 加 --yes 执行 (N2: 先写 kill_reason 再 killpg)")
        return 1
    from .executor import Executor

    ex = Executor()
    with state.connect() as conn:
        if ":" in ref:
            # R1: <batch_name>:<task> — batch 段是 name, 解析为最新 id
            b_name, t = _parse_task_ref(ref)
            b = _batch_id_from_name(b_name)
            if not b:
                print(f"错误: 批次不存在: {b_name}", file=sys.stderr)
                return 1
            targets = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND task_id=? AND status='running'",
                (b, t),
            ).fetchall()
        else:
            b = _batch_id_from_name(ref)
            if not b:
                print(f"错误: 批次不存在: {ref}", file=sys.stderr)
                return 1
            targets = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND status='running'",
                (b,),
            ).fetchall()
        if not targets:
            print(f"无运行中任务: {ref}")
            return 0
        for j in targets:
            # O5: killpg 前 kill -0 确认存活; 已死则清 reason
            if j["pgid"] and ex.alive(j["pgid"]):
                state.update_job(conn, j["id"], kill_reason="cancelled")
                ex.kill_pgid(j["pgid"])
                print(f"已取消 {j['id']} (pgid={j['pgid']})")
            else:
                state.update_job(
                    conn, j["id"], status="cancelled", kill_reason="cancelled",
                    finished_at=state.now(),
                )
                print(f"{j['id']} 已自然结束, 标记 cancelled")
    return 0


def _resolve_task_ref(ref: str) -> tuple[str, str]:
    """R1: <batch_name>:<task> -> (batch_id, task). batch 段是 name, 解析为最新 id."""
    b_name, task = _parse_task_ref(ref)
    b = _batch_id_from_name(b_name)
    if not b:
        print(f"错误: 批次不存在: {b_name}", file=sys.stderr)
        raise SystemExit(1)
    return b, task


def cmd_retry(args: argparse.Namespace) -> int:
    """sched retry <batch>:<task>: 解锁 blocked/cancelled/timed_out 重跑 (重置 retries/reason)."""
    batch, task = _resolve_task_ref(args.task)
    with state.connect() as conn:
        j = conn.execute(
            "SELECT * FROM jobs WHERE batch_id=? AND task_id=? ORDER BY version DESC LIMIT 1",
            (batch, task),
        ).fetchone()
        if not j:
            print(f"错误: 任务不存在 {batch}:{task}", file=sys.stderr)
            return 1
        if j["status"] not in ("blocked", "cancelled", "timed_out", "failed"):
            print(f"任务 {j['id']} 状态 {j['status']} 不可 retry")
            return 1
        state.update_job(
            conn, j["id"], status="pending", retries=0, kill_reason=None,
            pgid=None, gpu=None, rc=None, failure=None,
        )
        print(f"已解锁重跑: {j['id']}")
    return 0


def cmd_resubmit(args: argparse.Namespace) -> int:
    """sched resubmit <batch>:<task>: 新版本 Job 排队尾 (A1 force 语义)."""
    batch, task = _resolve_task_ref(args.task)
    with state.connect() as conn:
        j = conn.execute(
            "SELECT * FROM jobs WHERE batch_id=? AND task_id=? ORDER BY version DESC LIMIT 1",
            (batch, task),
        ).fetchone()
        if not j:
            print(f"错误: 任务不存在 {batch}:{task}", file=sys.stderr)
            return 1
        t = conn.execute(
            "SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
            (batch, task, j["version"]),
        ).fetchone()
        spec = json.loads(t["spec"])
        new_v = j["version"] + 1
        # 新版本任务记录 (同 spec) + 新 Job
        state.insert_task(conn, batch, task, new_v, spec, 0)

        from .fingerprint import compute_fingerprint

        fp, stage_fps, rev = compute_fingerprint(
            spec.get("cmd"), spec.get("stages"), spec.get("cwd_abs", "."),
            spec.get("git"), {},
        )
        state.insert_job(
            conn, f"{batch}-{task}-v{new_v}", batch, task, new_v, fp, stage_fps
        )
        # Q4: 检测下游依赖告警
        deps = conn.execute(
            "SELECT name FROM batches WHERE depends_on LIKE ?", (f'%"{batch.split("-")[0]}"%',)
        ).fetchall()
        for d in deps:
            print(f"⚠️ 提示: 批次 '{d['name']}' depends_on 本批次, 上游已更新, 请重提下游 (Q4)")
        print(f"已 resubmit: {batch}:{task} -> v{new_v} (排队尾)")
    return 0


def _tail_n(path: str, n: int) -> list[str]:
    """读取文件最后 n 行 (纯 stdlib, 大文件不整体读入)."""
    lines: list[str] = []
    with open(path, "rb") as f:
        try:
            f.seek(0, 2)  # 到文件尾
            size = f.tell()
            block = 8192
            buf = b""
            while size > 0 and len(lines) < n:
                read = min(block, size)
                size -= read
                f.seek(size)
                buf = f.read(read) + buf
                lines = buf.split(b"\n")
            lines = buf.split(b"\n")
        except OSError:
            return []
    return [l.decode("utf-8", errors="replace") for l in lines if l]


def cmd_log(args: argparse.Namespace) -> int:
    """sched log <batch>:<task> [-f] [-n N]: tail 任务日志 (纯 stdlib, 无 subprocess)."""
    batch, task = _resolve_task_ref(args.task)
    log_path = os.path.join(
        state.default_state_dir(), state.hostname(), "logs", batch, f"{task}.log"
    )
    if not os.path.exists(log_path):
        print(f"日志不存在: {log_path}", file=sys.stderr)
        return 1
    if args.f:
        # 原生 tail -f: 先输出尾部 N 行, 再轮询增量 (Q8: 无读线程竞态, CLI 侧纯读)
        # 管道重定向时必须 flush (Python stdout 块缓冲会吞掉增量)
        try:
            tail = "\n".join(_tail_n(log_path, args.n))
            print(tail, flush=True)
            pos = os.path.getsize(log_path)
            while True:
                time.sleep(1)
                cur = os.path.getsize(log_path)
                if cur > pos:
                    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                        f.seek(pos)
                        print(f.read(), end="", flush=True)
                    pos = cur
                elif cur < pos:
                    pos = 0  # 文件被截断/轮转, 重新从头
        except KeyboardInterrupt:
            return 0
        return 0
    print("\n".join(_tail_n(log_path, args.n)))
    return 0


def cmd_list_gpus(args: argparse.Namespace) -> int:
    with state.connect() as conn:
        rows = conn.execute("SELECT * FROM gpus ORDER BY idx").fetchall()
        for g in rows:
            q = " (QUARANTINED)" if g["quarantined"] else ""
            print(
                f"GPU{g['idx']} [{g['status']:<10}] job={g['job'] or '-'}{q}"
            )
    return 0


def cmd_gpu_ok(args: argparse.Namespace) -> int:
    """解除 quarantine (P2)."""
    with state.connect() as conn:
        conn.execute(
            "UPDATE gpus SET quarantined=0, updated_at=? WHERE idx=?",
            (state.now(), args.idx),
        )
        print(f"GPU{args.idx} 已解除 quarantine")
    return 0


def cmd_gpu_ignore(args: argparse.Namespace) -> int:
    """静默告警 unmanaged 卡 (Q3)."""
    with state.connect() as conn:
        conn.execute(
            "UPDATE gpus SET ignore_until=?, updated_at=? WHERE idx=?",
            (state.now(), state.now(), args.idx),
        )
        print(f"GPU{args.idx} 已忽略告警 (卡仍占用, 不派发)")
    return 0


def cmd_gpu_free(args: argparse.Namespace) -> int:
    """强制回 free (Q3, 需 --yes)."""
    if not args.yes:
        print(f"确认 GPU{args.idx} 无真实外部任务后强制回 free? 加 --yes", file=sys.stderr)
        return 1
    with state.connect() as conn:
        conn.execute(
            "UPDATE gpus SET status='free', job_id=NULL, quarantined=0, updated_at=? WHERE idx=?",
            (state.now(), args.idx),
        )
        print(f"GPU{args.idx} 已强制回 free")
    return 0


def cmd_daemon(args: argparse.Namespace) -> int:
    from . import daemon

    if args.action == "start":
        print(daemon.start(fake=args.fake or False))
    elif args.action == "stop":
        print(daemon.stop())
    elif args.action == "status":
        print(daemon.status_str())
    elif args.action == "check":
        issues = daemon.check(fake=args.fake or False)
        fails = 0
        for i in issues:
            mark = {"ok": "✅", "warn": "⚠️", "fail": "❌"}[i["level"]]
            print(f"  {mark} {i['item']}: {i['detail']}")
            if i["level"] == "fail":
                fails += 1
        print(f"\n{fails} 项 FAIL" if fails else "\n全部通过 ✅")
    return 0


# ---------- 入口 ----------

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="sched", description=f"sched v{__version__} 统一任务调度框架"
    )
    sub = ap.add_subparsers(dest="cmd")

    p = sub.add_parser("init", help="生成 config.json (M0)")
    p.add_argument("--config", help="config.json 路径 (默认 {STATE}/config.json)")
    p.set_defaults(fn=cmd_init)

    p = sub.add_parser("submit", help="提交 batch.json 批次")
    p.add_argument("batch", help="batch.json 路径")
    p.add_argument("--dry-run", action="store_true",
                   help="只预览不入队 (skip 预测 + 依赖就绪 + 展开命令, §G4)")
    p.add_argument("--json", action="store_true", help="dry-run 输出 JSON (供脚本解析)")
    p.set_defaults(fn=cmd_submit)

    p = sub.add_parser("run", help="一行提交单任务 (B14 L1)")
    p.add_argument("--gpus", type=int, default=1, help="申请 GPU 数量 (R5)")
    p.add_argument("--duration", type=int, default=None, help="预计时长(分钟), 超时=2x")
    p.add_argument("--cwd", default=None, help="工作目录 (默认 {ROOT})")
    p.add_argument("--out", default=None, help="产物路径 (声明后 done 需产物存在)")
    p.add_argument("--venv", default=None, help="venv 语义名 (默认 config 第一个)")
    p.add_argument("cmd", nargs=argparse.REMAINDER, help="-- 后的 shell 命令")
    p.set_defaults(fn=cmd_run)

    p = sub.add_parser("status", help="三视图总览")
    p.add_argument("batch", nargs="?", default=None)
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_status)

    p = sub.add_parser("task", help="单任务详情")
    p.add_argument("task", help="<batch>:<task>")
    p.set_defaults(fn=cmd_task)

    p = sub.add_parser("history", help="历史查询")
    p.add_argument("batch", nargs="?", default=None)
    p.add_argument("--limit", type=int, default=50, help="最大行数 (默认 50)")
    p.add_argument("--status", default=None, help="按状态过滤, 逗号分隔 (如 done,failed)")
    p.set_defaults(fn=cmd_history)

    p = sub.add_parser("cancel", help="取消 (组级 kill)")
    p.add_argument("batch", help="<batch> 或 <batch>:<task>")
    p.add_argument("--yes", action="store_true")
    p.set_defaults(fn=cmd_cancel)

    p = sub.add_parser("retry", help="解锁 blocked 重跑")
    p.add_argument("task", help="<batch>:<task>")
    p.set_defaults(fn=cmd_retry)

    p = sub.add_parser("resubmit", help="重新提交 (新版本排队尾)")
    p.add_argument("task", help="<batch>:<task>")
    p.set_defaults(fn=cmd_resubmit)

    p = sub.add_parser("log", help="任务日志")
    p.add_argument("task", help="<batch>:<task>")
    p.add_argument("-f", action="store_true", help="实时跟踪")
    p.add_argument("-n", type=int, default=20)
    p.set_defaults(fn=cmd_log)

    p = sub.add_parser("list-gpus", help="GPU 状态视图")
    p.set_defaults(fn=cmd_list_gpus)

    p = sub.add_parser("gpu-ok", help="解除 quarantine")
    p.add_argument("idx", type=int)
    p.set_defaults(fn=cmd_gpu_ok)

    p = sub.add_parser("gpu-ignore", help="静默告警 unmanaged 卡")
    p.add_argument("idx", type=int)
    p.set_defaults(fn=cmd_gpu_ignore)

    p = sub.add_parser("gpu-free", help="强制回 free (需 --yes)")
    p.add_argument("idx", type=int)
    p.add_argument("--yes", action="store_true")
    p.set_defaults(fn=cmd_gpu_free)

    p = sub.add_parser("daemon", help="daemon 生命周期")
    p.add_argument("action", choices=["start", "stop", "status", "check"])
    p.add_argument("--fake", action="store_true", help="fake-gpu 模式 (P3)")
    p.set_defaults(fn=cmd_daemon)

    args = ap.parse_args(argv)
    if not getattr(args, "fn", None):
        ap.print_help()
        return 1
    # 所有命令先确保建表 (幂等; daemon 侧也建, 双保险)
    try:
        state.init_db()
    except Exception:
        pass
    try:
        return args.fn(args)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
