"""CLI (文档 §4.2 / B9 / R1).

任务级命令统一 <batch>:<task> 双段引用 (R1).
本地 Mac CLI 只做 ssh 跳转; 计算节点上直接执行 (N5 双层结构).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import sqlite3
import sys
import time
import uuid
from datetime import datetime
from typing import Any

from . import state, __version__
from .executor import PROGRESS_RE
from .config import (
    ConfigError,
    config_path,
    default_state_dir,
    load_config,
    parse_gpus,
    resolve_template,
)
from .schema import (
    SchemaError,
    check_dependency_cycle,
    parse_shell_cmd,
    validate_batch,
)
from .templates import expand_cmd


def _is_foreign_host(cfg: dict) -> bool:
    import socket

    node = str(cfg.get("node") or "").strip()
    return bool(node) and socket.gethostname().strip() != node


def _load_cfg():
    try:
        return load_config()
    except ConfigError as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(1)
def _ensure_running_locked() -> str:
    from . import daemon

    return daemon.ensure_running()





def _batch_id_from_name(name: str) -> str | None:
    """batch name -> 最新批次 id (带时间戳). 找不到返回 None."""
    with state.connect() as conn:
        row = conn.execute(
            "SELECT id FROM batches WHERE name=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (name,),
        ).fetchone()
    return row["id"] if row else None


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
    # ---- 通知配置引导 (docs/sched_notify_design.md §3) ----
    notify_on = input("\n启用任务完成通知? (y/N) ").strip().lower()
    if notify_on in ("y", "yes"):
        cfg["notify"] = {
            "on": ["batch_done", "batch_blocked"],
        }
        print("  渠道选择 (可多选, 逗号分隔):")
        print("    1. file   — 写入 inbox (给 LLM agent 读, 推荐)")
        print("    2. email  — 发送邮件 (需 SMTP 配置)")
        print("    3. command — 调用脚本 (给 agent 推送唤醒)")
        channels = input("  选择渠道 [1]: ").strip() or "1"
        for ch in channels.split(","):
            ch = ch.strip()
            if ch == "1":
                cfg["notify"]["file"] = {"enabled": True}
            elif ch == "2":
                smtp_host = input("  SMTP host [smtp.exmail.qq.com]: ").strip() or "smtp.exmail.qq.com"
                smtp_port = int(input("  SMTP port [465]: ").strip() or "465")
                email_to = input("  收件人邮箱: ").strip()
                cfg["notify"]["email"] = {
                    "smtp_host": smtp_host,
                    "smtp_port": smtp_port,
                    "user": input("  发件人邮箱: ").strip(),
                    "password_env": "SCHED_SMTP_PASSWORD",
                    "from": input("  发件人邮箱 (同上): ").strip(),
                    "to": [email_to] if email_to else [],
                }
            elif ch == "3":
                cmd_path = input("  command 脚本路径: ").strip()
                if cmd_path:
                    cfg["notify"]["command"] = [cmd_path]
        print(f"  通知已配置: {list(cfg['notify'].keys())}")
    else:
        print("  通知未启用 (后续可用 sched config-edit 手动添加 notify 段)")

    # M16: 原子写 —— 崩溃不留截断的 config.json (截断会导致 load_config 全线报错)
    tmp_p = p + ".tmp"
    with open(tmp_p, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)
    os.replace(tmp_p, p)
    print(f"已生成 {p}")
    print("下一步: `sched daemon start --check` 跑前置检查 (M0)")
    return 0


def _dry_run_preview(norm: dict, cfg: dict, *, use_state: bool = True) -> dict:
    """§G4 A 类 dry-run: 纯只读预览 (不写 state).

    返回 {"tasks": [{id, cmd_flat, stage_preds, skip, reason}],
          "dep_status": {name: (status, n_done, n_total)}, "git_rev"}.
    skip 预测 = 产物指纹有效 (A2) 且规则校验通过 (D8) -> skip;
    依赖就绪 = depends_on 上游当前状态 (O1 name->id 解析, 一次 SELECT).
    """
    from .artifacts import check_artifact
    from .fingerprint import compute_fingerprint


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
                cmd_e = expand_cmd(s["cmd"], cfg, stage_art, cwd_abs)
                stages_e.append(cmd_e)
                # 指纹 (A2): 展开 cmd + git rev + venv
                fp, _, rev = compute_fingerprint(
                    None, [{"cmd": cmd_e}], cwd_abs, t["git"], venv_paths,
                    runtime_prefix=t.get("runtime_prefix"),
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
            cmd_e = expand_cmd(t["cmd"], cfg, None, cwd_abs)
            fp, _, rev = compute_fingerprint(
                cmd_e, None, cwd_abs, t["git"], venv_paths,
                runtime_prefix=t.get("runtime_prefix"),
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
    if use_state:
        with state.connect() as conn:
            for dep in norm["depends_on"]:
                row = conn.execute(
                    "SELECT id, status FROM batches WHERE name=?"
                    " ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    (dep,),
                ).fetchone()
                if row:
                    dep_status[dep] = row["status"]
                else:
                    dep_status[dep] = "NOT_FOUND"
    else:
        dep_status = {dep: "UNAVAILABLE" for dep in norm["depends_on"]}

    return {
        "tasks": preview_tasks,
        "dep_status": dep_status,
        "git_rev": git_rev,
        "n_skip": n_skip,
        "n_run": n_run,
    }


def _print_dry_run_preview(norm: dict, args: argparse.Namespace, prev: dict, conflict: bool) -> None:
    print(f"=== dry-run: {norm['name']} ({len(norm['tasks'])} 任务, mode={norm['mode']}) ===")
    if conflict:
        print("  ⚠️ 同名批次已有未终态实例 — 实际提交会被定案 6 拒绝")
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
                "UNAVAILABLE": "当前节点无法读取状态, 实际提交由 daemon 重新解析",
            }.get(st, st)
            print(f"  {mark} {dep}: {st} — {note}")
    print("--- 任务预览 ---")
    for pt in prev["tasks"]:
        tag = "SKIP" if pt["skip"] else "RUN "
        print(f"  [{tag}] {pt['id']}")
        for sp in pt["stages"]:
            st = "SKIP" if sp["skip"] else "RUN "
            print(f"      stage{sp['stage']} [{st}] {sp['reason']}")
        flat = pt["cmd_flat"]
        shown = flat[:120] + ("..." if len(flat) > 120 else "")
        print(f"      cmd: {shown}")
    print("--- 汇总 ---")
    print(f"  将跑 {prev['n_run']} / 将 skip {prev['n_skip']} / 共 {len(norm['tasks'])} 任务")
    if prev["git_rev"]:
        print(f"  ⚠️ 预测基于当前 git rev {prev['git_rev'][:12]} (提交前若 pull 代码则预测作废, §G4)")
    if args.json:
        print(json.dumps(prev, ensure_ascii=False, indent=2))


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
    except (SchemaError, ConfigError) as e:
        print(f"校验失败: {e}", file=sys.stderr)
        return 1

    # B12-b: 项目级 colocate 禁用提示 (dry-run 与实提交都看得到)
    _warn_colocate_disabled(norm, cfg)

    # B18: 用户站点包检测提示 (配置了 PYTHONNOUSERSITE 隔离后不再打扰)
    if not (_load_cfg().get("task_default_env") or {}).get("PYTHONNOUSERSITE"):
        import glob as _glob
        _hits = [d for d in _glob.glob(os.path.expanduser(
            "~/.local/lib/python3.*/site-packages")) if os.listdir(d)]
        if _hits:
            print(f"ℹ️ 检测到用户站点包 ({_hits[0]} 非空)。若任务 import 到"
                    "非预期来源的包, 可在 config 设 "
                    'task_default_env.PYTHONNOUSERSITE="1" 隔离')

    # B15: 未声明运行环境的任务 -> 一次性警告
    _unwarn = [t["id"] for t in norm.get("tasks", [])
               if not t.get("runtime") and not any(
                   "{VENV:" in str(c) for c in (t.get("cmd") or []))
               and not any("{VENV:" in str(c) for st in (t.get("stages") or [])
                           for c in (st.get("cmd") or []))]
    if _unwarn:
        print(f"⚠️ 任务 {', '.join(_unwarn)} 未声明运行环境"
              " ({VENV} 或 runtime 字段), 指纹仅含 git rev")

    from datetime import datetime

    # M13: 批次 id 到毫秒 (与 cmd_run 一致) —— 秒级精度下同秒重提/并发 submit
    # 撞主键抛裸 IntegrityError; 毫秒 + IntegrityError 兜底友好报错
    bid = f"{norm['name']}-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}"
    foreign_write = _is_foreign_host(cfg) and not os.environ.get("SCHED_ALLOW_FOREIGN_WRITE")
    if foreign_write:
        bid = f"{bid}-{uuid.uuid4().hex[:12]}"
    dry_run = bool(getattr(args, "dry_run", False))

    # B27/C2: 网关 submit 只投递 inbox 文件; control_requests 由计算节点
    # daemon 每轮扫描后本地写入, 避免 NFS+WAL 跨主机双写。该分支必须
    # 位于所有 state.connect() 之前。
    if foreign_write and not dry_run:
        with state.submission_lock():
            if state.submission_shutdown_active():
                print(
                    "错误: daemon 正在退出，未投递 payload；请先恢复 daemon 后重试",
                    file=sys.stderr,
                )
                return 2
            inbox_dir = state.submission_inbox_dir()
            os.makedirs(inbox_dir, exist_ok=True)
            payload_path = os.path.join(inbox_dir, f"submit-{uuid.uuid4().hex}.json")
            tmp_path = payload_path + ".tmp"
            try:
                with open(tmp_path, "w", encoding="utf-8") as pf:
                    json.dump(
                        {
                            "spec": spec,
                            "bid": bid,
                            "project": norm.get("project"),
                            "tasks": len(norm["tasks"]),
                        },
                        pf,
                        ensure_ascii=False,
                        indent=2,
                    )
                os.replace(tmp_path, payload_path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        print(f"已投递: {bid} ({len(norm['tasks'])} 任务) -> {cfg.get('node')} (inbox)")
        health = _daemon_health()
        heartbeat_age = health.get("heartbeat_age_s")
        tick_age = health.get("tick_ok_age_s")
        if heartbeat_age is None or heartbeat_age > 60:
            print("⚠️ daemon 未运行或心跳已过期；payload 已落 inbox，恢复 daemon 后才会消费")
            print("请先恢复 daemon，再用 sched verify 确认批次入队")
        elif tick_age is None or health.get("frozen"):
            print("⚠️ daemon 心跳存在但调度 tick 未确认完成；请检查 daemon.log 后再用 sched verify")
        else:
            print("由 daemon 扫描消费入队 (下一 tick); sched verify 确认结果")
        return 0

    # 登录节点 dry-run 不能创建/迁移/写入 state.db。依赖状态降级为
    # UNAVAILABLE，实际提交时由 daemon 在同一数据库事务内重新解析。
    if not (foreign_write and dry_run):
        # 依赖 name 存在性 (O1): 提交时解析为最新同 name 批次 id
        with state.connect() as conn:
            for dep in norm["depends_on"]:
                row = conn.execute(
                    "SELECT id FROM batches WHERE name=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    (dep,),
                ).fetchone()
                if not row:
                    print(f"错误: depends_on 引用的批次不存在: '{dep}' (O1)", file=sys.stderr)
                    return 1

        # 依赖环检测 (§3.4e B3): 按 name 拓扑 DFS (当前批次 + 已存在批次全图)
        def _dep_graph() -> dict[str, list[str]]:
            """name -> latest batch's depends_on names (including current batch)."""
            g: dict[str, list[str]] = {norm["name"]: list(norm["depends_on"])}
            latest: dict[str, tuple[str, int, list[str]]] = {}
            with state.connect() as conn:
                rows = conn.execute(
                    "SELECT rowid, name, depends_on, created_at FROM batches"
                ).fetchall()
                for row in rows:
                    key = (row["created_at"] or "", row["rowid"])
                    current = latest.get(row["name"])
                    if current is None or key > current[:2]:
                        latest[row["name"]] = (
                            key[0],
                            key[1],
                            json.loads(row["depends_on"] or "[]"),
                        )
            for name, (_created_at, _rowid, depends_on) in latest.items():
                g.setdefault(name, depends_on)
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

    from .fingerprint import compute_fingerprint
    prepared_tasks = []
    for i, t in enumerate(norm["tasks"]):
        try:
            cmd_e = expand_cmd(t["cmd"], cfg) if t["cmd"] else None
        except (SchemaError, ConfigError) as e:
            print(f"校验失败: {e}", file=sys.stderr)
            return 1
        stages_e = None
        if t["stages"]:
            stage_art: dict[int, dict] = {}
            stages_e = []
            for j, s in enumerate(t["stages"]):
                stage_art[j] = s["artifacts"]
                try:
                    stage_cmd = expand_cmd(s["cmd"], cfg, stage_art, t["cwd_abs"])
                except (SchemaError, ConfigError) as e:
                    print(f"校验失败: {e}", file=sys.stderr)
                    return 1
                stages_e.append(
                    {
                        "cmd": stage_cmd,
                        "artifacts": s["artifacts"],
                        "probes": s.get("probes"),
                        "retry_transform": s.get("retry_transform"),
                        "paths_escape": s.get("paths_escape", False),
                    }
                )
        fp, stage_fps, _rev = compute_fingerprint(
            cmd_e, stages_e, t["cwd_abs"], t["git"], cfg.get("venvs", {}),
            runtime_prefix=t.get("runtime_prefix"),
        )
        prepared_tasks.append((i, t, cmd_e, stages_e, fp, stage_fps))
    if foreign_write and dry_run:
        prev = _dry_run_preview(norm, cfg, use_state=False)
        _print_dry_run_preview(norm, args, prev, conflict=False)
        return 0

    db_context = state.connect() if dry_run else state.submission_connect()
    with db_context as conn:
        # 同名批次未全部终态 -> 拒绝 (定案 6)
        # 决策 7A: dry-run 跳过该检查 —— 纯只读预览不产生副作用, 拦截反而
        # 挡住"现有批次终态后要提交什么"的预览场景; 预览中降级为提示
        existing = conn.execute(
            "SELECT status FROM batches WHERE name=?", (norm["name"],)
        ).fetchall()
        conflict = any(
            b["status"] not in ("done", "blocked", "discarded")
            for b in existing
        )
        if conflict and not dry_run:
            print(
                f"错误: 同名批次 '{norm['name']}' 已有未终态批次 (定案 6),"
                " 请改名或用 sched resubmit",
                file=sys.stderr,
            )
            return 1

        if dry_run:
            prev = _dry_run_preview(norm, cfg)
            _print_dry_run_preview(norm, args, prev, conflict)
            return 0
        try:
            state.insert_batch(
                conn, bid, norm["name"], norm["mode"], norm["depends_on"],
                norm["gpus"], norm["cwd"], norm["env"], norm.get("notify"),
                norm.get("project"), norm.get("priority", 0),
            )
        except sqlite3.IntegrityError:
            # M13: 并发 submit 同时通过定案 6 检查 -> 撞主键, 转友好错误
            print(f"错误: 批次 id 冲突 {bid} (并发提交?), 请重试", file=sys.stderr)
            return 1
        for i, t, cmd_e, stages_e, fp, stage_fps in prepared_tasks:
            # 任务级 spec 已在事务外展开 (M9); 事务内只写规范化结果。
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
                "max_parallel": t.get("max_parallel"),
                # B13: 透传新增任务级字段 (漏传 = 功能静默失效, force_rerun 曾中招)
                "_force_rerun": t.get("_force_rerun"),
                "progress_regex": t.get("progress_regex"),
                "runtime": t.get("runtime"),
                "runtime_prefix": t.get("runtime_prefix"),
            }
            state.insert_task(
                conn, bid, t["id"], 1, spec_json, i,
                norm.get("project"),
            )
            state.insert_job(
                conn, f"{bid}-{t['id']}-v1", bid, t["id"], 1,
                fp, stage_fps, norm.get("project"),
            )
        conn.commit()
        wake_result = _ensure_running_locked()

    # BugFix (2026-08-26, sd_repro_v3 消失事故): "已入队"/ensure_running 此前
    # 在 with 事务块**内部** —— commit 发生在块退出时, 若 ensure_running 抛
    # 异常 (如 NFS 读配置瞬断 -> ConfigError), 整个事务回滚但 "已入队" 已
    # 打印, 用户以为成功实际批次消失。打印必须在提交之后。
    print(f"已入队: {bid} ({len(norm['tasks'])} 任务, mode={norm['mode']})")
    print(wake_result)
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """sched run [flags] -- <cmd>: 一行提交单任务 (B14 L1 / N8 / O9 / P8 / R5)."""
    cfg = _load_cfg()
    # argparse REMAINDER 会把 `--` 分隔符也收进 args.cmd, 剥离之 (N8: `--` 后才是命令)
    cmd_parts = list(args.cmd)
    while cmd_parts and cmd_parts[0] == "--":
        cmd_parts.pop(0)
    shell_cmd = " ".join(cmd_parts)
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
    if venv_name not in venvs:  # M12: 未知名友好报错 (原 KeyError 裸 traceback)
        print(
            f"错误: venv '{venv_name}' 未在 config.venvs 中定义"
            f" (可用: {', '.join(venvs) or '无'})",
            file=sys.stderr,
        )
        return 1
    interp = venvs[venv_name]

    # 批次名到毫秒: 同一秒连续提交不碰撞 (batch.id UNIQUE)
    ts = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
    batch_name = f"run-{tokens[0].split('/')[-1]}-{ts[-6:]}"
    bid = f"{batch_name}-{ts}"

    cwd = args.cwd or "{ROOT}"
    cwd_abs = os.path.realpath(os.path.expanduser(resolve_template(cwd, cfg)))

    # resources: --cpu-only -> gpu:0 (CPU-only, 不占 GPU 槽位); --cpus 记录配额
    # C3 修复: --gpus >1 此前被静默忽略且回显说谎 (schema resources.gpu ∈ {0,1},
    # 多卡未打通 allocator); 直接拒绝, 不再假装支持
    if args.gpus and args.gpus > 1 and not args.cpu_only:
        print(
            "错误: 暂不支持多卡任务 (--gpus 仅接受 1; 多卡训练请用 batch.json 拆多任务)",
            file=sys.stderr,
        )
        return 1
    resources: dict[str, Any] = {}
    if args.cpu_only:
        resources["gpu"] = 0
    if args.cpus:
        resources["cpus"] = args.cpus

    # N8: `--` 后是 shell 字符串, 由 bash -lc 执行 (保持管道/重定向灵活性).
    # 注意: args.cmd 经外层 shell 解析后内层引号已丢失 (argv 传参的固有限制),
    # 这里对每个 token 分别 shlex.quote 再拼接, 重建正确的 shell 语法 ——
    # `python -c "code"` 会变成 `python -c 'code'`, 带空格参数不会被拆散.
    # venv 通过 PATH 注入生效: bash 解析 `python` -> venv/bin/python
    # (executor 另注入 CUDA_VISIBLE_DEVICES: GPU 任务=卡号, CPU-only="")
    venv_bin = os.path.dirname(interp)
    shell_env = {
        "PATH": venv_bin + os.pathsep + os.environ.get("PATH", ""),
        "VIRTUAL_ENV": os.path.dirname(venv_bin),
    }
    shell_cmd_quoted = " ".join(shlex.quote(t) for t in cmd_parts)

    task_spec = {
        "id": "run",
        # bash -lc 执行 shell 字符串 (cmd[0] 非 {VENV:} 模板 —— run 是独立形态, 不走 batch 校验)
        "cmd": ["/bin/bash", "-lc", shell_cmd_quoted],
        "stages": None,
        "cwd_abs": cwd_abs,
        "git": None,
        "env": shell_env,
        "resources": resources,
        "duration_min": args.duration,
        "max_retry": 0,
        "artifacts": (
            {"out": {"path": args.out}} if args.out else {}
        ),
        "retry_transform": None,
        "probes": None,
    }

    from .fingerprint import compute_fingerprint

    if getattr(args, "dry_run", False):
        # §G4 A 类 (与 submit 同一预览路径): 纯只读, 不 insert, 不拉起 daemon
        norm = {
            "name": batch_name,
            "tasks": [task_spec],
            "depends_on": [],
        }
        prev = _dry_run_preview(norm, cfg)
        print(f"=== dry-run: {batch_name} (1 任务, mode=mix) ===")
        for pt in prev["tasks"]:
            tag = "SKIP" if pt["skip"] else "RUN "
            print(f"  [{tag}] {pt['id']}")
            for sp in pt["stages"]:
                st = "SKIP" if sp["skip"] else "RUN "
                print(f"      stage{sp['stage']} [{st}] {sp['reason']}")
            flat = pt["cmd_flat"]
            shown = flat[:120] + ("..." if len(flat) > 120 else "")
            print(f"      cmd: {shown}")
        print("--- 汇总 ---")
        print(f"  将跑 {prev['n_run']} / 将 skip {prev['n_skip']} / 共 1 任务")
        if prev["git_rev"]:
            print(f"  ⚠️ 预测基于当前 git rev {prev['git_rev'][:12]} (提交前若 pull 代码则预测作废, §G4)")
        return 0

    # B11c: run 快捷提交同样强制项目归属 (无 project = 绕过隔离, 拒绝)
    proj = getattr(args, "project", None)
    if not proj:
        known = ", ".join(sorted(cfg.get("projects", {}).keys())) or "无"
        print(
            f"错误: 缺少 --project (B11c 项目隔离, 可选: {known})",
            file=sys.stderr,
        )
        return 1
    if proj not in cfg.get("projects", {}):
        print(f"错误: project '{proj}' 未在 config.projects 中定义", file=sys.stderr)
        return 1

    fp, stage_fps, rev = compute_fingerprint(
        task_spec["cmd"], None, cwd_abs, None, cfg.get("venvs", {})
    )
    with state.submission_connect() as conn:
        state.insert_batch(
            conn, bid, batch_name, "mix", [], None, "{ROOT}", None,
            project=proj,
        )
        state.insert_task(conn, bid, "run", 1, task_spec, 0, proj)
        state.insert_job(conn, f"{bid}-run-v1", bid, "run", 1, fp, stage_fps, proj)
        conn.commit()
        wake_result = _ensure_running_locked()

    res_txt = "cpu-only" if args.cpu_only else "gpu=1"
    print(f"已入队: {bid} ({res_txt}, duration={args.duration}min)")
    print(f"  status/cancel 用批次名: {batch_name}")
    print(wake_result)
    return 0


def _daemon_health() -> dict[str, Any]:
    """B26: 读取 daemon 心跳/tick_ok 文件年龄 (CLI 侧只读文件, 不开 DB)."""
    import os as _os
    from .config import default_state_dir as _dsd
    try:
        cfg = _load_cfg()
    except (Exception, SystemExit):
        cfg = {}
    host = cfg.get("node")
    if not host:
        return {}
    base = _os.path.join(_dsd(), str(host))
    def _age(name: str):
        try:
            return round(max(0.0, time.time() - _os.path.getmtime(_os.path.join(base, name))), 1)
        except OSError:
            return None
    hb_age = _age("daemon.heartbeat")
    tick_age = _age("daemon.tick_ok")
    return {
        "heartbeat_age_s": hb_age,
        "tick_ok_age_s": tick_age,
        # 冻结判定: tick_ok 超 90s 未更新 (阈值同 dispatcher._check_frozen)
        "frozen": bool(tick_age is not None and tick_age > 90),
    }


def cmd_verify(args: argparse.Namespace) -> int:
    """B27: 提交凭证 —— 确认批次已真实持久化 (防吞批假成功)."""
    cfg = _load_cfg()
    name = args.batch.strip()
    with state.connect() as conn:
        rows = conn.execute(
            "SELECT id, name, status, created_at, project FROM batches"
            " WHERE id=? OR name=? ORDER BY created_at DESC LIMIT 3",
            (name, name),
        ).fetchall()
    if not rows:
        print(f"❌ 未找到批次: {name}")
        print("   可能原因: 登录节点直提被守护检查点覆盖; 请在计算节点重提或检查 submit_inbox")
        return 1
    print(f"✅ 批次已持久化:")
    for r in rows:
        print(f"   {r['id']} [{r['status']}] {r['created_at']} project={r['project'] or '-'}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """sched status [batch]: 三视图总览 + --json + --project (B11c)."""
    cfg = _load_cfg()
    out: dict[str, Any] = {"batches": [], "jobs": [], "gpus": []}
    proj_filter = getattr(args, "project", None)
    if proj_filter and proj_filter not in cfg.get("projects", {}):
        known = ", ".join(sorted(cfg.get("projects", {}).keys())) or "无"
        print(f"错误: project 未在 config.projects 中定义: {proj_filter} (可选: {known})",
              file=sys.stderr)
        return 1
    # B26: 调度健康可见性 —— hb=进程活性, tick_ok=主循环真的在完成调度轮
    out["daemon_health"] = _daemon_health()

    with state.connect() as conn:
        batches = conn.execute(
            "SELECT * FROM batches ORDER BY created_at DESC"
        ).fetchall()
        if args.batch and not any(b["name"] == args.batch for b in batches):
            # 与 history/cancel/retry/diag 一致: 批次名不存在要报错而非静默空表
            print(f"错误: 批次不存在: {args.batch}", file=sys.stderr)
            return 1
        for b in batches:
            if args.batch and b["name"] != args.batch:
                continue
            # B11c: 项目过滤 (batches.project 列; NULL = 旧数据/无项目)
            if proj_filter and b["project"] != proj_filter:
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
                    "project": b["project"] if "project" in b.keys() else None,
                }
            )
        jobs = conn.execute("SELECT * FROM jobs ORDER BY rowid").fetchall()
        # batch_id -> name 映射 (显示用, 避免截断 batch_id 丢 name 首字符)
        name_by_id = {b["id"]: b["name"] for b in batches}
        # task spec 的 resources 预加载 (batch_id, task_id, version) -> resources
        # (带 version: resubmit 新版本改了 resources 时, 旧 job 显示旧 spec 的值)
        res_by_task: dict[tuple[str, str, int], dict] = {}
        for r in conn.execute("SELECT batch_id, id, version, spec FROM tasks").fetchall():
            try:
                spec = json.loads(r["spec"])
            except (json.JSONDecodeError, TypeError):
                spec = {}
            res_by_task[(r["batch_id"], r["id"], r["version"])] = spec.get("resources") or {}
        proj_batch_ids = None
        if proj_filter:
            proj_batch_ids = {
                b["id"] for b in conn.execute(
                    "SELECT id FROM batches WHERE project=?", (proj_filter,)
                ).fetchall()
            }
        elif args.batch:
            proj_batch_ids = {
                b["id"] for b in conn.execute(
                    "SELECT id FROM batches WHERE name=?", (args.batch,)
                ).fetchall()
            }
        # B1: 配额排队标记 -- 项目 running GPU 任务数已达 gpu_quota 时,
        # 该项目 pending 任务实际处于"等配额"状态, 视图层显式标注 (不落库)
        running_gpu_by_proj: dict[str, int] = {}
        for r in conn.execute(
            "SELECT project, COUNT(*) AS n FROM jobs"
            " WHERE status='running' AND gpu IS NOT NULL AND project IS NOT NULL"
            " GROUP BY project"
        ):
            running_gpu_by_proj[r["project"]] = r["n"]

        def _quota_wait(j) -> bool:
            proj = j["project"] if "project" in j.keys() else None
            if not proj or j["status"] != "pending":
                return False
            pcfg = cfg.get("projects", {}).get(proj, {})
            quota = int(pcfg.get("gpu_quota", 0) or 0)
            return quota > 0 and running_gpu_by_proj.get(proj, 0) >= quota

        for j in jobs:
            if proj_batch_ids is not None and j["batch_id"] not in proj_batch_ids:
                continue
            res = res_by_task.get((j["batch_id"], j["task_id"], j["version"]), {})
            st = j["status"]
            if st == "pending" and _quota_wait(j):
                st = "pending(quota)"
            out["jobs"].append(
                {
                    "id": j["id"], "batch": j["batch_id"], "task": j["task_id"],
                    "status": st, "gpu": j["gpu"], "version": j["version"],
                    "resources": res,
                    "retries": j["retries"], "failure": j["failure"],
                    "started_at": j["started_at"], "finished_at": j["finished_at"],
                    "progress": (j["progress"] if "progress" in j.keys() else None),
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
        # CPU 配额: running 任务 CPU 占用 (与 dispatcher._task_cpus 同口径)
        cpus_total = cfg.get("cpus_total", 0)
        cpu_used = 0
        for j in conn.execute("SELECT * FROM jobs WHERE status='running'").fetchall():
            res = res_by_task.get((j["batch_id"], j["task_id"], j["version"]), {})
            cpu_used += _task_cpus_of(res, cfg)
        out["cpu"] = {"used": cpu_used, "total": cpus_total}

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
        if j["gpu"] is not None:
            extra = f" gpu={j['gpu']}"
        elif j["resources"].get("gpu", 1) == 0:
            extra = " cpu"
        else:
            extra = ""
        cpus = j["resources"].get("cpus")
        if cpus:
            extra += f" cpus={cpus}"
        # P4: running 任务进度列 (从日志尾部 best-effort 解析 epoch/trial)
        prog = ""
        if j["status"] == "running":
            # B13-§5: progress_regex 解析结果优先, 回退 P4 启发式 (epoch/trial)
            p = j.get("progress") or _job_progress(j["batch"], j["task"], j["version"])
            if p:
                prog = f" {p}"
        fail = f" ({j['failure']})" if j["failure"] else ""
        bname = name_by_id.get(j["batch"], j["batch"])
        print(f"  {bname:<22}:{j['task']:<20} [{j['status']:<10}]{extra}{prog}{fail}")
        if args.detail:
            # P5: 中间档视图 — 每任务起止时间/耗时/version
            t0 = j.get("started_at") or "-"
            t1 = j.get("finished_at") or "-"
            dur = "-"
            if j.get("started_at") and j.get("finished_at"):
                try:
                    a = datetime.fromisoformat(j["started_at"])
                    b = datetime.fromisoformat(j["finished_at"])
                    dur = f"{int((b - a).total_seconds())}s"
                except (ValueError, TypeError):
                    pass
            pj = j.get("progress") or _job_progress(j["batch"], j["task"], j["version"]) \
                if j["status"] == "running" else None
            print(f"      v{j['version']}  start={t0}  end={t1}  耗时={dur}"
                  + (f"  进度={pj}" if pj else ""))
    print("=== GPU ===")
    for g in out["gpus"]:
        q = " QUARANTINED" if g["quarantined"] else ""
        # 多归属展示 (定案 39 E 连带): assigned 卡显示 gpu_jobs 全部 job (共享共存)
        jobs_txt = str(g["job"]) if g["job"] else "None"
        with state.connect() as conn:
            gj = conn.execute(
                "SELECT job_id, vram_gib FROM gpu_jobs WHERE gpu_id=? ORDER BY job_id",
                (g["idx"],),
            ).fetchall()
        if gj:
            # 注意: 不嵌套 f-string (PEP 701 嵌套引号需 Py3.12+, 远程 3.11 兼容)
            jobs_txt = ",".join(
                r["job_id"] + (f"({r['vram_gib']}GiB)" if r["vram_gib"] else "")
                for r in gj
            )
        print(f"  GPU{g['idx']} [{g['status']:<10}] job={jobs_txt}{q}")
    c = out.get("cpu")
    if c:
        total_txt = str(c["total"]) if c["total"] else "未配置"
        print(f"=== CPU ===\n  占用 {c['used']} / {total_txt} 核")
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
            spec = None
            row = conn.execute(
                "SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
                (batch, task, j["version"]),
            ).fetchone()
            if row:
                try:
                    spec = json.loads(row["spec"])
                except (json.JSONDecodeError, TypeError):
                    spec = None
            res = (spec or {}).get("resources") or {}
            gpu_txt = "cpu" if j["gpu"] is None and res.get("gpu", 1) == 0 else j["gpu"]
            cpus_txt = f" cpus={res['cpus']}" if res.get("cpus") else ""
            print(f"  gpu: {gpu_txt}  pgid: {j['pgid']}  retries: {j['retries']}{cpus_txt}")
            print(f"  rc: {j['rc']}  failure: {j['failure'] or '-'}")
            print(f"  kill_reason: {j['kill_reason'] or '-'}")
            print(f"  git_rev: {j['git_rev'] or '-'}")
            print(f"  log: {state.default_state_dir()}/{state.hostname()}/logs/{batch}/{task}-v{j['version']}.log")
    return 0


def cmd_history(args: argparse.Namespace) -> int:
    """sched history [batch] [--limit N] [--status s1,s2]: 终态任务历史.

    展示批次名 (非截断 batch_id) + 耗时 + 失败原因; 支持按批次过滤.
    """
    limit = getattr(args, "limit", 50)
    statuses = getattr(args, "status", None)
    proj_filter = getattr(args, "project", None)
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
        # B11c: 项目过滤 (jobs.project 列, insert_job 起即落库; 旧行 NULL 不命中)
        if proj_filter and proj_filter not in cfg.get("projects", {}):
            known = ", ".join(sorted(cfg.get("projects", {}).keys())) or "无"
            print(f"错误: project 未在 config.projects 中定义: {proj_filter} (可选: {known})",
                  file=sys.stderr)
            return 1
        if statuses:
            sts = [s.strip() for s in statuses.split(",") if s.strip()]
            if sts:
                where += " AND status IN (%s)" % ",".join("?" * len(sts))
                params.extend(sts)
        if proj_filter:
            where += " AND project=?"
            params.append(proj_filter)
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
    """sched cancel <batch>[:task]: 取消批次/任务.

    - running: **转发 daemon 执行 kill** (事故记录 4, 2026-08-17): CLI 在登录
      节点看不到计算节点进程组 (PID namespace, 定案 44 同类), 本地 killpg
      恒失败曾致孤儿占卡 —— 改为写 control_requests 队列, daemon 每轮 tick
      在计算节点本地完成 alive 预检 (O5) + 写 kill_reason + killpg + reap 释放.
    - pending: 排队中未开始, 无进程可杀, 直接标 cancelled (终态, 不重试)
    - 下游依赖告警 (Q4): 上游取消后有 cancelled 终态, 依赖它的批次将永久挂起
    """
    ref = args.batch
    # B13-§6d: 项目级批量取消 —— sched cancel --project <name> --yes
    bulk_proj = getattr(args, "bulk_project", None) or (
        args.project if not ref else None)
    if not ref and bulk_proj:
        if not args.yes:
            print(f"确认取消项目 {bulk_proj} 的全部 active/blocked 批次? 加 --yes 执行")
            return 1
        with state.connect() as conn:
            rows = conn.execute(
                "SELECT id, name FROM batches WHERE project=?"
                " AND status IN ('queued','active','blocked')",
                (bulk_proj,),
            ).fetchall()
        if not rows:
            print(f"项目 {bulk_proj} 无可取消批次")
            return 0
        print(f"项目 {bulk_proj}: {len(rows)} 个批次待取消")
        rc = 0
        for r in rows:
            args.batch = r["id"]   # 以批次 id 精确定位同名实例
            rc = cmd_cancel(args) or rc
        return rc
    if not ref:
        print("用法: sched cancel <batch>[:task] 或 cancel --project <name>", file=sys.stderr)
        return 1
    if not args.yes:
        print(f"确认取消 {ref}? 加 --yes 执行 (转发 daemon: 先写 kill_reason 再 killpg)")
        return 1
    with state.submission_connect() as conn:
        if ":" in ref:
            # R1: <batch_name>:<task> — batch 段是 name, 解析为最新 id
            b_name, t = _parse_task_ref(ref)
            b = _batch_id_from_name(b_name)
            if not b:
                print(f"错误: 批次不存在: {b_name}", file=sys.stderr)
                return 1
            # B11c: --project 归属校验 (同名批次可能属于不同项目)
            proj_req = getattr(args, "project", None)
            if proj_req:
                brow = conn.execute(
                    "SELECT project FROM batches WHERE id=?", (b,)
                ).fetchone()
                if brow and brow["project"] != proj_req:
                    print(f"错误: 批次 {b_name} 属于项目 {brow['project']}, 非 {proj_req}",
                          file=sys.stderr)
                    return 1
            targets = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND task_id=? AND status='running'",
                (b, t),
            ).fetchall()
            pendings = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND task_id=? AND status='pending'",
                (b, t),
            ).fetchall()
        else:
            row = conn.execute("SELECT id FROM batches WHERE id=?", (ref,)).fetchone()
            b = row["id"] if row else _batch_id_from_name(ref)
            if not b:
                print(f"错误: 批次不存在: {ref}", file=sys.stderr)
                return 1
            targets = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND status='running'",
                (b,),
            ).fetchall()
            pendings = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND status='pending'",
                (b,),
            ).fetchall()
        n = 0
        for j in targets:
            # 事故记录 4: 不本地 killpg (登录节点看不到计算节点进程组)。
            # 写控制请求, daemon 在计算节点本地执行 alive 预检 + kill_reason
            # + killpg + reap 释放 GPU。请求幂等: 同一 job 重复 cancel 无副作用。
            state.insert_control_request(conn, j["id"])
            print(f"已转发取消 {j['id']} (daemon 执行 kill, pgid={j['pgid']})")
            n += 1
        for j in pendings:
            # 排队中未启动: 无进程可杀, 直接标终态 (daemon 不再派发)
            state.update_job(
                conn, j["id"], status="cancelled", kill_reason="cancelled",
                finished_at=state.now(),
            )
            print(f"已取消排队任务 {j['id']} (pending, 未启动)")
            n += 1
        if n == 0:
            print(f"无运行中/排队任务: {ref}")
            return 0
        # Q4: 下游依赖告警 (与 resubmit 对称; 上游含 cancelled 终态, 下游永不解锁)
        name = conn.execute(
            "SELECT name FROM batches WHERE id=?", (b,)
        ).fetchone()
        if name:
            deps = conn.execute(
                "SELECT name FROM batches WHERE depends_on LIKE ?",
                (f'%"{name["name"]}"%',),
            ).fetchall()
            for d in deps:
                print(f"⚠️ 提示: 批次 '{d['name']}' depends_on 本批次, 上游已取消, 下游将挂起 (Q4)")
    # 转发后确认: daemon 处理是异步的 (POLL_SEC=10s tick), 等几秒让下一轮
    # tick 完成 alive 预检 + killpg; 不阻塞等待终态 (reap 下一轮才收敛).
    if n > 0:
        if targets:
            health = _daemon_health()
            heartbeat_age = health.get("heartbeat_age_s")
            tick_age = health.get("tick_ok_age_s")
            if heartbeat_age is None or heartbeat_age > 60:
                print("⚠️ daemon 未运行或心跳已过期；取消请求已写入队列，暂不会执行")
            elif tick_age is None or health.get("frozen"):
                print("⚠️ daemon 心跳存在但调度 tick 未确认；请检查 daemon.log 后再复查")
            else:
                print("(daemon 将在下轮 tick 执行 kill, 可用 sched status 复查)")
        else:
            print("(排队任务已直接取消, 可用 sched status 复查)")
    return 0


def _resolve_task_ref(ref: str) -> tuple[str, str]:
    """R1: <batch_name>:<task> -> (batch_id, task). batch 段是 name, 解析为最新 id."""
    b_name, task = _parse_task_ref(ref)
    b = _batch_id_from_name(b_name)
    if not b:
        print(f"错误: 批次不存在: {b_name}", file=sys.stderr)
        raise SystemExit(1)
    return b, task


def _rev_diff_warn(conn, j) -> str | None:
    """P3: job.git_rev vs 当前仓库 rev (任务 cwd) 不一致 -> 返回警告文本.

    retry/resubmit 复用提交时的旧 spec —— 代码更新后重跑的是旧命令,
    lsr infer 数据修复事故 (2026-08-15 事故记录 3) 的直接教训.
    """
    if not j["git_rev"]:
        return None
    row = conn.execute(
        "SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
        (j["batch_id"], j["task_id"], j["version"]),
    ).fetchone()
    if not row:
        return None
    try:
        spec = json.loads(row["spec"])
    except (json.JSONDecodeError, TypeError):
        return None
    cwd = (spec or {}).get("cwd_abs")
    if not cwd:
        return None
    from .fingerprint import _git_rev

    cur = _git_rev(cwd)
    if not cur or cur == j["git_rev"]:
        return None
    return (f"⚠️ 代码已更新 ({j['git_rev'][:12]} -> {cur[:12]}): retry 复用提交时的"
            "旧 spec; 如需新 spec 请用 sched resubmit 或重新提交批次")


def cmd_retry(args: argparse.Namespace) -> int:
    """sched retry <batch>[:task]: 解锁失败终态重跑.

    - <batch>:<task> -> 单任务
    - <batch> (无 :task) -> 批次级: 该批所有 failed/blocked/cancelled/timed_out 任务
    - P3: git_rev 与当前仓库不一致 -> 警告 (retry 复用旧 spec)
    """
    ref = args.task
    with state.submission_connect() as conn:
        if ":" in ref:
            batch, task = _resolve_task_ref(ref)
            targets = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND task_id=? ORDER BY version DESC LIMIT 1",
                (batch, task),
            ).fetchall()
            if not targets:
                print(f"错误: 任务不存在 {batch}:{task}", file=sys.stderr)
                return 1
        else:
            b = _batch_id_from_name(ref)
            if not b:
                print(f"错误: 批次不存在: {ref}", file=sys.stderr)
                return 1
            # B16: discarded 批次不可 retry (任务不会被派发, 防静默挂起)
            bstat = conn.execute(
                "SELECT status FROM batches WHERE id=?", (b,)
            ).fetchone()
            if bstat and bstat["status"] == "discarded":
                print(f"错误: 批次已退役 (discarded), 请用新批次名重新提交",
                      file=sys.stderr)
                return 1
            # C4 修复: 每 task 只取最新 version —— 否则 resubmit 产生的旧版本
            # 失败终态 job 会被复活, 与新版本并发执行写相同产物路径
            targets = conn.execute(
                "SELECT j.* FROM jobs j"
                " JOIN (SELECT task_id, MAX(version) AS mv FROM jobs"
                "       WHERE batch_id=? GROUP BY task_id) t"
                "   ON j.batch_id=? AND j.task_id=t.task_id AND j.version=t.mv"
                " WHERE j.status IN ('blocked','cancelled','timed_out','failed')",
                (b, b),
            ).fetchall()
        if not targets:
            print(f"无失败终态任务: {ref}")
            return 0
        blocked_markers = [
            j for j in targets
            if j["kill_reason"] != "probe"
            and state.launch_marker_active(j["id"])
        ]
        if blocked_markers:
            labels = ", ".join(j["id"] for j in blocked_markers)
            print(
                f"错误: 任务 {labels} 仍有未确认的进程组，请等待 daemon 完成清理后再 retry",
                file=sys.stderr,
            )
            return 1
        n = 0
        for j in targets:
            if j["status"] not in ("blocked", "cancelled", "timed_out", "failed"):
                continue
            w = _rev_diff_warn(conn, j)
            if w:
                print(w)
            termination_pending = (
                j["kill_reason"] == "probe" and j["pgid"] is not None
            )
            state.update_job(
                conn,
                j["id"],
                status="pending",
                retries=0,
                kill_reason=j["kill_reason"] if termination_pending else None,
                pgid=j["pgid"] if termination_pending else None,
                gpu=None,
                rc=None,
                failure=None,
                started_at=None,
                finished_at=None,  # 清陈旧时间戳, pending 期间不显示旧耗时
            )
            print(f"已解锁重跑: {j['id']}")
            n += 1
        print(f"({n} 个任务)")
        conn.commit()
        wake_result = _ensure_running_locked()
    print(wake_result)
    return 0


def cmd_markers(args: argparse.Namespace) -> int:
    """sched markers: 一行查看批次终态 marker (P7).

    daemon 在批次进入终态 (done/blocked) 时写 {STATE}/<hostname>/markers/{name}.{kind}
    (决策 5B 按节点隔离), blocked 解除回 active 时删 .blocked. 按修改时间倒序, 最新在前.
    """
    d = os.path.join(state.default_state_dir(), state.hostname(), "markers")
    if not os.path.isdir(d):
        print("(无 marker — 尚无批次进入终态)")
        return 0
    files = [f for f in os.listdir(d) if f.endswith((".done", ".blocked"))]
    if not files:
        print("(无 marker)")
        return 0
    files.sort(
        key=lambda f: os.path.getmtime(os.path.join(d, f)), reverse=True
    )
    for f in files:
        p = os.path.join(d, f)
        try:
            with open(p, encoding="utf-8") as fh:
                content = fh.read().strip()
        except OSError:
            content = "(不可读)"
        mark = "✅" if f.endswith(".done") else "❌"
        mtime = datetime.fromtimestamp(
            os.path.getmtime(p)
        ).strftime("%m-%d %H:%M")
        print(f"  {mark} {f:<42} {mtime}  {content}")
    return 0


def cmd_resubmit(args: argparse.Namespace) -> int:
    """sched resubmit <batch>[:<task>] [--failed|--all] [--dry-run]: 新版本排队尾.

    - <batch>:<task>      单任务新版本排队尾 (原有语义)
    - <batch> --failed    该批全部失败终态任务 (failed/blocked/timed_out/interrupted;
                          cancelled 属人工决策, 不含 —— 需要时用 :task 单独指定)
    - <batch> --all       该批全部任务
    --dry-run             只列将重跑的清单, 不写入

    批次按名解析为最新实例; discarded 守卫; 完成后自动拉起 idle daemon.
    """
    cfg = _load_cfg()
    if args.failed and args.resubmit_all:
        print("错误: --failed 与 --all 互斥", file=sys.stderr)
        return 1
    ref = args.task
    batch_level = ":" not in ref
    mode = "all" if args.resubmit_all else ("failed" if args.failed else None)
    if batch_level and mode is None:
        print("错误: 批次级 resubmit 需要 --failed 或 --all"
              " (单任务请用 <batch>:<task>)", file=sys.stderr)
        return 1
    if (not batch_level) and mode is not None:
        print("错误: 单任务引用 (<batch>:<task>) 不需要 --failed/--all",
              file=sys.stderr)
        return 1

    if batch_level:
        batch = _batch_id_from_name(ref)
        if not batch:
            print(f"错误: 批次不存在: {ref}", file=sys.stderr)
            return 1
    else:
        batch, task = _resolve_task_ref(ref)

    db_context = state.connect() if args.dry_run else state.submission_connect()
    with db_context as conn:
        bstat = conn.execute(
            "SELECT status FROM batches WHERE id=?", (batch,)
        ).fetchone()
        if bstat and bstat["status"] == "discarded":
            print("错误: 批次已退役 (discarded), 请用新批次名重新提交",
                  file=sys.stderr)
            return 1
        if bstat and bstat["status"] == "queued":
            print("错误: queued 批次 (等上游依赖) 不支持 resubmit;"
                  " 上游完成后批次会自动 active", file=sys.stderr)
            return 1
        bproj = conn.execute(
            "SELECT project, name FROM batches WHERE id=?", (batch,)
        ).fetchone()
        proj = bproj["project"] if bproj else None
        bname = bproj["name"] if bproj else batch

        # 收集目标任务最新版本 job 行
        if batch_level:
            jrows = conn.execute(
                "SELECT j.* FROM jobs j"
                " JOIN (SELECT task_id, MAX(version) AS mv FROM jobs"
                "       WHERE batch_id=? GROUP BY task_id) t"
                "   ON j.batch_id=? AND j.task_id=t.task_id AND j.version=t.mv"
                " ORDER BY j.rowid",
                (batch, batch),
            ).fetchall()
            if mode == "failed":
                jrows = [j for j in jrows if j["status"] in
                         ("failed", "blocked", "timed_out", "interrupted")]
        else:
            jrows = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND task_id=?"
                " ORDER BY version DESC LIMIT 1",
                (batch, task),
            ).fetchall()
        if not jrows:
            msg = ("无匹配任务" if mode == "failed"
                   else f"任务不存在 {batch}:{task}")
            print(f"错误: {msg}", file=sys.stderr)
            return 1
        target_tasks = {j["task_id"] for j in jrows}
        active_targets = [
            j for j in conn.execute(
                "SELECT task_id, status, version FROM jobs"
                " WHERE batch_id=?"
                "   AND status IN ('running','pending','waiting_quota','waiting_dep')",
                (batch,),
            ).fetchall()
            if j["task_id"] in target_tasks
        ]
        marker_targets = [
            j for j in jrows if state.launch_marker_active(j["id"])
        ]
        if marker_targets:
            labels = ", ".join(f"{j['task_id']}v{j['version']}" for j in marker_targets)
            print(
                f"错误: 进程组终止尚未确认完成: {labels}; 请等待 daemon 完成清理",
                file=sys.stderr,
            )
            return 1
        if active_targets:
            labels = ", ".join(
                f"{j['task_id']}[{j['status']}]v{j['version']}"
                for j in active_targets
            )
            print(
                f"错误: 禁止 resubmit 仍在运行/排队的任务: {labels}；"
                "先等待所有版本终态后再提交",
                file=sys.stderr,
            )
            return 1

        if args.dry_run:
            print(f"[dry-run] 将 resubmit {len(jrows)} 个任务 (各生成新版本排队尾):")
            for j in jrows:
                print(f"  {j['task_id']} [{j['status']}] v{j['version']} -> v{j['version'] + 1}")
            return 0

        from .fingerprint import compute_fingerprint

        done_labels = []
        for j in jrows:
            t = conn.execute(
                "SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
                (batch, j["task_id"], j["version"]),
            ).fetchone()
            if not t:
                continue
            spec = json.loads(t["spec"])
            new_v = j["version"] + 1
            state.insert_task(conn, batch, j["task_id"], new_v, spec, 0, proj)
            fp, stage_fps, rev = compute_fingerprint(
                spec.get("cmd"), spec.get("stages"), spec.get("cwd_abs", "."),
                spec.get("git"), cfg.get("venvs", {}),
                runtime_prefix=spec.get("runtime_prefix"),
            )
            state.insert_job(
                conn, f"{batch}-{j['task_id']}-v{new_v}", batch,
                j["task_id"], new_v, fp, stage_fps, proj,
            )
            done_labels.append(f"{j['task_id']}->v{new_v}")

        # Q4: 下游依赖告警 (C5: 按 name 全串匹配防截断漏报)
        deps = conn.execute(
            "SELECT name FROM batches WHERE depends_on LIKE ?", (f'%"{bname}"%',)
        ).fetchall()
        for d in deps:
            print(f"⚠️ 提示: 批次 '{d['name']}' depends_on 本批次, 上游已更新, 请重提下游 (Q4)")
        # B17: 若批次因失败终态被钉在 blocked, 重提交后自动回 active
        # (配合 dispatcher 的"最新版本"settle 口径, 否则旧失败行永久冻结新 pending)
        if bstat and bstat["status"] == "blocked":
            # 复用外层事务连接 —— 嵌套 connect 会 database is locked
            conn.execute(
                "UPDATE batches SET status='active' WHERE id=?", (batch,))
            try:
                mk = os.path.join(default_state_dir(), state.hostname(),
                                  "markers", f"{bname}.blocked")
                if os.path.isfile(mk):
                    os.remove(mk)
            except OSError:
                pass
            print("批次已回 active (旧版本失败终态不再阻塞新版本派发)")

        print(f"已 resubmit {len(done_labels)} 个任务: {', '.join(done_labels)}")
        conn.commit()
        wake_result = _ensure_running_locked()

    print(wake_result)
    return 0


def _task_cpus_of(resources: dict, cfg: dict) -> int:
    """任务 CPU 占用 (与 dispatcher._task_cpus 同口径, status 展示用)."""
    cpus = resources.get("cpus")
    if cpus:
        return int(cpus)
    if resources.get("gpu", 1) == 0:
        return 1
    return int(cfg.get("gpu_job_cpus", 8))


def _job_progress(batch_id: str, task_id: str, version: int) -> str | None:
    """P4: running 任务进度 (从日志尾部解析 epoch/trial, best-effort).

    日志路径与 dispatcher._job_log_path 同构: {STATE}/{host}/logs/{batch}/{task}-v{version}.log
    (审查 L1: 带 version 防 resubmit 新版本覆盖旧日志).
    """
    log_path = os.path.join(
        state.default_state_dir(), state.hostname(), "logs", batch_id,
        f"{task_id}-v{version}.log",
    )
    if not os.path.exists(log_path):
        return None
    lines = _tail_n(log_path, 200)
    for line in reversed(lines):
        m = PROGRESS_RE.search(line)
        if m:
            cur = m.group(1)
            total = m.group(2) or "?"
            return f"{cur}/{total}"
    return None


def _warn_colocate_disabled(norm: dict, cfg: dict) -> None:
    """B12-b: 项目禁用 colocate 时, 对 gpu_share 任务提交期提前告知降级."""
    pc = cfg.get("projects", {}).get(norm.get("project") or "", {})
    if pc.get("colocate") is False and any(
        (t.get("resources") or {}).get("gpu_share") for t in norm.get("tasks", [])
    ):
        print(
            f"⚠️ 项目 {norm['project']} 已禁用 colocate:"
            " gpu_share 任务将按独占运行 (装箱声明被忽略)"
        )


def cmd_clean(args: argparse.Namespace) -> int:
    """sched clean <batch>: 清除批次全部版本的产物指纹 (B13-§4c).

    之后对该批次的 submit/resubmit 不再命中 SKIP, 任务强制重跑.
    不删除产物文件本身 —— 只清"指纹匹配记录"; 需要连产物一起清理时手动删文件.
    """
    b = _batch_id_from_name(args.batch)
    if not b:
        print(f"错误: 批次不存在: {args.batch}", file=sys.stderr)
        return 1
    if not args.yes:
        print(f"确认清除 {b} 的全部产物指纹? 加 --yes 执行")
        return 1
    with state.connect() as conn:
        # 最新版本任务声明的产物文件一并删除 —— 否则产物仍有效时,
        # 新提交的自洽指纹照样 SKIP (B13-§4 语义修正的配套)
        removed = []
        trows = conn.execute(
            "SELECT t.spec FROM tasks t"
            " JOIN (SELECT id, MAX(version) AS mv FROM tasks"
            "       WHERE batch_id=? GROUP BY id) latest"
            "   ON t.batch_id=? AND t.id=latest.id AND t.version=latest.mv",
            (b, b),
        ).fetchall()
        for tr in trows:
            try:
                spec = json.loads(tr["spec"] or "{}")
            except (json.JSONDecodeError, TypeError):
                continue
            cwd = spec.get("cwd_abs") or "."
            for a in (spec.get("artifacts") or {}).values():
                ap = str(a.get("path", ""))
                if not ap:
                    continue
                if not os.path.isabs(ap):
                    ap = os.path.normpath(os.path.join(cwd, ap))
                if os.path.isfile(ap):
                    try:
                        os.remove(ap)
                        removed.append(ap)
                    except OSError:
                        pass
        cur = conn.execute(
            "UPDATE jobs SET fingerprint=NULL, stage_fingerprints=NULL"
            " WHERE batch_id=?",
            (b,),
        )
        n = cur.rowcount
        conn.execute(
            "UPDATE jobs SET status='pending' WHERE batch_id=? AND status='skip'",
            (b,),
        )
    for ap in removed[:10]:
        print(f"  已删产物: {ap}")
    print(f"✅ 已清除 {n} 个任务的指纹并删除 {len(removed)} 个产物 ({b}); 后续将重跑")
    return 0


def _deep_merge(base: dict, patch: dict) -> None:
    """递归深合并 patch 到 base (projects/venvs 等嵌套对象按名合并, 不整体替换)."""
    for k, v in patch.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


def cmd_discard(args: argparse.Namespace) -> int:
    """sched discard <batch>: 退役被取代的 blocked 批次 (backlog#1, B16).

    适用: 同名新实例已取代旧批次, 旧 blocked 实例永久滞留视图的场景.
    行为: 仅翻转批次状态为 discarded; 任务行保持 failed/blocked 原样
    (保留排查证据). discarded 批次不被 settle 复活、不可 retry/resubmit
    (需用新批次名重新提交). 依赖本批次的下游将挂起 —— 与 cancel 同警告.
    """
    if not args.yes:
        print("确认退役? 加 --yes 执行", file=sys.stderr)
        return 1
    # 支持批次名或完整 id (同名多实例需按 id 逐一退役)
    brow = None
    b = args.batch
    with state.connect() as conn:
        row = conn.execute(
            "SELECT * FROM batches WHERE id=?", (args.batch,)
        ).fetchone()
        if row is None:
            row = conn.execute(
                "SELECT * FROM batches WHERE name=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (args.batch,),
            ).fetchone()
            if row is not None:
                b = row["id"]
        brow = row
    if brow is None:
        print(f"错误: 批次不存在: {args.batch}", file=sys.stderr)
        return 1
    with state.connect() as conn:
        if brow is None:
            print(f"错误: 批次不存在: {b}", file=sys.stderr)
            return 1
        if brow["status"] not in ("blocked", "queued"):
            print(f"错误: 仅 blocked/queued 批次可退役 (当前 {brow['status']});"
                    " done 无需退役", file=sys.stderr)
            return 1
        # 仅拒真 running; queued 批次的 pending 由下方统一转 cancelled
        running = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE batch_id=?"
            " AND status='running'", (b,),
        ).fetchone()[0]
        if running:
            print(f"错误: 批次仍有 {running} 个运行中任务,"
                    " 请先 sched cancel", file=sys.stderr)
            return 1
        # queued 批次的 pending 任务一并标记 cancelled (kill_reason 留痕);
        # blocked 批次的失败终态任务保留原状 (排查证据)
        pend = conn.execute(
            "SELECT id FROM jobs WHERE batch_id=? AND status='pending'", (b,),
        ).fetchall()
        for jr in pend:
            state.update_job(
                conn, jr["id"], status="cancelled", kill_reason="discarded",
                finished_at=state.now(),
            )
        n = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE batch_id=?"
            " AND status IN ('failed','blocked','timed_out','interrupted','cancelled')",
            (b,),
        ).fetchone()[0]
        conn.execute(
            "UPDATE batches SET status='discarded' WHERE id=?", (b,))
        # Q4 下游依赖告警 (与 cancel 对称)
        deps = conn.execute(
            "SELECT name FROM batches WHERE depends_on LIKE ?",
            (f'%"{brow["name"]}"%',),
        ).fetchall()
        for d in deps:
            print(f"⚠️ 提示: 批次 '{d['name']}' depends_on 本批次, 已退役, 下游将挂起")
    print(f"✅ 批次 {b} 已退役 (涉及 {n} 个任务, 失败终态证据保留); "
          "重跑请用新批次名提交")
    return 0


def cmd_config_get(args: argparse.Namespace) -> int:
    """sched config get: 输出当前完整配置 (JSON)."""
    print(json.dumps(_load_cfg(), ensure_ascii=False, indent=2))
    return 0


def cmd_config_set(args: argparse.Namespace) -> int:
    """sched config set -f <patch.json> [--yes]: 深合并补丁 -> 校验 -> 原子写 -> 热重载.

    冷键 (node/state_dir/user/schema_version/gpus 卡集与容量) 变更直接拒绝 ——
    与 daemon 侧热更新拒绝逻辑一致 (B12-a)。写入成功后自动写 config_reload
    控制请求, daemon 下个 tick (<10s) 生效, 不受 NFS mtime 缓存延迟影响。
    """
    import copy as _copy

    try:
        with open(args.file, "r", encoding="utf-8") as f:
            patch = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"错误: 读取 patch 失败: {e}", file=sys.stderr)
        return 1
    if not isinstance(patch, dict) or not patch:
        print("错误: patch 必须是非空 JSON 对象", file=sys.stderr)
        return 1
    if not args.yes:
        print("确认写入配置? 加 --yes 执行", file=sys.stderr)
        return 1

    old = _load_cfg()
    new_cfg = _copy.deepcopy(old)
    _deep_merge(new_cfg, patch)

    # 冷键拒绝 (与 dispatcher CONFIG_COLD_KEYS 同口径)
    cold = [k for k in ("node", "state_dir", "user", "schema_version")
            if old.get(k) != new_cfg.get(k)]
    og, ng = parse_gpus(old), parse_gpus(new_cfg)
    if (og[0], og[1]) != (ng[0], ng[1]):
        cold.append("gpus(卡集或容量覆盖)")
    if cold:
        print(f"错误: 含冷键变更 {cold} —— 热更新拒绝, 请手动编辑并重启 daemon",
              file=sys.stderr)
        return 1

    # 全量校验: 写临时文件走 load_config 完整管线 (含 parse_gpus/notify 等)
    cfg_p = config_path()
    tmp_p = cfg_p + ".tmp-set"
    with open(tmp_p, "w", encoding="utf-8") as f:
        json.dump(new_cfg, f, indent=2, ensure_ascii=False)
    try:
        load_config(tmp_p)
    except Exception as e:
        if os.path.exists(tmp_p):
            os.remove(tmp_p)
        print(f"错误: 新配置校验失败 (未写入): {e}", file=sys.stderr)
        return 1
    os.replace(tmp_p, cfg_p)

    with state.connect() as conn:
        state.insert_control_request(conn, "*config*", op="config_reload")
    changed = sorted(set(_flatten_keys(patch)))
    print(f"✅ 配置已写入并请求热重载: {', '.join(changed)}")
    return 0


def _flatten_keys(d: dict, prefix: str = "") -> list[str]:
    out = []
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.extend(_flatten_keys(v, key))
        else:
            out.append(key)
    return out


def cmd_config_reload(args: argparse.Namespace) -> int:
    """sched config reload: 请求 daemon 热更新配置 (B12-a).

    CLI 本地先 load_config() 预校验 —— 语法/结构错误当场报给调用者,
    通过后才写 control_request 让 daemon 下个 tick (<10s) 换配置.
    冷键变更 (node/state_dir/user/gpus 卡集) daemon 会拒绝并提示重启.
    """
    try:
        load_config()
    except Exception as e:
        print(f"配置校验失败 (未发送重载请求): {e}", file=sys.stderr)
        return 1
    with state.connect() as conn:
        req_id = state.insert_control_request(
            conn, "*config*", op="config_reload")
    print(f"✅ 配置校验通过, 重载请求 #{req_id} 已入队 (daemon <10s 内生效)")
    print("   注意: node/state_dir/user/gpus 卡集为冷键, 变更需重启 daemon")
    return 0


def _incident_verdicts(payload: dict) -> list[str]:
    """启发式判读: 只列假设不下结论 (数据会撒谎, 碎片化看起来就像任务太大)."""
    v: list[str] = []
    mem = payload.get("memory") or {}
    ext = mem.get("external_pids") or []
    ext_mem = sum(e.get("mem_mib") or 0 for e in ext) / 1024.0
    if ext and ext_mem >= 1.0:
        pids = ", ".join(str(e.get("pid")) for e in ext[:5])
        v.append(f"疑似调度器外进程挤占显存 (pid {pids}, ~{ext_mem:.1f} GiB)"
                 " —— 检查同卡其他用户")
    elif mem.get("degraded"):
        v.append("物理查询降级: 外部进程可能存在但不可见, 建议人工 nvidia-smi 复核")
    failed = payload.get("failed") or {}
    dpk, fpk = failed.get("declared_vram_gib"), failed.get("profile_peak_gib")
    if dpk and fpk and fpk > dpk * 1.3:
        v.append(f"历史实测峰值 {fpk:.1f} GiB > 声明 {dpk:.1f} GiB"
                 " —— 声明值偏低, 装箱按声明算会低估占用")
    for cr in payload.get("co_runners") or []:
        cd, cp = cr.get("declared_vram_gib"), cr.get("profile_peak_gib")
        if cd and cp and cp > cd * 1.5 and cp - cd > 2.0:
            v.append(f"邻居 {cr.get('task')} 实测峰值 {cp:.1f} >> 声明 {cd:.1f}"
                     " GiB —— 疑似邻居越界挤占")
    actual, packed = mem.get("actual_used_gib"), mem.get("packed_sum_gib")
    if (not ext and actual is not None and packed is not None
            and not mem.get("degraded") and abs(actual - packed) <= packed * 0.2):
        v.append("无外部进程且物理用量≈记账值 —— 任务本身过大或碎片化")
    tl = payload.get("timeline") or []
    if len(tl) >= 4:
        used = [p["used_gib"] for p in tl]
        span = max(used) - min(used)
        cap = mem.get("cap_gib") or 1
        if span > cap * 0.25:
            v.append("事发前显存快速爬升 —— 任务随训练进度增长 (activation 累积类)")
        elif span < cap * 0.05 and max(used) < cap * 0.9:
            v.append("事发前显存平稳 —— 更像瞬时冲击 (外部进程落入 / 瞬时峰值)")
    return v


def _incidents_json(args: argparse.Namespace) -> int:
    """--json 机器可读输出 (dsh 看板契约)."""
    with state.connect() as conn:
        if args.incident_id:
            row = conn.execute(
                "SELECT * FROM incidents WHERE id=?", (args.incident_id,)
            ).fetchone()
            if not row:
                print(json.dumps({"ok": False, "error": "not found"}))
                return 1
            try:
                payload = json.loads(row["payload"])
            except (json.JSONDecodeError, TypeError):
                payload = {}
            print(json.dumps({
                "ok": True,
                "incident": {
                    "id": row["id"], "ts": row["ts"], "kind": row["kind"],
                    "gpu_idx": row["gpu_idx"], "job_id": row["job_id"],
                    "batch_id": row["batch_id"],
                    "payload": payload,
                    "verdicts": _incident_verdicts(payload),
                },
            }, ensure_ascii=False))
            return 0
        q = ("SELECT id, ts, kind, gpu_idx, job_id, batch_id"
             " FROM incidents")
        params: list = []
        conds = []
        if args.job:
            conds.append("job_id=?"); params.append(args.job)
        if args.gpu is not None:
            conds.append("gpu_idx=?"); params.append(args.gpu)
        if conds:
            q += " WHERE " + " AND ".join(conds)
        q += " ORDER BY id DESC LIMIT ?"
        params.append(args.limit)
        rows = conn.execute(q, params).fetchall()
    out = [{"id": r["id"], "ts": r["ts"], "kind": r["kind"],
            "gpu_idx": r["gpu_idx"], "job_id": r["job_id"],
            "batch_id": r["batch_id"]} for r in rows]
    print(json.dumps({"ok": True, "incidents": out}, ensure_ascii=False))
    return 0


def cmd_incidents(args: argparse.Namespace) -> int:
    """sched incidents [id] [--limit N] [--job ID] [--gpu N] [--json]: 事故快照查询 (F2)."""
    if getattr(args, "json", False):
        return _incidents_json(args)
    with state.connect() as conn:
        if args.incident_id:
            row = conn.execute(
                "SELECT * FROM incidents WHERE id=?", (args.incident_id,)
            ).fetchone()
            if not row:
                print(f"错误: incident #{args.incident_id} 不存在", file=sys.stderr)
                return 1
            _diag_incident(row)  # 详情视图复用 diag 渲染 (含判读)
            print()
            try:
                full = json.loads(row["payload"])
            except (json.JSONDecodeError, TypeError):
                return 0
            print("--- 完整 payload ---")
            print(json.dumps(full, ensure_ascii=False, indent=2))
            return 0
        q, params = "SELECT id, ts, kind, gpu_idx, job_id, batch_id FROM incidents", []
        conds = []
        if args.job:
            conds.append("job_id=?"); params.append(args.job)
        if args.gpu is not None:
            conds.append("gpu_idx=?"); params.append(args.gpu)
        if conds:
            q += " WHERE " + " AND ".join(conds)
        q += " ORDER BY id DESC LIMIT ?"
        params.append(args.limit)
        rows = conn.execute(q, params).fetchall()
    if not rows:
        print("无事故快照")
        return 0
    print(f"{'id':>4}  {'ts':<19} {'kind':<9} {'gpu':>3}  job / batch")
    for r in rows:
        print(f"{r['id']:>4}  {r['ts']:<19} {r['kind']:<9}"
              f" {('-' if r['gpu_idx'] is None else r['gpu_idx']):>3}"
              f"  {r['job_id']} / {r['batch_id']}")
    print(f"\n共 {len(rows)} 条; 详情: sched incidents <id>")
    return 0


def cmd_diag(args: argparse.Namespace) -> int:
    """sched diag <batch>[:task]: 一站式失败诊断 (P1).

    - <batch>:<task> -> 单任务
    - <batch> (无 :task) -> 批次级: 所有非 done/skip 任务
    每任务输出: 状态/rc/failure + 实际命令 + git rev 对比 + 日志尾部 15 行.
    """
    ref = args.task
    with state.connect() as conn:
        if ":" in ref:
            batch, task = _resolve_task_ref(ref)
            targets = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND task_id=?"
                " ORDER BY version DESC LIMIT 1",
                (batch, task),
            ).fetchall()
            if not targets:
                print(f"错误: 任务不存在 {batch}:{task}", file=sys.stderr)
                return 1
        else:
            b = _batch_id_from_name(ref)
            if not b:
                print(f"错误: 批次不存在: {ref}", file=sys.stderr)
                return 1
            targets = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND status NOT IN ('done','skip')"
                " ORDER BY rowid",
                (b,),
            ).fetchall()
        if not targets:
            print(f"无异常任务: {ref}")
            return 0
        cfg = _load_cfg()
        for j in targets:
            _diag_one(conn, j, cfg)
    return 0


def _diag_one(conn, j, cfg: dict) -> None:
    """单任务诊断块: 状态 + 命令 + git 对比 + 日志尾部."""
    batch, task = j["batch_id"], j["task_id"]
    print(f"=== {batch}:{task} (v{j['version']}) ===")
    print(f"  status: {j['status']}  rc: {j['rc'] or '-'}  failure: {j['failure'] or '-'}")
    print(f"  retries: {j['retries']}  gpu: {j['gpu'] or '-'}")
    if "runtime" in j.keys() and j["runtime"]:
        print(f"  runtime: {j['runtime']}")
    w = _rev_diff_warn(conn, j)
    if w:
        print(f"  {w}")
    else:
        print(f"  git_rev: {j['git_rev'] or '-'}")
    row = conn.execute(
        "SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
        (batch, task, j["version"]),
    ).fetchone()
    spec: dict = {}
    if row:
        try:
            spec = json.loads(row["spec"])
        except (json.JSONDecodeError, TypeError):
            spec = {}
    for line in _diag_cmds(spec, cfg):
        print(f"  cmd: {line}")
    # B15: 运行时声明展示
    if spec.get("runtime"):
        print(f"  runtime: {json.dumps(spec['runtime'], ensure_ascii=False)}"
              f" -> {spec.get('runtime_prefix', '')}")
    else:
        has_venv = any("{VENV:" in str(c)
                       for c in (spec.get("cmd") or [])) or any(
            "{VENV:" in str(c) for st in (spec.get("stages") or [])
            for c in (st.get("cmd") or []))
        if not has_venv:
            print("  runtime: 未声明")
    log_path = os.path.join(
        state.default_state_dir(), state.hostname(), "logs", batch,
        f"{task}-v{j['version']}.log",  # C1 修复: 与 dispatcher._job_log_path 同构
    )
    print(f"  log: {log_path}")
    # F2: failure=oom/gpu_fault 时附最新事故快照摘要 + 启发式判读
    if (j["failure"] or "") in ("oom", "gpu_fault"):
        inc = state.latest_incident_for_job(conn, j["id"])
        if inc:
            _diag_incident(inc)
        else:
            print("  incident: 无快照 (早于 P1 的失败或采集降级)")
    tail = _tail_n(log_path, 15)
    if tail:
        print("  --- 日志尾部 15 行 ---")
        for l in tail:
            print(f"  | {l}")
    print()


def _diag_incident(inc) -> None:
    """打印单条事故快照摘要 + 判读."""
    try:
        payload = json.loads(inc["payload"])
    except (json.JSONDecodeError, TypeError):
        print("  incident: 快照 payload 解析失败")
        return
    f = payload.get("failed") or {}
    m = payload.get("memory") or {}
    print(f"  incident #{inc['id']} @{inc['ts']} kind={inc['kind']}"
          f" gpu={inc['gpu_idx']} mode={f.get('dispatch_mode')}"
          f"{' [degraded]' if payload.get('degraded') else ''}")
    print(f"    肇事: declared={f.get('declared_vram_gib')}"
          f" profile_peak={f.get('profile_peak_gib')}"
          f" retries={f.get('retries')}")
    print(f"    显存: cap={m.get('cap_gib')} packed={m.get('packed_sum_gib')}"
          f" actual={m.get('actual_used_gib')}")
    for cr in payload.get("co_runners") or []:
        print(f"    邻居: {cr.get('batch')}:{cr.get('task')} [{cr.get('status')}]"
              f" declared={cr.get('declared_vram_gib')}"
              f" peak={cr.get('profile_peak_gib')}"
              f" runtime={cr.get('runtime_sec')}s")
    for e in m.get("external_pids") or []:
        print(f"    外部进程: pid={e.get('pid')}"
              f" mem_mib={e.get('mem_mib', '?')}")
    verdicts = _incident_verdicts(payload)
    if verdicts:
        print("    --- 判读假设 ---")
        for v in verdicts:
            print(f"    ? {v}")
    ex = payload.get("log_excerpt")
    if ex:
        print("    --- 日志摘录 ---")
        for l in ex.splitlines()[-6:]:
            print(f"    | {l}")


def _diag_cmds(spec: dict, cfg: dict) -> list[str]:
    """展示实际命令 (简化展开: {VENV:}->路径, {ROOT}->cwd; stage 引用保留原样)."""
    cwd = spec.get("cwd_abs") or "."
    venvs = cfg.get("venvs", {})

    def expand(tok: str) -> str:
        if tok.startswith("{VENV:") and tok.endswith("}"):
            name = tok[len("{VENV:"):-1]
            return venvs.get(name, tok)
        if tok == "{ROOT}":
            return cwd
        return tok

    out: list[str] = []
    if spec.get("cmd"):
        out.append(" ".join(shlex.quote(expand(t)) for t in spec["cmd"]))
    for i, st in enumerate(spec.get("stages") or []):
        nm = f"stage{i}"  # stage dict 无 name 键, 用索引 (与 executor echo 标记一致)
        cmd = st.get("cmd")
        if cmd:
            out.append(f"[{nm}] " + " ".join(shlex.quote(expand(t)) for t in cmd))
        else:
            out.append(f"[{nm}] (无 cmd)")
    return out or ["(无命令, spec 为空)"]


def _tail_n(path: str, n: int) -> list[str]:
    """读取文件最后 n 行 (纯 stdlib, 大文件不整体读入).

    文件不存在/不可读 -> 返回 [] (diag 对 pending 任务无日志文件不崩).
    """
    lines: list[str] = []
    try:
        with open(path, "rb") as f:
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
    # 审查 L1: 取最新版本构造带版本日志路径 (与 dispatcher._job_log_path 一致)
    with state.connect() as conn:
        row = conn.execute(
            "SELECT version FROM jobs WHERE batch_id=? AND task_id=?"
            " ORDER BY version DESC LIMIT 1",
            (batch, task),
        ).fetchone()
    version = row["version"] if row else 1
    log_path = os.path.join(
        state.default_state_dir(), state.hostname(), "logs", batch,
        f"{task}-v{version}.log",
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
                try:
                    cur = os.path.getsize(log_path)
                except OSError:
                    # 日志被删除 (state 清理/重建): 报错退出, 不再裸 traceback
                    print(f"\n日志已消失: {log_path}", file=sys.stderr, flush=True)
                    return 1
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
    """GPU 状态视图 (2026-08-17 缺口 3: 加显存列; B12-c: 打包数/上限标注)."""
    cfg = _load_cfg()
    _, _, gpu_max_jobs = parse_gpus(cfg)
    cap_all = int(cfg.get("co_locate_max_jobs", 3))
    with state.connect() as conn:
        rows = conn.execute("SELECT * FROM gpus ORDER BY idx").fetchall()
        for g in rows:
            q = " (QUARANTINED)" if g["quarantined"] else ""
            mem = g["mem_total_gib"]
            mem_s = f"{float(mem):.1f}GiB" if mem else "mem=?"
            n = conn.execute(
                "SELECT COUNT(*) FROM gpu_jobs WHERE gpu_id=?", (g["idx"],)
            ).fetchone()[0]
            cap = min([cap_all] + [gpu_max_jobs[g["idx"]]] if g["idx"] in gpu_max_jobs else [cap_all])
            cap_s = f" packed={n}/{cap}"
            if n > cap:
                cap_s += " OVER-CAP(排水中)"
            print(
                f"GPU{g['idx']} [{g['status']:<10}] {mem_s:>8} job={g['job_id'] or '-'}{cap_s}{q}"
            )
    return 0


def cmd_gpu_set_mem(args: argparse.Namespace) -> int:
    """sched gpu-set-mem <idx> <gib>: 运行时覆盖该卡容量 (GiB).

    config.gpus[{idx,mem_gib}] 是启动时覆盖 (daemon 重启后探测覆盖); 本命令
    只更新 state.db/list-gpus, 运行中 daemon 的 allocator 缓存不变;
    重启探测会覆盖该临时值. 如需调度生效请改 config.gpus 后重启 daemon.
    """
    if args.gib <= 0:
        print(f"错误: mem_gib 必须 > 0 (got {args.gib})", file=sys.stderr)
        return 1
    with state.connect() as conn:
        row = conn.execute("SELECT * FROM gpus WHERE idx=?", (args.idx,)).fetchone()
        if not row:
            print(f"错误: GPU{args.idx} 不在配置集 (sched list-gpus 查看)", file=sys.stderr)
            return 1
        conn.execute(
            "UPDATE gpus SET mem_total_gib=? WHERE idx=?", (float(args.gib), args.idx)
        )
        print(f"GPU{args.idx} 容量已写入 state.db/list-gpus: {float(args.gib):.1f} GiB (仅临时记录; 重启探测会覆盖, 调度生效请改 config.gpus)")
    return 0


def cmd_gpu_ok(args: argparse.Namespace) -> int:
    """解除 quarantine (P2)."""
    with state.connect() as conn:
        row = conn.execute("SELECT idx FROM gpus WHERE idx=?", (args.idx,)).fetchone()
        if not row:
            print(f"错误: GPU{args.idx} 不在配置集 (sched list-gpus 查看)", file=sys.stderr)
            return 1
        conn.execute(
            "UPDATE gpus SET quarantined=0, updated_at=? WHERE idx=?",
            (state.now(), args.idx),
        )
        print(f"GPU{args.idx} 已解除 quarantine")
    return 0


def cmd_gpu_ignore(args: argparse.Namespace) -> int:
    """静默告警 unmanaged 卡 (Q3).

    ignore_until 非 NULL = 已人工确认, dispatcher 不再每轮告警 (C2 修复:
    此前该列只写不读, 命令为空操作); 卡恢复 free 时标记自动复位.
    """
    with state.connect() as conn:
        row = conn.execute("SELECT idx FROM gpus WHERE idx=?", (args.idx,)).fetchone()
        if not row:
            print(f"错误: GPU{args.idx} 不在配置集 (sched list-gpus 查看)", file=sys.stderr)
            return 1
        conn.execute(
            "UPDATE gpus SET ignore_until=?, updated_at=? WHERE idx=?",
            (state.now(), state.now(), args.idx),
        )
        print(f"GPU{args.idx} 已忽略告警 (卡仍占用, 不派发; 恢复 free 时自动复位)")
    return 0


def cmd_gpu_free(args: argparse.Namespace) -> int:
    """强制回 free (Q3, 需 --yes)."""
    if not args.yes:
        print(f"确认 GPU{args.idx} 无真实外部任务后强制回 free? 加 --yes", file=sys.stderr)
        return 1
    with state.connect() as conn:
        row = conn.execute("SELECT idx FROM gpus WHERE idx=?", (args.idx,)).fetchone()
        if not row:
            print(f"错误: GPU{args.idx} 不在配置集 (sched list-gpus 查看)", file=sys.stderr)
            return 1
        conn.execute(
            "UPDATE gpus SET status='free', job_id=NULL, quarantined=0,"
            " ignore_until=NULL, updated_at=? WHERE idx=?",
            (state.now(), args.idx),
        )
        # 多归属 (§3.2e): 清该卡 gpu_jobs 残留 (强制回 free 应无挂靠 job)
        conn.execute("DELETE FROM gpu_jobs WHERE gpu_id=?", (args.idx,))
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


# ---------- 通知 (设计 docs/sched_notify_design.md) ----------

def cmd_notify_test(args: argparse.Namespace) -> int:
    """sched notify-test: 发测试通知, 验证 config.notify 各渠道可用."""
    from . import notify

    cfg = _load_cfg()
    ncfg = cfg.get("notify")
    if not ncfg:
        print("config.notify 未配置 (功能关闭); 参见 docs/sched_notify_design.md §3")
        return 1
    event = {
        "event": "batch_done",
        "batch": "notify-test", "batch_id": "-",
        "node": state.hostname(), "git_rev": None,
        "started_at": state.now(), "finished_at": state.now(),
        "duration_min": 0,
        "counts": {"done": 1}, "failures": [],
    }
    results = notify.send(event, cfg)
    if not results:
        print("事件 batch_done 不在 notify.on 列表, 未发送任何渠道")
        return 1
    fails = 0
    for r in results:
        print(f"  {r}")
        if r.startswith("FAIL"):
            fails += 1
    return 1 if fails else 0


def cmd_notify_inbox(args: argparse.Namespace) -> int:
    """sched notify-inbox: 列通知事件 (LLM agent 检查点, 设计 §10 L1)."""
    from . import notify

    files = notify.list_inbox(unacked_only=not args.all)
    if args.json:
        out = []
        for p in files:
            try:
                with open(p, encoding="utf-8") as f:
                    out.append(json.load(f) | {"_file": p})
            except (OSError, json.JSONDecodeError):
                out.append({"_file": p, "_error": "不可读"})
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0
    if not files:
        print("(inbox 空 — 无未读通知事件)")
        return 0
    for p in files:
        try:
            with open(p, encoding="utf-8") as f:
                ev = json.load(f)
            mark = "✅" if ev.get("event") == "batch_done" else "❌"
            n_fail = len(ev.get("failures") or [])
            extra = f" ({n_fail} 失败)" if n_fail else ""
            print(f"  {mark} {os.path.basename(p):<52} {ev.get('batch')}{extra}")
        except (OSError, json.JSONDecodeError):
            print(f"  ⚠️ {os.path.basename(p)} (不可读)")
    print(f"\n共 {len(files)} 条; 处理后确认: sched notify-ack <文件路径>")
    return 0


def cmd_notify_ack(args: argparse.Namespace) -> int:
    """sched notify-ack <文件>: 确认事件 (rename .acked, N 天后自动清理)."""
    from . import notify

    try:
        new = notify.ack(args.file)
    except FileNotFoundError:
        print(f"错误: 事件文件不存在: {args.file}", file=sys.stderr)
        return 1
    print(f"已确认: {new}")
    return 0


# ---------- 入口 ----------

# B11c: 多项目 CLI 命令
def cmd_project_list(args: argparse.Namespace) -> int:
    """sched project list: 列出已配置项目及配额/用量."""
    cfg = _load_cfg()
    projects = cfg.get("projects", {})
    if not projects:
        print("未配置任何项目")
        return 0
    with state.connect() as conn:
        print(f"{'项目':<16} {'GPU配额':<7} {'优先级':<6} {'colocate':<9} "
              f"{'单卡上限':<8} {'亲和卡':<12} {'已用/配额':<10} 根目录")
        for name, pcfg in projects.items():
            quota = pcfg.get("gpu_quota", 0)
            prio = pcfg.get("priority", 0)
            aff = pcfg.get("gpu_affinity", [])
            root = pcfg.get("root", "")
            # B12-b/c: colocate 三态与项目级打包上限可视化
            col = pcfg.get("colocate")
            col_s = "跟随全局" if col is None else ("on" if col else "off")
            mjs = str(pcfg["max_jobs"]) if pcfg.get("max_jobs") else "-"
            used = conn.execute(
                "SELECT COUNT(*) FROM jobs"
                " WHERE status='running' AND gpu IS NOT NULL AND project=?",
                (name,),
            ).fetchone()[0]
            quota_str = f"{used}/{quota}" if quota > 0 else f"{used}/∞"
            print(f"{name:<16} {quota:<7} {prio:<6} {col_s:<9} "
                  f"{mjs:<8} {str(aff):<12} {quota_str:<10} {root}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="sched", description=f"sched v{__version__} 统一任务调度框架"
    )
    sub = ap.add_subparsers(dest="cmd")

    p = sub.add_parser("init", help="生成 config.json (M0)")
    p.add_argument("--config", help="config.json 路径 (默认 {STATE}/config.json)")
    p.set_defaults(fn=cmd_init)

    p = sub.add_parser("verify", help="确认批次已持久化 (提交凭证)")
    p.add_argument("batch", help="批次名或完整 id")
    p.set_defaults(fn=cmd_verify)

    p = sub.add_parser("submit", help="提交 batch.json 批次")
    p.add_argument("batch", help="batch.json 路径")
    p.add_argument("--dry-run", action="store_true",
                   help="只预览不入队 (skip 预测 + 依赖就绪 + 展开命令, §G4)")
    p.add_argument("--json", action="store_true", help="dry-run 输出 JSON (供脚本解析)")
    p.set_defaults(fn=cmd_submit)

    p = sub.add_parser("run", help="一行提交单任务 (B14 L1)")
    p.add_argument("--project", required=True,
                   help="项目名 (B11c 隔离, 必填)")
    p.add_argument("--gpus", type=int, default=1, help="申请 GPU 数量 (R5)")
    p.add_argument("--cpus", type=int, default=None, help="CPU 配额 (记录+status 显示, B4)")
    p.add_argument("--cpu-only", action="store_true",
                   help="CPU-only 任务 (resources.gpu=0, 不占 GPU 槽位)")
    p.add_argument("--duration", type=int, default=None, help="预计时长(分钟), 超过该时长即终止")
    p.add_argument("--cwd", default=None, help="工作目录 (默认 {ROOT})")
    p.add_argument("--out", default=None, help="产物路径 (声明后 done 需产物存在)")
    p.add_argument("--venv", default=None, help="venv 语义名 (默认 config 第一个)")
    p.add_argument("--dry-run", action="store_true",
                   help="预览不提交 (§G4 A 类: 展开命令 + skip 预测, 纯只读)")
    p.add_argument("cmd", nargs=argparse.REMAINDER, help="-- 后的 shell 命令")
    p.set_defaults(fn=cmd_run)

    p = sub.add_parser("status", help="三视图总览")
    p.add_argument("batch", nargs="?", default=None)
    p.add_argument("--json", action="store_true")
    p.add_argument("--detail", action="store_true",
                   help="任务视图含起止时间/耗时/version (P5)")
    p.add_argument("--project", default=None,
                   help="按项目过滤 (B11c)")
    p.set_defaults(fn=cmd_status)

    p = sub.add_parser("task", help="单任务详情")
    p.add_argument("task", help="<batch>:<task>")
    p.set_defaults(fn=cmd_task)

    p = sub.add_parser("history", help="历史查询")
    p.add_argument("batch", nargs="?", default=None)
    p.add_argument("--limit", type=int, default=50, help="最大行数 (默认 50)")
    p.add_argument("--status", default=None, help="按状态过滤, 逗号分隔 (如 done,failed)")
    p.add_argument("--project", default=None, help="按项目过滤 (B11c)")
    p.set_defaults(fn=cmd_history)

    p = sub.add_parser("markers", help="批次终态 marker 一行查看 (P7)")
    p.set_defaults(fn=cmd_markers)

    p = sub.add_parser("cancel", help="取消 (组级 kill)")
    p.add_argument("batch", nargs="?", default=None,
                   help="<batch> 或 <batch>:<task> (与 --project 二选一)")
    p.add_argument("--project", default=None,
                   help="批量取消: 该项目全部 active/blocked 批次 (需 --yes)")
    p.add_argument("--yes", action="store_true")
    p.set_defaults(fn=cmd_cancel)

    p = sub.add_parser("retry", help="解锁 blocked 重跑 (批次级或单任务)")
    p.add_argument("task", help="<batch> 或 <batch>:<task> (批次级=全部失败终态)")
    p.set_defaults(fn=cmd_retry)

    p = sub.add_parser("diag", help="一站式失败诊断 (状态+命令+git+日志)")
    p.add_argument("task", help="<batch> 或 <batch>:<task> (批次级=全部非 done/skip)")
    p.set_defaults(fn=cmd_diag)

    p = sub.add_parser("discard", help="退役被取代的 blocked 批次")
    p.add_argument("batch", help="批次名或 id")
    p.add_argument("--yes", action="store_true", help="确认执行")
    p.set_defaults(fn=cmd_discard)

    p = sub.add_parser("clean", help="清除批次产物指纹 (强制后续重跑)")
    p.add_argument("batch", help="批次名或 id")
    p.add_argument("--yes", action="store_true", help="确认执行")
    p.set_defaults(fn=cmd_clean)

    p = sub.add_parser("config", help="配置管理 (B12-a 热更新)")
    sub_cfg = p.add_subparsers(dest="config_cmd", required=True)
    p_reload = sub_cfg.add_parser("reload", help="请求 daemon 热更新 config.json")
    p_reload.set_defaults(fn=cmd_config_reload)
    sub_cfg.add_parser("get", help="输出当前完整配置 (JSON)").set_defaults(fn=cmd_config_get)
    p_set = sub_cfg.add_parser("set", help="深合并补丁写入配置并触发热重载")
    p_set.add_argument("-f", "--file", required=True, help="patch JSON 文件路径")
    p_set.add_argument("--yes", action="store_true", help="确认执行")
    p_set.set_defaults(fn=cmd_config_set)

    p = sub.add_parser("incidents", help="事故快照查询 (OOM/gpu_fault 现场)")
    p.add_argument("incident_id", nargs="?", type=int,
                   help="指定快照 id 查看详情 (含完整 payload)")
    p.add_argument("--limit", type=int, default=20, help="列表条数 (默认 20)")
    p.add_argument("--job", help="按 job id 过滤")
    p.add_argument("--gpu", type=int, default=None, help="按 GPU 过滤")
    p.add_argument("--json", action="store_true", help="机器可读 JSON 输出 (看板契约)")
    p.set_defaults(fn=cmd_incidents)

    p = sub.add_parser("resubmit", help="重新提交 (新版本排队尾; 支持批次级 --failed/--all)")
    p.add_argument("task", help="<batch>:<task> 或 <batch> (--failed/--all)")
    p.add_argument("--failed", action="store_true",
                   help="批次级: 重跑全部失败终态任务 (failed/blocked/timed_out/interrupted)")
    p.add_argument("--all", dest="resubmit_all", action="store_true",
                   help="批次级: 重跑全部任务")
    p.add_argument("--dry-run", action="store_true", help="只列清单不写入")
    p.set_defaults(fn=cmd_resubmit)

    p = sub.add_parser("log", help="任务日志")
    p.add_argument("task", help="<batch>:<task>")
    p.add_argument("-f", action="store_true", help="实时跟踪")
    p.add_argument("-n", type=int, default=20)
    p.set_defaults(fn=cmd_log)

    p = sub.add_parser("list-gpus", help="GPU 状态视图 (含显存)")
    p.set_defaults(fn=cmd_list_gpus)

    p = sub.add_parser("gpu-set-mem", help="运行时覆盖 GPU 容量 (GiB)")
    p.add_argument("idx", type=int)
    p.add_argument("gib", type=float)
    p.set_defaults(fn=cmd_gpu_set_mem)

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

    p = sub.add_parser("notify-test", help="发测试通知验证 config.notify 配置")
    p.set_defaults(fn=cmd_notify_test)

    p = sub.add_parser("notify-inbox", help="列通知事件 (agent 检查点, 设计 §10)")
    p.add_argument("--all", action="store_true", help="含已确认 (.acked)")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_notify_inbox)

    p = sub.add_parser("notify-ack", help="确认通知事件 (rename .acked)")
    p.add_argument("file", help="事件文件路径 (notify-inbox 列出)")
    p.set_defaults(fn=cmd_notify_ack)

    # B11c: 多项目命令
    p = sub.add_parser("project", help="多项目管理")
    sub_p = p.add_subparsers(dest="project_action", required=True)
    p_list = sub_p.add_parser("list", help="列出项目及配额/用量")
    p_list.set_defaults(fn=cmd_project_list)

    args = ap.parse_args(argv)
    if not getattr(args, "fn", None):
        ap.print_help()
        return 1
    # B24d (2026-08-26, sd_repro_v3 丢失事故): 写操作跨主机执行 = 静默丢数据。
    # state.db 在 NFS 上以 WAL 模式被双主机共享 (网关 CLI 写 + 计算节点 daemon
    # 读/写/检查点), SQLite 官方明确不支持此场景 —— 跨主机锁不可靠时, 网关提交
    # 的事务会被 daemon 的检查点静默抹掉 (已实测 100% 复现)。写操作必须在
    # config.node 所指的计算节点上执行; 违反则拒绝并给出明确指引。
    # B27: submit 不在顶层守卫列表 —— 它有专属 inbox 投递通道 (cmd_submit 内),
    # 在登录节点上会把 spec 落 inbox + 插控制请求行, 由 daemon 消费入库。
    _WRITE_COMMANDS = {
        "run", "cancel", "retry", "resubmit", "discard", "clean",
        "config", "gpu-ok", "gpu-free", "gpu-ignore", "gpu-set-mem",
    }
    is_read_only_config = (
        getattr(args, "cmd", None) == "config"
        and getattr(args, "config_cmd", None) == "get"
    )
    if (
        getattr(args, "cmd", None) in _WRITE_COMMANDS
        and not is_read_only_config
        and not os.environ.get("SCHED_ALLOW_FOREIGN_WRITE")
    ):
        try:
            from .config import load_config as _lc
            import socket as _socket
            _node = str(_lc().get("node") or "")
            if _node and _socket.gethostname() != _node:
                print(
                    f"错误: 写操作 ({args.cmd}) 必须在计算节点 {_node} 上执行,"
                    f" 当前在登录节点 {_socket.gethostname()}。\n"
                    "原因: state.db 经 NFS 双主机共享时 WAL 跨主机锁不可靠,"
                    " 登录节点提交的事务会被 daemon 检查点静默抹掉。\n"
                    f"做法: 进入计算节点会话 (screen/srun) 后再执行 sched {args.cmd}? "
                    f"(确知风险强制继续: SCHED_ALLOW_FOREIGN_WRITE=1)",
                    file=sys.stderr,
                )
                return 2
        except Exception:
            pass  # 配置不可读等场景交由后续正常路径报错
    foreign_submit = False
    if getattr(args, "cmd", None) == "submit" and not os.environ.get("SCHED_ALLOW_FOREIGN_WRITE"):
        try:
            foreign_submit = _is_foreign_host(load_config())
        except Exception:
            pass
    # Gateway submit is file-only; foreign dry-run is also DB-free.
    if not foreign_submit and not is_read_only_config:
        try:
            state.init_db()
        except Exception as e:
            print(f"警告: state DB 初始化失败 ({e}), 后续命令可能报错", file=sys.stderr)
    try:
        return args.fn(args)
    except state.SubmissionBlocked as e:
        print(f"错误: {e}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
