"""CLI (文档 §4.2 / B9 / R1).

任务级命令统一 <batch>:<task> 双段引用 (R1).
本地 Mac CLI 只做 ssh 跳转; 计算节点上直接执行 (N5 双层结构).
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import io
import json
import math
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
from . import artifacts
from .executor import PROGRESS_RE
from .config import (
    ConfigError,
    config_path,
    default_state_dir,
    load_config,
    parse_gpus,
    resolve_template,
    task_environment,
)
from .schema import (
    SchemaError,
    check_dependency_cycle,
    parse_shell_cmd,
    validate_batch,
    validate_persisted_dependencies,
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





def _resolve_batch_ref(ref: str, conn=None) -> str | None:
    """Resolve an exact batch ID first, otherwise the latest matching name."""
    def resolve(db) -> str | None:
        row = db.execute("SELECT id FROM batches WHERE id=?", (ref,)).fetchone()
        if row:
            return row["id"]
        row = db.execute(
            "SELECT id FROM batches WHERE name=?"
            " ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (ref,),
        ).fetchone()
        return row["id"] if row else None

    if conn is not None:
        return resolve(conn)
    with state.connect() as db:
        return resolve(db)


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
    state.ensure_private_directory(os.path.dirname(p) or ".")
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
    # ---- 通知配置引导 (现行配置见 docs/reference.md) ----
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
        print(
            "  通知未启用"
            " (后续可用 sched config set -f patch.json --yes 添加 notify 段)"
        )

    # M16: 原子写 —— 崩溃不留截断的 config.json (截断会导致 load_config 全线报错)
    tmp_p = p + ".tmp"
    with state.open_private_text(tmp_p, "w") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)
    os.replace(tmp_p, p)
    print(f"已生成 {p}")
    print(
        "下一步: 先用 `sched daemon check` 跑前置检查，"
        "再用 `sched daemon start` 启动 (M0)"
    )
    return 0


def _dry_run_preview(norm: dict, cfg: dict, *, use_state: bool = True) -> dict:
    """Build a read-only preview using the same trust boundaries as execution."""
    from .artifacts import check_artifacts
    from .fingerprint import compute_fingerprint

    producer_fps: dict[tuple[str | None, str], str] = {}
    dep_status: dict[str, str] = {}
    if use_state:
        with state.connect() as conn:
            producers = conn.execute(
                "SELECT project, task_id, fingerprint FROM jobs"
                " WHERE status IN ('done','skip') AND fingerprint IS NOT NULL"
                " ORDER BY finished_at DESC, rowid DESC"
            ).fetchall()
            for row in producers:
                key = (row["project"], row["task_id"])
                producer_fps.setdefault(key, row["fingerprint"])
            for dep in norm["depends_on"]:
                row = conn.execute(
                    "SELECT status FROM batches WHERE name=?"
                    " ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    (dep,),
                ).fetchone()
                dep_status[dep] = row["status"] if row else "NOT_FOUND"
    else:
        dep_status = {dep: "UNAVAILABLE" for dep in norm["depends_on"]}

    def predict_task(
        task: dict, fingerprint: str | None, artifacts: dict, cwd_abs: str
    ) -> tuple[bool, str]:
        if task.get("_force_rerun"):
            return False, "force_rerun=true (必跑)"
        if not artifacts:
            return False, "无产物声明 (必跑)"
        producer = producer_fps.get(
            (task.get("project", norm.get("project")), task["id"])
        )
        if not producer or not fingerprint or producer != fingerprint:
            return False, "无可信匹配的 producer fingerprint (必跑)"
        failures: list[str] = []
        for key, result in check_artifacts(
            artifacts, cwd_abs, paths_escape=task.get("paths_escape", False)
        ).items():
            if result is not None:
                failures.append(f"{key}:{result}")
        if failures:
            return False, "产物缺失/无效: " + "; ".join(failures)
        return True, "producer fingerprint 匹配且产物规则通过"

    venv_paths = cfg.get("venvs", {})
    preview_tasks: list[dict[str, Any]] = []
    git_rev: str | None = None
    n_skip = 0
    n_run = 0
    for task in norm["tasks"]:
        cwd_abs = task["cwd_abs"]
        if task["stages"]:
            expanded_stages: list[list[str]] = []
            stage_artifacts: dict[int, dict] = {}
            fingerprint_stages: list[dict] = []
            for index, stage in enumerate(task["stages"]):
                stage_artifacts[index] = stage["artifacts"]
                expanded = expand_cmd(
                    stage["cmd"], cfg, stage_artifacts, cwd_abs
                )
                expanded_stages.append(expanded)
                fingerprint_stages.append({"cmd": expanded, "artifacts": stage["artifacts"]})
            _, _, rev = compute_fingerprint(
                None,
                fingerprint_stages,
                cwd_abs,
                task["git"],
                venv_paths,
                runtime_prefix=task.get("runtime_prefix"),
                execution_env=task_environment(cfg, norm.get("env"), task.get("env")),
                artifacts=task.get("artifacts"),
            )
            git_rev = rev or git_rev
            reason = (
                "force_rerun=true (必跑)"
                if task.get("_force_rerun")
                else "新提交无可信 stage checkpoint sidecar (必跑)"
            )
            stage_preds = [
                {"stage": index, "skip": False, "reason": reason}
                for index in range(len(expanded_stages))
            ]
            n_run += len(stage_preds)
            preview_tasks.append(
                {
                    "id": task["id"],
                    "cmd_flat": " && ".join(
                        " ".join(command) for command in expanded_stages
                    ),
                    "stages": stage_preds,
                    "skip": False,
                }
            )
            continue

        expanded = expand_cmd(task["cmd"], cfg, None, cwd_abs)
        fingerprint, _, rev = compute_fingerprint(
            expanded,
            None,
            cwd_abs,
            task["git"],
            venv_paths,
            runtime_prefix=task.get("runtime_prefix"),
            execution_env=task_environment(cfg, norm.get("env"), task.get("env")),
            artifacts=task.get("artifacts"),
        )
        git_rev = rev or git_rev
        skip, reason = predict_task(
            task, fingerprint, task["artifacts"], cwd_abs
        )
        if skip:
            n_skip += 1
        else:
            n_run += 1
        preview_tasks.append(
            {
                "id": task["id"],
                "cmd_flat": " ".join(expanded),
                "stages": [{"stage": 0, "skip": skip, "reason": reason}],
                "skip": skip,
            }
        )

    return {
        "tasks": preview_tasks,
        "dep_status": dep_status,
        "git_rev": git_rev,
        "n_skip": n_skip,
        "n_run": n_run,
    }


def _print_dry_run_preview(norm: dict, args: argparse.Namespace, prev: dict, conflict: bool) -> None:
    if args.json:
        print(json.dumps(prev, ensure_ascii=False, indent=2))
        return
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

    diagnostic_stream = (
        sys.stderr
        if getattr(args, "dry_run", False) and getattr(args, "json", False)
        else sys.stdout
    )
    _warn_colocate_disabled(norm, cfg, stream=diagnostic_stream)

    # B18: 用户站点包检测提示 (配置了 PYTHONNOUSERSITE 隔离后不再打扰)
    if not (_load_cfg().get("task_default_env") or {}).get("PYTHONNOUSERSITE"):
        import glob as _glob
        _hits = [d for d in _glob.glob(os.path.expanduser(
            "~/.local/lib/python3.*/site-packages")) if os.listdir(d)]
        if _hits:
            print(
                f"ℹ️ 检测到用户站点包 ({_hits[0]} 非空)。若任务 import 到"
                "非预期来源的包, 可在 config 设 "
                'task_default_env.PYTHONNOUSERSITE="1" 隔离',
                file=diagnostic_stream,
            )

    # B15: 未声明运行环境的任务 -> 一次性警告
    _unwarn = [t["id"] for t in norm.get("tasks", [])
               if not t.get("runtime") and not any(
                   "{VENV:" in str(c) for c in (t.get("cmd") or []))
               and not any("{VENV:" in str(c) for st in (t.get("stages") or [])
                           for c in (st.get("cmd") or []))]
    if _unwarn:
        print(
            f"⚠️ 任务 {', '.join(_unwarn)} 未声明运行环境"
            " ({VENV} 或 runtime 字段), 指纹不记录运行环境路径",
            file=diagnostic_stream,
        )

    from datetime import datetime

    # M13: 批次 id 到毫秒 (与 cmd_run 一致) —— 秒级精度下同秒重提/并发 submit
    # 撞主键抛裸 IntegrityError; 毫秒 + IntegrityError 兜底友好报错
    bid = f"{norm['name']}-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}"
    foreign_write = (
        _is_foreign_host(cfg)
        and os.environ.get("SCHED_ALLOW_FOREIGN_WRITE") != "1"
    )
    if foreign_write:
        bid = f"{bid}-{uuid.uuid4().hex[:12]}"
    dry_run = bool(getattr(args, "dry_run", False))
    stateless_dry_run = dry_run and (
        foreign_write or not os.path.isfile(state.db_path())
    )

    # B27/C2: 网关 submit 只投递 inbox 文件; control_requests 由计算节点
    # daemon 每轮扫描后本地写入, 避免 NFS+WAL 跨主机双写。该分支必须
    # 位于所有 state.connect() 之前。
    if foreign_write and not dry_run:
        tmp_path: str | None = None
        payload_path: str | None = None
        renamed = False
        try:
            with state.submission_lock():
                if state.submission_shutdown_active():
                    print(
                        "错误: daemon 正在退出，未投递 payload；请先恢复 daemon 后重试",
                        file=sys.stderr,
                    )
                    return 2
                inbox_dir = state.submission_inbox_dir()
                state.ensure_private_directory(inbox_dir)
                payload_path = os.path.join(
                    inbox_dir,
                    f"submit-{uuid.uuid4().hex}.json",
                )
                tmp_path = payload_path + ".tmp"
                with state.open_private_text(tmp_path, "w") as pf:
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
                    pf.flush()
                    os.fsync(pf.fileno())
                os.replace(tmp_path, payload_path)
                renamed = True
                directory_flags = (
                    os.O_RDONLY
                    | getattr(os, "O_DIRECTORY", 0)
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0)
                )
                inbox_fd = os.open(inbox_dir, directory_flags)
                try:
                    os.fsync(inbox_fd)
                finally:
                    os.close(inbox_fd)
        except Exception as exc:
            if tmp_path is not None and not renamed:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            if renamed:
                detail = f"; payload 保留在 {payload_path} 供诊断"
            else:
                detail = ""
            print(
                f"错误: inbox payload 持久化失败: {exc}{detail}",
                file=sys.stderr,
            )
            return 2
        print(
            f"已投递: {bid} ({len(norm['tasks'])} 任务) "
            f"-> {cfg.get('node')} (inbox)"
        )
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

    # Foreign-host or first-run dry-run cannot create/migrate/write state.db.
    # Dependency and producer state is UNAVAILABLE; the daemon rechecks on submit.
    if not stateless_dry_run:
        with state.connect() as conn:
            try:
                validate_persisted_dependencies(
                    conn,
                    norm["name"],
                    norm["depends_on"],
                )
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
                        "paths_escape": s.get("paths_escape", False),
                    }
                )
        fp, stage_fps, _rev = compute_fingerprint(
            cmd_e, stages_e, t["cwd_abs"], t["git"], cfg.get("venvs", {}),
            runtime_prefix=t.get("runtime_prefix"),
            execution_env=task_environment(cfg, norm.get("env"), t.get("env")),
            artifacts=t.get("artifacts"),
        )
        prepared_tasks.append((i, t, cmd_e, stages_e, fp, stage_fps))
    if stateless_dry_run:
        prev = _dry_run_preview(norm, cfg, use_state=False)
        _print_dry_run_preview(norm, args, prev, conflict=False)
        return 0

    db_context = state.connect() if dry_run else state.submission_connect()
    with db_context as conn:
        if not dry_run:
            try:
                # Fingerprint expansion above may take long enough for another
                # submit to replace a dependency name.  The submission gate is
                # the commit-time authority, so validate the latest graph again.
                validate_persisted_dependencies(
                    conn,
                    norm["name"],
                    norm["depends_on"],
                )
            except SchemaError as e:
                print(f"校验失败: {e}", file=sys.stderr)
                return 1
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
                None, norm["cwd"], norm["env"], norm.get("notify"),
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
                "paths_escape": t.get("paths_escape", False),
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
    wake_deferred = state.defer_after_commit(_ensure_running_locked)
    # BugFix (2026-08-26, sd_repro_v3 消失事故): "已入队"/ensure_running 此前
    # 在 with 事务块**内部** —— commit 发生在块退出时, 若 ensure_running 抛
    # 异常 (如 NFS 读配置瞬断 -> ConfigError), 整个事务回滚但 "已入队" 已
    # 打印, 用户以为成功实际批次消失。打印必须在提交之后。
    print(f"已入队: {bid} ({len(norm['tasks'])} 任务, mode={norm['mode']})")
    if not wake_deferred:
        print(_ensure_running_locked())
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """sched run [flags] -- <cmd>: 一行提交单任务 (B14 L1 / N8 / O9 / P8 / R5)."""
    cfg = _load_cfg()
    # argparse REMAINDER 会把 `--` 分隔符也收进 args.cmd, 剥离之 (N8: `--` 后才是命令)
    cmd_parts = list(args.cmd)
    while cmd_parts and cmd_parts[0] == "--":
        cmd_parts.pop(0)
    shell_cmd = shlex.join(cmd_parts)
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
    if args.gpus is not None and (args.gpus != 1 or args.cpu_only):
        print(
            "错误: --gpus 仅接受 1，且不能与 --cpu-only 同时使用",
            file=sys.stderr,
        )
        return 1
    for flag, value in (("--cpus", args.cpus), ("--duration", args.duration)):
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value <= 0):
            print(f"错误: {flag} 必须是正整数", file=sys.stderr)
            return 1
    proj = getattr(args, "project", None)
    if not proj or proj not in cfg.get("projects", {}):
        print("错误: --project 必须指定 config.projects 中已注册的项目", file=sys.stderr)
        return 1
    resources: dict[str, Any] = {}
    if args.cpu_only:
        resources["gpu"] = 0
    if args.cpus:
        resources["cpus"] = args.cpus

    # Preserve argv quoting and the configured venv PATH. A login shell would
    # source profiles that can silently replace PATH with the system Python.
    # (executor 另注入 CUDA_VISIBLE_DEVICES: GPU 任务=卡号, CPU-only="")
    venv_bin = os.path.dirname(interp)
    shell_env = {
        "PATH": venv_bin + os.pathsep + os.environ.get("PATH", ""),
        "VIRTUAL_ENV": os.path.dirname(venv_bin),
    }
    shell_cmd_quoted = " ".join(shlex.quote(t) for t in cmd_parts)

    task_spec = {
        "id": "run",
        "cmd": ["/bin/bash", "-c", shell_cmd_quoted],
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
        "paths_escape": bool(args.out and os.path.isabs(args.out)),
        "probes": None,
        "project": proj,
    }

    from .fingerprint import compute_fingerprint

    if getattr(args, "dry_run", False):
        # §G4 A 类 (与 submit 同一预览路径): 纯只读, 不 insert, 不拉起 daemon
        norm = {
            "name": batch_name,
            "project": proj,
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

    fp, stage_fps, rev = compute_fingerprint(
        task_spec["cmd"], None, cwd_abs, None, cfg.get("venvs", {}),
        execution_env=task_environment(cfg, None, task_spec.get("env")),
        artifacts=task_spec.get("artifacts"),
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
    _load_cfg()
    reference = args.batch.strip()
    with state.connect() as conn:
        batch_id = _resolve_batch_ref(reference, conn)
        row = conn.execute(
            "SELECT id, name, status, created_at, project FROM batches WHERE id=?",
            (batch_id,),
        ).fetchone() if batch_id else None
    if not row:
        print(f"❌ 未找到批次: {reference}")
        print("   可能原因: 登录节点直提被守护检查点覆盖; 请检查 submit_inbox")
        return 1
    print("✅ 批次已持久化:")
    print(
        f"   {row['id']} [{row['status']}] {row['created_at']}"
        f" project={row['project'] or '-'}"
    )
    return 0


def _encode_status_cursor(rank: int, created_at: str, rowid: int) -> str:
    payload = json.dumps(
        [rank, created_at, rowid],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_status_cursor(value: Any) -> tuple[int, str, int] | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str) or len(value) > 1024:
        raise ValueError("cursor 必须是有界字符串")
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = base64.b64decode(
            padded.encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
        parsed = json.loads(raw.decode("utf-8"))
        rank, created_at, rowid = parsed
    except (
        ValueError,
        TypeError,
        UnicodeError,
        json.JSONDecodeError,
        RecursionError,
    ) as exc:
        raise ValueError("无效 status cursor") from exc
    if (
        isinstance(rank, bool)
        or rank not in (0, 1)
        or not isinstance(created_at, str)
        or not created_at
        or isinstance(rowid, bool)
        or not isinstance(rowid, int)
        or rowid <= 0
    ):
        raise ValueError("无效 status cursor")
    return rank, created_at, rowid


def _encode_status_job_cursor(
    created_at: str,
    batch_rowid: int,
    task_order: int,
    task_id: str,
    job_rowid: int,
) -> str:
    payload = json.dumps(
        [created_at, batch_rowid, task_order, task_id, job_rowid],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_status_job_cursor(
    value: Any,
) -> tuple[str, int, int, str, int] | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str) or len(value) > 2048:
        raise ValueError("job cursor 必须是有界字符串")
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = base64.b64decode(
            padded.encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
        created_at, batch_rowid, task_order, task_id, job_rowid = json.loads(
            raw.decode("utf-8")
        )
    except (
        ValueError,
        TypeError,
        UnicodeError,
        json.JSONDecodeError,
        RecursionError,
    ) as exc:
        raise ValueError("无效 status job cursor") from exc
    integers = (batch_rowid, task_order, job_rowid)
    if (
        not isinstance(created_at, str)
        or not created_at
        or not isinstance(task_id, str)
        or not task_id
        or any(isinstance(item, bool) or not isinstance(item, int) for item in integers)
        or batch_rowid <= 0
        or task_order < 0
        or job_rowid <= 0
    ):
        raise ValueError("无效 status job cursor")
    return created_at, batch_rowid, task_order, task_id, job_rowid


def cmd_status(args: argparse.Namespace) -> int:
    """sched status [batch]: bounded, coherent latest-version current state."""
    cfg = _load_cfg()
    limit = max(1, min(1000, int(getattr(args, "limit", 200))))
    out: dict[str, Any] = {
        "schema_version": 1,
        "limit": limit,
        "batches": [],
        "jobs": [],
        "gpus": [],
        "truncated": {"batches": False, "jobs": False},
        "next_cursor": None,
        "next_job_cursor": None,
    }
    try:
        batch_cursor = _decode_status_cursor(
            getattr(args, "cursor", None)
        )
        job_cursor = _decode_status_job_cursor(
            getattr(args, "job_cursor", None)
        )
    except ValueError as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 1
    proj_filter = getattr(args, "project", None)
    if proj_filter and proj_filter not in cfg.get("projects", {}):
        known = ", ".join(sorted(cfg.get("projects", {}).keys())) or "无"
        print(
            f"错误: project 未在 config.projects 中定义: {proj_filter}"
            f" (可选: {known})",
            file=sys.stderr,
        )
        return 1
    out["daemon_health"] = _daemon_health()
    gpu_assignments: dict[int, list] = {}

    with state.connect() as conn:
        # Pin every query below to one SQLite view even when this command is
        # called in writer mode by an in-process client.
        conn.execute("BEGIN")
        selected_batch_id = None
        if getattr(args, "batch", None):
            selected_batch_id = _resolve_batch_ref(args.batch, conn)
            if not selected_batch_id:
                print(f"错误: 批次不存在: {args.batch}", file=sys.stderr)
                return 1

        state_rank_sql = (
            "CASE WHEN b.status IN ('done','discarded','cancelled') THEN 1 ELSE 0 END"
        )
        batch_where: list[str] = []
        batch_params: list[Any] = []
        if selected_batch_id:
            batch_where.append("b.id=?")
            batch_params.append(selected_batch_id)
        if proj_filter:
            batch_where.append("b.project=?")
            batch_params.append(proj_filter)
        if batch_cursor is not None:
            if selected_batch_id:
                print(
                    "错误: 指定批次时不能同时使用 --cursor",
                    file=sys.stderr,
                )
                return 1
            rank, created_at, rowid = batch_cursor
            batch_where.append(
                f"({state_rank_sql}>? OR "
                f"({state_rank_sql}=? AND "
                "(b.created_at<? OR "
                "(b.created_at=? AND b.rowid<?))))"
            )
            batch_params.extend(
                [rank, rank, created_at, created_at, rowid]
            )
        where_sql = (
            " WHERE " + " AND ".join(batch_where) if batch_where else ""
        )
        batch_rows = conn.execute(
            "SELECT b.*,"
            f" {state_rank_sql} AS state_rank,"
            " b.rowid AS batch_rowid,"
            " (SELECT COUNT(*) FROM jobs j"
            "   WHERE j.batch_id=b.id"
            "     AND j.version=(SELECT MAX(j2.version) FROM jobs j2"
            "       WHERE j2.batch_id=j.batch_id AND j2.task_id=j.task_id))"
            " AS current_jobs,"
            " (SELECT COUNT(*) FROM jobs j"
            "   WHERE j.batch_id=b.id AND j.status IN ('done','skip')"
            "     AND j.version=(SELECT MAX(j2.version) FROM jobs j2"
            "       WHERE j2.batch_id=j.batch_id AND j2.task_id=j.task_id))"
            " AS completed_jobs"
            " FROM batches b"
            + where_sql
            + f" ORDER BY {state_rank_sql} ASC,"
            " b.created_at DESC, b.rowid DESC LIMIT ?",
            (*batch_params, limit + 1),
        ).fetchall()
        out["truncated"]["batches"] = len(batch_rows) > limit
        batch_rows = batch_rows[:limit]
        if out["truncated"]["batches"] and batch_rows:
            last_batch = batch_rows[-1]
            out["next_cursor"] = _encode_status_cursor(
                int(last_batch["state_rank"]),
                str(last_batch["created_at"]),
                int(last_batch["batch_rowid"]),
            )
        visible_batch_ids = [row["id"] for row in batch_rows]
        name_by_id = {row["id"]: row["name"] for row in batch_rows}
        for batch in batch_rows:
            batch_id = batch["id"]
            batch_name = batch["name"]
            out["batches"].append(
                {
                    "id": batch_id,
                    "name": batch_name,
                    "batch_id": batch_id,
                    "batch_name": batch_name,
                    "mode": batch["mode"],
                    "status": batch["status"],
                    "depends_on": json.loads(batch["depends_on"] or "[]"),
                    "progress": (
                        f"{batch['completed_jobs']}/{batch['current_jobs']}"
                    ),
                    "project": batch["project"],
                    "revision": int(batch["revision"]),
                }
            )

        latest_jobs = []
        if visible_batch_ids:
            placeholders = ",".join("?" for _ in visible_batch_ids)
            job_cursor_sql = ""
            job_cursor_params: list[Any] = []
            if job_cursor is not None:
                (
                    job_created_at,
                    job_batch_rowid,
                    job_task_order,
                    job_task_id,
                    job_rowid,
                ) = job_cursor
                task_order_sql = "COALESCE(t.order_idx, 2147483647)"
                job_cursor_sql = (
                    " WHERE (b.created_at<?"
                    " OR (b.created_at=? AND b.rowid<?)"
                    f" OR (b.created_at=? AND b.rowid=? AND {task_order_sql}>?)"
                    f" OR (b.created_at=? AND b.rowid=? AND {task_order_sql}=?"
                    " AND j.task_id>?)"
                    f" OR (b.created_at=? AND b.rowid=? AND {task_order_sql}=?"
                    " AND j.task_id=? AND j.rowid>?))"
                )
                job_cursor_params = [
                    job_created_at,
                    job_created_at,
                    job_batch_rowid,
                    job_created_at,
                    job_batch_rowid,
                    job_task_order,
                    job_created_at,
                    job_batch_rowid,
                    job_task_order,
                    job_task_id,
                    job_created_at,
                    job_batch_rowid,
                    job_task_order,
                    job_task_id,
                    job_rowid,
                ]
            latest_jobs = conn.execute(
                "WITH latest AS ("
                " SELECT batch_id, task_id, MAX(version) AS version"
                " FROM jobs WHERE batch_id IN ("
                + placeholders
                + ") GROUP BY batch_id, task_id)"
                " SELECT j.*, t.spec AS task_spec, t.order_idx AS task_order,"
                " b.created_at AS job_batch_created_at,"
                " b.rowid AS job_batch_rowid, j.rowid AS job_rowid,"
                " COALESCE(t.order_idx, 2147483647) AS job_task_order"
                " FROM jobs j JOIN latest l"
                " ON j.batch_id=l.batch_id AND j.task_id=l.task_id"
                " AND j.version=l.version"
                " LEFT JOIN tasks t ON t.batch_id=j.batch_id"
                " AND t.id=j.task_id AND t.version=j.version"
                " JOIN batches b ON b.id=j.batch_id"
                + job_cursor_sql
                + " ORDER BY b.created_at DESC, b.rowid DESC,"
                " COALESCE(t.order_idx, 2147483647), j.task_id, j.rowid"
                " LIMIT ?",
                (*visible_batch_ids, *job_cursor_params, limit + 1),
            ).fetchall()
        out["truncated"]["jobs"] = len(latest_jobs) > limit
        latest_jobs = latest_jobs[:limit]
        if out["truncated"]["jobs"] and latest_jobs:
            last_job = latest_jobs[-1]
            out["next_job_cursor"] = _encode_status_job_cursor(
                str(last_job["job_batch_created_at"]),
                int(last_job["job_batch_rowid"]),
                int(last_job["job_task_order"]),
                str(last_job["task_id"]),
                int(last_job["job_rowid"]),
            )

        running_gpu_by_project = {
            row["project"]: row["n"]
            for row in conn.execute(
                "WITH latest AS ("
                " SELECT batch_id, task_id, MAX(version) AS version"
                " FROM jobs GROUP BY batch_id, task_id)"
                " SELECT j.project, COUNT(*) AS n FROM jobs j JOIN latest l"
                " ON j.batch_id=l.batch_id AND j.task_id=l.task_id"
                " AND j.version=l.version"
                " WHERE j.status='running' AND j.gpu IS NOT NULL"
                " AND j.project IS NOT NULL GROUP BY j.project"
            ).fetchall()
        }

        def quota_wait(job, resources: dict) -> bool:
            project = job["project"]
            if (
                not project
                or job["status"] != "pending"
                or resources.get("gpu", 1) == 0
            ):
                return False
            project_cfg = cfg.get("projects", {}).get(project, {})
            quota = int(project_cfg.get("gpu_quota", 0) or 0)
            return (
                quota > 0
                and running_gpu_by_project.get(project, 0) >= quota
            )

        for job in latest_jobs:
            try:
                task_spec = json.loads(job["task_spec"] or "{}")
            except (json.JSONDecodeError, TypeError):
                task_spec = {}
            resources = task_spec.get("resources") or {}
            stored_status = job["status"]
            wait_reason = None
            status = stored_status
            if stored_status == "waiting_quota":
                status, wait_reason = "pending", "quota"
            elif stored_status == "waiting_dep":
                status, wait_reason = "pending", "dependency"
            elif quota_wait(job, resources):
                wait_reason = "quota"
            batch_id = job["batch_id"]
            out["jobs"].append(
                {
                    "id": job["id"],
                    "batch_id": batch_id,
                    "batch_name": name_by_id.get(batch_id, batch_id),
                    "task": job["task_id"],
                    "status": status,
                    "wait_reason": wait_reason,
                    "gpu": job["gpu"],
                    "version": job["version"],
                    "resources": resources,
                    "retries": job["retries"],
                    "failure": job["failure"],
                    "started_at": job["started_at"],
                    "finished_at": job["finished_at"],
                    "progress": (
                        job["progress"] if "progress" in job.keys() else None
                    ),
                }
            )

        for assignment in conn.execute(
            "SELECT gpu_id, job_id, vram_gib FROM gpu_jobs"
            " ORDER BY gpu_id, job_id"
        ).fetchall():
            gpu_assignments.setdefault(assignment["gpu_id"], []).append(
                {
                    "job_id": assignment["job_id"],
                    "vram_gib": assignment["vram_gib"],
                }
            )
        for gpu in conn.execute("SELECT * FROM gpus ORDER BY idx").fetchall():
            out["gpus"].append(
                {
                    "idx": gpu["idx"],
                    "status": gpu["status"],
                    "job": gpu["job_id"],
                    "quarantined": gpu["quarantined"],
                    "revision": int(gpu["revision"]),
                    "assignments": gpu_assignments.get(gpu["idx"], []),
                }
            )

        cpu_used = 0
        running_specs = conn.execute(
            "WITH latest AS ("
            " SELECT batch_id, task_id, MAX(version) AS version"
            " FROM jobs GROUP BY batch_id, task_id)"
            " SELECT t.spec FROM jobs j JOIN latest l"
            " ON j.batch_id=l.batch_id AND j.task_id=l.task_id"
            " AND j.version=l.version"
            " LEFT JOIN tasks t ON t.batch_id=j.batch_id"
            " AND t.id=j.task_id AND t.version=j.version"
            " WHERE j.status='running'"
        ).fetchall()
        for row in running_specs:
            try:
                task_spec = json.loads(row["spec"] or "{}")
            except (json.JSONDecodeError, TypeError):
                task_spec = {}
            cpu_used += _task_cpus_of(task_spec.get("resources") or {}, cfg)
        out["cpu"] = {"used": cpu_used, "total": cfg.get("cpus_total", 0)}

    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0

    print("=== 批次 ===")
    for batch in out["batches"]:
        print(
            f"  {batch['name']:<28} [{batch['status']:<8}]"
            f" {batch['progress']:<6} dep={batch['depends_on']}"
        )
    print("=== 任务 ===")
    for job in out["jobs"]:
        if job["gpu"] is not None:
            extra = f" gpu={job['gpu']}"
        elif job["resources"].get("gpu", 1) == 0:
            extra = " cpu"
        else:
            extra = ""
        cpus = job["resources"].get("cpus")
        if cpus:
            extra += f" cpus={cpus}"
        progress = ""
        if job["status"] == "running":
            parsed = job.get("progress") or _job_progress(
                job["batch_id"], job["task"], job["version"]
            )
            if parsed:
                progress = f" {parsed}"
        status_text = job["status"]
        if job["wait_reason"]:
            status_text += f"({job['wait_reason']})"
        failure = f" ({job['failure']})" if job["failure"] else ""
        print(
            f"  {job['batch_name']:<22}:{job['task']:<20}"
            f" [{status_text:<10}]{extra}{progress}{failure}"
        )
        if args.detail:
            started = job.get("started_at") or "-"
            finished = job.get("finished_at") or "-"
            duration = "-"
            if job.get("started_at") and job.get("finished_at"):
                try:
                    start = datetime.fromisoformat(job["started_at"])
                    end = datetime.fromisoformat(job["finished_at"])
                    duration = f"{int((end - start).total_seconds())}s"
                except (ValueError, TypeError):
                    pass
            print(
                f"      v{job['version']}  start={started}  end={finished}"
                f"  耗时={duration}"
                + (f"  进度={progress.strip()}" if progress else "")
            )
    print("=== GPU ===")
    for gpu in out["gpus"]:
        quarantined = " QUARANTINED" if gpu["quarantined"] else ""
        jobs_text = str(gpu["job"]) if gpu["job"] else "None"
        assignments = gpu_assignments.get(gpu["idx"], [])
        if assignments:
            jobs_text = ",".join(
                row["job_id"]
                + (f"({row['vram_gib']}GiB)" if row["vram_gib"] else "")
                for row in assignments
            )
        print(
            f"  GPU{gpu['idx']} [{gpu['status']:<10}]"
            f" job={jobs_text}{quarantined}"
        )
    cpu = out.get("cpu")
    if cpu:
        total_text = str(cpu["total"]) if cpu["total"] else "未配置"
        print(f"=== CPU ===\n  占用 {cpu['used']} / {total_text} 核")
    return 0


def cmd_task(args: argparse.Namespace) -> int:
    """Return one task's version timeline in human or stable JSON form."""
    batch, task = _resolve_task_ref(args.task)
    output: dict[str, Any] = {
        "schema_version": 1,
        "batch_id": batch,
        "batch_name": batch,
        "batch_revision": 0,
        "task": task,
        "jobs": [],
    }
    with state.connect() as conn:
        conn.execute("BEGIN")
        batch_row = conn.execute(
            "SELECT name, revision FROM batches WHERE id=?",
            (batch,),
        ).fetchone()
        if batch_row is not None:
            output["batch_name"] = batch_row["name"]
            output["batch_revision"] = int(batch_row["revision"])
        jobs = conn.execute(
            "SELECT * FROM jobs WHERE batch_id=? AND task_id=? ORDER BY version",
            (batch, task),
        ).fetchall()
        if not jobs:
            print(f"错误: 任务不存在 {batch}:{task}", file=sys.stderr)
            return 1
        for job in jobs:
            spec = None
            row = conn.execute(
                "SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
                (batch, task, job["version"]),
            ).fetchone()
            if row:
                try:
                    spec = json.loads(row["spec"])
                except (json.JSONDecodeError, TypeError):
                    spec = None
            resources = (spec or {}).get("resources") or {}
            duration_seconds = None
            if job["started_at"] and job["finished_at"]:
                try:
                    started = datetime.fromisoformat(job["started_at"])
                    finished = datetime.fromisoformat(job["finished_at"])
                    duration_seconds = (finished - started).total_seconds()
                except (ValueError, TypeError):
                    pass
            log_path = (
                f"{state.default_state_dir()}/{state.hostname()}/logs/"
                f"{batch}/{task}-v{job['version']}.log"
            )
            output["jobs"].append(
                {
                    "id": job["id"],
                    "status": job["status"],
                    "version": job["version"],
                    "submitted_at": job["submitted_at"],
                    "started_at": job["started_at"],
                    "finished_at": job["finished_at"],
                    "duration_seconds": duration_seconds,
                    "gpu": job["gpu"],
                    "pgid": job["pgid"],
                    "retries": job["retries"],
                    "rc": job["rc"],
                    "failure": job["failure"],
                    "kill_reason": job["kill_reason"],
                    "git_rev": job["git_rev"],
                    "resources": resources,
                    "spec": spec,
                    "log": log_path,
                }
            )

    if getattr(args, "json", False):
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    for job in output["jobs"]:
        print(f"=== {job['id']} ===")
        print(f"  status: {job['status']}")
        print(f"  submitted: {job['submitted_at']}")
        print(f"  started:   {job['started_at'] or '-'}")
        print(f"  finished:  {job['finished_at'] or '-'}")
        elapsed = (
            f"{job['duration_seconds']:.0f}s"
            if job["duration_seconds"] is not None
            else "-"
        )
        print(f"  elapsed:   {elapsed}")
        resources = job["resources"]
        gpu_text = (
            "cpu"
            if job["gpu"] is None and resources.get("gpu", 1) == 0
            else job["gpu"]
        )
        cpus_text = (
            f" cpus={resources['cpus']}" if resources.get("cpus") else ""
        )
        print(
            f"  gpu: {gpu_text}  pgid: {job['pgid']}"
            f"  retries: {job['retries']}{cpus_text}"
        )
        print(f"  rc: {job['rc']}  failure: {job['failure'] or '-'}")
        print(f"  kill_reason: {job['kill_reason'] or '-'}")
        print(f"  git_rev: {job['git_rev'] or '-'}")
        print(f"  log: {job['log']}")
    return 0


def _encode_history_cursor(finished_at: str, rowid: int) -> str:
    payload = json.dumps(
        [finished_at, rowid],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_history_cursor(value: Any) -> tuple[str, int] | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str) or len(value) > 1024:
        raise ValueError("cursor 必须是有界字符串")
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = base64.b64decode(
            padded.encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
        parsed = json.loads(raw.decode("utf-8"))
        finished_at, rowid = parsed
    except (
        ValueError,
        TypeError,
        UnicodeError,
        json.JSONDecodeError,
        RecursionError,
    ) as exc:
        raise ValueError("无效 history cursor") from exc
    if (
        not isinstance(finished_at, str)
        or isinstance(rowid, bool)
        or not isinstance(rowid, int)
        or rowid <= 0
    ):
        raise ValueError("无效 history cursor")
    return finished_at, rowid


def cmd_history(args: argparse.Namespace) -> int:
    """Return bounded terminal task history in human or stable JSON form."""
    cfg = _load_cfg()
    limit = max(1, min(200, int(getattr(args, "limit", 50))))
    statuses = getattr(args, "status", None)
    project = getattr(args, "project", None)
    as_json = bool(getattr(args, "json", False))
    try:
        history_cursor = _decode_history_cursor(
            getattr(args, "cursor", None)
        )
    except ValueError as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 1
    if project and project not in cfg.get("projects", {}):
        known = ", ".join(sorted(cfg.get("projects", {}))) or "无"
        print(
            f"错误: project 未在 config.projects 中定义: {project}"
            f" (可选: {known})",
            file=sys.stderr,
        )
        return 1

    with state.connect() as conn:
        where = (
            "WHERE j.status IN"
            " ('done','skip','failed','blocked','cancelled','timed_out','interrupted')"
        )
        params: list[Any] = []
        if getattr(args, "batch", None):
            batch_id = _resolve_batch_ref(args.batch, conn)
            if not batch_id:
                print(f"错误: 批次不存在: {args.batch}", file=sys.stderr)
                return 1
            where += " AND j.batch_id=?"
            params.append(batch_id)
        if statuses:
            selected = [
                status.strip() for status in statuses.split(",") if status.strip()
            ]
            if selected:
                where += " AND j.status IN (%s)" % ",".join("?" * len(selected))
                params.extend(selected)
        if project:
            where += " AND j.project=?"
            params.append(project)
        if history_cursor is not None:
            finished_at, rowid = history_cursor
            where += (
                " AND (COALESCE(j.finished_at,'')<?"
                " OR (COALESCE(j.finished_at,'')=? AND j.rowid<?))"
            )
            params.extend([finished_at, finished_at, rowid])
        rows = conn.execute(
            "SELECT j.*, b.name AS batch_name,"
            " j.rowid AS job_rowid,"
            " COALESCE(j.finished_at,'') AS history_finished_at"
            f" FROM jobs j JOIN batches b ON b.id=j.batch_id {where}"
            " ORDER BY COALESCE(j.finished_at,'') DESC, j.rowid DESC"
            " LIMIT ?",
            (*params, limit + 1),
        ).fetchall()

    truncated = len(rows) > limit
    page_rows = rows[:limit]
    next_cursor = None
    if truncated and page_rows:
        last_job = page_rows[-1]
        next_cursor = _encode_history_cursor(
            str(last_job["history_finished_at"]),
            int(last_job["job_rowid"]),
        )
    history = []
    for job in page_rows:
        duration_seconds = None
        if job["started_at"] and job["finished_at"]:
            try:
                started = datetime.fromisoformat(job["started_at"])
                finished = datetime.fromisoformat(job["finished_at"])
                duration_seconds = (finished - started).total_seconds()
            except (ValueError, TypeError):
                pass
        batch_id = job["batch_id"]
        history.append(
            {
                "id": job["id"],
                "batch_id": batch_id,
                "batch_name": job["batch_name"],
                "task": job["task_id"],
                "status": job["status"],
                "version": job["version"],
                "rc": job["rc"],
                "gpu": job["gpu"],
                "started_at": job["started_at"],
                "finished_at": job["finished_at"],
                "duration_seconds": duration_seconds,
                "failure": job["failure"] or job["kill_reason"],
            }
        )

    if as_json:
        print(
            json.dumps(
                {
                    "schema_version": 1,
                    "history": history,
                    "limit": limit,
                    "truncated": truncated,
                    "next_cursor": next_cursor,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if not history:
        print("(无历史任务)")
        return 0
    print(
        f"{'批次':<20} {'任务':<16} {'状态':<10} {'rc':<4}"
        f" {'耗时':<8} {'gpu':<4} {'失败原因'}"
    )
    for row in history:
        duration = (
            "-"
            if row["duration_seconds"] is None
            else f"{row['duration_seconds']:.0f}s"
        )
        rc = "-" if row["rc"] is None else str(row["rc"])
        gpu = "-" if row["gpu"] is None else str(row["gpu"])
        failure = row["failure"] or "-"
        print(
            f"  {row['batch_name']:<18} {row['task']:<16}"
            f" [{row['status']:<8}] {rc:<4} {duration:<8} {gpu:<4} {failure}"
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
            print(
                f"确认取消项目 {bulk_proj} 的全部 queued/active/blocked 批次?"
                " 加 --yes 执行"
            )
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
        # Classification and mutation must be one writer transaction.  Without
        # this early claim the daemon can launch a selected pending job before
        # we mark it cancelled, leaving an untracked process and GPU lease.
        if not conn.in_transaction:
            conn.execute("BEGIN IMMEDIATE")
        if ":" in ref:
            # R1: <batch_name>:<task> — batch 段是 name, 解析为最新 id
            b_name, t = _parse_task_ref(ref)
            b = _resolve_batch_ref(b_name, conn)
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
                "SELECT * FROM jobs WHERE batch_id=? AND task_id=?"
                " AND status IN ('pending','waiting_quota','waiting_dep')",
                (b, t),
            ).fetchall()
        else:
            b = _resolve_batch_ref(ref, conn)
            if not b:
                print(f"错误: 批次不存在: {ref}", file=sys.stderr)
                return 1
            targets = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND status='running'",
                (b,),
            ).fetchall()
            pendings = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=?"
                " AND status IN ('pending','waiting_quota','waiting_dep')",
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
            changed = conn.execute(
                "UPDATE jobs SET status='cancelled', kill_reason='cancelled',"
                " finished_at=? WHERE id=?"
                " AND status IN ('pending','waiting_quota','waiting_dep')",
                (state.now(), j["id"]),
            ).rowcount
            if changed != 1:
                continue
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


def _resolve_task_ref(ref: str, conn=None) -> tuple[str, str]:
    """Resolve <batch-id-or-latest-name>:<task> to its canonical pair."""
    batch_ref, task = _parse_task_ref(ref)
    batch = _resolve_batch_ref(batch_ref, conn)
    if not batch:
        print(f"错误: 批次不存在: {batch_ref}", file=sys.stderr)
        raise SystemExit(1)
    return batch, task


def _same_name_nonterminal_conflict(conn, batch_id: str):
    """Return another same-name batch that already owns runnable lifecycle.

    All callers hold the submission gate.  This is the reverse half of the
    submit-side same-name check: an older terminal instance must not be reopened
    after a newer instance was accepted first.
    """
    return conn.execute(
        "SELECT other.id, other.status FROM batches target"
        " JOIN batches other ON other.name=target.name AND other.id<>target.id"
        " WHERE target.id=?"
        " AND other.status NOT IN ('done','blocked','discarded')"
        " ORDER BY other.created_at DESC, other.rowid DESC LIMIT 1",
        (batch_id,),
    ).fetchone()


def _print_same_name_reopen_conflict(action: str, conflict) -> None:
    print(
        f"错误: {action} 拒绝重开旧批次；同名批次 "
        f"{conflict['id']} 已处于非终态 {conflict['status']}"
        "，请先等待其收敛或取消/退役后重试",
        file=sys.stderr,
    )


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
    reopened = False
    with state.submission_connect() as conn:
        # Prevent daemon settlement from changing the batch between the status
        # snapshot below and the first job mutation.  request-bound commands
        # already own an IMMEDIATE outer transaction, so do not nest BEGIN.
        if not conn.in_transaction:
            conn.execute("BEGIN IMMEDIATE")
        if ":" in ref:
            batch, task = _resolve_task_ref(ref, conn)
        else:
            batch = _resolve_batch_ref(ref, conn)
            if not batch:
                print(f"错误: 批次不存在: {ref}", file=sys.stderr)
                return 1
            task = None
        batch_row = conn.execute(
            "SELECT status FROM batches WHERE id=?",
            (batch,),
        ).fetchone()
        if not batch_row:
            print(f"错误: 批次不存在: {ref}", file=sys.stderr)
            return 1
        if batch_row["status"] == "discarded":
            print(
                "错误: 批次已退役 (discarded), 请用新批次名重新提交",
                file=sys.stderr,
            )
            return 1
        if task is not None:
            targets = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND task_id=?"
                " ORDER BY version DESC LIMIT 1",
                (batch, task),
            ).fetchall()
            if not targets:
                print(f"错误: 任务不存在 {batch}:{task}", file=sys.stderr)
                return 1
        else:
            # C4: revive only each task's latest generation.
            targets = conn.execute(
                "SELECT j.* FROM jobs j"
                " JOIN (SELECT task_id, MAX(version) AS mv FROM jobs"
                "       WHERE batch_id=? GROUP BY task_id) t"
                "   ON j.batch_id=? AND j.task_id=t.task_id AND j.version=t.mv"
                " WHERE j.status IN ('blocked','cancelled','timed_out','failed')",
                (batch, batch),
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
        conflict = _same_name_nonterminal_conflict(conn, batch)
        if conflict is not None:
            _print_same_name_reopen_conflict("retry", conflict)
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
        if n and batch_row["status"] == "blocked":
            conn.execute(
                "UPDATE batches SET status='active' WHERE id=?",
                (batch,),
            )
            reopened = True
        print(f"({n} 个任务)")

    wake_deferred = state.defer_after_commit(_ensure_running_locked)
    if not wake_deferred:
        print(_ensure_running_locked())
    if reopened:
        print("批次已回 active (终态 marker 将由 daemon 在派发前协调)")
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
    """Create new task versions after preparing every fingerprint off-transaction."""
    cfg = _load_cfg()
    if args.failed and args.resubmit_all:
        print("错误: --failed 与 --all 互斥", file=sys.stderr)
        return 1
    ref = args.task
    batch_level = ":" not in ref
    mode = "all" if args.resubmit_all else ("failed" if args.failed else None)
    if batch_level and mode is None:
        print(
            "错误: 批次级 resubmit 需要 --failed 或 --all"
            " (单任务请用 <batch>:<task>)",
            file=sys.stderr,
        )
        return 1
    if not batch_level and mode is not None:
        print(
            "错误: 单任务引用 (<batch>:<task>) 不需要 --failed/--all",
            file=sys.stderr,
        )
        return 1

    if batch_level:
        batch = _resolve_batch_ref(ref)
        if not batch:
            print(f"错误: 批次不存在: {ref}", file=sys.stderr)
            return 1
        task = None
    else:
        batch, task = _resolve_task_ref(ref)

    prepared_specs = []
    with state.connect() as conn:
        batch_row = conn.execute(
            "SELECT status, project, name, env FROM batches WHERE id=?", (batch,)
        ).fetchone()
        if not batch_row:
            print(f"错误: 批次不存在: {ref}", file=sys.stderr)
            return 1
        if batch_row["status"] == "discarded":
            print(
                "错误: 批次已退役 (discarded), 请用新批次名重新提交",
                file=sys.stderr,
            )
            return 1
        if batch_row["status"] == "queued":
            print(
                "错误: queued 批次 (等上游依赖) 不支持 resubmit;"
                " 上游完成后批次会自动 active",
                file=sys.stderr,
            )
            return 1
        project = batch_row["project"]
        batch_name = batch_row["name"]
        if batch_level:
            jobs = conn.execute(
                "SELECT j.* FROM jobs j"
                " JOIN (SELECT task_id, MAX(version) AS version FROM jobs"
                "       WHERE batch_id=? GROUP BY task_id) latest"
                " ON j.batch_id=? AND j.task_id=latest.task_id"
                " AND j.version=latest.version ORDER BY j.rowid",
                (batch, batch),
            ).fetchall()
            if mode == "failed":
                jobs = [
                    job
                    for job in jobs
                    if job["status"]
                    in ("failed", "blocked", "timed_out", "interrupted")
                ]
        else:
            jobs = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND task_id=?"
                " ORDER BY version DESC LIMIT 1",
                (batch, task),
            ).fetchall()
        if not jobs:
            message = (
                "无匹配任务"
                if mode == "failed"
                else f"任务不存在 {batch}:{task}"
            )
            print(f"错误: {message}", file=sys.stderr)
            return 1
        target_tasks = {job["task_id"] for job in jobs}
        active = [
            row
            for row in conn.execute(
                "SELECT j.task_id, j.status, j.version FROM jobs j"
                " WHERE j.batch_id=? AND (j.status='running' OR ("
                "   j.status IN ('pending','waiting_quota','waiting_dep')"
                "   AND j.version=(SELECT MAX(j2.version) FROM jobs j2"
                "     WHERE j2.batch_id=j.batch_id AND j2.task_id=j.task_id)"
                " ))",
                (batch,),
            ).fetchall()
            if row["task_id"] in target_tasks
        ]
        if active:
            labels = ", ".join(
                f"{row['task_id']}[{row['status']}]v{row['version']}"
                for row in active
            )
            print(
                f"错误: 禁止 resubmit 仍在运行/排队的任务: {labels}；"
                "先等待所有版本终态后再提交",
                file=sys.stderr,
            )
            return 1
        marked = [
            job
            for job in conn.execute(
                "SELECT * FROM jobs WHERE batch_id=?",
                (batch,),
            ).fetchall()
            if job["task_id"] in target_tasks
            and state.launch_marker_active(job["id"])
        ]
        if marked:
            labels = ", ".join(
                f"{job['task_id']}v{job['version']}" for job in marked
            )
            print(
                f"错误: 进程组终止尚未确认完成: {labels};"
                " 请等待 daemon 完成清理",
                file=sys.stderr,
            )
            return 1
        if args.dry_run:
            print(
                f"[dry-run] 将 resubmit {len(jobs)} 个任务"
                " (各生成新版本排队尾):"
            )
            for job in jobs:
                print(
                    f"  {job['task_id']} [{job['status']}]"
                    f" v{job['version']} -> v{job['version'] + 1}"
                )
            return 0
        for job in jobs:
            task_row = conn.execute(
                "SELECT spec, order_idx FROM tasks"
                " WHERE batch_id=? AND id=? AND version=?",
                (batch, job["task_id"], job["version"]),
            ).fetchone()
            if not task_row:
                print(
                    f"错误: 任务 spec 缺失 {batch}:{job['task_id']}"
                    f":v{job['version']}",
                    file=sys.stderr,
                )
                return 1
            try:
                spec = json.loads(task_row["spec"])
            except (json.JSONDecodeError, TypeError) as error:
                print(
                    f"错误: 任务 spec 损坏 {batch}:{job['task_id']}: {error}",
                    file=sys.stderr,
                )
                return 1
            spec.pop("retry_transform", None)
            for stage in spec.get("stages") or []:
                if isinstance(stage, dict):
                    stage.pop("retry_transform", None)
                    stage.pop("probes", None)
            prepared_specs.append(
                {
                    "task_id": job["task_id"],
                    "old_version": job["version"],
                    "new_version": job["version"] + 1,
                    "spec": spec,
                    "order_idx": task_row["order_idx"],
                }
            )

    from .fingerprint import compute_fingerprint

    for prepared in prepared_specs:
        spec = prepared["spec"]
        fingerprint, stage_fingerprints, _ = compute_fingerprint(
            spec.get("cmd"),
            spec.get("stages"),
            spec.get("cwd_abs", "."),
            spec.get("git"),
            cfg.get("venvs", {}),
            runtime_prefix=spec.get("runtime_prefix"),
            execution_env=task_environment(cfg, json.loads(batch_row["env"] or "{}"), spec.get("env")),
            artifacts=spec.get("artifacts"),
        )
        prepared["fingerprint"] = fingerprint
        prepared["stage_fingerprints"] = stage_fingerprints

    reopened = False
    dependency_names: list[str] = []
    labels: list[str] = []
    with state.submission_connect() as conn:
        # The final status/version checks and publication are one write
        # transaction.  Without this early writer claim, daemon settlement can
        # commit active->done after our SELECT but before insert_task, leaving
        # a terminal batch with a new pending generation.
        if not conn.in_transaction:
            conn.execute("BEGIN IMMEDIATE")
        current_batch = conn.execute(
            "SELECT status FROM batches WHERE id=?", (batch,)
        ).fetchone()
        if not current_batch or current_batch["status"] in ("discarded", "queued"):
            print(
                "错误: fingerprint 准备期间批次状态已变化，请重试",
                file=sys.stderr,
            )
            return 1
        conflict = _same_name_nonterminal_conflict(conn, batch)
        if conflict is not None:
            _print_same_name_reopen_conflict("resubmit", conflict)
            return 1
        for prepared in prepared_specs:
            latest = conn.execute(
                "SELECT version, status FROM jobs WHERE batch_id=? AND task_id=?"
                " ORDER BY version DESC LIMIT 1",
                (batch, prepared["task_id"]),
            ).fetchone()
            if (
                not latest
                or latest["version"] != prepared["old_version"]
                or latest["status"]
                in ("running", "pending", "waiting_quota", "waiting_dep")
            ):
                print(
                    f"错误: fingerprint 准备期间任务状态已变化:"
                    f" {prepared['task_id']}，请重试",
                    file=sys.stderr,
                )
                return 1
        for prepared in prepared_specs:
            new_version = prepared["new_version"]
            task_id = prepared["task_id"]
            state.insert_task(
                conn,
                batch,
                task_id,
                new_version,
                prepared["spec"],
                prepared["order_idx"],
                project,
            )
            state.insert_job(
                conn,
                f"{batch}-{task_id}-v{new_version}",
                batch,
                task_id,
                new_version,
                prepared["fingerprint"],
                prepared["stage_fingerprints"],
                project,
            )
            labels.append(f"{task_id}->v{new_version}")
        if current_batch["status"] in ("done", "blocked"):
            conn.execute(
                "UPDATE batches SET status='active' WHERE id=?", (batch,)
            )
            reopened = True
        dependency_names = [
            row["name"]
            for row in conn.execute(
                "SELECT name FROM batches WHERE depends_on LIKE ?",
                (f'%"{batch_name}"%',),
            ).fetchall()
        ]
    wake_deferred = state.defer_after_commit(_ensure_running_locked)
    if not wake_deferred:
        wake_result = _ensure_running_locked()

    for dependency in dependency_names:
        print(
            f"⚠️ 提示: 批次 '{dependency}' depends_on 本批次,"
            " 上游已更新, 请重提下游 (Q4)"
        )
    if reopened:
        print("批次已回 active (终态 marker 将由 daemon 在派发前协调)")
    print(f"已 resubmit {len(labels)} 个任务: {', '.join(labels)}")
    if not wake_deferred:
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


def _warn_colocate_disabled(
    norm: dict, cfg: dict, *, stream=None
) -> None:
    """Warn when project configuration downgrades requested co-location."""
    pc = cfg.get("projects", {}).get(norm.get("project") or "", {})
    if pc.get("colocate") is False and any(
        (t.get("resources") or {}).get("gpu_share") for t in norm.get("tasks", [])
    ):
        print(
            f"⚠️ 项目 {norm['project']} 已禁用 colocate:"
            " gpu_share 任务将按独占运行 (装箱声明被忽略)",
            file=stream,
        )


def cmd_clean(args: argparse.Namespace) -> int:
    """Clear fingerprints and remove artifacts of latest skipped generations.

    This is a two-phase mutation.  Fingerprints are committed while the batch
    remains terminal, artifacts are then removed, and only afterwards are the
    latest skipped generations published as runnable work.  Confined artifact
    paths use the runtime's descriptor-relative policy; ``paths_escape``
    remains an explicit opt-in at each declaration.
    """
    b = _resolve_batch_ref(args.batch)
    if not b:
        print(f"错误: 批次不存在: {args.batch}", file=sys.stderr)
        return 1
    if not args.yes:
        print(f"确认清除 {b} 的全部产物与指纹? 加 --yes 执行")
        return 1

    deletions: list[tuple[str, str, Any]] = []
    n = 0
    requeued = 0
    reopened = False
    removed: list[str] = []

    # Hold the global submission gate across both database phases and artifact
    # deletion.  This deliberately favors correctness over clean throughput:
    # no submit/retry/resubmit or idle-shutdown handshake may interleave while
    # external outputs are being removed.  The daemon may still settle state,
    # so phase 2 revalidates the terminal batch before publishing runnable work.
    with state.submission_lock():
        with state.submission_connect() as conn:
            batch_row = conn.execute(
                "SELECT status FROM batches WHERE id=?",
                (b,),
            ).fetchone()
            if not batch_row:
                print(f"错误: 批次不存在: {args.batch}", file=sys.stderr)
                return 1
            if batch_row["status"] not in ("done", "blocked"):
                print(
                    "错误: clean 仅允许 done/blocked 终态批次；"
                    "请先等待任务收敛或取消运行任务",
                    file=sys.stderr,
                )
                return 1
            conflict = _same_name_nonterminal_conflict(conn, b)
            if conflict is not None:
                _print_same_name_reopen_conflict("clean", conflict)
                return 1
            jobs = conn.execute(
                "SELECT id, status FROM jobs WHERE batch_id=?",
                (b,),
            ).fetchall()
            unsafe = [
                job["id"]
                for job in jobs
                if job["status"] == "running"
                or state.launch_marker_active(job["id"])
            ]
            if unsafe:
                print(
                    "错误: clean 拒绝仍在运行或有未决进程组的任务: "
                    + ", ".join(unsafe),
                    file=sys.stderr,
                )
                return 1
            # Artifact declarations may intentionally alias paths across batch
            # generations.  Without producer ownership metadata we cannot
            # prove that deleting this batch's paths is harmless to an already
            # running writer, so clean is conservatively a node-idle operation.
            # New launches are excluded by the surrounding submission gate.
            other_running = conn.execute(
                "SELECT id FROM jobs WHERE status='running' ORDER BY rowid LIMIT 10"
            ).fetchall()
            if other_running:
                print(
                    "错误: clean 在节点仍有运行任务时拒绝删除共享产物路径: "
                    + ", ".join(row["id"] for row in other_running),
                    file=sys.stderr,
                )
                return 1
            other_active = conn.execute(
                "SELECT id FROM batches WHERE id<>? AND status='active'"
                " ORDER BY rowid LIMIT 10",
                (b,),
            ).fetchall()
            if other_active:
                print(
                    "错误: clean 拒绝在其他 active 批次可能继续使用共享产物时删除: "
                    + ", ".join(row["id"] for row in other_active),
                    file=sys.stderr,
                )
                return 1

            trows = conn.execute(
                "SELECT t.id, t.version, t.spec FROM jobs j"
                " JOIN (SELECT task_id, MAX(version) AS mv FROM jobs"
                "       WHERE batch_id=? GROUP BY task_id) latest"
                "   ON j.task_id=latest.task_id AND j.version=latest.mv"
                " JOIN tasks t ON t.batch_id=j.batch_id"
                "   AND t.id=j.task_id AND t.version=j.version"
                " WHERE j.batch_id=? AND j.status='skip'",
                (b, b),
            ).fetchall()
            for tr in trows:
                task_label = f"{tr['id']}v{tr['version']}"
                try:
                    spec = json.loads(tr["spec"] or "{}")
                except (json.JSONDecodeError, TypeError) as error:
                    raise state.StateError(
                        f"clean 拒绝无效任务规格 {task_label}: {error}"
                    ) from error
                if not isinstance(spec, dict):
                    raise state.StateError(
                        f"clean 拒绝无效任务规格 {task_label}: spec 必须是对象"
                    )
                cwd = spec.get("cwd_abs") or "."
                if not isinstance(cwd, str):
                    raise state.StateError(
                        f"clean 拒绝无效任务规格 {task_label}: cwd_abs 必须是字符串"
                    )

                def collect(
                    group: Any,
                    paths_escape: Any,
                    group_label: str,
                ) -> None:
                    if group is None:
                        return
                    if not isinstance(group, dict):
                        raise state.StateError(
                            f"clean 拒绝无效任务规格 {task_label}:"
                            f" {group_label} 必须是对象"
                        )
                    if not isinstance(paths_escape, bool):
                        raise state.StateError(
                            f"clean 拒绝无效任务规格 {task_label}:"
                            f" {group_label}.paths_escape 必须是布尔值"
                        )
                    for artifact_name, rule in group.items():
                        if not isinstance(rule, dict):
                            raise state.StateError(
                                f"clean 拒绝无效任务规格 {task_label}:"
                                f" {group_label}.{artifact_name} 必须是对象"
                            )
                        path = rule.get("path")
                        if not isinstance(path, str) or not path or "\0" in path:
                            raise state.StateError(
                                f"clean 拒绝无效任务规格 {task_label}:"
                                f" {group_label}.{artifact_name}.path 无效"
                            )
                        deletions.append((cwd, path, paths_escape))

                collect(
                    spec.get("artifacts"),
                    spec.get("paths_escape", False),
                    "artifacts",
                )
                stages = spec.get("stages")
                if stages is not None and not isinstance(stages, list):
                    raise state.StateError(
                        f"clean 拒绝无效任务规格 {task_label}: stages 必须是数组"
                    )
                for stage_index, stage in enumerate(stages or []):
                    if not isinstance(stage, dict):
                        raise state.StateError(
                            f"clean 拒绝无效任务规格 {task_label}:"
                            f" stages[{stage_index}] 必须是对象"
                        )
                    collect(
                        stage.get("artifacts"),
                        stage.get("paths_escape", False),
                        f"stages[{stage_index}].artifacts",
                    )

            # Phase 1 commits before any external deletion.  A commit failure
            # therefore leaves both artifacts and terminal state untouched.
            n = conn.execute(
                "UPDATE jobs SET fingerprint=NULL, stage_fingerprints=NULL"
                " WHERE batch_id=?",
                (b,),
            ).rowcount

        # Jobs remain skip/done/failed in a terminal batch throughout artifact
        # removal, so an already-running daemon has nothing it may dispatch.
        for cwd, path, paths_escape in deletions:
            try:
                deleted = artifacts.unlink_artifact(
                    cwd,
                    path,
                    paths_escape=paths_escape,
                    raise_on_error=True,
                )
            except artifacts.ArtifactError as error:
                raise state.StateError(
                    "clean 产物删除失败；可能已删除部分产物，phase 1 已清除"
                    "指纹但批次仍保持终态。修复路径或存储问题后可安全重试: "
                    f"{error}"
                ) from error
            if deleted:
                removed.append(
                    path
                    if os.path.isabs(path)
                    else os.path.normpath(os.path.join(cwd, path))
                )

        with state.submission_connect() as conn:
            # Phase 2 publishes runnable state.  Claim the SQLite writer before
            # reading the terminal batch so daemon settlement cannot change
            # blocked->done between our snapshot and the requeue DML.
            if not conn.in_transaction:
                conn.execute("BEGIN IMMEDIATE")
            current_batch = conn.execute(
                "SELECT status FROM batches WHERE id=?",
                (b,),
            ).fetchone()
            if (
                not current_batch
                or current_batch["status"] not in ("done", "blocked")
            ):
                raise state.StateError(
                    "clean 清理产物期间批次状态已变化；"
                    "指纹已清但未重新排队，请确认终态后重试"
                )
            conflict = _same_name_nonterminal_conflict(conn, b)
            if conflict is not None:
                raise state.StateError(
                    "clean 清理产物期间出现同名非终态批次 "
                    f"{conflict['id']} ({conflict['status']}); "
                    "指纹已清但未重新排队"
                )
            requeued = conn.execute(
                "UPDATE jobs SET status='pending'"
                " WHERE batch_id=? AND status='skip'"
                " AND version=(SELECT MAX(latest.version) FROM jobs latest"
                "   WHERE latest.batch_id=jobs.batch_id"
                "     AND latest.task_id=jobs.task_id)",
                (b,),
            ).rowcount
            if requeued and current_batch["status"] == "done":
                changed = conn.execute(
                    "UPDATE batches SET status='active'"
                    " WHERE id=? AND status='done'",
                    (b,),
                ).rowcount
                if changed != 1:
                    raise state.StateError(
                        "clean 无法原子重开 done 批次；请重试"
                    )
                reopened = True

    for artifact_path in removed[:10]:
        print(f"  已删产物: {artifact_path}")
    print(
        f"✅ 已清除 {n} 个任务的指纹并删除 {len(removed)} 个产物"
        f" ({b}); "
        + (
            f"已重新排队 {requeued} 个最新 skip 任务"
            if requeued
            else "未发现最新 skip，未发布新任务"
        )
    )
    if reopened:
        print("批次已回 active (终态 marker 将由 daemon 在派发前协调)")
        print(_ensure_running_locked())
    return 0


def _deep_merge(base: dict, patch: dict) -> None:
    """递归深合并 patch 到 base (projects/venvs 等嵌套对象按名合并, 不整体替换)."""
    for k, v in patch.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


def cmd_discard(args: argparse.Namespace) -> int:
    """Retire the canonical blocked/queued batch while preserving evidence."""
    if not args.yes:
        print("确认退役? 加 --yes 执行", file=sys.stderr)
        return 1
    with state.submission_connect() as conn:
        # Serialize the queued/blocked precondition with daemon dependency
        # unlock.  Either discard wins and unlock sees no queued row, or unlock
        # wins and this command re-reads active and refuses.
        if not conn.in_transaction:
            conn.execute("BEGIN IMMEDIATE")
        batch_id = _resolve_batch_ref(args.batch, conn)
        batch = conn.execute(
            "SELECT * FROM batches WHERE id=?", (batch_id,)
        ).fetchone() if batch_id else None
        if batch is None:
            print(f"错误: 批次不存在: {args.batch}", file=sys.stderr)
            return 1
        if batch["status"] not in ("blocked", "queued"):
            print(
                f"错误: 仅 blocked/queued 批次可退役"
                f" (当前 {batch['status']}); done 无需退役",
                file=sys.stderr,
            )
            return 1
        running = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE batch_id=? AND status='running'",
            (batch_id,),
        ).fetchone()[0]
        if running:
            print(
                f"错误: 批次仍有 {running} 个运行中任务, 请先 sched cancel",
                file=sys.stderr,
            )
            return 1
        pending = conn.execute(
            "SELECT id FROM jobs WHERE batch_id=? AND status='pending'",
            (batch_id,),
        ).fetchall()
        for job in pending:
            state.update_job(
                conn,
                job["id"],
                status="cancelled",
                kill_reason="discarded",
                finished_at=state.now(),
            )
        affected = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE batch_id=?"
            " AND status IN"
            " ('failed','blocked','timed_out','interrupted','cancelled')",
            (batch_id,),
        ).fetchone()[0]
        changed = conn.execute(
            "UPDATE batches SET status='discarded' WHERE id=?"
            " AND status IN ('blocked','queued')",
            (batch_id,),
        ).rowcount
        if changed != 1:
            raise state.StateError(
                "discard 终态发布竞态：批次已不再是 blocked/queued"
            )
        dependencies = conn.execute(
            "SELECT name FROM batches WHERE depends_on LIKE ?",
            (f'%"{batch["name"]}"%',),
        ).fetchall()
    for dependency in dependencies:
        print(
            f"⚠️ 提示: 批次 '{dependency['name']}' depends_on 本批次,"
            " 已退役, 下游将挂起"
        )
    print(
        f"✅ 批次 {batch_id} 已退役 (涉及 {affected} 个任务,"
        " 失败终态证据保留); 重跑请用新批次名提交"
    )
    return 0


def cmd_config_get(args: argparse.Namespace) -> int:
    """sched config get: 输出当前完整配置 (JSON)."""
    print(json.dumps(_load_cfg(), ensure_ascii=False, indent=2))
    return 0


def cmd_config_set(args: argparse.Namespace) -> int:
    # Serialize the complete read/merge/replace cycle with other CLI writers.
    # Atomic rename alone neither merges concurrent patches nor protects the
    # shared validation temp path from another config set process.
    with state.submission_lock():
        return _config_set_serialized(args)


def _config_set_serialized(args: argparse.Namespace) -> int:
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
    try:
        og, ng = parse_gpus(old), parse_gpus(new_cfg)
    except ConfigError as exc:
        print(f"错误: 新配置校验失败 (未写入): {exc}", file=sys.stderr)
        return 1
    if (og[0], og[1]) != (ng[0], ng[1]):
        cold.append("gpus(卡集或容量覆盖)")
    if cold:
        print(f"错误: 含冷键变更 {cold} —— 热更新拒绝, 请手动编辑并重启 daemon",
              file=sys.stderr)
        return 1

    # 全量校验: 写临时文件走 load_config 完整管线 (含 parse_gpus/notify 等)
    cfg_p = config_path()
    tmp_p = cfg_p + ".tmp-set"
    with state.open_private_text(tmp_p, "w") as f:
        json.dump(new_cfg, f, indent=2, ensure_ascii=False)
    try:
        load_config(tmp_p)
    except Exception as e:
        if os.path.exists(tmp_p):
            os.remove(tmp_p)
        print(f"错误: 新配置校验失败 (未写入): {e}", file=sys.stderr)
        return 1
    os.replace(tmp_p, cfg_p)
    changed = sorted(set(_flatten_keys(patch)))
    try:
        with state.connect() as conn:
            state.insert_control_request(conn, "*config*", op="config_reload")
    except Exception as error:
        print(f"✅ 配置文件已应用: {', '.join(changed)}")
        print(
            "警告: control reload 请求入队失败"
            f" ({error}); daemon 仍会通过 mtime 检测热重载",
            file=sys.stderr,
        )
        return 0
    print(f"✅ 配置文件已应用并请求热重载: {', '.join(changed)}")
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
            batch, task = _resolve_task_ref(ref, conn)
            targets = conn.execute(
                "SELECT * FROM jobs WHERE batch_id=? AND task_id=?"
                " ORDER BY version DESC LIMIT 1",
                (batch, task),
            ).fetchall()
            if not targets:
                print(f"错误: 任务不存在 {batch}:{task}", file=sys.stderr)
                return 1
        else:
            b = _resolve_batch_ref(ref, conn)
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
    if not math.isfinite(args.gib) or args.gib <= 0:
        print(f"错误: mem_gib 必须是有限正数 (got {args.gib})", file=sys.stderr)
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

    try:
        if args.action == "start":
            text = daemon.start(fake=getattr(args, "fake", False))
            print(text)
            return 1 if any(
                marker in text
                for marker in ("拒绝", "失败", "错误", "超时", "请到计算节点")
            ) else 0
        if args.action == "stop":
            text = daemon.stop()
            print(text)
            return 1 if any(
                marker in text
                for marker in (
                    "拒绝",
                    "失败",
                    "错误",
                    "超时",
                    "请到计算节点",
                    "放弃 kill",
                )
            ) else 0
        if args.action == "status":
            print(daemon.status_str())
            return 0
        issues = daemon.check(fake=getattr(args, "fake", False))
        failures = 0
        for issue in issues:
            mark = {"ok": "✅", "warn": "⚠️", "fail": "❌"}[issue["level"]]
            print(f"  {mark} {issue['item']}: {issue['detail']}")
            if issue["level"] == "fail":
                failures += 1
        print(f"\n{failures} 项 FAIL" if failures else "\n全部通过 ✅")
        return 1 if failures else 0
    except Exception as error:
        print(f"错误: daemon {args.action} 失败: {error}", file=sys.stderr)
        return 1


# ---------- 通知 (现行配置与命令参考: docs/reference.md) ----------

def cmd_notify_test(args: argparse.Namespace) -> int:
    """sched notify-test: 发测试通知, 验证 config.notify 各渠道可用."""
    from . import notify

    cfg = _load_cfg()
    ncfg = cfg.get("notify")
    if not ncfg:
        print("config.notify 未配置 (功能关闭); 参见 docs/reference.md")
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
            except (OSError, ValueError, TypeError):
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
            if not isinstance(ev, dict):
                raise ValueError("事件必须是 JSON 对象")
            mark = "✅" if ev.get("event") == "batch_done" else "❌"
            n_fail = len(ev.get("failures") or [])
            extra = f" ({n_fail} 失败)" if n_fail else ""
            print(f"  {mark} {os.path.basename(p):<52} {ev.get('batch')}{extra}")
        except (OSError, ValueError, TypeError):
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
    except (OSError, ValueError) as exc:
        print(f"错误: 无法确认事件: {exc}", file=sys.stderr)
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


_REQUEST_CAPTURE_BYTES = 2 * 1024 * 1024


class _BoundedTextCapture(io.TextIOBase):
    """UTF-8 text sink whose retained bytes never exceed a fixed ceiling."""

    def __init__(self, limit: int = _REQUEST_CAPTURE_BYTES) -> None:
        super().__init__()
        self._limit = max(128, int(limit))
        self._payload_limit = self._limit - 96
        self._buffer = bytearray()
        self._dropped = 0

    def writable(self) -> bool:
        return True

    def write(self, value: str) -> int:
        if not isinstance(value, str):
            raise TypeError("capture accepts text only")
        for offset in range(0, len(value), 64 * 1024):
            encoded = value[offset : offset + 64 * 1024].encode(
                "utf-8",
                errors="replace",
            )
            remaining = self._payload_limit - len(self._buffer)
            if remaining > 0:
                self._buffer.extend(encoded[:remaining])
            self._dropped += max(0, len(encoded) - max(0, remaining))
        return len(value)

    def getvalue(self) -> str:
        retained = bytes(self._buffer).decode("utf-8", errors="ignore")
        if not self._dropped:
            return retained
        marker = f"\n[output truncated: {self._dropped} bytes dropped]\n"
        value = retained + marker
        encoded = value.encode("utf-8")
        if len(encoded) <= self._limit:
            return value
        return encoded[: self._limit].decode("utf-8", errors="ignore")


def _run_captured_mutation(
    command: list[str],
    conn: sqlite3.Connection | None = None,
) -> tuple[int, str, str, list[Any]]:
    captured_stdout = _BoundedTextCapture()
    captured_stderr = _BoundedTextCapture()
    callbacks: list[Any] = []
    code = 1
    try:
        with contextlib.redirect_stdout(captured_stdout), contextlib.redirect_stderr(
            captured_stderr
        ):
            if conn is None:
                code = int(main(command))
            else:
                with state.bind_connection(conn) as callbacks:
                    code = int(main(command))
    except SystemExit as exc:
        code = int(exc.code) if isinstance(exc.code, int) else 1
    except Exception as exc:
        code = 1
        captured_stderr.write(f"错误: mutation 执行异常: {exc}\n")
    return code, captured_stdout.getvalue(), captured_stderr.getvalue(), callbacks


def _canonical_assignment_precondition(raw: Any) -> list[dict[str, Any]] | None:
    if raw is None:
        return None
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > 64 * 1024:
        raise ValueError("GPU assignments precondition 必须是有界 JSON")
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError, RecursionError) as exc:
        raise ValueError("GPU assignments precondition 不是合法 JSON") from exc
    if not isinstance(parsed, list):
        raise ValueError("GPU assignments precondition 必须是数组")
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for assignment in parsed:
        if not isinstance(assignment, dict) or set(assignment) != {
            "job_id",
            "vram_gib",
        }:
            raise ValueError("GPU assignment 必须只含 job_id/vram_gib")
        job_id = assignment["job_id"]
        vram_gib = assignment["vram_gib"]
        if (
            not isinstance(job_id, str)
            or not job_id
            or len(job_id) > 512
            or job_id in seen
        ):
            raise ValueError("GPU assignment job_id 无效或重复")
        if vram_gib is not None and (
            isinstance(vram_gib, bool)
            or not isinstance(vram_gib, (int, float))
            or not math.isfinite(float(vram_gib))
            or vram_gib < 0
        ):
            raise ValueError("GPU assignment vram_gib 无效")
        seen.add(job_id)
        result.append({"job_id": job_id, "vram_gib": vram_gib})
    result.sort(key=lambda item: item["job_id"])
    return result


def cmd_request(args: argparse.Namespace) -> int:
    """Execute a mutation once with revision-bound durable replay."""
    request_id = str(getattr(args, "request_id", ""))
    command = list(getattr(args, "command", []) or [])
    if command[:1] == ["--"]:
        command = command[1:]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", request_id):
        print("错误: request_id 格式无效", file=sys.stderr)
        return 64
    allowed = {
        "submit",
        "cancel",
        "retry",
        "resubmit",
        # gpu-set-mem is intentionally excluded: mem_total_gib is a
        # restart-ephemeral state/list-gpus value and is not revision/CAS-bound.
        "gpu-free",
        "gpu-ignore",
        "gpu-ok",
        "daemon",
        "config",
    }
    if not command or command[0] not in allowed:
        print("错误: request 只允许调度器 mutation 子命令", file=sys.stderr)
        return 64
    if command[0] == "daemon" and (
        len(command) < 2 or command[1] not in {"start", "stop"}
    ):
        print("错误: request 只允许 daemon start/stop", file=sys.stderr)
        return 64
    if command[0] == "config" and (
        len(command) < 2 or command[1] != "set"
    ):
        print("错误: request 只允许 config set", file=sys.stderr)
        return 64

    expect_kind = str(getattr(args, "expect_kind", "none") or "none")
    expect_id = getattr(args, "expect_id", None)
    expect_status = getattr(args, "expect_status", None)
    expect_version = getattr(args, "expect_version", None)
    expect_quarantined = getattr(args, "expect_quarantined", None)
    expect_revision = getattr(args, "expect_revision", None)
    try:
        expect_assignments = _canonical_assignment_precondition(
            getattr(args, "expect_assignments_json", None)
        )
    except ValueError as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 64

    if (
        isinstance(expect_revision, bool)
        or not isinstance(expect_revision, int)
        or expect_revision < 0
    ):
        print("错误: mutation 必须提供非负 --expect-revision", file=sys.stderr)
        return 64

    target_kind = "none"
    target_id: str | None = None
    if command[0] in {"cancel", "retry", "resubmit"}:
        if len(command) < 2:
            print("错误: mutation 缺少目标", file=sys.stderr)
            return 64
        target_id = command[1]
        target_kind = "task" if ":" in target_id else "batch"
    elif command[0].startswith("gpu-"):
        if len(command) < 2:
            print("错误: GPU mutation 缺少目标", file=sys.stderr)
            return 64
        target_id = command[1]
        target_kind = "gpu"

    if target_kind != expect_kind or (
        target_kind != "none" and target_id != expect_id
    ):
        print("错误: mutation precondition 与命令目标不匹配", file=sys.stderr)
        return 64
    if expect_kind == "none":
        if (
            expect_revision != 0
            or any(
                value is not None
                for value in (
                    expect_id,
                    expect_status,
                    expect_version,
                    expect_quarantined,
                    expect_assignments,
                )
            )
        ):
            print("错误: 无目标 mutation 只接受 --expect-revision 0", file=sys.stderr)
            return 64
    elif (
        not isinstance(expect_id, str)
        or not expect_id
        or not isinstance(expect_status, str)
        or not expect_status
    ):
        print("错误: mutation precondition 字段不完整", file=sys.stderr)
        return 64
    if expect_kind == "task" and (
        not isinstance(expect_version, int)
        or isinstance(expect_version, bool)
        or expect_version < 1
        or not isinstance(expect_id, str)
        or expect_id.count(":") != 1
    ):
        print("错误: task mutation precondition 无效", file=sys.stderr)
        return 64
    if expect_kind != "task" and expect_version is not None:
        print("错误: 非 task mutation 不接受 version precondition", file=sys.stderr)
        return 64
    if expect_kind == "gpu" and (
        not isinstance(expect_id, str)
        or not expect_id.isdigit()
        or expect_assignments is None
    ):
        print("错误: GPU mutation precondition 无效", file=sys.stderr)
        return 64
    if expect_kind != "gpu" and (
        expect_quarantined is not None or expect_assignments is not None
    ):
        print("错误: 非 GPU mutation 不接受 GPU precondition", file=sys.stderr)
        return 64
    if expect_quarantined is not None and (
        not isinstance(expect_quarantined, int)
        or isinstance(expect_quarantined, bool)
        or expect_quarantined not in {0, 1}
    ):
        print("错误: GPU quarantined precondition 无效", file=sys.stderr)
        return 64

    expectation = {
        "kind": expect_kind,
        "id": expect_id,
        "status": expect_status,
        "version": expect_version,
        "quarantined": expect_quarantined,
        "revision": expect_revision,
        "assignments": expect_assignments,
    }
    argv_json = json.dumps(
        {"command": command, "expect": expectation},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )

    def existing_result(conn: sqlite3.Connection):
        return conn.execute(
            "SELECT argv, status, code, stdout, stderr, output_compacted"
            " FROM operation_requests WHERE request_id=?",
            (request_id,),
        ).fetchone()

    def replay(existing) -> int | None:
        if existing is None:
            return None
        if existing["argv"] != argv_json:
            print(
                "错误: request_id 已绑定到不同 mutation",
                file=sys.stderr,
            )
            return 64
        if existing["status"] != "done":
            print(
                "错误: prior mutation outcome unknown; refusing replay",
                file=sys.stderr,
            )
            return 75
        sys.stdout.write(existing["stdout"] or "")
        sys.stderr.write(existing["stderr"] or "")
        return int(existing["code"])

    def precondition_conflict(conn: sqlite3.Connection) -> str | None:
        if expect_kind == "none":
            return None
        if expect_kind == "batch":
            row = conn.execute(
                "SELECT status, revision FROM batches WHERE id=?",
                (expect_id,),
            ).fetchone()
            if row is None:
                return "batch absent"
            if row["status"] != expect_status:
                return (
                    f"batch status changed: expected {expect_status},"
                    f" found {row['status']}"
                )
            if row["revision"] != expect_revision:
                return (
                    f"batch revision changed: expected {expect_revision},"
                    f" found {row['revision']}"
                )
            return None
        if expect_kind == "task":
            batch_id, task_id = expect_id.split(":", 1)
            row = conn.execute(
                "SELECT j.status, j.version, b.revision"
                " FROM jobs j JOIN batches b ON b.id=j.batch_id"
                " WHERE j.batch_id=? AND j.task_id=?"
                " ORDER BY j.version DESC LIMIT 1",
                (batch_id, task_id),
            ).fetchone()
            if row is None:
                return "task absent"
            actual_status = {
                "waiting_quota": "pending",
                "waiting_dep": "pending",
            }.get(row["status"], row["status"])
            if actual_status != expect_status or row["version"] != expect_version:
                return (
                    f"task changed: expected {expect_status} v{expect_version},"
                    f" found {actual_status} v{row['version']}"
                )
            if row["revision"] != expect_revision:
                return (
                    f"batch revision changed: expected {expect_revision},"
                    f" found {row['revision']}"
                )
            return None
        row = conn.execute(
            "SELECT status, quarantined, revision FROM gpus WHERE idx=?",
            (int(expect_id),),
        ).fetchone()
        if row is None:
            return "GPU absent"
        if row["status"] != expect_status:
            return (
                f"GPU status changed: expected {expect_status},"
                f" found {row['status']}"
            )
        if (
            expect_quarantined is not None
            and row["quarantined"] != expect_quarantined
        ):
            return (
                "GPU quarantine changed:"
                f" expected {expect_quarantined}, found {row['quarantined']}"
            )
        if row["revision"] != expect_revision:
            return (
                f"GPU revision changed: expected {expect_revision},"
                f" found {row['revision']}"
            )
        assignments = [
            {"job_id": item["job_id"], "vram_gib": item["vram_gib"]}
            for item in conn.execute(
                "SELECT job_id, vram_gib FROM gpu_jobs"
                " WHERE gpu_id=? ORDER BY job_id",
                (int(expect_id),),
            ).fetchall()
        ]
        if assignments != expect_assignments:
            return (
                "GPU assignments changed:"
                f" expected {expect_assignments}, found {assignments}"
            )
        return None

    unbound = command[0] in {"daemon", "config"}
    if unbound:
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            state.compact_operation_outputs(conn)
            existing = existing_result(conn)
            replay_code = replay(existing)
            if replay_code is not None:
                return replay_code
            conflict = precondition_conflict(conn)
            conn.execute(
                "INSERT INTO operation_requests"
                " (request_id, argv, status, created_at)"
                " VALUES (?, ?, 'started', ?)",
                (request_id, argv_json, state.now()),
            )
            if conflict is not None:
                stderr = f"错误: mutation precondition failed: {conflict}\n"
                conn.execute(
                    "UPDATE operation_requests SET status='done', code=65,"
                    " stdout='', stderr=?, finished_at=? WHERE request_id=?",
                    (stderr, state.now(), request_id),
                )
                sys.stderr.write(stderr)
                return 65

        code, stdout, stderr, _callbacks = _run_captured_mutation(command)
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE operation_requests"
                " SET status='done', code=?, stdout=?, stderr=?, finished_at=?"
                " WHERE request_id=? AND status='started'",
                (code, stdout, stderr, state.now(), request_id),
            )
        sys.stdout.write(stdout)
        sys.stderr.write(stderr)
        return code

    deferred: list[Any] = []
    with state.submission_lock(), state.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        state.compact_operation_outputs(conn)
        existing = existing_result(conn)
        replay_code = replay(existing)
        if replay_code is not None:
            return replay_code
        conn.execute(
            "INSERT INTO operation_requests"
            " (request_id, argv, status, created_at)"
            " VALUES (?, ?, 'started', ?)",
            (request_id, argv_json, state.now()),
        )
        conflict = precondition_conflict(conn)
        if conflict is not None:
            code = 65
            stdout = ""
            stderr = f"错误: mutation precondition failed: {conflict}\n"
        else:
            conn.execute("SAVEPOINT request_mutation")
            code, stdout, stderr, deferred = _run_captured_mutation(command, conn)
            if code:
                conn.execute("ROLLBACK TO request_mutation")
                deferred.clear()
            conn.execute("RELEASE request_mutation")
        conn.execute(
            "UPDATE operation_requests"
            " SET status='done', code=?, stdout=?, stderr=?, finished_at=?"
            " WHERE request_id=? AND status='started'",
            (code, stdout, stderr, state.now(), request_id),
        )

    while deferred:
        effect = deferred.pop(0)
        try:
            effect()
        except Exception as exc:
            print(f"警告: mutation 已提交，但提交后副作用失败: {exc}", file=sys.stderr)
    sys.stdout.write(stdout)
    sys.stderr.write(stderr)
    return code


def main(argv: list[str] | None = None) -> int:
    state.set_read_only(False)
    state.set_query_only(False)
    ap = argparse.ArgumentParser(
        prog="sched", description=f"sched v{__version__} 统一任务调度框架"
    )
    # Keep the parser's routing key separate from subcommand payload fields.
    # `run` intentionally exposes a positional `cmd` remainder; reusing that
    # name for the selected subcommand replaces "run" with a list and breaks
    # every set-membership check below before cmd_run can execute.
    sub = ap.add_subparsers(dest="_subcommand")

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
    p.add_argument(
        "--gpus",
        type=int,
        default=None,
        help="申请 GPU 数量 (当前只支持 1；零 GPU 请用 --cpu-only)",
    )
    p.add_argument("--cpus", type=int, default=None, help="CPU 配额 (记录+status 显示, B4)")
    p.add_argument("--cpu-only", action="store_true",
                   help="CPU-only 任务 (resources.gpu=0, 不占 GPU 槽位)")
    p.add_argument("--duration", type=int, default=None, help="预计时长(分钟), 超过该时长即终止")
    p.add_argument(
        "--cwd",
        default=None,
        help="工作目录 (默认 {ROOT}，即 default_project 根目录)",
    )
    p.add_argument("--out", default=None, help="产物路径 (声明后 done 需产物存在)")
    p.add_argument("--venv", default=None, help="venv 语义名 (默认 config 第一个)")
    p.add_argument("--dry-run", action="store_true",
                   help="预览不提交 (§G4 A 类: 展开命令 + skip 预测, 纯只读)")
    p.add_argument("cmd", nargs=argparse.REMAINDER, help="-- 后的 shell 命令")
    p.set_defaults(fn=cmd_run)

    p = sub.add_parser("status", help="三视图总览")
    p.add_argument("batch", nargs="?", default=None)
    p.add_argument("--json", action="store_true")
    p.add_argument("--limit", type=int, default=200, help="批次/任务最大行数 (默认 200)")
    p.add_argument(
        "--cursor",
        default=None,
        help="上一页 JSON 的 next_cursor（稳定键集分页）",
    )
    p.add_argument(
        "--job-cursor",
        default=None,
        help="上一页 JSON 的 next_job_cursor（独立任务键集分页）",
    )
    p.add_argument("--detail", action="store_true",
                   help="任务视图含起止时间/耗时/version (P5)")
    p.add_argument("--project", default=None,
                   help="按项目过滤 (B11c)")
    p.set_defaults(fn=cmd_status)

    p = sub.add_parser("task", help="单任务详情")
    p.add_argument("task", help="<batch>:<task>")
    p.add_argument("--json", action="store_true", help="稳定版本化 JSON 输出")
    p.set_defaults(fn=cmd_task)

    p = sub.add_parser("history", help="历史查询")
    p.add_argument("batch", nargs="?", default=None)
    p.add_argument("--limit", type=int, default=50, help="最大行数 (默认 50)")
    p.add_argument(
        "--cursor",
        default=None,
        help="上一页 JSON 的 next_cursor（稳定键集分页）",
    )
    p.add_argument("--json", action="store_true", help="稳定版本化 JSON 输出")
    p.add_argument("--status", default=None, help="按状态过滤, 逗号分隔 (如 done,failed)")
    p.add_argument("--project", default=None, help="按项目过滤 (B11c)")
    p.set_defaults(fn=cmd_history)

    p = sub.add_parser("markers", help="批次终态 marker 一行查看 (P7)")
    p.set_defaults(fn=cmd_markers)

    p = sub.add_parser("cancel", help="取消 (组级 kill)")
    p.add_argument("batch", nargs="?", default=None,
                   help="<batch> 或 <batch>:<task> (与 --project 二选一)")
    p.add_argument("--project", default=None,
                   help="批量取消: 该项目全部 queued/active/blocked 批次 (需 --yes)")
    p.add_argument("--yes", action="store_true")
    p.set_defaults(fn=cmd_cancel)

    p = sub.add_parser("retry", help="解锁 blocked 重跑 (批次级或单任务)")
    p.add_argument("task", help="<batch> 或 <batch>:<task> (批次级=全部失败终态)")
    p.set_defaults(fn=cmd_retry)

    p = sub.add_parser("diag", help="一站式失败诊断 (状态+命令+git+日志)")
    p.add_argument("task", help="<batch> 或 <batch>:<task> (批次级=全部非 done/skip)")
    p.set_defaults(fn=cmd_diag)

    p = sub.add_parser("discard", help="退役被取代的 blocked/queued 批次")
    p.add_argument("batch", help="批次名或 id")
    p.add_argument("--yes", action="store_true", help="确认执行")
    p.set_defaults(fn=cmd_discard)

    p = sub.add_parser("clean", help="清除批次最新产物与指纹 (强制后续重跑)")
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

    p = sub.add_parser(
        "request",
        help="以 durable request_id 最多执行一次 mutation",
    )
    p.add_argument("request_id")
    p.add_argument(
        "--expect-kind",
        choices=["none", "batch", "task", "gpu"],
        default="none",
    )
    p.add_argument("--expect-id")
    p.add_argument("--expect-status")
    p.add_argument("--expect-version", type=int)
    p.add_argument("--expect-quarantined", type=int, choices=[0, 1])
    p.add_argument("--expect-revision", type=int, required=True)
    p.add_argument(
        "--expect-assignments-json",
        help='GPU 当前 assignments JSON，如 [{"job_id":"j","vram_gib":1.5}]',
    )
    p.add_argument("command", nargs="+")
    p.set_defaults(fn=cmd_request)

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

    command = getattr(args, "_subcommand", None)
    daemon_action = getattr(args, "action", None) if command == "daemon" else None
    config_get = (
        command == "config" and getattr(args, "config_cmd", None) == "get"
    )
    dry_run = bool(getattr(args, "dry_run", False))
    allow_foreign_write = os.environ.get("SCHED_ALLOW_FOREIGN_WRITE") == "1"
    cfg = None
    config_error = None
    if command != "init":
        try:
            from . import config as config_module

            cfg = config_module.load_config()
        except Exception as error:
            config_error = error

    if (
        command == "daemon"
        and daemon_action in ("start", "stop", "check")
        and not allow_foreign_write
        and cfg is None
    ):
        print(
            f"错误: 无法读取配置并验证 daemon 写入主机，拒绝执行: {config_error}",
            file=sys.stderr,
        )
        return 2

    foreign = bool(cfg and _is_foreign_host(cfg))
    if (
        command == "daemon"
        and daemon_action in ("start", "stop", "check")
        and foreign
        and not allow_foreign_write
    ):
        print(
            f"错误: daemon {daemon_action} 必须在计算节点"
            f" {cfg.get('node')} 上执行；当前主机只允许 daemon status",
            file=sys.stderr,
        )
        return 2

    write_commands = {
        "run",
        "cancel",
        "retry",
        "resubmit",
        "request",
        "discard",
        "clean",
        "config",
        "gpu-ok",
        "gpu-free",
        "gpu-ignore",
        "gpu-set-mem",
        "notify-test",
        "notify-ack",
    }
    protected_write = (
        command in write_commands and not config_get and not dry_run
    )
    if protected_write and foreign and not allow_foreign_write:
        print(
            f"错误: 写操作 ({command}) 必须在计算节点 {cfg.get('node')} 上执行,"
            " 当前主机仅允许只读查询。"
            " (明确强制继续: SCHED_ALLOW_FOREIGN_WRITE=1)",
            file=sys.stderr,
        )
        return 2

    read_commands = {
        "verify",
        "status",
        "task",
        "history",
        "markers",
        "incidents",
        "diag",
        "log",
        "list-gpus",
        "notify-inbox",
        "project",
    }
    db_read_commands = read_commands - {"markers", "notify-inbox"}
    foreign_submit = command == "submit" and foreign and not allow_foreign_write
    foreign_read = foreign and (
        command in read_commands
        or dry_run
        or (command == "daemon" and daemon_action == "status")
    )
    local_read = not foreign and (
        command in read_commands
        or (command == "daemon" and daemon_action == "status")
    )
    local_db_read = not foreign and command in db_read_commands
    state.set_read_only(foreign_read or dry_run)

    should_init = (
        command != "init"
        and cfg is not None
        and not config_get
        and not foreign_submit
        and not foreign_read
        and not dry_run
        and (not local_read or local_db_read)
    )
    if should_init:
        try:
            if local_db_read:
                state.ensure_db_initialized()
            else:
                state.init_db()
        except Exception as error:
            print(
                f"错误: state DB 初始化或迁移失败 ({error}); 拒绝执行命令",
                file=sys.stderr,
            )
            state.set_read_only(False)
            state.set_query_only(False)
            return 1
    # Local query commands use a coherent private WAL snapshot and never open
    # the NFS-backed source through SQLite.  Initialization above still creates
    # or migrates a fresh/legacy DB before query-only mode is enabled.
    state.set_query_only(local_db_read and not dry_run)
    try:
        return args.fn(args)
    except state.SubmissionBlocked as error:
        print(f"错误: {error}", file=sys.stderr)
        return 2
    except state.StateError as error:
        print(f"错误: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    finally:
        state.set_read_only(False)
        state.set_query_only(False)


if __name__ == "__main__":
    sys.exit(main())
