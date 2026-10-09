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

from . import recovery, recovery_state
from . import state, execution_state, __version__
from . import artifacts
from .executor import PROGRESS_RE
from .config import (
    ConfigError,
    config_path,
    default_state_dir,
    load_config,
    parse_gpus,
    project_gpu_enabled,
    resolve_template,
    task_environment,
)
from .schema import (
    SchemaError,
    check_dependency_cycle,
    parse_shell_cmd,
    validate_batch,
    validate_persisted_dependencies,
    validate_project_gpu_access,
)
from .execution_policy import (
    ExecutionPolicyError, digest as execution_digest,
    project_roots as _execution_project_roots, revalidate_binding,
)

def native_exec_project_roots(cfg):
    return {**_legacy_project_roots(cfg), **_execution_project_roots(cfg)}

from .templates import expand_cmd
from ._legacy_execution import (
    NATIVE_EXEC_ALL_INTERNAL_FIELDS,
    NATIVE_EXEC_V2_CONTRACT_FIELD,
    NativeExecProfileError,
    native_exec_project_roots as _legacy_project_roots,
    native_exec_reserved_batch_names,
)


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

_NATIVE_EXEC_METADATA_KEYS = (
    "_native_exec_profile_id",
    "_native_exec_profile_sha256",
    "_native_exec_project_root_identity_sha256",
    "_native_exec_submitted_argv",
)


def _native_exec_fingerprint_kwargs(task: dict) -> dict[str, str]:
    if isinstance(task.get("_execution_binding"), dict):
        return {"execution_binding_sha256": execution_digest(task["_execution_binding"])}
    """Return native fingerprint binding only for a complete normalized tuple."""
    if all(key in task for key in _NATIVE_EXEC_METADATA_KEYS):
        return {
            "native_exec_profile_sha256": task[
                "_native_exec_profile_sha256"
            ],
            "native_exec_project_root_identity_sha256": task[
                "_native_exec_project_root_identity_sha256"
            ],
        }
    return {}


def _persist_native_exec_metadata(source: dict, destination: dict) -> None:
    recovery.copy_fields(source, destination)
    if source.get("execution") is not None:
        destination["execution"] = source["execution"]
        destination["_execution_binding"] = source["_execution_binding"]
    """Persist the schema-issued native metadata without partial tuples."""
    if all(key in source for key in _NATIVE_EXEC_METADATA_KEYS):
        destination.update(
            {key: source[key] for key in _NATIVE_EXEC_METADATA_KEYS}
        )
        if NATIVE_EXEC_V2_CONTRACT_FIELD in source:
            destination[NATIVE_EXEC_V2_CONTRACT_FIELD] = source[
                NATIVE_EXEC_V2_CONTRACT_FIELD
            ]


def _native_exec_metadata_error(task: dict, expanded_cmd: Any) -> str | None:
    """Reject partial or command-drifted native metadata before persistence."""
    present = [key for key in _NATIVE_EXEC_METADATA_KEYS if key in task]
    if NATIVE_EXEC_V2_CONTRACT_FIELD in task and not present:
        return "native exec V2 contract requires a complete metadata tuple"
    if present and len(present) != len(_NATIVE_EXEC_METADATA_KEYS):
        return "native exec metadata must be an all-or-none tuple"
    if present and (
        not isinstance(task["_native_exec_profile_id"], str)
        or not task["_native_exec_profile_id"]
    ):
        return "native exec profile id must be non-empty"
    if present and (
        not isinstance(task["_native_exec_profile_sha256"], str)
        or re.fullmatch(
            r"[0-9a-f]{64}", task["_native_exec_profile_sha256"]
        )
        is None
    ):
        return "native exec profile sha256 must be lowercase 64-hex"
    if present and (
        not isinstance(
            task["_native_exec_project_root_identity_sha256"], str
        )
        or re.fullmatch(
            r"[0-9a-f]{64}",
            task["_native_exec_project_root_identity_sha256"],
        )
        is None
    ):
        return "native exec project root identity sha256 must be lowercase 64-hex"
    if present and task["_native_exec_submitted_argv"] != expanded_cmd:
        return "native exec submitted argv does not match expanded command"
    return None


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
        "node": input("daemon 计算节点名（必填）: ").strip(),
        "state_dir": input(f"state 目录 [{default_state_dir()}]: ").strip()
        or default_state_dir(),
        "gpus": [0, 1, 2, 3],
        "projects": {
            "example": {
                "root": input("example 项目根目录: ").strip(),
                "git": True,
            }
        },
        "default_project": "example",
        "venvs": {
            "python": input("python 解释器路径: ").strip(),
        },
    }
    if not cfg["node"]:
        print("错误: 必须显式填写 daemon 计算节点名", file=sys.stderr)
        return 1
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
                **_native_exec_fingerprint_kwargs(task),
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
            **_native_exec_fingerprint_kwargs(task),
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

    preview = {
        "tasks": preview_tasks,
        "dep_status": dep_status,
        "git_rev": git_rev,
        "n_skip": n_skip,
        "n_run": n_run,
    }
    if norm.get("depends_on_exact"):
        preview["exact_dependencies"] = {"selectors": norm["depends_on_exact"],
                                         "sources_checked": use_state, "dispatch_ready": None}
    return preview


def _print_dry_run_preview(norm: dict, args: argparse.Namespace, prev: dict, conflict: bool) -> None:
    if args.json:
        print(json.dumps(prev, ensure_ascii=False, indent=2))
        return
    print(f"=== dry-run: {norm['name']} ({len(norm['tasks'])} 任务, mode={norm['mode']}) ===")
    if conflict:
        if norm["mode"] == "strict":
            print("  ⚠️ strict 批次名已消费 — 实际提交会被拒绝")
        else:
            print("  ⚠️ 同名批次已有未终态实例 — 实际提交会被定案 6 拒绝")
    if norm.get("depends_on_exact"):
        print("  exact 依赖按显式 instance/batch/task/version 冻结；预览不授予派发权")
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
    if getattr(args, "request_id", None) is None:
        if getattr(args, "expect_instance", None) is not None or getattr(args, "expect_project", None) is not None:
            print("错误: submit identity expectations require --request-id", file=sys.stderr)
            return 64
        return _cmd_submit_impl(args)
    from .integration import run_submission
    try:
        with open(args.batch, "r", encoding="utf-8") as stream:
            spec = json.load(stream)
        return run_submission(args, _load_cfg(), spec, _cmd_submit_impl)
    except OSError as error:
        print(f"错误: submission receipt unavailable: {error}", file=sys.stderr)
        return 75
    except (ValueError, TypeError) as error:
        print(f"错误: idempotent submission failed: {error}", file=sys.stderr)
        return 64


def _cmd_submit_impl(args: argparse.Namespace) -> int:
    """sched submit batch.json [--dry-run]: 校验 -> 预览(dry) 或 入队."""
    cfg = _load_cfg()
    path = args.batch
    try:
        with open(path, "r", encoding="utf-8") as f:
            spec = getattr(args, "_submission_spec", None)
            if spec is None:
                spec = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"错误: 读取 {path} 失败: {e}", file=sys.stderr)
        return 1
    try:
        norm = validate_batch(spec, cfg)
    except (SchemaError, ConfigError) as e:
        print(f"校验失败: {e}", file=sys.stderr)
        return 1
    if any(
        NATIVE_EXEC_V2_CONTRACT_FIELD in task for task in norm["tasks"]
    ):
        print(
            "校验失败: native exec profile V2 仅完成冻结合同兼容校验；"
            "实际 retained/bootstrap launcher 尚未接入，拒绝持久化或启动",
            file=sys.stderr,
        )
        return 1
    try:
        check_dependency_cycle(norm["depends_on"], cfg)
    except (SchemaError, ConfigError) as e:
        print(f"校验失败: {e}", file=sys.stderr)
        return 1

    diagnostic_stream = (
        sys.stderr
        if getattr(args, "json", False)
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
               if not t.get("execution") and not t.get("runtime") and not any(
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
    ticket = getattr(args, "_submission_ticket", None)
    if ticket is not None:
        bid = ticket["batch_id"]
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
                            "submission": ticket,
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
        if getattr(args, "json", False):
            print(json.dumps({"schema_version": 1, "batch_id": bid,
                              "delivery": "inbox", "persisted": False,
                              "tasks": len(norm["tasks"]), "project": norm.get("project")},
                             ensure_ascii=False))
        else:
            print(
                f"已投递: {bid} ({len(norm['tasks'])} 任务) "
                f"-> {cfg.get('node')} (inbox)"
            )
        health = _daemon_health()
        heartbeat_age = health.get("heartbeat_age_s")
        tick_age = health.get("tick_ok_age_s")
        if heartbeat_age is None or heartbeat_age > 60:
            print("⚠️ daemon 未运行或心跳已过期；payload 已落 inbox，恢复 daemon 后才会消费", file=diagnostic_stream)
            print("请先恢复 daemon，再用 sched verify 确认批次入队", file=diagnostic_stream)
        elif tick_age is None or health.get("frozen"):
            print("⚠️ daemon 心跳存在但调度 tick 未确认完成；请检查 daemon.log 后再用 sched verify", file=diagnostic_stream)
        else:
            print("由 daemon 扫描消费入队 (下一 tick); sched verify 确认结果", file=diagnostic_stream)
        return 0

    # Foreign-host or first-run dry-run cannot create/migrate/write state.db.
    # Dependency and producer state is UNAVAILABLE; the daemon rechecks on submit.
    if not stateless_dry_run:
        # Strict native profile names are durable one-shot capabilities.  This
        # early read avoids git/fingerprint work for an already consumed name;
        # the authoritative check is repeated under submission_connect below.
        if norm["mode"] == "strict" and not dry_run:
            with state.connect() as conn:
                consumed = conn.execute(
                    "SELECT 1 FROM batches WHERE name=? LIMIT 1",
                    (norm["name"],),
                ).fetchone()
            if consumed is not None:
                print(
                    f"错误: strict 批次名 '{norm['name']}' 已消费；"
                    "必须配置新的 native profile/batch name 并重启 daemon",
                    file=sys.stderr,
                )
                return 1
        # 依赖 name 存在性 (O1): 提交时解析为最新同 name 批次 id
        with state.connect() as conn:
            try:
                validate_persisted_dependencies(
                    conn,
                    norm["name"],
                    norm["depends_on"],
                    norm.get("depends_on_exact", []),
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
        native_metadata_error = _native_exec_metadata_error(t, cmd_e)
        if native_metadata_error is not None:
            print(f"校验失败: {native_metadata_error}", file=sys.stderr)
            return 1
        fp, stage_fps, _rev = compute_fingerprint(
            cmd_e, stages_e, t["cwd_abs"], t["git"], cfg.get("venvs", {}),
            runtime_prefix=t.get("runtime_prefix"),
            execution_env=task_environment(cfg, norm.get("env"), t.get("env")),
            artifacts=t.get("artifacts"),
            **_native_exec_fingerprint_kwargs(t),
        )
        try:
            recovery.freeze(t, fp)
        except recovery.RecoveryError as error:
            print(f"校验失败: {error}", file=sys.stderr)
            return 1
        prepared_tasks.append((i, t, cmd_e, stages_e, fp, stage_fps))
    if stateless_dry_run:
        prev = _dry_run_preview(norm, cfg, use_state=False)
        _print_dry_run_preview(norm, args, prev, conflict=False)
        return 0

    db_context = state.connect() if dry_run else state.submission_connect()
    with db_context as conn:
        if not dry_run:
            try:
                if ticket is not None:
                    from .integration import check_submission
                    check_submission(conn, ticket, spec, norm.get("project"))
                validate_project_gpu_access(_load_cfg(), norm.get("project"), norm["tasks"])
                # Fingerprint expansion above may take long enough for another
                # submit to replace a dependency name.  The submission gate is
                # the commit-time authority, so validate the latest graph again.
                validate_persisted_dependencies(
                    conn,
                    norm["name"],
                    norm["depends_on"],
                    norm.get("depends_on_exact", []),
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
        strict_consumed = norm["mode"] == "strict" and bool(existing)
        ordinary_conflict = any(
            b["status"] not in ("done", "blocked", "discarded")
            for b in existing
        )
        conflict = strict_consumed or ordinary_conflict
        if conflict and not dry_run:
            if strict_consumed:
                print(
                    f"错误: strict 批次名 '{norm['name']}' 已消费；"
                    "必须配置新的 native profile/batch name 并重启 daemon",
                    file=sys.stderr,
                )
            else:
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
                failure_policy=norm["failure_policy"],
                exact_dependencies=norm.get("depends_on_exact", []),
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
            _persist_native_exec_metadata(t, spec_json)
            for key in ("depends_on", "depends_on_exact"):
                if key in t:
                    spec_json[key] = t[key]
            state.insert_task(
                conn, bid, t["id"], 1, spec_json, i,
                norm.get("project"),
            )
            state.insert_job(
                conn, f"{bid}-{t['id']}-v1", bid, t["id"], 1,
                fp, stage_fps, norm.get("project"),
            )
        from .task_dependencies import bind_new_batch
        try:
            bind_new_batch(conn, bid)
        except (ValueError, TypeError, RecursionError) as error:
            raise state.StateError(f"任务依赖接受失败: {error}") from error
        if ticket is not None:
            from .integration import complete_submission
            complete_submission(conn, ticket, {"outcome": "accepted", "batch_id": bid,
                "delivery": "database", "persisted": True, "project": norm.get("project"),
                "tasks": len(norm["tasks"])})
    wake_deferred = state.defer_after_commit(_ensure_running_locked)
    # BugFix (2026-08-26, sd_repro_v3 消失事故): "已入队"/ensure_running 此前
    # 在 with 事务块**内部** —— commit 发生在块退出时, 若 ensure_running 抛
    # 异常 (如 NFS 读配置瞬断 -> ConfigError), 整个事务回滚但 "已入队" 已
    # 打印, 用户以为成功实际批次消失。打印必须在提交之后。
    if getattr(args, "json", False):
        print(json.dumps({"schema_version": 1, "batch_id": bid,
                          "delivery": "database", "persisted": True,
                          "tasks": len(norm["tasks"]), "project": norm.get("project")},
                         ensure_ascii=False))
    else:
        print(f"已入队: {bid} ({len(norm['tasks'])} 任务, mode={norm['mode']})")
    if not wake_deferred:
        if getattr(args, "json", False):
            try:
                print(_ensure_running_locked(), file=sys.stderr)
            except Exception as error:
                # Submission already committed; daemon startup is independent.
                print(f"批次已持久化，但 daemon 唤醒失败: {error}", file=sys.stderr)
        else:
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
    try:
        reserved_native_names = native_exec_reserved_batch_names(cfg)
    except NativeExecProfileError as error:
        print(f"错误: native_exec_profiles 配置非法: {error}", file=sys.stderr)
        return 1
    if batch_name in reserved_native_names:
        print(
            f"错误: sched run 生成的批次名 '{batch_name}' 已由 "
            "admin native_exec_profile 保留；拒绝 mix 快捷提交",
            file=sys.stderr,
        )
        return 1

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
    if getattr(args, "host_mem_gib", None) is not None:
        resources["host_mem_gib"] = args.host_mem_gib

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

    try:
        validate_project_gpu_access(cfg, proj, [task_spec])
    except SchemaError as error:
        print(f"校验失败: {error}", file=sys.stderr)
        return 1

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
        try:
            validate_project_gpu_access(_load_cfg(), proj, [task_spec])
        except SchemaError as error:
            print(f"校验失败: {error}", file=sys.stderr)
            return 1
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
    """The same read-only health contract as daemon status --json."""
    try:
        cfg = _load_cfg()
    except (Exception, SystemExit):
        cfg = {}
    if not cfg.get("node"):
        return {}
    from .daemon import health_snapshot
    return health_snapshot()


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
        rejection = None
        if not row:
            prefix = f"submit {json.dumps(reference, ensure_ascii=True)} => "
            receipt = conn.execute(
                "SELECT result FROM control_requests WHERE op='batch_submit'"
                " AND status='done' AND substr(result,1,?)=? ORDER BY id DESC LIMIT 1",
                (len(prefix), prefix),
            ).fetchone()
            if receipt:
                rejection = receipt["result"][len(prefix):]
    if not row:
        if rejection:
            print(f"❌ 批次投递被拒绝: {reference}\n   {rejection}")
            return 1
        print(f"❌ 未找到批次: {reference}")
        print("   批次可能仍在等待 inbox 消费；请使用投递时的完整 batch ID 核对回执")
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
    if getattr(args, "include_cpu_capacity", False) and not args.json:
        print("错误: --include-cpu-capacity 仅用于 status --json", file=sys.stderr)
        return 1
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
            if (status == "pending" and resources.get("gpu", 1) != 0
                    and not project_gpu_enabled(cfg, job["project"])):
                wait_reason = "project_gpu_disabled"
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

        from . import cpu_capacity
        try:
            cpu_used = cpu_capacity.reserved(conn, cfg, lambda spec: _task_cpus_of(spec.get("resources") or {}, cfg))
        except cpu_capacity.CpuReservationUnknown:
            cpu_used = None
        capacity = cpu_capacity.recorded(conn, cfg)
        # Unknown auto is never represented as zero/unlimited or a string in
        # the default strict two-integer CPU contract accepted by old clients.
        if cpu_used is not None and (capacity["mode"] != "auto" or capacity["available"]):
            out["cpu"] = {"used": cpu_used, "total": capacity["effective_total"]}
        if getattr(args, "include_cpu_capacity", False):
            from .integration import CONTRACTS
            out["cpu_capacity"] = {"contract": CONTRACTS["cpu_capacity"], "used": cpu_used, **capacity}

        from . import resources as admission
        memory_used = admission.memory_usage(conn, cfg)
        snapshot = admission.admission_snapshot()
        sample = snapshot.get("sample")
        limit = cfg.get("host_mem_total_gib", 0)
        if limit:
            out["host_memory"] = {
                "used_gib": memory_used, "total_gib": limit,
                "reserve_gib": cfg.get("host_mem_reserve_gib", 16),
                "default_job_gib": cfg.get("host_mem_default_gib", 8),
                "available_gib": sample.get("MemAvailable") if isinstance(sample, dict) else None,
            }
        draining = admission.drain_state() is not None
        batch_states = {b["id"]: b["status"] for b in out["batches"]}
        for job in out["jobs"]:
            if job["status"] != "pending" or job["wait_reason"] == "project_gpu_disabled":
                continue
            batch_status = batch_states.get(job["batch_id"])
            if batch_status == "queued":
                job["wait_reason"] = "dependency"
            elif batch_status != "active":
                job["wait_reason"] = "batch_blocked"
            elif draining:
                job["wait_reason"] = "draining"
            elif job["wait_reason"] is None:
                res = job["resources"]
                try:
                    requested_memory = admission.host_mem_gib({"resources": res}, cfg)
                except (TypeError, ValueError, AttributeError):
                    requested_memory = limit + 1  # Legacy invalid spec awaits daemon rejection.
                if cpu_used is None or (capacity["mode"] == "auto" and not capacity["available"]) or (capacity["effective_total"] and cpu_used + _task_cpus_of(res, cfg) > capacity["effective_total"]):
                    job["wait_reason"] = "cpu"
                elif limit and memory_used + requested_memory > limit:
                    job["wait_reason"] = "host_memory"
                else:
                    reason = snapshot.get("waits", {}).get(job["id"])
                    if reason in admission.WAIT_REASONS:
                        job["wait_reason"] = reason

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
        print(f"=== CPU ===\n  声明预留 {cpu['used']} / {total_text} 核 (非实测利用率)")
    memory = out.get("host_memory")
    if memory:
        print(f"=== 主机内存 ===\n  声明预留 {memory['used_gib']:g} / {memory['total_gib']:g} GiB")
    return 0


def cmd_batch_policy(args: argparse.Namespace) -> int:
    """Read policy without migrating, or mutate within a CAS-bound request."""
    from .batch_policy import failure_policy, validate_failure_policy
    from .integration import CONTRACTS, instance_id
    writing = args.failure_policy is not None
    if writing and state._bound_connection.get() is None:
        print("错误: batch-policy 写操作必须通过完整 batch CAS 的 sched request", file=sys.stderr)
        return 64
    if not writing and args.reopen:
        print("错误: --reopen 需要 --failure-policy continue_independent", file=sys.stderr)
        return 64
    if writing and not args.yes:
        print("未确认: batch-policy 写操作需要 --yes", file=sys.stderr)
        return 1
    state.set_read_only(not writing)
    try:
        cfg = load_config()
        if writing and _is_foreign_host(cfg) and os.environ.get("SCHED_ALLOW_FOREIGN_WRITE") != "1":
            print("错误: batch-policy 写操作必须在配置的计算节点执行", file=sys.stderr)
            return 2
        with state.connect() as conn:
            if not conn.in_transaction:
                conn.execute("BEGIN")
            batch_id = args.batch if writing else _resolve_batch_ref(args.batch, conn)
            batch = state.get_batch(conn, batch_id)
            if batch is None:
                print("错误: 批次不存在", file=sys.stderr)
                return 65 if writing else 1
            stored = "failure_policy" in batch.keys()
            if writing:
                policy = validate_failure_policy(args.failure_policy)
                if not stored:
                    raise state.StateError("batch-policy 写操作需要 schema 11")
                if batch["mode"] != "mix":
                    print("错误: 历史 strict 批次不能通过 batch-policy 修改或重开", file=sys.stderr)
                    return 65
                if batch["status"] not in ("queued", "active", "blocked"):
                    print("错误: 终态/退役批次不能修改 failure_policy", file=sys.stderr)
                    return 65
                if args.reopen:
                    eligible = conn.execute(
                        "SELECT 1 FROM jobs j WHERE batch_id=? AND status IN ('pending','waiting_quota','waiting_dep','running')"
                        " AND version=(SELECT MAX(j2.version) FROM jobs j2"
                        " WHERE j2.batch_id=j.batch_id AND j2.task_id=j.task_id) LIMIT 1", (batch_id,),
                    ).fetchone()
                    if policy != "continue_independent" or batch["status"] != "blocked" or not eligible:
                        print("错误: --reopen 仅适用于有未完成任务的 blocked 批次和 continue_independent", file=sys.stderr)
                        return 65
                conn.execute("UPDATE batches SET failure_policy=?,status=? WHERE id=?",
                             (policy, "active" if args.reopen else batch["status"], batch_id))
                batch = state.get_batch(conn, batch_id)
            output = {"schema_version": 1, "query": "batch_policy", "contract": CONTRACTS["batch_policy"],
                      "instance_id": instance_id(conn), "batch_id": batch_id, "project": batch["project"],
                      "batch_revision": batch["revision"], "status": batch["status"],
                      "failure_policy": failure_policy(batch), "source": "stored" if stored else "legacy_default",
                      "effect": "policy_updated" if writing else "none", "task_dag_supported": _task_dag_available(conn)}
        if args.json:
            print(json.dumps(output, ensure_ascii=False))
        else:
            print(f"{batch_id}: failure_policy={output['failure_policy']} status={output['status']} revision={output['batch_revision']}")
        return 0
    except (ValueError, ConfigError, state.StateError) as error:
        print(f"错误: {error}", file=sys.stderr)
        return 1
    finally:
        state.set_read_only(False)


def cmd_batch_dependencies(args: argparse.Namespace) -> int:
    from . import dependencies
    from .integration import CONTRACTS, instance_id
    from .execution_policy import digest
    state.set_read_only(True)
    try:
        load_config()
        if not 1 <= args.limit <= 1000 or args.cursor < 0:
            raise ValueError("limit 必须为 1..1000，cursor 必须为非负偏移量")
        with state.connect() as conn:
            conn.execute("BEGIN")
            batch_id = _resolve_batch_ref(args.batch, conn)
            batch = state.get_batch(conn, batch_id)
            if batch is None:
                raise ValueError("批次不存在")
            facts = dependencies.facts(conn, batch)
            selected = facts[args.cursor:args.cursor + args.limit]
            more = args.cursor + len(selected) < len(facts)
            output = {"schema_version": 1, "query": "batch_dependencies", "contract": CONTRACTS["batch_dependencies"],
                      "instance_id": instance_id(conn), "batch_id": batch_id, "batch_revision": batch["revision"],
                      "status": batch["status"], "source": "stored" if "depends_on_exact" in batch.keys() else "legacy_only",
                      "binding_sha256": digest({"legacy": json.loads(batch["depends_on"] or "[]"), "exact": dependencies.stored(batch)}),
                      "dependencies": selected, "total": len(facts), "truncated": more,
                      "next_cursor": args.cursor + len(selected) if more else None,
                      "effect": "none", "external_artifacts_checked": False, "task_dag_supported": _task_dag_available(conn)}
            output["launch_markers_checked"] = False
        if args.json:
            print(json.dumps(output, ensure_ascii=False))
        else:
            print(f"{batch_id}: {output['total']} dependency facts; recorded state only")
        return 0
    except (ValueError, TypeError, ConfigError, state.StateError) as error:
        print(f"错误: {error}", file=sys.stderr)
        return 1
    finally:
        state.set_read_only(False)


def _task_dag_available(conn):
    from .task_dependencies import available
    return available(conn)


def _dependency_selectors(raw):
    from .dependencies import normalize
    if not isinstance(raw, str) or len(raw.encode()) > 64 * 1024:
        raise ValueError("dependencies-json 超过 64 KiB 上限")
    return normalize(json.loads(raw))


def cmd_dependency_update(args):
    from . import task_dependencies
    if state._bound_connection.get() is None or task_dependencies.request_id.get() is None:
        print("错误: dependency-update 必须通过完整 task CAS 的 request", file=sys.stderr)
        return 64
    if not args.yes:
        print("未确认: dependency-update 需要 --yes", file=sys.stderr)
        return 1
    try:
        cfg = load_config()
        if _is_foreign_host(cfg):
            raise ValueError("dependency-update 必须在配置的计算节点执行")
        batch, task = args.task.split(":", 1)
        with state.connect() as conn:
            job = conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id=? ORDER BY version DESC LIMIT 1", (batch, task)).fetchone()
            if job is None:
                raise ValueError("任务不存在")
            event_id = task_dependencies.update(conn, job, _dependency_selectors(args.dependencies_json), reopen=args.reopen)
        print(json.dumps({"dependency_event_id": event_id}))
        return 0
    except (ValueError, TypeError, state.StateError, ConfigError, RecursionError) as error:
        print(f"错误: {error}", file=sys.stderr)
        return 65


def cmd_task_dependencies(args):
    from . import task_dependencies
    from .integration import CONTRACTS, instance_id
    state.set_read_only(True)
    try:
        load_config()
        if not 1 <= args.limit <= 1000 or not 0 <= args.cursor <= 10_000 or (args.version is not None and args.version < 1):
            raise ValueError("limit 需 1..1000、cursor 需 0..10000、version 需正整数")
        with state.connect() as conn:
            conn.execute("BEGIN")
            batch_ref, task = args.task.split(":", 1)
            batch = _resolve_batch_ref(batch_ref, conn)
            job = conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id=?" + (" AND version=?" if args.version else "") + " ORDER BY version DESC LIMIT 1",
                               (batch, task, args.version) if args.version else (batch, task)).fetchone()
            if job is None:
                raise ValueError("任务不存在")
            if args.event_id:
                if args.cursor:
                    raise ValueError("event-id 与 cursor 互斥")
                event = conn.execute("SELECT * FROM task_dependency_events WHERE event_id=? AND job_id=?", (args.event_id, job["id"])).fetchone() if task_dependencies.available(conn) else None
                if event is None:
                    raise ValueError("该任务版本没有此依赖事件")
                latest = task_dependencies.latest(conn, job)
                result = {"event": task_dependencies.decode_event(event), "effective": latest["event_id"] == event["event_id"],
                          "external_artifacts_checked": False, "launch_markers_checked": False}
            else:
                result = task_dependencies.facts(conn, job, limit=args.limit, cursor=args.cursor)
            result.update(schema_version=1, contract=CONTRACTS["task_dependencies"], query="task_dependencies",
                          instance_id=instance_id(conn), batch_id=batch, task_id=task, version=job["version"],
                          batch_revision=state.get_batch(conn, batch)["revision"], effect="none",
                          task_dag_supported=task_dependencies.available(conn), dispatch_ready=None)
        print(json.dumps(result, ensure_ascii=False) if args.json else f"{batch}:{task}: recorded task dependency facts")
        return 0
    except (ValueError, TypeError, state.StateError, ConfigError, RecursionError) as error:
        print(f"错误: {error}", file=sys.stderr)
        return 1
    finally:
        state.set_read_only(False)


def cmd_admission_explain(args):
    from . import admission
    state.set_read_only(True)
    try:
        cfg = load_config()
        batch_ref, task_id = _parse_task_ref(args.task)
        with state.connect() as conn:
            conn.execute("BEGIN")
            batch_id = _resolve_batch_ref(batch_ref, conn)
            batch = state.get_batch(conn, batch_id) if batch_id else None
            if batch is None:
                raise ValueError("批次不存在")
            version = args.version
            if version is None:
                version = conn.execute("SELECT MAX(version) FROM jobs WHERE batch_id=? AND task_id=?", (batch_id, task_id)).fetchone()[0]
            job = conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id=? AND version=?", (batch_id, task_id, version)).fetchone()
            raw = conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?", (batch_id, task_id, version)).fetchone()
            if job is None or raw is None:
                raise ValueError("精确任务版本不存在")
            if len(raw[0].encode()) > 1024 * 1024:
                raise ValueError("任务 spec 超过 1 MiB 查询容量")
            spec = json.loads(raw[0])
            if not isinstance(spec, dict):
                raise ValueError("任务 spec 不是对象")
            output = admission.explain(conn, cfg, batch, job, spec)
        print(json.dumps(output, ensure_ascii=False, allow_nan=False) if args.json else
              f"{job['id']}: {', '.join(output['reasons'] + output['unknown']) or '资源快照可容纳'}；未授予派发权")
        return 0
    except (ValueError, TypeError, KeyError, state.StateError, ConfigError, RecursionError, OverflowError) as error:
        print(f"错误: {error}", file=sys.stderr)
        return 1
    finally:
        state.set_read_only(False)


def cmd_task_facts(args):
    from . import pending_cancel
    from .integration import CONTRACTS, instance_id
    state.set_read_only(True)
    try:
        load_config()
        selectors = pending_cancel.normalize(args.tasks_json)
        with state.connect() as conn:
            conn.execute("BEGIN")
            batch = dict(pending_cancel.exact_batch(conn, args.batch))
            tasks = pending_cancel.facts(conn, batch, selectors)
            output = {"schema_version": 1, "contract": CONTRACTS["task_facts"], "query": "task_facts",
                      "instance_id": instance_id(conn), "batch_id": batch["id"], "batch_revision": batch.get("revision"),
                      "batch_status": batch["status"], "tasks": tasks, "truncated": False,
                      "effect": "none", "cancel_ready": None, "launch_markers_checked": False,
                      "external_artifacts_checked": False, "source": "recorded_private_snapshot"}
        print(json.dumps(output, ensure_ascii=False) if args.json else f"{batch['id']}: {len(tasks)} exact task facts; local startup files not checked")
        return 0
    except (ValueError, TypeError, state.StateError, ConfigError, RecursionError) as error:
        print(f"错误: {error}", file=sys.stderr)
        return 1
    finally:
        state.set_read_only(False)


def cmd_cancel_pending(args):
    from . import pending_cancel
    if state._bound_connection.get() is None or pending_cancel.request_id.get() is None:
        print("错误: cancel-pending 必须通过完整 batch/instance CAS 的 request", file=sys.stderr)
        return 64
    if not args.yes:
        print("未确认: cancel-pending 需要 --yes", file=sys.stderr)
        return 1
    try:
        cfg = load_config()
        if _is_foreign_host(cfg):
            raise ValueError("cancel-pending 必须在配置的计算节点执行")
        selectors = pending_cancel.normalize(args.tasks_json, bindings=True)
        with state.connect() as conn:
            result = pending_cancel.perform(conn, args.batch, selectors)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (ValueError, TypeError, state.StateError, ConfigError, RecursionError) as error:
        print(f"错误: {error}", file=sys.stderr)
        return 65


def cmd_artifact_check(args: argparse.Namespace) -> int:
    """Read current files against one frozen task spec; never settle a job."""
    from .integration import CONTRACTS, instance_id
    state.set_read_only(True)
    try:
        cfg = load_config()
        if _is_foreign_host(cfg):
            print("错误: artifact-check 必须在配置的计算节点检查任务文件", file=sys.stderr)
            return 2
        if args.version is not None and args.version < 1:
            raise ValueError("version 必须为正整数")
        with state.connect() as conn:
            conn.execute("BEGIN")
            batch, task_id = _resolve_task_ref(args.task, conn)
            params = (batch, task_id, args.version) if args.version is not None else (batch, task_id)
            job = conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id=?" +
                               (" AND version=?" if args.version is not None else "") + " ORDER BY version DESC LIMIT 1", params).fetchone()
            if job is None:
                raise ValueError("任务或版本不存在")
            row = conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?", (batch, task_id, job["version"])).fetchone()
            if row is None:
                raise ValueError("任务 spec 不存在")
            spec = json.loads(row["spec"])
            if not isinstance(spec, dict):
                raise ValueError("任务 spec 无效")
            revision = conn.execute("SELECT revision FROM batches WHERE id=?", (batch,)).fetchone()[0]
            output = {"schema_version": 1, "query": "artifact_check", "contract": CONTRACTS["artifact_check"],
                      "instance_id": instance_id(conn), "batch_id": batch, "batch_revision": revision,
                      "job_id": job["id"], "task_id": task_id, "version": job["version"], "recorded_status": job["status"],
                      "recorded_rc": job["rc"], "effect": "none", "historical_failure_reconstructed": False}
        # No DB transaction spans file reads or isolated regex checks.
        checks = artifacts.inspect_declared_artifacts(spec, spec.get("cwd_abs") or ".")
        output.update(observed_at=state.now(), checks=checks, passed=all(check["passed"] for check in checks))
        if args.json:
            print(json.dumps(output, ensure_ascii=False))
        else:
            print("当前产物规则通过" if output["passed"] else "当前产物规则未通过")
            for check in checks:
                print(f"  {check['scope']}:{check['name']}: {check['reason_code']}")
            print("只读检查；未改变任务状态，不能代替执行事实或科学验收")
        return 0 if output["passed"] else 1
    except (ValueError, ConfigError, state.StateError, RecursionError) as error:
        print(f"错误: {error}", file=sys.stderr)
        return 1
    finally:
        state.set_read_only(False)


def cmd_cpu_scopes(args: argparse.Namespace) -> int:
    from . import cpu_scope_state
    from .integration import CONTRACTS, instance_id
    state.set_read_only(True)
    try:
        cfg = load_config()
        with state.connect() as conn:
            conn.execute("BEGIN")
            result = {"schema_version": 1, "query": "cpu_scopes", "contract": CONTRACTS["cpu_scopes"],
                      "instance_id": instance_id(conn), "node": str(cfg["node"]), "effect": "none",
                      **cpu_scope_state.query(conn, scope_id=args.scope_id, limit=args.limit, cursor=args.cursor)}
        print(json.dumps(result, ensure_ascii=False, indent=None if args.json else 2))
        return 0
    except (ValueError, ConfigError, state.StateError, TypeError, KeyError, sqlite3.Error, RecursionError) as error:
        print(f"错误: cpu-scopes 查询失败: {error}", file=sys.stderr)
        return 1
    finally:
        state.set_read_only(False)


def cmd_cpu_isolation(args: argparse.Namespace) -> int:
    from . import cpu_isolation
    from .integration import CONTRACTS, instance_id
    state.set_read_only(True)
    try:
        cfg = load_config()
        with state.connect() as conn:
            conn.execute("BEGIN")
            result = {"schema_version": 1, "query": "cpu_isolation", "contract": CONTRACTS["cpu_isolation"],
                      "instance_id": instance_id(conn), "node": state.hostname(), "effect": "none",
                      **cpu_isolation.query(conn, cfg, limit=args.limit, cursor=args.cursor)}
        print(json.dumps(result, ensure_ascii=False, indent=None if args.json else 2))
        return 0
    except (ValueError, ConfigError, state.StateError, TypeError, KeyError, sqlite3.Error, RecursionError) as error:
        print(f"错误: cpu-isolation 查询失败: {error}", file=sys.stderr)
        return 1
    finally:
        state.set_read_only(False)


def cmd_cpu_capacity(args: argparse.Namespace) -> int:
    from . import cpu_capacity
    from .integration import CONTRACTS, instance_id
    state.set_read_only(True)
    try:
        cfg = load_config()
        with state.connect() as conn:
            conn.execute("BEGIN")
            try:
                used = cpu_capacity.reserved(conn, cfg, lambda spec: _task_cpus_of(spec.get("resources") or {}, cfg))
            except cpu_capacity.CpuReservationUnknown:
                used = None
            result = {"schema_version": 1, "query": "cpu_capacity", "contract": CONTRACTS["cpu_capacity"],
                      "instance_id": instance_id(conn), "node": state.hostname(), "effect": "none",
                      "used": used,
                      "reservation_error": "legacy_running_cpu_reservation_unknown" if used is None else None,
                      **cpu_capacity.recorded(conn, cfg)}
        print(json.dumps(result, ensure_ascii=False, indent=None if args.json else 2))
        return 0
    except (ValueError, ConfigError, state.StateError, TypeError, KeyError, sqlite3.Error, RecursionError) as error:
        print(f"错误: cpu-capacity 查询失败: {error}", file=sys.stderr)
        return 1
    finally:
        state.set_read_only(False)


def _daemon_lease_snapshot(*, lease_id=None, limit=20, cursor=None, after_seq=0, current=False):
    from .cluster_lease import query
    from .integration import CONTRACTS, instance_id
    from .daemon import _read_lease_owner
    state.set_read_only(True)
    try:
        load_config()
        with state.connect() as conn:
            conn.execute("BEGIN")
            if current and conn.execute("PRAGMA user_version").fetchone()[0] >= 17:
                owner = _read_lease_owner()
                latest = conn.execute("SELECT lease_id FROM daemon_leases ORDER BY rowid DESC LIMIT 1").fetchone()
                lease_id = owner["lease_id"] if owner else latest[0] if latest else None
                # An older owner can lack provenance; do not fabricate birth.
                if lease_id and conn.execute("SELECT 1 FROM daemon_leases WHERE lease_id=?", (lease_id,)).fetchone() is None:
                    return {"schema_version": 1, "contract": CONTRACTS["daemon_lease"], "available": False,
                            "reason": "origin_not_recorded", "leases": [], "effect": "none", "admission_granted": False}
            return {"schema_version": 1, "query": "daemon_lease", "contract": CONTRACTS["daemon_lease"],
                    "instance_id": instance_id(conn), "effect": "none", "admission_granted": False,
                    **query(conn, lease_id=lease_id, limit=limit, cursor=cursor, after_seq=after_seq)}
    finally:
        state.set_read_only(False)


def cmd_daemon_lease(args: argparse.Namespace) -> int:
    try:
        result = _daemon_lease_snapshot(lease_id=args.lease_id, limit=args.limit, cursor=args.cursor, after_seq=args.after_seq)
        print(json.dumps(result, ensure_ascii=False, indent=None if args.json else 2))
        return 0
    except (ValueError, ConfigError, state.StateError, TypeError, KeyError, RecursionError, sqlite3.Error) as error:
        print(f"错误: daemon-lease 查询失败: {error}", file=sys.stderr)
        return 1


def cmd_storage_explain(args: argparse.Namespace) -> int:
    from .storage import explain
    from .integration import CONTRACTS, instance_id
    state.set_read_only(True)
    try:
        cfg = load_config()
        with state.connect() as conn:
            conn.execute("BEGIN")
            batch, task_id = _resolve_task_ref(args.task, conn)
            if args.version is not None and args.version < 1:
                raise ValueError("version 必须为正整数")
            params = (batch, task_id, args.version) if args.version is not None else (batch, task_id)
            job = conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id=?" +
                               (" AND version=?" if args.version is not None else "") + " ORDER BY version DESC LIMIT 1", params).fetchone()
            if job is None:
                raise ValueError("精确任务/版本不存在")
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?", (batch, task_id, job["version"])).fetchone()[0])
            result = {"schema_version": 1, "query": "storage_explain", "contract": CONTRACTS["storage_explain"],
                      "instance_id": instance_id(conn), "batch_id": batch, "task_id": task_id, "version": job["version"],
                      "job_id": job["id"],
                      "effect": "none", "admission_granted": False, "hard_isolation": False, **explain(conn, cfg, job, spec)}
        print(json.dumps(result, ensure_ascii=False) if args.json else "存储准入记录（只读，不授予派发权）\n" + json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (ValueError, ConfigError, state.StateError, TypeError, RecursionError) as error:
        print(f"错误: {error}", file=sys.stderr)
        return 1
    finally:
        state.set_read_only(False)


def cmd_allocations(args: argparse.Namespace) -> int:
    from .allocation import query
    from .integration import CONTRACTS, instance_id
    state.set_read_only(True)
    try:
        load_config()
        with state.connect() as conn:
            conn.execute("BEGIN")
            batch, task_id = _resolve_task_ref(args.task, conn)
            if conn.execute("SELECT 1 FROM jobs WHERE batch_id=? AND task_id=?", (batch, task_id)).fetchone() is None:
                raise ValueError("任务不存在")
            result = query(conn, batch, task_id, version=args.version, allocation_id=args.allocation_id,
                           limit=args.limit, cursor=args.cursor)
            output = {"schema_version": 1, "query": "allocations", "contract": CONTRACTS["allocations"],
                      "instance_id": instance_id(conn), "batch_id": batch, "task_id": task_id,
                      "version_filter": args.version, "effect": "none", "settlement_authority": False,
                      "historical_failure_reconstructed": False, "worker_identity_inferred": False, **result}
        if args.json:
            print(json.dumps(output, ensure_ascii=False))
        else:
            print("不可变分配/分层观察（只读，不授予执行或结算权）")
            for item in result["allocations"]:
                print(f"  {item['allocation_id']} v{item['version']} attempt={item['ordinal']}")
        return 0
    except (ValueError, ConfigError, state.StateError, RecursionError) as error:
        print(f"错误: {error}", file=sys.stderr)
        return 1
    finally:
        state.set_read_only(False)


def cmd_artifact_validations(args: argparse.Namespace) -> int:
    """Query frozen completion observations; do not inspect files or migrate."""
    if args._subcommand == "artifact-revalidations":
        from .artifact_revalidation import list_records as event_records
        list_records = lambda conn, batch, task, **kw: event_records(conn, batch, task,
            version=kw["version"], limit=kw["limit"], cursor=kw["cursor"], event_id=kw["validation_id"])
        name, items = "artifact_revalidations", "events"
    else:
        from .artifact_validation import list_records
        name, items = "artifact_validations", "validations"
    from .integration import CONTRACTS, instance_id
    state.set_read_only(True)
    try:
        load_config()
        with state.connect() as conn:
            conn.execute("BEGIN")
            batch, task_id = _resolve_task_ref(args.task, conn)
            params = (batch, task_id, args.version) if args.version is not None else (batch, task_id)
            job = conn.execute("SELECT id FROM jobs WHERE batch_id=? AND task_id=?" +
                               (" AND version=?" if args.version is not None else "") + " LIMIT 1", params).fetchone()
            if job is None:
                raise ValueError("任务或版本不存在")
            result = list_records(conn, batch, task_id, version=args.version, limit=args.limit,
                                  cursor=args.cursor, validation_id=args.validation_id)
            output = {"schema_version": 1, "query": name,
                      "contract": CONTRACTS[name], "instance_id": instance_id(conn),
                      "batch_id": batch, "task_id": task_id, "version_filter": args.version,
                      "historical_failure_reconstructed": False, "settlement_authority": False,
                      "evidence_included": args.validation_id is not None, **result}
        if args.json:
            print(json.dumps(output, ensure_ascii=False))
        else:
            label = "产物复验事件" if name == "artifact_revalidations" else "首次产物验证记录"
            print(label + "（只读，不重新结算）" if result["available"] else "旧库未保存此类记录；查询不迁移")
            for record in result[items]:
                print(f"  {record.get('validation_id') or record.get('event_id')} v{record['job_version']} passed={record['passed']}")
        return 0
    except (ValueError, ConfigError, state.StateError, RecursionError) as error:
        print(f"错误: {error}", file=sys.stderr)
        return 1
    finally:
        state.set_read_only(False)


def cmd_artifact_revalidate(args: argparse.Namespace) -> int:
    from . import artifact_revalidation as revalidation, artifact_validation as initial
    if state._bound_connection.get() is None or revalidation.request_id.get() is None:
        print("错误: artifact-revalidate 必须通过完整 task CAS 的 sched request", file=sys.stderr)
        return 64
    if not args.yes:
        print("未确认: artifact-revalidate 需要 --yes", file=sys.stderr)
        return 1
    try:
        cfg = load_config()
        if _is_foreign_host(cfg):
            raise ValueError("artifact-revalidate 必须在配置的计算节点执行")
        batch, task = args.task.split(":", 1)
        with state.connect() as conn:
            job = conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id=? ORDER BY version DESC LIMIT 1", (batch, task)).fetchone()
            if job is None:
                raise ValueError("任务不存在")
            source = conn.execute("SELECT * FROM artifact_validations WHERE validation_id=?", (args.validation_id,)).fetchone()
            if source is None:
                raise ValueError("原始验证记录不存在，不能从当前文件补造")
            spec = json.loads(conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?", (batch, task, job["version"])).fetchone()[0])
            result = revalidation.perform(conn, job, spec, initial.decode(source), settle=args.settle,
                                          reopen=args.reopen, system_retries=args.system_retries)
        print(json.dumps({"query": "artifact_revalidation", "event_id": result["event_id"],
                          "passed": result["passed"], "settled": result["settled"], "reason": result["reason"]}, ensure_ascii=False))
        return 0
    except (ValueError, state.StateError, ConfigError) as error:
        print(f"错误: {error}", file=sys.stderr)
        return 65


def cmd_execution(args: argparse.Namespace) -> int:
    if args.task == "list":
        from .execution_queries import list_executions
        if args.version is not None:
            print("错误: execution list 不接受 --version", file=sys.stderr)
            return 1
        try:
            with state.connect() as conn:
                conn.execute("BEGIN")
                batch = _resolve_batch_ref(args.batch, conn=conn) if args.batch else None
                if args.batch and batch is None:
                    raise ValueError("批次不存在")
                output = list_executions(conn, project=args.project, batch=batch, backend=args.backend,
                    phase=args.phase, owner_status=args.owner_status,
                    limit=args.limit if args.limit is not None else 50, cursor=args.cursor)
        except (ValueError, state.StateError) as error:
            print(f"错误: {error}", file=sys.stderr)
            return 1
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    if any(getattr(args, key, None) is not None for key in ("project", "batch", "backend", "phase", "owner_status", "limit", "cursor")):
        print("错误: 列表筛选参数只用于 execution list", file=sys.stderr)
        return 1
    from . import execution_state
    from .execution_diagnostics import legacy_session, summarize
    batch, task = _resolve_task_ref(args.task)
    if args.version is not None and args.version < 1:
        print("错误: version 必须为正整数", file=sys.stderr)
        return 1
    with state.connect() as conn:
        conn.execute("BEGIN")
        batch_row = state.get_batch(conn, batch)
        if batch_row is None:
            print("错误: 批次不存在", file=sys.stderr)
            return 1
        batch_revision = batch_row["revision"]
        from .integration import instance_id
        execution_instance = instance_id(conn)
        execution_project = batch_row["project"]
        jobs = conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id=?" +
                            (" AND version=?" if args.version is not None else "") + " ORDER BY version",
                            (batch, task, args.version) if args.version is not None else (batch, task)).fetchall()
        if not jobs:
            print("错误: 任务或版本不存在", file=sys.stderr)
            return 1
        attempts = []
        diagnostics = []
        legacy_sessions = []
        for job in jobs:
            row = execution_state.get(conn, job["id"])
            attempt = (execution_state.public(
                row, owner_binding=execution_state.get_owner_binding(conn, job["id"]),
                owner_health_record=execution_state.owner_health(conn, job["id"]))
                if row is not None else None)
            if row is not None:
                attempts.append(attempt)
            legacy = legacy_session(conn, job["id"])
            if legacy is not None:
                legacy_sessions.append(legacy)
            spec = conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
                                (batch, task, job["version"])).fetchone()
            diagnostics.append(summarize(job, spec["spec"] if spec else None,
                                         batch_row["mode"], attempt, legacy))
    output = {"schema_version": 1, "batch_id": batch, "batch_revision": batch_revision,
              "instance_id": execution_instance, "project": execution_project,
              "task_id": task, "attempts": attempts, "diagnostics": diagnostics,
              "legacy_sessions": legacy_sessions}
    print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


def cmd_recovery(args: argparse.Namespace) -> int:
    try:
        with state.connect() as conn:
            conn.execute("BEGIN")
            batch, task_id = _resolve_task_ref(args.task, conn)
            version = getattr(args, "version", None)
            if version is not None and version < 1:
                raise ValueError("version 必须为正整数")
            job = conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id=?" +
                (" AND version=?" if version is not None else "") + " ORDER BY version DESC LIMIT 1",
                (batch, task_id, version) if version is not None else (batch, task_id)).fetchone()
            if job is None:
                raise ValueError("任务不存在")
            row = conn.execute("SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?", (batch, task_id, job["version"])).fetchone()
            spec = json.loads(row["spec"])
            declaration = spec.get("recovery")
            durable = recovery_state.public(conn, job["id"])
            output = {"schema_version": 1, "batch_id": batch, "task_id": task_id,
                      "job_id": job["id"], "version": job["version"], "enabled": declaration is not None,
                      **durable}
            if declaration is not None:
                value = json.loads(recovery.context(state.host_dir(), job, spec, create=False))
                try:
                    checkpoint = recovery.CheckpointStore(value).load()
                    checkpoint_state = "absent" if checkpoint is None else "verified"
                    receipt = recovery.report(state.host_dir(), job, spec)
                    allowed = recovery.smoke_gate(conn, state.host_dir(), job, spec)
                except FileNotFoundError:
                    checkpoint_state, checkpoint, receipt = "absent", None, None
                    allowed = recovery.smoke_gate(conn, state.host_dir(), job, spec)
                except (ValueError, OSError):
                    checkpoint_state, checkpoint, receipt, allowed = "invalid", None, None, False
                output.update({"protocol": recovery.PROTOCOL, "mode": declaration["mode"],
                    "binding_sha256": spec["_recovery_binding"], "smoke_job_id": declaration.get("smoke_job_id"),
                    "smoke_ready": allowed, "checkpoint": {"state": checkpoint_state,
                    "payload_sha256": recovery.digest(checkpoint) if checkpoint_state == "verified" else None},
                    "report": receipt})
    except (ValueError, state.StateError) as error:
        print(f"错误: {error}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(output, ensure_ascii=False, indent=2))
    else:
        print(f"{output['job_id']}: recovery={'enabled' if output['enabled'] else 'disabled'}")
        if output["enabled"]:
            print(f"mode={output['mode']} smoke_ready={output['smoke_ready']} checkpoint={output['checkpoint']['state']}")
    return 0


def cmd_capabilities(args: argparse.Namespace) -> int:
    from .execution.capabilities import snapshot
    result = snapshot()
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        for name, backend in result["backends"].items():
            print(f"{name}: {backend['status']} ({backend['reason'] or ', '.join(backend['verified'])})")
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
            "SELECT name, revision, project FROM batches WHERE id=?",
            (batch,),
        ).fetchone()
        if batch_row is not None:
            output["batch_name"] = batch_row["name"]
            output["batch_revision"] = int(batch_row["revision"])
            output["project"] = batch_row["project"]
        from .integration import instance_id
        output["instance_id"] = instance_id(conn)
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


def _validate_gpu_requeue(conn, cfg: dict, batch: str, project: str | None, jobs) -> None:
    specs = []
    for job in jobs:
        row = conn.execute(
            "SELECT spec FROM tasks WHERE batch_id=? AND id=? AND version=?",
            (batch, job["task_id"], job["version"]),
        ).fetchone()
        try:
            spec = json.loads(row["spec"]) if row else None
        except (ValueError, TypeError) as error:
            raise SchemaError(f"任务 spec 损坏: {job['task_id']}") from error
        if not isinstance(spec, dict):
            raise SchemaError(f"任务 spec 缺失或非法: {job['task_id']}")
        specs.append(spec)
    validate_project_gpu_access(cfg, project, specs)


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
            "SELECT status, project, name, mode FROM batches WHERE id=?",
            (batch,),
        ).fetchone()
        if not batch_row:
            print(f"错误: 批次不存在: {ref}", file=sys.stderr)
            return 1
        if batch_row["mode"] == "strict":
            print(
                "错误: strict 批次的原生执行绑定不允许 retry;"
                " 请重新审批并提交新批次",
                file=sys.stderr,
            )
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
        try:
            _validate_gpu_requeue(
                conn, _load_cfg(), batch, batch_row["project"],
                [job for job in targets if job["status"] in ("blocked", "cancelled", "timed_out", "failed")],
            )
        except SchemaError as error:
            print(f"retry 拒绝: {error}", file=sys.stderr)
            return 1
        for job in targets:
            task_row = conn.execute(
                "SELECT spec FROM tasks"
                " WHERE batch_id=? AND id=? AND version=?",
                (job["batch_id"], job["task_id"], job["version"]),
            ).fetchone()
            if task_row is None:
                continue
            try:
                task_spec = json.loads(task_row["spec"])
            except (json.JSONDecodeError, TypeError):
                task_spec = None
            if isinstance(task_spec, dict) and any(
                key in task_spec for key in NATIVE_EXEC_ALL_INTERNAL_FIELDS
            ):
                print(
                    "错误: 含原生执行绑定的任务不允许 retry;"
                    " 请重新审批并提交新批次",
                    file=sys.stderr,
                )
                return 1
            if isinstance(task_spec, dict) and task_spec.get("recovery") is not None:
                print("错误: recovery 任务不能 retry 重放；使用 resubmit 创建新版本", file=sys.stderr)
                return 1
            if execution_state.get(conn, job["id"]) is not None:
                print(
                    "错误: execution attempt 已消费，不允许 retry 重放；"
                    "确认旧任务终态后使用 resubmit 创建新版本",
                    file=sys.stderr,
                )
                return 1
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
            "SELECT status, project, name, env, mode FROM batches WHERE id=?", (batch,)
        ).fetchone()
        if not batch_row:
            print(f"错误: 批次不存在: {ref}", file=sys.stderr)
            return 1
        if batch_row["mode"] == "strict":
            print(
                "错误: strict 批次的原生执行绑定不允许 resubmit;"
                " 请重新审批并提交新批次",
                file=sys.stderr,
            )
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
        try:
            _validate_gpu_requeue(conn, cfg, batch, project, jobs)
        except SchemaError as error:
            print(f"resubmit 拒绝: {error}", file=sys.stderr)
            return 1
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
            if any(key in spec for key in NATIVE_EXEC_ALL_INTERNAL_FIELDS):
                print(
                    "错误: 含原生执行绑定的任务不允许 resubmit;"
                    " 请重新审批并提交新批次",
                    file=sys.stderr,
                )
                return 1
            if "execution" in spec:
                try:
                    revalidate_binding(spec, cfg, project, json.loads(batch_row["env"] or "{}"))
                except ExecutionPolicyError as error:
                    print(f"resubmit 拒绝: {error}", file=sys.stderr)
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
            **_native_exec_fingerprint_kwargs(spec),
        )
        try:
            recovery.verify_binding(spec, fingerprint)
        except recovery.RecoveryError as error:
            print(f"resubmit 拒绝: {error}", file=sys.stderr)
            return 1
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
        try:
            validate_project_gpu_access(_load_cfg(), project, [item["spec"] for item in prepared_specs])
        except SchemaError as error:
            print(f"resubmit 拒绝: {error}", file=sys.stderr)
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
        from . import task_dependencies
        try:
            for prepared in prepared_specs:
                old = conn.execute("SELECT * FROM jobs WHERE batch_id=? AND task_id=? AND version=?",
                                   (batch, prepared["task_id"], prepared["old_version"])).fetchone()
                new = state.get_job(conn, f"{batch}-{prepared['task_id']}-v{prepared['new_version']}")
                task_dependencies.inherit(conn, old, new)
            task_dependencies.validate_cycles(conn, [f"{batch}-{p['task_id']}-v{p['new_version']}" for p in prepared_specs])
        except (ValueError, TypeError, RecursionError) as error:
            raise state.StateError(f"任务依赖继承失败: {error}") from error
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
                "SELECT status, mode FROM batches WHERE id=?",
                (b,),
            ).fetchone()
            if not batch_row:
                print(f"错误: 批次不存在: {args.batch}", file=sys.stderr)
                return 1
            if batch_row["mode"] == "strict":
                print(
                    "错误: strict native 批次是一次性执行，不能 clean 或重新排队",
                    file=sys.stderr,
                )
                return 1
            legacy_specs = conn.execute(
                "SELECT t.spec FROM tasks t WHERE t.batch_id=?", (b,)
            ).fetchall()
            for row in legacy_specs:
                spec = json.loads(row["spec"] or "{}")
                if isinstance(spec, dict) and any(key in spec for key in NATIVE_EXEC_ALL_INTERNAL_FIELDS):
                    print("错误: 历史 native 绑定不能 clean 或重新排队", file=sys.stderr)
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
                "SELECT t.id, t.version, t.spec, j.id AS job_id FROM jobs j"
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
                if execution_state.get(conn, tr["job_id"]) is not None:
                    print("错误: 已消费 execution attempt 的任务不能 clean 后重排", file=sys.stderr)
                    return 1
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
                if spec.get("recovery") is not None:
                    print("错误: clean 不能将 recovery skip 版本重新排队；使用 resubmit", file=sys.stderr)
                    return 1
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

    冷键 (node/state_dir/user/schema_version/gpus/native_exec_profiles 及其引用的 project root)
    变更直接拒绝 ——
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
    cold = [k for k in ("node", "state_dir", "user", "schema_version", "native_exec_profiles", "execution_backends")
            if old.get(k) != new_cfg.get(k)]
    try:
        og, ng = parse_gpus(old), parse_gpus(new_cfg)
    except ConfigError as exc:
        print(f"错误: 新配置校验失败 (未写入): {exc}", file=sys.stderr)
        return 1
    if (og[0], og[1]) != (ng[0], ng[1]):
        cold.append("gpus(卡集或容量覆盖)")
    try:
        native_roots_changed = (
            native_exec_project_roots(old)
            != native_exec_project_roots(new_cfg)
        )
    except NativeExecProfileError as error:
        print(f"错误: 新配置校验失败 (未写入): {error}", file=sys.stderr)
        return 1
    if native_roots_changed:
        cold.append("native_exec_project_roots")
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
        from .config import validate_gpu_affinity_pool
        validate_gpu_affinity_pool(new_cfg)
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
    冷键变更 (node/state_dir/user/gpus/native_exec_profiles 及其引用的 project root) daemon
    会拒绝并提示重启.
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
    print(
        "   注意: node/state_dir/user/gpus 卡集、native_exec_profiles "
        "及其引用的 project root 为冷键, 变更需重启 daemon"
    )
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
    print(f"  status: {j['status']}  rc: {j['rc'] if j['rc'] is not None else '-'}  failure: {j['failure'] or '-'}")
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
        if getattr(args, "include_lease", False) and (args.action != "status" or not getattr(args, "json", False)):
            raise ValueError("--include-lease 仅用于 daemon status --json")
        if getattr(args, "json", False) and args.action not in ("status", "check"):
            raise ValueError("--json 仅用于 daemon status/check")
        if getattr(args, "stop_when_idle", False) and args.action != "drain":
            raise ValueError("--stop-when-idle 仅用于 daemon drain")
        if args.action != "foreground" and (getattr(args, "supervise", False) or getattr(args, "restart_delay_sec", None) is not None or getattr(args, "max_restarts", None) is not None):
            raise ValueError("supervisor options require daemon foreground")
        if args.action == "foreground":
            from .supervisor import foreground
            return foreground(fake=getattr(args, "fake", False), supervise=getattr(args, "supervise", False), restart_delay_sec=3 if args.restart_delay_sec is None else args.restart_delay_sec, max_restarts=0 if args.max_restarts is None else args.max_restarts)
        if args.action in ("drain", "resume"):
            from . import resources as admission
            if args.action == "drain":
                admission.set_drain(stop=getattr(args, "stop_when_idle", False))
                print("已暂停新派发；运行中任务继续，pending 保留。" +
                      ("排空后 daemon 自动退出。" if getattr(args, "stop_when_idle", False) else "用 daemon resume 恢复。"))
            else:
                admission.resume()
                print("已解除排空；运行中的 daemon 将恢复派发。未运行时请 daemon start。")
            return 0
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
            if getattr(args, "json", False):
                result = {"schema_version": 1, **daemon.health_snapshot()}
                if getattr(args, "include_lease", False):
                    result["lease"] = _daemon_lease_snapshot(current=True, limit=1)
                print(json.dumps(result, ensure_ascii=False))
            else:
                print(daemon.status_str())
            return 0
        issues = daemon.check(fake=getattr(args, "fake", False))
        failures = sum(issue["level"] == "fail" for issue in issues)
        if getattr(args, "json", False):
            import socket
            print(json.dumps({"schema_version": 1, "query": "daemon_check", "sched_version": __version__,
                "node": state.hostname(), "query_host": socket.gethostname(), "observed_at": time.time(),
                "fake": bool(getattr(args, "fake", False)), "passed": failures == 0,
                "summary": {level: sum(issue["level"] == level for issue in issues) for level in ("ok", "warn", "fail")},
                "checks": issues}, ensure_ascii=False, indent=2))
            return 1 if failures else 0
        for issue in issues:
            mark = {"ok": "✅", "warn": "⚠️", "fail": "❌"}[issue["level"]]
            print(f"  {mark} {issue['item']}: {issue['detail']}")
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
    if not projects and not getattr(args, "json", False):
        print("未配置任何项目")
        return 0
    rows = []
    with state.connect() as conn:
        for name, pcfg in projects.items():
            quota = int(pcfg.get("gpu_quota", 0) or 0)
            enabled = project_gpu_enabled(cfg, name)
            prio = pcfg.get("priority", 0)
            aff = pcfg.get("gpu_affinity", [])
            root = pcfg.get("root", "")
            # B12-b/c: colocate 三态与项目级打包上限可视化
            col = pcfg.get("colocate")
            used = conn.execute(
                "SELECT COUNT(*) FROM jobs"
                " WHERE status='running' AND gpu IS NOT NULL AND project=?",
                (name,),
            ).fetchone()[0]
            rows.append({
                "name": name, "gpu_enabled": enabled, "gpu_quota": quota,
                "gpu_access": "disabled" if not enabled else "limited" if quota else "unlimited",
                "gpu_used": used, "priority": prio, "colocate": col,
                "max_jobs": pcfg.get("max_jobs"), "gpu_affinity": aff, "root": root,
            })
    if getattr(args, "json", False):
        print(json.dumps({"schema_version": 1, "projects": rows}, ensure_ascii=False, indent=2))
        return 0
    print(f"{'项目':<16} {'GPU访问':<7} {'GPU配额':<7} {'优先级':<6} {'colocate':<9} "
          f"{'单卡上限':<8} {'亲和卡':<12} {'已用/配额':<10} 根目录")
    for row in rows:
        access = {"disabled": "禁用", "unlimited": "无限制", "limited": "限额"}[row["gpu_access"]]
        quota = row["gpu_quota"]
        quota_str = f"{row['gpu_used']}/{quota}" if quota else f"{row['gpu_used']}/∞"
        col = row["colocate"]
        col_s = "跟随全局" if col is None else "on" if col else "off"
        mjs = str(row["max_jobs"]) if row["max_jobs"] else "-"
        print(f"{row['name']:<16} {access:<7} {quota:<7} {row['priority']:<6} {col_s:<9} "
              f"{mjs:<8} {str(row['gpu_affinity']):<12} {quota_str:<10} {row['root']}")
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
    request_id: str | None = None,
) -> tuple[int, str, str, list[Any]]:
    captured_stdout = _BoundedTextCapture()
    captured_stderr = _BoundedTextCapture()
    callbacks: list[Any] = []
    code = 1
    from . import artifact_revalidation, task_dependencies, pending_cancel
    token = artifact_revalidation.request_id.set(request_id)
    dependency_token = task_dependencies.request_id.set(request_id)
    pending_token = pending_cancel.request_id.set(request_id)
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
    finally:
        artifact_revalidation.request_id.reset(token)
        task_dependencies.request_id.reset(dependency_token)
        pending_cancel.request_id.reset(pending_token)
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


class RequestValidationError(ValueError):
    def __init__(self, message: str, reason_code: str = "invalid_request", *, missing_fields=()):
        super().__init__(message)
        self.reason_code = reason_code
        self.missing_fields = list(missing_fields)


def _request_rejection(args, error: RequestValidationError) -> int:
    """Describe this invocation only; never negate an older RID's effect."""
    if getattr(args, "json", False):
        from .integration import CONTRACTS
        print(json.dumps({"schema_version": 1, "contract": CONTRACTS["request_validation"],
                          "request_id": getattr(args, "request_id", None), "valid": False, "code": 64,
                          "error": {"reason_code": error.reason_code, "stage": "pre_dispatch",
                                    "message": str(error), "missing_fields": error.missing_fields,
                                    "target_kind": getattr(args, "expect_kind", "none"),
                                    "dispatch_entered": False, "request_record_created_this_invocation": False,
                                    "effect_of_this_invocation": "none"}}, ensure_ascii=False))
    print(f"错误: {error}", file=sys.stderr)
    return 64


def _request_envelope(args: argparse.Namespace) -> tuple[str, list[str], dict]:
    """Shared pure validation. No state, files, RID reservation or mutation."""
    request_id = str(getattr(args, "request_id", ""))
    command = list(getattr(args, "command", []) or [])
    if command[:1] == ["--"]:
        command = command[1:]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", request_id):
        raise RequestValidationError("request_id 格式无效", "invalid_request_id")
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
        "batch-policy",
        "artifact-revalidate",
        "dependency-update",
        "cancel-pending",
    }
    if not command or command[0] not in allowed:
        raise RequestValidationError("request 只允许调度器 mutation 子命令", "unsupported_mutation")
    if command[0] == "daemon" and (
        len(command) < 2 or command[1] not in {"start", "stop", "drain", "resume"}
    ):
        raise RequestValidationError("request 只允许 daemon start/stop/drain/resume", "unsupported_mutation")
    if command[:2] == ["daemon", "drain"] and command[2:] not in ([], ["--stop-when-idle"]):
        raise RequestValidationError("request daemon drain 只接受 --stop-when-idle")
    if command[:2] == ["daemon", "resume"] and command[2:]:
        raise RequestValidationError("request daemon resume 不接受额外参数")
    if command[0] == "config" and (
        len(command) < 2 or command[1] != "set"
    ):
        raise RequestValidationError("request 只允许 config set", "unsupported_mutation")

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
        raise RequestValidationError(str(exc), "invalid_assignments") from exc

    if (
        isinstance(expect_revision, bool)
        or not isinstance(expect_revision, int)
        or expect_revision < 0
    ):
        raise RequestValidationError("mutation 必须提供非负 --expect-revision", "invalid_precondition",
                                     missing_fields=["expect_revision"] if expect_revision is None else [])

    target_kind = "none"
    target_id: str | None = None
    if command[0] in {"cancel", "retry", "resubmit", "artifact-revalidate", "dependency-update"}:
        if len(command) < 2:
            raise RequestValidationError("mutation 缺少目标", "missing_target")
        target_id = command[1]
        target_kind = "task" if ":" in target_id else "batch"
    elif command[0] in {"batch-policy", "cancel-pending"}:
        if len(command) < 2:
            raise RequestValidationError("batch-policy 缺少目标", "missing_target")
        target_kind, target_id = "batch", command[1]
    elif command[0].startswith("gpu-"):
        if len(command) < 2:
            raise RequestValidationError("GPU mutation 缺少目标", "missing_target")
        target_id = command[1]
        target_kind = "gpu"

    if target_kind != expect_kind or (
        target_kind != "none" and target_id != expect_id
    ):
        raise RequestValidationError("mutation precondition 与命令目标不匹配", "target_mismatch",
                                     missing_fields=["expect_id"] if target_kind != "none" and not expect_id else [])
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
            raise RequestValidationError("无目标 mutation 只接受 --expect-revision 0", "invalid_precondition")
    elif (
        not isinstance(expect_id, str)
        or not expect_id
        or not isinstance(expect_status, str)
        or not expect_status
    ):
        raise RequestValidationError("mutation precondition 字段不完整", "missing_precondition",
                                     missing_fields=[name for name, value in
                                                     (("expect_id", expect_id), ("expect_status", expect_status)) if not value])
    if expect_kind == "task" and (
        not isinstance(expect_version, int)
        or isinstance(expect_version, bool)
        or expect_version < 1
        or not isinstance(expect_id, str)
        or expect_id.count(":") != 1
    ):
        raise RequestValidationError("task mutation precondition 无效", "invalid_precondition",
                                     missing_fields=["expect_version"] if expect_version is None else [])
    if expect_kind != "task" and expect_version is not None:
        raise RequestValidationError("非 task mutation 不接受 version precondition", "invalid_precondition")
    if expect_kind == "gpu" and (
        not isinstance(expect_id, str)
        or not expect_id.isdigit()
        or expect_assignments is None
    ):
        raise RequestValidationError("GPU mutation precondition 无效", "invalid_precondition",
                                     missing_fields=["expect_assignments_json"] if expect_assignments is None else [])
    if expect_kind != "gpu" and (
        expect_quarantined is not None or expect_assignments is not None
    ):
        raise RequestValidationError("非 GPU mutation 不接受 GPU precondition", "invalid_precondition")
    if expect_quarantined is not None and (
        not isinstance(expect_quarantined, int)
        or isinstance(expect_quarantined, bool)
        or expect_quarantined not in {0, 1}
    ):
        raise RequestValidationError("GPU quarantined precondition 无效", "invalid_precondition")

    expectation = {
        "kind": expect_kind,
        "id": expect_id,
        "status": expect_status,
        "version": expect_version,
        "quarantined": expect_quarantined,
        "revision": expect_revision,
        "assignments": expect_assignments,
    }
    for key in ("instance", "project"):
        value = getattr(args, "expect_" + key, None)
        if value is not None:
            if not isinstance(value, str) or not value or value.startswith("-") or any(c.isspace() for c in value):
                raise RequestValidationError("invalid identity/project precondition", "invalid_precondition")
            if key == "project" and expect_kind not in {"batch", "task"}:
                raise RequestValidationError("project precondition requires a batch or task", "invalid_precondition")
            expectation[key] = value
    # Parse the exact nested command, but never invoke its handler. The same
    # parser executes mutations later; preflight does not promise CAS success.
    with contextlib.redirect_stderr(io.StringIO()):
        try:
            parsed = _build_parser().parse_args(command)
        except SystemExit as exc:
            raise RequestValidationError("mutation 命令参数无效", "invalid_command") from exc
    if command[0] == "batch-policy" and parsed.failure_policy is None:
        raise RequestValidationError("request batch-policy 需要 --failure-policy", "unsupported_mutation")
    if command[0] == "artifact-revalidate" and (target_kind != "task" or "instance" not in expectation
            or not re.fullmatch("[0-9a-f]{32}", expectation.get("instance", ""))
            or not re.fullmatch("[0-9a-f]{64}", parsed.validation_id)
            or not 0 <= parsed.system_retries <= 2 or (parsed.reopen and not parsed.settle)):
        raise RequestValidationError("artifact-revalidate 需要精确 task/instance、有效原记录 ID 与复验参数", "invalid_precondition")
    if command[0] == "dependency-update":
        if target_kind != "task" or not re.fullmatch("[0-9a-f]{32}", expectation.get("instance", "")):
            raise RequestValidationError("dependency-update 需要完整 task/instance CAS", "invalid_precondition")
        try:
            _dependency_selectors(parsed.dependencies_json)
        except (ValueError, RecursionError) as error:
            raise RequestValidationError(str(error), "invalid_command") from error
    if command[0] == "cancel-pending":
        if target_kind != "batch" or not re.fullmatch("[0-9a-f]{32}", expectation.get("instance", "")):
            raise RequestValidationError("cancel-pending 需要完整 batch/instance CAS", "invalid_precondition")
        try:
            from .pending_cancel import normalize
            normalize(parsed.tasks_json, bindings=True)
        except (ValueError, RecursionError) as error:
            raise RequestValidationError(str(error), "invalid_command") from error
    return request_id, command, expectation


def cmd_request_validate(args) -> int:
    try:
        request_id, command, expectation = _request_envelope(args)
    except RequestValidationError as error:
        return _request_rejection(args, error)
    from .integration import CONTRACTS, canonical
    import hashlib
    output = {"schema_version": 1, "contract": CONTRACTS["request_validation"],
              "request_id": request_id, "valid": True, "code": 0,
              "binding_sha256": hashlib.sha256(canonical({"command": command, "expect": expectation}).encode()).hexdigest(),
              "target_kind": expectation["kind"], "state_checked": False,
              "dispatch_entered": False, "request_record_created_this_invocation": False,
              "effect_of_this_invocation": "none"}
    print(json.dumps(output, ensure_ascii=False) if args.json else "请求格式有效；未检查当前态，未预占 RID")
    return 0


def cmd_request(args: argparse.Namespace) -> int:
    """Execute a mutation once with revision-bound durable replay."""
    try:
        request_id, command, expectation = _request_envelope(args)
    except RequestValidationError as error:
        return _request_rejection(args, error)
    expect_kind, expect_id = expectation["kind"], expectation["id"]
    expect_status, expect_version = expectation["status"], expectation["version"]
    expect_revision, expect_quarantined = expectation["revision"], expectation["quarantined"]
    expect_assignments = expectation["assignments"]
    target_kind, target_id = expect_kind, expect_id
    argv_json = json.dumps(
        {"command": command, "expect": expectation},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )

    def existing_result(conn: sqlite3.Connection):
        if conn.execute("SELECT 1 FROM submission_requests WHERE request_id=?", (request_id,)).fetchone():
            return {"argv": "different submission", "status": "done", "code": 64}
        from .integration import load_ticket
        if load_ticket(request_id) is not None:
            return {"argv": "different submission", "status": "done", "code": 64}
        return conn.execute(
            "SELECT argv, status, code, stdout, stderr, output_compacted, result_json"
            " FROM operation_requests WHERE request_id=?",
            (request_id,),
        ).fetchone()

    def emit(code, result_json, *, replayed=False, dispatch_entered=False):
        from .integration import CONTRACTS
        print(json.dumps({"schema_version": 1, "contract": CONTRACTS["request_result"],
                          "request_id": request_id, "phase": "done", "code": code,
                          "replayed": replayed, "result": json.loads(result_json) if result_json else None,
                          "dispatch_entered": dispatch_entered,
                          "request_record_created_this_invocation": not replayed,
                          "effect_of_this_invocation": "none" if replayed or not dispatch_entered else "see_receipt"}, ensure_ascii=False))

    def conflict_result(conn, conflict):
        from .integration import canonical, mutation_result
        result = json.loads(mutation_result(conn, command, 65, target_kind, target_id))
        result["error"] = {"reason_code": "precondition_conflict", "stage": "precondition",
                           "message": conflict["message"], "conflict_reason": conflict["reason_code"],
                           "actual": conflict["actual"], "target_kind": target_kind,
                           "expected": expectation, "dispatch_entered": False,
                           "request_record_created_this_invocation": True,
                           "effect_of_this_invocation": "none"}
        return canonical(result)

    def replay(existing) -> int | None:
        if existing is None:
            return None
        if existing["argv"] != argv_json:
            return _request_rejection(args, RequestValidationError(
                "request_id 已绑定到不同 mutation", "request_binding_mismatch"))
        if existing["status"] != "done":
            print(
                "错误: prior mutation outcome unknown; refusing replay",
                file=sys.stderr,
            )
            if getattr(args, "json", False):
                from .integration import CONTRACTS
                print(json.dumps({"schema_version": 1, "contract": CONTRACTS["request_result"],
                                  "request_id": request_id, "phase": "unknown", "code": 75,
                                  "reason_code": "prior_outcome_unknown", "result": None,
                                  "dispatch_entered": False, "request_record_created_this_invocation": False,
                                  "effect_of_this_invocation": "none"}))
            return 75
        if getattr(args, "json", False):
            emit(int(existing["code"]), existing["result_json"], replayed=True)
        else:
            sys.stdout.write(existing["stdout"] or "")
        sys.stderr.write(existing["stderr"] or "")
        return int(existing["code"])

    def precondition_conflict(conn: sqlite3.Connection) -> dict | None:
        def changed(reason, message, actual=None):
            return {"reason_code": reason, "message": message, "actual": actual}

        from .integration import instance_id
        if "instance" in expectation and instance_id(conn) != expectation["instance"]:
            return changed("instance_changed", "scheduler instance changed", {"instance": instance_id(conn)})
        if "project" in expectation:
            batch = expect_id.split(":", 1)[0] if expect_kind == "task" else expect_id
            owner = conn.execute("SELECT project FROM batches WHERE id=?", (batch,)).fetchone()
            if owner is None or owner[0] != expectation["project"]:
                return changed("project_changed", "scheduler project changed", {"project": owner[0] if owner else None})
        if expect_kind == "none":
            return None
        if expect_kind == "batch":
            row = conn.execute(
                "SELECT status, revision FROM batches WHERE id=?",
                (expect_id,),
            ).fetchone()
            if row is None:
                return changed("target_absent", "batch absent")
            if row["status"] != expect_status:
                return changed("status_changed", (
                    f"batch status changed: expected {expect_status},"
                    f" found {row['status']}"
                ), dict(row))
            if row["revision"] != expect_revision:
                return changed("revision_changed", (
                    f"batch revision changed: expected {expect_revision},"
                    f" found {row['revision']}"
                ), dict(row))
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
                return changed("target_absent", "task absent")
            actual_status = {
                "waiting_quota": "pending",
                "waiting_dep": "pending",
            }.get(row["status"], row["status"])
            if actual_status != expect_status or row["version"] != expect_version:
                return changed("task_changed", (
                    f"task changed: expected {expect_status} v{expect_version},"
                    f" found {actual_status} v{row['version']}"
                ), {**dict(row), "status": actual_status})
            if row["revision"] != expect_revision:
                return changed("revision_changed", (
                    f"batch revision changed: expected {expect_revision},"
                    f" found {row['revision']}"
                ), {**dict(row), "status": actual_status})
            return None
        row = conn.execute(
            "SELECT status, quarantined, revision FROM gpus WHERE idx=?",
            (int(expect_id),),
        ).fetchone()
        if row is None:
            return changed("target_absent", "GPU absent")
        if row["status"] != expect_status:
            return changed("status_changed", (
                f"GPU status changed: expected {expect_status},"
                f" found {row['status']}"
            ), dict(row))
        if (
            expect_quarantined is not None
            and row["quarantined"] != expect_quarantined
        ):
            return changed("quarantine_changed", (
                "GPU quarantine changed:"
                f" expected {expect_quarantined}, found {row['quarantined']}"
            ), dict(row))
        if row["revision"] != expect_revision:
            return changed("revision_changed", (
                f"GPU revision changed: expected {expect_revision},"
                f" found {row['revision']}"
            ), dict(row))
        assignments = [
            {"job_id": item["job_id"], "vram_gib": item["vram_gib"]}
            for item in conn.execute(
                "SELECT job_id, vram_gib FROM gpu_jobs"
                " WHERE gpu_id=? ORDER BY job_id",
                (int(expect_id),),
            ).fetchall()
        ]
        if assignments != expect_assignments:
            return changed("assignments_changed", (
                "GPU assignments changed:"
                f" expected {expect_assignments}, found {assignments}"
            ), {**dict(row), "assignments": assignments})
        return None

    unbound = command[0] in {"daemon", "config"}
    if unbound:
        with state.submission_lock(), state.connect() as conn:
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
                stderr = f"错误: mutation precondition failed: {conflict['message']}\n"
                result_json = conflict_result(conn, conflict)
                conn.execute(
                    "UPDATE operation_requests SET status='done', code=65,"
                    " stdout='', stderr=?, finished_at=?, result_json=? WHERE request_id=?",
                    (stderr, state.now(), result_json, request_id),
                )
                if getattr(args, "json", False):
                    emit(65, result_json)
                sys.stderr.write(stderr)
                return 65

        code, stdout, stderr, _callbacks = _run_captured_mutation(command)
        with state.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            from .integration import mutation_result
            result_json = mutation_result(conn, command, code, target_kind, target_id)
            conn.execute(
                "UPDATE operation_requests"
                " SET status='done', code=?, stdout=?, stderr=?, finished_at=?, result_json=?"
                " WHERE request_id=? AND status='started'",
                (code, stdout, stderr, state.now(), result_json, request_id),
            )
        if getattr(args, "json", False):
            emit(code, result_json, dispatch_entered=True)
        else:
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
            stderr = f"错误: mutation precondition failed: {conflict['message']}\n"
        else:
            conn.execute("SAVEPOINT request_mutation")
            code, stdout, stderr, deferred = _run_captured_mutation(command, conn, request_id=request_id)
            if code:
                conn.execute("ROLLBACK TO request_mutation")
                deferred.clear()
            conn.execute("RELEASE request_mutation")
        from .integration import mutation_result
        result_json = (conflict_result(conn, conflict) if conflict is not None else
                       mutation_result(conn, command, code, target_kind, target_id, request_id=request_id))
        conn.execute(
            "UPDATE operation_requests"
            " SET status='done', code=?, stdout=?, stderr=?, finished_at=?, result_json=?"
            " WHERE request_id=? AND status='started'",
            (code, stdout, stderr, state.now(), result_json, request_id),
        )

    while deferred:
        effect = deferred.pop(0)
        try:
            effect()
        except Exception as exc:
            print(f"警告: mutation 已提交，但提交后副作用失败: {exc}", file=sys.stderr)
    if getattr(args, "json", False):
        emit(code, result_json, dispatch_entered=conflict is None)
    else:
        sys.stdout.write(stdout)
    sys.stderr.write(stderr)
    return code


def cmd_integration_query(args) -> int:
    from . import integration
    state.set_read_only(True)
    try:
        if args._subcommand == "identity":
            output = integration.identity()
        else:
            wait_sec = getattr(args, "wait_sec", 0)
            if not math.isfinite(wait_sec) or not 0 <= wait_sec <= 60:
                raise ValueError("--wait-sec 必须是 0..60 的有限秒数")
            expected = getattr(args, "expect_instance", None)
            if expected is not None and not re.fullmatch(r"[0-9a-f]{32}", expected):
                raise ValueError("--expect-instance 必须是 32 位小写十六进制实例 ID")
            deadline = time.monotonic() + wait_sec
            previous: dict[str, dict] = {}
            observed_instance = None
            while True:
                many = args._subcommand == "request-status-many"
                output = (integration.request_status_many(args.request_ids) if many
                          else integration.request_status(args.request_id))
                current_instance = output["instance_id"]
                if expected is not None and current_instance != expected:
                    raise ValueError("scheduler instance unavailable or changed")
                if observed_instance is not None and current_instance != observed_instance:
                    raise ValueError("scheduler instance changed during wait")
                observed_instance = current_instance
                rows = output["requests"] if many else [output]
                for row in rows:
                    prior = previous.get(row["request_id"])
                    if prior and prior["found"] and not row["found"]:
                        observed_at = row["observed_at"]
                        row.update(prior)
                        row.update(observation_incomplete=True, query_observed_at=observed_at,
                                   reason_code="prior_receipt_evidence_retained")
                    if prior and prior["found"] and row["found"] and prior["binding_sha256"] != row["binding_sha256"]:
                        raise ValueError("request binding changed during wait")
                    previous[row["request_id"]] = dict(row)
                settled = all(row["phase"] == "done" and not row.get("observation_incomplete", False) for row in rows)
                remaining = deadline - time.monotonic()
                output["wait_timed_out"] = bool(wait_sec and not settled and remaining <= 0)
                if settled or remaining <= 0:
                    break
                # Each query closes its snapshot before sleeping. No writer,
                # open transaction, submit, or RID change occurs in this loop.
                time.sleep(min(0.5, remaining))
        print(json.dumps(output, ensure_ascii=False))
        return 0
    except (state.StateError, ValueError, OSError) as error:
        if args._subcommand != "identity":
            print(json.dumps({"schema_version": 1, "query": "request_status_error",
                              "reason_code": "invalid_query" if isinstance(error, ValueError) else "query_unavailable",
                              "code": 1, "observation_complete": False}))
        print(f"错误: {error}", file=sys.stderr)
        return 1
    finally:
        state.set_read_only(False)


def cmd_version(args) -> int:
    """Report installed code compatibility without consulting configured state."""
    from .integration import CONTRACTS
    if args.json:
        print(json.dumps({
            "schema_version": 1,
            "query": "version",
            "contracts": CONTRACTS,
            "sched_version": __version__,
            "database_schema": {
                "write": state.DB_SCHEMA_VERSION,
                "read_min": 1,
                "read_max": state.DB_SCHEMA_VERSION,
            },
        }))
    else:
        print(f"sched {__version__}")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="sched", description=f"sched v{__version__} 统一任务调度框架"
    )
    ap.add_argument("--version", action="version", version=f"sched {__version__}")
    # Keep the parser's routing key separate from subcommand payload fields.
    # `run` intentionally exposes a positional `cmd` remainder; reusing that
    # name for the selected subcommand replaces "run" with a list and breaks
    # every set-membership check below before cmd_run can execute.
    sub = ap.add_subparsers(dest="_subcommand")

    p = sub.add_parser("version", help="show installed version without reading state")
    p.add_argument("--json", action="store_true", help="structured version and schema compatibility")
    p.set_defaults(fn=cmd_version)

    for query in ("identity", "request-status", "request-status-many"):
        p = sub.add_parser(query, help="read-only integration identity or receipt")
        if query == "request-status":
            p.add_argument("request_id")
        if query == "request-status-many":
            p.add_argument("request_ids", nargs="+")
        if query != "identity":
            p.add_argument("--wait-sec", type=float, default=0, help="只读等待原回执，0..60 秒；不重投")
            p.add_argument("--expect-instance", help="验证查询实例，不匹配时拒绝继续等待")
        p.add_argument("--json", action="store_true")
        p.set_defaults(fn=cmd_integration_query)

    p = sub.add_parser("artifact-check", help="计算节点只读检查当前产物，不复验结算或重训")
    p.add_argument("task", help="<batch-id-or-name>:<task>")
    p.add_argument("--version", type=int)
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_artifact_check)

    p = sub.add_parser("batch-dependencies", help="只读查询显式 exact 绑定与动态名称依赖，不检查当前文件")
    p.add_argument("batch")
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--cursor", type=int, default=0, help="实时偏移量续页，不是完整当前态")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_batch_dependencies)

    p = sub.add_parser("task-dependencies", help="只读冻结任务 DAG 和已记录阻塞路径，不授予派发权")
    p.add_argument("task")
    p.add_argument("--version", type=int)
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--cursor", type=int, default=0)
    p.add_argument("--event-id", help="读取这个版本的一条不可变依赖事件；与 cursor 互斥")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_task_dependencies)

    p = sub.add_parser("dependency-update", help="经 task/instance CAS request 更新未启动版本的冻结依赖")
    p.add_argument("task")
    p.add_argument("--dependencies-json", required=True, help="内联 exact JSON，随 RID 冻结；不是文件路径")
    p.add_argument("--reopen", action="store_true")
    p.add_argument("--yes", action="store_true")
    p.set_defaults(fn=cmd_dependency_update)

    p = sub.add_parser("admission-explain", help="只读资源/装箱解释，复用派发判断，不探测网关或授予派发权")
    p.add_argument("task", help="<batch-id-or-name>:<task>")
    p.add_argument("--version", type=int)
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_admission_explain)

    p = sub.add_parser("storage-explain", help="只读计算节点已记录的磁盘/inode/用户 quota 准入，不探测网关")
    p.add_argument("task")
    p.add_argument("--version", type=int)
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_storage_explain)

    p = sub.add_parser("allocations", help="只读不可变启动/资源关联及分层观察，不探测进程或当前产物")
    p.add_argument("task", help="<batch-id-or-name>:<task>")
    p.add_argument("--version", type=int)
    p.add_argument("--allocation-id", help="读取一条完整分配和有界事件")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--cursor")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_allocations)

    p = sub.add_parser("task-facts", help="有界精确代际事实，只读私有快照，不授予取消权")
    p.add_argument("batch", help="完整 batch ID，不按名称解析")
    p.add_argument("--tasks-json", required=True, help="内联 task_id/version 数组，1..100 项")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_task_facts)

    p = sub.add_parser("cancel-pending", help="经一次 batch/instance CAS 原子取消明确且从未启动的任务组")
    p.add_argument("batch")
    p.add_argument("--tasks-json", required=True, help="task-facts 的精确 binding 数组，内联冻结")
    p.add_argument("--yes", action="store_true")
    p.set_defaults(fn=cmd_cancel_pending)

    p = sub.add_parser("artifact-validations", help="只读查询不可变首次产物验证记录，不检查当前文件")
    p.add_argument("task", help="<batch-id-or-name>:<task>")
    p.add_argument("--version", type=int)
    p.add_argument("--limit", type=int, default=20, help="摘要数量，1..100")
    p.add_argument("--cursor", help="按 validation_id 的实时续页游标，不是完整快照")
    p.add_argument("--validation-id", help="读取该任务的一条完整冻结证据，与 cursor 互斥")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_artifact_validations)

    p = sub.add_parser("artifact-revalidations", help="只读查询产物复验和结算事件")
    p.add_argument("task")
    p.add_argument("--version", type=int)
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--cursor")
    p.add_argument("--event-id", dest="validation_id")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_artifact_validations)

    p = sub.add_parser("artifact-revalidate", help="经 task CAS request 仅复验，显式 settle 才重新结算")
    p.add_argument("task")
    p.add_argument("--validation-id", required=True)
    p.add_argument("--system-retries", type=int, default=0, help="0..2 次系统错误退避；不重训")
    p.add_argument("--settle", action="store_true")
    p.add_argument("--reopen", action="store_true", help="结算后无其他失败时显式恢复 blocked 批次派发")
    p.add_argument("--yes", action="store_true")
    p.set_defaults(fn=cmd_artifact_revalidate)

    p = sub.add_parser("init", help="生成 config.json (M0)")
    p.add_argument("--config", help="config.json 路径 (默认 {STATE}/config.json)")
    p.set_defaults(fn=cmd_init)

    p = sub.add_parser("verify", help="确认批次已持久化 (提交凭证)")
    p.add_argument("batch", help="批次名或完整 id")
    p.set_defaults(fn=cmd_verify)

    p = sub.add_parser("submit", help="提交 batch.json 批次")
    p.add_argument("batch", help="batch.json 路径")
    p.add_argument("--request-id")
    p.add_argument("--expect-instance")
    p.add_argument("--expect-project")
    p.add_argument("--dry-run", action="store_true",
                   help="只预览不入队 (skip 预测 + 依赖就绪 + 展开命令, §G4)")
    p.add_argument("--json", action="store_true", help="提交结果或 dry-run 预览输出 JSON")
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
    p.add_argument("--host-mem-gib", type=float, default=None, help="主机内存预留 (GiB)")
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
    p.add_argument("--include-cpu-capacity", action="store_true", help="--json: 显式增加配置/已记录容量/来源/租约扩展")
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

    p = sub.add_parser("execution", help="通用执行尝试与原始退出事实")
    p.add_argument("task", help="<batch>:<task> 或 list")
    p.add_argument("--version", type=int)
    p.add_argument("--json", action="store_true")
    from .execution_queries import PHASES, OWNER_STATES
    p.add_argument("--project")
    p.add_argument("--batch")
    p.add_argument("--backend")
    p.add_argument("--phase", choices=PHASES)
    p.add_argument("--owner-status", choices=OWNER_STATES)
    p.add_argument("--limit", type=int)
    p.add_argument("--cursor")
    p.set_defaults(fn=cmd_execution)

    p = sub.add_parser("recovery", help="断点与精确 smoke 门禁的只读查询")
    p.add_argument("task", help="<batch-id-or-name>:<task-id>")
    p.add_argument("--json", action="store_true")
    p.add_argument("--version", type=int, help="指定历史版本；checkpoint 仍是 group 当前最新断点")
    p.set_defaults(fn=cmd_recovery)

    p = sub.add_parser("capabilities", help="本机 execution 能力检查，不读取配置或状态库")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_capabilities)

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

    p = sub.add_parser("batch-policy", help="只读查询批次失败策略；写操作经 request CAS")
    p.add_argument("batch")
    p.add_argument("--failure-policy", choices=["freeze", "continue_independent"])
    p.add_argument("--reopen", action="store_true", help="显式重开有未完成任务的 blocked 批次，不重试失败任务")
    p.add_argument("--yes", action="store_true")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_batch_policy)

    for request_command in ("request", "request-validate"):
        p = sub.add_parser(request_command, help="幂等 mutation" if request_command == "request" else "只校验完整请求格式，不读写 state")
        p.add_argument("request_id")
        p.add_argument("--expect-kind", choices=["none", "batch", "task", "gpu"], default="none")
        p.add_argument("--expect-id")
        p.add_argument("--expect-instance")
        p.add_argument("--expect-project")
        p.add_argument("--expect-status")
        p.add_argument("--expect-version", type=int)
        p.add_argument("--expect-quarantined", type=int, choices=[0, 1])
        p.add_argument("--expect-revision", type=int)
        p.add_argument("--expect-assignments-json", help='GPU 当前 assignments JSON，如 [{"job_id":"j","vram_gib":1.5}]')
        p.add_argument("--json", action="store_true", help="结构化结果；不改变请求绑定")
        p.add_argument("command", nargs="+")
        p.set_defaults(fn=cmd_request if request_command == "request" else cmd_request_validate)

    p = sub.add_parser("cpu-capacity", help="只读声明/已记录计算节点 CPU 容量；不探测网关、不授予执行权")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_cpu_capacity)

    p = sub.add_parser("cpu-isolation", help="只读已记录活跃 CPU claims；affinity 不等于 cgroup 硬隔离")
    p.add_argument("--json", action="store_true")
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--cursor")
    p.set_defaults(fn=cmd_cpu_isolation)

    p = sub.add_parser("cpu-scopes", help="只读原 CPU scope intent/inode/未决生命周期；不探测内核或授予执行权")
    p.add_argument("--scope-id")
    p.add_argument("--json", action="store_true")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--cursor")
    p.set_defaults(fn=cmd_cpu_scopes)

    p = sub.add_parser("daemon-lease", help="只读已记录 daemon 启动/租约事实，不探测 Slurm 或授予执行权")
    p.add_argument("--lease-id")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--cursor")
    p.add_argument("--after-seq", type=int, default=0)
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_daemon_lease)

    p = sub.add_parser("daemon", help="daemon 生命周期")
    p.add_argument("action", choices=["start", "stop", "status", "check", "drain", "resume", "foreground"])
    p.add_argument("--json", action="store_true", help="status/check: 结构化健康状态或前置检查")
    p.add_argument("--include-lease", action="store_true", help="status --json: 显式增加已记录租约扩展；默认契约不变")
    p.add_argument("--supervise", action="store_true", help="foreground: restart only after unexpected owned-child exit")
    p.add_argument("--restart-delay-sec", type=float, default=None)
    p.add_argument("--max-restarts", type=int, default=None, help="foreground: 0 means unlimited restarts")
    p.add_argument("--stop-when-idle", action="store_true", help="drain: running 清空后退出，保留 pending")
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
    p_list.add_argument("--json", action="store_true", help="结构化项目 GPU 访问策略与用量")
    p_list.set_defaults(fn=cmd_project_list)

    return ap


def main(argv: list[str] | None = None) -> int:
    state.set_read_only(False)
    state.set_query_only(False)
    ap = _build_parser()
    args = ap.parse_args(argv)
    if not getattr(args, "fn", None):
        ap.print_help()
        return 1

    command = getattr(args, "_subcommand", None)
    if command in {"capabilities", "version", "identity", "request-status", "request-status-many", "request-validate", "artifact-check", "artifact-validations", "artifact-revalidations", "artifact-revalidate", "batch-policy", "batch-dependencies", "task-dependencies", "dependency-update", "task-facts", "cancel-pending", "admission-explain", "allocations", "storage-explain", "daemon-lease", "cpu-capacity", "cpu-isolation", "cpu-scopes"}:
        return args.fn(args)
    if command == "request":
        try:
            _request_envelope(args)
        except RequestValidationError as error:
            return _request_rejection(args, error)
    daemon_action = getattr(args, "action", None) if command == "daemon" else None
    daemon_write_action = daemon_action
    if command == "request":
        wrapped = list(getattr(args, "command", []) or [])
        if wrapped[:1] == ["--"]:
            wrapped = wrapped[1:]
        if len(wrapped) >= 2 and wrapped[0] == "daemon":
            daemon_write_action = wrapped[1]
    daemon_requires_host = daemon_write_action in {
        "start", "stop", "check", "drain", "resume", "foreground",
    }
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
        daemon_requires_host
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
        daemon_requires_host
        and command == "daemon"
        and foreign
        and not allow_foreign_write
    ):
        print(
            f"错误: daemon {daemon_write_action} 必须在计算节点"
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
        "execution",
        "recovery",
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
    # the NFS-backed source through SQLite.  Complete private WAL schemas from
    # older builds remain queryable without migration; fresh or incomplete
    # state still follows the existing initialization path above.
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
