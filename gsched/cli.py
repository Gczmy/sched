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
from datetime import datetime
from typing import Any

from . import state, __version__
from .executor import PROGRESS_RE
from .config import (
    ConfigError,
    config_path,
    default_state_dir,
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
                    cfg["notify"]["command"] = {"cmd": cmd_path}
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

    def _expand_root(tok: str) -> str:
        """cmd 里的 {ROOT} 模板展开为项目根目录绝对路径."""
        if isinstance(tok, str) and "{ROOT}" in tok:
            return resolve_template(tok, cfg)
        return tok

    def _expand_cmd(cmd_list: list[str], stage_artifacts: dict[int, dict] | None = None,
                    cwd_abs: str | None = None) -> list[str]:
        out = []
        for tok in cmd_list:
            tok = _expand_venv(tok)
            tok = _expand_root(tok)
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
                " ORDER BY created_at DESC, rowid DESC LIMIT 1",
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
                "SELECT id FROM batches WHERE name=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
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

    # M13: 批次 id 到毫秒 (与 cmd_run 一致) —— 秒级精度下同秒重提/并发 submit
    # 撞主键抛裸 IntegrityError; 毫秒 + IntegrityError 兜底友好报错
    bid = f"{norm['name']}-{datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]}"

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

    def _expand_root(tok: str) -> str:
        """cmd 里的 {ROOT} 模板展开为项目根目录绝对路径."""
        if isinstance(tok, str) and "{ROOT}" in tok:
            return resolve_template(tok, cfg)
        return tok

    def _expand_cmd(cmd_list: list[str], stage_artifacts: dict[int, dict] | None = None,
                    cwd_abs: str | None = None) -> list[str]:
        """cmd 模板展开: {VENV:name} + {ROOT} + {stageN_<key>} (N7, 前序 stage 产物路径)."""
        out = []
        for tok in cmd_list:
            tok = _expand_venv(tok)
            tok = _expand_root(tok)
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
        # 决策 7A: dry-run 跳过该检查 —— 纯只读预览不产生副作用, 拦截反而
        # 挡住"现有批次终态后要提交什么"的预览场景; 预览中降级为提示
        existing = conn.execute(
            "SELECT status FROM batches WHERE name=?", (norm["name"],)
        ).fetchall()
        conflict = any(b["status"] not in ("done", "blocked") for b in existing)
        if conflict and not getattr(args, "dry_run", False):
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
            if conflict:
                print(f"  ⚠️ 同名批次已有未终态实例 — 实际提交会被定案 6 拒绝")
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
                flat = pt["cmd_flat"]
                shown = flat[:120] + ("..." if len(flat) > 120 else "")
                print(f"      cmd: {shown}")
            print("--- 汇总 ---")
            print(f"  将跑 {prev['n_run']} / 将 skip {prev['n_skip']} / 共 {len(norm['tasks'])} 任务")
            if prev["git_rev"]:
                print(f"  ⚠️ 预测基于当前 git rev {prev['git_rev'][:12]} (提交前若 pull 代码则预测作废, §G4)")
            if args.json:
                # 只输出 JSON (对齐 status --json 惯例, 供脚本直接解析)
                print(json.dumps(prev, ensure_ascii=False, indent=2))
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
                "max_parallel": t.get("max_parallel"),
            }
            state.insert_task(
                conn, bid, t["id"], 1, spec_json, i,
                norm.get("project"),
            )
            # Job 指纹 (A2): 指纹用展开后的 cmd (venv 路径入指纹)
            fp, stage_fps, rev = compute_fingerprint(
                cmd_e, stages_e, t["cwd_abs"], t["git"], cfg.get("venvs", {})
            )
            state.insert_job(
                conn, f"{bid}-{t['id']}-v1", bid, t["id"], 1,
                fp, stage_fps, norm.get("project"),
            )

    print(f"已入队: {bid} ({len(norm['tasks'])} 任务, mode={norm['mode']})")
    from . import daemon
    print(daemon.ensure_running())  # 定案 38: daemon 未运行自动拉起 (idle 退出后)
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

    with state.connect() as conn:
        state.insert_batch(
            conn, bid, batch_name, "mix", [], None, "{ROOT}", None,
            proj,
        )
        state.insert_task(conn, bid, "run", 1, task_spec, 0, proj)
        fp, stage_fps, rev = compute_fingerprint(
            task_spec["cmd"], None, cwd_abs, None, cfg.get("venvs", {})
        )
        state.insert_job(conn, f"{bid}-run-v1", bid, "run", 1, fp, stage_fps, proj)

    res_txt = "cpu-only" if args.cpu_only else "gpu=1"
    print(f"已入队: {bid} ({res_txt}, duration={args.duration}min)")
    print(f"  status/cancel 用批次名: {batch_name}")
    from . import daemon
    print(daemon.ensure_running())  # 定案 38: daemon 未运行自动拉起 (idle 退出后)
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
            p = _job_progress(j["batch"], j["task"], j["version"])
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
            print(f"      v{j['version']}  start={t0}  end={t1}  耗时={dur}")
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
    if not args.yes:
        print(f"确认取消 {ref}? 加 --yes 执行 (转发 daemon: 先写 kill_reason 再 killpg)")
        return 1
    with state.connect() as conn:
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
            b = _batch_id_from_name(ref)
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
        print("(daemon 将在下轮 tick 执行 kill, 可用 sched status 复查)")
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
    with state.connect() as conn:
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
        n = 0
        for j in targets:
            if j["status"] not in ("blocked", "cancelled", "timed_out", "failed"):
                continue
            w = _rev_diff_warn(conn, j)
            if w:
                print(w)
            state.update_job(
                conn, j["id"], status="pending", retries=0, kill_reason=None,
                pgid=None, gpu=None, rc=None, failure=None,
                started_at=None, finished_at=None,  # 清陈旧时间戳, pending 期间不显示旧耗时
            )
            print(f"已解锁重跑: {j['id']}")
            n += 1
        print(f"({n} 个任务)")
    from . import daemon
    print(daemon.ensure_running())  # 定案 38: retry 产生可派发工作, daemon 未运行自动拉起
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
        # B11c: project 继承自原批次行
        bproj = conn.execute(
            "SELECT project FROM batches WHERE id=?", (batch,)
        ).fetchone()
        proj = bproj["project"] if bproj else None
        # 新版本任务记录 (同 spec) + 新 Job
        state.insert_task(conn, batch, task, new_v, spec, 0, proj)

        from .fingerprint import compute_fingerprint

        fp, stage_fps, rev = compute_fingerprint(
            spec.get("cmd"), spec.get("stages"), spec.get("cwd_abs", "."),
            spec.get("git"), {},
        )
        state.insert_job(
            conn, f"{batch}-{task}-v{new_v}", batch, task, new_v,
            fp, stage_fps, proj,
        )
        # Q4: 检测下游依赖告警
        # C5 修复: name 从 DB 查 (与 cmd_cancel 同法) —— batch id 形如
        # {name}-{时间戳} 且 name 可含 '-', split("-")[0] 会截错导致漏报
        brow = state.get_batch(conn, batch)
        bname = brow["name"] if brow else batch
        deps = conn.execute(
            "SELECT name FROM batches WHERE depends_on LIKE ?", (f'%"{bname}"%',)
        ).fetchall()
        for d in deps:
            print(f"⚠️ 提示: 批次 '{d['name']}' depends_on 本批次, 上游已更新, 请重提下游 (Q4)")
        print(f"已 resubmit: {batch}:{task} -> v{new_v} (排队尾)")
    from . import daemon
    print(daemon.ensure_running())  # 定案 38: resubmit 产生可派发工作, daemon 未运行自动拉起
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


def cmd_incidents(args: argparse.Namespace) -> int:
    """sched incidents [id] [--limit N] [--job ID] [--gpu N]: 事故快照查询 (F2)."""
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
    """GPU 状态视图 (2026-08-17 缺口 3: 加显存列 mem_total_gib)."""
    with state.connect() as conn:
        rows = conn.execute("SELECT * FROM gpus ORDER BY idx").fetchall()
        for g in rows:
            q = " (QUARANTINED)" if g["quarantined"] else ""
            mem = g["mem_total_gib"]
            mem_s = f"{float(mem):.1f}GiB" if mem else "mem=?"
            print(
                f"GPU{g['idx']} [{g['status']:<10}] {mem_s:>8} job={g['job_id'] or '-'}{q}"
            )
    return 0


def cmd_gpu_set_mem(args: argparse.Namespace) -> int:
    """sched gpu-set-mem <idx> <gib>: 运行时覆盖该卡容量 (GiB).

    config.gpus[{idx,mem_gib}] 是启动时覆盖 (daemon 重启后探测覆盖); 本命令
    立即生效, 适合临时改容量 (如共享装箱余量调整) 不重启 daemon.
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
        print(f"GPU{args.idx} 容量已设为 {float(args.gib):.1f} GiB (下次 daemon 重启探测会覆盖, 如需持久化改 config.gpus)")
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
        print(f"{'项目':<20} {'GPU配额':<8} {'优先级':<6} {'亲和卡':<12} {'已用/配额':<12} {'根目录'}")
        for name, pcfg in projects.items():
            quota = pcfg.get("gpu_quota", 0)
            prio = pcfg.get("priority", 0)
            aff = pcfg.get("gpu_affinity", [])
            root = pcfg.get("root", "")
            used = conn.execute(
                "SELECT COUNT(*) FROM jobs WHERE status='running' AND project=?", (name,)
            ).fetchone()[0]
            quota_str = f"{used}/{quota}" if quota > 0 else f"{used}/∞"
            print(f"{name:<20} {quota:<8} {prio:<6} {str(aff):<12} {quota_str:<12} {root}")
    return 0


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
    p.add_argument("--project", required=True,
                   help="项目名 (B11c 隔离, 必填)")
    p.add_argument("--gpus", type=int, default=1, help="申请 GPU 数量 (R5)")
    p.add_argument("--cpus", type=int, default=None, help="CPU 配额 (记录+status 显示, B4)")
    p.add_argument("--cpu-only", action="store_true",
                   help="CPU-only 任务 (resources.gpu=0, 不占 GPU 槽位)")
    p.add_argument("--duration", type=int, default=None, help="预计时长(分钟), 超时=2x")
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
    p.add_argument("batch", help="<batch> 或 <batch>:<task>")
    p.add_argument("--yes", action="store_true")
    p.set_defaults(fn=cmd_cancel)

    p = sub.add_parser("retry", help="解锁 blocked 重跑 (批次级或单任务)")
    p.add_argument("task", help="<batch> 或 <batch>:<task> (批次级=全部失败终态)")
    p.set_defaults(fn=cmd_retry)

    p = sub.add_parser("diag", help="一站式失败诊断 (状态+命令+git+日志)")
    p.add_argument("task", help="<batch> 或 <batch>:<task> (批次级=全部非 done/skip)")
    p.set_defaults(fn=cmd_diag)

    p = sub.add_parser("config", help="配置管理 (B12-a 热更新)")
    sub_cfg = p.add_subparsers(dest="config_cmd", required=True)
    p_reload = sub_cfg.add_parser("reload", help="请求 daemon 热更新 config.json")
    p_reload.set_defaults(fn=cmd_config_reload)

    p = sub.add_parser("incidents", help="事故快照查询 (OOM/gpu_fault 现场)")
    p.add_argument("incident_id", nargs="?", type=int,
                   help="指定快照 id 查看详情 (含完整 payload)")
    p.add_argument("--limit", type=int, default=20, help="列表条数 (默认 20)")
    p.add_argument("--job", help="按 job id 过滤")
    p.add_argument("--gpu", type=int, default=None, help="按 GPU 过滤")
    p.set_defaults(fn=cmd_incidents)

    p = sub.add_parser("resubmit", help="重新提交 (新版本排队尾)")
    p.add_argument("task", help="<batch>:<task>")
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
    # 所有命令先确保建表 (幂等; daemon 侧也建, 双保险)
    # M18: init 失败 (state 目录不可写/磁盘满/DB 损坏) 不再静默吞噬 ——
    # 打 warning 继续 (只读命令可能仍可用), 失败会在第一次 SQL 处显式报错
    try:
        state.init_db()
    except Exception as e:
        print(f"警告: state DB 初始化失败 ({e}), 后续命令可能报错", file=sys.stderr)
    try:
        return args.fn(args)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
