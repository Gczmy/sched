"""批次终态通知 (设计: docs/sched_notify_design.md).

调度器保持"无意识": 批次进终态 (done/blocked) 时由 dispatcher 一次性触发,
本模块负责构造统一事件 JSON 并投递到已配置渠道。渠道注册表结构 ——
新增渠道 = 加一个函数 + CHANNELS 注册一行 (webhook 预留占位)。

- email: smtplib, 协议按 port 自动选 (465 SSL / 587 STARTTLS / 其他明文),
  password_env 可选 (缺省 = 无认证内网 relay); 密码不落盘 (决策 3)
- file: 事件 JSON 落 {STATE}/<node>/notify_inbox/ —— LLM agent 的拉渠道 (§10)
- command: 事件 JSON 走 stdin 喂用户脚本 —— LLM agent 的推渠道 (§10, v1.5)
- 故障降级: 单渠道异常只记入返回结果, 绝不上抛影响调度
"""

from __future__ import annotations

import json
import os
import re
import smtplib
import subprocess
from email.header import Header
from email.mime.text import MIMEText
from typing import Any

from . import state

SMTP_TIMEOUT_SEC = 10
COMMAND_TIMEOUT_SEC = 10  # command 渠道子进程超时 (§6 每渠道 timeout=10)
ACKED_KEEP_DAYS = 7  # acked 事件保留天数 (dispatcher tick 顺带清理)

ALL_EVENTS = ("batch_done", "batch_blocked")


# ---------- 事件构造 (所有渠道同源) ----------

def build_event(conn, batch, host_dir: str) -> dict[str, Any]:
    """批次终态 -> 统一事件 JSON (email 正文/inbox 文件/command stdin 同源).

    counts/failures 只计每 task 最新 version (与 C4 retry 同口径, resubmit
    旧版本不重复计数)。failures 带日志绝对路径 —— agent 拿到即可 Read 排雷。
    """
    kind = "batch_done" if batch["status"] == "done" else "batch_blocked"
    rows = conn.execute(
        "SELECT j.* FROM jobs j"
        " JOIN (SELECT task_id, MAX(version) AS mv FROM jobs"
        "       WHERE batch_id=? GROUP BY task_id) t"
        "   ON j.batch_id=? AND j.task_id=t.task_id AND j.version=t.mv",
        (batch["id"], batch["id"]),
    ).fetchall()
    counts: dict[str, int] = {}
    failures: list[dict[str, Any]] = []
    starts, fins, git_rev = [], [], None
    for j in rows:
        counts[j["status"]] = counts.get(j["status"], 0) + 1
        if j["started_at"]:
            starts.append(j["started_at"])
        if j["finished_at"]:
            fins.append(j["finished_at"])
        if not git_rev and j["git_rev"]:
            git_rev = j["git_rev"]
        if j["status"] in ("failed", "blocked", "cancelled", "timed_out"):
            failures.append({
                "task_id": j["task_id"],
                "status": j["status"],
                "failure": j["failure"],
                "rc": j["rc"],
                "log": os.path.join(
                    host_dir, "logs", batch["id"],
                    f"{j['task_id']}-v{j['version']}.log",
                ),
            })
    started = min(starts) if starts else None
    finished = max(fins) if fins else None
    duration_min = None
    if started and finished:
        import time as _t

        try:
            duration_min = round(
                (_t.mktime(_t.strptime(finished, "%Y-%m-%d %H:%M:%S"))
                 - _t.mktime(_t.strptime(started, "%Y-%m-%d %H:%M:%S"))) / 60
            )
        except ValueError:
            pass
    return {
        "event": kind,
        "batch": batch["name"],
        "batch_id": batch["id"],
        "project": batch["project"] if "project" in batch.keys() else None,
        "node": state.hostname(),
        "git_rev": git_rev,
        "started_at": started,
        "finished_at": finished,
        "duration_min": duration_min,
        "counts": counts,
        "failures": failures,
    }


def render_subject(event: dict[str, Any]) -> str:
    mark = "✅" if event["event"] == "batch_done" else "❌"
    n_done = event["counts"].get("done", 0) + event["counts"].get("skip", 0)
    n_all = sum(event["counts"].values())
    dur = f", 耗时 {event['duration_min']}min" if event["duration_min"] is not None else ""
    status = "done" if event["event"] == "batch_done" else "blocked"
    proj = f"[{event['project']}] " if event.get("project") else ""
    return f"[sched] {proj}批次 {event['batch']} {mark} {status} ({n_done}/{n_all} 任务{dur})"


def render_text(event: dict[str, Any]) -> str:
    """纯文本正文 (email body; inbox JSON 里同字段, 供人读)."""
    lines = [
        f"批次: {event['batch']} ({event['batch_id']})"
        + (f"  [project: {event['project']}]" if event.get("project") else ""),
        f"状态: {'done' if event['event'] == 'batch_done' else 'blocked'}",
        f"节点: {event['node']}" + (f"    git: {event['git_rev']}" if event["git_rev"] else ""),
        f"耗时: {event['started_at']} → {event['finished_at']}",
        "任务: " + " / ".join(f"{k} {v}" for k, v in sorted(event["counts"].items())),
    ]
    if event["failures"]:
        lines.append("失败明细:")
        for f in event["failures"]:
            lines.append(
                f"  {f['task_id']} [{f['status']}] failure={f['failure']} rc={f['rc']}"
            )
            lines.append(f"    日志: {f['log']}")
    else:
        lines.append("失败明细: (无)")
    return "\n".join(lines)


# ---------- 渠道 ----------

def _send_email(event: dict[str, Any], ncfg: dict[str, Any]) -> str:
    ecfg = ncfg["email"]
    host = ecfg["smtp_host"]
    port = int(ecfg.get("smtp_port", 465))
    to = ecfg["to"]
    msg = MIMEText(render_text(event), "plain", "utf-8")
    msg["Subject"] = Header(render_subject(event), "utf-8")
    msg["From"] = ecfg["from"]
    msg["To"] = ", ".join(to)
    if port == 465:
        smtp: smtplib.SMTP = smtplib.SMTP_SSL(host, port, timeout=SMTP_TIMEOUT_SEC)
    else:
        smtp = smtplib.SMTP(host, port, timeout=SMTP_TIMEOUT_SEC)
        if port == 587:
            smtp.starttls()
    with smtp:
        # 决策 3: password_env 可选 —— 缺省 = 无认证内网 relay
        pwd_env = ecfg.get("password_env")
        pwd = os.environ.get(pwd_env) if pwd_env else None
        if pwd and ecfg.get("user"):
            smtp.login(ecfg["user"], pwd)
        smtp.sendmail(ecfg["from"], to, msg.as_string())
    return f"email -> {','.join(to)}"


def _send_file(event: dict[str, Any], _ncfg: dict[str, Any]) -> str:
    """file 渠道 (§10 L1): 事件 JSON 落 notify_inbox, 供 LLM agent 拉取."""
    d = inbox_dir()
    os.makedirs(d, exist_ok=True)
    ts = (event.get("finished_at") or state.now()).replace("-", "").replace(" ", "-").replace(":", "")
    safe = re.sub(r"[^0-9A-Za-z_一-鿿-]", "_", event["batch"])
    kind = "done" if event["event"] == "batch_done" else "blocked"
    p = os.path.join(d, f"{ts}-{safe}.{kind}.json")
    with open(p, "w", encoding="utf-8") as f:
        json.dump(event, f, ensure_ascii=False, indent=2)
    return f"file -> {p}"


def _send_command(event: dict[str, Any], ncfg: dict[str, Any]) -> str:
    """command 渠道 (§10 L2 推): 事件 JSON 走 stdin 喂用户脚本, 唤醒 LLM agent.

    脚本由用户按 harness 定制 (tmux send-keys / headless 调用, 示例见
    sched/scripts/notify_*.sh), 框架只负责喂 stdin, 不绑定任何 harness。
    rc!=0 / 超时 -> 抛异常交 send() 降级为 FAIL (与其他渠道同语义)。
    """
    cmd: list[str] = ncfg["command"]
    p = subprocess.run(
        cmd,
        input=json.dumps(event, ensure_ascii=False),
        capture_output=True, text=True, timeout=COMMAND_TIMEOUT_SEC,
    )
    if p.returncode != 0:
        raise RuntimeError(
            f"rc={p.returncode} stderr={p.stderr.strip()[:200]}"
        )
    return f"command -> {cmd[0]}"


CHANNELS = {"email": _send_email, "file": _send_file, "command": _send_command}
_RESERVED = ("webhook",)  # 预留占位 (决策 2): schema 保留字段, 实现时只加函数


def inbox_dir() -> str:
    """{STATE}/<node>/notify_inbox/ (决策 5B 同款按节点隔离)."""
    return os.path.join(state.default_state_dir(), state.hostname(), "notify_inbox")


def send(event: dict[str, Any], cfg: dict[str, Any]) -> list[str]:
    """投递到已配置渠道. 返回每渠道结果串 ("ok: ..." / "FAIL: ...").

    单渠道异常只记结果不上抛 (通知故障绝不影响调度, §6);
    notify 段缺省 / 事件不在 on 列表 -> 空列表 (功能关闭)。
    """
    ncfg = (cfg or {}).get("notify") or {}
    if not ncfg or event["event"] not in ncfg.get("on", ALL_EVENTS):
        return []
    results: list[str] = []
    for name in _RESERVED:
        if ncfg.get(name) is not None:
            results.append(f"SKIP: {name} 渠道预留未实现 (设计 §4)")
    for name, fn in CHANNELS.items():
        channel_cfg = ncfg.get(name)
        if channel_cfg is None:
            continue
        if isinstance(channel_cfg, dict) and channel_cfg.get("enabled") is False:
            continue
        try:
            results.append(f"ok: {fn(event, ncfg)}")
        except Exception as e:  # noqa: BLE001 — 降级是设计语义
            results.append(f"FAIL: {name}: {e}")
    return results


# ---------- inbox 管理 ----------

def list_inbox(unacked_only: bool = True) -> list[str]:
    """inbox 事件文件路径 (新->旧). unacked_only=False 含已确认."""
    d = inbox_dir()
    if not os.path.isdir(d):
        return []
    files = []
    for f in os.listdir(d):
        if not f.endswith(".json") and not f.endswith(".json.acked"):
            continue
        if unacked_only and f.endswith(".acked"):
            continue
        files.append(os.path.join(d, f))
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files


def ack(path: str) -> str:
    """确认事件: rename 加 .acked 后缀. 返回新路径."""
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    new = path if path.endswith(".acked") else path + ".acked"
    if new != path:
        os.rename(path, new)
    return new


def cleanup_acked(days: int = ACKED_KEEP_DAYS) -> int:
    """清理 N 天前已确认事件 (dispatcher tick 顺带). 返回删除数."""
    import time as _t

    d = inbox_dir()
    if not os.path.isdir(d):
        return 0
    cutoff = _t.time() - days * 86400
    n = 0
    for f in os.listdir(d):
        if not f.endswith(".acked"):
            continue
        p = os.path.join(d, f)
        try:
            if os.path.getmtime(p) < cutoff:
                os.unlink(p)
                n += 1
        except OSError:
            pass
    return n
