"""daemon 生命周期 (文档 §2.1c / B13 H2 / §7 M0).

- start: setsid 脱离会话启动 dispatcher (托管分层: systemd user > tmux > screen > setsid 裸后台)
- stop: 未完成任务 cancelled 收尾 (N11)
- status: PID 文件 + 心跳双校验
- check: H2 前置检查清单 (M0)
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from typing import Any

from . import state
from .config import ConfigError, load_config

HOST_DIR = os.path.join(state.default_state_dir(), state.hostname())


def _host_dir() -> str:
    return HOST_DIR


def _pid_file() -> str:
    return os.path.join(_host_dir(), "daemon.pid")


def _heartbeat_file() -> str:
    return os.path.join(_host_dir(), "daemon.heartbeat")


def _read_pid() -> int | None:
    try:
        with open(_pid_file()) as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def _pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _heartbeat_fresh() -> bool:
    try:
        return time.time() - os.path.getmtime(_heartbeat_file()) < 60
    except OSError:
        return False


def is_running() -> bool:
    """判活: 心跳为主 (跨节点, 2026-08-15 修复).

    心跳文件在共享 NFS home 下, 登录节点/其他节点都能读到 daemon 的心跳 mtime
    —— 跨节点判活有效; PID kill -0 仅同节点有效 (登录节点看不到计算节点的 PID,
    旧实现误判"未运行" → 误触发 start → 连锁 nvidia-smi 检查失败).
    心跳过期 (>60s) 才判死 (daemon 卡死/崩溃/退出).
    """
    return _heartbeat_fresh()


def ensure_running() -> str:
    """定案 38: 所有"会产生可派发工作"的命令 (submit/run/retry/resubmit) 共享的
    再启动通道——daemon 未运行 (含 idle 自动退出后) 自动拉起.

    fake 由 SCHED_FAKE_GPUS 环境变量驱动 (验收/测试场景), 与 daemon start --fake 一致.
    start 本身幂等: 已在运行则跳过.
    """
    if is_running():
        return "daemon 已在运行"
    return start(fake=bool(os.environ.get("SCHED_FAKE_GPUS")))


def status_str() -> str:
    pid = _read_pid()
    if _heartbeat_fresh():
        return f"运行中 (pid={pid}, host={state.hostname()})"
    if _pid_alive(pid):
        return f"PID 存在但心跳过期 (pid={pid}, 可能卡死, F3/F4 会处理)"
    return "未运行"


# ---------- start ----------

def start(fake: bool = False) -> str:
    if is_running():
        return f"daemon 已在运行 ({status_str()})"
    # 前置检查 (M0)
    issues = check(fake=fake)
    hard = [i for i in issues if i.get("level") == "fail"]
    if hard:
        return "前置检查未通过, 拒绝启动:\n" + "\n".join(
            f"  [FAIL] {i['item']}: {i['detail']}" for i in hard
        )

    # 双 fork + setsid 脱离会话 (§2.1c 第 4 档兜底; systemd/tmux 托管由外部调用)
    os.makedirs(_host_dir(), exist_ok=True)
    log_path = os.path.join(_host_dir(), "daemon.log")

    # 构造 dispatcher 命令 (本进程作为 daemon 入口)
    if fake:
        env = dict(os.environ)
        env["SCHED_FAKE_GPUS"] = env.get("SCHED_FAKE_GPUS", "0,1,2,3")
    else:
        env = dict(os.environ)

    cmd = [sys.executable, "-m", "gsched.dispatcher_main", "--daemon"]
    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=open(log_path, "a"),
        stderr=subprocess.STDOUT,
        start_new_session=True,  # setsid: 脱离 ssh/screen 会话
        stdin=subprocess.DEVNULL,
    )
    # 等 PID 文件出现
    for _ in range(20):
        time.sleep(0.5)
        if is_running():
            return f"daemon 启动成功 (pid={_read_pid()}, 日志 {log_path})"
    return f"daemon 启动中 (pid={proc.pid}); 若未就绪请查 {log_path}"


def stop() -> str:
    pid = _read_pid()
    if not _pid_alive(pid):
        if _heartbeat_fresh():
            # 跨节点: PID 不可见但心跳新鲜 = daemon 在别的主机 (共享 home) 运行
            return (f"daemon 在 {state.hostname()} 运行 (心跳新鲜, 但本机 PID 不可见——"
                    "PID namespace 跨节点不同)。请到计算节点执行 sched daemon stop")
        _cleanup()
        return "daemon 未运行"
    # N11: 先标 cancelled 再 kill (由 dispatcher 的 SIGTERM handler 收尾)
    os.kill(pid, 15)  # SIGTERM -> dispatcher.stop()
    for _ in range(20):
        time.sleep(0.5)
        if not _pid_alive(pid):
            _cleanup()
            return f"daemon 已停止 (pid={pid})"
    os.kill(pid, 9)
    _cleanup()
    return f"daemon 强制停止 (pid={pid})"


def _cleanup() -> None:
    for p in (_pid_file(), _heartbeat_file()):
        try:
            os.unlink(p)
        except OSError:
            pass


# ---------- H2 前置检查 (M0) ----------

def check(fake: bool = False) -> list[dict[str, str]]:
    """返回检查项列表 [{item, detail, level: ok|warn|fail}]. fake 模式跳过 GPU/磁盘."""
    issues: list[dict[str, str]] = []
    cfg: dict[str, Any] | None = None
    try:
        cfg = load_config()
    except ConfigError as e:
        issues.append({"item": "config.json", "detail": str(e), "level": "fail"})
        return issues

    def add(item: str, detail: str, level: str = "ok") -> None:
        issues.append({"item": item, "detail": detail, "level": level})

    # 用户身份 (H2)
    import getpass

    who = getpass.getuser()
    if who == cfg.get("user"):
        add("用户身份", f"whoami={who} == config.user", "ok")
    else:
        add("用户身份", f"whoami={who} != config.user={cfg.get('user')}", "fail")

    # ROOT / state 可写
    for label, p in (
        ("state 目录", state.default_state_dir()),
        ("node state 目录", _host_dir()),
    ):
        try:
            os.makedirs(p, exist_ok=True)
            test = os.path.join(p, ".write_test")
            open(test, "w").close()
            os.unlink(test)
            add(label, f"可写 {p}", "ok")
        except OSError as e:
            add(label, f"不可写: {e}", "fail")

    # nvidia-smi (2026-08-15: CPU-only 环境降级) —— config.gpus 空 = 纯 CPU 部署,
    # 无 GPU 需求时 nvidia-smi 缺失合法 (warn); 声明了 GPU 但本机无 nvidia-smi 才 fail
    gpus = cfg.get("gpus") or []
    if fake:
        add("nvidia-smi", "fake 模式跳过", "ok")
    elif shutil.which("nvidia-smi"):
        r = subprocess.run(["nvidia-smi", "-L"], capture_output=True, text=True)
        add("nvidia-smi", "可查询" if r.returncode == 0 else r.stderr.strip(), "ok" if r.returncode == 0 else "fail")
    elif not gpus:
        add("nvidia-smi", "未找到 (config.gpus 为空, 纯 CPU 部署合法)", "warn")
    else:
        add("nvidia-smi", f"未找到 (config.gpus={gpus} 声明了 GPU, 但本机无 nvidia-smi)", "fail")

    # venv
    for name, path in cfg.get("venvs", {}).items():
        if os.path.isfile(path):
            add(f"venv {name}", f"可执行 {path}", "ok")
        else:
            add(f"venv {name}", f"不存在 {path}", "fail")

    # git 仓库 (A2 指纹)
    for name, proj in cfg.get("projects", {}).items():
        if not proj.get("git"):
            add(f"git {name}", "git:false 跳过", "ok")
            continue
        root = proj.get("root", "")
        r = subprocess.run(
            ["git", "-C", root, "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode == 0:
            add(f"git {name}", f"{root} rev={r.stdout.strip()[:8]}", "ok")
        else:
            add(f"git {name}", f"{root} 不是 git 仓库", "fail")

    # ulimit -n (H2, 定案 13: >= 8192)
    if not fake:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft >= 8192 or hard >= 8192:
            add("ulimit -n", f"soft={soft} hard={hard}", "ok")
        else:
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (8192, hard))
                add("ulimit -n", f"已自提 soft 8192 (hard={hard})", "ok")
            except (ValueError, OSError):
                add("ulimit -n", f"soft={soft} < 8192 且自提失败 (torch 多 worker 可能报 Too many open files)", "warn")

    # 磁盘 (B8: 剩余 >= 20GB)
    if not fake:
        try:
            st = os.statvfs(state.default_state_dir())
            free_gb = st.f_bavail * st.f_frsize / 1e9
            add("磁盘", f"剩余 {free_gb:.1f}GB", "ok" if free_gb >= 20 else "warn")
        except OSError:
            pass

    # 终端复用工具探测 (2.1c)
    tools = []
    if shutil.which("systemctl"):
        r = subprocess.run(
            ["systemctl", "--user", "is-system-running"], capture_output=True, text=True,
        )
        if r.returncode == 0 and "running" in r.stdout:
            tools.append("systemd-user")
    for t in ("tmux", "screen", "setsid"):
        if shutil.which(t):
            tools.append(t)
    add("终端工具", " > ".join(tools) + " (托管选层: systemd user > tmux > screen > setsid)", "ok")

    return issues
