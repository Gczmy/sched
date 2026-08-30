"""daemon 生命周期 (文档 §2.1c / B13 H2 / §7 M0).

- start: setsid 脱离会话启动 dispatcher (托管分层: systemd user > tmux > screen > setsid 裸后台)
- stop: 未完成任务 cancelled 收尾 (N11)
- status: PID 文件 + 心跳双校验
- check: H2 前置检查清单 (M0)
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import socket
import stat
import time
from contextlib import ExitStack
from typing import Any

from . import state
from .config import ConfigError, load_config
from .executor import process_start_token
STOP_TIMEOUT_SEC = 120
START_TIMEOUT_SEC = 10.0
START_POLL_SEC = 0.5
START_STOP_GRACE_SEC = 2.0



def _host_dir() -> str:
    return state.host_dir()


def _pid_file() -> str:
    return os.path.join(_host_dir(), "daemon.pid")


def _heartbeat_file() -> str:
    return os.path.join(_host_dir(), "daemon.heartbeat")

def _owner_file() -> str:
    return os.path.join(_host_dir(), "dispatcher.lock", "owner.json")


def _read_lease_owner() -> dict[str, Any] | None:
    try:
        # These flags are part of the safety boundary.  Do not silently fall
        # back to path-following behavior on a platform that lacks them.
        directory_flags = (
            os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
        )
        owner_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
        with ExitStack() as stack:
            # Pin and verify every mutable path component below state_dir
            # before a PID read from the lease may authorize a signal.
            host_fd = os.open(_host_dir(), directory_flags)
            stack.callback(os.close, host_fd)
            host_stat = os.fstat(host_fd)
            if (
                not stat.S_ISDIR(host_stat.st_mode)
                or host_stat.st_uid != os.getuid()
                or stat.S_IMODE(host_stat.st_mode) & 0o022
            ):
                return None
            lock_fd = os.open("dispatcher.lock", directory_flags, dir_fd=host_fd)
            stack.callback(os.close, lock_fd)
            lock_stat = os.fstat(lock_fd)
            if (
                not stat.S_ISDIR(lock_stat.st_mode)
                or lock_stat.st_uid != os.getuid()
                or stat.S_IMODE(lock_stat.st_mode) & 0o022
            ):
                return None
            owner_fd = os.open("owner.json", owner_flags, dir_fd=lock_fd)
            stack.callback(os.close, owner_fd)
            owner_stat = os.fstat(owner_fd)
            if (
                not stat.S_ISREG(owner_stat.st_mode)
                or owner_stat.st_uid != os.getuid()
                or owner_stat.st_nlink != 1
                or stat.S_IMODE(owner_stat.st_mode) & 0o022
                or owner_stat.st_size > 4096
            ):
                return None
            raw = os.read(owner_fd, 4097)
            if len(raw) > 4096:
                return None
            owner = json.loads(raw.decode("utf-8"))
    except (
        AttributeError,
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        NotImplementedError,
        RecursionError,
        TypeError,
        ValueError,
    ):
        return None
    if not isinstance(owner, dict) or owner.get("schema_version") != 1:
        return None
    pid = owner.get("pid")
    lease_id = owner.get("lease_id")
    start_token = owner.get("start_token")
    physical_host = owner.get("physical_host")
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid <= 0
        or not isinstance(lease_id, str)
        or not lease_id
        or not isinstance(start_token, str)
        or not start_token
        or not isinstance(physical_host, str)
        or not physical_host.strip()
    ):
        return None
    return {
        "schema_version": 1,
        "lease_id": lease_id,
        "pid": pid,
        "start_token": start_token,
        "physical_host": physical_host.strip(),
    }


def _read_pid() -> int | None:
    owner = _read_lease_owner()
    if owner is not None:
        return owner["pid"]
    try:
        with open(_pid_file(), encoding="utf-8") as stream:
            return int(stream.read().strip())
    except (OSError, ValueError, TypeError):
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
    """Restart the daemon when work was submitted during a coordinated shutdown."""
    if state.idle_shutdown_pending():
        for _ in range(700):
            if not is_running():
                break
            time.sleep(0.1)
        if is_running():
            return "daemon 正在退出未完成, 请稍后重试 (任务已保留)"
        state.clear_idle_shutdown()
        return start(fake=bool(os.environ.get("SCHED_FAKE_GPUS")))
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

def _heartbeat_matches_current_lease() -> bool:
    try:
        owner_stat = os.lstat(_owner_file())
        heartbeat_stat = os.lstat(_heartbeat_file())
    except OSError:
        return False
    return bool(
        stat.S_ISREG(owner_stat.st_mode)
        and stat.S_ISREG(heartbeat_stat.st_mode)
        and time.time() - heartbeat_stat.st_mtime < 60
        and heartbeat_stat.st_mtime_ns >= owner_stat.st_mtime_ns
    )


def _started_child_ready(pid: int, expected_start: str | None) -> bool:
    if expected_start is None or not _heartbeat_matches_current_lease():
        return False
    owner = _read_lease_owner()
    return bool(
        owner is not None
        and owner["pid"] == pid
        and owner["start_token"] == expected_start
        and process_start_token(pid) == expected_start
    )


def _terminate_unready_child(
    proc: subprocess.Popen,
    expected_start: str | None,
) -> str:
    if proc.poll() is not None:
        return f"子进程已退出 rc={proc.returncode}"
    if expected_start is None or process_start_token(proc.pid) != expected_start:
        return "子进程 identity 无法确认，拒绝发送信号并保留"
    try:
        os.kill(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return "子进程已退出"
    except (PermissionError, OSError) as error:
        return f"SIGTERM 失败 ({error})，保留子进程"
    try:
        proc.wait(timeout=START_STOP_GRACE_SEC)
        return "已发送 SIGTERM 并确认退出"
    except subprocess.TimeoutExpired:
        pass
    if process_start_token(proc.pid) != expected_start:
        return "TERM 后 identity 无法确认，拒绝 SIGKILL 并保留"
    try:
        os.kill(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        return "TERM 后已退出"
    except (PermissionError, OSError) as error:
        return f"SIGKILL 失败 ({error})，保留子进程"
    try:
        proc.wait(timeout=START_STOP_GRACE_SEC)
    except subprocess.TimeoutExpired:
        return "SIGKILL 后仍无法确认退出，保留 ownership 状态"
    return "已 exact SIGKILL 并确认退出"


def start(fake: bool = False, force: bool = False) -> str:
    if not force and is_running():
        return f"daemon 已在运行 ({status_str()})"
    issues = check(fake=fake)
    hard = [item for item in issues if item.get("level") == "fail"]
    if hard:
        return "前置检查未通过, 拒绝启动:\n" + "\n".join(
            f"  [FAIL] {item['item']}: {item['detail']}" for item in hard
        )

    state.ensure_private_directory(_host_dir())
    log_path = os.path.join(_host_dir(), "daemon.log")
    env = dict(os.environ)
    if fake:
        env["SCHED_FAKE_GPUS"] = env.get("SCHED_FAKE_GPUS", "0,1,2,3")
    else:
        env.pop("SCHED_FAKE_GPUS", None)

    cmd = [sys.executable, "-m", "gsched.dispatcher_main", "--daemon"]
    log_stream = state.open_private_text(log_path, "a")
    try:
        proc = subprocess.Popen(
            cmd,
            env=env,
            stdout=log_stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            stdin=subprocess.DEVNULL,
        )
    finally:
        log_stream.close()

    expected_start = process_start_token(proc.pid)
    deadline = time.monotonic() + START_TIMEOUT_SEC
    while True:
        rc = proc.poll()
        if rc is not None:
            return (
                f"daemon 启动失败: 子进程在就绪前退出 rc={rc} "
                f"(pid={proc.pid}, 日志 {log_path})"
            )
        if expected_start is None:
            expected_start = process_start_token(proc.pid)
        if _started_child_ready(proc.pid, expected_start):
            return f"daemon 启动成功 (pid={proc.pid}, 日志 {log_path})"
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(START_POLL_SEC, remaining))

    if _started_child_ready(proc.pid, expected_start):
        return f"daemon 启动成功 (pid={proc.pid}, 日志 {log_path})"
    cleanup = _terminate_unready_child(proc, expected_start)
    return (
        f"daemon 启动失败: pid={proc.pid} 在 {START_TIMEOUT_SEC:g}s 内未就绪; "
        f"{cleanup}; 请查 {log_path}"
    )


def stop() -> str:
    owner = _read_lease_owner()
    if owner is None:
        if os.path.lexists(_owner_file()):
            return (
                "daemon lease 缺少可验证的 physical_host 或 ownership 内容无效; "
                "拒绝发送信号并保留 sidecar"
            )
        if _heartbeat_fresh():
            return (
                "daemon 心跳新鲜但 exact lease 不可验证; "
                "拒绝发送信号并保留 ownership 状态。"
                "请到计算节点核验并执行 sched daemon stop"
            )
        if not _cleanup(None):
            return (
                "daemon lease/sidecar 状态在清理时不可验证、"
                "已变化或清理失败; 拒绝报告未运行并保留 sidecar"
            )
        return "daemon 未运行 (无可验证 lease)"
    owner_physical_host = owner.get("physical_host")
    if not isinstance(owner_physical_host, str):
        owner_physical_host = ""
    else:
        owner_physical_host = owner_physical_host.strip()
    try:
        local_physical_host = socket.gethostname().strip()
    except (OSError, AttributeError):
        local_physical_host = ""
    if (
        not owner_physical_host
        or not local_physical_host
        or owner_physical_host != local_physical_host
    ):
        return (
            f"daemon lease 属于 physical_host={owner_physical_host!r}, "
            f"本机 physical_host={local_physical_host!r}; "
            "拒绝发送信号并保留 sidecar"
        )
    pid = owner["pid"]
    expected_start = owner["start_token"]
    if not _pid_alive(pid):
        if _heartbeat_fresh():
            return (
                f"daemon 在 {state.hostname()} 运行 (心跳新鲜, "
                "但本机 PID 不可见)。请到计算节点执行 sched daemon stop"
            )
        if not _cleanup(owner):
            return (
                "daemon lease/sidecar 状态在清理前不可验证、"
                "已变化或清理失败; 拒绝报告停止成功并保留 sidecar"
            )
        return f"daemon 已停止 (pid={pid})"
    actual_start = process_start_token(pid)
    if actual_start is None or actual_start != expected_start:
        return (
            f"daemon pid={pid} 身份无法确认 (start token 不匹配或不可读); "
            "拒绝发送信号并保留 ownership 状态"
        )

    stop_token = None
    try:
        with state.submission_lock():
            stop_token = state.mark_idle_shutdown()
    except OSError:
        pass

    def clear_stop_token() -> None:
        if stop_token is None:
            return
        try:
            with state.submission_lock():
                state.clear_idle_shutdown(stop_token)
        except OSError:
            pass

    # Re-attest immediately before the destructive operation.
    if process_start_token(pid) != expected_start:
        clear_stop_token()
        return (
            f"daemon pid={pid} 身份在 stop 前变化; "
            "拒绝发送信号并保留 ownership 状态"
        )
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        clear_stop_token()
        if not _cleanup(owner):
            return (
                "daemon lease/sidecar 状态在进程退出后不可验证、"
                "已变化或清理失败; 拒绝报告停止成功并保留 sidecar"
            )
        return f"daemon 已停止 (pid={pid})"
    except (PermissionError, OSError) as exc:
        clear_stop_token()
        return (
            f"daemon pid={pid} SIGTERM 发送失败: {exc}; "
            "保留 ownership 状态"
        )
    for _ in range(int(STOP_TIMEOUT_SEC / 0.5)):
        time.sleep(0.5)
        actual_start = process_start_token(pid)
        if actual_start == expected_start:
            continue
        if actual_start is None and _pid_alive(pid):
            # Token lookup can fail transiently; liveness alone does not attest
            # death and must not authorize ownership cleanup.
            continue
        if not _cleanup(owner):
            return (
                "daemon lease/sidecar 状态在停止确认后不可验证、"
                "已变化或清理失败; 拒绝报告停止成功并保留 sidecar"
            )
        return f"daemon 已停止 (pid={pid})"
    return (
        f"daemon 停止超时 (pid={pid}, 已等待 {STOP_TIMEOUT_SEC}s); "
        "拒绝强杀，保留 daemon ownership 状态"
    )


def _cleanup(expected_owner: dict[str, Any] | None) -> bool:
    """Remove derived sidecars only for an exact or fully absent lease."""
    import fcntl

    owner_path = _owner_file()
    lock_dir = os.path.dirname(owner_path)
    guard_path = f"{lock_dir}.guard"
    try:
        with state.open_private_text(guard_path, "a+") as guard:
            fcntl.flock(guard.fileno(), fcntl.LOCK_EX)
            try:
                current_owner = _read_lease_owner()
                if current_owner is None:
                    # A graceful dispatcher removes its exact lease before the
                    # process exits.  Under the shared guard, a fully absent
                    # owner and lock directory therefore proves completed
                    # self-cleanup rather than successor replacement.
                    try:
                        os.lstat(owner_path)
                    except FileNotFoundError:
                        pass
                    except OSError:
                        return False
                    else:
                        return False
                    try:
                        os.lstat(lock_dir)
                    except FileNotFoundError:
                        pass
                    except OSError:
                        return False
                    else:
                        return False
                else:
                    if current_owner != expected_owner:
                        return False
                    try:
                        lock_stat = os.lstat(lock_dir)
                    except OSError:
                        return False
                    if not stat.S_ISDIR(lock_stat.st_mode):
                        return False

                for path in (_pid_file(), _heartbeat_file()):
                    try:
                        os.unlink(path)
                    except FileNotFoundError:
                        pass
                    except OSError:
                        return False
                return True
            finally:
                fcntl.flock(guard.fileno(), fcntl.LOCK_UN)
    except (OSError, state.StateError):
        return False


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
            state.ensure_private_directory(p)
            test = os.path.join(p, ".write_test")
            state.open_private_text(test, "x").close()
            os.unlink(test)
            add(label, f"可写 {p}", "ok")
        except (OSError, state.StateError) as e:
            add(label, f"不可写: {e}", "fail")

    # nvidia-smi (2026-08-15: CPU-only 环境降级) —— config.gpus 空 = 纯 CPU 部署,
    # 无 GPU 需求时 nvidia-smi 缺失合法 (warn); 声明了 GPU 但本机无 nvidia-smi 才 fail
    from .config import parse_gpus

    gpus, _, _ = parse_gpus(cfg)
    if fake:
        add("nvidia-smi", "fake 模式跳过", "ok")
    elif shutil.which("nvidia-smi"):
        r = subprocess.run(["nvidia-smi", "-L"], capture_output=True, text=True, timeout=10)
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
