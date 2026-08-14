"""daemon 进程入口 (由 daemon.start 以 setsid 启动).

- 加载 config, 启动 Dispatcher 主循环
- SIGTERM: 未完成任务 cancelled 收尾 (N11) -> 退出
"""

from __future__ import annotations

import argparse
import os
import signal
import sys

from .config import load_config


def main() -> int:
    ap = argparse.ArgumentParser(prog="sched-daemon")
    ap.add_argument("--daemon", action="store_true")
    args = ap.parse_args()

    fake = bool(os.environ.get("SCHED_FAKE_GPUS"))

    from .dispatcher import Dispatcher

    try:
        cfg = load_config()
    except Exception as e:
        print(f"config 加载失败: {e}", file=sys.stderr)
        return 1

    # M1: 确保建表 (dispatcher 进程独立启动, 不依赖 CLI 侧 init_db)
    from . import state as state_mod

    state_mod.init_db()

    d = Dispatcher(cfg, fake=fake)

    def _sigterm(signum, frame):
        print("[daemon] SIGTERM 收尾: 未完成任务标 cancelled")
        d.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)

    if not d.acquire_lock():
        print("[daemon] 另一个 dispatcher 已在运行, 退出", file=sys.stderr)
        return 1

    try:
        d.run()
    except Exception as e:
        print(f"[daemon] 异常退出: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
