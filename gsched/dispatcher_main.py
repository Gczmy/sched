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
        # 审查 B1: 信号处理器绝不直接 d.stop() —— SIGTERM 可能落在主循环任一
        # `with state.connect()` 块内 (每 tick 都持事务), stop() 嵌套开连接会
        # database is locked (WAL 单写者) 炸出 handler, 导致 running 任务未标
        # cancelled、GPU 卡残留 assigned (与 N11 事故同族). 只置标志, 主循环
        # 在 tick 边界 (任何 connect() 块之外) 执行收尾.
        print("[daemon] SIGTERM: 请求优雅停止 (主循环 tick 边界收尾)")
        d.request_stop()

    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)

    if not d.acquire_lock():
        print("[daemon] 另一个 dispatcher 已在运行, 退出", file=sys.stderr)
        return 1

    try:
        d.run()
    except Exception as e:
        print(f"[daemon] 异常退出: {e}", file=sys.stderr)
        monitor = getattr(d, "_cluster_lease", None)
        if monitor is not None:
            try:
                monitor.finish("exception_exit_no_worker_wait")
            except Exception:
                print("[daemon] 租约退出证据不可写，保留最后观察", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
