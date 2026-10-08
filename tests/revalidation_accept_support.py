"""Linux-only fault injection into one exact private daemon regex child.

No state/DB access. pidfd and double-checked /proc identity prevent signalling
unrelated processes or a replacement PID. The daemon still owns timeout/wait.
"""
import os
from pathlib import Path
import signal
import threading
import time

from gsched.artifacts import REGEX_CHECK_PROGRAM


def proc_identity(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return int(fields[1]), fields[19]


class StopFirstRegex:
    def __init__(self, daemon_pid, pattern):
        self.daemon_pid, self.pattern = daemon_pid, pattern
        self.daemon_identity = proc_identity(daemon_pid)
        self.fd = None
        self.error = None
        self.stopped = threading.Event()
        self.cancelled = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        try:
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline and not self.cancelled.is_set():
                if proc_identity(self.daemon_pid) != self.daemon_identity:
                    raise RuntimeError("private daemon identity changed")
                for path in Path("/proc").iterdir():
                    if not path.name.isdigit():
                        continue
                    try:
                        pid = int(path.name)
                        identity = proc_identity(pid)
                        if identity[0] != self.daemon_pid:
                            continue
                        argv = (path / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
                        expected = [b"-I", b"-c", REGEX_CHECK_PROGRAM.encode(), self.pattern.encode(), b"0"]
                        if argv[1:] != expected:
                            continue
                        fd = os.pidfd_open(pid, 0)
                        try:
                            if proc_identity(pid) != identity:
                                continue
                            signal.pidfd_send_signal(fd, signal.SIGSTOP)
                            self.fd = fd
                            fd = None
                            self.stopped.set()
                            return
                        finally:
                            if fd is not None:
                                os.close(fd)
                    except (ProcessLookupError, FileNotFoundError, PermissionError):
                        continue
                time.sleep(0.001)
            raise RuntimeError("did not observe the exact private regex child")
        except BaseException as error:
            self.error = error

    def check(self):
        self.thread.join(timeout=46)
        if self.error:
            raise self.error
        assert self.stopped.is_set() and not self.thread.is_alive()

    def close(self):
        self.cancelled.set()
        self.thread.join(timeout=2)
        if self.fd is not None:
            try:
                signal.pidfd_send_signal(self.fd, signal.SIGCONT)
            except ProcessLookupError:
                pass
            finally:
                os.close(self.fd)
                self.fd = None
