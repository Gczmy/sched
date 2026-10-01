"""Exercise unavailable kernel/permission evidence with real isolated seccomp filters."""
from __future__ import annotations

import argparse
import ctypes
import errno
import json
import platform
from pathlib import Path
import subprocess
import sys
from unittest import mock

from gsched.execution import BackendUnavailable, LinuxFdBackend, capabilities

DENIALS = {"execveat_missing": (322, errno.ENOSYS), "close_range_permission": (436, errno.EPERM),
           "memfd_permission": (319, errno.EPERM), "socket_permission": (41, errno.EPERM)}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def restrict(number, error):
    class Filter(ctypes.Structure):
        _fields_ = [("code", ctypes.c_ushort), ("jt", ctypes.c_ubyte), ("jf", ctypes.c_ubyte), ("k", ctypes.c_uint)]
    class Program(ctypes.Structure):
        _fields_ = [("len", ctypes.c_ushort), ("filter", ctypes.POINTER(Filter))]
    # Load the syscall number, deny precisely one number, allow all others.
    instructions = (Filter * 4)(Filter(0x20, 0, 0, 0), Filter(0x15, 0, 1, number),
                                Filter(0x06, 0, 0, 0x00050000 | error), Filter(0x06, 0, 0, 0x7fff0000))
    program = Program(len(instructions), instructions)
    libc = ctypes.CDLL(None, use_errno=True)
    for result in (libc.prctl(38, 1, 0, 0, 0), libc.prctl(22, 2, ctypes.byref(program), 0, 0)):
        require(result == 0, "isolated seccomp filter installation failed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--deny", choices=DENIALS)
    args = parser.parse_args()
    require(sys.platform == "linux" and platform.machine() == "x86_64", "acceptance requires Linux x86_64")
    baseline = capabilities.snapshot()["backends"]
    require(all(baseline[k]["status"] == "available" for k in ("linux_fd", "linux_fd_owner")), "native baseline unavailable")
    if args.deny:
        restrict(*DENIALS[args.deny])
        with mock.patch("subprocess.Popen", side_effect=AssertionError("capability probe launched a child")):
            value = capabilities.snapshot()["backends"]
            kind = "linux_fd" if args.deny.startswith(("execveat", "close_range")) else "linux_fd_owner"
            reason = "kernel_fd_exec_unavailable" if kind == "linux_fd" else "owner_primitives_unavailable"
            require(value[kind]["status"] == "unavailable" and value[kind]["reason"] == reason
                    and value[kind]["verified"] == [], "denied primitive was incorrectly verified")
            if kind == "linux_fd":
                try:
                    LinuxFdBackend()
                except BackendUnavailable as error:
                    require(error.reason == reason, "wrong admission failure reason")
                else:
                    raise RuntimeError("denied backend admitted")
        print(json.dumps({"denial": args.deny, "backend": kind, "reason": reason}), flush=True)
    else:
        for denial in DENIALS:
            subprocess.run([sys.executable, str(Path(__file__).resolve()), "--deny", denial], check=True, timeout=30)
        print("PASS: real syscall denials never verify unavailable capabilities or fall back", flush=True)


if __name__ == "__main__":
    main()
