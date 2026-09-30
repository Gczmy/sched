"""Local preflight evidence, without launching children or connecting owners."""
from __future__ import annotations

import os
import select
import socket
import struct
import time
import uuid

from .. import __version__
from ..execution_policy import sealed_bytes
from .backend import INTERFACE_VERSION, BackendUnavailable, LinuxFdBackend, SubprocessBackend
from .persistent import PersistentLinuxFdBackend, boot_id, start_ticks


def _owner_primitives():
    if not hasattr(select, "poll"):
        raise OSError("poll unavailable")
    uuid.UUID(boot_id())
    ticks = start_ticks(os.getpid())
    if type(ticks) is not int or ticks <= 0:
        raise OSError("process identity unavailable")
    descriptor = sealed_bytes(b"{}", "sched-capabilities")
    os.close(descriptor)
    # Exercise abstract UNIX addressing and credentials without creating files,
    # contacting a recorded service, or starting a subprocess.
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind("\0gsched-capability-" + uuid.uuid4().hex)
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        pid, uid, _ = struct.unpack("3i", left.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        if pid != os.getpid() or uid != os.getuid():
            raise OSError("peer credentials unavailable")
    finally:
        left.close()
        right.close()


def _result(backend, status, reason=None):
    capabilities = sorted(backend.capabilities)
    return {"status": status, "reason": reason, "declared": capabilities,
            "verified": capabilities if status == "available" else []}


def snapshot():
    ordinary = _result(SubprocessBackend, "available" if os.name == "posix" else "unavailable",
                       None if os.name == "posix" else "non_posix")
    try:
        LinuxFdBackend()
    except BackendUnavailable as error:
        native = _result(LinuxFdBackend, "unavailable", error.reason)
    except Exception:
        native = _result(LinuxFdBackend, "unknown", "probe_failed")
    else:
        native = _result(LinuxFdBackend, "available")
    if native["status"] != "available":
        owner = _result(PersistentLinuxFdBackend, native["status"], native["reason"])
    else:
        try:
            _owner_primitives()
        except (OSError, AttributeError, ValueError, NotImplementedError):
            owner = _result(PersistentLinuxFdBackend, "unavailable", "owner_primitives_unavailable")
        except Exception:
            owner = _result(PersistentLinuxFdBackend, "unknown", "probe_failed")
        else:
            owner = _result(PersistentLinuxFdBackend, "available")
    return {"schema_version": 1, "query": "execution_capabilities", "sched_version": __version__,
            "interface_version": INTERFACE_VERSION, "query_host": socket.gethostname(), "observed_at": time.time(),
            "backends": {"subprocess": ordinary, "linux_fd": native, "linux_fd_owner": owner}}
