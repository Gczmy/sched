"""Explicit, project-independent launch-time CPU/cgroup bindings.

The caller owns delegation, controller configuration and scope lifetime. This
module neither creates cgroups nor interprets application data or device policy.
Affinity alone is not a non-widenable cgroup boundary.
"""
from __future__ import annotations

import ctypes
from dataclasses import dataclass
import os
import stat
import sys

CONSTRAINTS_VERSION = "sched-execution-constraints/v1"
MAX_CPU_INDEX = 2 ** 20 - 1
MAX_AFFINITY_COUNT = 8192


@dataclass(frozen=True)
class LaunchConstraints:
    cpu_affinity: tuple[int, ...] = ()
    cgroup_procs_fd: int | None = None
    interface_version: str = CONSTRAINTS_VERSION

    def __post_init__(self):
        if self.interface_version != CONSTRAINTS_VERSION:
            raise ValueError("unsupported launch constraints interface")
        cpus = self.cpu_affinity
        if (type(cpus) is not tuple or len(cpus) > MAX_AFFINITY_COUNT
                or any(type(cpu) is not int or not 0 <= cpu <= MAX_CPU_INDEX for cpu in cpus)
                or tuple(sorted(set(cpus))) != cpus):
            raise ValueError("CPU affinity must be an ordered unique bounded tuple")
        if self.cgroup_procs_fd is not None and (type(self.cgroup_procs_fd) is not int or self.cgroup_procs_fd < 0):
            raise ValueError("invalid cgroup.procs descriptor")
        if not cpus and self.cgroup_procs_fd is None:
            raise ValueError("launch constraints must bind CPUs or a cgroup")

    def validate(self):
        from .backend import BackendUnavailable
        if sys.platform != "linux":
            raise BackendUnavailable("launch constraints require Linux", reason="non_linux")
        if self.cpu_affinity and not set(self.cpu_affinity) <= os.sched_getaffinity(0):
            raise BackendUnavailable("requested CPUs outside current affinity", reason="cpu_affinity_unavailable")
        descriptor = self.cgroup_procs_fd
        if descriptor is not None:
            import fcntl
            flags = fcntl.fcntl(descriptor, fcntl.F_GETFL)
            info = os.fstat(descriptor)
            path = os.readlink(f"/proc/self/fd/{descriptor}")
            if (not stat.S_ISREG(info.st_mode) or not path.endswith("/cgroup.procs")
                    or _filesystem_magic(descriptor) != 0x63677270
                    or flags & os.O_ACCMODE not in (os.O_WRONLY, os.O_RDWR)):
                raise ValueError("descriptor must retain writable cgroup-v2 cgroup.procs")
        return self


def _filesystem_magic(descriptor):
    # Linux statfs starts with a native long. The oversized aligned buffer is
    # read only for that field; no architecture-dependent trailing layout used.
    libc = ctypes.CDLL(None, use_errno=True)
    function = libc.fstatfs
    function.argtypes = (ctypes.c_int, ctypes.c_void_p)
    function.restype = ctypes.c_int
    buffer = (ctypes.c_long * 64)()
    if function(descriptor, ctypes.byref(buffer)) != 0:
        number = ctypes.get_errno()
        raise OSError(number, os.strerror(number))
    return buffer[0]


# This bootstrap runs with -I/-S and a fixed, minimal environment. Caller env,
# Python startup hooks and loader variables are supplied only at the final exec,
# after both cgroup join and exact affinity verification. No preexec_fn executes
# Python in the forked, possibly multithreaded scheduler.
SUBPROCESS_ENTRY = r'''
import errno,json,os,sys
boot,error_fd = map(int,sys.argv[1:])
try:
    raw=os.pread(boot,2*1024*1024+1,0)
    os.close(boot)
    if len(raw)>2*1024*1024: raise OSError(errno.E2BIG,'constraint bootstrap bound')
    body=json.loads(raw)
    descriptor=body['cgroup_procs_fd']
    if descriptor is not None:
        if os.write(descriptor,b'0\n') != 2: raise OSError(errno.EIO,'short cgroup join')
        os.close(descriptor)
    cpus=body['cpu_affinity']
    if cpus:
        os.sched_setaffinity(0,cpus)
        if os.sched_getaffinity(0) != set(cpus): raise OSError(errno.EXDEV,'affinity differs')
    os.set_inheritable(error_fd,False)
    os.execvpe(body['argv'][0],body['argv'],body['env'])
except BaseException as error:
    number=getattr(error,'errno',None) or errno.EIO
    try: os.write(error_fd,(str(number)+'\n').encode('ascii'))
    except OSError: pass
    os._exit(127)
'''
