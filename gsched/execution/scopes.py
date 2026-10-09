"""Explicit delegated cpuset scopes; no lease changes or process/wait inference.

The scheduler must durably store an intent before create, then the returned
inode binding before configure/launch. Restoring a binding only observes the
original scope: it never recreates, reconfigures or issues another launch FD.
The delegate and clients must be trusted; same-UID delegation is not a sandbox.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import os
from pathlib import PurePosixPath
import re
import stat
import sys
import uuid

from .backend import BackendUnavailable
from .constraints import LaunchConstraints, MAX_AFFINITY_COUNT, MAX_CPU_INDEX, _filesystem_magic

SCOPE_VERSION = "sched-cpu-scope/v1"
MAX_TEXT = 65536
MAX_MEMS = 4096


class ScopeUnavailable(BackendUnavailable):
    pass


def _refuse(reason):
    raise ScopeUnavailable(reason, reason=reason)


def _indices(text, *, maximum=MAX_AFFINITY_COUNT):
    if type(text) is not str or not text or len(text) > MAX_TEXT:
        _refuse("scope_cpu_or_memory_list_invalid")
    values = []
    for part in text.split(","):
        if re.fullmatch(r"(?:0|[1-9][0-9]{0,6})(?:-(?:0|[1-9][0-9]{0,6}))?", part) is None:
            _refuse("scope_cpu_or_memory_list_invalid")
        edges = list(map(int, part.split("-")))
        first, last = edges[0], edges[-1]
        if not 0 <= first <= last <= MAX_CPU_INDEX or len(values) + last - first + 1 > maximum:
            _refuse("scope_cpu_or_memory_list_exceeds_bound")
        if values and first <= values[-1]:
            _refuse("scope_cpu_or_memory_list_not_ordered")
        values.extend(range(first, last + 1))
    return tuple(values)


def _read(directory, name):
    descriptor = os.open(name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=directory)
    try:
        raw = os.read(descriptor, MAX_TEXT + 1)
        if len(raw) > MAX_TEXT:
            _refuse("scope_control_read_exceeds_bound")
        return raw.decode("ascii").strip()
    finally:
        os.close(descriptor)


def _write(directory, name, value):
    descriptor = os.open(name, os.O_WRONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=directory)
    try:
        raw = (value + "\n").encode("ascii")
        if os.write(descriptor, raw) != len(raw):
            _refuse("scope_control_short_write")
    finally:
        os.close(descriptor)


def _context():
    if sys.platform != "linux":
        _refuse("scope_requires_linux")
    descriptor = os.open("/proc/sys/kernel/random/boot_id", os.O_RDONLY | os.O_CLOEXEC)
    try:
        boot = os.read(descriptor, 128).decode("ascii").strip()
    finally:
        os.close(descriptor)
    namespace = os.readlink("/proc/self/ns/mnt")
    if re.fullmatch(r"[0-9a-f-]{36}", boot) is None or re.fullmatch(r"mnt:\[[0-9]+\]", namespace) is None:
        _refuse("scope_kernel_identity_unknown")
    return boot, namespace, os.geteuid()


def _open_directory(path):
    if (type(path) is not str or len(path) > 4096 or "\0" in path or not path.startswith("/")
            or path.startswith("//") or str(PurePosixPath(path)) != path or ".." in PurePosixPath(path).parts or path == "/"):
        raise ValueError("scope parent must be an explicit normalized absolute directory")
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        for part in PurePosixPath(path).parts[1:]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        result, descriptor = descriptor, None
        return result
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _directory_info(descriptor):
    info = os.fstat(descriptor)
    if not stat.S_ISDIR(info.st_mode) or _filesystem_magic(descriptor) != 0x63677270:
        _refuse("scope_parent_not_cgroup_v2")
    if info.st_uid not in (0, os.geteuid()) or info.st_mode & 0o022:
        _refuse("scope_parent_not_private_single_user_delegation")
    return info


@dataclass(frozen=True)
class ScopeParent:
    path: str
    device: int
    inode: int
    boot_id: str
    mount_namespace: str
    uid: int

    def __post_init__(self):
        if (type(self.path) is not str or not self.path.startswith("/") or len(self.path) > 4096
                or "\0" in self.path or self.path.startswith("//") or str(PurePosixPath(self.path)) != self.path
                or ".." in PurePosixPath(self.path).parts or self.path == "/"
                or type(self.device) is not int or self.device < 0
                or type(self.inode) is not int or self.inode <= 0
                or type(self.uid) is not int or self.uid < 0
                or type(self.boot_id) is not str or re.fullmatch(r"[0-9a-f-]{36}", self.boot_id) is None
                or type(self.mount_namespace) is not str or re.fullmatch(r"mnt:\[[0-9]+\]", self.mount_namespace) is None):
            raise ValueError("invalid immutable scope parent")


@dataclass(frozen=True)
class CpuScopeIntent:
    parent: ScopeParent
    scope_id: str
    cpus: tuple[int, ...]
    mems: tuple[int, ...]
    binding_sha256: str
    interface_version: str = SCOPE_VERSION

    def __post_init__(self):
        if (type(self.parent) is not ScopeParent or type(self.scope_id) is not str
                or re.fullmatch(r"[0-9a-f]{32}", self.scope_id) is None
                or type(self.binding_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", self.binding_sha256) is None
                or self.interface_version != SCOPE_VERSION):
            raise ValueError("invalid CPU scope intent")
        for values, maximum in ((self.cpus, MAX_AFFINITY_COUNT), (self.mems, MAX_MEMS)):
            if (type(values) is not tuple or not 0 < len(values) <= maximum
                    or any(type(v) is not int or not 0 <= v <= MAX_CPU_INDEX for v in values)
                    or tuple(sorted(set(values))) != values):
                raise ValueError("invalid CPU scope indices")

    @property
    def name(self):
        return "sched-cpu-" + self.scope_id

    def to_dict(self):
        value = asdict(self)
        value["cpus"], value["mems"] = list(self.cpus), list(self.mems)
        return value

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict or set(value) != set(cls.__dataclass_fields__) or type(value["parent"]) is not dict or any(type(value[k]) is not list for k in ("cpus", "mems")):
            raise ValueError("invalid serialized CPU scope intent")
        return cls(**{**value, "parent": ScopeParent(**value["parent"]),
                      "cpus": tuple(value["cpus"]), "mems": tuple(value["mems"])})


@dataclass(frozen=True)
class CpuScopeBinding:
    intent: CpuScopeIntent
    device: int
    inode: int

    def __post_init__(self):
        if type(self.intent) is not CpuScopeIntent or type(self.device) is not int or self.device < 0 or type(self.inode) is not int or self.inode <= 0:
            raise ValueError("invalid original CPU scope inode binding")

    def to_dict(self):
        return {"intent": self.intent.to_dict(), "device": self.device, "inode": self.inode}

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict or set(value) != {"intent", "device", "inode"}:
            raise ValueError("invalid serialized CPU scope binding")
        return cls(CpuScopeIntent.from_dict(value["intent"]), value["device"], value["inode"])


class DelegatedCpuScopes:
    """Only this explicit subtree; never enable or change parent controllers."""
    def __init__(self, parent_path):
        boot, namespace, uid = _context()
        self._fd = _open_directory(parent_path)
        try:
            info = _directory_info(self._fd)
            self.parent = ScopeParent(parent_path, info.st_dev, info.st_ino, boot, namespace, uid)
            self._verify()
            if not os.access(".", os.W_OK, dir_fd=self._fd, effective_ids=True):
                _refuse("scope_parent_not_writable")
        except BaseException:
            self.close()
            raise

    def _verify(self):
        if self._fd is None:
            raise RuntimeError("CPU scope manager closed")
        if _context() != (self.parent.boot_id, self.parent.mount_namespace, self.parent.uid):
            _refuse("scope_original_kernel_context_changed")
        info = _directory_info(self._fd)
        path = os.stat(self.parent.path, follow_symlinks=False)
        if (info.st_dev, info.st_ino) != (self.parent.device, self.parent.inode) or (path.st_dev, path.st_ino) != (info.st_dev, info.st_ino) or os.readlink(f"/proc/self/fd/{self._fd}") != self.parent.path:
            _refuse("scope_original_parent_identity_changed")
        # A non-root, empty domain must already have cpuset delegated. Reading
        # cpuset.cpus also excludes the hierarchy root's special exemption.
        _read(self._fd, "cpuset.cpus")
        if _read(self._fd, "cgroup.type") != "domain" or _read(self._fd, "cgroup.procs"):
            _refuse("scope_parent_not_empty_domain")
        if "cpuset" not in _read(self._fd, "cgroup.subtree_control").split():
            _refuse("scope_cpuset_not_delegated")
        return (_indices(_read(self._fd, "cpuset.cpus.effective"), maximum=65536),
                _indices(_read(self._fd, "cpuset.mems.effective"), maximum=MAX_MEMS))

    def intent(self, cpus, binding_sha256, *, scope_id=None):
        available, mems = self._verify()
        intent = CpuScopeIntent(self.parent, scope_id or uuid.uuid4().hex, cpus, mems, binding_sha256)
        if not set(cpus) <= set(available) or not set(cpus) <= os.sched_getaffinity(0):
            _refuse("scope_cpus_outside_original_authority")
        return intent

    def create(self, intent):
        self._verify()
        if type(intent) is not CpuScopeIntent or intent.parent != self.parent:
            raise ValueError("scope intent belongs to another original parent")
        available, mems = self._verify()
        if not set(intent.cpus) <= set(available) or not set(intent.cpus) <= os.sched_getaffinity(0) or intent.mems != mems:
            _refuse("scope_original_cpu_or_memory_capacity_changed")
        # EEXIST is unresolved, not permission to use/recreate a previous name.
        os.mkdir(intent.name, mode=0o700, dir_fd=self._fd)
        descriptor = os.open(intent.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self._fd)
        try:
            info = _directory_info(descriptor)
            return CpuScope(CpuScopeBinding(intent, info.st_dev, info.st_ino), os.dup(self._fd), descriptor, "created")
        except BaseException:
            os.close(descriptor)
            raise  # Never delete an inconclusive scope automatically.

    def restore(self, binding):
        self._verify()
        if type(binding) is not CpuScopeBinding or binding.intent.parent != self.parent:
            raise ValueError("scope binding belongs to another original parent")
        descriptor = os.open(binding.intent.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self._fd)
        try:
            scope = CpuScope(binding, os.dup(self._fd), descriptor, "restored")
        except BaseException:
            os.close(descriptor)
            raise
        try:
            scope._identity()
            return scope
        except BaseException:
            scope.close()
            raise

    def close(self):
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None


class CpuScope:
    def __init__(self, binding, parent_fd, scope_fd, phase):
        self.binding, self._parent, self._fd, self._phase = binding, parent_fd, scope_fd, phase
        self._procs = None
        self._device_guard = None

    def _identity(self):
        if self._fd is None:
            raise RuntimeError("CPU scope handle closed")
        parent = self.binding.intent.parent
        if _context() != (parent.boot_id, parent.mount_namespace, parent.uid):
            _refuse("scope_original_kernel_context_changed")
        origin = _directory_info(self._parent)
        scope = _directory_info(self._fd)
        path = os.stat(parent.path, follow_symlinks=False)
        name = os.stat(self.binding.intent.name, dir_fd=self._parent, follow_symlinks=False)
        if ((origin.st_dev, origin.st_ino) != (parent.device, parent.inode)
                or (path.st_dev, path.st_ino) != (parent.device, parent.inode)
                or (scope.st_dev, scope.st_ino) != (self.binding.device, self.binding.inode)
                or (name.st_dev, name.st_ino) != (scope.st_dev, scope.st_ino)
                or os.readlink(f"/proc/self/fd/{self._parent}") != parent.path):
            _refuse("scope_original_inode_binding_changed")

    def observe(self):
        self._identity()
        if _read(self._fd, "cgroup.type") != "domain":
            _refuse("scope_original_domain_changed")
        fields = {}
        for line in _read(self._fd, "cgroup.events").splitlines():
            key, number = line.split()
            if key in fields or number not in ("0", "1"):
                _refuse("scope_population_unknown")
            fields[key] = int(number)
        if "populated" not in fields:
            _refuse("scope_population_unknown")
        procs = _read(self._fd, "cgroup.procs").splitlines()
        if len(procs) > 10000 or any(re.fullmatch(r"[1-9][0-9]{0,9}", pid) is None for pid in procs):
            _refuse("scope_population_unknown")
        cpu_text, mem_text = _read(self._fd, "cpuset.cpus.effective"), _read(self._fd, "cpuset.mems.effective")
        effective_cpus = _indices(cpu_text, maximum=65536) if cpu_text else ()
        effective_mems = _indices(mem_text, maximum=MAX_MEMS) if mem_text else ()
        requested, requested_mems = _read(self._fd, "cpuset.cpus"), _read(self._fd, "cpuset.mems")
        configured = (effective_cpus == self.binding.intent.cpus and effective_mems == self.binding.intent.mems
                      and bool(requested) and _indices(requested) == self.binding.intent.cpus
                      and bool(requested_mems) and _indices(requested_mems, maximum=MAX_MEMS) == self.binding.intent.mems)
        return {"scope_configured": configured, "populated": bool(fields["populated"]),
                "direct_process_count": len(procs), "effective_cpus": list(effective_cpus),
                "effective_mems": list(effective_mems), "admission_granted": False,
                "wait_authority_granted": False, "device_isolation": "not_configured"}

    def configure(self):
        if self._phase != "created":
            raise RuntimeError("only a new unconsumed scope may be configured")
        self._phase = "configuring"  # Failure is consumed and remains unresolved.
        observation = self.observe()
        if observation["populated"] or observation["direct_process_count"]:
            _refuse("scope_not_empty_before_configuration")
        _write(self._fd, "cpuset.mems", ",".join(map(str, self.binding.intent.mems)))
        _write(self._fd, "cpuset.cpus", ",".join(map(str, self.binding.intent.cpus)))
        observation = self.observe()
        if not observation["scope_configured"]:
            _refuse("scope_effective_cpu_or_memory_binding_differs")
        self._phase = "configured"
        return observation

    def constraints(self):
        if self._phase != "configured":
            raise RuntimeError("restored or consumed scope cannot grant another launch")
        self._phase = "launch_capability_issued"
        if self._device_guard is not None:
            self._device_guard()
        observation = self.observe()
        if not observation["scope_configured"] or observation["populated"] or observation["direct_process_count"]:
            _refuse("scope_changed_before_launch")
        self._procs = os.open("cgroup.procs", os.O_WRONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=self._fd)
        return LaunchConstraints(self.binding.intent.cpus, self._procs).validate()

    def remove(self):
        observation = self.observe()
        if observation["populated"] or observation["direct_process_count"]:
            _refuse("scope_not_empty_for_cleanup")
        self._identity()
        os.rmdir(self.binding.intent.name, dir_fd=self._parent)
        self._phase = "removed"
        self.close()
        return {"scope_removed": True, "wait_authority_granted": False}

    def close(self):
        for name in ("_procs", "_fd", "_parent"):
            descriptor = getattr(self, name)
            if descriptor is not None:
                os.close(descriptor)
                setattr(self, name, None)
