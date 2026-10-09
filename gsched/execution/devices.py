"""Bounded exact device policy for an original, explicitly delegated CPU scope.

This is an opt-in execution primitive, not scheduler GPU inventory or a sandbox.
Callers persist intent before installation and binding before granting launch.
Recovery only observes; unknown installation never retries or detaches a policy.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import os
import re
import struct
import sys

from .backend import BackendUnavailable
from .scopes import CpuScope, CpuScopeBinding

DEVICE_VERSION = "sched-device-policy/v1"
MAX_RULES = 256


@dataclass(frozen=True, order=True)
class DeviceRule:
    kind: str
    major: int
    minor: int
    access: int

    def __post_init__(self):
        if (type(self.kind) is not str or self.kind not in ("block", "char")
                or any(type(v) is not int or not 0 <= v <= 0xffffffff for v in (self.major, self.minor))
                or type(self.access) is not int or not 1 <= self.access <= 7):
            raise ValueError("invalid exact device rule")

    def to_dict(self):
        return dict(kind=self.kind, major=self.major, minor=self.minor, access=self.access)


@dataclass(frozen=True)
class DevicePolicy:
    rules: tuple[DeviceRule, ...]

    def __post_init__(self):
        if (type(self.rules) is not tuple or len(self.rules) > MAX_RULES
                or any(type(rule) is not DeviceRule for rule in self.rules)
                or tuple(sorted(self.rules)) != self.rules
                or len({(r.kind, r.major, r.minor) for r in self.rules}) != len(self.rules)):
            raise ValueError("device rules must be bounded, unique and ordered")

    def to_dict(self):
        return dict(interface_version=DEVICE_VERSION, default="deny", rules=[r.to_dict() for r in self.rules])

    @classmethod
    def from_dict(cls, value):
        if (type(value) is not dict or set(value) != {"interface_version", "default", "rules"}
                or value["interface_version"] != DEVICE_VERSION or value["default"] != "deny"
                or type(value["rules"]) is not list or len(value["rules"]) > MAX_RULES
                or any(type(r) is not dict or set(r) != {"kind", "major", "minor", "access"} for r in value["rules"])):
            raise ValueError("invalid serialized device policy")
        return cls(tuple(DeviceRule(**r) for r in value["rules"]))

    def compile(self, *, byteorder=None):
        byteorder = sys.byteorder if byteorder is None else byteorder
        if type(byteorder) is not str or byteorder not in ("little", "big"):
            raise ValueError("invalid device bytecode endianness")
        # No maps, helpers, paths or wildcard nodes. JMP32 compares unsigned
        # device numbers correctly even when bit 31 is set in an immediate.
        instructions = []
        def emit(code, dst=0, src=0, offset=0, immediate=0):
            registers = dst | (src << 4) if byteorder == "little" else (dst << 4) | src
            instructions.append([code, registers, offset, immediate])
        emit(0x61, 2, 1, 0)  # access_type
        emit(0xbf, 3, 2)
        emit(0x77, 3, immediate=16)
        emit(0x57, 2, immediate=0xffff)
        emit(0x61, 4, 1, 4)  # major
        emit(0x61, 5, 1, 8)  # minor
        emit(0x16, 3)        # no requested access is invalid
        emit(0xbf, 6, 3)
        emit(0x57, 6, immediate=~7)
        emit(0x56, 6)        # unknown access bits are invalid
        for rule in self.rules:
            start = len(instructions)
            emit(0x56, 2, immediate=1 if rule.kind == "block" else 2)
            emit(0x56, 4, immediate=rule.major)
            emit(0x56, 5, immediate=rule.minor)
            emit(0xbf, 6, 3)
            emit(0x57, 6, immediate=7 ^ rule.access)
            emit(0x56, 6)
            emit(0xb7, 0, immediate=1)
            emit(0x95)
            for index in (start, start + 1, start + 2, start + 5):
                instructions[index][2] = start + 8 - index - 1
        deny = len(instructions)
        emit(0xb7)
        emit(0x95)
        for index in (6, 9):
            instructions[index][2] = deny - index - 1
        encoding = "<BBhi" if byteorder == "little" else ">BBhi"
        return b"".join(struct.pack(encoding, code, regs, offset,
                immediate if immediate < 0x80000000 else immediate - 0x100000000)
                for code, regs, offset, immediate in instructions)


@dataclass(frozen=True)
class DeviceIntent:
    scope: CpuScopeBinding
    policy: DevicePolicy
    byteorder: str = field(default_factory=lambda: sys.byteorder)

    def __post_init__(self):
        if (type(self.scope) is not CpuScopeBinding or type(self.policy) is not DevicePolicy
                or type(self.byteorder) is not str or self.byteorder not in ("little", "big")):
            raise ValueError("device intent requires original scope and exact policy")

    @property
    def program_sha256(self):
        return hashlib.sha256(self.policy.compile(byteorder=self.byteorder)).hexdigest()

    def to_dict(self):
        return dict(scope=self.scope.to_dict(), policy=self.policy.to_dict(), program_sha256=self.program_sha256)

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict or set(value) != {"scope", "policy", "program_sha256"}:
            raise ValueError("invalid serialized device intent")
        scope = CpuScopeBinding.from_dict(value["scope"])
        policy = DevicePolicy.from_dict(value["policy"])
        # The v1 digest already binds the encoding. Verify both bounded formats
        # without reinterpreting the persisted bytes using the query host's ABI.
        for byteorder in ("little", "big"):
            result = cls(scope, policy, byteorder)
            if value["program_sha256"] == result.program_sha256:
                return result
        raise ValueError("device intent bytecode digest differs")


@dataclass(frozen=True)
class DeviceBinding:
    intent: DeviceIntent
    program_id: int
    program_tag: str

    def __post_init__(self):
        if (type(self.intent) is not DeviceIntent or type(self.program_id) is not int
                or not 0 < self.program_id <= 0xffffffff or type(self.program_tag) is not str
                or re.fullmatch(r"[0-9a-f]{16}", self.program_tag) is None):
            raise ValueError("invalid original device program binding")

    def to_dict(self):
        return dict(intent=self.intent.to_dict(), program_id=self.program_id, program_tag=self.program_tag)

    @classmethod
    def from_dict(cls, value):
        if type(value) is not dict or set(value) != {"intent", "program_id", "program_tag"}:
            raise ValueError("invalid serialized device binding")
        return cls(DeviceIntent.from_dict(value["intent"]), value["program_id"], value["program_tag"])


def _native():
    try:
        from . import _fdexec
    except ImportError as exc:
        raise BackendUnavailable("device_native_unavailable", reason="device_native_unavailable") from exc
    if getattr(_fdexec, "device_policy_interface_version", None) != DEVICE_VERSION:
        raise BackendUnavailable("device_native_interface_mismatch", reason="device_native_interface_mismatch")
    return _fdexec


def _refuse(reason):
    raise BackendUnavailable(reason, reason=reason)


class DeviceScope:
    """No implicit install, replace, detach, scope creation or launch on restore."""
    def __init__(self, scope, intent, *, binding=None):
        if (type(scope) is not CpuScope or type(intent) is not DeviceIntent
                or scope.binding != intent.scope
                or (binding is None and scope._phase != "configured")
                or (binding is not None and (type(binding) is not DeviceBinding
                    or binding.intent != intent or scope._phase != "restored"))
                or scope._device_guard is not None):
            raise ValueError("device policy requires an unconsumed original scope")
        self.scope, self.intent, self.binding = scope, intent, binding
        self._phase = "restored" if binding is not None else "new"
        # Immediately prevent plain CPU launch, including before install/failure.
        scope._device_guard = self.observe

    def _empty(self):
        observation = self.scope.observe()
        if not observation["scope_configured"] or observation["populated"] or observation["direct_process_count"]:
            _refuse("device_scope_not_configured_empty")

    def install(self):
        if self._phase != "new" or self.scope._phase != "configured":
            raise RuntimeError("device installation already consumed or restored")
        self._phase = "installing"
        self.scope._phase = "device_installing"
        if self.intent.byteorder != sys.byteorder:
            _refuse("device_intent_foreign_byteorder")
        native = _native()
        self._empty()
        if native.device_program_query(self.scope._fd)["program_ids"]:
            _refuse("device_scope_already_has_policy")
        program = native.device_program_load(self.intent.policy.compile(byteorder=self.intent.byteorder))
        try:
            identity = native.device_program_info(program)
            binding = DeviceBinding(self.intent, identity["program_id"], identity["program_tag"])
            self._empty()
            if native.device_program_query(self.scope._fd)["program_ids"]:
                _refuse("device_scope_policy_changed_before_attach")
            native.device_program_attach(self.scope._fd, program)
            self.binding = binding
            self.observe()
        finally:
            os.close(program)  # Direct attachment retains its reference; no detach.
        self._phase, self.scope._phase = "installed", "configured"
        return binding

    def observe(self):
        if self.binding is None:
            _refuse("device_installation_not_confirmed")
        self.scope._identity()
        native = _native()
        observed = native.device_program_query(self.scope._fd)
        if observed != {"program_ids": [self.binding.program_id], "attach_flags": 2}:
            _refuse("device_original_attachment_changed")
        program = native.device_program_fd(self.binding.program_id)
        try:
            if native.device_program_info(program) != {"program_id": self.binding.program_id,
                    "program_tag": self.binding.program_tag}:
                _refuse("device_original_program_changed")
        finally:
            os.close(program)
        return {"device_attachment_verified": True, "program_id": self.binding.program_id,
                "admission_granted": False, "wait_authority_granted": False}

    def constraints(self):
        if self._phase != "installed":
            raise RuntimeError("restored or consumed device scope cannot grant launch")
        self._phase = "launch_capability_issued"
        return self.scope.constraints()  # Also invokes guard for plain CPU callers.
