"""Versioned, project-independent execution primitives.

Native execution is explicitly installed and selected.  Neither backend imports
project code or interprets application protocols.  An observed child exit proves
that child exited; it does not prove its descendants or a scientific workflow
completed.
"""

from .backend import (
    INTERFACE_VERSION,
    BackendUnavailable,
    ExecutionEnvelope,
    ExecutionObservation,
    LinuxFdBackend,
    Owner,
    Prepared,
    SubprocessBackend,
    retained_owners,
)
from .persistent import PersistentLinuxFdBackend, PersistentOwner, OwnerUnavailable
from .constraints import LaunchConstraints, CONSTRAINTS_VERSION
from .scopes import CpuScopeIntent, CpuScopeBinding, DelegatedCpuScopes, ScopeUnavailable, SCOPE_VERSION
from .devices import DeviceRule, DevicePolicy, DeviceIntent, DeviceBinding, DeviceScope, DEVICE_VERSION

__all__ = [
    "INTERFACE_VERSION", "BackendUnavailable", "ExecutionEnvelope",
    "ExecutionObservation", "LinuxFdBackend", "Owner", "Prepared",
    "SubprocessBackend", "retained_owners",
    "PersistentLinuxFdBackend", "PersistentOwner", "OwnerUnavailable",
    "LaunchConstraints", "CONSTRAINTS_VERSION",
    "CpuScopeIntent", "CpuScopeBinding", "DelegatedCpuScopes", "ScopeUnavailable", "SCOPE_VERSION",
    "DeviceRule", "DevicePolicy", "DeviceIntent", "DeviceBinding", "DeviceScope", "DEVICE_VERSION",
]
