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

__all__ = [
    "INTERFACE_VERSION", "BackendUnavailable", "ExecutionEnvelope",
    "ExecutionObservation", "LinuxFdBackend", "Owner", "Prepared",
    "SubprocessBackend", "retained_owners",
]
