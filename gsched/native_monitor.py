"""Retained inputs for the actual Step5G S/M startup and terminal lifecycle.

The reviewed caller owns these original descriptors throughout ``start``.
Native SMO duplicates them before clone3. This is not a formal launch plan:
deployment/runtime admission and V/P dispatch remain separate prerequisites.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NativeMonitorLaunch:
    monitor_fd: int
    monitor_path: str
    monitor_bytes: int
    monitor_sha256: bytes
    request_fd: int
    request_bytes: int
    request_sha256: bytes
    root_fd: int
    root_path: str
    input_fd: int
    output_fd: int
    error_fd: int
    deadline_ns: int
    evidence_relative_path: str

    def native_arguments(self) -> tuple:
        """Exact bridge order; native code validates every value before birth."""
        return (
            self.monitor_fd, self.monitor_path, self.monitor_bytes, self.monitor_sha256,
            self.request_fd, self.request_bytes, self.request_sha256,
            self.root_fd, self.root_path, self.input_fd, self.output_fd, self.error_fd,
            self.deadline_ns, self.evidence_relative_path,
        )
