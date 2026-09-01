"""Isolated Step 5D direct-parent control-endpoint construction.

This module is deliberately narrower than a daemon launch backend.  It creates
one AF_UNIX stream socketpair, gives only the native-side endpoint to the
retained :class:`~gsched.native_launch.NativeLaunchPlan` factory, closes that
source endpoint, and retains the peer endpoint privately in the creating
process.  It provides no public peer FD or endpoint-transfer API.

The resulting owner is generated/no-data engineering state only.  It does not
authenticate a scheduler role, implement the Step 5D control protocol, launch
a process, execute ``logical_submitted_argv``, publish a result, or make V2
submissions runnable.  Real daemon integration remains a separately reviewed
change.
"""

from __future__ import annotations

import os
import socket
import sys
from typing import Any

from .native_launch import (
    NativeLaunchPlan,
    NativeLaunchPlanError,
    NativeLaunchUnavailable,
    _create_native_launch_plan,
)


_OWNER_AUTHORITY = object()


class NativeStep5DNoDataLaunchOwner:
    """Own one retained plan and its unexported scheduler-side peer endpoint."""

    __slots__ = ("_closed", "_owner_pid", "_peer_endpoint", "_plan")

    scheduler_role_authority_claimed = False
    external_anchor_authenticated = False
    formal_ready = False
    scientific_result = False
    logical_python_executed = False
    external_formal_authority_claimed = False

    def __init__(
        self,
        authority: object,
        *,
        owner_pid: int,
        plan: NativeLaunchPlan,
        peer_endpoint: socket.socket,
    ) -> None:
        if authority is not _OWNER_AUTHORITY:
            raise NativeLaunchPlanError(
                "Step 5D no-data launch owners require scheduler-internal authority"
            )
        self._owner_pid = owner_pid
        self._plan = plan
        self._peer_endpoint = peer_endpoint
        self._closed = False

    @property
    def owner_pid(self) -> int:
        return self._owner_pid

    @property
    def closed(self) -> bool:
        return self._closed

    def _require_current_owner(self) -> None:
        if self._closed:
            raise NativeLaunchPlanError(
                "Step 5D no-data launch owner is already closed"
            )
        if os.getpid() != self._owner_pid:
            raise NativeLaunchPlanError(
                "Step 5D peer endpoint may be used only by its creating process"
            )

    @property
    def plan(self) -> NativeLaunchPlan:
        """Return the retained native-side plan without exposing the peer."""

        self._require_current_owner()
        return self._plan

    @property
    def peer_endpoint_retained(self) -> bool:
        """Report peer retention without disclosing or transferring its FD."""

        self._require_current_owner()
        return self._peer_endpoint.fileno() >= 0

    def validate_live_plan(self) -> None:
        """Revalidate the plan while preserving same-process peer ownership."""

        self.plan.validate_live_fds()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._plan.close()
        finally:
            self._peer_endpoint.close()

    def __enter__(self) -> NativeStep5DNoDataLaunchOwner:
        self._require_current_owner()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


def _create_step5d_no_data_launch_owner(
    *,
    profile_id: Any,
    profile_sha256: Any,
    project_root_identity_sha256: Any,
    project_root_path: Any,
    logical_submitted_argv: Any,
    launcher_sha256: Any,
    request_frame_sha256: Any,
    request_body_sha256: Any,
    log_relative_path: Any,
    launcher_fd: Any,
    request_fd: Any,
    project_root_fd: Any,
    log_fd: Any,
) -> NativeStep5DNoDataLaunchOwner:
    """Create one isolated owner without accepting a caller-supplied peer.

    The peer endpoint never leaves this function except as private state of the
    returned owner.  Only the opposite endpoint's numeric FD is supplied to the
    retained-plan factory, and that source socket is closed immediately after
    the factory has made and validated its owned native-side copy.
    """

    if not sys.platform.startswith("linux"):
        raise NativeLaunchUnavailable(
            "Step 5D no-data owner construction requires Linux; no fallback is allowed"
        )

    owner_pid = os.getpid()
    peer_endpoint: socket.socket | None = None
    native_endpoint: socket.socket | None = None
    plan: NativeLaunchPlan | None = None
    try:
        peer_endpoint, native_endpoint = socket.socketpair(
            socket.AF_UNIX,
            socket.SOCK_STREAM,
        )
        peer_endpoint.set_inheritable(False)
        native_endpoint.set_inheritable(False)
        plan = _create_native_launch_plan(
            profile_id=profile_id,
            profile_sha256=profile_sha256,
            project_root_identity_sha256=project_root_identity_sha256,
            project_root_path=project_root_path,
            logical_submitted_argv=logical_submitted_argv,
            launcher_sha256=launcher_sha256,
            request_frame_sha256=request_frame_sha256,
            request_body_sha256=request_body_sha256,
            log_relative_path=log_relative_path,
            launcher_fd=launcher_fd,
            request_fd=request_fd,
            control_fd=native_endpoint.fileno(),
            project_root_fd=project_root_fd,
            log_fd=log_fd,
        )
        native_endpoint.close()
        native_endpoint = None
        owner = NativeStep5DNoDataLaunchOwner(
            _OWNER_AUTHORITY,
            owner_pid=owner_pid,
            plan=plan,
            peer_endpoint=peer_endpoint,
        )
        plan = None
        peer_endpoint = None
        return owner
    except BaseException:
        if plan is not None:
            plan.close()
        raise
    finally:
        if native_endpoint is not None:
            try:
                native_endpoint.close()
            except OSError:
                pass
        if peer_endpoint is not None:
            try:
                peer_endpoint.close()
            except OSError:
                pass


__all__ = [
    "NativeStep5DNoDataLaunchOwner",
]
