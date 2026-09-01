"""Production-derived Step 5D foundation alignment declaration.

The declaration is deliberately narrower than the frozen Step 5D protocol.
It describes only the retained launch-plan digest split and the existing
direct-parent endpoint-construction foundation. It opens no endpoint, maps no
descriptor, launches no process, and grants no scheduler or formal authority.

The main-repository validator independently reconstructs this projection from
the pinned production modules and traces endpoint construction with inert
objects. Consequently, copying these values without matching production
behavior cannot make the cross-repository gate pass.
"""

from __future__ import annotations

import hashlib
import inspect
import struct
from typing import Any

from . import native_launch
from . import native_step5d


FOUNDATION_ALIGNMENT_SCHEMA = (
    "m2b_step5d_scheduler_foundation_alignment/v1"
)
FOUNDATION_ALIGNMENT_SCOPE = (
    "digest_and_direct_parent_endpoint_foundation_only"
)
FOUNDATION_VECTOR_ID = "m2b_step5d_foundation_digest_vector/v1"
FOUNDATION_VECTOR_BODY = (
    b'{"schema":"m2b_step5d_foundation_digest_vector/v1"}'
)
PARENT_STEP5C_ALIGNMENT_SHA256 = (
    "0c069c5a11ef19dcbc5c0f3d4b6ed8b36225b7b2584b7fb14853c11f0505e5e8"
)

AUTHORITY_FIELDS = (
    "scheduler_role_authority_claimed",
    "external_anchor_authenticated",
    "formal_ready",
    "scientific_result",
    "logical_python_executed",
    "external_formal_authority_claimed",
)
UNIMPLEMENTED_STEP5D_SECTIONS = (
    "canonical_request_construction",
    "launch_nonce_owner_lifetime_replay_guard",
    "challenge_ack_receipt_failure_control_protocol",
    "isolated_native_runtime_and_child_wait",
)
FORBIDDEN_DIGEST_ALIASES = (
    "request_sha256",
    "body_sha256",
)
FORBIDDEN_PEER_EXPORTS = (
    "peer_endpoint",
    "peer_fd",
    "detach_peer_endpoint",
    "transfer_peer_endpoint",
)
CONSTRUCTION_TRACE = (
    "socketpair_created",
    "peer_endpoint_noninheritable",
    "native_endpoint_noninheritable",
    "native_endpoint_fd_passed_to_plan_factory",
    "native_endpoint_closed",
    "direct_parent_peer_endpoint_retained",
)


def _slots(value: type[object]) -> list[str]:
    declared = value.__slots__
    if isinstance(declared, str):
        return [declared]
    return list(declared)


def _parameter_names(value: object) -> list[str]:
    return list(inspect.signature(value).parameters)


def _digest_vector() -> tuple[str, str]:
    header = struct.Struct("!8sBBBBII")
    outer = struct.Struct("!I")
    payload = header.pack(
        native_launch.NATIVE_LAUNCH_WIRE_MAGIC,
        native_launch.NATIVE_LAUNCH_WIRE_VERSION,
        native_launch.NATIVE_LAUNCH_CHANNEL_REQUEST,
        native_launch.NATIVE_LAUNCH_MESSAGE_REQUEST,
        native_launch.NATIVE_LAUNCH_FLAGS_NONE,
        native_launch.NATIVE_LAUNCH_REQUEST_SEQUENCE,
        len(FOUNDATION_VECTOR_BODY),
    ) + FOUNDATION_VECTOR_BODY
    frame = outer.pack(len(payload)) + payload
    parsed = native_launch._validate_native_request_frame(frame)
    if parsed != FOUNDATION_VECTOR_BODY:
        raise RuntimeError("foundation digest vector body drifted")
    return (
        hashlib.sha256(frame).hexdigest(),
        hashlib.sha256(FOUNDATION_VECTOR_BODY).hexdigest(),
    )


def foundation_alignment_projection() -> dict[str, Any]:
    """Return a plain-JSON declaration of the incomplete foundation."""

    plan_type = native_launch.NativeLaunchPlan
    plan_slots = _slots(plan_type)
    owner_type = native_step5d.NativeStep5DNoDataLaunchOwner
    owner_slots = _slots(owner_type)
    plan_factory_parameters = _parameter_names(
        native_launch._create_native_launch_plan
    )
    owner_factory = native_step5d._create_step5d_no_data_launch_owner
    owner_factory_parameters = _parameter_names(owner_factory)
    frame_digest, body_digest = _digest_vector()

    authority = {
        field: getattr(owner_type, field)
        for field in AUTHORITY_FIELDS
    }
    authority["step5d_complete"] = False
    public_exports = list(native_step5d.__all__)

    return {
        "schema": FOUNDATION_ALIGNMENT_SCHEMA,
        "scope": FOUNDATION_ALIGNMENT_SCOPE,
        "parent_step5c": {
            "alignment_projection_sha256": (
                PARENT_STEP5C_ALIGNMENT_SHA256
            ),
            "same_scheduler_checkout_revalidated": True,
        },
        "inherited_transport": {
            "actual_argv": list(native_launch.NATIVE_ACTUAL_ARGV),
            "actual_environment": [
                list(item)
                for item in native_launch.NATIVE_ACTUAL_ENV_ITEMS
            ],
            "request_fd": native_launch.NATIVE_REQUEST_FD,
            "control_fd": native_launch.NATIVE_CONTROL_FD,
            "project_root_fd": native_launch.NATIVE_PROJECT_ROOT_FD,
            "wire_schema": native_launch.NATIVE_LAUNCH_WIRE_SCHEMA,
            "wire_magic_hex": (
                native_launch.NATIVE_LAUNCH_WIRE_MAGIC.hex()
            ),
            "wire_version": native_launch.NATIVE_LAUNCH_WIRE_VERSION,
            "outer_header_bytes": (
                native_launch.NATIVE_LAUNCH_WIRE_OUTER_HEADER_BYTES
            ),
            "wire_header_bytes": (
                native_launch.NATIVE_LAUNCH_WIRE_HEADER_BYTES
            ),
            "maximum_body_bytes": (
                native_launch.NATIVE_LAUNCH_WIRE_MAX_BODY
            ),
            "request_channel": (
                native_launch.NATIVE_LAUNCH_CHANNEL_REQUEST
            ),
            "request_message_type": (
                native_launch.NATIVE_LAUNCH_MESSAGE_REQUEST
            ),
            "flags": native_launch.NATIVE_LAUNCH_FLAGS_NONE,
            "request_sequence": (
                native_launch.NATIVE_LAUNCH_REQUEST_SEQUENCE
            ),
        },
        "native_launch_plan_digest_vector": {
            "production_type": "gsched.native_launch.NativeLaunchPlan",
            "plan_schema": native_launch.NATIVE_LAUNCH_PLAN_SCHEMA,
            "plan_slots": plan_slots,
            "factory_keyword_only_parameters": plan_factory_parameters,
            "digest_field_names": [
                "request_frame_sha256",
                "request_body_sha256",
            ],
            "forbidden_digest_aliases_present": sorted(
                name
                for name in FORBIDDEN_DIGEST_ALIASES
                if name in plan_slots or hasattr(plan_type, name)
            ),
            "vector_id": FOUNDATION_VECTOR_ID,
            "request_frame_sha256": frame_digest,
            "request_body_sha256": body_digest,
        },
        "direct_parent_endpoint_construction": {
            "production_owner_type": (
                "gsched.native_step5d.NativeStep5DNoDataLaunchOwner"
            ),
            "owner_slots": owner_slots,
            "factory_keyword_only_parameters": (
                owner_factory_parameters
            ),
            "factory_accepts_control_fd": (
                "control_fd" in owner_factory_parameters
            ),
            "factory_accepts_peer_endpoint": (
                "peer_endpoint" in owner_factory_parameters
            ),
            "socket_family": "AF_UNIX",
            "socket_type": "SOCK_STREAM",
            "construction_trace": list(CONSTRUCTION_TRACE),
            "parent_retained_endpoint_count": 1,
            "caller_peer_endpoint_handoff_count": 0,
            "public_exports": public_exports,
            "forbidden_peer_exports_present": sorted(
                name
                for name in FORBIDDEN_PEER_EXPORTS
                if name in public_exports or hasattr(owner_type, name)
            ),
        },
        "unimplemented_step5d_sections": list(
            UNIMPLEMENTED_STEP5D_SECTIONS
        ),
        "authority_boundary": authority,
    }


__all__ = [
    "FOUNDATION_ALIGNMENT_SCHEMA",
    "FOUNDATION_ALIGNMENT_SCOPE",
    "foundation_alignment_projection",
]
