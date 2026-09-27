# Ported from MPC_OTSF main@fa1cd85 under the frozen Step 5D contract.
# Kept self-contained: this module does not import MPC_OTSF or test fixtures.
"""Pure validators for the scheduler-to-native Step 5C launch-control wire.

This is deliberately a small, non-authoritative interface fixture.  The
scheduler will eventually exec the reviewed native artifact with one exact
argv, an empty environment, and three fixed inherited descriptors.  This
module performs no I/O and cannot attest that those descriptors exist, are
sealed, refer to the project root, or have an authenticated peer.

The framed body remains opaque bytes.  In particular, this wire is distinct
from :mod:`experiments.m2b_monitor_channel`, does not parse its JSON messages,
and does not authorize a Python process or a scientific task.
"""
from __future__ import annotations

import struct
from collections.abc import Mapping, Sequence
from typing import Final, NamedTuple


NATIVE_ACTUAL_ARGV: Final = (
    "m2b-exec-monitor[native-entry-v1]",
    "--native-entry-v1",
)

NATIVE_INHERITED_REQUEST_FD: Final = 3
NATIVE_INHERITED_CONTROL_FD: Final = 4
NATIVE_INHERITED_PROJECT_ROOT_FD: Final = 5
NATIVE_EXEC_TARGET_CLOEXEC_REQUIRED: Final = True

NATIVE_REQUEST_REQUIRED_LINUX_SEALS: Final = (
    "F_SEAL_WRITE",
    "F_SEAL_GROW",
    "F_SEAL_SHRINK",
    "F_SEAL_SEAL",
)

NATIVE_LAUNCH_WIRE_SCHEMA: Final = (
    "m2b_scheduler_native_launch_control_frame/v1"
)
NATIVE_LAUNCH_WIRE_MAGIC: Final = b"M2BNLC01"
NATIVE_LAUNCH_WIRE_VERSION: Final = 1
NATIVE_LAUNCH_WIRE_OUTER_HEADER_BYTES: Final = 4
NATIVE_LAUNCH_WIRE_HEADER_BYTES: Final = 20
NATIVE_LAUNCH_WIRE_MAX_BODY: Final = 1024 * 1024

NATIVE_LAUNCH_CHANNEL_REQUEST: Final = 1
NATIVE_LAUNCH_CHANNEL_CONTROL: Final = 2
NATIVE_LAUNCH_MESSAGE_REQUEST: Final = 1
NATIVE_LAUNCH_MESSAGE_CONTROL: Final = 2
NATIVE_LAUNCH_FLAGS_NONE: Final = 0
NATIVE_LAUNCH_REQUEST_SEQUENCE: Final = 0

_OUTER_HEADER = struct.Struct("!I")
_WIRE_HEADER = struct.Struct("!8sBBBBII")


class NativeLaunchProtocolViolation(ValueError):
    """The detached invocation or launch-control frame is not exact."""


class NativeLaunchFrame(NamedTuple):
    """A detached frame whose body has deliberately not been interpreted."""

    schema: str
    channel: int
    message_type: int
    sequence: int
    opaque_body: bytes
    external_authority_claimed: bool
    formal_ready: bool


def validate_native_entry_invocation(
    argv: Sequence[object],
    environment: Mapping[object, object],
) -> tuple[str, str]:
    """Validate the exact actual native argv and the exact empty environment."""

    if (
        type(argv) not in (list, tuple)
        or any(type(value) is not str for value in argv)
        or tuple(argv) != NATIVE_ACTUAL_ARGV
    ):
        raise NativeLaunchProtocolViolation("actual_native_argv_mismatch")
    if type(environment) is not dict:
        raise NativeLaunchProtocolViolation(
            "actual_native_environment_not_plain_dict"
        )
    if environment:
        raise NativeLaunchProtocolViolation("actual_native_environment_not_empty")
    return NATIVE_ACTUAL_ARGV


def _validate_frame_fields(
    *,
    channel: object,
    message_type: object,
    sequence: object,
    opaque_body: object,
) -> tuple[int, int, int, bytes]:
    if type(channel) is not int:
        raise NativeLaunchProtocolViolation("launch_frame_channel_invalid")
    if type(message_type) is not int:
        raise NativeLaunchProtocolViolation("launch_frame_message_type_invalid")
    if type(sequence) is not int or not 0 <= sequence <= 0xFFFFFFFF:
        raise NativeLaunchProtocolViolation("launch_frame_sequence_invalid")
    if type(opaque_body) is not bytes:
        raise NativeLaunchProtocolViolation("launch_frame_body_not_bytes")
    if not opaque_body or len(opaque_body) > NATIVE_LAUNCH_WIRE_MAX_BODY:
        raise NativeLaunchProtocolViolation("launch_frame_body_size_invalid")

    if channel == NATIVE_LAUNCH_CHANNEL_REQUEST:
        if message_type != NATIVE_LAUNCH_MESSAGE_REQUEST:
            raise NativeLaunchProtocolViolation("launch_frame_request_type_mismatch")
        if sequence != NATIVE_LAUNCH_REQUEST_SEQUENCE:
            raise NativeLaunchProtocolViolation("launch_frame_request_sequence_mismatch")
    elif channel == NATIVE_LAUNCH_CHANNEL_CONTROL:
        if message_type != NATIVE_LAUNCH_MESSAGE_CONTROL:
            raise NativeLaunchProtocolViolation("launch_frame_control_type_mismatch")
    else:
        raise NativeLaunchProtocolViolation("launch_frame_channel_invalid")
    return channel, message_type, sequence, opaque_body


def encode_native_launch_frame(
    *,
    channel: int,
    message_type: int,
    sequence: int,
    opaque_body: bytes,
) -> bytes:
    """Encode one exact bounded frame without assigning meaning to its body."""

    channel, message_type, sequence, body = _validate_frame_fields(
        channel=channel,
        message_type=message_type,
        sequence=sequence,
        opaque_body=opaque_body,
    )
    payload = _WIRE_HEADER.pack(
        NATIVE_LAUNCH_WIRE_MAGIC,
        NATIVE_LAUNCH_WIRE_VERSION,
        channel,
        message_type,
        NATIVE_LAUNCH_FLAGS_NONE,
        sequence,
        len(body),
    ) + body
    return _OUTER_HEADER.pack(len(payload)) + payload


def decode_native_launch_frame(frame: bytes) -> NativeLaunchFrame:
    """Decode exactly one frame; truncation and trailing bytes fail closed."""

    if type(frame) is not bytes:
        raise NativeLaunchProtocolViolation("launch_frame_not_bytes")
    if len(frame) < _OUTER_HEADER.size + _WIRE_HEADER.size:
        raise NativeLaunchProtocolViolation("launch_frame_truncated")
    (payload_length,) = _OUTER_HEADER.unpack_from(frame)
    if payload_length < _WIRE_HEADER.size:
        raise NativeLaunchProtocolViolation("launch_frame_payload_too_short")
    if payload_length > _WIRE_HEADER.size + NATIVE_LAUNCH_WIRE_MAX_BODY:
        raise NativeLaunchProtocolViolation("launch_frame_payload_too_large")
    if len(frame) != _OUTER_HEADER.size + payload_length:
        raise NativeLaunchProtocolViolation("launch_frame_length_mismatch")

    payload = frame[_OUTER_HEADER.size :]
    magic, version, channel, message_type, flags, sequence, body_length = (
        _WIRE_HEADER.unpack_from(payload)
    )
    if magic != NATIVE_LAUNCH_WIRE_MAGIC:
        raise NativeLaunchProtocolViolation("launch_frame_magic_mismatch")
    if version != NATIVE_LAUNCH_WIRE_VERSION:
        raise NativeLaunchProtocolViolation("launch_frame_version_mismatch")
    if flags != NATIVE_LAUNCH_FLAGS_NONE:
        raise NativeLaunchProtocolViolation("launch_frame_flags_not_zero")
    if body_length != payload_length - _WIRE_HEADER.size:
        raise NativeLaunchProtocolViolation("launch_frame_body_length_mismatch")
    body = payload[_WIRE_HEADER.size :]
    channel, message_type, sequence, body = _validate_frame_fields(
        channel=channel,
        message_type=message_type,
        sequence=sequence,
        opaque_body=body,
    )
    return NativeLaunchFrame(
        schema=NATIVE_LAUNCH_WIRE_SCHEMA,
        channel=channel,
        message_type=message_type,
        sequence=sequence,
        opaque_body=bytes(body),
        external_authority_claimed=False,
        formal_ready=False,
    )


__all__ = [
    "NATIVE_ACTUAL_ARGV",
    "NATIVE_EXEC_TARGET_CLOEXEC_REQUIRED",
    "NATIVE_INHERITED_CONTROL_FD",
    "NATIVE_INHERITED_PROJECT_ROOT_FD",
    "NATIVE_INHERITED_REQUEST_FD",
    "NATIVE_LAUNCH_CHANNEL_CONTROL",
    "NATIVE_LAUNCH_CHANNEL_REQUEST",
    "NATIVE_LAUNCH_FLAGS_NONE",
    "NATIVE_LAUNCH_MESSAGE_CONTROL",
    "NATIVE_LAUNCH_MESSAGE_REQUEST",
    "NATIVE_LAUNCH_REQUEST_SEQUENCE",
    "NATIVE_LAUNCH_WIRE_HEADER_BYTES",
    "NATIVE_LAUNCH_WIRE_MAGIC",
    "NATIVE_LAUNCH_WIRE_MAX_BODY",
    "NATIVE_LAUNCH_WIRE_OUTER_HEADER_BYTES",
    "NATIVE_LAUNCH_WIRE_SCHEMA",
    "NATIVE_LAUNCH_WIRE_VERSION",
    "NATIVE_REQUEST_REQUIRED_LINUX_SEALS",
    "NativeLaunchFrame",
    "NativeLaunchProtocolViolation",
    "decode_native_launch_frame",
    "encode_native_launch_frame",
    "validate_native_entry_invocation",
]
