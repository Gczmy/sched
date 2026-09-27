# Ported from MPC_OTSF main@fa1cd85 under the frozen Step 5D contract.
# Kept self-contained: this module does not import MPC_OTSF or test fixtures.
"""Pure Step 5D peer-bound no-data request/control protocol.

This module is a scheduler/fixture-side construction and validation aid.  It
performs no process creation, descriptor inspection, data access, logical
Python execution, or authority attestation.  The native entry remains the
component that must independently enforce the frozen C-side contract.
"""
from __future__ import annotations

import hashlib
import json
import stat
from collections.abc import Mapping
from typing import Any, Final, NamedTuple

from .native_deployment import NativeDeployment, require_deployment

from .native_step5d_wire import (
    NATIVE_ACTUAL_ARGV,
    NATIVE_EXEC_TARGET_CLOEXEC_REQUIRED,
    NATIVE_INHERITED_CONTROL_FD,
    NATIVE_INHERITED_PROJECT_ROOT_FD,
    NATIVE_INHERITED_REQUEST_FD,
    NATIVE_LAUNCH_CHANNEL_CONTROL,
    NATIVE_LAUNCH_CHANNEL_REQUEST,
    NATIVE_LAUNCH_FLAGS_NONE,
    NATIVE_LAUNCH_MESSAGE_CONTROL,
    NATIVE_LAUNCH_MESSAGE_REQUEST,
    NATIVE_LAUNCH_REQUEST_SEQUENCE,
    NATIVE_LAUNCH_WIRE_HEADER_BYTES,
    NATIVE_LAUNCH_WIRE_MAGIC,
    NATIVE_LAUNCH_WIRE_MAX_BODY,
    NATIVE_LAUNCH_WIRE_OUTER_HEADER_BYTES,
    NATIVE_LAUNCH_WIRE_SCHEMA,
    NATIVE_LAUNCH_WIRE_VERSION,
    NATIVE_REQUEST_REQUIRED_LINUX_SEALS,
    decode_native_launch_frame,
    encode_native_launch_frame,
)


PARENT_PROTOCOL_SHA256: Final = (
    "622452da8689f964064e68fe4570ba5af3e5c002b182067af4255fa460cc8bfa"
)
REQUEST_SCHEMA: Final = "m2b_native_peer_bound_stop_request/v1"
REQUEST_KIND: Final = "peer_bound_no_data_stop"
SCIENTIFIC_INPUT_POLICY: Final = (
    "forbid_dataset_cache_checkpoint_raw_result_and_logical_python"
)
PEER_ROLE: Final = "launch_owner_control_parent"

CHALLENGE_SCHEMA: Final = "m2b_native_launch_challenge/v1"
ACK_SCHEMA: Final = "m2b_launch_owner_native_stop_ack/v1"
ACK_DECISION: Final = "acknowledge_peer_bound_no_data_stop_only"
RECEIPT_SCHEMA: Final = "m2b_native_launch_peer_bound_stop/v1"
RECEIPT_RESULT: Final = "peer_bound_no_data_stop"
FAILURE_SCHEMA: Final = "m2b_native_launch_failure/v1"

CONTROL_CHALLENGE_SEQUENCE: Final = 0
CONTROL_ACK_SEQUENCE: Final = 1
CONTROL_RECEIPT_SEQUENCE: Final = 2
CONTROL_FAILURE_SEQUENCE: Final = 0xFFFFFFFF
CONTROL_DEADLINE_MS: Final = 10_000

REQUEST_VALIDATION_PRECEDENCE: Final = (
    "1: invalid UTF-8/JSON, duplicate key or noncanonical bytes -> "
    "request_body_noncanonical/65",
    "2: wrong exact or nested keyset, wrong JSON primitive/container type, "
    "boolean-as-integer, numeric range, fixed schema/peer-role/root-project "
    "string, or request-internal relation such as credentials.pid != "
    "process_identity.pid -> request_schema_mismatch/65; "
    "active_phase_authorization_sha256 structurally permits only null or "
    "string; semantic fields assigned to later steps are excluded",
    "3: a structurally valid string protocol_sha256 unequal to the parent "
    "digest -> request_protocol_mismatch/78",
    "4: a structurally valid string target phase outside the enum, or an "
    "exact logical argv/profile inconsistency -> "
    "request_target_phase_mismatch/78",
    "5: a structurally valid scheduler prefix with wrong project, run-to-RC "
    "relation, batch or task value -> scheduler_identity_prefix_mismatch/78; "
    "run ID and launch marker otherwise have shape-only validation",
    "6: a structurally valid but wrong request_kind/scientific policy string, "
    "or any string rather than null active authorization -> "
    "request_no_data_policy_mismatch/78",
    "7: a structurally valid argv digest unequal to canonical argv, or "
    "body/frame digest-name swap in control -> request_digest_mismatch/65",
)

ACK_FAILURE_PRECEDENCE: Final = (
    "0: if syscall failure, timeout or premature EOF prevents a complete "
    "bounded ACK stream plus write-half EOF -> control_channel_io_failure/74",
    "1: within a completely received stream, MSG_CTRUNC, malformed or unknown "
    "cmsg, or the required first candidate's frame/canonical/key/type/schema "
    "error -> control_message_invalid/65",
    "2: first frame sequence is not exactly 1 -> "
    "control_sequence_mismatch/65",
    "3: after one complete valid first candidate, any remaining byte -> "
    "control_replay_detected/65 whether or not the remainder can be parsed as "
    "a second or duplicate frame",
    "4: request frame/body digest differs or the two digest meanings are "
    "swapped -> request_digest_mismatch/65",
    "5: launch nonce or monitor nonce differs -> control_nonce_mismatch/65",
    "6: target phase or decision differs -> control_message_invalid/65",
    "7: no SCM_CREDENTIALS was observed, any credential is zero or unequal to "
    "SO_PEERCRED/getppid/request, or multiple observations drift -> "
    "peer_credentials_mismatch/77",
)

PHASE_PROFILES: Final = (
    "preparation",
    "raw_collection",
    "aggregation",
)
REQUEST_KEYS: Final = frozenset(
    {
        "schema",
        "request_kind",
        "target_phase_profile",
        "protocol_sha256",
        "active_phase_authorization_sha256",
        "scheduler_identity_prefix",
        "launch_nonce",
        "control_peer_expectation",
        "project_root_expectation",
        "submitted_logical_argv",
        "submitted_logical_argv_sha256",
        "scientific_input_policy",
    }
)
SCHEDULER_PREFIX_KEYS: Final = frozenset(
    {
        "SCHED_BATCH_ID",
        "SCHED_TASK_ID",
        "SCHED_RUN_ID",
        "SCHED_PROJECT",
        "SCHED_RC_PREFIX",
        "SCHED_LAUNCH_MARKER",
    }
)
CONTROL_PEER_KEYS: Final = frozenset(
    {"peer_role", "credentials", "process_identity"}
)
CREDENTIAL_KEYS: Final = frozenset({"pid", "uid", "gid"})
PROCESS_IDENTITY_KEYS: Final = frozenset(
    {
        "boot_id_sha256",
        "pid",
        "start_ticks",
        "parent_pid",
        "process_group_id",
        "cgroup_identity_sha256",
    }
)
PROJECT_ROOT_KEYS: Final = frozenset(
    {
        "project",
        "canonical_absolute_path",
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_gid",
    }
)

CHALLENGE_KEYS: Final = frozenset(
    {
        "schema",
        "request_frame_sha256",
        "request_body_sha256",
        "launch_nonce",
        "monitor_nonce",
        "target_phase_profile",
    }
)
ACK_KEYS: Final = CHALLENGE_KEYS | {"decision"}
RECEIPT_KEYS: Final = frozenset(
    {
        "schema",
        "request_frame_sha256",
        "request_body_sha256",
        "launch_nonce",
        "monitor_nonce",
        "target_phase_profile",
        "result",
        "kernel_peer_identity_bound",
        "direct_parent_live_at_final_observation",
        "peer_process_image_endpoint_observations_equal",
        "project_root_endpoint_observations_equal",
        "unexpected_inherited_fd_count",
        "scheduler_role_authority_claimed",
        "external_anchor_authenticated",
        "formal_ready",
        "scientific_result",
        "logical_python_executed",
        "external_formal_authority_claimed",
    }
)
FAILURE_KEYS: Final = frozenset(
    {
        "schema",
        "failed_state",
        "reason_id",
        "exit_code",
        "request_frame_sha256",
        "request_body_sha256",
        "scheduler_role_authority_claimed",
        "external_anchor_authenticated",
        "formal_ready",
        "scientific_result",
        "logical_python_executed",
        "external_formal_authority_claimed",
    }
)

STATE_MACHINE: Final = (
    "ENTRY_EXACT",
    "PLATFORM_CAPABILITY",
    "FD_SHAPE",
    "REQUEST_FRAME_EXACT_AND_EOF",
    "BODY_CANONICAL",
    "REQUEST_NO_DATA_POLICY",
    "PEER_PIDFD_PINNED",
    "PROJECT_ROOT_BOUND",
    "CHALLENGE_SENT_SEQUENCE_0",
    "STOP_ACK_VERIFIED_SEQUENCE_1",
    "PEER_AND_ROOT_REVALIDATED",
    "PEER_BOUND_STOP_RECEIPT_SEQUENCE_2",
    "EX_UNAVAILABLE",
)
NATIVE_FAILURE_REASONS: Final = (
    "actual_native_argv_mismatch",
    "actual_native_environment_not_empty",
    "fixed_inherited_descriptors_invalid",
    "descriptor_inventory_io_failure",
    "fixed_descriptor_io_failure",
    "request_frame_invalid",
    "request_body_noncanonical",
    "request_schema_mismatch",
    "request_digest_mismatch",
    "request_protocol_mismatch",
    "request_target_phase_mismatch",
    "scheduler_identity_prefix_mismatch",
    "request_no_data_policy_mismatch",
    "peer_credentials_mismatch",
    "peer_process_identity_mismatch",
    "peer_process_image_mismatch",
    "project_root_identity_mismatch",
    "control_message_invalid",
    "control_sequence_mismatch",
    "control_nonce_mismatch",
    "control_replay_detected",
    "control_channel_io_failure",
    "peer_identity_io_failure",
    "project_root_io_failure",
    "monitor_random_io_failure",
    "platform_or_capability_unavailable",
    "authority_invariant_failure",
)
PRELAUNCH_FAILURE_REASONS: Final = ("launch_nonce_already_consumed",)
POSITIVE_TERMINAL_REASON: Final = "peer_bound_no_data_stop"
REASON_TO_EXIT_CODE: Final = {
    "actual_native_argv_mismatch": 64,
    "actual_native_environment_not_empty": 78,
    "fixed_inherited_descriptors_invalid": 78,
    "descriptor_inventory_io_failure": 74,
    "fixed_descriptor_io_failure": 74,
    "request_frame_invalid": 65,
    "request_body_noncanonical": 65,
    "request_schema_mismatch": 65,
    "request_digest_mismatch": 65,
    "request_protocol_mismatch": 78,
    "request_target_phase_mismatch": 78,
    "scheduler_identity_prefix_mismatch": 78,
    "request_no_data_policy_mismatch": 78,
    "launch_nonce_already_consumed": 65,
    "peer_credentials_mismatch": 77,
    "peer_process_identity_mismatch": 77,
    "peer_process_image_mismatch": 77,
    "project_root_identity_mismatch": 77,
    "control_message_invalid": 65,
    "control_sequence_mismatch": 65,
    "control_nonce_mismatch": 65,
    "control_replay_detected": 65,
    "control_channel_io_failure": 74,
    "peer_identity_io_failure": 74,
    "project_root_io_failure": 74,
    "monitor_random_io_failure": 74,
    "platform_or_capability_unavailable": 69,
    "authority_invariant_failure": 70,
    "peer_bound_no_data_stop": 69,
}
STABLE_REASON_IDS: Final = tuple(REASON_TO_EXIT_CODE)

FAILURE_REASON_STATES: Final = {
    "actual_native_argv_mismatch": frozenset({"ENTRY_EXACT"}),
    "actual_native_environment_not_empty": frozenset({"ENTRY_EXACT"}),
    "fixed_inherited_descriptors_invalid": frozenset({"FD_SHAPE"}),
    "descriptor_inventory_io_failure": frozenset({"FD_SHAPE"}),
    "fixed_descriptor_io_failure": frozenset({"FD_SHAPE"}),
    "request_frame_invalid": frozenset({"REQUEST_FRAME_EXACT_AND_EOF"}),
    "request_body_noncanonical": frozenset({"BODY_CANONICAL"}),
    "request_schema_mismatch": frozenset({"BODY_CANONICAL"}),
    "request_digest_mismatch": frozenset(
        {"REQUEST_NO_DATA_POLICY", "STOP_ACK_VERIFIED_SEQUENCE_1"}
    ),
    "request_protocol_mismatch": frozenset({"REQUEST_NO_DATA_POLICY"}),
    "request_target_phase_mismatch": frozenset({"REQUEST_NO_DATA_POLICY"}),
    "scheduler_identity_prefix_mismatch": frozenset(
        {"REQUEST_NO_DATA_POLICY"}
    ),
    "request_no_data_policy_mismatch": frozenset(
        {"REQUEST_NO_DATA_POLICY"}
    ),
    "peer_credentials_mismatch": frozenset(
        {
            "PEER_PIDFD_PINNED",
            "STOP_ACK_VERIFIED_SEQUENCE_1",
            "PEER_AND_ROOT_REVALIDATED",
        }
    ),
    "peer_process_identity_mismatch": frozenset(
        {"PEER_PIDFD_PINNED", "PEER_AND_ROOT_REVALIDATED"}
    ),
    "peer_process_image_mismatch": frozenset(
        {"PEER_PIDFD_PINNED", "PEER_AND_ROOT_REVALIDATED"}
    ),
    "project_root_identity_mismatch": frozenset(
        {"PROJECT_ROOT_BOUND", "PEER_AND_ROOT_REVALIDATED"}
    ),
    "control_message_invalid": frozenset({"STOP_ACK_VERIFIED_SEQUENCE_1"}),
    "control_sequence_mismatch": frozenset(
        {"STOP_ACK_VERIFIED_SEQUENCE_1"}
    ),
    "control_nonce_mismatch": frozenset({"STOP_ACK_VERIFIED_SEQUENCE_1"}),
    "control_replay_detected": frozenset(
        {"STOP_ACK_VERIFIED_SEQUENCE_1"}
    ),
    "control_channel_io_failure": frozenset(
        {
            "CHALLENGE_SENT_SEQUENCE_0",
            "STOP_ACK_VERIFIED_SEQUENCE_1",
            "PEER_AND_ROOT_REVALIDATED",
            "PEER_BOUND_STOP_RECEIPT_SEQUENCE_2",
        }
    ),
    "peer_identity_io_failure": frozenset(
        {"PEER_PIDFD_PINNED", "PEER_AND_ROOT_REVALIDATED"}
    ),
    "project_root_io_failure": frozenset(
        {"PROJECT_ROOT_BOUND", "PEER_AND_ROOT_REVALIDATED"}
    ),
    "monitor_random_io_failure": frozenset({"CHALLENGE_SENT_SEQUENCE_0"}),
    "platform_or_capability_unavailable": frozenset(
        {"PLATFORM_CAPABILITY"}
    ),
    "authority_invariant_failure": frozenset(
        {"PEER_BOUND_STOP_RECEIPT_SEQUENCE_2"}
    ),
}

FAILURE_DIGEST_KNOWNNESS: Final = {
    reason: (
        (True, False)
        if reason == "request_body_noncanonical"
        else (True, True)
        if reason
        not in {
            "actual_native_argv_mismatch",
            "actual_native_environment_not_empty",
            "fixed_inherited_descriptors_invalid",
            "descriptor_inventory_io_failure",
            "fixed_descriptor_io_failure",
            "request_frame_invalid",
            "platform_or_capability_unavailable",
        }
        else (False, False)
    )
    for reason in NATIVE_FAILURE_REASONS
}

_SHA256_LENGTH: Final = 64
_MAX_DYNAMIC_STRING_BYTES: Final = 4096
_MAX_PATH_BYTES: Final = 4095
_MAX_PID: Final = (1 << 31) - 1
_MAX_UINT32: Final = (1 << 32) - 1
_MAX_UINT64: Final = (1 << 64) - 1


class Step5DProtocolViolation(ValueError):
    """One exact fail-closed reason selected by the frozen precedence."""

    def __init__(self, reason_id: str):
        super().__init__(reason_id)
        self.reason_id = reason_id
        self.exit_code = REASON_TO_EXIT_CODE[reason_id]


class RequestDigests(NamedTuple):
    request_frame_sha256: str
    request_body_sha256: str


def _raise(reason_id: str) -> None:
    raise Step5DProtocolViolation(reason_id)


def _validate_json_value(value: Any) -> None:
    value_type = type(value)
    if value is None or value_type in (str, bool, int):
        if value_type is str:
            try:
                value.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ValueError("JSON string is not valid UTF-8") from exc
        return
    if value_type is list:
        for child in value:
            _validate_json_value(child)
        return
    if value_type is dict:
        for key, child in value.items():
            if type(key) is not str:
                raise ValueError("JSON object key is not exact text")
            _validate_json_value(child)
        return
    raise ValueError("Step 5D JSON permits only null, bool, int, str, list and dict")


def canonical_json_bytes(value: Any) -> bytes:
    """Return compact sorted UTF-8 JSON with the Step 5D scalar model."""

    _validate_json_value(value)
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ValueError("value is not Step 5D canonical JSON") from exc


def parse_canonical_json_object(payload: bytes) -> dict[str, Any]:
    """Parse one exact canonical plain object and reject duplicate keys/floats."""

    if type(payload) is not bytes:
        raise TypeError("canonical payload must be exact bytes")

    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, child in pairs:
            if key in result:
                raise ValueError("duplicate JSON object key")
            result[key] = child
        return result

    def reject_number(_token: str) -> Any:
        raise ValueError("floating-point and non-finite JSON are forbidden")

    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=object_pairs,
            parse_float=reject_number,
            parse_constant=reject_number,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("payload is not strict canonical JSON") from exc
    if type(value) is not dict or canonical_json_bytes(value) != payload:
        raise ValueError("payload is not one canonical plain object")
    return value


def _plain_object(value: Any, keys: frozenset[str]) -> dict[str, Any]:
    if type(value) is not dict or frozenset(value) != keys:
        _raise("request_schema_mismatch")
    return value


def _exact_text(value: Any) -> str:
    if type(value) is not str:
        _raise("request_schema_mismatch")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        _raise("request_schema_mismatch")
    if b"\x00" in encoded:
        _raise("request_schema_mismatch")
    return value


def _bounded_text(value: Any, *, minimum: int, maximum: int) -> str:
    text = _exact_text(value)
    length = len(text.encode("utf-8"))
    if not minimum <= length <= maximum:
        _raise("request_schema_mismatch")
    return text


def _is_lower_sha256(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == _SHA256_LENGTH
        and all(character in "0123456789abcdef" for character in value)
    )


def _require_sha256(value: Any) -> str:
    if not _is_lower_sha256(value):
        _raise("request_schema_mismatch")
    return value


def _strict_int(value: Any, *, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        _raise("request_schema_mismatch")
    return value


def _validate_path(value: Any) -> str:
    path = _bounded_text(value, minimum=2, maximum=_MAX_PATH_BYTES)
    if (
        not path.startswith("/")
        or path.startswith("//")
        or path.endswith("/")
    ):
        _raise("request_schema_mismatch")
    components = path[1:].split("/")
    if any(
        component in ("", ".", "..")
        or len(component.encode("utf-8")) > 255
        for component in components
    ):
        _raise("request_schema_mismatch")
    return path


def _validate_request_structure(request: dict[str, Any], deployment: NativeDeployment) -> None:
    if frozenset(request) != REQUEST_KEYS:
        _raise("request_schema_mismatch")
    if request["schema"] != REQUEST_SCHEMA or type(request["schema"]) is not str:
        _raise("request_schema_mismatch")
    for key in (
        "request_kind",
        "target_phase_profile",
        "protocol_sha256",
        "scientific_input_policy",
    ):
        _exact_text(request[key])
    authorization = request["active_phase_authorization_sha256"]
    if authorization is not None and type(authorization) is not str:
        _raise("request_schema_mismatch")
    if type(authorization) is str:
        _exact_text(authorization)

    prefix = _plain_object(
        request["scheduler_identity_prefix"], SCHEDULER_PREFIX_KEYS
    )
    for key in SCHEDULER_PREFIX_KEYS:
        _exact_text(prefix[key])
    _bounded_text(
        prefix["SCHED_RUN_ID"], minimum=1, maximum=_MAX_DYNAMIC_STRING_BYTES
    )
    _bounded_text(
        prefix["SCHED_LAUNCH_MARKER"],
        minimum=1,
        maximum=_MAX_DYNAMIC_STRING_BYTES,
    )

    launch_nonce = request["launch_nonce"]
    _require_sha256(launch_nonce)

    peer = _plain_object(request["control_peer_expectation"], CONTROL_PEER_KEYS)
    if type(peer["peer_role"]) is not str or peer["peer_role"] != PEER_ROLE:
        _raise("request_schema_mismatch")
    credentials = _plain_object(peer["credentials"], CREDENTIAL_KEYS)
    credential_pid = _strict_int(
        credentials["pid"], minimum=1, maximum=_MAX_PID
    )
    _strict_int(credentials["uid"], minimum=0, maximum=_MAX_UINT32)
    _strict_int(credentials["gid"], minimum=0, maximum=_MAX_UINT32)
    identity = _plain_object(peer["process_identity"], PROCESS_IDENTITY_KEYS)
    _require_sha256(identity["boot_id_sha256"])
    identity_pid = _strict_int(identity["pid"], minimum=1, maximum=_MAX_PID)
    _strict_int(identity["start_ticks"], minimum=1, maximum=_MAX_UINT64)
    _strict_int(identity["parent_pid"], minimum=1, maximum=_MAX_PID)
    _strict_int(identity["process_group_id"], minimum=1, maximum=_MAX_PID)
    _require_sha256(identity["cgroup_identity_sha256"])
    if credential_pid != identity_pid:
        _raise("request_schema_mismatch")

    root = _plain_object(request["project_root_expectation"], PROJECT_ROOT_KEYS)
    if type(root["project"]) is not str or root["project"] != deployment.project:
        _raise("request_schema_mismatch")
    _validate_path(root["canonical_absolute_path"])
    _strict_int(root["st_dev"], minimum=0, maximum=_MAX_UINT64)
    _strict_int(root["st_ino"], minimum=1, maximum=_MAX_UINT64)
    mode = _strict_int(root["st_mode"], minimum=0, maximum=_MAX_UINT32)
    if not stat.S_ISDIR(mode):
        _raise("request_schema_mismatch")
    _strict_int(root["st_uid"], minimum=0, maximum=_MAX_UINT32)
    _strict_int(root["st_gid"], minimum=0, maximum=_MAX_UINT32)

    argv = request["submitted_logical_argv"]
    if type(argv) is not list:
        _raise("request_schema_mismatch")
    for item in argv:
        _bounded_text(item, minimum=0, maximum=_MAX_DYNAMIC_STRING_BYTES)
    _require_sha256(request["submitted_logical_argv_sha256"])


def validate_request_body(payload: bytes, *, deployment: NativeDeployment) -> dict[str, Any]:
    """Validate one request body in the exact frozen first-failure order."""

    try:
        request = parse_canonical_json_object(payload)
    except (TypeError, ValueError):
        _raise("request_body_noncanonical")
    _validate_request_structure(request, require_deployment(deployment))

    if request["protocol_sha256"] != PARENT_PROTOCOL_SHA256:
        _raise("request_protocol_mismatch")

    phase = request["target_phase_profile"]
    if phase not in PHASE_PROFILES:
        _raise("request_target_phase_mismatch")
    expected_argv = deployment.argv(phase)
    if tuple(request["submitted_logical_argv"]) != expected_argv:
        _raise("request_target_phase_mismatch")

    prefix = request["scheduler_identity_prefix"]
    expected_batch = deployment.batch_name(phase)
    expected_task = deployment.task_id(phase)
    run_id = prefix["SCHED_RUN_ID"]
    expected_rc = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:24]
    if (
        prefix["SCHED_PROJECT"] != deployment.project
        or prefix["SCHED_BATCH_ID"] != expected_batch
        or prefix["SCHED_TASK_ID"] != expected_task
        or prefix["SCHED_RC_PREFIX"] != expected_rc
    ):
        _raise("scheduler_identity_prefix_mismatch")

    if (
        request["request_kind"] != REQUEST_KIND
        or request["active_phase_authorization_sha256"] is not None
        or request["scientific_input_policy"] != SCIENTIFIC_INPUT_POLICY
    ):
        _raise("request_no_data_policy_mismatch")

    expected_argv_sha = hashlib.sha256(
        canonical_json_bytes(list(expected_argv))
    ).hexdigest()
    if request["submitted_logical_argv_sha256"] != expected_argv_sha:
        _raise("request_digest_mismatch")
    return request


def build_request_body(
    *,
    deployment: NativeDeployment,
    target_phase_profile: str,
    scheduler_identity_prefix: Mapping[str, Any],
    launch_nonce: str,
    control_peer_expectation: Mapping[str, Any],
    project_root_expectation: Mapping[str, Any],
) -> bytes:
    """Construct and self-validate one stop-only request body."""

    if target_phase_profile not in PHASE_PROFILES:
        _raise("request_target_phase_mismatch")
    argv = list(require_deployment(deployment).argv(target_phase_profile))
    request = {
        "schema": REQUEST_SCHEMA,
        "request_kind": REQUEST_KIND,
        "target_phase_profile": target_phase_profile,
        "protocol_sha256": PARENT_PROTOCOL_SHA256,
        "active_phase_authorization_sha256": None,
        "scheduler_identity_prefix": dict(scheduler_identity_prefix),
        "launch_nonce": launch_nonce,
        "control_peer_expectation": dict(control_peer_expectation),
        "project_root_expectation": dict(project_root_expectation),
        "submitted_logical_argv": argv,
        "submitted_logical_argv_sha256": hashlib.sha256(
            canonical_json_bytes(argv)
        ).hexdigest(),
        "scientific_input_policy": SCIENTIFIC_INPUT_POLICY,
    }
    payload = canonical_json_bytes(request)
    validate_request_body(payload, deployment=deployment)
    return payload


def encode_request_frame(body: bytes, *, deployment: NativeDeployment) -> bytes:
    validate_request_body(body, deployment=deployment)
    return encode_native_launch_frame(
        channel=NATIVE_LAUNCH_CHANNEL_REQUEST,
        message_type=NATIVE_LAUNCH_MESSAGE_REQUEST,
        sequence=NATIVE_LAUNCH_REQUEST_SEQUENCE,
        opaque_body=body,
    )


def request_digests(frame: bytes, *, deployment: NativeDeployment) -> RequestDigests:
    try:
        decoded = decode_native_launch_frame(frame)
    except (TypeError, ValueError):
        _raise("request_frame_invalid")
    if (
        decoded.channel != NATIVE_LAUNCH_CHANNEL_REQUEST
        or decoded.message_type != NATIVE_LAUNCH_MESSAGE_REQUEST
        or decoded.sequence != NATIVE_LAUNCH_REQUEST_SEQUENCE
    ):
        _raise("request_frame_invalid")
    validate_request_body(decoded.opaque_body, deployment=deployment)
    return RequestDigests(
        request_frame_sha256=hashlib.sha256(frame).hexdigest(),
        request_body_sha256=hashlib.sha256(decoded.opaque_body).hexdigest(),
    )


def _require_control_binding(value: Mapping[str, Any]) -> None:
    for key in (
        "request_frame_sha256",
        "request_body_sha256",
        "launch_nonce",
        "monitor_nonce",
    ):
        if not _is_lower_sha256(value.get(key)):
            raise Step5DProtocolViolation("control_message_invalid")
    if value.get("target_phase_profile") not in PHASE_PROFILES:
        raise Step5DProtocolViolation("control_message_invalid")


def build_challenge_body(
    *,
    digests: RequestDigests,
    launch_nonce: str,
    monitor_nonce: str,
    target_phase_profile: str,
) -> bytes:
    value = {
        "schema": CHALLENGE_SCHEMA,
        "request_frame_sha256": digests.request_frame_sha256,
        "request_body_sha256": digests.request_body_sha256,
        "launch_nonce": launch_nonce,
        "monitor_nonce": monitor_nonce,
        "target_phase_profile": target_phase_profile,
    }
    _require_control_binding(value)
    return canonical_json_bytes(value)


def parse_challenge_frame(frame: bytes) -> dict[str, Any]:
    try:
        decoded = decode_native_launch_frame(frame)
        value = parse_canonical_json_object(decoded.opaque_body)
    except (TypeError, ValueError):
        raise Step5DProtocolViolation("control_message_invalid")
    if (
        decoded.channel != NATIVE_LAUNCH_CHANNEL_CONTROL
        or decoded.message_type != NATIVE_LAUNCH_MESSAGE_CONTROL
        or decoded.sequence != CONTROL_CHALLENGE_SEQUENCE
        or frozenset(value) != CHALLENGE_KEYS
        or value.get("schema") != CHALLENGE_SCHEMA
    ):
        raise Step5DProtocolViolation("control_message_invalid")
    _require_control_binding(value)
    return value


def build_ack_body(challenge: Mapping[str, Any]) -> bytes:
    value = dict(challenge)
    if frozenset(value) != CHALLENGE_KEYS or value.get("schema") != CHALLENGE_SCHEMA:
        raise Step5DProtocolViolation("control_message_invalid")
    _require_control_binding(value)
    value["schema"] = ACK_SCHEMA
    value["decision"] = ACK_DECISION
    return canonical_json_bytes(value)


def encode_ack_frame(challenge: Mapping[str, Any]) -> bytes:
    return encode_native_launch_frame(
        channel=NATIVE_LAUNCH_CHANNEL_CONTROL,
        message_type=NATIVE_LAUNCH_MESSAGE_CONTROL,
        sequence=CONTROL_ACK_SEQUENCE,
        opaque_body=build_ack_body(challenge),
    )


def build_receipt_body(binding: Mapping[str, Any]) -> bytes:
    """Construct the unique authority-negative sequence-2 stop receipt body."""

    _require_control_binding(binding)
    value = {
        "schema": RECEIPT_SCHEMA,
        "request_frame_sha256": binding["request_frame_sha256"],
        "request_body_sha256": binding["request_body_sha256"],
        "launch_nonce": binding["launch_nonce"],
        "monitor_nonce": binding["monitor_nonce"],
        "target_phase_profile": binding["target_phase_profile"],
        "result": RECEIPT_RESULT,
        "kernel_peer_identity_bound": True,
        "direct_parent_live_at_final_observation": True,
        "peer_process_image_endpoint_observations_equal": True,
        "project_root_endpoint_observations_equal": True,
        "unexpected_inherited_fd_count": 0,
        "scheduler_role_authority_claimed": False,
        "external_anchor_authenticated": False,
        "formal_ready": False,
        "scientific_result": False,
        "logical_python_executed": False,
        "external_formal_authority_claimed": False,
    }
    return canonical_json_bytes(value)


def encode_receipt_frame(binding: Mapping[str, Any]) -> bytes:
    return encode_native_launch_frame(
        channel=NATIVE_LAUNCH_CHANNEL_CONTROL,
        message_type=NATIVE_LAUNCH_MESSAGE_CONTROL,
        sequence=CONTROL_RECEIPT_SEQUENCE,
        opaque_body=build_receipt_body(binding),
    )


def build_failure_body(
    *,
    failed_state: str,
    reason_id: str,
    request_frame_sha256: str | None,
    request_body_sha256: str | None,
) -> bytes:
    """Construct one optional authority-negative diagnostic failure body."""

    if failed_state not in STATE_MACHINE or reason_id not in NATIVE_FAILURE_REASONS:
        raise Step5DProtocolViolation("control_message_invalid")
    for digest in (request_frame_sha256, request_body_sha256):
        if digest is not None and not _is_lower_sha256(digest):
            raise Step5DProtocolViolation("control_message_invalid")
    if failed_state not in FAILURE_REASON_STATES[reason_id]:
        raise Step5DProtocolViolation("control_message_invalid")
    knownness = (
        request_frame_sha256 is not None,
        request_body_sha256 is not None,
    )
    if knownness != FAILURE_DIGEST_KNOWNNESS[reason_id]:
        raise Step5DProtocolViolation("control_message_invalid")
    value = {
        "schema": FAILURE_SCHEMA,
        "failed_state": failed_state,
        "reason_id": reason_id,
        "exit_code": REASON_TO_EXIT_CODE[reason_id],
        "request_frame_sha256": request_frame_sha256,
        "request_body_sha256": request_body_sha256,
        "scheduler_role_authority_claimed": False,
        "external_anchor_authenticated": False,
        "formal_ready": False,
        "scientific_result": False,
        "logical_python_executed": False,
        "external_formal_authority_claimed": False,
    }
    return canonical_json_bytes(value)


def encode_failure_frame(
    *,
    failed_state: str,
    reason_id: str,
    request_frame_sha256: str | None,
    request_body_sha256: str | None,
) -> bytes:
    return encode_native_launch_frame(
        channel=NATIVE_LAUNCH_CHANNEL_CONTROL,
        message_type=NATIVE_LAUNCH_MESSAGE_CONTROL,
        sequence=CONTROL_FAILURE_SEQUENCE,
        opaque_body=build_failure_body(
            failed_state=failed_state,
            reason_id=reason_id,
            request_frame_sha256=request_frame_sha256,
            request_body_sha256=request_body_sha256,
        ),
    )


def parse_receipt_frame(
    frame: bytes,
    *,
    expected_binding: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        decoded = decode_native_launch_frame(frame)
        value = parse_canonical_json_object(decoded.opaque_body)
    except (TypeError, ValueError):
        raise Step5DProtocolViolation("control_message_invalid")
    if (
        decoded.channel != NATIVE_LAUNCH_CHANNEL_CONTROL
        or decoded.message_type != NATIVE_LAUNCH_MESSAGE_CONTROL
        or decoded.sequence != CONTROL_RECEIPT_SEQUENCE
        or frozenset(value) != RECEIPT_KEYS
        or value.get("schema") != RECEIPT_SCHEMA
        or value.get("result") != RECEIPT_RESULT
    ):
        raise Step5DProtocolViolation("control_message_invalid")
    _require_control_binding(value)
    for key in CHALLENGE_KEYS - {"schema"}:
        if value[key] != expected_binding[key]:
            raise Step5DProtocolViolation("control_message_invalid")
    expected_fixed = {
        "kernel_peer_identity_bound": True,
        "direct_parent_live_at_final_observation": True,
        "peer_process_image_endpoint_observations_equal": True,
        "project_root_endpoint_observations_equal": True,
        "unexpected_inherited_fd_count": 0,
        "scheduler_role_authority_claimed": False,
        "external_anchor_authenticated": False,
        "formal_ready": False,
        "scientific_result": False,
        "logical_python_executed": False,
        "external_formal_authority_claimed": False,
    }
    for key, expected in expected_fixed.items():
        if type(value[key]) is not type(expected) or value[key] != expected:
            raise Step5DProtocolViolation("control_message_invalid")
    return value


def parse_failure_frame(frame: bytes) -> dict[str, Any]:
    try:
        decoded = decode_native_launch_frame(frame)
        value = parse_canonical_json_object(decoded.opaque_body)
    except (TypeError, ValueError):
        raise Step5DProtocolViolation("control_message_invalid")
    if (
        decoded.channel != NATIVE_LAUNCH_CHANNEL_CONTROL
        or decoded.message_type != NATIVE_LAUNCH_MESSAGE_CONTROL
        or decoded.sequence != CONTROL_FAILURE_SEQUENCE
        or frozenset(value) != FAILURE_KEYS
        or value.get("schema") != FAILURE_SCHEMA
        or value.get("failed_state") not in STATE_MACHINE
        or value.get("reason_id") not in NATIVE_FAILURE_REASONS
        or type(value.get("exit_code")) is not int
        or value["exit_code"] != REASON_TO_EXIT_CODE[value["reason_id"]]
    ):
        raise Step5DProtocolViolation("control_message_invalid")
    for key in ("request_frame_sha256", "request_body_sha256"):
        if value[key] is not None and not _is_lower_sha256(value[key]):
            raise Step5DProtocolViolation("control_message_invalid")
    reason_id = value["reason_id"]
    if value["failed_state"] not in FAILURE_REASON_STATES[reason_id]:
        raise Step5DProtocolViolation("control_message_invalid")
    knownness = (
        value["request_frame_sha256"] is not None,
        value["request_body_sha256"] is not None,
    )
    if knownness != FAILURE_DIGEST_KNOWNNESS[reason_id]:
        raise Step5DProtocolViolation("control_message_invalid")
    for key in (
        "scheduler_role_authority_claimed",
        "external_anchor_authenticated",
        "formal_ready",
        "scientific_result",
        "logical_python_executed",
        "external_formal_authority_claimed",
    ):
        if value.get(key) is not False:
            raise Step5DProtocolViolation("control_message_invalid")
    return value


def scheduler_prefix(
    *, deployment: NativeDeployment, target_phase_profile: str, run_id: str, launch_marker: str
) -> dict[str, str]:
    if target_phase_profile not in PHASE_PROFILES:
        _raise("request_target_phase_mismatch")
    _bounded_text(run_id, minimum=1, maximum=_MAX_DYNAMIC_STRING_BYTES)
    _bounded_text(
        launch_marker, minimum=1, maximum=_MAX_DYNAMIC_STRING_BYTES
    )
    require_deployment(deployment)
    return {
        "SCHED_BATCH_ID": deployment.batch_name(target_phase_profile),
        "SCHED_TASK_ID": deployment.task_id(target_phase_profile),
        "SCHED_RUN_ID": run_id,
        "SCHED_PROJECT": deployment.project,
        "SCHED_RC_PREFIX": hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:24],
        "SCHED_LAUNCH_MARKER": launch_marker,
    }


def alignment_projection(*, deployment: NativeDeployment) -> dict[str, Any]:
    """Return the complete shared Step 5D scheduler/native projection."""

    require_deployment(deployment)
    false_authority_fields = [
        "scheduler_role_authority_claimed",
        "external_anchor_authenticated",
        "formal_ready",
        "scientific_result",
        "logical_python_executed",
        "external_formal_authority_claimed",
    ]
    return {
        "inherited_transport": {
            "immutable_in_this_contract": True,
            "actual_native_argv": list(NATIVE_ACTUAL_ARGV),
            "actual_native_environment": {},
            "open_fd_set_exact": [0, 1, 2, 3, 4, 5],
            "native_executable_launch_fd_closed_by_cloexec": (
                NATIVE_EXEC_TARGET_CLOEXEC_REQUIRED
            ),
            "request_fd": {
                "number": NATIVE_INHERITED_REQUEST_FD,
                "file_type": "regular",
                "access_mode": "O_RDONLY",
                "initial_offset": 0,
                "contains_exactly_one_frame_then_eof": True,
                "required_linux_seals": list(
                    NATIVE_REQUEST_REQUIRED_LINUX_SEALS
                ),
            },
            "control_fd": {
                "number": NATIVE_INHERITED_CONTROL_FD,
                "family": "AF_UNIX",
                "type": "SOCK_STREAM",
                "connected": True,
            },
            "project_root_fd": NATIVE_INHERITED_PROJECT_ROOT_FD,
            "wire": {
                "schema": NATIVE_LAUNCH_WIRE_SCHEMA,
                "magic_ascii": NATIVE_LAUNCH_WIRE_MAGIC.decode("ascii"),
                "version": NATIVE_LAUNCH_WIRE_VERSION,
                "outer_header_bytes": NATIVE_LAUNCH_WIRE_OUTER_HEADER_BYTES,
                "header_bytes": NATIVE_LAUNCH_WIRE_HEADER_BYTES,
                "minimum_body_bytes": 1,
                "maximum_body_bytes": NATIVE_LAUNCH_WIRE_MAX_BODY,
                "request_channel": NATIVE_LAUNCH_CHANNEL_REQUEST,
                "control_channel": NATIVE_LAUNCH_CHANNEL_CONTROL,
                "request_message_type": NATIVE_LAUNCH_MESSAGE_REQUEST,
                "control_message_type": NATIVE_LAUNCH_MESSAGE_CONTROL,
                "flags": NATIVE_LAUNCH_FLAGS_NONE,
                "request_sequence": NATIVE_LAUNCH_REQUEST_SEQUENCE,
            },
            "inheritance_rule": (
                "Every listed value is a normative copy of the Step 5C "
                "transport. Any mismatch with the parent Step 5C constants "
                "or evidence fails cross-repository alignment; omission here "
                "grants no fallback."
            ),
        },
        "canonical_json": {
            "encoding": "UTF-8 without BOM",
            "serialization": (
                "json.dumps(value, ensure_ascii=False, sort_keys=True, "
                "separators=(',', ':'), allow_nan=False).encode('utf-8')"
            ),
            "input_bytes_must_equal_reserialization": True,
            "top_level_must_be_plain_object": True,
            "duplicate_keys_forbidden": True,
            "extra_keys_forbidden": True,
            "trailing_lf_or_whitespace_forbidden": True,
            "nonfinite_numbers_forbidden": True,
            "floating_point_values_forbidden_in_step5d_messages": True,
            "json_boolean_must_never_be_accepted_as_integer": True,
            "integer_native_type_rule": (
                "every PID, UID, GID, start-tick and stat integer must "
                "losslessly round-trip through its corresponding Linux "
                "native type; negative, overflowed or truncated values are "
                "rejected"
            ),
            "c_string_rule": (
                "every field consumed as a C string is valid UTF-8, "
                "NUL-free and within its field-specific UTF-8 byte bound"
            ),
        },
        "request": {
            "schema": REQUEST_SCHEMA,
            "exact_keyset": sorted(REQUEST_KEYS),
            "fixed_values": {
                "request_kind": REQUEST_KIND,
                "protocol_sha256": PARENT_PROTOCOL_SHA256,
                "active_phase_authorization_sha256": None,
                "scientific_input_policy": SCIENTIFIC_INPUT_POLICY,
                "peer_role": PEER_ROLE,
                "scheduler_project": deployment.project,
                "project_root_project": deployment.project,
            },
            "field_types": {
                "schema": (
                    "string exactly m2b_native_peer_bound_stop_request/v1"
                ),
                "request_kind": (
                    "structurally a JSON string; semantically exactly "
                    "peer_bound_no_data_stop"
                ),
                "target_phase_profile": (
                    "structurally a JSON string; semantically one of "
                    "preparation, raw_collection or aggregation"
                ),
                "protocol_sha256": (
                    "structurally a JSON string; semantically exactly "
                    + PARENT_PROTOCOL_SHA256
                ),
                "active_phase_authorization_sha256": (
                    "structurally JSON null or string; semantically must be "
                    "null"
                ),
                "scheduler_identity_prefix": "scheduler_identity_prefix_v1",
                "launch_nonce": (
                    "lowercase 64-hex from 32 cryptographically secure random "
                    "bytes generated by the scheduler-side direct-parent "
                    "launch owner for one request and unused earlier in the "
                    "current owner lifetime"
                ),
                "control_peer_expectation": "control_peer_expectation_v1",
                "project_root_expectation": "project_root_expectation_v1",
                "submitted_logical_argv": (
                    "plain array canonically equal to "
                    "submitted_logical_argv_profiles[target_phase_profile]; "
                    "every element is valid UTF-8, NUL-free and at most 4096 "
                    "bytes"
                ),
                "submitted_logical_argv_sha256": (
                    "lowercase 64-hex over "
                    "canonical_json(submitted_logical_argv)"
                ),
                "scientific_input_policy": (
                    "structurally a JSON string; semantically exactly "
                    "forbid_dataset_cache_checkpoint_raw_result_and_"
                    "logical_python"
                ),
            },
            "phase_profiles": list(PHASE_PROFILES),
            "logical_argv_profiles": {
                phase: list(deployment.argv(phase))
                for phase in PHASE_PROFILES
            },
            "logical_argv_profile_rule": {
                "mapping_keyset_exact": list(PHASE_PROFILES),
                "status": "logical stop-only intent; never executed in Step 5D",
                "future_parser_gate": (
                    "the current bootstrap CLI does not promise to consume "
                    "--phase-profile, and the phase observation payload "
                    "recognizes later formal-probe batch/task/seventh-field "
                    "semantics rather than these Step 5D claims. Any future "
                    "logical execution requires a separate reviewed change "
                    "that aligns parser, phase payload, batch, retained "
                    "closure and external verifier without changing this "
                    "request silently."
                ),
            },
            "scalar_validation_rules": {
                "strict_integer": "type is JSON integer and not JSON boolean",
                "pid": (
                    "strictly positive and losslessly representable as Linux "
                    "pid_t"
                ),
                "uid_gid": (
                    "nonnegative and losslessly representable as Linux uid_t "
                    "or gid_t respectively"
                ),
                "start_ticks": (
                    "strictly positive and losslessly representable as "
                    "uint64_t"
                ),
                "stat_fields": (
                    "nonnegative and losslessly representable in the exact "
                    "native struct stat member type"
                ),
                "scheduler_dynamic_string": (
                    "valid UTF-8, NUL-free and 1..4096 bytes"
                ),
                "path": "valid UTF-8, NUL-free and 2..4095 bytes",
            },
            "nested_schemas": {
                "scheduler_identity_prefix_v1": {
                    "exact_keyset": sorted(SCHEDULER_PREFIX_KEYS),
                    "rules": {
                        "SCHED_BATCH_ID": (
                            "exact Step 5D batch claim "
                            + deployment.batch_name_template.replace("{phase}", "{target_phase_profile}")
                        ),
                        "SCHED_TASK_ID": (
                            "exact Step 5D task claim "
                            + deployment.task_id_template.replace("{phase}", "{target_phase_profile}")
                        ),
                        "SCHED_RUN_ID": (
                            "untrusted scheduler_dynamic_string; shape only "
                            "because this slice has no request-external "
                            "expected value"
                        ),
                        "SCHED_PROJECT": "string exactly " + deployment.project,
                        "SCHED_RC_PREFIX": (
                            "first 24 lowercase hexadecimal characters of "
                            "SHA256(SCHED_RUN_ID UTF-8)"
                        ),
                        "SCHED_LAUNCH_MARKER": (
                            "untrusted scheduler_dynamic_string; shape only "
                            "because this slice has no request-external "
                            "expected value"
                        ),
                    },
                    "seventh_field_absent": (
                        "SCHED_EXEC_ATTESTATION_SHA256 is forbidden in this "
                        "Step 5D request prefix because this slice does not "
                        "construct or attest it"
                    ),
                },
                "control_peer_expectation_v1": {
                    "exact_keyset": sorted(CONTROL_PEER_KEYS),
                    "rules": {
                        "peer_role": (
                            "string exactly launch_owner_control_parent"
                        ),
                        "credentials": "credentials_v1",
                        "process_identity": "process_identity_v1",
                    },
                },
                "credentials_v1": {
                    "exact_keyset": sorted(CREDENTIAL_KEYS),
                    "rules": {
                        "pid": "strict pid equal to process_identity.pid",
                        "uid": "strict uid",
                        "gid": "strict gid",
                    },
                },
                "process_identity_v1": {
                    "exact_keyset": sorted(PROCESS_IDENTITY_KEYS),
                    "field_rules": {
                        "boot_id_sha256": "lowercase 64-hex",
                        "pid": (
                            "strict pid equal to credentials.pid and native "
                            "getppid()"
                        ),
                        "start_ticks": (
                            "strictly positive strict start_ticks"
                        ),
                        "parent_pid": (
                            "strictly positive strict pid_t value from the "
                            "peer's bracketed /proc/<pid>/stat"
                        ),
                        "process_group_id": (
                            "strictly positive strict pid_t value"
                        ),
                        "cgroup_identity_sha256": (
                            "lowercase 64-hex over the canonical JSON "
                            "serialization of the complete parsed and "
                            "canonical-sorted parent-protocol "
                            "linux_cgroup_record_v1 array"
                        ),
                    },
                    "source_rule": (
                        "Exactly inherit parent_protocol.future_record_schema_"
                        "common.lifecycle_artifact_schemas.process_identity: "
                        "pidfd_open before two bracketed O_RDONLY|O_NOFOLLOW "
                        "/proc/<pid>/stat reads, one boot-id read and one "
                        "complete cgroup read; pidfd remains non-readable and "
                        "the six fields remain equal through the second stat "
                        "read."
                    ),
                    "equality_rule": (
                        "all six fields are canonically equal; PID alone is "
                        "never process identity"
                    ),
                },
                "project_root_expectation_v1": {
                    "exact_keyset": sorted(PROJECT_ROOT_KEYS),
                    "rules": {
                        "project": "string exactly " + deployment.project,
                        "canonical_absolute_path": (
                            "path that starts with one slash, is not root, has "
                            "no trailing slash, NUL, //, empty, dot or dot-dot "
                            "component, and has only components of 1..255 "
                            "UTF-8 bytes"
                        ),
                        "st_dev": "strict stat field",
                        "st_ino": "strictly positive strict stat field",
                        "st_mode": (
                            "strict stat field whose file type is directory; "
                            "the full value, not only S_IFMT, is compared "
                            "everywhere"
                        ),
                        "st_uid": "strict uid and strict stat field",
                        "st_gid": "strict gid and strict stat field",
                    },
                },
            },
            "validation_precedence": list(REQUEST_VALIDATION_PRECEDENCE),
            "digest_rules": {
                "request_frame_sha256": (
                    "lowercase SHA256 over the exact fully sealed FD 3 bytes, "
                    "including the 4-byte outer length, 20-byte wire header "
                    "and canonical body"
                ),
                "request_body_sha256": (
                    "lowercase SHA256 over the exact canonical request-body "
                    "bytes only"
                ),
                "scheduler_plan_migration": (
                    "The existing Step 5C NativeLaunchPlan.request_sha256 "
                    "names the whole-frame digest. Before Step 5D "
                    "implementation it must be renamed request_frame_sha256 "
                    "and a distinct request_body_sha256 must be added; aliases "
                    "or one digest used for both objects are forbidden."
                ),
            },
            "authority_rule": (
                "The request body and its digests are untrusted claims. Step "
                "5D validates their exact syntax/policy and binds them to one "
                "live direct-parent connection, but does not authenticate a "
                "reviewed scheduler role, daemon restart, external verifier, "
                "project code or formal phase."
            ),
        },
        "control": {
            "frame_channel": NATIVE_LAUNCH_CHANNEL_CONTROL,
            "frame_message_type": NATIVE_LAUNCH_MESSAGE_CONTROL,
            "flags": NATIVE_LAUNCH_FLAGS_NONE,
            "challenge": {
                "sequence": CONTROL_CHALLENGE_SEQUENCE,
                "schema": CHALLENGE_SCHEMA,
                "sender": "native_entry",
                "exact_keyset": sorted(CHALLENGE_KEYS),
            },
            "ack": {
                "sequence": CONTROL_ACK_SEQUENCE,
                "schema": ACK_SCHEMA,
                "sender": "live_direct_parent_control_peer",
                "exact_keyset": sorted(ACK_KEYS),
                "decision": ACK_DECISION,
                "ancillary_data": (
                    "the first nonempty sendmsg segment explicitly carries "
                    "one SCM_CREDENTIALS; later user-space sends attach no "
                    "ancillary item, but SO_PASSCRED may cause the kernel to "
                    "attach credentials to later stream segments. Native "
                    "requires at least one observed SCM_CREDENTIALS and every "
                    "observed credential equal to SO_PEERCRED, getppid and "
                    "the request; after the complete ACK bytes native requires "
                    "write-half EOF but does not claim which process invoked "
                    "shutdown(SHUT_WR)."
                ),
            },
            "receipt": {
                "sequence": CONTROL_RECEIPT_SEQUENCE,
                "schema": RECEIPT_SCHEMA,
                "sender": "native_entry",
                "exact_keyset": sorted(RECEIPT_KEYS),
                "fixed_values": {
                    "result": RECEIPT_RESULT,
                    "kernel_peer_identity_bound": True,
                    "direct_parent_live_at_final_observation": True,
                    "peer_process_image_endpoint_observations_equal": True,
                    "project_root_endpoint_observations_equal": True,
                    "unexpected_inherited_fd_count": 0,
                    **{field: False for field in false_authority_fields},
                },
                "termination": (
                    "native sends exactly this one complete sequence-2 frame "
                    "with MSG_NOSIGNAL, calls shutdown(SHUT_WR), sends no "
                    "trailing frame/bytes and then returns EX_UNAVAILABLE"
                ),
            },
            "failure": {
                "sequence": CONTROL_FAILURE_SEQUENCE,
                "schema": FAILURE_SCHEMA,
                "sender": "native_entry_when_control_fd_is_already_shape_valid",
                "exact_keyset": sorted(FAILURE_KEYS),
                "rules": {
                    "failed_state": (
                        "one state-machine state whose validation detected "
                        "the failure, whether or not that state completed, "
                        "before FAIL_CLOSED"
                    ),
                    "reason_id": (
                        "one failure_contract.native_failure_reason_ids entry"
                    ),
                    "exit_code": (
                        "integer exactly equal to "
                        "failure_contract.reason_to_exit_code[reason_id]"
                    ),
                    "request_frame_sha256": (
                        "null before the exact sealed frame is fixed, "
                        "otherwise its lowercase 64-hex SHA256"
                    ),
                    "request_body_sha256": (
                        "null before canonical body bytes are fixed, otherwise "
                        "their lowercase 64-hex SHA256"
                    ),
                    **{
                        field: "boolean exactly false"
                        for field in false_authority_fields
                    },
                },
                "authority": (
                    "diagnostic only; absence is allowed when FD 4 is not "
                    "trustworthy, presence never converts a failure into "
                    "authority, and failure-message send failure never "
                    "replaces the original reason or sysexit"
                ),
            },
            "message_encoding_and_types": {
                "canonical_rule": (
                    "Every challenge, ACK, receipt and optional failure body "
                    "obeys canonical_json exactly, including exact keysets, "
                    "no duplicate/extra keys, no floats, no boolean-as-integer "
                    "and input bytes equal to canonical reserialization."
                ),
                "common_binding_fields": {
                    "request_frame_sha256": "lowercase 64-hex",
                    "request_body_sha256": (
                        "lowercase 64-hex distinct in meaning from "
                        "request_frame_sha256 even if a pathological byte "
                        "collision were ever observed"
                    ),
                    "launch_nonce": (
                        "lowercase 64-hex canonically equal to the request"
                    ),
                    "monitor_nonce": (
                        "lowercase 64-hex generated from exactly 32 getrandom "
                        "bytes"
                    ),
                    "target_phase_profile": (
                        "one of preparation, raw_collection or aggregation"
                    ),
                },
                "challenge": {
                    "schema": (
                        "string exactly m2b_native_launch_challenge/v1"
                    )
                },
                "ack": {
                    "schema": (
                        "string exactly m2b_launch_owner_native_stop_ack/v1"
                    ),
                    "decision": (
                        "string exactly "
                        "acknowledge_peer_bound_no_data_stop_only"
                    ),
                },
                "receipt": {
                    "schema": (
                        "string exactly m2b_native_launch_peer_bound_stop/v1"
                    ),
                    "result": "string exactly peer_bound_no_data_stop",
                    "true_boolean_fields": [
                        "kernel_peer_identity_bound",
                        "direct_parent_live_at_final_observation",
                        "peer_process_image_endpoint_observations_equal",
                        "project_root_endpoint_observations_equal",
                    ],
                    "zero_integer_fields": ["unexpected_inherited_fd_count"],
                    "false_boolean_fields": false_authority_fields,
                },
                "failure": {
                    "schema": "string exactly m2b_native_launch_failure/v1",
                    "failed_state": (
                        "string naming exactly one state_machine entry whose "
                        "validation detected the failure before FAIL_CLOSED"
                    ),
                    "reason_id": (
                        "string in failure_contract.native_failure_reason_ids"
                    ),
                    "exit_code": (
                        "strict JSON integer, not boolean, equal to "
                        "failure_contract.reason_to_exit_code[reason_id]"
                    ),
                    "request_digests": (
                        "each is null before its exact bytes are fixed and "
                        "otherwise lowercase 64-hex"
                    ),
                    "authority_boolean_fields": (
                        "scheduler_role_authority_claimed, "
                        "external_anchor_authenticated, formal_ready, "
                        "scientific_result, logical_python_executed and "
                        "external_formal_authority_claimed are JSON boolean "
                        "false"
                    ),
                },
            },
            "deadline_and_signal_rule": {
                "absolute_monotonic_deadline_ms": CONTROL_DEADLINE_MS,
                "deadline_interval": (
                    "sample once immediately before the first challenge-frame "
                    "send; challenge send, complete ACK plus EOF receive, final "
                    "peer/root revalidation, complete receipt send and native "
                    "shutdown(SHUT_WR) must all finish before that absolute time"
                ),
                "eintr_never_resets_deadline": True,
                "send_uses_msg_nosignal": True,
                "timeout_epipe_sigpipe_or_premature_eof": (
                    "control_channel_io_failure"
                ),
            },
            "partial_io_rule": (
                "read/write/recvmsg loops handle EINTR and partial transfers. "
                "For ACK, native accumulates SCM_CREDENTIALS across all "
                "recvmsg calls, parses one bounded stream through peer "
                "write-half EOF, rejects MSG_CTRUNC, every unknown ancillary "
                "item and any credential drift, and applies "
                "ack_failure_precedence. Missing credentials/EOF, bytes after "
                "the frame, duplicate/reordered frame or unknown messages fail "
                "closed. Only positive P01-P04 fixture cases require the parent "
                "to parse exactly one canonical receipt, observe native "
                "write-half EOF, then wait for the same child exit code 69; "
                "negative cases instead require their mapped fail-closed "
                "outcome and no success receipt, while prelaunch replay creates "
                "no child."
            ),
            "ack_failure_precedence": list(ACK_FAILURE_PRECEDENCE),
            "durability": (
                "The sequence-2 receipt is connection-local evidence only and "
                "is not a durable probe record, authorization or append-only "
                "ledger entry"
            ),
        },
        "state_machine": list(STATE_MACHINE),
        "state_machine_rules": {
            "forward_only": True,
            "one_terminal_failure_state": "FAIL_CLOSED",
            "prelaunch_terminal_failure_state": (
                "PRELAUNCH_REJECTED_FAIL_CLOSED exists only in the stateful "
                "launch owner for launch_nonce_already_consumed before "
                "socketpair, request memfd or native child creation; it is not "
                "a native state and emits no control frame"
            ),
            "failure_action": (
                "emit at most one exact sequence-4294967295 diagnostic failure "
                "message only if FD 4 has already passed shape checks, close "
                "owned temporary descriptors, emit at most one stable stderr "
                "line, then return the mapped sysexit"
            ),
            "forbidden_at_every_state": [
                "fork",
                "vfork",
                "clone for process creation",
                "posix_spawn",
                "execve or execveat of logical Python",
                "openat below project-root FD",
                "dataset/cache/checkpoint/raw-result access",
                "durable success publication",
            ],
        },
        "failure_contract": {
            "exit_codes": {
                "64": "EX_USAGE for actual native argv mismatch",
                "65": (
                    "EX_DATAERR for frame, canonical JSON, exact key/type/hash, "
                    "sequence, nonce or replay violation"
                ),
                "69": (
                    "EX_UNAVAILABLE for unsupported platform/capability or the "
                    "unique positive peer-bound no-data stop; only the "
                    "sequence-2 receipt distinguishes the positive stop"
                ),
                "70": (
                    "EX_SOFTWARE for an internal invariant failure after "
                    "peer/root identity binding was acquired"
                ),
                "74": (
                    "EX_IOERR for descriptor inventory/shape probe I/O, "
                    "short/failed control I/O, recvmsg, pidfd, proc, getrandom "
                    "or fstat operations"
                ),
                "77": (
                    "EX_NOPERM for live peer credentials/process identity or "
                    "project-root identity mismatch"
                ),
                "78": (
                    "EX_CONFIG for nonempty actual env, inherited FD shape, "
                    "protocol, target profile, scheduler-prefix, "
                    "null-authorization or scientific-input-policy mismatch"
                ),
            },
            "stable_reason_ids": list(STABLE_REASON_IDS),
            "native_failure_reason_ids": list(NATIVE_FAILURE_REASONS),
            "scheduler_side_prelaunch_reason_ids": list(
                PRELAUNCH_FAILURE_REASONS
            ),
            "positive_terminal_reason_id": POSITIVE_TERMINAL_REASON,
            "reason_to_exit_code": dict(REASON_TO_EXIT_CODE),
            "scheduler_prelaunch_failure_rule": (
                "launch_nonce_already_consumed enters "
                "PRELAUNCH_REJECTED_FAIL_CLOSED in the stateful direct-parent "
                "launch owner before socketpair, request memfd or native "
                "process creation; therefore no native state or failure frame "
                "exists. If represented as a process/CLI sysexit it is 65."
            ),
            "global_first_failure_selection": {
                "rule": (
                    "validate in state_machine order, stop at the first failed "
                    "state, emit its one stable reason, and never perform later "
                    "observations to replace it"
                ),
                "ENTRY_EXACT": (
                    "actual_native_argv_mismatch before "
                    "actual_native_environment_not_empty"
                ),
                "PLATFORM_CAPABILITY": (
                    "platform_or_capability_unavailable before "
                    "inherited-descriptor inspection"
                ),
                "FD_SHAPE": (
                    "apply step5d_descriptor_inventory.ordered_failure_"
                    "selection exactly: inventory I/O, then successful "
                    "inventory set comparison, then FD 0..5 probes one at a "
                    "time with each probe's I/O before that probe's shape "
                    "comparison; stop on the first failure"
                ),
                "REQUEST_FRAME_AND_BODY": (
                    "request_frame_invalid before request_body_schema."
                    "validation_precedence; the latter is ordered exactly as "
                    "listed"
                ),
                "INITIAL_PEER": (
                    "peer credentials, then six-field process identity, then "
                    "executable/cmdline image; an I/O failure at each "
                    "observation maps to peer_identity_io_failure before that "
                    "observation's mismatch reason"
                ),
                "INITIAL_PROJECT_ROOT": (
                    "project_root_io_failure before "
                    "project_root_identity_mismatch, subject to authority_"
                    "model.project_root.failure_classification"
                ),
                "MONITOR_NONCE": (
                    "monitor_random_io_failure before challenge construction"
                ),
                "ACK": (
                    "control_protocol.ack_failure_precedence is exhaustive and "
                    "ordered"
                ),
                "FINAL_REVALIDATION": (
                    "peer six-field process identity, peer image, project "
                    "root, then final getppid; each observation's I/O reason "
                    "precedes its mismatch reason and later observations are "
                    "not performed after failure"
                ),
                "INTERNAL_ONLY": (
                    "authority_invariant_failure is permitted only after "
                    "peer/root binding and revalidation succeeded and no "
                    "external-input, I/O or policy reason applies"
                ),
            },
            "request_reason_selection": {
                "request_body_noncanonical": (
                    "strict UTF-8/JSON parse, duplicate key or canonical byte "
                    "reserialization failure"
                ),
                "request_schema_mismatch": (
                    "exact/nested keyset, JSON field type/range, fixed "
                    "schema/peer-role/root-project string or request-internal "
                    "relation mismatch; a forbidden added authority field is "
                    "an extra-key schema mismatch; semantic fields assigned "
                    "to later precedence steps are excluded"
                ),
                "request_digest_mismatch": (
                    "submitted_logical_argv_sha256 mismatch or a control "
                    "binding uses body/frame digest under the wrong name"
                ),
                "request_protocol_mismatch": (
                    "well-shaped protocol_sha256 is not the exact parent "
                    "protocol digest"
                ),
                "request_target_phase_mismatch": (
                    "target phase is not one of the three exact profiles or is "
                    "inconsistent with the exact logical argv mapping; "
                    "batch/task fields are excluded from this reason"
                ),
                "scheduler_identity_prefix_mismatch": (
                    "well-shaped six-field prefix violates exact project, "
                    "run-to-RC relation, batch or task semantic rules; "
                    "otherwise-valid run ID and launch marker values are "
                    "shape-only untrusted claims"
                ),
                "request_no_data_policy_mismatch": (
                    "well-shaped request_kind, active authorization or "
                    "scientific_input_policy violates the fixed peer-bound "
                    "no-data-stop values"
                ),
            },
            "mapping_completeness_rule": (
                "stable_reason_ids is exactly the disjoint union of native_"
                "failure_reason_ids, scheduler_side_prelaunch_reason_ids and "
                "positive_terminal_reason_id, and reason_to_exit_code has "
                "exactly the same keyset"
            ),
            "stderr_is_not_authority": True,
        },
        "false_authority_fields": false_authority_fields,
    }


__all__ = [
    "ACK_DECISION",
    "ACK_FAILURE_PRECEDENCE",
    "ACK_KEYS",
    "ACK_SCHEMA",
    "CHALLENGE_KEYS",
    "CHALLENGE_SCHEMA",
    "CONTROL_ACK_SEQUENCE",
    "CONTROL_CHALLENGE_SEQUENCE",
    "CONTROL_DEADLINE_MS",
    "CONTROL_FAILURE_SEQUENCE",
    "CONTROL_RECEIPT_SEQUENCE",
    "FAILURE_KEYS",
    "FAILURE_DIGEST_KNOWNNESS",
    "FAILURE_REASON_STATES",
    "FAILURE_SCHEMA",
    "NATIVE_FAILURE_REASONS",
    "PARENT_PROTOCOL_SHA256",
    "PEER_ROLE",
    "PHASE_PROFILES",
    "POSITIVE_TERMINAL_REASON",
    "PRELAUNCH_FAILURE_REASONS",
    "PROJECT_ROOT_KEYS",
    "REASON_TO_EXIT_CODE",
    "RECEIPT_KEYS",
    "RECEIPT_RESULT",
    "RECEIPT_SCHEMA",
    "REQUEST_KEYS",
    "REQUEST_KIND",
    "REQUEST_SCHEMA",
    "REQUEST_VALIDATION_PRECEDENCE",
    "RequestDigests",
    "SCIENTIFIC_INPUT_POLICY",
    "STATE_MACHINE",
    "STABLE_REASON_IDS",
    "Step5DProtocolViolation",
    "alignment_projection",
    "build_ack_body",
    "build_challenge_body",
    "build_failure_body",
    "build_receipt_body",
    "build_request_body",
    "canonical_json_bytes",
    "encode_ack_frame",
    "encode_failure_frame",
    "encode_receipt_frame",
    "encode_request_frame",
    "parse_canonical_json_object",
    "parse_challenge_frame",
    "parse_failure_frame",
    "parse_receipt_frame",
    "request_digests",
    "scheduler_prefix",
    "validate_request_body",
]
