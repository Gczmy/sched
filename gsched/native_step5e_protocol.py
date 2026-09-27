"""Frozen Step5E serialization for the delegated no-data sender.

Pure byte/schema operations: no FD creation, process observation or authority.
The native verifier and external R must independently enforce their own gates.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import re
import stat
import struct
from typing import Any

from . import native_step5d_protocol as parent
from .native_deployment import NativeDeployment, require_deployment

CONTRACT_SHA256 = '72702e8c1db9e55f6e3c74456d1e46857b13968a1a2a4d5d65a5bb7c58d55265'
FRAME_PAYLOAD_MAX = 196608
EMBEDDED_FRAME_MAX = 131096
ANCHOR_MAX = 1048576
FILE_MAX = 2097152
FILES_TOTAL_MAX = 16777216
SCOPE = 'generated_no_data_session_only'
DECISION = 'acknowledge_no_data_external_verification_only'
REQUEST_SCHEMA = 'm2b_step5e_scheduler_request/v1'
CHALLENGE_SCHEMA = 'm2b_step5e_external_challenge/v1'
ACK_SCHEMA = 'm2b_step5e_scheduler_ack/v1'
SUCCESS_SCHEMA = 'm2b_step5e_external_stop/v1'
ANCHOR_SCHEMA = 'm2b_step5e_external_anchor/v1'
CONTROL_KEYS = frozenset(('schema', 'session_id', 'anchor_nonce', 'monitor_nonce',
                         'step5d_request_frame_sha256', 'anchor_sha256',
                         'retained_code_manifest_sha256', 'decision'))
REQUEST_KEYS = frozenset(('schema', 'session_id', 'anchor_nonce',
                         'step5d_request_frame_base64'))
ANCHOR_KEYS = frozenset(('schema', 'scope', 'protocol_sha256', 'session_id',
                        'anchor_nonce', 'phase', 'scheduler_identity_prefix',
                        'issuer_process_identity', 'subject_process_identity',
                        'subject_credentials', 'root_identity', 'code_manifest',
                        'authorization'))
STARTUP_SCHEMAS = {name: 'm2b_step5e_start_' + name.lower() + '/v1'
                   for name in ('INIT', 'READY', 'ADOPT', 'ADOPTED', 'GO', 'STOP', 'ABORT')}
STARTUP_KEYS = {
    name: frozenset(('schema', 'session_id')) | (
        frozenset(('anchor_nonce', 'phase', 'scheduler_identity_prefix', 'root_identity'))
        if name == 'INIT' else frozenset(('launch_nonce',))
        if name in ('READY', 'ADOPT', 'ADOPTED') else frozenset())
    for name in STARTUP_SCHEMAS
}
OBSERVATIONS = dict(delegated_subject_matched=True, scheduler_prefix_matched=True,
                    retained_bytes_verified=True, source_endpoints_equal=True,
                    subject_endpoints_equal=True, continuous_monitoring_proved=False,
                    python_loaded_code_proved=False)
AUTHORITY = dict(scheduler_role_authority_claimed=False, formal_ready=False,
                 scientific_result=False, logical_python_executed=False,
                 external_formal_authority_claimed=False)


class Step5EProtocolViolation(ValueError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def fail(reason: str) -> None:
    raise Step5EProtocolViolation(reason)


def canonical(value: Any) -> bytes:
    try:
        return parent.canonical_json_bytes(value)
    except (parent.Step5DProtocolViolation, TypeError, ValueError, RecursionError):
        fail('control_message_invalid')


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def exact_object(value: Any, keys: frozenset[str], reason: str) -> dict:
    if type(value) is not dict or value.keys() != keys:
        fail(reason)
    return value


def integer(value: Any, minimum: int, maximum: int, reason: str) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        fail(reason)
    return value


def sha(value: Any, reason: str) -> str:
    if type(value) is not str or re.fullmatch('[0-9a-f]{64}', value) is None:
        fail(reason)
    return value


def session(value: Any, reason: str) -> str:
    if type(value) is not str or re.fullmatch('[A-Za-z0-9-]{1,128}', value) is None:
        fail(reason)
    return value


def credentials(value: Any, reason: str) -> dict:
    exact_object(value, parent.CREDENTIAL_KEYS, reason)
    integer(value['pid'], 1, 2147483647, reason)
    for key in ('uid', 'gid'):
        integer(value[key], 0, 4294967295, reason)
    return value


def process_identity(value: Any, reason: str) -> dict:
    exact_object(value, parent.PROCESS_IDENTITY_KEYS, reason)
    for key in ('pid', 'parent_pid', 'process_group_id'):
        integer(value[key], 1, 2147483647, reason)
    integer(value['start_ticks'], 1, 18446744073709551615, reason)
    for key in ('boot_id_sha256', 'cgroup_identity_sha256'):
        sha(value[key], reason)
    return value


def path_text(value: Any, *, absolute: bool, reason: str) -> str:
    if type(value) is not str or (not absolute and '\\' in value) or '\x00' in value:
        fail(reason)
    try:
        raw = value.encode('utf-8', 'strict')
    except UnicodeError:
        fail(reason)
    if not 1 <= len(raw) <= 4095 or value.startswith('/') != absolute:
        fail(reason)
    parts = raw[1:].split(b'/') if absolute else raw.split(b'/')
    if any(part in (b'', b'.', b'..') or len(part) > 255 for part in parts):
        fail(reason)
    return value


def root_identity(value: Any, reason: str, *, deployment: NativeDeployment) -> dict:
    exact_object(value, parent.PROJECT_ROOT_KEYS, reason)
    if value['project'] != require_deployment(deployment).project:
        fail(reason)
    path_text(value['canonical_absolute_path'], absolute=True, reason=reason)
    integer(value['st_dev'], 0, 18446744073709551615, reason)
    integer(value['st_ino'], 1, 18446744073709551615, reason)
    mode = integer(value['st_mode'], 0, 4294967295, reason)
    if not stat.S_ISDIR(mode):
        fail(reason)
    for key in ('st_uid', 'st_gid'):
        integer(value[key], 0, 4294967295, reason)
    return value


def prefix(value: Any, phase: Any, reason: str, *, deployment: NativeDeployment) -> dict:
    exact_object(value, parent.SCHEDULER_PREFIX_KEYS, reason)
    if type(phase) is not str or phase not in parent.PHASE_PROFILES:
        fail(reason)
    try:
        expected = parent.scheduler_prefix(deployment=deployment, target_phase_profile=phase,
            run_id=value['SCHED_RUN_ID'], launch_marker=value['SCHED_LAUNCH_MARKER'])
    except (parent.Step5DProtocolViolation, TypeError, ValueError):
        fail(reason)
    if canonical(value) != canonical(expected):
        fail(reason)
    return value


def parse_json(payload: bytes, reason: str) -> dict:
    try:
        return parent.parse_canonical_json_object(payload)
    except (parent.Step5DProtocolViolation, ValueError, TypeError, RecursionError):
        fail(reason)


def encode_frame(value: dict) -> bytes:
    payload = canonical(value)
    if not 1 <= len(payload) <= FRAME_PAYLOAD_MAX:
        fail('control_message_invalid')
    return struct.pack('!I', len(payload)) + payload


def parse_frame(frame: bytes, reason: str = 'control_message_invalid') -> dict:
    if type(frame) is not bytes or len(frame) < 5:
        fail(reason)
    length, = struct.unpack('!I', frame[:4])
    if not 1 <= length <= FRAME_PAYLOAD_MAX or length != len(frame) - 4:
        fail(reason)
    return parse_json(frame[4:], reason)


def startup_message(name: str, session_id: str, *, deployment: NativeDeployment | None = None, **fields: Any) -> dict:
    if name not in STARTUP_SCHEMAS:
        fail('startup_protocol_failure')
    value = dict(schema=STARTUP_SCHEMAS[name], session_id=session_id, **fields)
    validate_startup(value, name, expected_session=session_id, deployment=deployment)
    return value


def validate_startup(value: dict, name: str, *, expected_session: str,
                     expected_launch_nonce: str | None = None, deployment: NativeDeployment | None = None) -> dict:
    reason = 'startup_protocol_failure'
    if name not in STARTUP_SCHEMAS:
        fail(reason)
    exact_object(value, STARTUP_KEYS[name], reason)
    if value['schema'] != STARTUP_SCHEMAS[name] or session(value['session_id'], reason) != expected_session:
        fail(reason)
    if name == 'INIT':
        sha(value['anchor_nonce'], reason)
        prefix(value['scheduler_identity_prefix'], value['phase'], reason, deployment=deployment)
        root_identity(value['root_identity'], reason, deployment=deployment)
    if name in ('READY', 'ADOPT', 'ADOPTED'):
        nonce = sha(value['launch_nonce'], reason)
        if expected_launch_nonce is not None and nonce != expected_launch_nonce:
            fail(reason)
    return value


def parse_embedded(frame: bytes, *, deployment: NativeDeployment) -> dict:
    reason = 'request_noncanonical_or_frame_invalid'
    if type(frame) is not bytes or not 1 <= len(frame) <= EMBEDDED_FRAME_MAX:
        fail(reason)
    try:
        parent.request_digests(frame, deployment=deployment)
        decoded = parent.decode_native_launch_frame(frame)
        return parent.validate_request_body(decoded.opaque_body, deployment=deployment)
    except parent.Step5DProtocolViolation as exc:
        fail('request_contract_or_profile_mismatch' if exc.exit_code == 78 else reason)
    except (ValueError, TypeError):
        fail(reason)


def request_envelope(session_id: str, anchor_nonce: str, frame: bytes, *, deployment: NativeDeployment) -> dict:
    reason = 'request_noncanonical_or_frame_invalid'
    session(session_id, reason); sha(anchor_nonce, reason); parse_embedded(frame, deployment=deployment)
    return dict(schema=REQUEST_SCHEMA, session_id=session_id, anchor_nonce=anchor_nonce,
                step5d_request_frame_base64=base64.b64encode(frame).decode('ascii'))


def parse_request(frame: bytes, *, deployment: NativeDeployment) -> tuple[dict, bytes, dict]:
    reason = 'request_noncanonical_or_frame_invalid'
    value = parse_frame(frame, reason)
    exact_object(value, REQUEST_KEYS, reason)
    if value['schema'] != REQUEST_SCHEMA:
        fail(reason)
    session(value['session_id'], reason); sha(value['anchor_nonce'], reason)
    encoded = value['step5d_request_frame_base64']
    if type(encoded) is not str:
        fail(reason)
    try:
        embedded = base64.b64decode(encoded.encode('ascii'), validate=True)
    except (UnicodeError, ValueError, binascii.Error):
        fail(reason)
    if base64.b64encode(embedded).decode('ascii') != encoded:
        fail(reason)
    return value, embedded, parse_embedded(embedded, deployment=deployment)


def validate_control(value: dict, *, ack: bool = False) -> dict:
    reason = 'control_message_invalid'
    exact_object(value, CONTROL_KEYS, reason)
    if value['schema'] != (ACK_SCHEMA if ack else CHALLENGE_SCHEMA) or value['decision'] != DECISION:
        fail(reason)
    session(value['session_id'], reason)
    for key in CONTROL_KEYS - {'schema', 'session_id', 'decision'}:
        sha(value[key], reason)
    return value


def ack_for(challenge: dict, *, session_id: str, anchor_nonce: str,
            request_sha256: str) -> dict:
    validate_control(challenge)
    if (challenge['session_id'], challenge['anchor_nonce'], challenge['step5d_request_frame_sha256']) != (
            session_id, anchor_nonce, request_sha256):
        fail('control_message_invalid')
    return dict(challenge, schema=ACK_SCHEMA)


def validate_ack(value: dict, challenge: dict) -> dict:
    validate_control(challenge); validate_control(value, ack=True)
    if canonical(value) != canonical(dict(challenge, schema=ACK_SCHEMA)):
        fail('control_message_invalid')
    return value


def validate_manifest(value: Any) -> list:
    reason = 'anchor_schema_mismatch'
    if type(value) is not list or not 1 <= len(value) <= 64:
        fail(reason)
    previous = b''
    total = 0
    for item in value:
        exact_object(item, frozenset(('relative_path', 'size_bytes', 'sha256', 'role')), reason)
        raw = path_text(item['relative_path'], absolute=False, reason=reason).encode('utf-8')
        if raw <= previous or item['role'] not in ('reviewed_main_source', 'reviewed_scheduler_source'):
            fail(reason)
        previous = raw
        total += integer(item['size_bytes'], 0, FILE_MAX, reason)
        sha(item['sha256'], reason)
    if total > FILES_TOTAL_MAX:
        fail(reason)
    return value


def parse_anchor(payload: bytes, *, deployment: NativeDeployment) -> dict:
    reason = 'anchor_schema_mismatch'
    if type(payload) is not bytes or not 1 <= len(payload) <= ANCHOR_MAX:
        fail(reason)
    value = parse_json(payload, 'anchor_noncanonical')
    exact_object(value, ANCHOR_KEYS, reason)
    if value['schema'] != ANCHOR_SCHEMA:
        fail(reason)
    for key in ('scope', 'protocol_sha256'):
        if type(value[key]) is not str:
            fail(reason)
    sha(value['protocol_sha256'], reason)
    session(value['session_id'], reason); sha(value['anchor_nonce'], reason)
    prefix(value['scheduler_identity_prefix'], value['phase'], reason, deployment=deployment)
    issuer = process_identity(value['issuer_process_identity'], reason)
    subject = process_identity(value['subject_process_identity'], reason)
    creds = credentials(value['subject_credentials'], reason)
    root_identity(value['root_identity'], reason, deployment=deployment); validate_manifest(value['code_manifest'])
    if creds['pid'] != subject['pid']:
        fail(reason)
    if value['scope'] != SCOPE or value['protocol_sha256'] != CONTRACT_SHA256 or value['authorization'] is not None:
        fail('anchor_contract_mismatch')
    if subject['parent_pid'] != issuer['pid']:
        fail('anchor_identity_mismatch')
    return value


def match_external(anchor: dict, envelope: dict, embedded: dict) -> None:
    if (envelope['session_id'], envelope['anchor_nonce']) != (anchor['session_id'], anchor['anchor_nonce']):
        fail('anchor_identity_mismatch')
    for actual, expected in (
        (embedded['target_phase_profile'], anchor['phase']),
        (embedded['scheduler_identity_prefix'], anchor['scheduler_identity_prefix']),
        (embedded['control_peer_expectation']['credentials'], anchor['subject_credentials']),
        (embedded['control_peer_expectation']['process_identity'], anchor['subject_process_identity']),
        (embedded['project_root_expectation'], anchor['root_identity']),
    ):
        if canonical(actual) != canonical(expected):
            fail('external_identity_mismatch')
