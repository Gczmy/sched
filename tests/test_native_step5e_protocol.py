"""Frozen cross-repository vectors; run only in the isolated Linux CPU job.

M2B_STEP5E_CONTRACT_ROOT points to the reviewed main checkout. The surrounding
runner is responsible for exact source loading and external revision binding.
"""
import copy
import json
import os
from pathlib import Path
import struct

import pytest

from gsched import native_step5e_protocol as p


@pytest.fixture(scope='module')
def vectors():
    root = Path(os.environ['M2B_STEP5E_CONTRACT_ROOT']).resolve(strict=True)
    freeze = json.loads((root / 'scripts/contract/m2b_step5e_contract_freeze_v1.json').read_text())
    data = (root / freeze['vectors_path']).read_bytes()
    assert p.digest(data) == freeze['vectors_file_sha256']
    result = json.loads(data)
    assert result['machine_canonical_sha256'] == p.CONTRACT_SHA256 == freeze['machine_canonical_sha256']
    corrected_bytes = (root / 'scripts/contract/m2b_step5e_raw_collection_vector_erratum_v1.json').read_bytes()
    assert p.digest(corrected_bytes) == '8b7fefa8173b955a4013dcb1c051b308d7a8e9f3a8539a815920552cf3cc72aa'
    corrected = json.loads(corrected_bytes)
    assert corrected['source_vectors_file_sha256'] == freeze['vectors_file_sha256']
    assert corrected['machine_canonical_sha256'] == p.CONTRACT_SHA256
    result['raw_collection_erratum'] = corrected['vector']
    return result


@pytest.mark.parametrize('phase_index', range(3))
def test_frozen_phase_exchange(vectors, phase_index):
    v = vectors['raw_collection_erratum'] if phase_index == 1 else vectors['vectors'][phase_index]
    anchor = p.parse_anchor(v['anchor_canonical_utf8'].encode())
    envelope, frame, body = p.parse_request(bytes.fromhex(v['request']['packet_hex']))
    assert p.digest(frame) == v['step5d_frame_sha256']
    p.match_external(anchor, envelope, body)
    assert p.encode_frame(p.request_envelope(anchor['session_id'], anchor['anchor_nonce'], frame)).hex() == v['request']['packet_hex']
    challenge = p.validate_control(p.parse_frame(bytes.fromhex(v['challenge']['packet_hex'])))
    ack = p.ack_for(challenge, session_id=anchor['session_id'], anchor_nonce=anchor['anchor_nonce'], request_sha256=p.digest(frame))
    assert p.encode_frame(ack).hex() == v['ack']['packet_hex']
    assert p.validate_ack(ack, challenge) == ack
    for entry in v['startup']:
        message = p.parse_frame(bytes.fromhex(entry['wire']['packet_hex']))
        p.validate_startup(message, entry['name'], expected_session=anchor['session_id'],
                           expected_launch_nonce=body['launch_nonce'])
        assert p.encode_frame(message).hex() == entry['wire']['packet_hex']
    assert v['startup'][0]['wire']['canonical_utf8'].find(anchor['anchor_nonce']) >= 0


def test_original_raw_collection_vector_has_invalid_session(vectors):
    original = vectors['vectors'][1]
    assert json.loads(original['anchor_canonical_utf8'])['session_id'] == 'review-session-raw_collection'
    with pytest.raises(p.Step5EProtocolViolation, match='^anchor_schema_mismatch$'):
        p.parse_anchor(original['anchor_canonical_utf8'].encode())


@pytest.mark.parametrize('field', ['session_id', 'anchor_nonce'])
def test_external_envelope_binding_rejects(vectors, field):
    v = vectors['vectors'][0]
    anchor = p.parse_anchor(v['anchor_canonical_utf8'].encode())
    envelope, _, body = p.parse_request(bytes.fromhex(v['request']['packet_hex']))
    envelope[field] = 'f' * 64
    with pytest.raises(p.Step5EProtocolViolation, match='^anchor_identity_mismatch$'):
        p.match_external(anchor, envelope, body)


@pytest.mark.parametrize('key', sorted(p.parent.SCHEDULER_PREFIX_KEYS))
def test_every_prefix_field_is_external(vectors, key):
    v = vectors['vectors'][0]
    anchor = p.parse_anchor(v['anchor_canonical_utf8'].encode())
    envelope, _, body = p.parse_request(bytes.fromhex(v['request']['packet_hex']))
    body['scheduler_identity_prefix'][key] += '-substitution'
    with pytest.raises(p.Step5EProtocolViolation, match='^external_identity_mismatch$'):
        p.match_external(anchor, envelope, body)


@pytest.mark.parametrize('encoded', ['Zh==', 'Zg', 'Zg==\n', '_w==', 'Zg==='])
def test_noncanonical_base64_rejected(vectors, encoded):
    envelope = p.parse_frame(bytes.fromhex(vectors['vectors'][0]['request']['packet_hex']))
    envelope['step5d_request_frame_base64'] = encoded
    with pytest.raises(p.Step5EProtocolViolation, match='^request_noncanonical_or_frame_invalid$'):
        p.parse_request(p.encode_frame(envelope))


@pytest.mark.parametrize('value', [True, False, -1, 0, 2147483648, '12002', None])
def test_pid_types_do_not_coerce(vectors, value):
    anchor = json.loads(vectors['vectors'][0]['anchor_canonical_utf8'])
    anchor['subject_credentials']['pid'] = value
    with pytest.raises(p.Step5EProtocolViolation, match='^anchor_schema_mismatch$'):
        p.parse_anchor(p.canonical(anchor))


@pytest.mark.parametrize('key', sorted(p.CONTROL_KEYS - {'schema'}))
def test_ack_requires_every_echo_field(vectors, key):
    v = vectors['vectors'][0]
    challenge = p.parse_frame(bytes.fromhex(v['challenge']['packet_hex']))
    ack = p.parse_frame(bytes.fromhex(v['ack']['packet_hex']))
    ack[key] = 'f' * 64
    with pytest.raises(p.Step5EProtocolViolation, match='^control_message_invalid$'):
        p.validate_ack(ack, challenge)


@pytest.mark.parametrize('payload', [b'{"x":1,"x":2}', b'{"a":true, "b":1}', b'{"x":1.0}',
                                    b'{"x":-0}', b'{"x":"\\u0061"}', b'{"x":"\xff"}'])
def test_canonical_rejections(payload):
    with pytest.raises(p.Step5EProtocolViolation):
        p.parse_frame(struct.pack('!I', len(payload)) + payload)


def test_escaped_dynamic_maximum_is_not_rejected_by_ascii_estimate(vectors):
    case = next(v for v in vectors['boundary_vectors'] if v['id'] == 'escaped_dynamic_max')
    _, frame, body = p.parse_request(bytes.fromhex(case['request']['packet_hex']))
    assert len(frame) == case['embedded_frame_bytes']
    assert body['scheduler_identity_prefix']['SCHED_RUN_ID'] == '\x01' * 4096


@pytest.mark.parametrize('mutation', ['unknown', 'missing', 'manifest_order', 'manifest_bool_size', 'bad_path'])
def test_anchor_structure_rejected(vectors, mutation):
    anchor = json.loads(vectors['vectors'][0]['anchor_canonical_utf8'])
    if mutation == 'unknown': anchor['extra'] = None
    if mutation == 'missing': del anchor['authorization']
    if mutation == 'manifest_order': anchor['code_manifest'].reverse()
    if mutation == 'manifest_bool_size': anchor['code_manifest'][0]['size_bytes'] = False
    if mutation == 'bad_path': anchor['code_manifest'][0]['relative_path'] = '../escape'
    with pytest.raises(p.Step5EProtocolViolation, match='^anchor_schema_mismatch$'):
        p.parse_anchor(p.canonical(anchor))


def test_startup_cannot_rebind_session_or_consumed_nonce(vectors):
    v = vectors['vectors'][0]
    adopted = next(e for e in v['startup'] if e['name'] == 'ADOPTED')
    value = p.parse_frame(bytes.fromhex(adopted['wire']['packet_hex']))
    for kwargs in (dict(expected_session='wrong'),
                   dict(expected_session=value['session_id'], expected_launch_nonce='f' * 64)):
        with pytest.raises(p.Step5EProtocolViolation, match='^startup_protocol_failure$'):
            p.validate_startup(copy.deepcopy(value), 'ADOPTED', **kwargs)
