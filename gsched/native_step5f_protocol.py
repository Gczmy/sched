"""Step5F isolated request encoding; no daemon integration or production authority.

The external R chooses every expectation. This owner only consumes a nonce and
encodes a fixed request once within the creating process.
"""
from __future__ import annotations

import hashlib
import json
import os
import re

CONTRACT_SHA256 = 'd64dd6a07dafc9855515e3f2a05903b206a2a028c250980d991695e9fda9cef1'
PROFILE = 'trusted_bootstrap_guarded_generated_phase/v6'
SCHEMA = 'm2b_step5f_scheduler_request/v1'
PHASES = ('preparation', 'raw_collection', 'aggregation')
SCHEDULER_KEYS = ('SCHED_BATCH_ID', 'SCHED_LAUNCH_MARKER', 'SCHED_PROJECT',
                  'SCHED_RC_PREFIX', 'SCHED_RUN_ID', 'SCHED_TASK_ID', 'SCHED_EXEC_ATTESTATION_SHA256')
REQUEST_KEYS = frozenset(('schema', 'main_revision', 'scheduler_revision', 'nonce', 'session_id',
                         'phase', 'phase_sha256', 'runtime_manifest_sha256', 'prefix', 'poison_environment'))
_REGISTRY_PID = os.getpid()
_CONSUMED: set[str] = set()


class Step5FProtocolViolation(ValueError):
    pass


def require(value, reason='input_invalid'):
    if not value:
        raise Step5FProtocolViolation(reason)


def token(value):
    require(type(value) is str and re.fullmatch('[A-Za-z0-9-]{1,128}', value) is not None)
    return value


def hex_value(value, length=64):
    require(type(value) is str and re.fullmatch('[0-9a-f]{'+str(length)+'}', value) is not None)
    return value


def prefix(phase, run_id, launch_marker):
    require(type(phase) is str and phase in PHASES)
    token(run_id)
    token(launch_marker)
    return dict(SCHED_BATCH_ID='mpcotsf-cpu-m2b-step5d-peer-bound-stop-v1-'+phase,
                SCHED_TASK_ID='m2b_step5d_peer_bound_stop_'+phase, SCHED_PROJECT='mpcotsf',
                SCHED_RUN_ID=run_id, SCHED_LAUNCH_MARKER=launch_marker,
                SCHED_RC_PREFIX=hashlib.sha256(run_id.encode('ascii')).hexdigest()[:24])


def poison_environment():
    return {key: 'STEP5F-POISON-'+str(index) for index, key in enumerate(SCHEDULER_KEYS)}


def validate_request(value):
    require(type(value) is dict and value.keys() == REQUEST_KEYS)
    require(type(value['schema']) is str and value['schema'] == SCHEMA and type(value['phase']) is str and value['phase'] in PHASES)
    for key in ('main_revision', 'scheduler_revision'):
        hex_value(value[key], 40)
    for key in ('nonce', 'phase_sha256', 'runtime_manifest_sha256'):
        hex_value(value[key])
    token(value['session_id'])
    p = value['prefix']
    require(type(p) is dict and p.keys() == set(SCHEDULER_KEYS[:-1]))
    require(all(type(k) is str and type(v) is str for k,v in p.items()))
    require(p == prefix(value['phase'], p['SCHED_RUN_ID'], p['SCHED_LAUNCH_MARKER']))
    require(type(value['poison_environment']) is dict and value['poison_environment'] == poison_environment())
    require(all(type(k) is str and type(v) is str for k,v in value['poison_environment'].items()))
    return value


def canonical_request(value):
    """Pure reusable encoding; this helper does not consume or grant authority."""
    validate_request(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('ascii')


def parse_request(raw):
    require(type(raw) is bytes and 0 < len(raw) <= 1048576)

    def pairs(items):
        out = {}
        for key, value in items:
            require(key not in out)
            out[key] = value
        return out

    try:
        value = json.loads(raw.decode('ascii'), object_pairs_hook=pairs)
        require(canonical_request(value) == raw)
    except (UnicodeError, json.JSONDecodeError, TypeError, OverflowError, RecursionError) as exc:
        raise Step5FProtocolViolation('input_invalid') from exc
    return value


class RequestOwner:
    def __init__(self):
        self._pid = os.getpid()
        self._used = False

    def encode_once(self, value):
        require(os.getpid() == self._pid == _REGISTRY_PID, 'fork_owner_rejected')
        require(not self._used, 'request_already_consumed')
        # Freeze mutable input by a pure validation/encoding round-trip before
        # consumption; no callback or resource acquisition occurs after consume.
        fixed = parse_request(canonical_request(value))
        nonce = fixed['nonce']
        require(nonce not in _CONSUMED, 'nonce_already_consumed')
        _CONSUMED.add(nonce)
        self._used = True
        return canonical_request(fixed)
