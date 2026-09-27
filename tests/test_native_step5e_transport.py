"""Real Linux ancillary/ownership checks; generated sockets only."""
import os
import socket
import sys
import time

import pytest

from gsched import native_step5e_control as control
from gsched import native_step5e_protocol as p
from gsched import native_step5e_transport as transport


def credentials(pid=None):
    return dict(pid=os.getpid() if pid is None else pid, uid=os.getuid(), gid=os.getgid())


def deadline():
    return time.monotonic_ns() + 2000000000


@pytest.fixture
def pair():
    assert sys.platform.startswith('linux'), 'remote Linux job only; no skip'
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    for endpoint in (left, right):
        endpoint.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
    try:
        yield left, right
    finally:
        left.close(); right.close()


def test_adopt_enables_own_passcred_without_requiring_root_to_set_it(pair):
    verifier, subject = pair
    subject.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 0)
    owner = control.NativeStep5ESender(expected_root=credentials(os.getppid()), expected_session='test')
    try:
        owner._adopt(subject.detach())
        assert owner._channel.getsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED) == 1
        assert verifier.getsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED) == 1
        assert not owner._channel.get_inheritable()
    finally:
        owner.close()


@pytest.mark.parametrize('kind', ['malformed_frame', 'wrong_credentials', 'extra_rights', 'missing_rights'])
def test_startup_rejection_closes_rights_and_uses_startup_reason(pair, kind):
    sender, receiver = pair
    expected = credentials(os.getppid()) if kind == 'wrong_credentials' else credentials()
    owner = control.NativeStep5ESender(expected_root=expected, expected_session='test')
    owner._startup = receiver
    nonce = 'c' * 64
    owner._nonce = nonce
    source = os.open('/dev/null', os.O_RDONLY | os.O_CLOEXEC)
    before = set(os.listdir('/proc/self/fd'))
    try:
        if kind == 'malformed_frame':
            sender.sendall(b'\x00\x00\x00\x01{')
        else:
            message = p.startup_message('ADOPT', 'test', launch_nonce=nonce)
            rights = (source, source) if kind == 'extra_rights' else ()
            transport.send_frame(sender, message, deadline_ns=deadline(), rights=rights)
        count = 1 if kind in ('extra_rights', 'missing_rights') else 0
        with pytest.raises(p.Step5EProtocolViolation, match='^startup_protocol_failure$'):
            owner._receive_startup('ADOPT', deadline(), rights_count=count)
        assert set(os.listdir('/proc/self/fd')) == before
        assert owner._failure.reason == 'startup_protocol_failure'
    finally:
        os.close(source)
        owner.close()


@pytest.mark.parametrize('rejection', ['stopped_reentry', 'pid_guard'])
def test_rejected_invocation_closes_incoming_socket(pair, rejection):
    other, incoming = pair
    owner = control.NativeStep5ESender(expected_root=credentials(os.getppid()), expected_session='test')
    if rejection == 'stopped_reentry': owner._state = 'STOPPED'
    else: owner._pid += 1
    with pytest.raises(p.Step5EProtocolViolation, match='^startup_protocol_failure$'):
        owner.run(incoming, attempt_deadline_ns=deadline())
    assert incoming.fileno() == -1
    assert other.fileno() >= 0
    assert owner._startup is None


def test_wrong_credentials_precede_malformed_frame(pair):
    sender, receiver = pair
    sender.sendall(b'\x00\x00\x00\x01{')
    with pytest.raises(p.Step5EProtocolViolation, match='^control_credentials_mismatch$'):
        transport.receive_frame(receiver, deadline_ns=deadline(), expected_credentials=credentials(os.getppid()))


def test_real_frame_and_half_close(pair):
    sender, receiver = pair
    value = p.startup_message('GO', 'test')
    transport.send_frame(sender, value, deadline_ns=deadline())
    actual, rights, frame = transport.receive_frame(receiver, deadline_ns=deadline(), expected_credentials=credentials())
    assert actual == value and rights == () and frame == p.encode_frame(value)
    transport.shutdown_write(sender, deadline_ns=deadline())
    transport.require_eof(receiver, deadline_ns=deadline(), expected_credentials=credentials())


def test_credential_gated_trailing_byte_is_replay(pair):
    sender, receiver = pair
    sender.sendall(b'x')
    with pytest.raises(p.Step5EProtocolViolation, match='^control_replay$'):
        transport.require_eof(receiver, deadline_ns=deadline(), expected_credentials=credentials())


def test_nonce_replay_and_pid_guard_precede_lock():
    owner = control._NonceOwner()
    owner.claim('c' * 64)
    with pytest.raises(p.Step5EProtocolViolation, match='^startup_protocol_failure$'):
        owner.claim('c' * 64)
    owner.pid += 1
    owner.lock.acquire()
    try:
        with pytest.raises(p.Step5EProtocolViolation, match='^startup_protocol_failure$'):
            owner.claim('d' * 64)
    finally:
        owner.lock.release()
