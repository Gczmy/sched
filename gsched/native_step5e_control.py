"""One R-delegated scheduler no-data exchange; no process or endpoint creation.

R alone supplies the startup socket and, after READY, the request endpoint.
This sender does not receive V's result, does not wait for V, and cannot accept
an external verification result. Its normal return means only STOP completed.
"""
from __future__ import annotations

import os
import secrets
import socket
import threading

from . import _native_step5d_linux as linux
from . import native_step5d_protocol as parent
from . import native_step5e_protocol as p
from . import native_step5e_transport as transport
from .native_deployment import NativeDeployment, require_deployment


class _NonceOwner:
    __slots__ = ('pid', 'lock', 'consumed')

    def __init__(self):
        self.pid = os.getpid()
        self.lock = threading.Lock()
        self.consumed = set()

    def claim(self, nonce: str) -> None:
        # Check before acquiring a lock that a forked child could inherit held.
        if os.getpid() != self.pid:
            p.fail('startup_protocol_failure')
        p.sha(nonce, 'startup_protocol_failure')
        with self.lock:
            if nonce in self.consumed:
                p.fail('startup_protocol_failure')
            self.consumed.add(nonce)


class NativeStep5ESender:
    """PID-bound, single-use startup/request/ACK/STOP state machine.

    The trusted R bootstrap captures expected_root and expected_session before
    fork. attempt_deadline_ns is its monotonic t_setup+25s safety bound; R's
    independent5s setup/20s acceptance guards remain the acceptance authority.
    All sockets passed into run become owned by this object, even on rejection.
    """
    __slots__ = ('_pid', '_root', '_session', '_state', '_failure', '_init_bytes',
                 '_nonce_owner', '_nonce', '_request_frame', '_startup', '_channel', '_deployment')

    def __init__(self, *, deployment: NativeDeployment, expected_root: dict, expected_session: str):
        self._deployment = require_deployment(deployment)
        transport.capability()
        p.credentials(expected_root, 'startup_protocol_failure')
        p.session(expected_session, 'startup_protocol_failure')
        self._pid = os.getpid()
        self._root = p.parse_json(p.canonical(expected_root), 'startup_protocol_failure')
        self._session = expected_session
        self._state = 'NEW'
        self._failure = None
        self._init_bytes = None
        self._nonce_owner = None
        self._nonce = None
        self._request_frame = None
        self._startup = self._channel = None

    def _require(self, state: str) -> None:
        if os.getpid() != self._pid or os.getppid() != self._root['pid']:
            self._reject('startup_protocol_failure')
        if self._failure is not None:
            raise self._failure
        if self._state != state:
            self._reject('startup_protocol_failure')

    def _reject(self, reason: str) -> None:
        if self._failure is None:
            self._failure = p.Step5EProtocolViolation(reason)
        self._state = 'FAILED'
        raise self._failure

    def _receive_startup(self, name: str, deadline: int, *, rights_count=0):
        rights = ()
        try:
            value, rights, _ = transport.receive_frame(self._startup, deadline_ns=deadline,
                expected_credentials=self._root, rights_count=rights_count,
                io_reason='startup_protocol_failure')
            if value.get('schema') == p.STARTUP_SCHEMAS['ABORT']:
                p.validate_startup(value, 'ABORT', expected_session=self._session)
                self._reject('startup_protocol_failure')
            p.validate_startup(value, name, expected_session=self._session,
                               expected_launch_nonce=self._nonce, deployment=self._deployment)
            return value, rights
        except p.Step5EProtocolViolation:
            transport._close_rights(rights)
            self._reject('startup_protocol_failure')
        except BaseException:
            transport._close_rights(rights)
            raise

    def _send_startup(self, name: str, deadline: int):
        fields = {'launch_nonce': self._nonce} if name in ('READY', 'ADOPTED') else {}
        transport.send_frame(self._startup, p.startup_message(name, self._session, **fields),
                             deadline_ns=deadline, io_reason='startup_protocol_failure')

    def _adopt(self, descriptor: int) -> None:
        channel = None
        try:
            channel = socket.socket(fileno=descriptor)  # No new endpoint or descriptor.
            channel.set_inheritable(False)
            channel.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
            transport.check_channel(channel)
            self._channel = channel
        except BaseException:
            if channel is not None:
                channel.close()
            else:
                transport._close_rights((descriptor,))
            raise

    def run(self, startup: socket.socket, *, attempt_deadline_ns: int) -> None:
        try:
            self._require('NEW')
        except BaseException:
            # Take responsibility for this invocation's transferred descriptor
            # without replacing any resource from the earlier invocation.
            if startup is not self._startup:
                try:
                    startup.close()
                except OSError:
                    pass
            raise
        self._startup = startup
        try:
            transport.check_channel(startup)
            startup.set_inheritable(False)
            init, _ = self._receive_startup('INIT', attempt_deadline_ns)
            self._init_bytes = p.canonical(init)  # Immutable; no caller dictionary alias.
            self._state = 'INITIALIZED'
            self._nonce_owner = _NonceOwner()  # Lifetime begins inside S after authenticated INIT.
            self._nonce = secrets.token_hex(32)
            self._nonce_owner.claim(self._nonce)  # Before READY and R request-endpoint creation.
            self._send_startup('READY', attempt_deadline_ns)
            self._state = 'READY'
            _, descriptors = self._receive_startup('ADOPT', attempt_deadline_ns, rights_count=1)
            self._adopt(descriptors[0])
            self._send_startup('ADOPTED', attempt_deadline_ns)
            self._state = 'ADOPTED'
            self._receive_startup('GO', attempt_deadline_ns)
            self._require('ADOPTED')
            self._state = 'GO'
            init = p.parse_json(self._init_bytes, 'startup_protocol_failure')
            identity = linux.observe_process_identity(self._pid)
            transport.check_deadline(attempt_deadline_ns, 'control_io_or_deadline_failure')
            peer = dict(peer_role=parent.PEER_ROLE,
                        credentials=dict(pid=self._pid, uid=os.getuid(), gid=os.getgid()),
                        process_identity=identity)
            body = parent.build_request_body(target_phase_profile=init['phase'],
                deployment=self._deployment,
                scheduler_identity_prefix=init['scheduler_identity_prefix'], launch_nonce=self._nonce,
                control_peer_expectation=peer, project_root_expectation=init['root_identity'])
            self._request_frame = parent.encode_request_frame(body, deployment=self._deployment)
            envelope = p.request_envelope(self._session, init['anchor_nonce'], self._request_frame,
                                          deployment=self._deployment)
            transport.send_frame(self._channel, envelope, deadline_ns=attempt_deadline_ns)
            self._state = 'REQUEST_SENT'
            # V is created after S: INIT cannot contain its future PID. S only
            # echoes on the endpoint supplied by authenticated R. The observed
            # challenge peer here is not promoted to external V authentication.
            challenge, _, _ = transport.receive_frame(self._channel, deadline_ns=attempt_deadline_ns,
                                                        expected_credentials=None)
            ack = p.ack_for(challenge, session_id=self._session, anchor_nonce=init['anchor_nonce'],
                            request_sha256=p.digest(self._request_frame))
            self._require('REQUEST_SENT')
            transport.send_frame(self._channel, ack, deadline_ns=attempt_deadline_ns)
            transport.shutdown_write(self._channel, deadline_ns=attempt_deadline_ns)
            self._state = 'ACK_SHUTDOWN'
            self._receive_startup('STOP', attempt_deadline_ns)
            self._require('ACK_SHUTDOWN')
            self._state = 'STOPPED'
        except p.Step5EProtocolViolation as exc:
            self._reject(exc.reason)
        except (OSError, linux.Step5DLinuxError, parent.Step5DProtocolViolation) as exc:
            self._reject('startup_protocol_failure' if self._state in ('NEW', 'INITIALIZED', 'READY', 'ADOPTED')
                         else 'control_io_or_deadline_failure')
        finally:
            self.close()

    def close(self) -> None:
        # Only close this process's descriptor table; a copied owner is never
        # permission to manipulate the original parent's protocol state.
        for attribute in ('_channel', '_startup'):
            channel = getattr(self, attribute)
            if channel is not None:
                setattr(self, attribute, None)
                try:
                    channel.close()
                except OSError:
                    pass
