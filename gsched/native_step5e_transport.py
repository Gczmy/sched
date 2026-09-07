"""Linux stream frames with per-segment credentials and owned-rights cleanup.

This module adopts existing sockets only. It never creates endpoints or forks.
An expected peer is mandatory for R/S startup and R/V acceptance. S receiving
the challenge may observe an unpinned peer on its R-supplied capability; that
observation grants no verifier identity or acceptance authority.
"""
from __future__ import annotations

import array
import math
import os
import select
import socket
import struct
import sys
import time

from . import native_step5e_protocol as p

_CRED = struct.Struct('=iII')
_RIGHTS_MAX = 253  # Linux SCM_MAX_FD; truncation still rejects.
_ANCILLARY_BYTES = socket.CMSG_SPACE(_RIGHTS_MAX * array.array('i').itemsize) + socket.CMSG_SPACE(_CRED.size * 8)


def capability() -> None:
    if (not sys.platform.startswith('linux') or _CRED.size != 12
            or array.array('i').itemsize != 4
            or any(not hasattr(socket, name) for name in (
                'SCM_CREDENTIALS', 'MSG_CMSG_CLOEXEC', 'MSG_NOSIGNAL', 'MSG_DONTWAIT', 'SO_PASSCRED'))):
        p.fail('capability_unavailable')


def check_deadline(deadline_ns: int, reason: str) -> None:
    if type(deadline_ns) is not int or deadline_ns <= 0 or time.monotonic_ns() >= deadline_ns:
        p.fail(reason)


def check_channel(channel: socket.socket) -> None:
    capability()
    try:
        if (channel.family != socket.AF_UNIX
                or channel.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) != socket.SOCK_STREAM
                or channel.getsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED) != 1):
            p.fail('startup_protocol_failure')
        channel.getpeername()  # Must be connected; SO_PEERCRED is not an identity source.
    except OSError as exc:
        raise p.Step5EProtocolViolation('startup_protocol_failure') from exc


def _wait(channel, event: int, deadline_ns: int, reason: str) -> None:
    while True:
        check_deadline(deadline_ns, reason)
        poller = select.poll()
        poller.register(channel.fileno(), event)
        remaining_ms = max(1, math.ceil((deadline_ns - time.monotonic_ns()) / 1000000))
        try:
            events = poller.poll(remaining_ms)
        except InterruptedError:
            continue
        except OSError as exc:
            raise p.Step5EProtocolViolation(reason) from exc
        check_deadline(deadline_ns, reason)
        if events:
            return  # recv/send resolves POLLERR/HUP using the original deadline.


def _close_rights(descriptors) -> None:
    for descriptor in descriptors:
        try:
            os.close(descriptor)
        except OSError:
            pass  # Linux close is not retried; diagnostics cannot replace first failure.


def _controls(ancillary, flags):
    rights, peers = [], []
    invalid = bool(flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC))
    for level, kind, data in ancillary:
        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
            complete = len(data) - len(data) % 4
            installed = array.array('i')
            installed.frombytes(data[:complete])
            rights.extend(installed)
            invalid |= complete != len(data)
        elif level == socket.SOL_SOCKET and kind == socket.SCM_CREDENTIALS:
            if len(data) != _CRED.size:
                invalid = True
            else:
                pid, uid, gid = _CRED.unpack(data)
                peers.append(dict(pid=pid, uid=uid, gid=gid))
        else:
            invalid = True
    return rights, peers, invalid


def receive_segment(channel, maximum: int, *, deadline_ns: int, expected_credentials: dict | None,
                    allow_rights: bool = False, io_reason='control_io_or_deadline_failure'):
    """Return authenticated bytes and transferred rights; caller owns returned FDs.

    On every error this function closes all installed SCM_RIGHTS, even when an
    earlier deadline or ancillary defect is the selected failure.
    """
    capability()
    if expected_credentials is not None:
        p.credentials(expected_credentials, 'control_credentials_mismatch')
    rights = []
    try:
        while True:
            check_deadline(deadline_ns, io_reason)
            try:
                data, ancillary, flags, _ = channel.recvmsg(
                    maximum, _ANCILLARY_BYTES, socket.MSG_DONTWAIT | socket.MSG_CMSG_CLOEXEC)
                break
            except BlockingIOError:
                _wait(channel, select.POLLIN, deadline_ns, io_reason)
            except InterruptedError:
                continue
            except OSError as exc:
                raise p.Step5EProtocolViolation(io_reason) from exc
        # Extract all installed descriptors before even a deadline can reject.
        rights, peers, invalid = _controls(ancillary, flags)
        check_deadline(deadline_ns, io_reason)
        if invalid or (rights and not allow_rights):
            p.fail('control_message_invalid')
        if data:
            if not peers:
                p.fail('control_credentials_mismatch')
            for peer in peers:
                p.credentials(peer, 'control_credentials_mismatch')
                if peer != (expected_credentials if expected_credentials is not None else peers[0]):
                    p.fail('control_credentials_mismatch')
        elif rights:
            p.fail('control_message_invalid')
        return data, rights, peers[0] if peers else None
    except BaseException:
        _close_rights(rights)
        raise


def receive_frame(channel, *, deadline_ns: int, expected_credentials: dict | None,
                  rights_count: int = 0, io_reason='control_io_or_deadline_failure'):
    """One exact bounded frame; no read-ahead across the frame boundary."""
    frame, retained = bytearray(), []
    wanted = 4
    observed_peer = expected_credentials
    try:
        while len(frame) < wanted:
            data, received, peer = receive_segment(channel, wanted - len(frame),
                deadline_ns=deadline_ns, expected_credentials=observed_peer,
                allow_rights=rights_count > 0, io_reason=io_reason)
            retained.extend(received)
            if not data:
                p.fail(io_reason)
            if observed_peer is None:
                observed_peer = peer
            if len(retained) > rights_count:
                p.fail('control_message_invalid')
            frame.extend(data)
            if len(frame) == 4 and wanted == 4:
                payload_size, = struct.unpack('!I', frame)
                if not 1 <= payload_size <= p.FRAME_PAYLOAD_MAX:
                    p.fail('control_message_invalid')
                wanted = 4 + payload_size
        if len(retained) != rights_count:
            p.fail('control_message_invalid')
        value = p.parse_frame(bytes(frame))
        check_deadline(deadline_ns, io_reason)
        return value, tuple(retained), bytes(frame)
    except BaseException:
        _close_rights(retained)
        raise


def send_frame(channel, value: dict, *, deadline_ns: int, rights: tuple[int, ...] = (),
               io_reason='control_io_or_deadline_failure') -> None:
    capability()
    frame = p.encode_frame(value)
    ancillary = [(socket.SOL_SOCKET, socket.SCM_CREDENTIALS,
                  _CRED.pack(os.getpid(), os.getuid(), os.getgid()))]
    if rights:
        ancillary.append((socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', rights)))
    offset = 0
    while offset < len(frame):
        check_deadline(deadline_ns, io_reason)
        try:
            sent = channel.sendmsg([memoryview(frame)[offset:]], ancillary if offset == 0 else [],
                                   socket.MSG_DONTWAIT | socket.MSG_NOSIGNAL)
        except BlockingIOError:
            _wait(channel, select.POLLOUT, deadline_ns, io_reason)
            continue
        except InterruptedError:
            continue
        except OSError as exc:
            raise p.Step5EProtocolViolation(io_reason) from exc
        check_deadline(deadline_ns, io_reason)
        if sent <= 0:
            p.fail(io_reason)
        offset += sent


def shutdown_write(channel, *, deadline_ns: int, io_reason='control_io_or_deadline_failure') -> None:
    while True:
        check_deadline(deadline_ns, io_reason)
        try:
            channel.shutdown(socket.SHUT_WR)
            break
        except InterruptedError:
            continue
        except OSError as exc:
            raise p.Step5EProtocolViolation(io_reason) from exc
    check_deadline(deadline_ns, io_reason)


def require_eof(channel, *, deadline_ns: int, expected_credentials: dict,
                io_reason='control_io_or_deadline_failure') -> None:
    data, _, _ = receive_segment(channel, 1, deadline_ns=deadline_ns,
                                expected_credentials=expected_credentials, io_reason=io_reason)
    if data:
        p.fail('control_replay')
