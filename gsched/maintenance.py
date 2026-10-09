"""Cooperative upgrade fence, independent of the database and submission lock.

Unaware older writers are not fenced: callers must quiesce them before opening
an upgrade window. The stable lock inode is never replaced by rollback.
"""
from __future__ import annotations

import functools
import os
import time
from contextlib import contextmanager
from contextvars import ContextVar

_held = ContextVar("sched_maintenance_lock", default=None)
_authorized = ContextVar("sched_maintenance_actor", default=None)
LOCK_TIMEOUT = 5.0


def directory():
    from . import state
    from .config import default_state_dir
    # Resolve configuration before host_dir reads the runtime root. Respect
    # daemon's pinned/last-good binding instead of hot-switching a cold root.
    state.hostname()
    # host_dir validates the node; keep the fence outside the restored tree.
    node = os.path.basename(state.host_dir())
    return os.path.join(default_state_dir(), ".snapshot-control", node)


def window_path():
    return os.path.join(directory(), "window.json")


def _check():
    from . import state
    if os.path.lexists(window_path()) and _authorized.get() != (os.getpid(), directory()):
        raise state.SubmissionBlocked("upgrade maintenance window is open; use snapshot close before writes or daemon start")


@contextmanager
def gate(*, exclusive=False):
    from . import state
    try:
        import fcntl
    except ImportError as error:
        raise state.StateError("maintenance writes require POSIX flock") from error
    path = directory()
    held = _held.get()
    if held is not None and held[0] == os.getpid():
        if held[2] != path:
            raise state.StateError("state binding changed during a fenced writer operation")
        if exclusive and not held[1]:
            raise state.StateError("cannot upgrade an active writer lock to maintenance")
        if not exclusive:
            _check()
        yield
        return
    state.ensure_private_directory(path)
    with state.open_private_text(os.path.join(path, "gate.lock"), "a+") as lock:
        deadline = time.monotonic() + LOCK_TIMEOUT
        while True:
            try:
                fcntl.flock(lock.fileno(), (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise state.SubmissionBlocked("maintenance fence is busy; no state operation performed")
                time.sleep(.02)
        token = _held.set((os.getpid(), exclusive, path))
        try:
            if not exclusive:
                _check()
            yield
        finally:
            _held.reset(token)
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


@contextmanager
def actor():
    held = _held.get()
    if held is None or held[0] != os.getpid() or not held[1] or held[2] != directory():
        from . import state
        raise state.StateError("maintenance actor requires the exclusive stable fence")
    token = _authorized.set((os.getpid(), held[2]))
    try:
        yield
    finally:
        _authorized.reset(token)


def writer(function):
    @functools.wraps(function)
    def guarded(*args, **kwargs):
        with gate():
            return function(*args, **kwargs)
    return guarded
