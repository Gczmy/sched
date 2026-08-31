from __future__ import annotations

import errno
import fcntl
import os
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

import gsched.executor as executor_module
from gsched.executor import (
    LAUNCH_INTENT_NONCE_BYTES,
    LAUNCH_INTENT_TAG,
    Executor,
    _claim_abandoned_launch_intent,
    _create_launch_intent,
    _publish_launch_identity,
    _read_published_launch_identity,
    _unlink_owned_launch_intent,
    parse_launch_intent_payload,
)
from gsched.dispatcher import Dispatcher


class LaunchIntentExecutorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="sched-launch-intent-")
        self.addCleanup(self.tmp.cleanup)
        self.marker = os.path.join(self.tmp.name, "launch", "job.launch")
        self.log = os.path.join(self.tmp.name, "logs", "job.log")

    def test_exact_intent_payload_schema(self) -> None:
        nonce = "a" * (LAUNCH_INTENT_NONCE_BYTES * 2)
        exact = f"{LAUNCH_INTENT_TAG} {nonce}\n".encode("ascii")
        self.assertEqual(nonce, parse_launch_intent_payload(exact))

        rejected = (
            exact[:-1],
            exact + b"\n",
            exact.replace(b" ", b"  ", 1),
            exact.replace(b"a", b"A", 1),
            f"{LAUNCH_INTENT_TAG} {nonce[:-1]}\n".encode("ascii"),
            b"0 proc:123\n",
            b"",
        )
        for payload in rejected:
            with self.subTest(payload=payload):
                self.assertIsNone(parse_launch_intent_payload(payload))

    def test_intent_is_private_exact_and_locked_until_abandoned(self) -> None:
        intent = _create_launch_intent(self.marker)
        try:
            entry = os.lstat(self.marker)
            self.assertTrue(stat.S_ISREG(entry.st_mode))
            self.assertEqual(0o600, stat.S_IMODE(entry.st_mode))
            with open(self.marker, "rb") as stream:
                self.assertEqual(
                    intent.nonce,
                    parse_launch_intent_payload(stream.read()),
                )
            self.assertFalse(_claim_abandoned_launch_intent(self.marker))
            self.assertTrue(os.path.exists(self.marker))
        finally:
            os.close(intent.fd)

        self.assertTrue(_claim_abandoned_launch_intent(self.marker))
        self.assertFalse(os.path.lexists(self.marker))

    def test_existing_marker_is_never_replaced_by_intent_publication(self) -> None:
        os.makedirs(os.path.dirname(self.marker), mode=0o700)
        with open(self.marker, "wb") as stream:
            stream.write(b"foreign marker\n")

        with self.assertRaises(FileExistsError):
            _create_launch_intent(self.marker)

        with open(self.marker, "rb") as stream:
            self.assertEqual(b"foreign marker\n", stream.read())

    def test_capability_error_uses_attested_hardlink_fallback(self) -> None:
        with mock.patch.object(
            executor_module,
            "_atomic_rename_noreplace",
            side_effect=OSError(errno.ENOTSUP, "unsupported on shared fs"),
        ):
            intent = _create_launch_intent(self.marker)
        try:
            entry = os.lstat(self.marker)
            self.assertIn(entry.st_nlink, (1, 2))
            self.assertFalse(_claim_abandoned_launch_intent(self.marker))
        finally:
            os.close(intent.fd)
        self.assertTrue(_claim_abandoned_launch_intent(self.marker))

    def test_hardlink_fallback_accepts_nfs_cached_two_link_fd(self) -> None:
        real_fstat = os.fstat

        def stale_nfs_fstat(fd: int) -> os.stat_result:
            fields = list(real_fstat(fd))
            fields[3] = 2  # st_nlink remains stale while the locked FD is open.
            return os.stat_result(fields)

        with mock.patch.object(
            executor_module,
            "_atomic_rename_noreplace",
            side_effect=OSError(errno.EINVAL, "unsupported on NFS"),
        ), mock.patch.object(
            executor_module.os,
            "fstat",
            side_effect=stale_nfs_fstat,
        ):
            intent = _create_launch_intent(self.marker)
            try:
                self.assertTrue(os.path.isfile(self.marker))
                self.assertFalse(_claim_abandoned_launch_intent(self.marker))
            finally:
                os.close(intent.fd)

        self.assertTrue(_claim_abandoned_launch_intent(self.marker))
        self.assertFalse(os.path.lexists(self.marker))

    def test_non_capability_rename_error_never_falls_back(self) -> None:
        with mock.patch.object(
            executor_module,
            "_atomic_rename_noreplace",
            side_effect=OSError(errno.EIO, "shared storage I/O failure"),
        ), mock.patch.object(os, "link") as hardlink:
            with self.assertRaises(OSError) as raised:
                _create_launch_intent(self.marker)
        self.assertEqual(errno.EIO, raised.exception.errno)
        hardlink.assert_not_called()
        self.assertFalse(os.path.lexists(self.marker))

    def test_abandoned_hardlink_crash_window_with_two_names_is_claimable(
        self,
    ) -> None:
        os.makedirs(os.path.dirname(self.marker), mode=0o700)
        nonce = "b" * (LAUNCH_INTENT_NONCE_BYTES * 2)
        temporary = f"{self.marker}.intent.crash.{nonce}.tmp"
        fd = os.open(temporary, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(fd, f"{LAUNCH_INTENT_TAG} {nonce}\n".encode("ascii"))
            os.fsync(fd)
            fcntl.flock(fd, fcntl.LOCK_EX)
            os.link(temporary, self.marker, follow_symlinks=False)
            self.assertEqual(2, os.fstat(fd).st_nlink)
        finally:
            os.close(fd)

        self.assertTrue(_claim_abandoned_launch_intent(self.marker))
        self.assertFalse(os.path.lexists(self.marker))
        self.assertTrue(os.path.isfile(temporary))
        self.assertEqual(1, os.lstat(temporary).st_nlink)

    def test_two_link_identity_payload_is_never_accepted_as_identity(self) -> None:
        os.makedirs(os.path.dirname(self.marker), mode=0o700)
        peer = f"{self.marker}.peer"
        with open(peer, "wb") as stream:
            stream.write(f"{os.getpid()} proc:123\n".encode("ascii"))
        os.link(peer, self.marker, follow_symlinks=False)
        self.assertEqual(2, os.lstat(self.marker).st_nlink)
        self.assertIsNone(_read_published_launch_identity(self.marker))

    def test_intent_is_complete_before_atomic_final_path_publication(self) -> None:
        observed_source: list[str] = []

        def fail_before_publish(source: str, destination: str) -> None:
            self.assertEqual(self.marker, destination)
            self.assertFalse(os.path.lexists(destination))
            with open(source, "rb") as stream:
                self.assertIsNotNone(parse_launch_intent_payload(stream.read()))
            observed_source.append(source)
            raise OSError("injected atomic publication failure")

        with mock.patch.object(
            executor_module,
            "_atomic_rename_noreplace",
            side_effect=fail_before_publish,
        ):
            with self.assertRaisesRegex(OSError, "atomic publication failure"):
                _create_launch_intent(self.marker)

        self.assertEqual(1, len(observed_source))
        self.assertFalse(os.path.lexists(self.marker))
        self.assertFalse(os.path.lexists(observed_source[0]))

    def test_owned_cleanup_does_not_unlink_a_replacement_inode(self) -> None:
        intent = _create_launch_intent(self.marker)
        replacement = f"{self.marker}.replacement"
        with open(replacement, "wb") as stream:
            stream.write(b"replacement\n")
        os.replace(replacement, self.marker)
        try:
            self.assertFalse(_unlink_owned_launch_intent(self.marker, intent))
            with open(self.marker, "rb") as stream:
                self.assertEqual(b"replacement\n", stream.read())
        finally:
            os.close(intent.fd)

    def test_inherited_lock_blocks_claim_until_child_exits(self) -> None:
        intent = _create_launch_intent(self.marker)
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            pass_fds=(intent.fd,),
        )
        os.close(intent.fd)
        self.addCleanup(lambda: child.kill() if child.poll() is None else None)
        try:
            self.assertFalse(_claim_abandoned_launch_intent(self.marker))
            self.assertTrue(os.path.exists(self.marker))
        finally:
            child.terminate()
            child.wait(timeout=5)

        self.assertTrue(_claim_abandoned_launch_intent(self.marker))
        self.assertFalse(os.path.lexists(self.marker))

    def test_dispatcher_blocks_locked_intent_and_cleans_unlocked_intent(self) -> None:
        dispatcher = Dispatcher.__new__(Dispatcher)
        dispatcher.host_dir = self.tmp.name
        dispatcher.log_line = mock.Mock()
        dispatcher.executor = mock.Mock()
        job = {"id": "dispatcher-intent", "pgid": None}
        marker = dispatcher._launch_marker_path(job)
        intent = _create_launch_intent(marker)
        try:
            self.assertTrue(dispatcher._prepare_launch_marker(job))
            self.assertTrue(os.path.exists(marker))
        finally:
            os.close(intent.fd)

        self.assertFalse(dispatcher._prepare_launch_marker(job))
        self.assertFalse(os.path.lexists(marker))

    def test_child_can_replace_inherited_intent_with_legacy_identity(self) -> None:
        intent = _create_launch_intent(self.marker)
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        program = (
            "import os,sys,time\n"
            "sys.path.insert(0,sys.argv[1])\n"
            "from gsched.executor import _publish_launch_identity\n"
            "path=sys.argv[2]; fd=int(sys.argv[3]); nonce=sys.argv[4]\n"
            "_publish_launch_identity(path,os.getpid(),fd,nonce)\n"
            "os.close(fd)\n"
            "time.sleep(30)\n"
        )
        child = subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-c",
                program,
                repo_root,
                self.marker,
                str(intent.fd),
                intent.nonce,
            ],
            pass_fds=(intent.fd,),
        )
        os.close(intent.fd)
        self.addCleanup(lambda: child.kill() if child.poll() is None else None)
        try:
            identity = None
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                identity = _read_published_launch_identity(self.marker)
                if identity is not None:
                    break
                if child.poll() is not None:
                    break
                time.sleep(0.01)
            self.assertIsNotNone(identity)
            assert identity is not None
            self.assertEqual(child.pid, identity[0])
            self.assertIsNone(_claim_abandoned_launch_intent(self.marker))
        finally:
            child.terminate()
            child.wait(timeout=5)

    def test_parent_and_child_publication_is_idempotent_for_same_identity(self) -> None:
        intent = _create_launch_intent(self.marker)
        try:
            first = _publish_launch_identity(
                self.marker,
                os.getpid(),
                intent.fd,
                intent.nonce,
            )
            second = _publish_launch_identity(
                self.marker,
                os.getpid(),
                intent.fd,
                intent.nonce,
            )
            self.assertEqual(first, second)
            self.assertEqual(first, _read_published_launch_identity(self.marker))
        finally:
            os.close(intent.fd)

    def test_executor_publishes_intent_before_popen_and_passes_locked_fd(self) -> None:
        fake_proc = mock.Mock(pid=4242)
        fake_proc.poll.return_value = None
        observed: dict[str, object] = {}

        def inspect_popen(command, **kwargs):
            with open(self.marker, "rb") as stream:
                nonce = parse_launch_intent_payload(stream.read())
            self.assertIsNotNone(nonce)
            passed = kwargs.get("pass_fds")
            self.assertIsInstance(passed, tuple)
            assert isinstance(passed, tuple)
            self.assertEqual(1, len(passed))
            intent_fd = passed[0]
            probe_fd = os.open(self.marker, os.O_RDWR)
            try:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(
                        probe_fd,
                        fcntl.LOCK_EX | fcntl.LOCK_NB,
                    )
            finally:
                os.close(probe_fd)
            wrapper = command[-1]
            close_fragment = f"exec {intent_fd}>&-"
            self.assertIn(close_fragment, wrapper)
            self.assertLess(wrapper.index(close_fragment), wrapper.rindex("__sched_run"))
            observed["fd"] = intent_fd
            return fake_proc

        executor = Executor()
        with mock.patch(
            "gsched.executor.subprocess.Popen",
            side_effect=inspect_popen,
        ), mock.patch(
            "gsched.executor.process_start_token",
            return_value="proc:123",
        ):
            pgid = executor.launch(
                cmd=["/bin/true"],
                stages=None,
                cwd=self.tmp.name,
                env={"SCHED_LAUNCH_MARKER": self.marker},
                gpu=None,
                log_path=self.log,
            )

        self.assertEqual(4242, pgid)
        self.assertEqual((4242, "proc:123"), _read_published_launch_identity(self.marker))
        intent_fd = observed["fd"]
        assert isinstance(intent_fd, int)
        with self.assertRaises(OSError):
            os.fstat(intent_fd)

    def test_popen_failure_removes_only_the_unchanged_owned_intent(self) -> None:
        executor = Executor()
        observed_fd: list[int] = []

        def fail_popen(_command, **kwargs):
            passed = kwargs["pass_fds"]
            observed_fd.append(passed[0])
            with open(self.marker, "rb") as stream:
                self.assertIsNotNone(parse_launch_intent_payload(stream.read()))
            raise OSError("injected Popen failure")

        with mock.patch(
            "gsched.executor.subprocess.Popen",
            side_effect=fail_popen,
        ):
            with self.assertRaisesRegex(OSError, "injected Popen failure"):
                executor.launch(
                    cmd=["/bin/true"],
                    stages=None,
                    cwd=self.tmp.name,
                    env={"SCHED_LAUNCH_MARKER": self.marker},
                    gpu=None,
                    log_path=self.log,
                )

        self.assertFalse(os.path.lexists(self.marker))
        with self.assertRaises(OSError):
            os.fstat(observed_fd[0])

    def test_popen_failure_preserves_a_replacement_marker(self) -> None:
        executor = Executor()

        def replace_then_fail(_command, **_kwargs):
            replacement = f"{self.marker}.replacement"
            with open(replacement, "wb") as stream:
                stream.write(b"do-not-delete\n")
            os.replace(replacement, self.marker)
            raise OSError("injected Popen failure after replacement")

        with mock.patch(
            "gsched.executor.subprocess.Popen",
            side_effect=replace_then_fail,
        ):
            with self.assertRaisesRegex(OSError, "after replacement"):
                executor.launch(
                    cmd=["/bin/true"],
                    stages=None,
                    cwd=self.tmp.name,
                    env={"SCHED_LAUNCH_MARKER": self.marker},
                    gpu=None,
                    log_path=self.log,
                )

        with open(self.marker, "rb") as stream:
            self.assertEqual(b"do-not-delete\n", stream.read())


if __name__ == "__main__":
    unittest.main()
