from __future__ import annotations

import inspect
import socket
import unittest
from unittest import mock

from gsched.native_launch import (
    NativeLaunchPlan,
    NativeLaunchPlanError,
    NativeLaunchUnavailable,
)
from gsched.native_step5d import (
    NativeStep5DNoDataLaunchOwner,
    _create_step5d_no_data_launch_owner,
)


class NativeStep5DNoDataOwnerTests(unittest.TestCase):
    def _values(self) -> dict[str, object]:
        return {
            "profile_id": "step5d-no-data-v1",
            "profile_sha256": "a" * 64,
            "project_root_identity_sha256": "b" * 64,
            "project_root_path": "/reviewed/project",
            "logical_submitted_argv": [
                "/reviewed/python",
                "-I",
                "-S",
                "logical-stop-only.py",
            ],
            "launcher_sha256": "c" * 64,
            "request_frame_sha256": "d" * 64,
            "request_body_sha256": "e" * 64,
            "log_relative_path": "logs/step5d.log",
            "launcher_fd": 10,
            "request_fd": 11,
            "project_root_fd": 12,
            "log_fd": 13,
        }

    def test_factory_owns_peer_without_accepting_or_exporting_one(self) -> None:
        peer = mock.Mock(spec=socket.socket)
        native = mock.Mock(spec=socket.socket)
        peer.fileno.return_value = 101
        native.fileno.return_value = 102
        plan = mock.Mock(spec=NativeLaunchPlan)

        with mock.patch("gsched.native_step5d.sys.platform", "linux"), mock.patch(
            "gsched.native_step5d.socket.socketpair",
            return_value=(peer, native),
        ) as socketpair, mock.patch(
            "gsched.native_step5d._create_native_launch_plan",
            return_value=plan,
        ) as create_plan:
            owner = _create_step5d_no_data_launch_owner(**self._values())

        self.addCleanup(owner.close)
        socketpair.assert_called_once_with(socket.AF_UNIX, socket.SOCK_STREAM)
        peer.set_inheritable.assert_called_once_with(False)
        native.set_inheritable.assert_called_once_with(False)
        peer.fileno.assert_not_called()
        native.fileno.assert_called_once_with()
        native.close.assert_called_once_with()
        peer.close.assert_not_called()

        supplied = create_plan.call_args.kwargs
        self.assertEqual(102, supplied.pop("control_fd"))
        self.assertEqual(self._values(), supplied)
        self.assertNotIn(
            "control_fd",
            inspect.signature(_create_step5d_no_data_launch_owner).parameters,
        )
        self.assertFalse(hasattr(owner, "peer_fd"))
        self.assertFalse(hasattr(owner, "peer_endpoint"))
        self.assertIs(plan, owner.plan)
        self.assertTrue(owner.peer_endpoint_retained)

    def test_owner_keeps_every_authority_and_execution_claim_false(self) -> None:
        peer = mock.Mock(spec=socket.socket)
        native = mock.Mock(spec=socket.socket)
        native.fileno.return_value = 102
        plan = mock.Mock(spec=NativeLaunchPlan)
        with mock.patch("gsched.native_step5d.sys.platform", "linux"), mock.patch(
            "gsched.native_step5d.socket.socketpair",
            return_value=(peer, native),
        ), mock.patch(
            "gsched.native_step5d._create_native_launch_plan",
            return_value=plan,
        ):
            owner = _create_step5d_no_data_launch_owner(**self._values())
        self.addCleanup(owner.close)

        for field in (
            "scheduler_role_authority_claimed",
            "external_anchor_authenticated",
            "formal_ready",
            "scientific_result",
            "logical_python_executed",
            "external_formal_authority_claimed",
        ):
            with self.subTest(field=field):
                self.assertIs(False, getattr(owner, field))
        self.assertFalse(hasattr(owner, "launch"))
        self.assertFalse(hasattr(owner, "send"))
        self.assertFalse(hasattr(owner, "detach_peer"))

    def test_close_releases_plan_and_peer_once(self) -> None:
        peer = mock.Mock(spec=socket.socket)
        native = mock.Mock(spec=socket.socket)
        native.fileno.return_value = 102
        plan = mock.Mock(spec=NativeLaunchPlan)
        with mock.patch("gsched.native_step5d.sys.platform", "linux"), mock.patch(
            "gsched.native_step5d.socket.socketpair",
            return_value=(peer, native),
        ), mock.patch(
            "gsched.native_step5d._create_native_launch_plan",
            return_value=plan,
        ):
            owner = _create_step5d_no_data_launch_owner(**self._values())

        owner.close()
        owner.close()
        plan.close.assert_called_once_with()
        peer.close.assert_called_once_with()
        self.assertTrue(owner.closed)
        with self.assertRaisesRegex(NativeLaunchPlanError, "already closed"):
            _ = owner.plan

    def test_plan_failure_closes_both_unpublished_endpoints(self) -> None:
        peer = mock.Mock(spec=socket.socket)
        native = mock.Mock(spec=socket.socket)
        native.fileno.return_value = 102
        with mock.patch("gsched.native_step5d.sys.platform", "linux"), mock.patch(
            "gsched.native_step5d.socket.socketpair",
            return_value=(peer, native),
        ), mock.patch(
            "gsched.native_step5d._create_native_launch_plan",
            side_effect=NativeLaunchPlanError("fixture failure"),
        ):
            with self.assertRaisesRegex(NativeLaunchPlanError, "fixture failure"):
                _create_step5d_no_data_launch_owner(**self._values())

        native.close.assert_called_once_with()
        peer.close.assert_called_once_with()

    def test_non_linux_rejects_before_endpoint_construction(self) -> None:
        with mock.patch("gsched.native_step5d.sys.platform", "darwin"), mock.patch(
            "gsched.native_step5d.socket.socketpair"
        ) as socketpair:
            with self.assertRaisesRegex(NativeLaunchUnavailable, "requires Linux"):
                _create_step5d_no_data_launch_owner(**self._values())
        socketpair.assert_not_called()

    def test_owner_cannot_be_constructed_or_used_by_another_process(self) -> None:
        with self.assertRaisesRegex(NativeLaunchPlanError, "internal authority"):
            NativeStep5DNoDataLaunchOwner(
                object(),
                owner_pid=123,
                plan=mock.Mock(spec=NativeLaunchPlan),
                peer_endpoint=mock.Mock(spec=socket.socket),
            )

        peer = mock.Mock(spec=socket.socket)
        native = mock.Mock(spec=socket.socket)
        native.fileno.return_value = 102
        plan = mock.Mock(spec=NativeLaunchPlan)
        with mock.patch("gsched.native_step5d.sys.platform", "linux"), mock.patch(
            "gsched.native_step5d.os.getpid",
            return_value=100,
        ), mock.patch(
            "gsched.native_step5d.socket.socketpair",
            return_value=(peer, native),
        ), mock.patch(
            "gsched.native_step5d._create_native_launch_plan",
            return_value=plan,
        ):
            owner = _create_step5d_no_data_launch_owner(**self._values())
        self.addCleanup(owner.close)

        self.assertEqual(100, owner.owner_pid)
        with mock.patch("gsched.native_step5d.os.getpid", return_value=101):
            with self.assertRaisesRegex(NativeLaunchPlanError, "creating process"):
                _ = owner.plan
            with self.assertRaisesRegex(NativeLaunchPlanError, "creating process"):
                owner.validate_live_plan()
        plan.validate_live_fds.assert_not_called()


if __name__ == "__main__":
    unittest.main()
