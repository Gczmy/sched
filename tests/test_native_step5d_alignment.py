from __future__ import annotations

import hashlib
import inspect
import struct
import unittest
from unittest import mock

from gsched import native_launch, native_step5d
from gsched.native_step5d_alignment import (
    FOUNDATION_ALIGNMENT_SCHEMA,
    FOUNDATION_ALIGNMENT_SCOPE,
    foundation_alignment_projection,
)


class NativeStep5DFoundationAlignmentTests(unittest.TestCase):
    def test_projection_uses_exact_foundation_schema_and_transport(self) -> None:
        projection = foundation_alignment_projection()
        transport = projection["inherited_transport"]

        self.assertEqual(
            {
                "schema",
                "scope",
                "parent_step5c",
                "inherited_transport",
                "native_launch_plan_digest_vector",
                "direct_parent_endpoint_construction",
                "unimplemented_step5d_sections",
                "authority_boundary",
            },
            set(projection),
        )
        self.assertEqual(FOUNDATION_ALIGNMENT_SCHEMA, projection["schema"])
        self.assertEqual(FOUNDATION_ALIGNMENT_SCOPE, projection["scope"])
        self.assertEqual(
            list(native_launch.NATIVE_ACTUAL_ARGV),
            transport["actual_argv"],
        )
        self.assertEqual([], transport["actual_environment"])
        self.assertEqual(native_launch.NATIVE_REQUEST_FD, transport["request_fd"])
        self.assertEqual(native_launch.NATIVE_CONTROL_FD, transport["control_fd"])
        self.assertEqual(
            native_launch.NATIVE_PROJECT_ROOT_FD,
            transport["project_root_fd"],
        )
        self.assertEqual(
            native_launch.NATIVE_LAUNCH_WIRE_MAGIC.hex(),
            transport["wire_magic_hex"],
        )

    def test_digest_vector_binds_distinct_frame_and_body_fields(self) -> None:
        vector = foundation_alignment_projection()[
            "native_launch_plan_digest_vector"
        ]
        body = b'{"schema":"m2b_step5d_foundation_digest_vector/v1"}'
        header = struct.Struct("!8sBBBBII")
        outer = struct.Struct("!I")
        payload = header.pack(
            native_launch.NATIVE_LAUNCH_WIRE_MAGIC,
            native_launch.NATIVE_LAUNCH_WIRE_VERSION,
            native_launch.NATIVE_LAUNCH_CHANNEL_REQUEST,
            native_launch.NATIVE_LAUNCH_MESSAGE_REQUEST,
            native_launch.NATIVE_LAUNCH_FLAGS_NONE,
            native_launch.NATIVE_LAUNCH_REQUEST_SEQUENCE,
            len(body),
        ) + body
        frame = outer.pack(len(payload)) + payload

        self.assertEqual(
            list(native_launch.NativeLaunchPlan.__slots__),
            vector["plan_slots"],
        )
        self.assertEqual(
            list(
                inspect.signature(
                    native_launch._create_native_launch_plan
                ).parameters
            ),
            vector["factory_keyword_only_parameters"],
        )
        self.assertEqual(
            ["request_frame_sha256", "request_body_sha256"],
            vector["digest_field_names"],
        )
        self.assertEqual([], vector["forbidden_digest_aliases_present"])
        self.assertEqual(
            hashlib.sha256(frame).hexdigest(),
            vector["request_frame_sha256"],
        )
        self.assertEqual(
            hashlib.sha256(body).hexdigest(),
            vector["request_body_sha256"],
        )
        self.assertNotEqual(
            vector["request_frame_sha256"],
            vector["request_body_sha256"],
        )

    def test_owner_projection_keeps_peer_private_and_trace_inert(self) -> None:
        with mock.patch.object(
            native_step5d.socket,
            "socketpair",
            side_effect=AssertionError("projection opened a socketpair"),
        ), mock.patch.object(
            native_step5d,
            "_create_native_launch_plan",
            side_effect=AssertionError("projection created a plan"),
        ):
            owner = foundation_alignment_projection()[
                "direct_parent_endpoint_construction"
            ]

        factory = native_step5d._create_step5d_no_data_launch_owner
        parameters = inspect.signature(factory).parameters
        self.assertEqual(
            list(native_step5d.NativeStep5DNoDataLaunchOwner.__slots__),
            owner["owner_slots"],
        )
        self.assertEqual(
            list(parameters),
            owner["factory_keyword_only_parameters"],
        )
        self.assertNotIn("control_fd", parameters)
        self.assertNotIn("peer_endpoint", parameters)
        self.assertIs(False, owner["factory_accepts_control_fd"])
        self.assertIs(False, owner["factory_accepts_peer_endpoint"])
        self.assertEqual(1, owner["parent_retained_endpoint_count"])
        self.assertEqual(0, owner["caller_peer_endpoint_handoff_count"])
        self.assertEqual(
            ["NativeStep5DNoDataLaunchOwner"],
            owner["public_exports"],
        )
        self.assertEqual([], owner["forbidden_peer_exports_present"])
        self.assertEqual(
            [
                "socketpair_created",
                "peer_endpoint_noninheritable",
                "native_endpoint_noninheritable",
                "native_endpoint_fd_passed_to_plan_factory",
                "native_endpoint_closed",
                "direct_parent_peer_endpoint_retained",
            ],
            owner["construction_trace"],
        )
        self.assertEqual(
            {
                "scheduler_role_authority_claimed",
                "external_anchor_authenticated",
                "formal_ready",
                "scientific_result",
                "logical_python_executed",
                "external_formal_authority_claimed",
                "owner_pid",
                "closed",
                "plan",
                "peer_endpoint_retained",
                "validate_live_plan",
                "close",
            },
            {
                name
                for name in vars(
                    native_step5d.NativeStep5DNoDataLaunchOwner
                )
                if not name.startswith("_")
            },
        )

    def test_incomplete_sections_and_authority_boundary_are_exact(self) -> None:
        projection = foundation_alignment_projection()
        self.assertEqual(
            [
                "canonical_request_construction",
                "launch_nonce_owner_lifetime_replay_guard",
                "challenge_ack_receipt_failure_control_protocol",
                "isolated_native_runtime_and_child_wait",
            ],
            projection["unimplemented_step5d_sections"],
        )
        boundary = projection["authority_boundary"]
        self.assertEqual(
            {
                "scheduler_role_authority_claimed",
                "external_anchor_authenticated",
                "formal_ready",
                "scientific_result",
                "logical_python_executed",
                "external_formal_authority_claimed",
                "step5d_complete",
            },
            set(boundary),
        )
        self.assertTrue(all(value is False for value in boundary.values()))


if __name__ == "__main__":
    unittest.main()
