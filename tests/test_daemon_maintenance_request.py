"""Maintenance requests must survive replay without reversing later controls."""
import argparse
import os
from unittest import mock

from gsched import cli, config, resources, state
from test_review_cli_state import TempStateCase


class DaemonMaintenanceRequestTests(TempStateCase):
    @staticmethod
    def argv(request_id, *command):
        return ["request", request_id, "--expect-revision", "0", "--", *command]

    def request(self, request_id, *command):
        return self.capture(cli.main, self.argv(request_id, *command))

    def test_completed_replays_do_not_reverse_later_controls(self):
        for stop in (False, True):
            with self.subTest(stop=stop):
                command = ["daemon", "drain"] + (["--stop-when-idle"] if stop else [])
                drain_id, resume_id = f"drain-{stop}", f"resume-{stop}"
                first = self.request(drain_id, *command)
                self.assertEqual(0, first[0], first)
                self.assertEqual(stop, resources.drain_state()["stop"])
                resumed = self.request(resume_id, "daemon", "resume")
                self.assertEqual(0, resumed[0], resumed)
                self.assertIsNone(resources.drain_state())
                self.assertEqual(first, self.request(drain_id, *command))
                self.assertIsNone(resources.drain_state())
                resources.set_drain(stop=stop)
                self.assertEqual(resumed, self.request(resume_id, "daemon", "resume"))
                self.assertEqual(stop, resources.drain_state()["stop"])
                resources.resume()

    def test_request_id_binds_action_and_stop_flag(self):
        self.assertEqual(0, self.request("bound-drain", "daemon", "drain")[0])
        for command in (["daemon", "drain", "--stop-when-idle"], ["daemon", "resume"]):
            with self.subTest(command=command):
                result = self.request("bound-drain", *command)
                self.assertEqual(64, result[0], result)
                self.assertFalse(resources.drain_state()["stop"])

    def test_invalid_parameters_are_rejected_before_request_ledger(self):
        for command in (
            ["daemon", "drain", "--fake"],
            ["daemon", "drain", "--json"],
            ["daemon", "drain", "--stop-when-idle", "--stop-when-idle"],
            ["daemon", "resume", "--stop-when-idle"],
            ["daemon", "resume", "extra"],
            ["daemon", "status"],
        ):
            with self.subTest(command=command), mock.patch.object(state, "connect") as connect:
                result = self.capture(cli.cmd_request, argparse.Namespace(
                    request_id="invalid-args", command=command, expect_revision=0,
                ))
                self.assertEqual(64, result[0], result)
                connect.assert_not_called()
        self.assertIsNone(resources.drain_state())

    def test_interruption_after_control_change_leaves_unknown_outcome(self):
        class ProcessLost(BaseException):
            pass

        real_run = cli._run_captured_mutation

        def interrupted(command):
            result = real_run(command)
            self.assertEqual(0, result[0], result)
            raise ProcessLost()

        with mock.patch.object(cli, "_run_captured_mutation", side_effect=interrupted):
            with self.assertRaises(ProcessLost):
                self.request("lost-drain", "daemon", "drain", "--stop-when-idle")
        self.assertTrue(resources.drain_state()["stop"])
        resources.resume()
        result = self.request("lost-drain", "daemon", "drain", "--stop-when-idle")
        self.assertEqual(75, result[0], result)
        self.assertIn("outcome unknown", result[2])
        self.assertIsNone(resources.drain_state())

    def test_failed_control_persistence_is_not_reported_as_success_or_retried(self):
        with mock.patch.object(resources, "_sync_control_directory", side_effect=OSError("sync failed")):
            first = self.request("failed-drain", "daemon", "drain")
        self.assertEqual(1, first[0], first)
        self.assertIn("sync failed", first[2])
        resources.resume()
        self.assertEqual(first, self.request("failed-drain", "daemon", "drain"))
        self.assertIsNone(resources.drain_state())

    def test_host_guard_runs_before_database_and_control_writes(self):
        for command in (["daemon", "drain"], ["daemon", "drain", "--stop-when-idle"],
                        ["daemon", "resume"]):
            for wrapped in (False, True):
                for missing_config in (False, True):
                    argv = self.argv("foreign-maintenance", *command) if wrapped else command
                    with self.subTest(argv=argv, missing_config=missing_config), mock.patch.dict(
                        os.environ, {"SCHED_ALLOW_FOREIGN_WRITE": ""}
                    ), mock.patch("socket.gethostname", return_value="gateway"), mock.patch.object(
                        config, "load_config", return_value=self.cfg,
                        side_effect=ValueError("unreadable config") if missing_config else None,
                    ), mock.patch.object(state, "init_db") as init_db, mock.patch.object(
                        state, "connect"
                    ) as connect, mock.patch.object(resources, "set_drain") as drain, mock.patch.object(
                        resources, "resume"
                    ) as resume:
                        result = self.capture(cli.main, argv)
                        self.assertEqual(2, result[0], result)
                        init_db.assert_not_called()
                        connect.assert_not_called()
                        drain.assert_not_called()
                        resume.assert_not_called()

    def test_target_host_needs_no_override_and_resume_does_not_start_daemon(self):
        with mock.patch.dict(os.environ, {"SCHED_ALLOW_FOREIGN_WRITE": ""}), mock.patch(
            "socket.gethostname", return_value=self.cfg["node"]
        ), mock.patch("gsched.daemon.start") as start:
            for command in (["daemon", "drain"], ["daemon", "resume"]):
                result = self.capture(cli.main, command)
                self.assertEqual(0, result[0], result)
            self.assertEqual(0, self.request("local-drain", "daemon", "drain")[0])
            self.assertEqual(0, self.request("local-resume", "daemon", "resume")[0])
            self.assertIsNone(resources.drain_state())
            start.assert_not_called()
