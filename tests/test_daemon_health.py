"""Health is evidence, not a gateway-local PID guess or a restart instruction."""
import argparse
from contextlib import ExitStack, redirect_stdout
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from gsched import cli, daemon, resources, state


class DaemonHealthTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.stack.enter_context(mock.patch.object(daemon, "_host_dir", return_value=str(self.root)))
        self.stack.enter_context(mock.patch.object(state, "hostname", return_value="compute"))
        self.host = self.stack.enter_context(mock.patch.object(daemon.socket, "gethostname", return_value="gateway"))
        self.stack.enter_context(mock.patch.object(daemon.time, "time", return_value=1_900_000_000))
        self.drain = self.stack.enter_context(mock.patch.object(resources, "drain_state", return_value=None))
        self.owner_value = {"pid": 123, "start_token": "proc:456", "physical_host": "compute", "lease_id": "lease", "schema_version": 1}
        self.owner = self.stack.enter_context(mock.patch.object(daemon, "_read_lease_owner", return_value=self.owner_value))
        self.alive = self.stack.enter_context(mock.patch.object(daemon, "_pid_alive", return_value=True))
        self.token = self.stack.enter_context(mock.patch.object(daemon, "process_start_token", return_value="proc:456"))
        self.stack.enter_context(mock.patch.object(state, "connect", side_effect=AssertionError("health must not open DB")))

    def files(self, heartbeat=1, tick=2):
        for name, age in (("daemon.heartbeat", heartbeat), ("daemon.tick_ok", tick)):
            if age is not None:
                path = self.root / name
                path.touch()
                os.utime(path, (1_900_000_000 - age,) * 2)

    def test_foreign_pid_is_never_probed_even_on_stall(self):
        self.files(209, 225)
        health = daemon.health_snapshot()
        self.assertEqual("stalled", health["health_state"])
        self.assertEqual("unknown", health["process_state"])
        self.assertTrue(health["frozen"])
        self.assertIn("调度停滞", daemon.status_str())
        self.assertNotIn("未运行", daemon.status_str())
        self.alive.assert_not_called()
        self.token.assert_not_called()

    def test_fresh_gateway_healthy_and_draining(self):
        self.files()
        health = daemon.health_snapshot()
        self.assertEqual("healthy", health["health_state"])
        self.assertEqual("unknown", health["process_state"])
        self.assertIn("运行中", daemon.status_str())
        self.drain.return_value = {"stop": False}
        self.assertTrue(daemon.health_snapshot()["draining"])
        self.assertIn("已暂停新派发", daemon.status_str())
        self.alive.assert_not_called()

    def test_heartbeat_alone_does_not_hide_stalled_tick(self):
        self.files(1, 91)
        self.assertEqual("stalled", daemon.health_snapshot()["health_state"])

    def test_fresh_heartbeat_without_first_tick_is_delayed(self):
        self.files(1, None)
        self.assertEqual("delayed", daemon.health_snapshot()["health_state"])

    def test_thresholds(self):
        for hb, tick, expected in ((59.9, 90, "healthy"), (60, 90, "delayed"), (1, 90.1, "stalled")):
            with self.subTest(hb=hb, tick=tick):
                self.files(hb, tick)
                self.assertEqual(expected, daemon.health_snapshot()["health_state"])

    def test_same_host_alive_stalled_is_not_stopped(self):
        self.host.return_value = "compute"
        self.files(209, 225)
        health = daemon.health_snapshot()
        self.assertEqual(("running", "stalled"), (health["process_state"], health["health_state"]))

    def test_same_host_dead_or_reused_pid_confirms_original_exit(self):
        self.host.return_value = "compute"
        self.files(209, 225)
        for token, alive in ((None, False), ("proc:999", True)):
            self.token.return_value, self.alive.return_value = token, alive
            health = daemon.health_snapshot()
            self.assertEqual(("stopped", "stopped"), (health["process_state"], health["health_state"]))

    def test_unreadable_identity_does_not_authorize_start(self):
        self.host.return_value = "compute"
        self.token.return_value = None
        self.files(65, 65)
        self.assertEqual("unknown", daemon.health_snapshot()["process_state"])

    def test_owner_change_discards_process_evidence(self):
        self.host.return_value = "compute"
        self.files(65, 65)
        self.owner.side_effect = [self.owner_value, {**self.owner_value, "lease_id": "new"}]
        self.assertEqual("unknown", daemon.health_snapshot()["process_state"])

    def test_clean_stop_only_confirmed_on_target(self):
        self.owner.return_value = None
        self.assertEqual("unknown", daemon.health_snapshot()["health_state"])
        self.host.return_value = "compute"
        self.assertEqual("stopped", daemon.health_snapshot()["health_state"])
        (self.root / "dispatcher.lock").mkdir()
        self.assertEqual("unknown", daemon.health_snapshot()["health_state"])

    def test_unreadable_or_future_timestamp_is_unknown(self):
        self.files(-10, 1)
        self.assertEqual("timestamp_in_future", daemon.health_snapshot()["read_error"])
        with mock.patch.object(daemon.os, "stat", side_effect=PermissionError):
            health = daemon.health_snapshot()
        self.assertEqual("unknown", health["health_state"])
        self.assertEqual("health_file_unreadable", health["read_error"])

    def test_json_cli_and_status_share_health(self):
        self.files()
        with mock.patch.object(cli, "_load_cfg", return_value={"node": "compute"}):
            expected = cli._daemon_health()
        stream = io.StringIO()
        with redirect_stdout(stream):
            rc = cli.cmd_daemon(argparse.Namespace(action="status", json=True))
        self.assertEqual(0, rc)
        self.assertEqual({"schema_version": 1, **expected}, json.loads(stream.getvalue()))

    def test_json_flag_cannot_trigger_a_mutation(self):
        with mock.patch.object(daemon, "stop") as stop:
            self.assertEqual(1, cli.cmd_daemon(argparse.Namespace(action="stop", json=True)))
            stop.assert_not_called()


if __name__ == "__main__":
    unittest.main()
