import argparse
import json
import os

from gsched import cli, notify, state
from test_review_cli_state import TempStateCase


class NotificationBoundaryTests(TempStateCase):
    def test_ack_cannot_rename_config_or_follow_a_symlink(self):
        with self.assertRaises(ValueError):
            notify.ack(self.config_path)
        self.assertTrue(os.path.isfile(self.config_path))
        inbox = notify.inbox_dir()
        os.makedirs(inbox, exist_ok=True)
        link = os.path.join(inbox, "link.json")
        os.symlink(self.config_path, link)
        with self.assertRaises(ValueError):
            notify.ack(link)
        event = os.path.join(inbox, "event.json")
        with open(event, "w") as stream:
            stream.write('{}')
        self.assertEqual(event + ".acked", notify.ack(event))
        self.assertEqual(event + ".acked", notify.ack(event + ".acked"))

    def test_malformed_json_event_does_not_hide_other_notifications(self):
        inbox = notify.inbox_dir()
        os.makedirs(inbox, exist_ok=True)
        for name, body in (("bad", []), ("good", {"event": "batch_done", "batch": "b"})):
            with open(os.path.join(inbox, name + ".json"), "w") as stream:
                json.dump(body, stream)
        code, output, _ = self.capture(cli.cmd_notify_inbox, argparse.Namespace(all=False, json=True))
        self.assertEqual(0, code)
        events = json.loads(output)
        self.assertEqual(2, len(events))
        self.assertEqual(1, sum("_error" in event for event in events))
        code, output, _ = self.capture(cli.cmd_notify_inbox, argparse.Namespace(all=False, json=False))
        self.assertEqual(0, code)
        self.assertIn("不可读", output)

    def test_interrupted_task_is_in_terminal_notification_failure_details(self):
        self.seed_batch(job_status="interrupted", batch_status="blocked")
        with state.connect() as conn:
            batch = state.get_batch(conn, "batch-20260829-000000")
            event = notify.build_event(conn, batch, state.host_dir())
        self.assertEqual("interrupted", event["failures"][0]["status"])
