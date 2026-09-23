"""Regression coverage for the interrupted, idempotent production rollout."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


path = Path(__file__).resolve().parents[1] / 'docs/operations/resource-repair-20260922/activate_once.py'
spec = importlib.util.spec_from_file_location('resource_rollout', path)
rollout = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rollout)


class RolloutResumeTests(unittest.TestCase):
    def setUp(self):
        self.task = {'id': 'experiment', 'resources': {
            'gpu': 1, 'gpu_share': False, 'cpus': 30, 'host_mem_gib': 36}}
        self.batch = {'name': 'migration', 'project': 'test', 'tasks': [self.task]}
        self.snapshot = {
            'daemon_health': {'draining': True, 'frozen': False, 'heartbeat_age_s': 1},
            'batches': [{'id': 'migration-full-id', 'name': 'migration', 'project': 'test',
                         'depends_on': [], 'status': 'queued'}],
            'jobs': [{'id': 'job-full-id', 'batch_id': 'migration-full-id',
                      'batch_name': 'migration', 'task': 'experiment', 'version': 1,
                      'status': 'pending', 'wait_reason': 'dependency',
                      'resources': copy.deepcopy(self.task['resources'])}],
            'cpu': {'used': 0, 'total': 120}, 'host_memory': {'used_gib': 0}}

    def test_submit_visible_before_activation_tick(self):
        self.assertEqual(('migration-full-id', False),
                         rollout.paused_submission(self.snapshot, self.batch, self.task))
        self.snapshot['batches'][0]['status'] = 'active'
        self.snapshot['jobs'][0]['wait_reason'] = 'draining'
        self.assertEqual(('migration-full-id', True),
                         rollout.paused_submission(self.snapshot, self.batch, self.task))

    def test_transient_allowance_does_not_accept_real_dependency_or_wrong_spec(self):
        for change in ('dependency', 'resources', 'running', 'version', 'drain', 'heartbeat'):
            with self.subTest(change=change):
                snapshot = copy.deepcopy(self.snapshot)
                if change == 'dependency': snapshot['batches'][0]['depends_on'] = ['unmet']
                if change == 'resources': snapshot['jobs'][0]['resources']['host_mem_gib'] = 96
                if change == 'running': snapshot['jobs'][0]['status'] = 'running'
                if change == 'version': snapshot['jobs'][0]['version'] = 2
                if change == 'drain': snapshot['daemon_health']['draining'] = False
                if change == 'heartbeat': snapshot['daemon_health']['heartbeat_age_s'] = None
                with self.assertRaises(AssertionError):
                    rollout.paused_submission(snapshot, self.batch, self.task)

    def test_resume_replays_original_request_and_waits_for_activation(self):
        requests, resumed, checks = [], [], []
        snapshot = copy.deepcopy(self.snapshot)
        def complete():
            return snapshot['batches'], snapshot['jobs'], snapshot
        def cli(*args, structured=False):
            if args[0] == 'request':
                requests.append(args)
                return 'saved receipt (replay)'
            if args[0] == 'status':
                checks.append(args[1])
                if len(checks) >= 3:
                    snapshot['batches'][0]['status'] = 'active'
                    snapshot['jobs'][0]['wait_reason'] = 'draining'
                return copy.deepcopy(snapshot)
            if args[0] == 'verify': return 'persisted'
            if args == ('daemon', 'resume'):
                self.assertEqual('active', snapshot['batches'][0]['status'])
                self.assertTrue((root/'receipts.json').exists())
                resumed.append(True)
                snapshot['daemon_health']['draining'] = False
                snapshot['jobs'][0]['status'] = 'running'
                return 'resumed'
            raise AssertionError(args)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            filename = str(root/'experiment.json')
            Path(filename).write_text(json.dumps(self.batch))
            with patch.object(rollout, 'OPS', root), patch.object(rollout, 'cli', cli), \
                    patch.object(rollout, 'complete_status', complete), \
                    patch.object(rollout.time, 'sleep'):
                rollout.submit_and_resume([filename])
            self.assertEqual([('request', 'resource-rollout-2333-experiment',
                              '--expect-revision', '0', '--', 'submit', filename)], requests)
            self.assertEqual(['migration', 'migration-full-id', 'migration-full-id'], checks)
            self.assertEqual([True], resumed)
            self.assertEqual('migration-full-id', json.loads((root/'receipts.json').read_text())[0]['batch_id'])


if __name__ == '__main__':
    unittest.main()
