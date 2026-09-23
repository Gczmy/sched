from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from unittest import mock

from gsched import cli, config, resources, state
from gsched.schema import SchemaError, validate_batch
from test_review_cli_state import TempStateCase
from test_review_dispatcher_gpu import DispatcherStateCase


class HostAdmissionTests(DispatcherStateCase):
    def configure(self, **values):
        self.cfg.update(host_mem_total_gib=96, host_mem_reserve_gib=16)
        self.cfg.update(values)
        self.cfg['projects']['p']['gpu_quota'] = 0
        with open(self.config_path, 'w') as stream:
            json.dump(self.cfg, stream)

    def dispatch(self, sample=None):
        dispatcher = self.dispatcher()
        dispatcher._assign_in_tx = mock.Mock(return_value=1)
        def launch(conn, job, gpu):
            state.update_job(conn, job['id'], status='running', gpu=gpu)
            return True
        dispatcher._launch_job = mock.Mock(side_effect=launch)
        with mock.patch.object(resources, 'host_memory', return_value=sample):
            dispatcher._dispatch_ready_jobs()
        return dispatcher

    def test_reservations_precede_gpu_assignment_and_release_after_completion(self):
        self.configure()
        self.seed_jobs([('a', {'gpu': 1, 'host_mem_gib': 64}, 'pending'),
                        ('b', {'gpu': 1, 'host_mem_gib': 64}, 'pending'),
                        ('cpu', {'gpu': 0, 'host_mem_gib': 8}, 'pending')])
        dispatcher = self.dispatch({'MemTotal': 128, 'MemAvailable': 120})
        self.assertEqual(['a', 'cpu'], [c.args[1]['id'] for c in dispatcher._launch_job.call_args_list])
        self.assertEqual(1, dispatcher._assign_in_tx.call_count)
        self.assertEqual('host_memory', resources.admission_snapshot()['waits']['b'])
        with state.connect() as conn:
            self.assertEqual(72, resources.memory_usage(conn, self.cfg))
            state.update_job(conn, 'a', status='done', gpu=None)
        dispatcher = self.dispatch({'MemTotal': 128, 'MemAvailable': 120})
        self.assertEqual(['b'], [c.args[1]['id'] for c in dispatcher._launch_job.call_args_list])

    def test_physical_headroom_unknown_samples_and_same_tick_launches(self):
        self.configure()
        self.seed_jobs([('a', {'gpu': 1, 'host_mem_gib': 32}, 'pending'),
                        ('b', {'gpu': 1, 'host_mem_gib': 32}, 'pending')])
        for sample in (None, {'MemTotal': 128, 'MemAvailable': 40}):
            self.dispatch(sample)._assign_in_tx.assert_not_called()
        dispatcher = self.dispatch({'MemTotal': 128, 'MemAvailable': 70})
        self.assertEqual(1, dispatcher._assign_in_tx.call_count)

    def test_hot_memory_limit_is_read_at_gate_even_with_stale_dispatcher_config(self):
        self.configure(host_mem_total_gib=64)
        self.seed_jobs([('a', {'gpu': 1, 'host_mem_gib': 48}, 'pending')])
        fresh = dict(self.cfg, host_mem_total_gib=32)
        with open(self.config_path, 'w') as stream:
            json.dump(fresh, stream)
        self.dispatch({'MemTotal': 128, 'MemAvailable': 120})._assign_in_tx.assert_not_called()

    def test_drain_preserves_pending_and_waits_for_running_and_launch_recovery(self):
        self.configure()
        self.seed_jobs([('a', {'gpu': 1, 'host_mem_gib': 32}, 'running'),
                        ('b', {'gpu': 1, 'host_mem_gib': 32}, 'pending')])
        resources.set_drain(stop=True)
        dispatcher = self.dispatch({'MemTotal': 128, 'MemAvailable': 120})
        dispatcher._launch_job.assert_not_called()
        self.assertFalse(dispatcher._drain_complete())
        with state.connect() as conn:
            state.update_job(conn, 'a', status='done', gpu=None)
        with mock.patch.object(dispatcher, '_unresolved_launch_markers', return_value=['marker']):
            self.assertFalse(dispatcher._drain_complete())
        with mock.patch.object(dispatcher, '_unresolved_launch_markers', return_value=[]):
            self.assertTrue(dispatcher._drain_complete())
        with state.connect() as conn:
            self.assertEqual('pending', state.get_job(conn, 'b')['status'])
        resources.resume()
        self.assertIsNone(resources.drain_state())
        self.assertEqual(1, self.dispatch({'MemTotal': 128, 'MemAvailable': 120})._launch_job.call_count)

    def test_invalid_drain_pauses_without_authorizing_stop(self):
        self.configure()
        self.seed_jobs([('a', {'gpu': 1}, 'pending')])
        resources.write_private_json('daemon.drain.json', {'stop': 'true'})
        dispatcher = self.dispatch()
        dispatcher._launch_job.assert_not_called()
        self.assertFalse(dispatcher._drain_complete())

    def test_outstanding_uses_descendant_pss_not_shared_rss_and_fails_closed(self):
        self.configure()
        self.seed_jobs([('a', {'gpu': 1, 'host_mem_gib': 16}, 'running')])
        with state.connect() as conn:
            state.update_job(conn, 'a', pgid=999991)
        paths = [Path('/proc')/str(pid) for pid in (999991, 999992, 999993)]
        def read(path):
            pid = int(path.parent.name)
            if path.name == 'stat':
                return f'{pid} (worker) S {999991 if pid == 999992 else 1} '
            pss = {999991:2, 999992:6, 999993:90}[pid]
            return f'Rss: 104857600 kB\nPss: {pss*1024*1024} kB\n'
        with mock.patch.object(Path, 'iterdir', return_value=iter(paths)), mock.patch.object(Path, 'read_text', read):
            with state.connect() as conn:
                self.assertEqual(8, resources.memory_outstanding(conn, self.cfg))
        def partial(path):
            if path == paths[1]/'smaps_rollup':
                raise PermissionError('unreadable child')
            return read(path)
        with mock.patch.object(Path, 'iterdir', return_value=iter(paths)), mock.patch.object(Path, 'read_text', partial):
            with state.connect() as conn:
                self.assertEqual(14, resources.memory_outstanding(conn, self.cfg))


class HostResourceContracts(TempStateCase):
    def test_numeric_validation_rejects_nonfinite_and_boolean_values(self):
        for value in (True, -1, '8', float('nan'), float('inf'), 10**400):
            with self.subTest(value=value):
                cfg = dict(self.cfg, host_mem_total_gib=value)
                with self.assertRaises(config.ConfigError):
                    config._validate(cfg, 'test')
                spec = {'name': 'test', 'project': 'p', 'tasks': [
                    {'id': 'a', 'cmd': ['/bin/true'], 'resources': {'host_mem_gib': value}}]}
                with self.assertRaises(SchemaError):
                    validate_batch(spec, self.cfg)

    def test_status_explains_memory_and_drain_without_sampling_gateway(self):
        self.cfg.update(host_mem_total_gib=16, host_mem_default_gib=8)
        self.seed_batch(task_id='a', job_status='running')
        self.seed_batch(batch_id='second', name='second', task_id='b', job_status='pending')
        self.seed_batch(batch_id='third', name='third', task_id='c', job_status='running')
        with mock.patch.object(resources, 'host_memory', side_effect=AssertionError('gateway sample')):
            data = self.status_json()
        self.assertEqual({'host_memory'}, {j['wait_reason'] for j in data['jobs'] if j['status'] == 'pending'})
        self.assertEqual(16, data['host_memory']['used_gib'])
        self.assertIsNone(data['host_memory']['available_gib'])
        resources.set_drain()
        data = self.status_json()
        self.assertEqual({'draining'}, {j['wait_reason'] for j in data['jobs'] if j['status'] == 'pending'})
        self.assertEqual(0, self.capture(cli.cmd_daemon, argparse.Namespace(action='resume'))[0])

    def test_sample_expires_and_rejects_malformed_numbers(self):
        resources.publish_admission({'MemTotal': 128, 'MemAvailable': 99}, {})
        self.assertEqual(99, resources.admission_snapshot()['sample']['MemAvailable'])
        with mock.patch.object(resources.time, 'time', return_value=10**12):
            self.assertEqual({}, resources.admission_snapshot())
        resources.publish_admission({'MemTotal': 128, 'MemAvailable': 'bad'}, {})
        self.assertEqual({}, resources.admission_snapshot())
