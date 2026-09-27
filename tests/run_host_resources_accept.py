"""CLI lifecycle acceptance; run only on a Linux compute node (fake GPUs)."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time


def main():
    with tempfile.TemporaryDirectory(prefix='sched-host-resources-') as temporary:
        root = Path(temporary)
        # Keep real-memory admission viable on small Linux development machines.
        # Two 0.5 GiB reservations still exceed the configured usable 0.875 GiB.
        cfg = {'schema_version': 1, 'node': socket.gethostname(), 'user': os.environ['USER'],
               'state_dir': str(root/'state'), 'gpus': [0], 'cpus_total': 2,
               'host_mem_total_gib': 1, 'host_mem_reserve_gib': 0.125,
               'default_project': 'test', 'projects': {'test': {'root': str(root), 'git': False}},
               'venvs': {'python': sys.executable}}
        (root/'config.json').write_text(json.dumps(cfg))
        env = dict(os.environ, SCHED_STATE=str(root/'state'), SCHED_CONFIG=str(root/'config.json'),
                   SCHED_FAKE_GPUS='0:24', PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        env.pop('SCHED_ALLOW_FOREIGN_WRITE', None)
        def cli(*args):
            result = subprocess.run([sys.executable, '-m', 'gsched.cli', *args], env=env,
                                    capture_output=True, text=True, timeout=45)
            assert result.returncode == 0, (args, result.stdout, result.stderr)
            return result.stdout
        def snapshot():
            return json.loads(cli('status', '--json'))
        def wait(check):
            deadline = time.monotonic()+120
            while time.monotonic()<deadline:
                data = snapshot()
                if check(data): return data
                time.sleep(1)
            raise AssertionError(data)
        tasks = []
        for task in ('a', 'b'):
            code = ("import time; from pathlib import Path; "
                    f"release=Path({str(root/'release')!r}); "
                    "exec('while not release.exists():\\n time.sleep(.1)'); "
                    f"Path({str(root/(task+'.txt'))!r}).write_text('ok')")
            tasks.append({'id': task, 'cmd': [sys.executable, '-c', code],
                          'resources': {'gpu': 1, 'cpus': 1, 'host_mem_gib': 0.5}, 'max_retry': 0,
                          'artifacts': {'result': {'path': str(root/(task+'.txt'))}}})
        (root/'batch.json').write_text(json.dumps({'name': 'drain-lifecycle', 'project': 'test', 'tasks': tasks}))
        try:
            cli('daemon', 'drain')
            cli('submit', str(root/'batch.json'))
            data = wait(lambda d: len(d['jobs']) == 2 and
                        all(j['status']=='pending' and j['wait_reason']=='draining' for j in d['jobs']))
            assert all(j['status']=='pending' and j['wait_reason']=='draining' for j in data['jobs'])
            cli('daemon', 'resume')
            wait(lambda d: sum(j['status']=='running' for j in d['jobs']) == 1)
            cli('daemon', 'drain', '--stop-when-idle')
            (root/'release').touch()
            data = wait(lambda d: [j['status'] for j in d['jobs']] == ['done', 'pending'])
            deadline = time.monotonic()+60
            while '运行中' in cli('daemon', 'status') and time.monotonic()<deadline:
                time.sleep(1)
            assert '运行中' not in cli('daemon', 'status')
            assert data['host_memory']['used_gib'] == 0
            cli('daemon', 'resume')
            cli('daemon', 'start', '--fake')
            wait(lambda d: len(d['jobs'])==2 and all(j['status']=='done' for j in d['jobs']))
            print('PASS: drain preserves pending, completes running, exits, resumes and releases reservations')
        finally:
            cli('daemon', 'stop')


if __name__ == '__main__':
    main()
