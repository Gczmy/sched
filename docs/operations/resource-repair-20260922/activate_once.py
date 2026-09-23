"""One-shot, bounded rollout on lease 2333; all scheduler operations use CLI."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

OPS = Path('/home/zzhang54/zzhang54/Error_checking/trials/sched_migration_20260922')
RELEASE = Path('/home/zzhang54/zzhang54/sched-resource-release-20260922')
CAMPAIGN = Path('/home/zzhang54/zzhang54/Error_checking/trials/waveletmixer_sched_20260922')
WRAPPER = Path('/home/zzhang54/bin/sched')
OLD_BATCH = 'seed_campaigns_resume_2333_20260921-20260921221937840'
OLD_PATH = '/home/zzhang54/zzhang54/sched${PYTHONPATH:+:$PYTHONPATH}'
NEW_PATH = str(RELEASE)+'${PYTHONPATH:+:$PYTHONPATH}'


def read(path):
    return json.loads(path.read_text())


def publish(phase, **details):
    value = dict(time=time.time(), phase=phase, **details)
    temporary = OPS/'activation.pending.json'
    temporary.write_text(json.dumps(value, indent=2))
    os.replace(temporary, OPS/'activation.json')
    print(json.dumps(value), flush=True)


def cli(*args, structured=False):
    env = dict(os.environ)
    env.pop('SCHED_CONFIG', None)
    env.pop('SCHED_FAKE_GPUS', None)
    env.pop('SCHED_ALLOW_FOREIGN_WRITE', None)
    env['SCHED_STATE'] = '/home/zzhang54/.sched'
    result = subprocess.run([str(WRAPPER), *args], cwd=OPS, env=env,
                            capture_output=True, text=True, timeout=180)
    if result.returncode:
        raise RuntimeError((args, result.returncode, result.stdout[-2000:], result.stderr[-2000:]))
    return json.loads(result.stdout) if structured else result.stdout


def complete_status():
    batches, jobs = {}, {}
    batch_cursor = None
    seen = set()
    while True:
        base = ['status', '--json', '--limit', '1000']
        if batch_cursor:
            base += ['--cursor', batch_cursor]
        page = cli(*base, structured=True)
        for batch in page['batches']:
            batches[batch['id']] = batch
        for job in page['jobs']:
            jobs[job['id']] = job
        job_cursor = page['next_job_cursor']
        while job_cursor:
            assert ('job', batch_cursor, job_cursor) not in seen
            seen.add(('job', batch_cursor, job_cursor))
            detail = cli(*base, '--job-cursor', job_cursor, structured=True)
            for job in detail['jobs']:
                jobs[job['id']] = job
            job_cursor = detail['next_job_cursor']
            assert bool(job_cursor) == detail['truncated']['jobs']
        batch_cursor = page['next_cursor']
        assert bool(batch_cursor) == page['truncated']['batches']
        if not batch_cursor:
            return list(batches.values()), list(jobs.values()), page
        assert ('batch', batch_cursor) not in seen
        seen.add(('batch', batch_cursor))


def verify_files(*, continuing=False):
    assert os.uname().nodename == 'ambiorix' and os.environ.get('SLURM_JOB_ID') == '2333'
    assert len(os.sched_getaffinity(0)) >= 120
    assert read(RELEASE/'VERIFIED.json')['passed']
    assert read(CAMPAIGN/'validation.json')['passed']
    for root, name in ((RELEASE, 'RELEASE_MANIFEST.json'), (CAMPAIGN, 'REVIEW_MANIFEST.json')):
        for relative, digest in read(root/name)['files'].items():
            assert hashlib.sha256((root/relative).read_bytes()).hexdigest() == digest, relative
    expected_path = NEW_PATH if continuing else OLD_PATH
    assert expected_path in WRAPPER.read_text(), 'Wrapper changed; require a fresh rollout review'
    cfg = cli('config', 'get', structured=True)
    assert cfg['node'] == 'ambiorix' and cfg['cpus_total'] == 120
    assert cfg['projects']['wmpp_ett_trial_seed42']['gpu_quota'] == 2
    if continuing:
        assert (cfg['host_mem_total_gib'], cfg['host_mem_reserve_gib'],
                cfg['host_mem_default_gib']) == (96, 16, 8)
    files = read(CAMPAIGN/'batches/index.json')['files']
    assert len(files) == 22
    return files


def paused_submission(snapshot, batch, task):
    """A committed submit may remain queued until the next daemon tick."""
    health = snapshot['daemon_health']
    assert health['draining'] and not health['frozen'], health
    assert health['heartbeat_age_s'] is not None and health['heartbeat_age_s'] <= 90, health
    assert len(snapshot['batches']) == 1 and len(snapshot['jobs']) == 1
    stored, job = snapshot['batches'][0], snapshot['jobs'][0]
    assert stored['name'] == batch['name'] and stored['project'] == batch['project']
    assert stored['depends_on'] == batch.get('depends_on', []) == []
    assert job['batch_id'] == stored['id'] and job['task'] == task['id']
    assert job['status'] == 'pending' and job['version'] == 1, job
    assert (stored['status'], job['wait_reason']) in {
        ('queued', 'dependency'), ('active', 'draining')}, (stored, job)
    assert job['resources'] == task['resources'], job
    return stored['id'], stored['status'] == 'active'


def submit_and_resume(files):
    """Replay the original request IDs, checkpoint receipts, then resume once."""
    batches, jobs, snapshot = complete_status()
    assert snapshot['daemon_health']['draining'] and not snapshot['daemon_health']['frozen']
    assert not any(j['status'] == 'running' for j in jobs), 'Running work appeared'
    expected = [read(Path(filename)) for filename in files]
    names = {batch['name'] for batch in expected}
    assert len(names) == len(files)
    existing = [b for b in batches if b['name'] in names]
    assert len(existing) == len({b['name'] for b in existing}), 'Duplicate migration batches'
    receipts = []
    publish('submitting_while_drained', count=len(files))
    for filename, batch in zip(files, expected):
        task = batch['tasks'][0]
        # Do not change this ID, path or binding on retries: request replays its receipt.
        request_id = 'resource-rollout-2333-'+task['id']
        print(cli('request',request_id,'--expect-revision','0','--','submit',filename), flush=True)
        current = cli('status',batch['name'],'--json',structured=True)
        batch_id, _ = paused_submission(current, batch, task)
        print(cli('verify',batch_id),flush=True)
        receipts.append({'batch_id':batch_id,'task':task['id'],'request_id':request_id})
        temporary = OPS/'receipts.pending.json'
        temporary.write_text(json.dumps(receipts,indent=2))
        os.replace(temporary, OPS/'receipts.json')
    # Wait for queued -> active convergence, using immutable IDs from the receipts.
    deadline = time.monotonic()+180
    while True:
        ready = []
        for batch, receipt in zip(expected, receipts):
            current = cli('status',receipt['batch_id'],'--json',structured=True)
            batch_id, active = paused_submission(current, batch, batch['tasks'][0])
            assert batch_id == receipt['batch_id']
            ready.append(active)
        if all(ready):
            break
        if time.monotonic() >= deadline:
            raise RuntimeError('Submitted batches did not activate; keep daemon drained')
        time.sleep(2)
    print(cli('daemon','resume'),flush=True)
    for _ in range(30):
        time.sleep(5)
        _, jobs, snapshot = complete_status()
        running = [j for j in jobs if j['status']=='running' and j['batch_name'] in names]
        if running:
            assert len(running) <= 2
            assert snapshot['cpu']['used'] <= 120
            assert snapshot['host_memory']['used_gib'] <= 96
            assert snapshot['daemon_health']['draining'] is False
            assert snapshot['daemon_health']['frozen'] is False
            publish('active', release=str(RELEASE), receipts=receipts, running=[j['id'] for j in running],
                    cpu=snapshot['cpu'], host_memory=snapshot['host_memory'], daemon_health=snapshot['daemon_health'])
            return
    raise RuntimeError('No new experiment launched within 150 seconds; inspect the preserved queue')


def main(check_only=False, continuing=False):
    files = verify_files(continuing=continuing)
    if check_only:
        print('PASS: node/lease, 120 CPUs, CLI config, release and campaign seals, 22 independent batches')
        return
    if continuing:
        submit_and_resume(files)
        return
    original = WRAPPER.read_bytes()
    backup = OPS/'sched-wrapper.before'
    with backup.open('xb') as stream:
        stream.write(original)
    publish('waiting_for_old_training', deadline_hours=8, excluded='native_traffic_h96_seed42')
    deadline = time.monotonic()+8*3600
    while time.monotonic() < deadline:
        lines = (OPS/'drain_old_pool.log').read_text().splitlines()
        drained = any(line.startswith('{') and 'drained_at' in json.loads(line) for line in lines)
        if drained:
            old = cli('status', OLD_BATCH, '--json', structured=True)
            assert all(j['status']=='cancelled' for j in old['jobs']), 'Old pool still owns work'
            batches, jobs, snapshot = complete_status()
            if not any(j['status']=='running' for j in jobs):
                # No runnable work may race the old daemon's stop boundary.
                active = {b['id'] for b in batches if b['status']=='active'}
                assert not any(j['status']=='pending' and j['batch_id'] in active for j in jobs), 'Other active work arrived'
                break
        time.sleep(20)
    else:
        raise RuntimeError('Eight-hour drain deadline reached; no restart performed')
    verify_files()
    assert WRAPPER.read_bytes() == original
    publish('stopping_idle_daemon')
    print(cli('daemon','stop'), flush=True)
    stopped = cli('status','--json',structured=True)
    assert stopped['daemon_health']['heartbeat_age_s'] is None, 'Daemon heartbeat still present'
    rewritten = original.decode().replace(OLD_PATH, NEW_PATH)
    assert rewritten != original.decode()
    temporary = WRAPPER.with_name('sched.resource-release.pending')
    with temporary.open('x') as stream:
        stream.write(rewritten)
        stream.flush(); os.fsync(stream.fileno())
    temporary.chmod(WRAPPER.stat().st_mode & 0o777)
    os.replace(temporary, WRAPPER)
    # The new daemon starts paused; no partial migration can start training.
    print(cli('daemon','drain'), flush=True)
    patch = OPS/'resource-config.json'
    with patch.open('x') as stream:
        json.dump({'host_mem_total_gib':96, 'host_mem_reserve_gib':16, 'host_mem_default_gib':8,
                   'projects':{'wmpp_ett_trial_seed42':{'gpu_quota':2}}}, stream)
    print(cli('request','resource-rollout-2333-20260922-config','--expect-revision','0','--',
              'config','set','-f',str(patch),'--yes'), flush=True)
    print(cli('daemon','check'), flush=True)
    print(cli('daemon','start'), flush=True)
    submit_and_resume(files)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--check-only', action='store_true')
    parser.add_argument('--continue-submission', action='store_true',
                        help='Continue a paused, already configured deployment with the original request IDs')
    parser.add_argument('--release-path', type=Path, default=RELEASE)
    args = parser.parse_args()
    # Preserve the /home alias used by the wrapper; resolve() rewrites it to /mnt.
    RELEASE = args.release_path.absolute()
    NEW_PATH = str(RELEASE)+'${PYTHONPATH:+:$PYTHONPATH}'
    try:
        main(args.check_only, args.continue_submission)
    except BaseException as error:
        if not args.check_only:
            publish('failed', error=repr(error))
        raise
