"""Prepare a new immutable campaign; never submit or modify old results."""
import copy
import hashlib
import json
import math
from pathlib import Path
import shutil

BASE = Path('/home/zzhang54/zzhang54/Error_checking/trials')
OLD = BASE/'waveletmixer_script_seeds_20260919'
POOL = BASE/'seed_campaigns_resume_2333_20260921'
NEW = BASE/'waveletmixer_sched_20260922'
PY = '/home/zzhang54/miniconda3/envs/zzhang_venv/bin/python'


def read(path):
    return json.loads(path.read_text())


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x') as stream:
        json.dump(value, stream, indent=2, ensure_ascii=False)
        stream.write('\n')


def main():
    pool = read(POOL/'config/pool.json')
    done = {p.parent.name: read(p) for p in (POOL/'state/tasks').glob('*/result.json')}
    held = {p.parent.name for p in (POOL/'state/tasks').glob('*/claim.json')}
    pending = [t for t in pool['tasks'] if t['key'] not in done and t['key'] not in held]
    assert len(pending) == 26 and all(t['campaign'] == 'native' for t in pending)
    held_preflight = []
    runnable = []
    for task in pending:
        profile_path = POOL/'state/profiles'/task['profile_key']/'profile.json'
        profile = read(profile_path) if profile_path.exists() else None
        if profile is not None and (not profile.get('passed') or
                profile['max_cuda_reserved_bytes']*1.2+.25*1024**3 > .8*profile['gpu_total_bytes']):
            held_preflight.append({'task':task, 'profile':str(profile_path), 'reason':'Known full-batch preflight does not fit the unchanged 80% GPU budget'})
        else:
            runnable.append(task)
    assert len(runnable) == 22 and len(held_preflight) == 4
    peaks = {}
    for path in (OLD/'state').glob('*/result.json'):
        value = read(path)
        if value['status'] != 'complete':
            continue
        evidence = Path(value['run_directory'])/'runtime_peaks.json'
        if evidence.exists():
            measured = read(evidence)['host_known_pss_bytes']/1024**3
            shape = (value['dataset'], value['horizon'])
            peaks[shape] = max(peaks.get(shape, 0), measured)
    original = read(OLD/'plans/jobs.json')
    by_id = {j['job_id']: j for j in original['jobs']}
    NEW.mkdir(exist_ok=False)
    shutil.copytree(OLD/'source_snapshot', NEW/'source_snapshot')
    (NEW/'scripts').mkdir()
    for name in ('common.py', 'metrics.py', 'resource_guard.py', 'execution.py', 'run_one.py', 'probe.py'):
        shutil.copy2(OLD/'scripts'/name, NEW/'scripts'/name)
    # The final guard is fail-fast. Queueing belongs to sched, before GPU allocation.
    guard = NEW/'scripts/resource_guard.py'
    source = guard.read_text()
    needle = '    import fcntl\n    if min(estimate_gib, reserve_gib, total_budget_gib) <= 0:'
    replacement = "    import fcntl\n    timeout = 0  # sched owns admission; never wait holding an assigned GPU\n    if min(estimate_gib, reserve_gib, total_budget_gib) <= 0:"
    assert source.count(needle) == 1
    guard.write_text(source.replace(needle, replacement))
    runner = NEW/'scripts/run_one.py'
    source = runner.read_text()
    needle = '            while child.poll() is None:\n                reason = pressure_reason'
    replacement = "            while child.poll() is None:\n                if time.time() >= datetime.datetime.fromisoformat('2026-09-28T22:11:15+00:00').timestamp() - 180:\n                    raise CampaignCancelled('Lease shutdown boundary reached')\n                reason = pressure_reason"
    assert source.count(needle) == 1
    runner.write_text(source.replace(needle, replacement))
    checks = '''import argparse, datetime, json, os
from pathlib import Path
from common import ROOT, load_job, read_json
p = argparse.ArgumentParser(); p.add_argument('--job-id', required=True); p.add_argument('--profile', action='store_true'); a=p.parse_args()
plan, job = load_job(a.job_id)
if not a.profile:
    assert os.uname().nodename == 'ambiorix' and os.environ.get('SLURM_JOB_ID') == '2333', 'Wrong node/lease'
    remaining = datetime.datetime.fromisoformat('2026-09-28T22:11:15+00:00').timestamp() - datetime.datetime.now(datetime.timezone.utc).timestamp()
    assert remaining > 3600, 'Lease admission closed'
    old = Path('/home/zzhang54/zzhang54/Error_checking/trials/waveletmixer_script_seeds_20260919')
    assert not (old/'state'/a.job_id/'result.json').exists(), 'Original result exists; do not repeat'
    assert not list((old/'results'/a.job_id).glob('*')), 'Original attempt exists; do not repeat'
else:
    profile = read_json(ROOT/'probes'/a.job_id/'profile.json')
    assert profile.get('passed') and profile.get('full_batch') and profile.get('training_batches_per_phase', 0) >= 2, 'Full-batch preflight failed'
    assert profile['profile_key'] == job['profile_key'], 'Wrong shape profile'
    assert profile['source_sha256'] == plan['source_identity']['sha256'], 'Scientific source mismatch'
    assert profile['max_cuda_reserved_bytes']*1.2 + .25*1024**3 <= .8*profile['gpu_total_bytes'], 'Full batch exceeds 80% GPU budget; parameters unchanged'
print(json.dumps({'job':a.job_id, 'check':'profile' if a.profile else 'migration', 'passed':True}))
'''
    (NEW/'scripts/check_admission.py').write_text(checks)
    jobs, tasks, evidence = [], [], []
    for task in sorted(runnable, key=lambda t: (t['priority'], t['seed'] != 42, t['key'])):
        job = copy.deepcopy(by_id[task['local_key']])
        shape = (job['dataset'], job['horizon'])
        measured = peaks.get(shape)
        reason = 'completed same-shape PSS peak * 1.5 + 4 GiB, rounded up to 4 GiB'
        if measured is None and shape == ('traffic', 96):
            measured = peaks[('traffic', 192)]
            reason = 'conservative next-larger horizon PSS peak * 1.5 + 4 GiB'
        estimate = max(16, math.ceil((measured*1.5+4)/4)*4) if measured is not None else task['host_estimate_gib']
        if measured is None:
            reason = 'no completed comparable run; retain original conservative reservation'
        job['resource_policy'].update(host_estimate_gib=estimate, host_admission_timeout_seconds=0,
             host_total_budget_gib=96, host_reserve_gib=16, gpu_memory_fraction=.8,
             host_shared_root=str(NEW/'shared_host_memory'))
        jobs.append(job)
        jid = job['job_id']
        profile_path = POOL/'state/profiles'/task['profile_key']/'profile.json'
        if profile_path.exists():
            profile = read(profile_path)
            profile.update(profile_key=job['profile_key'], reused_from=str(profile_path),
                           original_pool_profile_key=task['profile_key'])
            write(NEW/'probes'/jid/'profile.json', profile)
        base = [PY, '-B', '-u']
        tasks.append({'id':task['key'], 'stages':[
            {'cmd':base+[str(NEW/'scripts/check_admission.py'), '--job-id', jid]},
            {'cmd':base+[str(NEW/'scripts/probe.py'), '--job-id', jid, '--output', str(NEW/'probes'/jid)]},
            {'cmd':base+[str(NEW/'scripts/check_admission.py'), '--job-id', jid, '--profile']},
            {'cmd':base+[str(NEW/'scripts/run_one.py'), '--job-id', jid]}],
            'cwd':str(NEW), 'git':False, 'paths_escape':True,
            'resources':{'gpu':1, 'gpu_share':False, 'cpus':30, 'host_mem_gib':estimate},
            'max_retry':0, 'duration_min':4320,
            'env':{'PYTHONDONTWRITEBYTECODE':'1', 'OMP_NUM_THREADS':'1', 'MKL_NUM_THREADS':'1',
                   'OPENBLAS_NUM_THREADS':'1', 'WMPP_GPU_MEMORY_FRACTION':'0.8'},
            'artifacts':{'result':{'path':str(NEW/'state'/jid/'result.json'), 'check':'json', 'has_key':'status'}}})
        evidence.append({'task':task['key'], 'host_mem_gib':estimate, 'completed_peak_gib':measured, 'reason':reason})
    original['jobs'] = jobs
    write(NEW/'plans/jobs.json', original)
    write(NEW/'batch.json', {'name':NEW.name, 'project':'wmpp_ett_trial_seed42', 'mode':'mix', 'tasks':tasks})
    batch_files = []
    for task in tasks:
        path = NEW/'batches'/(task['id']+'.json')
        write(path, {'name':NEW.name+'_'+task['id'], 'project':'wmpp_ett_trial_seed42', 'mode':'mix', 'tasks':[task]})
        batch_files.append(str(path))
    write(NEW/'batches/index.json', {'files':batch_files, 'note':'Submit each experiment independently; batch.json is review-only.'})
    write(NEW/'held_preflight.json', {'tasks':held_preflight})
    write(NEW/'migration.json', {'old_pool':str(POOL), 'old_campaign':str(OLD), 'lease':pool['lease'],
          'completed_preserved':[k for k,v in done.items() if v['status']=='complete'],
          'failures_preserved':[k for k,v in done.items() if v['status']!='complete'],
          'in_flight_excluded':sorted(held-set(done)), 'resources':evidence,
          'known_preflight_held':[t['task']['key'] for t in held_preflight],
          'scientific_arguments_unchanged':True, 'new_job_count':len(tasks)})
    manifest = {p.relative_to(NEW).as_posix():hashlib.sha256(p.read_bytes()).hexdigest()
                for base in [NEW/'scripts', NEW/'plans', NEW/'source_snapshot', NEW/'batches', NEW/'probes']
                for p in base.rglob('*') if p.is_file()}
    manifest['batch.json'] = hashlib.sha256((NEW/'batch.json').read_bytes()).hexdigest()
    write(NEW/'REVIEW_MANIFEST.json', {'files':manifest, 'source_identity':original['source_identity']['sha256']})
    print(json.dumps({'new_root':str(NEW), 'tasks':len(tasks), 'excluded':sorted(held-set(done)), 'resources':evidence}))


if __name__ == '__main__':
    main()
