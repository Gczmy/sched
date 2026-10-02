"""Explicit manual CUDA acceptance on an authorized, otherwise idle compute GPU.

Uses only public sched CLI and test-owned processes; retains private state/evidence.
Never discovers or changes an existing scheduler, another user's process or config.
"""
import argparse
import getpass
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]

class Acceptance:
    def __init__(self, work, gpu):
        self.work = work.resolve()
        self.work.mkdir(mode=0o700)  # Existing directories are refused.
        self.gpu = gpu
        self.env = {k:v for k,v in os.environ.items() if not k.startswith('SCHED_') and k!='CUDA_VISIBLE_DEVICES'}
        self.env.update(SCHED_STATE=str(self.work/'state'), SCHED_CONFIG=str(self.work/'config.json'), PYTHONPATH=str(ROOT), PYTHONDONTWRITEBYTECODE='1')
        executable = os.path.realpath(sys.executable)
        self.project = self.work/'project'
        self.project.mkdir(mode=0o700)
        (self.project/'worker.py').write_bytes((ROOT/'tests/fixtures/cuda_recovery_worker.py').read_bytes())
        self.config = {'schema_version':1,'node':socket.gethostname(),'user':getpass.getuser(),
            'state_dir':str(self.work/'state'),'gpus':[gpu],'cpus_total':4,'gpu_job_cpus':1,
            'default_project':'example','projects':{'example':{'root':str(self.project),'git':False,'gpu_admission':{}}},
            'venvs':{'python':sys.executable},'co_locate':True,'co_locate_safety':0.85}
        self.config['execution_backends'] = {name:{'kind':'linux_fd_owner','executable':executable,
            'sha256':hashlib.sha256(Path(executable).read_bytes()).hexdigest(), 'argv':[executable,'worker.py',group],
            'env':{'PYTHONPATH':str(ROOT)},'projects':['example'],'input_slots':{}}
            for group,name in ((g,'cuda-'+g) for g in ('a','b','c','task'))}
        (self.work/'config.json').write_text(json.dumps(self.config))
        self.log = (self.work/'foreground.log').open('w')
        self.proc = None
        self.retry = {'cooldown_sec':1}
        self.occupier = None
        self.evidence = {'schema_version':1,'version':self.cli('--version',as_json=False).strip(),'checks':[],'measurements':[]}

    def cli(self,*args,as_json=True):
        p=subprocess.run([sys.executable,'-m','gsched.cli',*args],env=self.env,capture_output=True,text=True,timeout=45)
        if p.returncode:
            raise RuntimeError(f'CLI {args} failed: {p.stdout}{p.stderr}')
        return json.loads(p.stdout) if as_json else p.stdout

    def check(self,name):
        self.evidence['checks'].append(name)
        print('PASS: '+name,flush=True)
        (self.work/'evidence.json').write_text(json.dumps(self.evidence,indent=2)+'\n')

    def wait(self,fn,timeout=90):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            result=fn()
            if result: return result
            if self.proc is not None and self.proc.poll() is not None:
                raise RuntimeError('foreground exited unexpectedly: '+(self.work/'foreground.log').read_text())
            time.sleep(.25)
        raise TimeoutError('condition did not converge; inspect private acceptance logs')

    def health(self): return self.cli('daemon','status','--json')
    def jobs(self,batch): return self.cli('status',batch,'--json')['jobs']
    def latest(self,batch,task='task'): return next(j for j in self.jobs(batch) if j['task']==task)

    def start(self):
        command="from gsched import dispatcher; dispatcher.POLL_SEC=1; from gsched import cli; raise SystemExit(cli.main(['daemon','foreground','--supervise','--restart-delay-sec','1','--max-restarts','3']))"
        self.proc=subprocess.Popen([sys.executable,'-c',command],env=self.env,stdout=self.log,stderr=subprocess.STDOUT)
        self.wait(lambda:self.health()['health_state']=='healthy')

    def stop(self):
        if self.proc is not None and self.proc.poll() is None:
            self.cli('daemon','stop',as_json=False)
            self.proc.wait(timeout=130)
        self.proc=None

    def submit(self,name,tasks):
        path=self.work/(name+'.json')
        path.write_text(json.dumps({'name':name,'project':'example','force_rerun':True,'tasks':tasks}))
        return self.cli('submit',str(path),'--json')['batch_id']

    def task(self,group='task',smoke=None,share=False):
        task={'id':group,'cmd':[os.path.realpath(sys.executable),'worker.py',group],'git':False,
              'resources':{'gpu':1,'gpu_share':share,'vram_gib':12,'cpus':1,'host_mem_gib':.25},
              'max_retry':0,'execution':{'backend':'cuda-'+group,'inputs':{}},
              'recovery':{'protocol':'sched-recovery/v1','mode':'smoke' if smoke is None else 'run',
                'code':{'worker.py':hashlib.sha256((self.project/'worker.py').read_bytes()).hexdigest()},
                'config':{'settings.json':hashlib.sha256((self.project/'settings.json').read_bytes()).hexdigest()},
                'inputs':{},'retry':self.retry}}
        if smoke is not None: task['recovery']['smoke_job_id']=smoke
        return task

    def seed(self,name,settings,groups=('task',),share=False,retry=None):
        self.retry = retry or {'cooldown_sec':1}
        (self.project/'settings.json').write_text(json.dumps(settings))
        smoke=self.submit(name+'-smoke',[self.task(g,share=share) for g in groups])
        self.wait(lambda:all(j['status']=='done' for j in self.jobs(smoke)))
        ids={j['task']:j['id'] for j in self.jobs(smoke)}
        return [self.task(g,ids[g],share=share) for g in groups]

    def free(self):
        return float(subprocess.check_output(['nvidia-smi','--query-gpu=memory.free','--format=csv,noheader,nounits','-i',str(self.gpu)],text=True).strip())/1024

    def occupy(self,target):
        self.release_occupier()
        path=self.work/'occupier.log'
        out=path.open('w')
        self.occupier=subprocess.Popen([sys.executable,str(self.project/'worker.py'),'--occupy',str(self.gpu),str(target)],env=self.env,stdout=out,stderr=subprocess.STDOUT)
        out.close()
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            if self.occupier.poll() is not None: raise RuntimeError(path.read_text())
            if '"ready": true' in path.read_text(): break
            time.sleep(.1)
        else: raise TimeoutError('occupier startup')
        free=self.free()
        self.evidence['measurements'].append({'target_free_gib':target,'actual_free_gib':free})
        assert abs(free-target)<.1,(target,free)
        return free

    def release_occupier(self):
        if self.occupier is not None:
            if self.occupier.poll() is None:self.occupier.terminate()
            self.occupier.wait(timeout=30)
            self.occupier=None

    def patch(self,value):
        p=self.work/'patch.json';p.write_text(json.dumps(value))
        self.cli('config','set','-f',str(p),'--yes',as_json=False)
        time.sleep(2)

    def pending_for(self,batch,seconds=4):
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            assert all(j['status']=='pending' for j in self.jobs(batch))
            time.sleep(.4)

    def attempts(self,batch,group='task'):
        return self.cli('execution',f'{batch}:{group}','--json')['attempts']

    def run(self):
        self.cli('daemon','check','--json')
        self.start()
        plain={'id':'task','cmd':[sys.executable,'worker.py','--ordinary'],'git':False,'resources':{'gpu':1,'cpus':1,'host_mem_gib':.25},'max_retry':0}
        assert self.occupy(12.15)>=12
        batch=self.submit('external-disabled',[plain]);self.pending_for(batch)
        self.patch({'projects':{'example':{'gpu_admission':{'allow_external_occupancy':True}}}})
        self.wait(lambda:self.latest(batch)['status']=='done')
        assert json.loads((self.project/'ordinary-result.json').read_text())['cuda_result']==1
        self.check('known external occupancy needs explicit permission; real CUDA kernel succeeds')
        assert self.occupy(11.90)<12
        batch=self.submit('below-floor',[plain]);self.pending_for(batch)
        assert self.occupy(12.15)>=12
        self.wait(lambda:self.latest(batch)['status']=='done')
        self.check('measured free memory below 12 GiB blocks; above 12 GiB resumes')
        self.release_occupier()
        tasks=self.seed('tiers',{'total':5,'oom_at':[2]},retry={'cooldown_sec':1,'min_free_gib_by_round':[12,20]})
        self.occupy(12.15)
        batch=self.submit('tiers',tasks)
        self.wait(lambda:self.latest(batch)['version']==2)
        self.pending_for(batch)
        self.release_occupier()
        self.wait(lambda:self.latest(batch)['status']=='done')
        assert self.latest(batch)['version']==2
        self.check('real OOM recovery waits for a higher 20 GiB tier and resumes when memory improves')
        (self.project/'starts.jsonl').unlink()
        self.patch({'projects':{'example':{'gpu_admission':{'allow_external_occupancy':False}}}})
        tasks=self.seed('fifo',{'total':5,'groups':{'a':{'oom_at':[2,4]}}},('a','b','c'))
        batch=self.submit('fifo',tasks)
        self.wait(lambda:all(j['status']=='done' for j in self.jobs(batch)),timeout=180)
        order=[json.loads(line)['group'] for line in (self.project/'starts.jsonl').read_text().splitlines()]
        assert order==['a','b','c','a','a'],order
        for group in ('a','b','c'):
            assert json.loads((self.project/f'result-{group}.json').read_text())['results']==[1,2,3,4,5]
        old=self.attempts(batch,'a');assert len(old)==3
        assert sorted(a['observation']['returncode'] for a in old)==[0,42,42]
        assert all(a['observation']['group_clean'] for a in old)
        self.check('real CUDA OOM saves progress; normal groups precede FIFO recovery; old waits retained')
        tasks=self.seed('reservation',{'total':5,'hold_step':2},('a','b'),share=True)
        batch=self.submit('reservation',tasks)
        self.wait(lambda:self.latest(batch,'a')['status']=='running')
        time.sleep(4)
        assert self.latest(batch,'b')['status']=='pending'
        (self.project/'release').touch()
        self.wait(lambda:all(j['status']=='done' for j in self.jobs(batch)))
        (self.project/'release').unlink()
        self.check('declared peaks reserve outstanding memory before delayed CUDA allocation')
        tasks=self.seed('restart',{'total':5,'hold_step':2})
        batch=self.submit('restart',tasks)
        self.wait(lambda:self.cli('recovery',f'{batch}:task','--json')['checkpoint']['state']=='verified')
        previous=self.attempts(batch)[0]['attempt_id'];owner=self.health()['pid']
        os.kill(owner,signal.SIGKILL)
        self.wait(lambda:self.health()['health_state']=='healthy' and self.health()['pid']!=owner)
        (self.project/'release').touch()
        self.wait(lambda:self.latest(batch)['status']=='done')
        attempts=self.attempts(batch)
        assert len(attempts)==1 and attempts[0]['attempt_id']==previous and attempts[0]['observation']['returncode']==0
        (self.project/'release').unlink()
        self.check('actual daemon SIGKILL reconnects persistent CUDA owner and original wait')
        tasks=self.seed('kill',{'total':5,'hold_step':2})
        batch=self.submit('kill',tasks)
        self.wait(lambda:self.cli('recovery',f'{batch}:task','--json')['checkpoint']['state']=='verified')
        original=self.wait(lambda:next((a for a in self.attempts(batch) if a['observation'] and a['observation'].get('pid')),None))
        os.kill(original['observation']['pid'],signal.SIGKILL)
        (self.project/'release').touch()
        self.wait(lambda:self.latest(batch)['status']=='done')
        attempts=self.attempts(batch)
        assert len(attempts)==2 and sorted(a['observation']['returncode'] for a in attempts)==[-9,0]
        assert self.latest(batch)['version']==2
        assert json.loads((self.project/'result-task.json').read_text())['results']==[1,2,3,4,5]
        (self.project/'release').unlink()
        self.check('actual CUDA worker SIGKILL preserves rc=-9 and resumes checkpoint in a new version')
        tasks=self.seed('controls',{'total':5,'hold_step':2})
        batch=self.submit('controls',tasks)
        self.wait(lambda:self.latest(batch)['status']=='running')
        self.patch({'projects':{'example':{'gpu_enabled':False}}})
        rejected=subprocess.run([sys.executable,'-m','gsched.cli','submit',str(self.work/'controls.json'),'--json'],env=self.env,capture_output=True,text=True,timeout=30)
        assert rejected.returncode!=0
        self.cli('daemon','drain','--stop-when-idle',as_json=False)
        (self.project/'release').touch()
        self.proc.wait(timeout=60)
        assert self.proc.returncode==0 and self.latest(batch)['status']=='done'
        assert self.health()['health_state']=='stopped'
        self.cli('daemon','resume',as_json=False)
        self.patch({'projects':{'example':{'gpu_enabled':True}}})
        self.start()
        self.stop()
        self.check('GPU disable preserves running work; drain exits without restart; resume and explicit stop work')

    def close(self):
        self.release_occupier()
        self.stop()
        self.log.close()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--gpu',type=int,required=True)
    p.add_argument('--work-dir',type=Path,required=True,help='new private directory; evidence and state are retained')
    args=p.parse_args()
    from gsched.execution import LinuxFdBackend
    LinuxFdBackend()  # Native must be explicitly built; never silently skip.
    probe=subprocess.check_output(['nvidia-smi','--query-compute-apps=pid,gpu_uuid','--format=csv,noheader'],text=True)
    uuid=subprocess.check_output(['nvidia-smi','--query-gpu=uuid','--format=csv,noheader','-i',str(args.gpu)],text=True).strip()
    assert uuid and not any(uuid in line for line in probe.splitlines()),'selected GPU has existing compute users'
    free=float(subprocess.check_output(['nvidia-smi','--query-gpu=memory.free','--format=csv,noheader,nounits','-i',str(args.gpu)],text=True).strip())/1024
    assert free>=20,'acceptance requires at least 20 GiB free at start'
    runner=Acceptance(args.work_dir,args.gpu)
    try:runner.run()
    finally:runner.close()
    print('PASS: all real GPU recovery acceptance checks',flush=True)

if __name__=='__main__':main()
