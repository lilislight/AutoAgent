"""Resumable B0/final/V1 comparison and four serial one-hour stability runs.

The default formal matrix uses five independent processes, twenty calls per
case and 128 unique-history calls. All database credentials stay in the ignored
output tree. Run heavy phases serially, using the repository virtual environment.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[3]


def hashes(directory):
    return {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in directory.rglob('*.py') if '__pycache__' not in p.parts}


def freeze(directory):
    manifest = directory/'provenance.json'
    if manifest.exists():
        recorded = json.loads(manifest.read_text())
        for label in ('b0','final','harness'):
            if hashes(directory/label) != recorded['hashes'][label]:
                raise RuntimeError('Frozen files changed: ' + label)
        if hashes(ROOT/'v1/autoagent') != recorded['hashes']['v1']:
            raise RuntimeError('Archived V1 changed during the experiment.')
        return
    original = ROOT/'.autoagent/performance/v2-optimization/step-00'
    expected = json.loads((original/'environment.json').read_text())['source_hashes']
    for name, digest in expected.items():
        if hashlib.sha256((original/name).read_bytes()).hexdigest() != digest:
            raise RuntimeError('B0 differs from its original recorded hash: ' + name)
    directory.mkdir(parents=True, exist_ok=True)
    ignore = shutil.ignore_patterns('__pycache__', '*.pyc')
    shutil.copytree(original/'source', directory/'b0', ignore=ignore)
    shutil.copytree(ROOT/'autoagent', directory/'final/autoagent', ignore=ignore)
    (directory/'harness').mkdir()
    for name in ('run.py','worker.py','business.py','diagnostics.py','acceptance.py'):
        shutil.copy2(Path(__file__).with_name(name), directory/'harness'/name)
    (directory/'workspace.patch').write_bytes(subprocess.check_output(['git','diff'],cwd=ROOT))
    h={label:hashes(directory/label) for label in ('b0','final','harness')}
    h['v1']=hashes(ROOT/'v1/autoagent')
    delta={name:dict(b0=h['b0'].get(name),final=h['final'].get(name))
           for name in h['b0'].keys()|h['final'].keys() if h['b0'].get(name)!=h['final'].get(name)}
    manifest.write_text(json.dumps(dict(created_at_us=time.time_ns()//1000,
        git_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        git_status=subprocess.check_output(['git','status','--short'],cwd=ROOT,text=True),
        hashes=h,source_delta=delta,python=sys.version,platform=platform.platform(),
        packages=sorted((d.metadata['Name'],d.version) for d in importlib.metadata.distributions()
                        if d.metadata['Name']),
        design=dict(repeats=5,calls=20,unique_calls=128,soak_seconds=3600,
                    artifact_bytes=8192,soak_definitions=64,soak_period_seconds=1),
        notes=['Actual B0 package from Step 0; unrelated Agent/trace changes are listed in source_delta.',
               'Shared host; no private CPU/cgroup isolation. Heavy phases are serial.',
               'V1 source stays archived and runs with cwd=v1.']),indent=2))


def regression(directory):
    target=directory/'verification';target.mkdir(exist_ok=True)
    done={json.loads(x)['module'] for x in (target/'modules.jsonl').read_text().splitlines()} if (target/'modules.jsonl').exists() else set()
    for path in sorted((ROOT/'tests').glob('test_*.py')):
        module='tests.'+path.stem
        if module in done:continue
        start=time.monotonic();logfile=target/(path.stem+'.log')
        print('TEST',module,flush=True)
        with logfile.open('w')as stream:
            result=subprocess.run([sys.executable,'-m','unittest',module,'-v'],cwd=ROOT,stdout=stream,stderr=subprocess.STDOUT)
        text=logfile.read_text();counts=re.findall(r'Ran (\d+) tests?',text)
        record=dict(module=module,exit_code=result.returncode,tests=int(counts[-1])if counts else 0,
                    seconds=time.monotonic()-start,
                    postgres_tests=sum(s.startswith('test_')and'PostgreSQL'in s for s in text.splitlines()),
                    skips=[s for s in text.splitlines()if'... skipped'in s],
                    failures=[s for s in text.splitlines()if s.startswith(('FAIL:','ERROR:'))])
        with (target/'modules.jsonl').open('a')as stream:stream.write(json.dumps(record)+'\n')
        print(json.dumps(record),flush=True)
    rows=[json.loads(x)for x in (target/'modules.jsonl').read_text().splitlines()]
    check=subprocess.run(['git','diff','--check'],cwd=ROOT,capture_output=True,text=True)
    summary=dict(modules=len(rows),tests=sum(r['tests']for r in rows),
                 postgres_tests=sum(r['postgres_tests']for r in rows),
                 skipped=sum(len(r['skips'])for r in rows),failed_modules=[r for r in rows if r['exit_code']],
                 compileall_exit_code=subprocess.run([sys.executable,'-m','compileall','-q','autoagent','tests'],cwd=ROOT).returncode,
                 diff_check_exit_code=check.returncode,diff_check_output=check.stdout+check.stderr,
                 test_hashes=hashes(ROOT/'tests'))
    (target/'summary.json').write_text(json.dumps(summary,indent=2))
    print('REGRESSION',json.dumps({k:v for k,v in summary.items()if k!='test_hashes'}),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',type=Path,default=ROOT/'.autoagent/performance/v2-optimization/step-09')
    parser.add_argument('--phase',choices=('smoke','verify','formal','extras','diagnostic','soak','all'),default='all')
    args=parser.parse_args();directory=args.directory.resolve();freeze(directory)
    spec=importlib.util.spec_from_file_location('acceptance_harness',directory/'harness/run.py')
    h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h);h.ROOT=ROOT
    if args.phase in ('verify','all'):regression(directory)
    if args.phase=='verify':return
    jobs=[]
    def add(phase,backend,version,mode,case,turn=0,env=None,duration=0):
        jobs.append(dict(phase=phase,backend=backend,version=version,mode=mode,case=case,
                         turn=turn,environment=env or {},duration=duration))
    versions=(('b0','durable'),('final','durable'),('v1','full'),('b0','buffered'),('final','buffered'),('v1','standard'))
    if args.phase=='smoke':
        for backend in ('sqlite','postgres'):
            for version,mode in versions:
                add('smoke',backend,version,mode,dict(name='smoke',family='chain',nodes=1,payload_bytes=16384,calls=2))
    if args.phase in ('formal','all'):
        cases=h.cases('artifact')
        for turn in range(5):
            order=versions if turn%2==0 else tuple(reversed(versions))
            for case in cases[turn%6:]+cases[:turn%6]:
                case=dict(case,calls=128 if case['name']=='unique-history'else 20)
                for backend in (('sqlite','postgres')if turn%2==0 else('postgres','sqlite')):
                    for version,mode in order:add('formal',backend,version,mode,case,turn)
    if args.phase in ('extras','all'):
        cases=[dict(name='default-threshold',family='chain',nodes=10,payload_bytes=16384,calls=10),
               dict(name='conditional-history',family='wide',width=16,conditional=True,reuse_session=True,payload_bytes=16384,calls=16),
               dict(name='graph-128',family='chain',nodes=128,payload_bytes=1024,calls=5),
               dict(name='concurrent-8',family='chain',nodes=10,payload_bytes=16384,calls=16,concurrency=8,captured_frontier_barrier=True),
               dict(name='slow-storage',family='chain',nodes=10,payload_bytes=16384,calls=5,persist_delay_ms=20),
               dict(name='wait-resume',family='wait',payload_bytes=16384,calls=3),
               dict(name='crash-at-wait',family='wait',payload_bytes=16384,calls=2,restart_at_wait=True),
               dict(name='storage-fault',family='chain',nodes=4,payload_bytes=16384,calls=2,
                    persist_fail_after=2,timeout_seconds=5,skip_warmup=True)]
        for turn in range(3):
            for backend in ('sqlite','postgres'):
                for case in cases:
                    if turn and case['name']in ('wait-resume','crash-at-wait','storage-fault'):continue
                    for version,mode in (versions[:2]+versions[3:5] if turn%2==0 else tuple(reversed(versions[:2]+versions[3:5]))):
                        add('extras',backend,version,mode,case,turn)
    if args.phase in ('diagnostic','all'):
        for backend in ('sqlite','postgres'):
            for version,mode in versions:
                if version=='v1':continue
                add('diagnostic',backend,version,mode,dict(name='sql-chain',family='chain',nodes=10,
                    payload_bytes=16384,calls=5,instrument_sql=True))
            for mode in ('durable','buffered'):
                add('diagnostic',backend,'final',mode,dict(name='cache-pressure',family='chain',nodes=4,
                    definitions=64,rotate_definitions=True,payload_bytes=16384,unique_payload=True,
                    calls=128,instrument_sql=True),env={'AUTOAGENT_WORKFLOW_GRAPH_CACHE_BYTES':'16384','AUTOAGENT_WRITER_METADATA_CACHE_BYTES':'16384'})
    if args.phase in ('soak','all'):
        for backend in ('sqlite','postgres'):
            for mode in ('durable','buffered'):
                add('soak',backend,'final',mode,dict(name='unique-session-revision-churn',family='chain',nodes=4,
                    definitions=64,rotate_definitions=True,payload_bytes=16384,unique_payload=True,calls=3600,
                    period_seconds=1,sample_health_every=60),duration=3600)
    results=directory/'results.jsonl'
    done={json.loads(x)['job'] for x in results.read_text().splitlines()
          if json.loads(x).get('accepted_result')}if results.exists()else set()
    for job in jobs:
        case=job['case'];version=job['version'];mode=job['mode']
        identity=f"{job['phase']}-{job['backend']}-{case['name']}-r{job['turn']}-{version}-{mode}"
        if identity in done:continue
        output=directory/'runs'/identity
        if output.exists():raise RuntimeError('Incomplete result retained; move aside before retry: '+str(output))
        if version=='v1':profile='v1-'+mode;source=None
        else:
            profile=('v2-artifacts-8k' if mode=='durable'else'v2-buffered-artifacts-8k')
            if case['name']=='default-threshold':profile='v2-default'if mode=='durable'else'v2-buffered'
            source=directory/version
        original=h.PROFILES[profile]
        h.PROFILES[profile]=dict(original,environment=dict(original.get('environment',{}),**job['environment']))
        (directory/'active.json').write_text(json.dumps(dict(job=identity,started_at_us=time.time_ns()//1000),indent=2))
        print('RUN',identity,flush=True)
        try:r=h.run_case(profile,case,output,backend=job['backend'],source_root=source,repeat=job['turn'],duration=job['duration'])
        finally:h.PROFILES[profile]=original
        expected='storage_fault_observed'if case.get('persist_fail_after')else'completed'
        valid=r['status']==expected and r['exit_code']==0 and not r.get('shutdown_timeout')
        if expected=='completed':
            valid=valid and (r['completed']>=1 if job['duration']else r['completed']==case['calls'])
            if version!='v1':
                health=r.get('barrier',{}).get('health',{})
                valid=valid and health.get('queued_count')==health.get('retained_bytes')==0 and health.get('fault')is None and health.get('accepted_frontier')==health.get('durable_frontier')
            if job['duration']:valid=valid and r['run_seconds']>=job['duration']
        r.update(job=identity,phase=job['phase'],version=version,mode=mode,turn=job['turn'],
                 directory=str(output),accepted_result=bool(valid))
        with results.open('a')as stream:stream.write(json.dumps(r)+'\n')
        print(json.dumps({k:r.get(k)for k in ('job','status','completed','run_seconds','latency_ms','accepted_result','error')}),flush=True)
        if not valid:raise RuntimeError('Acceptance case failed: '+str(output))
    (directory/'active.json').write_text(json.dumps(dict(phase=args.phase,state='completed'),indent=2))


if __name__=='__main__':main()
