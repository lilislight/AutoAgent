"""Frozen-package Server measurements for projection/control separation."""
import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
from pathlib import Path
import shutil
import time

ROOT=Path(__file__).resolve().parents[3]
DEFAULT=ROOT/'.autoagent/performance/projection-control-2026-10-25'


def hashes(path):
    return {str(p.relative_to(path)):hashlib.sha256(p.read_bytes()).hexdigest() for p in path.rglob('*.py')}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',type=Path,default=DEFAULT)
    parser.add_argument('--phase',choices=('freeze','probe','paired','formal','diagnostic','extras','viewer'),default='probe')
    args=parser.parse_args();base=args.directory.resolve()
    if args.phase=='freeze':
        if (base/'after').exists():raise RuntimeError('After snapshot already exists')
        shutil.copytree(ROOT/'autoagent',base/'after/autoagent',ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
        (base/'harness').mkdir(exist_ok=True)
        for name in ('run.py','worker.py','business.py','diagnostics.py','memory_server.py'):
            shutil.copy2(Path(__file__).with_name(name),base/'harness'/name)
        before=hashes(base/'before/autoagent');old=hashes(ROOT/'.autoagent/performance/query-optimization-2026-10-25/after/autoagent')
        provenance=dict(before_hashes=before,after_hashes=hashes(base/'after/autoagent'),
            harness_hashes=hashes(base/'harness'),preceding_final_identical=before==old,
            dependencies={name:importlib.metadata.version(name) for name in ('sqlalchemy','asyncpg','aiosqlite','httpx','pydantic','fastapi','uvicorn')})
        (base/'provenance.json').write_text(json.dumps(provenance,indent=2));return
    provenance=json.loads((base/'provenance.json').read_text())
    for name in ('before','after'):
        if hashes(base/name/'autoagent')!=provenance[name+'_hashes']:raise RuntimeError('Snapshot changed: '+name)
    if hashes(base/'harness')!=provenance['harness_hashes']:raise RuntimeError('Measurement harness changed')
    spec=importlib.util.spec_from_file_location('projection_harness',base/'harness/run.py')
    h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h);h.ROOT=ROOT
    results=base/'results.jsonl';done={json.loads(line)['job'] for line in results.read_text().splitlines()} if results.exists() else set()
    def save(row):
        with results.open('a') as stream:stream.write(json.dumps(row)+'\n')
        done.add(row['job'])
    if args.phase=='formal':
        if not provenance['preceding_final_identical']:raise RuntimeError('Historical source mismatch')
        for line in (ROOT/'.autoagent/performance/query-optimization-2026-10-25/results.jsonl').read_text().splitlines():
            row=json.loads(line)
            if row['phase']=='formal' and row['version']=='final':
                row=dict(row,version='before',job='historical-before-'+row['job'],baseline_source='identical preceding final package')
                if row['job'] not in done:save(row)
    jobs=[]
    def add(phase,backend,version,mode,case,turn=0):jobs.append(dict(phase=phase,backend=backend,version=version,mode=mode,case=case,turn=turn))
    if args.phase=='probe':
        for backend in ('sqlite','postgres'):
            for mode in ('buffered','durable'):
                for version in ('before','after'):
                    add('probe',backend,version,mode,dict(name='chain-10-16k',family='chain',nodes=10,payload_bytes=16384,calls=5))
    if args.phase=='paired':
        for turn in range(3):
            for backend in ('sqlite','postgres'):
                for case in (h.cases('artifact')[0],h.cases('artifact')[2]):
                    for version in (('before','after') if turn%2==0 else ('after','before')):
                        add('paired',backend,version,'buffered',dict(case,calls=10),turn)
    if args.phase=='formal':
        for mode in ('buffered','durable'):
            for turn in range(5):
                cases=h.cases('artifact')
                for case in cases[turn%6:]+cases[:turn%6]:
                    for backend in (('sqlite','postgres') if turn%2==0 else ('postgres','sqlite')):
                        add('formal',backend,'after',mode,dict(case,calls=128 if case['name']=='unique-history' else 20),turn)
    if args.phase=='diagnostic':
        for backend in ('sqlite','postgres'):
            for mode in ('buffered','durable'):
                for version in ('before','after'):
                    add('diagnostic',backend,version,mode,dict(name='sql-chain',family='chain',nodes=10,payload_bytes=16384,calls=5,instrument_sql=True))
    if args.phase=='extras':
        cases=[dict(name='conditional-history',family='wide',width=16,conditional=True,reuse_session=True,payload_bytes=16384,calls=8),
            dict(name='concurrent-8',family='chain',nodes=10,payload_bytes=16384,calls=16,concurrency=8,captured_frontier_barrier=True),
            dict(name='wait-resume',family='wait',payload_bytes=16384,calls=3),
            dict(name='crash-at-wait',family='wait',payload_bytes=16384,calls=2,restart_at_wait=True),
            dict(name='storage-fault',family='chain',nodes=4,payload_bytes=16384,calls=2,persist_fail_after=2,timeout_seconds=5,skip_warmup=True)]
        for backend in ('sqlite','postgres'):
            for mode in ('buffered','durable'):
                for case in cases:
                    for version in ('before','after'):add('extras',backend,version,mode,case)
    if args.phase=='viewer':
        for backend in ('sqlite','postgres'):
            for mode in ('buffered','durable'):
                for version in ('before','after'):
                    for case in (dict(name='trace-chain',family='chain',nodes=10,payload_bytes=16384,calls=5,trace_reads=3,skip_warmup=True),
                                 dict(name='trace-loop',family='loop',iterations=100,payload_bytes=16384,calls=3,trace_reads=3,skip_warmup=True)):
                        add('viewer',backend,version,mode,case)
    for job in jobs:
        name=f"{job['phase']}-{job['backend']}-{job['case']['name']}-r{job['turn']}-{job['version']}-{job['mode']}"
        if name in done:continue
        output=base/'runs'/name
        if output.exists():raise RuntimeError('Incomplete measurement retained: '+str(output))
        (base/'active.json').write_text(json.dumps(dict(job=name,started_at_us=time.time_ns()//1000)))
        print('RUN',name,flush=True)
        profile='v2-buffered-artifacts-8k' if job['mode']=='buffered' else 'v2-artifacts-8k'
        row=h.run_case(profile,job['case'],output,backend=job['backend'],source_root=base/job['version'],repeat=job['turn'])
        expected='storage_fault_observed' if job['case'].get('persist_fail_after') else 'completed'
        valid=row['status']==expected and row['exit_code']==0 and not row.get('shutdown_timeout')
        if expected=='completed':
            health=row.get('barrier',{}).get('health',{})
            valid=valid and row['completed']==job['case']['calls'] and health.get('queued_count')==health.get('retained_bytes')==0 and health.get('fault') is None and health.get('accepted_frontier')==health.get('durable_frontier')
        row.update(job=name,phase=job['phase'],version=job['version'],mode=job['mode'],turn=job['turn'],directory=str(output),accepted_result=bool(valid))
        save(row);print(json.dumps({key:row.get(key) for key in ('job','status','completed','latency_ms','accepted_result','error')}),flush=True)
        if not valid:raise RuntimeError('Measurement failed: '+name)
    (base/'active.json').write_text(json.dumps(dict(phase=args.phase,state='completed')))


if __name__=='__main__':main()
