"""Freeze and measure query-only changes against the preceding Server baseline."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import time

ROOT=Path(__file__).resolve().parents[3]


def hashes(root):
    return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob('*.py')if '__pycache__'not in p.parts}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',type=Path,default=ROOT/'.autoagent/performance/query-optimization-2026-10-25')
    parser.add_argument('--phase',choices=('probe','verify','paired','formal','extras','diagnostic','all'),default='all')
    args=parser.parse_args();base=args.directory.resolve()
    provenance=json.loads((base/'provenance.json').read_text())
    if hashes(base/'before/autoagent')!=provenance['before_hashes']:
        raise RuntimeError('The before snapshot changed')
    if not (base/'after').exists():
        shutil.copytree(ROOT/'autoagent',base/'after/autoagent',ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
        (base/'harness').mkdir()
        for name in ('run.py','worker.py','business.py','diagnostics.py','memory_server.py'):
            shutil.copy2(Path(__file__).with_name(name),base/'harness'/name)
        provenance['after_hashes']=hashes(base/'after/autoagent')
        provenance['harness_hashes']=hashes(base/'harness')
        provenance['changes']={name:dict(before=provenance['before_hashes'].get(name),after=provenance['after_hashes'].get(name))
            for name in provenance['before_hashes'].keys()|provenance['after_hashes'].keys()
            if provenance['before_hashes'].get(name)!=provenance['after_hashes'].get(name)}
        (base/'provenance.json').write_text(json.dumps(provenance,indent=2))
    if hashes(base/'after/autoagent')!=provenance['after_hashes'] or hashes(base/'harness')!=provenance['harness_hashes']:
        raise RuntimeError('The after snapshot or measurement tools changed')
    spec=importlib.util.spec_from_file_location('query_harness',base/'harness/run.py')
    h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h);h.ROOT=ROOT
    if args.phase=='verify':
        from tests.benchmarks.server_comparison.acceptance import regression
        regression(base)
        return
    results=base/'results.jsonl'
    done={json.loads(line)['job']for line in results.read_text().splitlines()}if results.exists()else set()
    def save(row):
        with results.open('a')as stream:stream.write(json.dumps(row)+'\n')
        done.add(row['job'])
    if args.phase in ('formal','all'):
        if provenance['previous_final_alignment']['changed']or provenance['previous_final_alignment']['removed']:
            raise RuntimeError('Historical baseline no longer matches the before package')
        old=ROOT/'.autoagent/performance/v2-optimization/step-09/results.jsonl'
        for line in old.read_text().splitlines():
            row=json.loads(line)
            if row['phase']=='formal'and row['version']=='final':
                row=dict(row,version='b0',job='historical-before-'+row['job'],baseline_source='preceding five-round measurement')
                if row['job']not in done:save(row)
    jobs=[]
    def add(phase,backend,version,mode,case,turn=0):
        jobs.append(dict(phase=phase,backend=backend,version=version,mode=mode,case=case,turn=turn))
    if args.phase in ('probe','all'):
        for backend in ('sqlite','postgres'):
            for mode in ('buffered','durable'):
                for version in ('b0','final'):
                    add('probe',backend,version,mode,dict(name='chain-10-16k',family='chain',nodes=10,payload_bytes=16384,calls=5))
    if args.phase in ('paired','all'):
        for turn in range(3):
            for backend in ('sqlite','postgres'):
                for case in (h.cases('artifact')[0],h.cases('artifact')[2]):
                    for version in (('b0','final')if turn%2==0 else('final','b0')):
                        add('paired',backend,version,'buffered',dict(case,calls=10),turn)
    if args.phase in ('formal','all'):
        for mode in ('buffered','durable'):
            for turn in range(5):
                cases=h.cases('artifact')
                for case in cases[turn%6:]+cases[:turn%6]:
                    for backend in (('sqlite','postgres')if turn%2==0 else('postgres','sqlite')):
                        add('formal',backend,'final',mode,dict(case,calls=128 if case['name']=='unique-history'else 20),turn)
    if args.phase in ('extras','all'):
        cases=[dict(name='conditional-history',family='wide',width=16,conditional=True,reuse_session=True,payload_bytes=16384,calls=8),
               dict(name='concurrent-8',family='chain',nodes=10,payload_bytes=16384,calls=16,concurrency=8,captured_frontier_barrier=True),
               dict(name='wait-resume',family='wait',payload_bytes=16384,calls=3),
               dict(name='crash-at-wait',family='wait',payload_bytes=16384,calls=2,restart_at_wait=True),
               dict(name='storage-fault',family='chain',nodes=4,payload_bytes=16384,calls=2,persist_fail_after=2,timeout_seconds=5,skip_warmup=True)]
        for backend in ('sqlite','postgres'):
            for mode in ('buffered','durable'):
                for case in cases:
                    for version in ('b0','final'):add('extras',backend,version,mode,case)
    if args.phase in ('diagnostic','all'):
        for backend in ('sqlite','postgres'):
            for mode in ('buffered','durable'):
                for version in ('b0','final'):
                    add('diagnostic',backend,version,mode,dict(name='sql-chain',family='chain',nodes=10,payload_bytes=16384,calls=5,instrument_sql=True))
    for job in jobs:
        name=f"{job['phase']}-{job['backend']}-{job['case']['name']}-r{job['turn']}-{job['version']}-{job['mode']}"
        if name in done:continue
        source=base/('before'if job['version']=='b0'else'after')
        profile='v2-buffered-artifacts-8k'if job['mode']=='buffered'else'v2-artifacts-8k'
        (base/'active.json').write_text(json.dumps(dict(job=name,started_at_us=time.time_ns()//1000)))
        print('RUN',name,flush=True)
        output=base/'runs'/name
        row=h.run_case(profile,job['case'],output,backend=job['backend'],source_root=source,repeat=job['turn'])
        expected='storage_fault_observed'if job['case'].get('persist_fail_after')else'completed'
        valid=row['status']==expected and row['exit_code']==0 and not row.get('shutdown_timeout')
        if expected=='completed':
            health=row.get('barrier',{}).get('health',{})
            valid=valid and row['completed']==job['case']['calls']and health.get('queued_count')==health.get('retained_bytes')==0 and health.get('fault')is None and health.get('accepted_frontier')==health.get('durable_frontier')
        row.update(job=name,phase=job['phase'],version=job['version'],mode=job['mode'],turn=job['turn'],directory=str(output),accepted_result=bool(valid))
        save(row)
        print(json.dumps({k:row.get(k)for k in ('job','status','completed','latency_ms','accepted_result','error')}),flush=True)
        if not valid:raise RuntimeError('Measurement failed: '+name)
    (base/'active.json').write_text(json.dumps(dict(phase=args.phase,state='completed')))


if __name__=='__main__':main()
