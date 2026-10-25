"""HTTP-only controller, external RSS guard and physical database measurements.

V1 is launched with cwd=v1; V2 with cwd=root. The controller imports neither
version's runtime. Samples and outputs are streamed to files, not retained as
Runtime Event collectors. No public network or provider credentials are used.
"""
from __future__ import annotations
import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import random
import signal
import socket
import sqlite3
import math
import subprocess
import sys
import threading
import time
from uuid import uuid4
import httpx

ROOT=Path(__file__).resolve().parents[3]
WORKER=Path(__file__).with_name('worker.py')
MIB=1024*1024
PROFILES={
 'v1-full-memory':dict(version='v1',event_mode='full',no_persistence=True),
 'v1-standard-memory':dict(version='v1',event_mode='standard',no_persistence=True),
 'v2-memory':dict(version='v2',no_persistence=True),
 'v1-minimal':dict(version='v1',event_mode='minimal'),
 'v1-standard':dict(version='v1',event_mode='standard'),
 'v1-full':dict(version='v1',event_mode='full'),
 'v1-full-lru':dict(version='v1',event_mode='full',retention='lru_durable_terminal'),
 'v2-default':dict(version='v2',environment={'AUTOAGENT_MEMORY_MONITOR_ENABLED':'false'}),
 'v2-buffered':dict(version='v2',persistence_mode='buffered',environment={'AUTOAGENT_MEMORY_MONITOR_ENABLED':'false'}),
 'v2-monitored':dict(version='v2',environment={'AUTOAGENT_MEMORY_MONITOR_ENABLED':'true'}),
 'v2-join':dict(version='v2',wait_mode='join',environment={'AUTOAGENT_MEMORY_MONITOR_ENABLED':'false'}),
 'v2-monitored-join':dict(version='v2',wait_mode='join',environment={'AUTOAGENT_MEMORY_MONITOR_ENABLED':'true'}),
 'v2-artifacts-8k':dict(version='v2',environment={'AUTOAGENT_MEMORY_MONITOR_ENABLED':'false','AUTOAGENT_ARTIFACT_THRESHOLD_BYTES':'8192'}),
 'v2-buffered-artifacts-8k':dict(version='v2',persistence_mode='buffered',environment={'AUTOAGENT_MEMORY_MONITOR_ENABLED':'false','AUTOAGENT_ARTIFACT_THRESHOLD_BYTES':'8192'}),
 'v2-evict':dict(version='v2',environment={'AUTOAGENT_MEMORY_MONITOR_ENABLED':'true',
     'AUTOAGENT_HOST_COLD_AFTER_SECONDS':'1','AUTOAGENT_HOST_MAX_IDLE_SECONDS':'2',
     'AUTOAGENT_HOST_MIN_RESIDENCY_SECONDS':'0','AUTOAGENT_HOST_RESIDENCY_INTERVAL_SECONDS':'.2'}),
}


def proc_memory(pid):
    try:
        fields={line.split(':',1)[0]:line.split(':',1)[1].strip() for line in Path(f'/proc/{pid}/status').read_text().splitlines() if ':' in line}
        result = {key:int(fields.get(name,'0 kB').split()[0])*1024 for key,name in
                [('rss_bytes','VmRSS'),('peak_rss_bytes','VmHWM')]}
        stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        result['cpu_seconds'] = (int(stat[11]) + int(stat[12])) / os.sysconf('SC_CLK_TCK')
        return result
    except (OSError,ValueError):
        return {}


def database_size(directory):
    path=Path(directory)/'runtime.db'
    sizes={p.name:p.stat().st_size for p in Path(directory).glob('runtime.db*') if p.is_file()}
    return dict(files=sizes,total_bytes=sum(sizes.values()))


def sqlite_stats(directory):
    path=Path(directory)/'runtime.db'
    if not path.exists():
        return {}
    try:
        with sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=5) as db:
            tables=[row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
            counts={name:db.execute('SELECT COUNT(*) FROM "'+name.replace('"','""')+'"').fetchone()[0] for name in tables}
            # A multi-GiB dbstat scan can dominate disk I/O after a soak run.
            # Physical file size remains authoritative; avoid perturbing other
            # measurements merely to classify every overflow page by table.
            detailed=path.stat().st_size <= 128*MIB
            allocated={row[0]:row[1] for row in db.execute('SELECT name,SUM(pgsize) FROM dbstat GROUP BY name')} if detailed else None
            return dict(rows=counts,allocated_bytes=allocated,page_count=db.execute('PRAGMA page_count').fetchone()[0],
                        freelist_count=db.execute('PRAGMA freelist_count').fetchone()[0],
                        allocation_note=None if detailed else 'Per-table dbstat omitted above 128 MiB; physical file sizes were still measured.')
    except sqlite3.Error as error:
        return {'error':str(error)}


def percentile(values,p):
    if not values:
        return None
    ordered=sorted(values)
    position=(len(ordered)-1)*p
    low,high=math.floor(position),math.ceil(position)
    return ordered[low]+(ordered[high]-ordered[low])*(position-low)


async def postgres_stats(url):
    import asyncpg
    from sqlalchemy.engine import make_url
    connection = await asyncpg.connect(make_url(url).set(drivername='postgresql').render_as_string(hide_password=False))
    try:
        rows = await connection.fetch("SELECT relname,pg_table_size(relid) AS table_bytes, pg_indexes_size(relid) AS index_bytes, pg_total_relation_size(relid) AS total_bytes FROM pg_stat_user_tables ORDER BY relname")
        counts = {}
        for row in rows:
            counts[row['relname']] = await connection.fetchval('SELECT COUNT(*) FROM "' + row['relname'].replace('"', '""') + '"')
        return dict(total_bytes=await connection.fetchval('SELECT pg_database_size(current_database())'),
                    relations=[dict(row) for row in rows], rows=counts,
                    note='Database allocation includes indexes and TOAST; shared PostgreSQL RSS/WAL are not attributed to this Server.')
    finally:
        await connection.close()


class AdmissionRejected(RuntimeError):
    pass


def run_case(profile,case,directory,*,guard_mib=768,repeat=0,duration=0,backend='sqlite',source_root=None):
    """Always remove a disposable PostgreSQL database, including startup failures."""
    if PROFILES[profile].get('no_persistence'):
        backend = 'none'
    database = None
    url = None
    if backend == 'postgres':
        from tests.postgres import PostgreSQLDatabase
        database = PostgreSQLDatabase()
        url = asyncio.run(database.create())
    try:
        return _run_case(profile,case,directory,guard_mib=guard_mib,repeat=repeat,
                         duration=duration,backend=backend,database=database,url=url,source_root=source_root)
    finally:
        if database is not None:
            asyncio.run(database.close())


def _run_case(profile,case,directory,*,guard_mib,repeat,duration,backend,database,url,source_root):
    directory=Path(directory).resolve(); directory.mkdir(parents=True,exist_ok=True)
    selected=PROFILES[profile]
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    url = url or 'sqlite+aiosqlite:///'+str(directory/'runtime.db')
    def storage():
        return asyncio.run(postgres_stats(url)) if database is not None else database_size(directory)
    config=dict(selected,case=case,directory=str(directory),port=port,repository_root=str(ROOT),
                database_url=url,timeout_seconds=case.get('timeout_seconds',600))
    if source_root is not None:config['source_root']=str(Path(source_root).resolve())
    config['worker_sha256'] = hashlib.sha256(WORKER.read_bytes()).hexdigest()
    (directory/'config.json').write_text(json.dumps(config,indent=2))
    environment={key:value for key,value in os.environ.items() if not key.startswith(('AUTOAGENT_','DEEPSEEK_','WEATHER_'))}
    environment['PYTHONPATH']=str(ROOT/'v1' if selected['version']=='v1' else source_root or ROOT)
    origin=f'http://127.0.0.1:{port}'
    started=time.monotonic();stop=threading.Event();guarded=threading.Event()
    phases={'value':'startup'}
    peaks={'rss_bytes':0,'peak_rss_bytes':0,'cpu_seconds':0}
    log=(directory/'server.log').open('w')
    process=subprocess.Popen([sys.executable,str(WORKER),'--version',selected['version'],'--config',str(directory/'config.json')],
        cwd=ROOT/'v1' if selected['version']=='v1' else ROOT,env=environment,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)

    def sample():
        with (directory/'memory.jsonl').open('w') as samples:
            while not stop.is_set():
                memory=proc_memory(process.pid)
                if memory:
                    for key,value in memory.items():peaks[key]=max(peaks[key],value)
                    samples.write(json.dumps(dict(seconds=time.monotonic()-started,phase=phases['value'],**memory))+'\n')
                    samples.flush()
                    if memory['rss_bytes']>guard_mib*MIB:
                        guarded.set()
                        try:process.kill()
                        except ProcessLookupError:pass
                        return
                stop.wait(.02)
    sampler=threading.Thread(target=sample,daemon=True);sampler.start()
    result=dict(profile=profile,case=case,repeat=repeat,guard_mib=guard_mib,backend=backend,
                worker_sha256=config['worker_sha256'],
                isolation='external process RSS guard; no private writable cgroup',status='error',completed=0,rejected=0)
    latency=[];submit=[];flush_times=[];digests=set()
    try:
        with httpx.Client(base_url=origin,timeout=config['timeout_seconds']+5,trust_env=False) as client:
            for _ in range(1200):
                if time.monotonic()-started>30:raise TimeoutError('Server startup timed out')
                if process.poll() is not None:raise RuntimeError('Server exited during startup')
                try:
                    response=client.get('/__bench/catalog',timeout=.5)
                    if response.status_code==200:break
                except httpx.TransportError:pass
                time.sleep(.025)
            else:raise TimeoutError('Server startup timed out')
            catalog=response.json();result['startup_seconds']=time.monotonic()-started;result['catalog']=catalog
            result['baseline_memory']=proc_memory(process.pid);result['baseline_storage']=storage()
            revision=catalog['revision']
            if case.get('registration_only'):
                result.update(status='completed',run_seconds=0,drained_storage=storage(),database=asyncio.run(postgres_stats(url)) if database else sqlite_stats(directory))
                return result
            payload=dict(step=0,blob='',rows=[])
            session_key = 'reuse-' + uuid4().hex
            session_id = None
            def call(index):
                nonlocal session_id,process
                incoming_payload=dict(payload,step=(index+1)*1000) if case.get('unique_payload') else payload
                tick=time.perf_counter()
                if selected.get('no_persistence'):
                    response=client.post('/__bench/invoke',json={'input':incoming_payload})
                elif selected['version']=='v1':
                    response=client.post(f'/api/v1/workflow-revisions/{revision}/invocations',json=dict(input={'value':incoming_payload},entry_node_id='entry',event_mode=selected['event_mode'],session_key=session_key if case.get('reuse_session') else f'case-{uuid4().hex}'))
                else:
                    definition = catalog.get('revisions', [])[index % len(catalog['revisions'])] if catalog.get('revisions') else dict(workflow_id='comparison', revision_id=revision)
                    response=client.post('/api/v2/executions',json=dict(**definition,
                        entry_node_id='entry',input=incoming_payload,session_mode='existing' if case.get('reuse_session') and session_id else 'new',session_id=session_id if case.get('reuse_session') else None))
                receipt_time=time.perf_counter()
                if response.status_code in (429,503):return dict(rejected=True,status=response.status_code,error=response.text[:500])
                response.raise_for_status();receipt=response.json();iid=receipt['invocation_id']
                session_id=receipt['session_id']
                response=client.get('/__bench/wait',params={'iid':iid});response.raise_for_status()
                record=response.json()
                if record.get('status')=='waiting':
                    if case.get('restart_at_wait'):
                        phases['value']='restart'
                        process.kill();process.wait()
                        process=subprocess.Popen([sys.executable,str(WORKER),'--version',selected['version'],'--config',str(directory/'config.json')],
                            cwd=ROOT/'v1' if selected['version']=='v1' else ROOT,env=environment,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
                        deadline=time.monotonic()+30
                        while time.monotonic()<deadline:
                            if process.poll() is not None:raise RuntimeError('Server recovery failed')
                            try:
                                if client.get('/__bench/catalog',timeout=.5).status_code==200:break
                            except httpx.TransportError:pass
                            time.sleep(.025)
                        else:raise TimeoutError('Server recovery timed out')
                        phases['value']='running'
                    reply=dict(step=1,blob='x'*case.get('payload_bytes',0),rows=[])
                    if selected['version']=='v1':
                        response=client.post(f'/api/v1/workflow-revisions/{revision}/resume',json=dict(session_key=receipt['session_key'],wait_key=record['wait_id'],output=reply))
                    else:
                        response=client.post('/api/v2/execution-actions',json=dict(action='respond',invocation_id=iid,
                            target_invocation_id=iid,wait_id=record['wait_id'],payload=reply,request_id=uuid4().hex))
                        response.raise_for_status();operation=response.json()
                        while operation['status']=='running':
                            time.sleep(.01)
                            operation=client.get('/api/v2/execution-operations/'+operation['operation_id']).json()
                        if operation['status']!='succeeded':raise RuntimeError(str(operation))
                    response.raise_for_status()
                    response=client.get('/__bench/wait',params={'iid':iid});response.raise_for_status();record=response.json()
                end=time.perf_counter()
                if selected['version']=='v1' and isinstance(record.get('output'),dict) and set(record['output'])=={'output'}:
                    record['output']=record['output']['output']
                if case.get('trace_reads'):
                    reads=[]
                    for _ in range(case['trace_reads']):
                        query_started=time.perf_counter()
                        trace=client.get(f'/api/v1/invocations/{iid}/trace' if selected['version']=='v1' else '/api/v2/execution-trace',
                                         params={'invocation_id':iid} if selected['version']=='v2' else None)
                        trace.raise_for_status()
                        reads.append(dict(milliseconds=(time.perf_counter()-query_started)*1000,response_bytes=len(trace.content)))
                    record['trace_reads']=reads
                return dict(index=index,latency_ms=(end-tick)*1000,submit_ms=(receipt_time-tick)*1000,
                            started_seconds=tick-started,ended_seconds=end-started,
                            client_worker=threading.current_thread().name,session_id=receipt['session_id'],**record)
            phases['value']='warmup';warmup=call(-1) if not case.get('skip_warmup') else dict(status='completed',output=None)
            if warmup.get('rejected'):
                result['rejected'] += 1
                raise AdmissionRejected('Warmup admission rejected: '+str(warmup))
            if warmup.get('status')!='completed':raise RuntimeError('Warmup failed: '+str(warmup))
            result['expected_output']=warmup['output']
            client.post('/__bench/flush').raise_for_status()
            result['warm_memory']=proc_memory(process.pid);result['warm_storage']=storage()
            if case.get('instrument_sql'):result['sql_before']=client.get('/__bench/sql').json()
            phases['value']='running';run_started=time.monotonic()
            result['measurement_start_seconds']=run_started-started
            if case.get('persist_fail_after'):client.post('/__bench/arm-fault').raise_for_status()
            if case.get('profile_cpu'):client.post('/__bench/start-profile').raise_for_status()
            calls=case.get('calls',5);concurrency=case.get('concurrency',1)
            with (directory/'calls.jsonl').open('w') as records:
                def consume(record):
                    records.write(json.dumps(record)+'\n');records.flush()
                    if record.get('rejected'):
                        result['rejected']+=1;return
                    if record.get('status')!='completed':raise RuntimeError('Execution failed: '+str(record))
                    if result['expected_output'] is None:result['expected_output']=record['output']
                    if case.get('unique_payload'):
                        step=(record['index']+1)*1000+case['nodes']
                        blob=f'{step:012d}:'+'x'*max(0,case['payload_bytes']-13)
                        expected=dict(step=step,count=1,bytes=len(blob),digest=hashlib.sha256(blob.encode()).hexdigest())
                    else:expected=result['expected_output']
                    if record['output']!=expected:raise AssertionError('Business output differs from the workload checksum')
                    result['completed']+=1;latency.append(record['latency_ms']);submit.append(record['submit_ms']);flush_times.append(record['flush_ms'])
                    every=case.get('sample_health_every',0)
                    if every and result['completed'] % every == 0:
                        response=client.get('/__bench/residency');response.raise_for_status()
                        with (directory/'health.jsonl').open('a') as samples:
                            samples.write(json.dumps(dict(seconds=time.monotonic()-run_started,
                                completed=result['completed'],**response.json()))+'\n')
                if duration:
                    index=0
                    while time.monotonic()-run_started<duration:
                        consume(call(index));index+=1
                        time.sleep(max(0,case.get('period_seconds',1)-(time.monotonic()-run_started-(index-1)*case.get('period_seconds',1))))
                elif concurrency>1:
                    with ThreadPoolExecutor(max_workers=concurrency) as pool:
                        for record in pool.map(call,range(calls)):consume(record)
                else:
                    for index in range(calls):consume(call(index))
            result['run_seconds']=time.monotonic()-run_started
            result['duration_requested_seconds']=duration
            if case.get('instrument_sql'):result['sql_after']=client.get('/__bench/sql').json()
            phases['value']='query'
            if session_id:
                tick=time.perf_counter()
                response=client.get(f'/api/v1/sessions/{session_id}/invocations' if selected['version']=='v1' else '/api/v2/session-invocations',
                    params={'limit':20, **({'session_id':session_id} if selected['version']=='v2' else {})})
                result['history_query']=dict(status=response.status_code, milliseconds=(time.perf_counter()-tick)*1000,
                                             response_bytes=len(response.content))
            if selected['version']=='v2':
                snapshot=client.get('/api/v2/memory')
                if snapshot.status_code==200:result['monitor']=snapshot.json()
            phases['value']='drain';barrier=client.post('/__bench/flush');barrier.raise_for_status();result['barrier']=barrier.json()
            result['drained_memory']=proc_memory(process.pid);result['drained_storage']=storage()
            phases['value']='idle';time.sleep(case.get('idle_seconds',.2))
            result['idle_barrier']=client.post('/__bench/flush').json()
            result['idle_memory']=proc_memory(process.pid);result['database']=asyncio.run(postgres_stats(url)) if database else sqlite_stats(directory)
            result['status']='completed' if result['rejected']==0 else 'capacity_rejected'
    except Exception as error:
        result['error']=str(error)[:1500]
        timed_out = isinstance(error,(TimeoutError,httpx.TimeoutException)) or isinstance(error,httpx.HTTPStatusError) and error.response.status_code==504
        result['status']='rss_guard_stopped' if guarded.is_set() else 'capacity_rejected' if isinstance(error,AdmissionRejected) else 'timeout' if timed_out else 'error'
        if case.get('persist_fail_after') and process.poll() is None:
            try:
                health=httpx.get(origin+'/__bench/status',timeout=2,trust_env=False).json()
                result['fault_health']=health
                if 'Injected benchmark persistence failure' in json.dumps(health):result['status']='storage_fault_observed'
            except Exception:
                pass
    finally:
        phases['value']='shutdown'
        result['before_close_storage']=storage()
        result['final_memory']=proc_memory(process.pid)
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
            try:process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill();process.wait();result['shutdown_timeout']=True
        stop.set();sampler.join(timeout=2);log.close()
        result.update(peaks,exit_code=process.returncode,total_seconds=time.monotonic()-started,
            closed_storage=storage(),latency_ms=dict(p50=percentile(latency,.5),p95=percentile(latency,.95),p99=percentile(latency,.99)),
            submit_ms=dict(p50=percentile(submit,.5),p95=percentile(submit,.95)),flush_ms_p50=percentile(flush_times,.5))
        if result['status']=='error':result['server_log_tail']='\n'.join((directory/'server.log').read_text().splitlines()[-20:])
        (directory/'result.json').write_text(json.dumps(result,indent=2))
    return result


def cases(suite):
    if suite=='optimization':return [
        dict(name='diagnostics-off',family='chain',nodes=10,payload_bytes=16384,calls=10),
        dict(name='diagnostics-on',family='chain',nodes=10,payload_bytes=16384,calls=10,instrument_sql=True),
        dict(name='conditional-history',family='wide',width=16,conditional=True,reuse_session=True,payload_bytes=16384,calls=16,instrument_sql=True),
        dict(name='revision-session-churn',family='chain',nodes=4,definitions=64,rotate_definitions=True,payload_bytes=16384,calls=128,instrument_sql=True)]
    if suite=='artifact':return [
        dict(name='chain-10-16k',family='chain',nodes=10,payload_bytes=16384,calls=5),
        dict(name='loop-100-16k',family='loop',iterations=100,payload_bytes=16384,calls=3),
        dict(name='map-100-16k',family='map',width=100,parallelism=8,payload_bytes=16384,calls=5),
        dict(name='text-overwrite',family='loop',iterations=20,payload_bytes=MIB,context='overwrite',calls=3),
        dict(name='context-append',family='loop',iterations=20,payload_bytes=65536,context='append',calls=3),
        dict(name='unique-history',family='chain',nodes=4,payload_bytes=16384,unique_payload=True,calls=128)]
    if suite=='smoke':return [dict(name='chain',family='chain',nodes=3,calls=2),dict(name='loop',family='loop',iterations=3,calls=2),dict(name='map',family='map',width=4,calls=2),dict(name='context',family='loop',iterations=3,context='append',payload_bytes=1024,calls=2)]
    if suite=='soak':return [dict(name='soak',family='chain',nodes=4,payload_bytes=16384,calls=3600,period_seconds=1)]
    if suite=='extended':return [
        dict(name='wide-32', family='wide',width=32,payload_bytes=16384,calls=5),
        dict(name='wide-256', family='wide',width=256,payload_bytes=1024,calls=3),
        dict(name='session-reuse',family='chain',nodes=4,payload_bytes=16384,reuse_session=True,calls=128),
        dict(name='slow-storage',family='chain',nodes=10,payload_bytes=65536,persist_delay_ms=20,calls=5),
        dict(name='sql-chain-10',family='chain',nodes=10,instrument_sql=True,calls=5),
        dict(name='wait-resume',family='wait',payload_bytes=16384,calls=5),
        dict(name='crash-at-wait',family='wait',payload_bytes=16384,restart_at_wait=True,calls=3),
        dict(name='chain-long-deadline',family='chain',nodes=1000,timeout_seconds=600,skip_warmup=True,calls=1),
        dict(name='unique-history',family='chain',nodes=4,payload_bytes=16384,unique_payload=True,calls=128),
        dict(name='failed-storage',family='chain',nodes=10,payload_bytes=16384,persist_fail_after=2,timeout_seconds=5,skip_warmup=True,calls=1),
        dict(name='cpu-diagnostic',family='chain',nodes=10,payload_bytes=16384,profile_cpu=True,calls=3),
        dict(name='trace-reader',family='chain',nodes=10,payload_bytes=16384,trace_reads=3,skip_warmup=True,calls=8),
        dict(name='idle-release',family='chain',nodes=4,payload_bytes=1048576,idle_seconds=8,calls=8)]
    result=[dict(name=f'chain-{n}',family='chain',nodes=n,calls=10 if n<100 else 3) for n in (1,10,100,1000)]
    result += [dict(name=f'loop-{n}',family='loop',iterations=n,payload_bytes=1024,calls=3) for n in (10,100)]
    result += [dict(name='text-overwrite',family='loop',iterations=20,payload_bytes=MIB,context='overwrite',calls=3),
               dict(name='context-append',family='loop',iterations=20,payload_bytes=65536,context='append',calls=3),
               dict(name='rows',family='chain',nodes=4,payload_bytes=MIB,shape='rows',calls=3)]
    result += [dict(name=f'map-{width}',family='map',width=width,parallelism=8,payload_bytes=1024,calls=3) for width in (100,1000)]
    result += [dict(name='many-definitions',family='chain',nodes=10,definitions=50,calls=3),
               dict(name='concurrent-sessions',family='chain',nodes=10,payload_bytes=65536,concurrency=8,calls=64)]
    if suite=='stress':
        result=[dict(name=f'register-chain-{n}',family='chain',nodes=n,registration_only=True,calls=0) for n in (2000,10000)]
        result += [dict(name=f'large-text-{size//MIB}m',family='loop',iterations=30,payload_bytes=size,context='overwrite',calls=2) for size in (8*MIB,32*MIB)]
        result += [dict(name='map-10000',family='map',width=10000,parallelism=32,payload_bytes=4096,calls=1,timeout_seconds=120)]
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--suite',choices=('smoke','baseline','stress','soak','extended','artifact','optimization'),default='smoke')
    parser.add_argument('--backend',choices=('sqlite','postgres'),default='sqlite')
    parser.add_argument('--source-root',type=Path,help='Pinned V2 package root containing autoagent/. V1 always uses the archive.')
    parser.add_argument('--cases',nargs='+',help='Select named cases within the suite.')
    parser.add_argument('--profiles',nargs='+',choices=tuple(PROFILES),default=['v1-minimal','v1-standard','v1-full','v2-default'])
    parser.add_argument('--repeat',type=int,default=1)
    parser.add_argument('--calls',type=int,help='Override measured calls per case; warmup remains separate.')
    parser.add_argument('--duration',type=float,default=3600)
    parser.add_argument('--guard-mib',type=int,default=768)
    parser.add_argument('--output',type=Path,required=True)
    options=parser.parse_args()
    if options.repeat < 1 or options.guard_mib < 1 or options.calls is not None and options.calls < 1 or not math.isfinite(options.duration) or options.duration <= 0:
        parser.error('repeat, guard-mib and duration must be positive and finite')
    names={case['name'] for case in cases(options.suite)}
    if options.cases and set(options.cases)-names:parser.error('Unknown case(s): '+', '.join(sorted(set(options.cases)-names)))
    if options.source_root and not (options.source_root/'autoagent/__init__.py').is_file():parser.error('source-root must contain the V2 package')
    options.output.mkdir(parents=True,exist_ok=True)
    manifest=dict(argv=sys.argv,started_at_us=time.time_ns()//1000,python=sys.version,platform=platform.platform(),logical_cpus=os.cpu_count(),
        git_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        packages={name:importlib.metadata.version(name) for name in ('pydantic','sqlalchemy','uvicorn','fastapi','httpx')},
        source_root=str(options.source_root.resolve()) if options.source_root else str(ROOT),
        source_hashes={str(Path(base)/p.relative_to((options.source_root/base) if base=='autoagent' and options.source_root else ROOT/base)):hashlib.sha256(p.read_bytes()).hexdigest() for base in ('autoagent','v1/autoagent','tests/benchmarks/server_comparison') for p in ((options.source_root/base) if base=='autoagent' and options.source_root else ROOT/base).rglob('*.py')})
    (options.output/'manifest.json').write_text(json.dumps(manifest,indent=2))
    selected_cases=[case for case in cases(options.suite) if not options.cases or case['name'] in options.cases]
    if options.calls is not None:selected_cases=[dict(case,calls=options.calls) for case in selected_cases]
    if not selected_cases:parser.error('No matching cases.')
    jobs=[(profile,case,repeat) for repeat in range(options.repeat) for case in selected_cases for profile in options.profiles]
    random.Random(42).shuffle(jobs)
    with (options.output/'results.jsonl').open('w') as output:
        for profile,case,repeat in jobs:
            directory=options.output/f"{case['name']}-{profile}-{repeat}"
            result=run_case(profile,case,directory,guard_mib=options.guard_mib,repeat=repeat,
                            duration=options.duration if options.suite=='soak' else 0, backend=options.backend,source_root=options.source_root)
            output.write(json.dumps(result)+'\n');output.flush()
            print(json.dumps({key:result.get(key) for key in ('profile','case','status','completed','rss_bytes','latency_ms','error')}),flush=True)
    return 0

if __name__=='__main__':raise SystemExit(main())
