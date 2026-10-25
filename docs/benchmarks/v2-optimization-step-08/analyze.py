from collections import defaultdict
from pathlib import Path
import json
import statistics as st
import sys

b = Path(sys.argv[1]) if len(sys.argv)>1 else Path(__file__).parent
rows = [json.loads(line) for line in (b/'results.jsonl').read_text().splitlines()]
groups = defaultdict(list)
fixed = ('runtime_events','user_events','workflow_revisions','operator_definitions','sessions','invocations',
         'node_executions','operator_calls','waits','graph_changes','edge_evaluations')
details = []
variables = []
for r in rows:
    assert r['status'] == 'completed' and r['completed'] == r['case']['calls'] and r['exit_code'] == 0
    h = r['barrier']['health']
    assert h['state'] == 'open' and h['queued_count'] == h['retained_bytes'] == 0 and h['fault'] is None
    assert h['accepted_frontier'] == h['durable_frontier']
    calls = [json.loads(line) for line in (Path(r['directory'])/'calls.jsonl').read_text().splitlines()]
    assert len(calls) == r['completed'] and len({x['session_id'] for x in calls}) == r['completed']
    c = r['case']['concurrency']
    first = [x['latency_ms'] for x in calls if x['index'] < c]
    inverse = [1/x for x in first]
    intervals = sorted([(x['started_seconds'],1) for x in calls] + [(x['ended_seconds'],-1) for x in calls])
    active = peak = 0
    for _, delta in intervals:
        active += delta
        peak = max(active, peak)
    samples = [json.loads(line) for line in (Path(r['directory'])/'memory.jsonl').read_text().splitlines()]
    m = dict(job=r['job'],backend=r['backend'],concurrency=c,delay_ms=r['delay_ms'],turn=r['turn'],
             diagnostic=r['diagnostic'],calls=r['completed'],throughput=r['completed']/r['run_seconds'],
             p50_ms=r['latency_ms']['p50'],p95_ms=r['latency_ms']['p95'],max_ms=max(x['latency_ms'] for x in calls),
             cpu_ms_call=(r['drained_memory']['cpu_seconds']-r['warm_memory']['cpu_seconds'])*1000/r['completed'],
             rss_mib=max(x['rss_bytes'] for x in samples if x['phase']=='running')/1048576,
             warm_rss_mib=r['warm_memory']['rss_bytes']/1048576,
             drained_rss_mib=r['drained_memory']['rss_bytes']/1048576,
             first_wave_inverse_latency_jain=sum(inverse)**2/(len(inverse)*sum(x*x for x in inverse)),
             first_wave_max_min_ratio=max(first)/min(first),peak_overlapping_requests=peak,
             first_wave_start_spread_ms=(max(x['started_seconds'] for x in calls if x['index']<c)
                                        -min(x['started_seconds'] for x in calls if x['index']<c))*1000,
             rows={name:r['database']['rows'].get(name) for name in fixed},
             storage_mib=r['closed_storage']['total_bytes']/1048576)
    if r['diagnostic']:
        a,z = r['sql_before'],r['sql_after']
        batch = {k:z['batches'][k]-a['batches'].get(k,0) for k in ('count','records','runtime_events','multi_session_batches')}
        for name in ('sessions_per_batch','runtime_events_per_batch'):
            batch[name] = {k:v-a['batches'].get(name,{}).get(k,0) for k,v in z['batches'][name].items()}
            assert sum(batch[name].values()) == batch['count']
        batch['events_per_batch'] = batch['runtime_events']/batch['count']
        m.update(batches=batch,driver_commits=z['phases']['driver_commit']['calls']-a['phases']['driver_commit']['calls'],
                 sql_commits=z['commits']-a['commits'],queue=z['queue'],
                 statements=z['statements']-a['statements'],
                 phases={name:{k:v-a['phases'].get(name,{}).get(k,0) for k,v in values.items()}
                         for name,values in z['phases'].items()})
        m['events_per_driver_commit'] = batch['runtime_events']/m['driver_commits']
    details.append(m)
    groups[r['backend'],c,r['delay_ms'],r['diagnostic']].append(m)

summary=[]
for (backend,c,delay,diagnostic),runs in groups.items():
    item=dict(backend=backend,concurrency=c,delay_ms=delay,diagnostic=diagnostic,runs=len(runs),calls=sum(r['calls'] for r in runs))
    for k in ('throughput','p50_ms','p95_ms','max_ms','cpu_ms_call','rss_mib','warm_rss_mib','drained_rss_mib',
              'first_wave_inverse_latency_jain','first_wave_max_min_ratio','first_wave_start_spread_ms','storage_mib'):
        item[k]=st.median(r[k] for r in runs)
    item['process_samples'] = [{k:r[k] for k in ('turn','throughput','p50_ms','p95_ms','cpu_ms_call','rss_mib')} for r in runs]
    if delay:
        controls = groups[backend,c,0,diagnostic]
        pairs=[]
        for r in runs:
            control=next((x for x in controls if x['turn']==r['turn']), None)
            if control is None:
                continue
            assert r['rows']==control['rows'],(r['job'],r['rows'],control['rows'])
            source=next(x for x in rows if x['job']==r['job'])
            baseline=next(x for x in rows if x['job']==control['job'])
            assert source['expected_output']==baseline['expected_output']
            differences={k:[baseline['database']['rows'].get(k),v] for k,v in source['database']['rows'].items()
                         if baseline['database']['rows'].get(k)!=v}
            if differences: variables.append(dict(job=r['job'],control=control['job'],rows=differences))
            pairs.append(dict(turn=r['turn'],throughput_change_pct=(r['throughput']/control['throughput']-1)*100,
                              p50_change_pct=(r['p50_ms']/control['p50_ms']-1)*100,
                              p95_change_pct=(r['p95_ms']/control['p95_ms']-1)*100,
                              rss_delta_mib=r['rss_mib']-control['rss_mib']))
        item['pairs']=pairs
    summary.append(item)
(b/'summary.json').write_text(json.dumps(summary,indent=2))
(b/'details.json').write_text(json.dumps(details,indent=2))
(b/'variable-counts.json').write_text(json.dumps(variables,indent=2))
print(json.dumps(dict(runs=len(rows),calls=sum(r['completed'] for r in rows))))
for r in summary:
    if not r['diagnostic']:
        print(r['backend'],r['concurrency'],r['delay_ms'],r['runs'],
              'p50',round(r['p50_ms'],1),'p95',round(r['p95_ms'],1),'rps',round(r['throughput'],3),
              'cpu',round(r['cpu_ms_call'],1),'rss',round(r['rss_mib'],1),
              'gains',[round(x['throughput_change_pct'],1) for x in r.get('pairs',[])])
