"""Aggregate version-separated acceptance results without mixing failed trials."""
from __future__ import annotations

import argparse
from collections import defaultdict
import json
import math
from pathlib import Path
import sqlite3
import statistics as st

FIXED=('runtime_events','user_events','workflow_revisions','operator_definitions','sessions',
       'invocations','node_executions','operator_calls','waits','graph_changes','edge_evaluations')
MIB=1048576


def memory(path, origin=0):
    buckets=defaultdict(lambda:dict(count=0,rss_sum=0,rss_max=0,cpu_first=None,cpu_last=None))
    peak=0
    with path.open()as stream:
        for line in stream:
            row=json.loads(line)
            if row['phase']!='running':continue
            peak=max(peak,row['rss_bytes'])
            bucket=buckets[int((row['seconds']-origin)//60)]
            bucket['count']+=1;bucket['rss_sum']+=row['rss_bytes'];bucket['rss_max']=max(bucket['rss_max'],row['rss_bytes'])
            if bucket['cpu_first']is None:bucket['cpu_first']=row['cpu_seconds']
            bucket['cpu_last']=row['cpu_seconds']
    return peak,[dict(minute=key,rss_mean_mib=v['rss_sum']/v['count']/MIB,
                      rss_peak_mib=v['rss_max']/MIB,cpu_seconds=v['cpu_last']-v['cpu_first'])
                 for key,v in sorted(buckets.items())]


def slope(points):
    if len(points)<2:return None
    xs=[p['minute']/60 for p in points];ys=[p['rss_mean_mib']for p in points]
    x,y=st.mean(xs),st.mean(ys)
    denominator=sum((a-x)**2 for a in xs)
    return sum((a-x)*(b-y)for a,b in zip(xs,ys))/denominator if denominator else 0


def closed_rows(row):
    if row['backend']=='postgres':return row['closed_storage'].get('rows',{})
    path=Path(row['directory'])/'runtime.db'
    with sqlite3.connect(f'file:{path}?mode=ro',uri=True,timeout=10)as conn:
        names=[r[0]for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
        return {name:conn.execute('SELECT COUNT(*) FROM "'+name.replace('"','""')+'"').fetchone()[0]for name in names}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',type=Path,default=Path('.autoagent/performance/v2-optimization/step-09'))
    args=parser.parse_args();base=args.directory.resolve()
    raw=[json.loads(line)for line in (base/'results.jsonl').read_text().splitlines()]
    outcomes=[{k:row.get(k)for k in ('job','phase','backend','version','mode','status','completed',
              'rejected','exit_code','shutdown_timeout','accepted_result','error','fault_health')}
              for row in raw]
    (base/'outcomes.json').write_text(json.dumps(outcomes,indent=2))
    groups=defaultdict(list);details=[];errors=[];variables=[];outputs={}
    for row in raw:
        if not row['accepted_result']:
            errors.append(dict(job=row['job'],status=row['status'],error=row.get('error')));continue
        if row['status']!='completed':continue
        business_key=(row['phase'],row['case']['name'])
        if business_key in outputs and outputs[business_key]!=row['expected_output']:
            errors.append(dict(job=row['job'],boundary='cross-version/backend business output mismatch'))
        outputs.setdefault(business_key,row['expected_output'])
        peak,minutes=memory(Path(row['directory'])/'memory.jsonl',row.get('measurement_start_seconds',0))
        metrics=dict(job=row['job'],phase=row['phase'],backend=row['backend'],version=row['version'],mode=row['mode'],
                     case=row['case']['name'],turn=row['turn'],calls=row['completed'],seconds=row['run_seconds'],
                     p50_ms=row['latency_ms']['p50'],p95_ms=row['latency_ms']['p95'],
                     cpu_ms_call=None if row['case'].get('restart_at_wait')else
                         (row['drained_memory']['cpu_seconds']-row['warm_memory']['cpu_seconds'])*1000/row['completed'],
                     throughput=row['completed']/row['run_seconds'],rss_mib=peak/MIB,
                     lifetime_rss_mib=row['rss_bytes']/MIB,lifetime_hwm_mib=row['peak_rss_bytes']/MIB,
                     warm_rss_mib=row['warm_memory']['rss_bytes']/MIB,
                     drained_rss_mib=row['drained_memory']['rss_bytes']/MIB,
                     storage_mib=row['closed_storage']['total_bytes']/MIB,
                     rows=row['database'].get('rows',{}))
        if row['version']!='v1':
            after=closed_rows(row);metrics['closed_rows']=after
            differences={name:[row['database']['rows'].get(name),after.get(name)]for name in FIXED
                         if row['database']['rows'].get(name)!=after.get(name)}
            if differences:errors.append(dict(job=row['job'],boundary='close changed canonical rows',rows=differences))
        if row['phase']=='diagnostic':
            a,z=row['sql_before'],row['sql_after']
            metrics['diagnostic']=dict(statements=z['statements']-a['statements'],selects=z['verbs'].get('SELECT',0)-a['verbs'].get('SELECT',0),
                driver_commits=z['phases']['driver_commit']['calls']-a['phases']['driver_commit']['calls'],
                batches={k:z['batches'][k]-a['batches'].get(k,0)for k in ('count','records','runtime_events')},
                queue=z['queue'],graph_cache=z['graph_cache'],metadata_cache=z['writer_metadata_cache'],
                phases={name:{k:v-a['phases'].get(name,{}).get(k,0)for k,v in item.items()}for name,item in z['phases'].items()})
        if row['phase']=='soak':
            relative=minutes
            tail=[p for p in relative if 30<=p['minute']<60]
            metrics['soak']=dict(duration_requested=row['duration_requested_seconds'],
                actual_seconds=row['run_seconds'],minute_samples=relative,last_half_rss_slope_mib_per_hour=slope(tail),
                first_quarter_rss_mib=st.median(p['rss_mean_mib']for p in relative if 0<=p['minute']<15),
                last_quarter_rss_mib=st.median(p['rss_mean_mib']for p in relative if 45<=p['minute']<60),
                health=[json.loads(line)for line in (Path(row['directory'])/'health.jsonl').read_text().splitlines()])
            if row['run_seconds']<3600:errors.append(dict(job=row['job'],boundary='short soak'))
        groups[row['phase'],row['backend'],row['case']['name'],row['version'],row['mode']].append(metrics)
        details.append(metrics)
    summary=[];comparisons=[]
    for key,runs in groups.items():
        phase,backend,case,version,mode=key
        item=dict(phase=phase,backend=backend,case=case,version=version,mode=mode,runs=len(runs),calls=sum(r['calls']for r in runs))
        for metric in ('p50_ms','p95_ms','cpu_ms_call','throughput','rss_mib','lifetime_rss_mib','lifetime_hwm_mib','warm_rss_mib','drained_rss_mib','storage_mib'):
            values=[r[metric]for r in runs if r[metric]is not None];item[metric]=st.median(values)if values else None
        item['process_samples']=[{k:r[k]for k in ('turn','p50_ms','p95_ms','cpu_ms_call','rss_mib','storage_mib')}for r in runs]
        summary.append(item)
        if version!='final' or phase not in ('formal','extras'):continue
        controls=groups.get((phase,backend,case,'b0',mode),[]);pairs=[]
        for row in runs:
            before=next((r for r in controls if r['turn']==row['turn']),None)
            if before is None:errors.append(dict(job=row['job'],boundary='missing B0'));continue
            mismatch={name:[before['rows'].get(name),row['rows'].get(name)]for name in FIXED
                      if before['rows'].get(name)!=row['rows'].get(name)}
            if mismatch:errors.append(dict(job=row['job'],boundary='canonical row mismatch',rows=mismatch))
            variable={name:[before['closed_rows'].get(name),value]for name,value in row['closed_rows'].items()
                      if name not in FIXED and before['closed_rows'].get(name)!=value}
            if variable:variables.append(dict(job=row['job'],rows=variable))
            pair=dict(turn=row['turn'],p50_reduction_pct=(1-row['p50_ms']/before['p50_ms'])*100,
                      p95_reduction_pct=(1-row['p95_ms']/before['p95_ms'])*100,
                      rss_delta_mib=row['rss_mib']-before['rss_mib'],storage_delta_mib=row['storage_mib']-before['storage_mib'])
            if row['cpu_ms_call']is not None:pair['cpu_reduction_pct']=(1-row['cpu_ms_call']/before['cpu_ms_call'])*100
            pairs.append(pair)
        if not controls:continue
        control={metric:st.median(r[metric]for r in controls)for metric in ('p50_ms','p95_ms','rss_mib','storage_mib')}
        comparisons.append(dict(phase=phase,backend=backend,case=case,mode=mode,b0=control,final=item,
            p50_reduction_pct=(1-item['p50_ms']/control['p50_ms'])*100,
            p95_reduction_pct=(1-item['p95_ms']/control['p95_ms'])*100,pairs=pairs))
    for name,value in (('summary',summary),('details',details),('comparisons',comparisons),('variable-counts',variables),('audit-errors',errors)):
        (base/(name+'.json')).write_text(json.dumps(value,indent=2))
    formal=[r for r in comparisons if r['phase']=='formal']
    aggregate={mode:dict(cases=len(rows),geometric_mean_speedup=math.exp(st.mean(math.log(r['b0']['p50_ms']/r['final']['p50_ms'])for r in rows)))
               for mode in ('durable','buffered')if(rows:=[r for r in formal if r['mode']==mode])}
    print(json.dumps(dict(total_runs=len(raw),completed_runs=sum(r['status']=='completed'for r in raw),
                         calls=sum(r['completed']for r in raw),audit_errors=len(errors),formal_aggregate=aggregate),indent=2))
    if errors:raise SystemExit('Audit failures retained in audit-errors.json')


if __name__=='__main__':main()
