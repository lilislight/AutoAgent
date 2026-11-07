"""Audit and report frozen-package projection/control Server measurements."""
from collections import defaultdict
import json
import math
from pathlib import Path
import statistics as st

ROOT=Path(__file__).resolve().parents[3]
BASE=ROOT/'.autoagent/performance/projection-control-2026-10-25'
MIB=1024*1024
FACTS=('runtime_events','user_events','workflow_revisions','operator_definitions','sessions','invocations')
DETAILS=('node_executions','operator_calls','waits','edge_evaluations','graph_changes')


def median(rows,fn):return st.median(fn(row)for row in rows)


def run_peak(row):
    peak=0
    for line in (Path(row['directory'])/'memory.jsonl').read_text().splitlines():
        point=json.loads(line)
        if point['phase']=='running':peak=max(peak,point['rss_bytes'])
    return peak/MIB


def metric(rows):
    return dict(rounds=len(rows),p50_ms=median(rows,lambda r:r['latency_ms']['p50']),
        p95_ms=median(rows,lambda r:r['latency_ms']['p95']),
        cpu_ms_call=median(rows,lambda r:(r['drained_memory']['cpu_seconds']-r['warm_memory']['cpu_seconds'])*1000/r['completed']),
        run_rss_mib=median(rows,run_peak),lifetime_rss_mib=median(rows,lambda r:r['rss_bytes']/MIB),
        storage_mib=median(rows,lambda r:r['closed_storage']['total_bytes']/MIB),
        warm_storage_mib=median(rows,lambda r:r['warm_storage']['total_bytes']/MIB))


def main():
    raw=[json.loads(line)for line in (BASE/'results.jsonl').read_text().splitlines()]
    errors=[];groups=defaultdict(list);outputs={}
    for row in raw:
        if not row['accepted_result']:errors.append(dict(job=row['job'],boundary='run failed'))
        if row['status']!='completed':continue
        key=(row['phase'],row['case']['name'])
        if key in outputs and outputs[key]!=row['expected_output']:
            errors.append(dict(job=row['job'],boundary='business output differs'))
        outputs.setdefault(key,row['expected_output'])
        counts=row.get('database',{}).get('rows',{})
        if row['version']=='after':
            unexpected=set(counts).intersection(DETAILS)
            if unexpected:errors.append(dict(job=row['job'],boundary='persistent detail tables exist',tables=sorted(unexpected)))
            for name in ('active_nodes','active_waits'):
                if counts.get(name,0):errors.append(dict(job=row['job'],boundary='completed work remains active',table=name,count=counts[name]))
        if row['phase']=='formal':groups[(row['backend'],row['mode'],row['case']['name'],row['version'])].append(row)
    comparisons=[]
    for backend,mode,name in sorted({key[:3]for key in groups}):
        before=groups.get((backend,mode,name,'before'),[]);after=groups.get((backend,mode,name,'after'),[])
        if not before or not after:continue
        b,a=metric(before),metric(after)
        for table in FACTS:
            bv={r['database']['rows'].get(table,0)for r in before};av={r['database']['rows'].get(table,0)for r in after}
            if bv!=av:errors.append(dict(backend=backend,mode=mode,case=name,boundary='fact counts differ',table=table,before=sorted(bv),after=sorted(av)))
        comparisons.append(dict(backend=backend,mode=mode,case=name,before=b,after=a,
            time_reduction_percent=100*(1-a['p50_ms']/b['p50_ms']),storage_reduction_percent=100*(1-a['storage_mib']/b['storage_mib'])))
    aggregate={mode:dict(cases=len(rows),time_reduction_percent=100*(1-math.exp(st.mean(math.log(r['after']['p50_ms']/r['before']['p50_ms'])for r in rows))))
               for mode in ('buffered','durable') if (rows:=[r for r in comparisons if r['mode']==mode])}
    diagnostics=[]
    for row in raw:
        if row['phase']!='diagnostic' or row['status']!='completed':continue
        b,a=row['sql_before'],row['sql_after']
        diagnostics.append(dict(backend=row['backend'],mode=row['mode'],version=row['version'],
            statements=a['statements']-b['statements'],selects=a['verbs'].get('SELECT',0)-b['verbs'].get('SELECT',0),
            batches=a['batches']['count']-b['batches']['count'],runtime_events=a['batches']['runtime_events']-b['batches']['runtime_events'],
            detail_rows=sum(row['database']['rows'].get(t,0)for t in DETAILS),
            operational_rows={t:row['database']['rows'].get(t,0)for t in ('sessions','invocations','active_nodes','active_waits')}))
    paired=[]
    paired_groups=defaultdict(list)
    for row in raw:
        if row['phase']=='paired':paired_groups[(row['backend'],row['case']['name'],row['version'])].append(row)
    for backend,name in sorted({key[:2]for key in paired_groups}):
        before=paired_groups[(backend,name,'before')];after=paired_groups[(backend,name,'after')]
        b=median(before,lambda r:r['latency_ms']['p50']);a=median(after,lambda r:r['latency_ms']['p50'])
        paired.append(dict(backend=backend,case=name,before_ms=b,after_ms=a,time_reduction_percent=100*(1-a/b)))
    viewer=[]
    for row in raw:
        if row['phase']!='viewer':continue
        calls=[json.loads(line)for line in (Path(row['directory'])/'calls.jsonl').read_text().splitlines()] if (Path(row['directory'])/'calls.jsonl').exists() else []
        reads=[call['trace_reads']for call in calls if call.get('trace_reads')]
        viewer.append(dict(backend=row['backend'],mode=row['mode'],version=row['version'],case=row['case']['name'],
            cold_ms=st.median(x[0]['milliseconds']for x in reads)if reads else None,
            warm_ms=st.median(y['milliseconds']for x in reads for y in x[1:])if reads else None,
            lifetime_rss_mib=row.get('rss_bytes',0)/MIB))
    summary=dict(runs=len(raw),historical_controls=sum('baseline_source'in r for r in raw),
        new_runs=sum('baseline_source'not in r for r in raw),comparisons=comparisons,aggregate=aggregate,paired=paired,diagnostics=diagnostics,viewer=viewer)
    (BASE/'audit-errors.json').write_text(json.dumps(errors,indent=2))
    (BASE/'summary.json').write_text(json.dumps(summary,indent=2))
    lines=['# Projection and control separation: steps 1–5','',
        'The baseline is V2 after the preceding query optimization. It is not the original unoptimized V2.',
        'Both packages run real Servers. Artifact threshold: 8 KiB; buffered window: 100 ms; durable delay: 0 ms.',
        'Formal controls reuse the preceding 120 runs only after all 210 baseline Python hashes match. Paired runs use the same current harness.',
        'Each formal configuration has five independent processes, 20 measured calls per process (unique-history: 128). No paid LLM calls are involved.','',
        '## Implementation and contract checks','',
        '1. Fixed Event history compares Graph, Timeline, Replay, Dataset, Node and Call details with zero DTO differences.',
        '2. Core owns boundary decisions. Durable operational indexes contain only active Nodes and Waits; recovery reads Operator bindings from CallStarted facts.',
        '3. Metadata projections use a byte-bounded TTL/LRU cache. Input/Output bodies remain in Events/Artifacts. Metadata readers release without cycle GC.',
        '4. Existing frontend APIs are retained. UI queries use a separate read-only pool. Immutable Dataset pages and historical cuts stay fixed.',
        '5. Detailed projection tables and their synchronous writes are removed. Accepted queries snapshot immutable values before releasing the Writer lock.',
        'Step 6 (offline database projections) is not implemented.','',
        'Browser verification covered live graph/Timeline, lazy Node Output, Replay stepping/return, and Child navigation. See browser-verification.json.',
        'The database format is now 6. Existing older databases are rejected; no automatic migration or compatibility layer is provided.','',
        '## Formal execution results','',
        '| Database | Mode | Workload | Before p50 ms | After p50 ms | Time reduction | Before RSS MiB | After RSS MiB | Before storage MiB | After storage MiB |',
        '|---|---|---|---:|---:|---:|---:|---:|---:|---:|']
    for r in comparisons:
        b,a=r['before'],r['after'];lines.append(f"| {r['backend']} | {r['mode']} | {r['case']} | {b['p50_ms']:.1f} | {a['p50_ms']:.1f} | {r['time_reduction_percent']:.1f}% | {b['run_rss_mib']:.1f} | {a['run_rss_mib']:.1f} | {b['storage_mib']:.2f} | {a['storage_mib']:.2f} |")
    lines+=['','RSS is the sampled process peak during running, not a cache-size estimate. Storage includes the remaining SQLite WAL/SHM files or PostgreSQL system/database overhead. Negative reductions are retained.','',
        'Aggregate time reduction is the geometric mean of normalized per-scenario p50 ratios; scenarios receive equal weight.']
    for mode,values in aggregate.items():lines.append(f"- {mode}: {values['time_reduction_percent']:.2f}% time reduction across {values['cases']} database/workload configurations.")
    lines+=['','## Current-machine paired checks','','Three alternating before/after rounds, ten calls per process. These supplement the historical formal controls; they do not replace them.','',
        '| Database | Workload | Before p50 ms | After p50 ms | Time reduction |',
        '|---|---|---:|---:|---:|']
    for p in paired:lines.append(f"| {p['backend']} | {p['case']} | {p['before_ms']:.1f} | {p['after_ms']:.1f} | {p['time_reduction_percent']:.1f}% |")
    lines+=['','## Server CPU per call','','CPU comes from process CPU-time deltas between warmup and the drained measurement. It excludes the separate PostgreSQL process.','',
        '| Database | Mode | Workload | Before CPU ms/call | After CPU ms/call |',
        '|---|---|---|---:|---:|']
    for r in comparisons:lines.append(f"| {r['backend']} | {r['mode']} | {r['case']} | {r['before']['cpu_ms_call']:.1f} | {r['after']['cpu_ms_call']:.1f} |")
    lines+=['','## SQL diagnostics (five Chain calls)','','| Database | Mode | Version | SELECT | Total SQL | Write batches | Runtime Events | Stored detail rows |','|---|---|---|---:|---:|---:|---:|---:|']
    for d in diagnostics:lines.append(f"| {d['backend']} | {d['mode']} | {d['version']} | {d['selects']} | {d['statements']} | {d['batches']} | {d['runtime_events']} | {d['detail_rows']} |")
    lines+=['','The projection_write diagnostic label now measures minimal operational-index writes. Nested phase elapsed times must not be added together. SQL COMMIT notifications include read transactions and are not fsync counts.',
        'Detailed rows are removed without reducing Runtime Event facts. Durable still performs 310 write batches for 310 measured Events; the formal durable latency gain is small. CPU also remains substantial, so these measurements do not establish fsync as the sole remaining bottleneck.',
        'SELECT counts do not improve in every case: PostgreSQL buffered Chain rises from 216 to 305. The after-run includes 60 active_nodes lookups in write_transaction and 61 in observation_sql. Removing unnecessary active-index existence reads is a concrete remaining optimization; it was not hidden or folded into the reported gains.','',
        '## UI read cost','','| Database | Mode | Workload | Version | Cold trace ms | Warm trace ms | Lifetime RSS MiB |','|---|---|---|---|---:|---:|---:|']
    for v in viewer:
        if v['cold_ms']is not None:lines.append(f"| {v['backend']} | {v['mode']} | {v['case']} | {v['version']} | {v['cold_ms']:.1f} | {v['warm_ms']:.1f} | {v['lifetime_rss_mib']:.1f} |")
    lines+=['','Cold reads reconstruct from Events and can be slower than precomputed database projections. Cache eviction affects read latency, not execution correctness. Oversized projections are not pinned; construction has an explicit budget. No new hour-long memory soak was run.','',
        '## Validation and provenance','',f"Runs: {summary['runs']}; reused historical controls: {summary['historical_controls']}; real new runs: {summary['new_runs']}; audit errors: {len(errors)}.",
        'See provenance.json, contract-differences.json, browser-verification.json, verification/summary.json, results.jsonl, and per-run logs.',
        'Complete regression is run per module, as in the baseline, so unrelated accumulated process RSS does not trigger Host admission. Earlier monolithic attempts and intermediate failures are retained separately; they are not reported as green.',
        'Known baseline failure: weather example CLI test references the removed examples.deepseek_weather_assistant.__main__ (openai/litellm subcases). Unrelated example implementation was not changed.']
    (BASE/'report.md').write_text('\n'.join(lines)+'\n')
    print(json.dumps(dict(runs=len(raw),audit_errors=len(errors),aggregate=aggregate)))


if __name__=='__main__':main()
