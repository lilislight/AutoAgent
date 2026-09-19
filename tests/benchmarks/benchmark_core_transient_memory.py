"""Isolated RSS/heap probes for transient Map copies and aggregate commit release."""
from __future__ import annotations
import argparse
import asyncio
import gc
import json
from pathlib import Path
import resource
import subprocess
import sys
import time
import tracemalloc

from autoagent import AutoAgentApp, AggregationContext, ContextPatch, Edge, InputMappingContext, Map, Node, OutputBindingContext, Workflow
from tests.benchmarks.benchmark_core_runtime_memory import Data, Seed, MIB, point, rss, available_worker_budget

CASES = [
    {'name': 'map_no_aggregate', 'invocations': 3, 'width': 4, 'rows': 16000},
    {'name': 'aggregate_binding_heap', 'invocations': 1, 'width': 4, 'rows': 4096, 'aggregate': True, 'trace': True},
]


def worker(case):
    resource.setrlimit(resource.RLIMIT_AS, (3 * 1024**3, 3 * 1024**3))
    async def query(value: Seed) -> Data:
        await asyncio.sleep(0)
        return {'step': value['step'], 'blob': '', 'rows': [
            {'id': 1000000 + i, 'value': 2000000 + i} for i in range(case['rows'])]}

    def inputs(context: InputMappingContext) -> list[Seed]:
        return [{'step': i} for i in range(case['width'])]

    def aggregate(context: AggregationContext) -> Seed:
        assert all(isinstance(v, dict) and isinstance(v['rows'], list) for v in context.outputs)
        return {'step': sum(len(v['rows']) for v in context.outputs)}

    def small(context: InputMappingContext) -> Seed:
        result = next(iter(context.incoming.values()))
        return result if case.get('aggregate') else {'step': sum(len(v['rows']) for v in result)}

    def finish(value: Seed) -> Seed:
        return value

    async def binding(context: OutputBindingContext) -> ContextPatch:
        gc.collect()
        point('binding', call_outputs_with_rows=sum(
            bool(c.output and c.output.get('rows')) for sid in app._repository.session_ids()
            for c in app._repository.state(sid).invocation.scheduler.operator_calls.values()))
        await asyncio.sleep(0)
        return ContextPatch()

    class Sink:
        async def append(self, event):
            await asyncio.sleep(0)

    workflow = Workflow('transient-probe', nodes=[
        Node('map', query, input_mapping=inputs,
             map=Map(max_parallelism=case['width'], aggregate=aggregate if case.get('aggregate') else None),
             output_binding=binding if case.get('aggregate') else None),
        Node('finish', finish, input_mapping=small)], edges=[Edge('map', 'finish')])
    app = AutoAgentApp(runtime_event_sink=Sink(), max_operator_concurrency=32)
    app.register_workflow(workflow)
    gc.collect()
    if case.get('trace'):
        tracemalloc.start()
    point('baseline')
    started = time.perf_counter()
    async def invoke_all():
        results = await asyncio.gather(*(app._invoke(workflow.id, {'step': 0}, session_id=f'root-{i}',
            session_context=None, entry_node_id=None, wait_for_boundary=True) for i in range(case['invocations'])))
        assert all(r.status == 'completed' and r.output == {'step': case['rows'] * case['width']} for r in results), results
    app._runtime_loop.run(invoke_all())
    gc.collect()
    point('after_run', elapsed_s=time.perf_counter() - started)
    app.close()
    print(json.dumps({'kind': 'done'}), flush=True)


def controller(output):
    output.mkdir(parents=True, exist_ok=True)
    results = []
    for case in CASES:
        path = output / (case['name'] + '.jsonl')
        limit = min(896 * MIB, available_worker_budget())
        if limit < 64 * MIB:
            raise RuntimeError('Insufficient memory headroom')
        start, peak, status = time.monotonic(), 0, 'completed'
        with path.open('w') as stream, (output / (case['name'] + '.stderr')).open('w') as errors:
            process = subprocess.Popen([sys.executable, '-m', __spec__.name, '--worker', json.dumps(case)], stdout=stream, stderr=errors)
            while process.poll() is None:
                peak = max(peak, rss(process.pid))
                if peak > limit or time.monotonic() - start > 120:
                    status = 'rss_guard_stopped' if peak > limit else 'timeout'
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                    break
                time.sleep(.01)
        records = [json.loads(line) for line in path.read_text().splitlines()]
        if status == 'completed' and (process.returncode or not records or records[-1].get('kind') != 'done'):
            status = 'error'
        results.append({'case': case, 'status': status, 'parent_peak_mib': peak / MIB, 'rss_guard_mib': limit / MIB, 'points': records})
        (output / 'transient_results.json').write_text(json.dumps(results, indent=2))
        print(case['name'], status, peak / MIB, flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker')
    parser.add_argument('--output', type=Path, required=False)
    args = parser.parse_args()
    if args.worker:
        worker(json.loads(args.worker))
    else:
        controller(args.output or Path('/tmp/core-transient-memory'))
