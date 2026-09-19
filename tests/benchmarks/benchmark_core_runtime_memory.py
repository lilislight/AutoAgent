"""Bounded isolated-process Core RSS audit for large loop/Map payloads.

The parent stops workers below hypothetical container limits. No Event history,
Host, persistence, saved checkpoints or large public results are retained.
"""
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

from typing_extensions import TypedDict
from autoagent import (AggregationContext, AutoAgentApp, ConditionContext, ContextOperation, ContextPatch,
                       Edge, InputMappingContext, Map, Node, OutputBindingContext, Workflow)

MIB = 1024 * 1024


class Seed(TypedDict):
    step: int


class Row(TypedDict):
    id: int
    value: int


class Data(TypedDict):
    step: int
    blob: str
    rows: list[Row]


def rss(pid='self'):
    try:
        for line in Path(f'/proc/{pid}/status').read_text().splitlines():
            if line.startswith('VmRSS:'):
                return int(line.split()[1]) * 1024
    except (FileNotFoundError, ProcessLookupError):
        pass
    return 0


def point(stage, **values):
    row = {'kind': 'point', 'stage': stage, 'rss_mib': rss() / MIB,
           'maxrss_mib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, **values}
    if tracemalloc.is_tracing():
        row['python_live_mib'], row['python_peak_mib'] = (x / MIB for x in tracemalloc.get_traced_memory())
    print(json.dumps(row), flush=True)
    return row


class Probe:
    def __init__(self, nodes_per_cycle):
        self.nodes_per_cycle = nodes_per_cycle
        self.events = 0
        self.cycles = {}
        self.big_outputs = 0

    async def append(self, event):
        self.events += 1
        # Only scalars are kept; the Event and all its payload references expire.
        if event.payload.kind == 'operator_call.completed':
            output = event.payload.output
            if hasattr(output, 'get') and (output.get('blob') or output.get('rows')):
                self.big_outputs += 1
                if self.big_outputs % 4 == 0:
                    point('outputs', big_outputs=self.big_outputs, events=self.events)
        if event.payload.kind == 'node_occurrence.completed':
            output = event.payload.output
            if isinstance(output, dict) or hasattr(output, 'get'):
                step = output.get('step', 0)
                if 'blob' in output and step and step % self.nodes_per_cycle == 0:
                    self.cycles[event.session_id] = step // self.nodes_per_cycle
                    point('cycle', cycles=dict(self.cycles), events=self.events)
        # An asynchronous ACK boundary permits simultaneous Invocation progress.
        await asyncio.sleep(0)


def make_workflow(case):
    target_bytes = int(case.get('payload_mib', 1) * MIB)
    width = case.get('map_width', 0)
    mixed = case.get('mixed', False)
    nodes = 1 if width and not mixed else 3
    total = case['cycles'] * nodes
    context_mode = case.get('context', 'overwrite')
    shape = case.get('shape', 'text')
    salt = 0
    # Constant seven-digit integers give a predictable ~1 MiB compact JSON list.
    row_bytes = len(json.dumps({'id': 1000000, 'value': 2000000}, separators=(',', ':'))) + 1
    row_count = max(0, (int(case.get('query_mib', case.get('payload_mib', 1)) * MIB) - 64) // row_bytes)
    shared_blob = 'S' * target_bytes if shape == 'shared_text' else None

    async def entry(value: Seed) -> Data:
        return {'step': value['step'], 'blob': '', 'rows': []}

    def blob(number):
        prefix = f'{number:012d}:'
        return prefix + ('x' * max(0, target_bytes - len(prefix)))

    async def produce(value: Data) -> Data:
        nonlocal salt
        await asyncio.sleep(.001 if width else 0)
        salt += 1
        if shape == 'rows':
            return {'step': value['step'] + 1, 'blob': '',
                    'rows': [{'id': 1000000 + i, 'value': 2000000 + salt * 1000 + i}
                             for i in range(row_count)]}
        return {'step': value['step'] + 1,
                'blob': shared_blob if shape == 'shared_text' else blob(salt) if shape == 'text' else '',
                'rows': []}

    async def query(value: Data) -> Data:
        nonlocal salt
        await asyncio.sleep(.001)
        salt += 1
        return {'step': value['step'] + 1, 'blob': '',
                'rows': [{'id': 1000000 + i, 'value': 2000000 + salt * 1000 + i}
                         for i in range(row_count)]}

    async def bind(context: OutputBindingContext) -> ContextPatch:
        nonlocal salt
        if context_mode == 'none':
            return ContextPatch()
        if shape == 'context_only':
            salt += 1
            value = blob(salt)
        else:
            value = context.output
        if context_mode == 'append':
            value = (*context.invocation_context.get('history', ()), value)
        key = 'history' if context_mode == 'append' else 'latest'
        if context_mode == 'growing_prompt':
            key = 'prompt'
            value = context.invocation_context.get('prompt', '') + context.output['blob']
        return ContextPatch(invocation=(ContextOperation.set(key, value),))

    async def again(context: ConditionContext) -> bool:
        return context.output['step'] < total

    async def finish(context: ConditionContext) -> bool:
        return context.output['step'] >= total

    async def final(value: Data) -> Seed:
        return {'step': value['step']}

    async def stripped(context: InputMappingContext) -> Data:
        incoming = next(iter(context.incoming.values()))
        return {'step': incoming['step'], 'blob': '', 'rows': []}

    async def prompt_input(context: InputMappingContext) -> Data:
        incoming = next(iter(context.incoming.values()))
        return {'step': incoming['step'], 'blob': context.invocation_context.get('prompt', ''), 'rows': []}

    async def mapped(context: InputMappingContext) -> list[Data]:
        small = await stripped(context)
        return [dict(small) for _ in range(width)]

    async def aggregate(context: AggregationContext) -> Data:
        return {'step': context.outputs[0]['step'], 'blob': '', 'rows': []}

    names = ['a', 'map', 'c'] if mixed else ['map'] if width else ['a', 'b', 'c']
    workflow = Workflow('runtime-memory', nodes=[Node('entry', entry), *[
        Node(name, query if mixed and name == 'map' else produce, output_binding=bind,
             input_mapping=(mapped if name == 'map' else prompt_input if context_mode == 'growing_prompt'
                            else stripped if case.get('strip_input') else None),
             map=Map(max_parallelism=width, aggregate=aggregate) if name == 'map' else None)
        for name in names], Node('exit', final)],
        edges=[Edge('entry', names[0]), *[Edge(a, b) for a, b in zip(names, names[1:])],
               Edge(names[-1], names[0], again, id='repeat'), Edge(names[-1], 'exit', finish, id='finish')])
    return workflow, nodes, row_count


def inspect_states(app):
    occurrences = calls = big_calls = shared_outputs = 0
    blob_ids = {}
    row_arrays = set()
    states = app._repository
    for sid in states.session_ids():
        inv = states.state(sid).invocation
        occurrences += len(inv.scheduler.occurrences)
        calls += len(inv.scheduler.operator_calls)
        for call in inv.scheduler.operator_calls.values():
            output = call.output
            if not hasattr(output, 'get'):
                continue
            if output.get('blob') or output.get('rows'):
                big_calls += 1
                occurrence = inv.scheduler.occurrences[call.occurrence_id]
                shared_outputs += occurrence.output is output
            for value in (call.input, call.output):
                if not hasattr(value, 'get'):
                    continue
                if value.get('blob'):
                    blob_ids[id(value['blob'])] = len(value['blob'])
                if value.get('rows'):
                    row_arrays.add(id(value['rows']))
    return {'resident_sessions': len(states.session_ids()), 'occurrences': occurrences,
            'calls': calls, 'big_calls': big_calls, 'call_node_output_same_object': shared_outputs,
            'unique_blob_objects_in_calls': len(blob_ids), 'unique_blob_mib_in_calls': sum(blob_ids.values()) / MIB,
            'distinct_row_arrays_in_call_inputs_outputs': len(row_arrays),
            'pending_events': len(states._pending)}


def worker(case):
    # Backup virtual-address-space limit; RSS is separately policed by the parent.
    resource.setrlimit(resource.RLIMIT_AS, (3 * 1024**3, 3 * 1024**3))
    workflow, nodes, row_count = make_workflow(case)
    probe = Probe(nodes)
    app = AutoAgentApp(runtime_event_sink=probe, max_operator_concurrency=32)
    app.register_workflow(workflow)
    gc.collect()
    if case.get('trace'):
        tracemalloc.start()
    point('baseline', row_count=row_count)
    count = case.get('invocations', 1)
    refs = []
    started = time.perf_counter()
    if case.get('sequential'):
        for index in range(count):
            result = app.invoke(workflow.id, {'step': 0},
                                session_id='reuse' if case.get('reuse') else f'root-{index}')
            assert result.status == 'completed', result.error
            if not case.get('reuse') or not refs:
                refs.append(result.ref)
            elif case.get('reuse'):
                refs[0] = result.ref
            del result
            gc.collect()
            point('invocation_done', index=index + 1, **inspect_states(app))
    else:
        async def concurrent():
            results = await asyncio.gather(*(app._invoke(workflow.id, {'step': 0}, session_id=f'root-{i}',
                session_context=None, entry_node_id=None, wait_for_boundary=True) for i in range(count)))
            assert all(r.status == 'completed' and r.output == {'step': case['cycles'] * nodes}
                       for r in results), [r.error for r in results]
            return [r.ref for r in results]
        refs = app._runtime_loop.run(concurrent())
    elapsed = time.perf_counter() - started
    gc.collect()
    point('after_run', elapsed_s=elapsed, **inspect_states(app))
    if case.get('cleanup', 'unload') == 'unload':
        for ref in refs:
            assert app.unload_session(ref) is None
        gc.collect()
        point('after_unload', resident_sessions=len(app._repository.session_ids()))
    assert app.close() is None
    gc.collect()
    point('after_close', resident_sessions=len(app._repository.session_ids()))
    del app
    gc.collect()
    point('after_drop_app')
    if tracemalloc.is_tracing():
        tracemalloc.stop()
    print(json.dumps({'kind': 'done', 'case': case}), flush=True)


def representation_probe():
    """Separate intrinsic Python-container size from compact JSON transfer size."""
    from autoagent.core.runtime.values import freeze
    count = 33822
    tracemalloc.start()
    rows = [{'id': 1000000 + i, 'value': 2000000 + i} for i in range(count)]
    raw = tracemalloc.get_traced_memory()[0]
    json_bytes = len(json.dumps(rows, separators=(',', ':')).encode())
    frozen = freeze(rows)
    both = tracemalloc.get_traced_memory()[0]
    del rows
    gc.collect()
    owned = tracemalloc.get_traced_memory()[0]
    del frozen
    gc.collect()
    released = tracemalloc.get_traced_memory()[0]
    tracemalloc.stop()
    return {'rows': count, 'compact_json_bytes': json_bytes, 'python_raw_live_bytes': raw,
            'raw_and_frozen_live_bytes': both, 'frozen_only_live_bytes': owned,
            'after_release_live_bytes': released,
            'method': 'Separate tracemalloc probe of the worker list/dict shape; no App or Event retention; not RSS.'}


def default_cases():
    cases = []
    for size in (1, 2):
        for cycles in (10, 30, 60):
            cases.append({'name': f'text_{size}m_{cycles}cycles', 'payload_mib': size, 'cycles': cycles})
    cases.extend([
        {'name': 'text_no_context', 'cycles': 30, 'payload_mib': 2, 'context': 'none'},
        {'name': 'same_text_reference', 'cycles': 30, 'payload_mib': 2, 'shape': 'shared_text'},
        {'name': 'context_only_overwrite', 'cycles': 60, 'payload_mib': 2, 'shape': 'context_only'},
        {'name': 'growing_prompt_single', 'cycles': 6, 'payload_mib': 1, 'context': 'growing_prompt'},
        {'name': 'growing_prompt_5_invocations', 'cycles': 6, 'payload_mib': 1, 'context': 'growing_prompt', 'invocations': 5, 'rss_limit_mib': 896},
        {'name': 'context_only_append', 'cycles': 30, 'payload_mib': 2, 'shape': 'context_only', 'context': 'append'},
        {'name': 'text_3_invocations', 'cycles': 15, 'payload_mib': 2, 'invocations': 3},
        {'name': 'text_5_invocations', 'cycles': 10, 'payload_mib': 2, 'invocations': 5},
        {'name': 'text_5_invocations_512_limit', 'cycles': 25, 'payload_mib': 2, 'invocations': 5},
        {'name': 'text_5_invocations_1g_limit', 'cycles': 40, 'payload_mib': 2, 'invocations': 5, 'rss_limit_mib': 896},
        {'name': 'rows_forward', 'shape': 'rows', 'cycles': 4},
        {'name': 'rows_strip_input', 'shape': 'rows', 'cycles': 4, 'strip_input': True},
        {'name': 'map4_rows_single', 'shape': 'rows', 'cycles': 2, 'map_width': 4},
        {'name': 'map4_rows_3_invocations_one_cycle', 'shape': 'rows', 'cycles': 1, 'map_width': 4, 'invocations': 3},
        {'name': 'map4_rows_3_invocations', 'shape': 'rows', 'cycles': 3, 'map_width': 4, 'invocations': 3},
        {'name': 'map8_rows_5_invocations', 'shape': 'rows', 'cycles': 4, 'map_width': 8, 'invocations': 5},
        {'name': 'mixed_3_invocations_one_cycle', 'mixed': True, 'cycles': 1, 'payload_mib': 2, 'query_mib': 1, 'map_width': 4, 'invocations': 3},
        {'name': 'mixed_3_invocations_512_limit', 'mixed': True, 'cycles': 3, 'payload_mib': 2, 'query_mib': 1, 'map_width': 4, 'invocations': 3},
        {'name': 'mixed_5_invocations_1g_limit', 'mixed': True, 'cycles': 3, 'payload_mib': 2, 'query_mib': 1, 'map_width': 4, 'invocations': 5, 'rss_limit_mib': 896},
        {'name': 'sequential_new_sessions', 'cycles': 10, 'invocations': 5, 'sequential': True},
        {'name': 'sequential_reused_session', 'cycles': 10, 'invocations': 5, 'sequential': True, 'reuse': True},
        {'name': 'trace_text_unload', 'cycles': 10, 'payload_mib': 2, 'trace': True},
        {'name': 'trace_context_overwrite', 'cycles': 20, 'payload_mib': 2, 'shape': 'context_only', 'trace': True},
        {'name': 'trace_close_only', 'cycles': 10, 'payload_mib': 2, 'trace': True, 'cleanup': 'close'},
        {'name': 'trace_map_rows', 'shape': 'rows', 'cycles': 1, 'map_width': 2, 'trace': True},
    ])
    return cases


def available_worker_budget():
    """Leave headroom for the controller and other cgroup residents on reruns."""
    caps = []
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'):
            caps.append(int(line.split()[1]) * 1024 // 2)
    root = Path('/sys/fs/cgroup')
    candidates = [root]
    try:
        for line in Path('/proc/self/cgroup').read_text().splitlines():
            if line.startswith('0::'):
                candidate = (root / line[3:].lstrip('/')).resolve()
                if candidate.is_relative_to(root):
                    candidates.extend([candidate, *[p for p in candidate.parents if p.is_relative_to(root)]])
        for directory in set(candidates):
            maximum = directory / 'memory.max'
            current = directory / 'memory.current'
            if maximum.exists() and current.exists():
                value = maximum.read_text().strip()
                if value != 'max':
                    caps.append(max(0, int(value) - int(current.read_text()) - 64 * MIB))
    except (OSError, ValueError):
        pass
    return min(caps, default=256 * MIB)


def controller(output, names):
    output.mkdir(parents=True, exist_ok=True)
    selected = set(names or ())
    previous = json.loads((output / 'results.json').read_text()) if selected and (output / 'results.json').exists() else []
    results = [row for row in previous if row['case']['name'] not in selected]
    for case in default_cases():
        if names and case['name'] not in names:
            continue
        path = output / (case['name'] + '.jsonl')
        limit = min(case.get('rss_limit_mib', 448) * MIB, available_worker_budget())
        if limit < 64 * MIB:
            raise RuntimeError('Insufficient memory headroom to start a benchmark worker.')
        start = time.monotonic()
        peak = 0
        status = 'completed'
        with path.open('w') as stream, (output / (case['name'] + '.stderr')).open('w') as errors:
            process = subprocess.Popen([sys.executable, '-m', 'tests.benchmarks.benchmark_core_runtime_memory', '--worker', json.dumps(case)],
                                       stdout=stream, stderr=errors)
            while process.poll() is None:
                peak = max(peak, rss(process.pid))
                if peak > limit or time.monotonic() - start > 180:
                    status = 'rss_guard_stopped' if peak > limit else 'timeout'
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                    break
                time.sleep(.01)
        records = [json.loads(line) for line in path.read_text().splitlines() if line.startswith('{')]
        if status == 'completed' and (process.returncode or not records or records[-1].get('kind') != 'done'):
            status = 'error'
        row = {'case': case, 'status': status, 'returncode': process.returncode,
               'parent_observed_peak_rss_mib': peak / MIB, 'rss_guard_mib': limit / MIB,
               'points': [r for r in records if r['kind'] == 'point']}
        results.append(row)
        (output / 'results.json').write_text(json.dumps(results, indent=2))
        print(case['name'], status, f'{peak/MIB:.1f} MiB', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker')
    parser.add_argument('--representation', action='store_true')
    parser.add_argument('--output', type=Path, default=Path('artifacts/core_runtime_memory'))
    parser.add_argument('--cases', nargs='*')
    args = parser.parse_args()
    if args.worker:
        worker(json.loads(args.worker))
    elif args.representation:
        print(json.dumps(representation_probe(), indent=2))
    else:
        controller(args.output, args.cases)
