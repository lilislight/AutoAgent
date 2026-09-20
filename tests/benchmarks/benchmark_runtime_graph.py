"""Compare complete graph execution on both lifecycle models, using a counting Sink."""
import asyncio
import gc
import json
import resource
import statistics
import time
import tracemalloc
from typing_extensions import TypedDict
from autoagent import AutoAgentApp, Workflow, Node, Map, InputMappingContext


class Input(TypedDict):
    value: int
    count: int
    size: int


class Output(TypedDict):
    text: str
    rows: list[dict[str, str]]


def produce(value: Input) -> Output:
    size = value['size']
    return {'text': 't' * size + str(value['value']),
            'rows': [{'value': 'd' * min(size, 1024) + str(i)} for i in range(64 if size else 0)]}


def items(context: InputMappingContext) -> list[Input]:
    value = context.invocation_input
    return [{'value': i, 'count': 1, 'size': value['size']} for i in range(value['count'])]


class Sink:
    def __init__(self): self.count = 0
    async def append(self, event): self.count += 1


async def drain(app):
    while tasks := [app._task_runtime.task(s) for s in app._task_runtime.active_sessions()]:
        await asyncio.gather(*(t for t in tasks if t is not None))
    assert all(app._repository.state(s).invocation.terminal for s in app._repository.session_ids())


def sample(count, depth, size, roots):
    sink = Sink()
    app = AutoAgentApp(runtime_event_sink=sink)
    workflow = Workflow('leaf', nodes=[Node('produce', produce)])
    for level in range(depth):
        workflow = Workflow(f'level-{level}', nodes=[Node('spawn', workflow, execution_mode='spawn')])
    if count > 1:
        workflow = Workflow('mapped', nodes=[Node('children', workflow, execution_mode='spawn', input_mapping=items, map=Map(max_parallelism=32))])
    app.register_workflow(workflow)
    async def run():
        results = await asyncio.gather(*(app.ainvoke(workflow.id, {'value': i, 'count': count, 'size': size}, session_id=f'root-{i}') for i in range(roots)))
        await app._await(app._submit(drain(app)))
        return results
    # Measure outside tracing for latency; tracing changes Python execution cost.
    timings = []
    try:
        for _ in range(4):
            start = time.perf_counter_ns()
            results = asyncio.run(run())
            timings.append((time.perf_counter_ns() - start) / 1e6)
        gc.collect()
        tracemalloc.start()
        results = asyncio.run(run())
        gc.collect()
        retained, peak = tracemalloc.get_traced_memory()
        before = len(app._repository.session_ids())
        for r in results:
            app.unload_session(r.ref)
        del results
        gc.collect()
        after, _ = tracemalloc.get_traced_memory()
        return {'median_ms': statistics.median(timings[1:]), 'samples_ms': timings[1:],
                'retained_bytes': retained, 'peak_bytes': peak, 'after_unload_bytes': after,
                'sessions_before_unload': before, 'sessions_after_unload': len(app._repository.session_ids()),
                'events_total': sink.count, 'rss_high_water_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
    finally:
        if tracemalloc.is_tracing(): tracemalloc.stop()
        app.close()


def main():
    cases = {'no_child': (1, 0, 0, 1), 'map_32': (32, 0, 0, 1),
             'map_100': (100, 0, 0, 1), 'nested_4': (1, 4, 0, 1),
             'five_roots_large_outputs': (4, 1, 1024 * 1024, 5)}
    print(json.dumps({name: sample(*args) for name, args in cases.items()}, indent=2))


if __name__ == '__main__': main()
