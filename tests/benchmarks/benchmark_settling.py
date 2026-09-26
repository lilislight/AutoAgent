"""Measure cancellation after every leaf starts; compatible with the baseline Core."""
import asyncio
import json
import statistics
import time
from typing_extensions import TypedDict
from autoagent import AutoAgentApp, InputMappingContext, Map, Node, Workflow


class Value(TypedDict):
    count: int


def items(ctx: InputMappingContext) -> list[Value]:
    return [{'count': 1} for _ in range(ctx.invocation_input['count'])]


class CountingSink:
    def __init__(self):
        self.count = 0

    async def append(self, event):
        self.count += 1


def sample(count, depth):
    timings, events = [], []
    for _ in range(8):
        started = 0
        async def slow(value: Value) -> Value:
            nonlocal started
            started += 1
            await asyncio.Event().wait()
            return value
        workflow = Workflow('leaf', nodes=[Node('slow', slow)])
        for level in range(depth):
            workflow = Workflow(f'level-{level}', nodes=[Node('spawn', workflow, execution_mode='spawn')])
        if count > 1:
            workflow = Workflow('mapped', nodes=[Node('spawn', workflow, execution_mode='spawn',
                input_mapping=items, map=Map(max_parallelism=count))])
        sink = CountingSink()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            ref = app.submit_invoke(workflow, {'count': count}).ref
            async def measure():
                while started != count:
                    await asyncio.sleep(0)
                before = sink.count
                start = time.perf_counter_ns()
                result = await app._cancel(ref, 'benchmark')
                elapsed = (time.perf_counter_ns() - start) / 1e6
                assert result.status == 'cancelled'
                assert not app._task_runtime.active_sessions()
                return elapsed, sink.count - before
            elapsed, event_count = app._runtime_loop.run(asyncio.wait_for(measure(), 10))
            timings.append(elapsed)
            events.append(event_count)
        finally:
            app.close()
    return {'median_ms': statistics.median(timings[1:]), 'samples_ms': timings[1:],
            'cancel_events': events[1:]}


if __name__ == '__main__':
    print(json.dumps({name: sample(*args) for name, args in {
        'cancel_leaf': (1, 0), 'cancel_map_32': (32, 0), 'cancel_nested_4': (1, 4)
    }.items()}, indent=2))
