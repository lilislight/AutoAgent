"""Measure a resident Spawn graph while its last large-input Child is blocked."""
import asyncio
import gc
import json
import statistics
import threading
import time
import tracemalloc
from typing_extensions import TypedDict
from autoagent import AutoAgentApp, InputMappingContext, Map, Node, Workflow


class BigInput(TypedDict):
    index: int
    text: str
    rows: list[dict[str, str]]


class Output(TypedDict):
    value: int
    text: str


class Sink:
    count = 0
    async def append(self, event):
        self.count += 1


def sample(count=64, size=1024*1024, rows=False, retain_output=False):
    release = threading.Event()
    def inputs(ctx: InputMappingContext) -> list[BigInput]:
        return [{'index': i, 'text': '' if rows else f'{i}:' + 'x' * size,
                 'rows': [{'key': f'{i}:{j}', 'value': 'v'*64} for j in range(size//128)] if rows else []}
                for i in range(count)]
    async def work(value: BigInput) -> Output:
        if value['index'] == count - 1:
            while not release.is_set():
                await asyncio.sleep(.002)
        return {'value': value['index'], 'text': value['text'] if retain_output else ''}
    child = Workflow('child', nodes=[Node('work', work)])
    root = Workflow('root', nodes=[Node('children', child, execution_mode='spawn',
        input_mapping=inputs, map=Map(max_parallelism=8))])
    sink = Sink()
    app = AutoAgentApp(runtime_event_sink=sink)
    try:
        app.register_workflow(root)
        gc.collect()
        tracemalloc.start()
        started = time.perf_counter()
        ref = app.submit_invoke(root.id, {}).ref
        async def stable():
            while True:
                states = [app._repository.state(sid) for sid in app._repository.session_ids() if sid != ref.session_id]
                inv = app._repository.state(ref.session_id).invocation
                if len(states) == count and sum(s.invocation is not None and s.invocation.terminal for s in states) == count-1 and inv.status in {'joining_children', 'settling'}:
                    # Await each finished Child finalizer, including its compact ACK.
                    if sum(app._task_runtime.is_live(s.session.id) for s in states) == 1:
                        return
                await asyncio.sleep(.002)
        app._runtime_loop.run(asyncio.wait_for(stable(), 60))
        elapsed = (time.perf_counter()-started)*1000
        gc.collect()
        retained, peak = tracemalloc.get_traced_memory()
        compacted = sum(type(app._repository.state(sid).invocation).__name__ == 'ChildResult'
                        for sid in app._repository.session_ids())
        release.set()
        assert app.join(ref, timeout=30).status == 'completed'
        app.unload_session(ref)
        gc.collect()
        after, _ = tracemalloc.get_traced_memory()
        return {'retained_bytes': retained, 'peak_bytes': peak, 'after_unload_bytes': after,
                'compacted_children': compacted, 'traced_elapsed_ms': elapsed, 'events': sink.count}
    finally:
        release.set()
        tracemalloc.stop()
        app.close()


if __name__ == '__main__':
    print(json.dumps({name: sample(*args) for name, args in {
        'text_64x1MiB_small_output': (64, 1024*1024, False, False),
        'rows_32x2048_small_output': (32, 256*1024, True, False),
        'text_32x1MiB_retained_output': (32, 1024*1024, False, True),
    }.items()}, indent=2))
