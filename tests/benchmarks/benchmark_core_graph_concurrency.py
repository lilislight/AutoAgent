"""Independent Session ACK overlap; preparation and profiling excluded from timings."""
import asyncio
import json
import statistics
import time
from autoagent import AutoAgentApp, Map, Node, Wait, Workflow
from tests.benchmarks.benchmark_core_audit import Value, identity, items


class DelaySink:
    def __init__(self):
        self.delay = 0
        self.active = 0
        self.peak = 0
        self.count = 0
        self.sessions = set()

    async def append(self, event):
        assert event.session_id not in self.sessions, 'overlapping same-Session append'
        self.sessions.add(event.session_id)
        self.active += 1
        self.peak = max(self.peak, self.active)
        self.count += 1
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
        finally:
            self.active -= 1
            self.sessions.remove(event.session_id)


def sample(kind, size, delay):
    sink = DelaySink()
    app = AutoAgentApp(runtime_event_sink=sink, max_operator_concurrency=100)
    waiting = Workflow('wait', nodes=[Node('approve', Wait(Value, Value))])
    try:
        if kind == 'children':
            parent = Workflow('parent', nodes=[Node('children', waiting,
                input_mapping=items, map=Map(max_parallelism=100))])
            result = app.invoke(parent, {'value': size})
            children = [app.join(ref) for ref in app.child_invocations(result.ref)]
        elif kind == 'roots':
            children = [app.invoke(waiting, {'value': i}) for i in range(size)]
        else:
            workflow = Workflow('map', nodes=[Node('map', identity,
                input_mapping=items, map=Map(max_parallelism=100))])
            app.register_workflow(workflow)
        sink.delay = delay
        sink.count = sink.peak = 0
        async def resume():
            results = await asyncio.gather(*(app._resume(c.ref, c.waits[0].id, {'value': i},
                wait_for_boundary=True) for i, c in enumerate(children)))
            assert all(r.status == 'completed' for r in results)
        start = time.perf_counter_ns()
        if kind == 'nodes':
            result = app.invoke(workflow.id, {'value': size})
            assert result.status == 'completed'
        else:
            app._runtime_loop.run(resume())
            if kind == 'children':
                assert app.join(result.ref).status == 'completed'
        elapsed = (time.perf_counter_ns() - start) / 1e6
        return {'elapsed_ms': elapsed, 'events': sink.count, 'peak_inflight_ack': sink.peak}
    finally:
        sink.delay = 0
        app.close()


def measure_graph_memory(size):
    """Measure retained State and coordination after ten same-Root replacements."""
    import gc
    import tracemalloc
    sink = DelaySink()
    app = AutoAgentApp(runtime_event_sink=sink, max_operator_concurrency=100)
    child = Workflow('memory-child', nodes=[Node('identity', identity)])
    parent = Workflow('memory-parent', nodes=[Node('children', child,
        input_mapping=items, map=Map(max_parallelism=100))])
    try:
        app.invoke(parent, {'value': size}, session_id='memory-root')
        gc.collect()
        tracemalloc.start()
        for _ in range(10):
            result = app.invoke(parent.id, {'value': size}, session_id='memory-root')
            assert result.status == 'completed'
            assert len(app._repository.session_ids()) == size + 1
        gc.collect()
        retained, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        return {'retained_bytes': retained, 'peak_bytes': peak,
                'resident_sessions': len(app._repository.session_ids())}
    finally:
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        app.close()


def main():
    if '--memory' in __import__('sys').argv:
        print(json.dumps({str(size): measure_graph_memory(size) for size in (32, 100)}, indent=2))
        return
    report = {}
    for kind, size in (('roots', 1), ('roots', 32), ('nodes', 32), ('children', 32), ('children', 100)):
        for delay in (0, .0005, .002, .01):
            rows = [sample(kind, size, delay) for _ in range(3)]
            report[f'{kind}_{size}_{delay}'] = {'median_ms': statistics.median(r['elapsed_ms'] for r in rows), 'samples': rows}
            print(f'{kind} {size} {delay} done', file=__import__('sys').stderr, flush=True)
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
