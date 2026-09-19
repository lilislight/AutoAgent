"""Lifecycle capture on/off: timed API calls exclude preparation and profiling."""
import gc
import json
import statistics
import time
import tracemalloc

from autoagent import AutoAgentApp, Node, Workflow
from tests.benchmarks.benchmark_core_audit import identity


def sample(operation, size, capture, allocations=False):
    app = AutoAgentApp()
    workflow = Workflow('checkpoint-cost', nodes=[Node('work', identity)])
    app.register_workflow(workflow)
    refs = [app.invoke(workflow.id, {'value': i}).ref for i in range(size)]
    calls = 0
    original = app._repository.capture_checkpoint
    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)
    app._repository.capture_checkpoint = counted
    try:
        if allocations:
            gc.collect()
            tracemalloc.start()
        start = time.perf_counter_ns()
        if operation == 'close':
            result = app.close(capture_checkpoint=capture)
            assert (result is not None) == capture
        else:
            result = [app.unload_session(ref, capture_checkpoint=capture) for ref in refs]
            assert all((item is not None) == capture for item in result)
        elapsed_ms = (time.perf_counter_ns() - start) / 1e6
        assert calls == (size if capture else 0)
        report = {'capture_calls': calls}
        if allocations:
            gc.collect()
            report['retained_bytes'], report['peak_bytes'] = tracemalloc.get_traced_memory()
            tracemalloc.stop()
        else:
            report['elapsed_ms'] = elapsed_ms
        return report
    finally:
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        app.close()


def main():
    report = {'method': '5 alternating enabled/disabled timing pairs; separate allocation run; preparation excluded'}
    for operation, size in [('close', 100), ('close', 1000), ('unload', 100)]:
        rows = {False: [], True: []}
        for trial in range(5):
            for capture in ((True, False) if trial % 2 == 0 else (False, True)):
                rows[capture].append(sample(operation, size, capture))
        report[f'{operation}_{size}'] = {
            str(capture): {'median_ms': statistics.median(row['elapsed_ms'] for row in rows[capture]),
                          'samples': rows[capture], 'allocations': sample(operation, size, capture, True)}
            for capture in (False, True)}
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
