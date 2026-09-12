"""Isolate cold derived-index allocations from the already resident revisions."""
import gc
import json
import statistics
import time
import tracemalloc

from autoagent.core.runtime._context_index import ContextRevisionIndex


def measure(revisions):
    samples = []
    for _ in range(5):
        start = time.perf_counter_ns()
        index = ContextRevisionIndex(revisions)
        samples.append((time.perf_counter_ns() - start) / 1e6)
        del index
    gc.collect()
    tracemalloc.start()
    try:
        index = ContextRevisionIndex(revisions)
        retained, peak = tracemalloc.get_traced_memory()
        return {'median_build_ms': statistics.median(samples), 'samples_ms': samples,
                'retained_bytes': retained, 'peak_bytes': peak,
                'strict_ancestor_entries': len(index.descendants)}
    finally:
        tracemalloc.stop()


def main():
    report = {}
    for count in (1000, 50000):
        report[f'flat_{count}'] = measure({(str(i),): 1 for i in range(count)})
        report[f'shared_parent_{count}'] = measure({('root', str(i)): 1 for i in range(count)})
        report[f'unique_parent_{count}'] = measure({(str(i), 'leaf'): 1 for i in range(count)})
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
