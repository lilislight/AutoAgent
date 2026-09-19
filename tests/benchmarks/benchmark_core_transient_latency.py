"""Registered small-data chains, isolated processes and repeated timed invocations."""
from __future__ import annotations
import argparse
import gc
import json
from pathlib import Path
import statistics
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--package-root', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.package_root:
        sys.path.insert(0, str(args.package_root.resolve()))
    from autoagent import AutoAgentApp, Edge, Node, Workflow
    from tests.benchmarks.benchmark_full_core import identity
    import autoagent
    rows = {}
    for count in (50, 100, 200, 400):
        workflow = Workflow(f'latency-{count}', nodes=[Node(f'n{i}', identity) for i in range(count)],
            edges=[Edge(f'n{i}', f'n{i+1}') for i in range(count-1)])
        samples = []
        for repeat in range(10):
            app = AutoAgentApp()
            app.register_workflow(workflow)
            gc.collect()
            start = time.perf_counter_ns()
            result = app.invoke(workflow.id, {'value': 1})
            elapsed = time.perf_counter_ns() - start
            assert result.status == 'completed' and result.output == {'value': 1}, result
            app.close()
            del app, result
            if repeat:
                samples.append(elapsed / 1e6)
        rows[str(count)] = {'median_ms': statistics.median(samples), 'samples_ms': samples}
    args.output.write_text(json.dumps({'package': autoagent.__file__, 'chains': rows}, indent=2))


if __name__ == '__main__':
    main()
