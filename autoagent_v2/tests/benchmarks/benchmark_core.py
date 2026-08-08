"""Manual V2 Core microbenchmarks.

Run from ``autoagent_v2`` with::

    python -m tests.benchmarks.benchmark_core

The script intentionally has no pass/fail thresholds; CI correctness tests prove
the scheduling semantics, while this tool records comparable local timings.
"""

from __future__ import annotations

import time

from autoagent.core import AutoAgentApp, Edge, Node, Workflow, WorkflowCompiler


def identity(value: int) -> int:
    return value


def benchmark_ir_lookup(node_count: int = 2_000, lookups: int = 200_000) -> float:
    nodes = [Node(f"node_{index}", identity) for index in range(node_count)]
    edges = [
        Edge(f"node_{index}", f"node_{index + 1}")
        for index in range(node_count - 1)
    ]
    ir = WorkflowCompiler().compile(Workflow("lookup", nodes=nodes, edges=edges))
    started = time.perf_counter()
    for index in range(lookups):
        ir.node(f"node_{index % node_count}")
    return time.perf_counter() - started


def benchmark_invocations(count: int = 1_000) -> float:
    workflow = Workflow("invoke", nodes=[Node("node", identity)])
    app = AutoAgentApp()
    app.register_workflow(workflow)
    started = time.perf_counter()
    for value in range(count):
        app.invoke(workflow, value)
    elapsed = time.perf_counter() - started
    app.close()
    return elapsed


if __name__ == "__main__":
    lookup_seconds = benchmark_ir_lookup()
    invocation_seconds = benchmark_invocations()
    print(f"IR lookup: {lookup_seconds:.6f}s")
    print(f"1,000 invocations: {invocation_seconds:.6f}s")
