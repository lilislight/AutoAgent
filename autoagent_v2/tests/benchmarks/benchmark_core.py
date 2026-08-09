"""Manual V2 Core microbenchmarks.

Run from ``autoagent_v2`` with::

    python -m tests.benchmarks.benchmark_core

The script intentionally has no pass/fail thresholds; CI correctness tests prove
the scheduling semantics, while this tool records comparable local timings.
"""

from __future__ import annotations

import time

from autoagent.core import (
    AutoAgentApp,
    Edge,
    Node,
    SerializedCheckpoint,
    SerializedEvent,
    Workflow,
    WorkflowCompiler,
)


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


class CountingSink:
    """Measure the immutable Core-to-Sink handoff without retaining records."""

    def __init__(self) -> None:
        self.event_count = 0
        self.event_bytes = 0
        self.checkpoint_count = 0
        self.checkpoint_bytes = 0

    async def wait_until_admissible(self) -> None:
        return None

    async def submit_events(self, events: tuple[SerializedEvent, ...]) -> None:
        self.event_count += len(events)
        self.event_bytes += sum(event.size_bytes for event in events)

    def offer_checkpoint(self, checkpoint: SerializedCheckpoint) -> None:
        self.checkpoint_count += 1
        self.checkpoint_bytes += checkpoint.size_bytes


def benchmark_invocations(
    count: int = 1_000,
    *,
    sink: CountingSink | None = None,
    event_mode: str = "standard",
) -> float:
    workflow = Workflow("invoke", nodes=[Node("node", identity)])
    app = AutoAgentApp(runtime_sink=sink)
    app.register_workflow(workflow)
    started = time.perf_counter()
    for value in range(count):
        app.invoke(workflow, value, event_mode=event_mode)
    elapsed = time.perf_counter() - started
    app.close()
    return elapsed


if __name__ == "__main__":
    lookup_seconds = benchmark_ir_lookup()
    core_only_seconds = benchmark_invocations()
    standard_sink = CountingSink()
    standard_seconds = benchmark_invocations(sink=standard_sink)
    full_sink = CountingSink()
    full_seconds = benchmark_invocations(sink=full_sink, event_mode="full")
    print(f"IR lookup: {lookup_seconds:.6f}s")
    print(f"1,000 Core-only invocations: {core_only_seconds:.6f}s")
    print(
        "1,000 Standard Sink invocations: "
        f"{standard_seconds:.6f}s, {standard_sink.event_count} events, "
        f"{standard_sink.event_bytes} event bytes, "
        f"{standard_sink.checkpoint_count} checkpoints, "
        f"{standard_sink.checkpoint_bytes} checkpoint bytes"
    )
    print(
        "1,000 Full Sink invocations: "
        f"{full_seconds:.6f}s, {full_sink.event_count} events, "
        f"{full_sink.event_bytes} event bytes, "
        f"{full_sink.checkpoint_count} checkpoints, "
        f"{full_sink.checkpoint_bytes} checkpoint bytes"
    )
