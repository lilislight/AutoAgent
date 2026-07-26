from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
import json
from pathlib import Path
from statistics import median
import tempfile
from time import perf_counter

from autoagent import AutoAgentApp, DatabaseBackend, RuntimeStore, Workflow


def _start_value() -> int:
    return 0


def _increment(value: int) -> int:
    return value + 1


def _build_chain(workflow_id: str, *, node_count: int) -> Workflow:
    workflow = Workflow(id=workflow_id)
    workflow.add_node(_start_value, node_id="node_0")
    for index in range(1, node_count):
        node_id = f"node_{index}"
        previous_id = f"node_{index - 1}"
        workflow.add_node(
            _increment,
            node_id=node_id,
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        workflow.add_edge(previous_id, node_id)
    return workflow


@dataclass(frozen=True)
class BenchmarkResult:
    backend: str
    event_mode: str
    node_count: int
    invocation_count: int
    median_invoke_ms: float
    p95_invoke_ms: float
    invocations_per_second: float
    flush_ms: float
    events_per_invocation: int


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * percentile))
    return ordered[index]


async def _run_case(
    *,
    backend_name: str,
    node_count: int,
    invocation_count: int,
    event_mode: str,
) -> BenchmarkResult:
    temporary_directory: tempfile.TemporaryDirectory[str] | None = None
    if backend_name == "memory":
        store = RuntimeStore()
    else:
        temporary_directory = tempfile.TemporaryDirectory()
        backend = DatabaseBackend.from_path(
            Path(temporary_directory.name) / "runtime.db",
        )
        store = RuntimeStore(backend=backend)

    app = AutoAgentApp(runtime_store=store)
    workflow = _build_chain(
        f"{backend_name}_{event_mode}_runtime_benchmark",
        node_count=node_count,
    )
    latencies: list[float] = []

    try:
        await app.astart()
        warmup = await app.ainvoke(
            workflow,
            session_id="warmup",
            event_mode=event_mode,
        )
        if warmup.state != "completed":
            raise RuntimeError(f"Warmup did not complete: {warmup.state}")
        await store.aflush()

        measured_started = perf_counter()
        last_invocation = None
        for index in range(invocation_count):
            started = perf_counter()
            last_invocation = await app.ainvoke(
                workflow,
                session_id=f"measured-{index}",
                event_mode=event_mode,
            )
            latencies.append(perf_counter() - started)
            if last_invocation.state != "completed":
                raise RuntimeError(
                    f"Invocation did not complete: {last_invocation.state}"
                )
        measured_elapsed = perf_counter() - measured_started

        flush_started = perf_counter()
        await store.aflush()
        flush_elapsed = perf_counter() - flush_started

        assert last_invocation is not None
        events = await store.alist_runtime_events(
            invocation_id=last_invocation.id,
            limit=100_000,
        )
    finally:
        await app.aclose()
        if temporary_directory is not None:
            temporary_directory.cleanup()

    return BenchmarkResult(
        backend=backend_name,
        event_mode=event_mode,
        node_count=node_count,
        invocation_count=invocation_count,
        median_invoke_ms=median(latencies) * 1_000,
        p95_invoke_ms=_percentile(latencies, 0.95) * 1_000,
        invocations_per_second=invocation_count / measured_elapsed,
        flush_ms=flush_elapsed * 1_000,
        events_per_invocation=len(events),
    )


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure AutoAgent RuntimeStore execution and persistence.",
    )
    parser.add_argument("--nodes", type=int, default=30)
    parser.add_argument("--invocations", type=int, default=10)
    parser.add_argument(
        "--backend",
        choices=("memory", "sqlite", "both"),
        default="both",
    )
    parser.add_argument(
        "--event-mode",
        choices=("minimal", "standard", "full", "all"),
        default="all",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON for storing benchmark history.",
    )
    args = parser.parse_args()
    if args.nodes < 1 or args.invocations < 1:
        parser.error("--nodes and --invocations must be positive")

    backends = ("memory", "sqlite") if args.backend == "both" else (args.backend,)
    event_modes = (
        ("minimal", "standard", "full")
        if args.event_mode == "all"
        else (args.event_mode,)
    )
    results = [
        await _run_case(
            backend_name=backend,
            node_count=args.nodes,
            invocation_count=args.invocations,
            event_mode=event_mode,
        )
        for backend in backends
        for event_mode in event_modes
    ]

    if args.json:
        print(json.dumps([asdict(result) for result in results], indent=2))
        return

    for result in results:
        print(
            f"{result.backend:>6}/{result.event_mode:<8}  "
            f"median={result.median_invoke_ms:8.2f} ms  "
            f"p95={result.p95_invoke_ms:8.2f} ms  "
            f"throughput={result.invocations_per_second:7.2f} inv/s  "
            f"flush={result.flush_ms:8.2f} ms  "
            f"events/invocation={result.events_per_invocation}"
        )


if __name__ == "__main__":
    asyncio.run(_main())
