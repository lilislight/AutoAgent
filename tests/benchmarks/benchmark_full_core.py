"""Small repeatable default Core storage and latency benchmark."""

from __future__ import annotations

import json
import statistics
import time
import tracemalloc
from unittest.mock import patch
from typing_extensions import TypedDict

from autoagent import AutoAgentApp, Edge, InputMappingContext, Map, Node, Workflow
from autoagent.core.runtime import RuntimeEvent, RuntimeState, StateReducer


class Value(TypedDict):
    value: int


class Batch(TypedDict):
    items: list[Value]


def identity(value: Value) -> Value:
    return value


def items(context: InputMappingContext) -> list[Value]:
    return context.invocation_input["items"]  # type: ignore[index,return-value]


class RuntimeEventCollector:
    """Collect canonical Runtime Events through the hosting boundary."""

    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []

    async def append(self, event: RuntimeEvent) -> None:
        self.events.append(event)


def _measure(
    workflow: Workflow,
    value: object,
    **kwargs: object,
) -> tuple[dict[str, int], tuple[RuntimeEvent, ...]]:
    collector = RuntimeEventCollector()
    app = AutoAgentApp(
        max_operator_concurrency=32,
        runtime_event_sink=collector,
    )
    tracemalloc.start()
    started = time.perf_counter_ns()
    try:
        result = app.invoke(workflow, value, **kwargs)  # type: ignore[arg-type]
        duration = time.perf_counter_ns() - started
        _current, peak = tracemalloc.get_traced_memory()
        runtime_event_bytes = sum(
            len(json.dumps(event.to_record(), separators=(",", ":")))
            for event in collector.events
        )
        checkpoint = app.unload_session(result.ref)
        checkpoint_bytes = len(
            json.dumps(checkpoint.to_record(), separators=(",", ":"))
        )
        return {
            "duration_ns": duration,
            "peak_bytes": peak,
            "runtime_event_count": len(collector.events),
            "runtime_event_bytes": runtime_event_bytes,
            "checkpoint_bytes": checkpoint_bytes,
        }, tuple(collector.events)
    finally:
        tracemalloc.stop()
        app.close()


def _measure_replay_prefixes(
    events: tuple[RuntimeEvent, ...],
) -> dict[str, dict[str, int]]:
    """Measure replay scaling and report authoritative codec boundaries."""

    report: dict[str, dict[str, int]] = {}
    for size in (50, 100, 200, 400):
        prefix = events[:size]
        samples: list[int] = []
        decode_count = 0
        state_version = 0
        for _ in range(5):
            started = time.perf_counter_ns()
            with patch.object(
                RuntimeState,
                "from_record",
                wraps=RuntimeState.from_record,
            ) as decode:
                state = StateReducer().reduce(prefix)
            samples.append(time.perf_counter_ns() - started)
            decode_count = decode.call_count
            state_version = state.state_version
        report[str(size)] = {
            "median_duration_ns": int(statistics.median(samples)),
            "runtime_event_count": len(prefix),
            "state_version": state_version,
            "runtime_state_decode_count": decode_count,
        }
    return report


def _measure_live_chain_scaling() -> dict[str, dict[str, int]]:
    """Measure live transition scaling without mixing in Workflow compilation."""

    report: dict[str, dict[str, int]] = {}
    for node_count in (50, 100, 200, 400):
        workflow_id = f"benchmark-live-chain-{node_count}"
        workflow = Workflow(
            workflow_id,
            nodes=[Node(f"node-{index}", identity) for index in range(node_count)],
            edges=[
                Edge(f"node-{index}", f"node-{index + 1}")
                for index in range(node_count - 1)
            ],
        )
        samples: list[int] = []
        for _ in range(3):
            app = AutoAgentApp(max_operator_concurrency=32)
            app.register_workflow(workflow)
            started = time.perf_counter_ns()
            try:
                result = app.invoke(workflow_id, {"value": 1})
                samples.append(time.perf_counter_ns() - started)
            finally:
                app.close()
        report[str(node_count)] = {
            "median_duration_ns": int(statistics.median(samples)),
        }
    return report


def main() -> None:
    node_count = 100
    chain = Workflow(
        "benchmark-chain",
        nodes=[Node(f"node-{index}", identity) for index in range(node_count)],
        edges=[
            Edge(f"node-{index}", f"node-{index + 1}")
            for index in range(node_count - 1)
        ],
    )
    mapped = Workflow(
        "benchmark-map",
        nodes=[
            Node(
                "map",
                identity,
                input_mapping=items,
                map=Map(max_parallelism=32),
            )
        ],
    )
    chain_metrics, chain_events = _measure(chain, {"value": 1})
    map_metrics, _map_events = _measure(
        mapped,
        {"items": [{"value": index} for index in range(500)]},
    )
    context_metrics, _context_events = _measure(
        Workflow("benchmark-context", nodes=[Node("node", identity)]),
        {"value": 1},
        session_context={"large": "x" * 1_000_000},
    )
    report = {
        "chain_100": chain_metrics,
        "map_500": map_metrics,
        "large_context_small_output": context_metrics,
        "reducer_replay_event_prefixes": _measure_replay_prefixes(chain_events),
        "live_registered_chain_nodes": _measure_live_chain_scaling(),
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
