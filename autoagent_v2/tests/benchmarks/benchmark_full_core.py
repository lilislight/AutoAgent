"""Small repeatable Full-mode Core storage and latency benchmark."""

from __future__ import annotations

import json
import time
import tracemalloc
from typing_extensions import TypedDict

from autoagent import AutoAgentApp, Edge, InputMappingContext, Map, Node, Workflow


class Value(TypedDict):
    value: int


class Batch(TypedDict):
    items: list[Value]


def identity(value: Value) -> Value:
    return value


def items(context: InputMappingContext) -> list[Value]:
    return context.invocation_input["items"]  # type: ignore[index,return-value]


def _measure(workflow: Workflow, value: object, **kwargs: object) -> dict[str, int]:
    app = AutoAgentApp(max_operator_concurrency=32)
    tracemalloc.start()
    started = time.perf_counter_ns()
    try:
        result = app.invoke(workflow, value, **kwargs)  # type: ignore[arg-type]
        duration = time.perf_counter_ns() - started
        _current, peak = tracemalloc.get_traced_memory()
        event_bytes = sum(
            len(json.dumps(event.to_record(), separators=(",", ":")))
            for event in result.events
        )
        return {
            "duration_ns": duration,
            "peak_bytes": peak,
            "event_count": len(result.events),
            "event_bytes": event_bytes,
        }
    finally:
        tracemalloc.stop()
        app.close()


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
    report = {
        "chain_100": _measure(chain, {"value": 1}),
        "map_500": _measure(
            mapped,
            {"items": [{"value": index} for index in range(500)]},
        ),
        "large_context_small_output": _measure(
            Workflow("benchmark-context", nodes=[Node("node", identity)]),
            {"value": 1},
            session_context={"large": "x" * 1_000_000},
        ),
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
