from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
import json
from statistics import median
from time import perf_counter, process_time
from typing import Any

from autoagent.core.runtime import RuntimeConcurrencyController
from autoagent.core.runtime.hooks import RuntimeEventLoop


@dataclass(frozen=True)
class CaseResult:
    case: str
    repeats: int
    median_wall_ms: float
    median_cpu_ms: float
    median_cpu_percent: float
    p95_latency_ms: float | None = None


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * percentile))
    return ordered[index]


async def _runtime_operation_case(
    *,
    repeats: int,
    duration_seconds: float,
) -> CaseResult:
    runtime_loop = RuntimeEventLoop(name="polling-benchmark-runtime")
    wall_values: list[float] = []
    cpu_values: list[float] = []
    try:
        for _ in range(repeats):
            wall_started = perf_counter()
            cpu_started = process_time()
            await runtime_loop.arun(asyncio.sleep(duration_seconds))
            cpu_values.append(process_time() - cpu_started)
            wall_values.append(perf_counter() - wall_started)
    finally:
        runtime_loop.stop()
    return _case_result(
        "long_runtime_operation",
        wall_values=wall_values,
        cpu_values=cpu_values,
    )


async def _contention_case(
    *,
    repeats: int,
    waiters: int,
    hold_seconds: float,
) -> CaseResult:
    wall_values: list[float] = []
    cpu_values: list[float] = []
    for _ in range(repeats):
        controller = RuntimeConcurrencyController()

        async def use_slot() -> None:
            async with controller.async_slot("shared", 1):
                await asyncio.sleep(hold_seconds)

        wall_started = perf_counter()
        cpu_started = process_time()
        await asyncio.gather(*(use_slot() for _ in range(waiters)))
        cpu_values.append(process_time() - cpu_started)
        wall_values.append(perf_counter() - wall_started)
    return _case_result(
        "concurrency_slot_contention",
        wall_values=wall_values,
        cpu_values=cpu_values,
    )


async def _dispatch_case(
    *,
    repeats: int,
    dispatches: int,
) -> CaseResult:
    runtime_loop = RuntimeEventLoop(name="dispatch-benchmark-runtime")
    wall_values: list[float] = []
    cpu_values: list[float] = []
    latencies: list[float] = []

    async def no_op() -> None:
        return None

    try:
        for _ in range(repeats):
            wall_started = perf_counter()
            cpu_started = process_time()
            for _ in range(dispatches):
                started = perf_counter()
                await runtime_loop.arun(no_op())
                latencies.append(perf_counter() - started)
            cpu_values.append(process_time() - cpu_started)
            wall_values.append(perf_counter() - wall_started)
    finally:
        runtime_loop.stop()
    return _case_result(
        "cross_thread_dispatch",
        wall_values=wall_values,
        cpu_values=cpu_values,
        p95_latency_ms=_percentile(latencies, 0.95) * 1_000,
    )


def _case_result(
    case: str,
    *,
    wall_values: list[float],
    cpu_values: list[float],
    p95_latency_ms: float | None = None,
) -> CaseResult:
    median_wall = median(wall_values)
    median_cpu = median(cpu_values)
    return CaseResult(
        case=case,
        repeats=len(wall_values),
        median_wall_ms=median_wall * 1_000,
        median_cpu_ms=median_cpu * 1_000,
        median_cpu_percent=(
            0.0 if median_wall == 0 else median_cpu / median_wall * 100
        ),
        p95_latency_ms=p95_latency_ms,
    )


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark Runtime loop polling and concurrency waiters.",
    )
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--operation-ms", type=float, default=1_000)
    parser.add_argument("--waiters", type=int, default=250)
    parser.add_argument("--hold-ms", type=float, default=1)
    parser.add_argument("--dispatches", type=int, default=500)
    parser.add_argument("--json", action="store_true")
    arguments = parser.parse_args()
    if (
        arguments.repeats < 1
        or arguments.operation_ms <= 0
        or arguments.waiters < 1
        or arguments.hold_ms < 0
        or arguments.dispatches < 1
    ):
        parser.error("Benchmark counts must be positive and hold-ms non-negative.")

    results = (
        await _runtime_operation_case(
            repeats=arguments.repeats,
            duration_seconds=arguments.operation_ms / 1_000,
        ),
        await _contention_case(
            repeats=arguments.repeats,
            waiters=arguments.waiters,
            hold_seconds=arguments.hold_ms / 1_000,
        ),
        await _dispatch_case(
            repeats=arguments.repeats,
            dispatches=arguments.dispatches,
        ),
    )
    payload: dict[str, Any] = {
        "config": {
            "repeats": arguments.repeats,
            "operation_ms": arguments.operation_ms,
            "waiters": arguments.waiters,
            "hold_ms": arguments.hold_ms,
            "dispatches": arguments.dispatches,
        },
        "results": [asdict(result) for result in results],
    }
    if arguments.json:
        print(json.dumps(payload, indent=2))
        return
    for result in results:
        latency = (
            ""
            if result.p95_latency_ms is None
            else f" p95_latency={result.p95_latency_ms:.3f} ms"
        )
        print(
            f"{result.case}: wall={result.median_wall_ms:.3f} ms "
            f"cpu={result.median_cpu_ms:.3f} ms "
            f"cpu_percent={result.median_cpu_percent:.2f}%{latency}"
        )


if __name__ == "__main__":
    asyncio.run(_main())
