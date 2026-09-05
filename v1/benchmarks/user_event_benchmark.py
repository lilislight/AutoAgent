from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import platform
import sqlite3
from statistics import median
import tempfile
from time import perf_counter
from typing import Any

from autoagent import (
    AutoAgentApp,
    DatabaseBackend,
    RuntimeStore,
    StreamingResult,
    UserEventMapping,
    Workflow,
    streaming_result,
)


_CASES = ("none", "completed", "stream", "stream_and_completed")


@dataclass
class _ChunkSummaryReducer:
    chunk_count: int = 0
    byte_count: int = 0

    def add(self, chunk: str) -> None:
        self.chunk_count += 1
        self.byte_count += len(chunk.encode("utf-8"))

    def finish(self) -> dict[str, int]:
        return {
            "chunk_count": self.chunk_count,
            "byte_count": self.byte_count,
        }


@dataclass(frozen=True)
class TrialResult:
    case: str
    repeat: int
    concurrency: int
    invocation_count: int
    chunk_count: int
    chunk_payload_bytes: int
    elapsed_seconds: float
    invocation_latencies_ms: tuple[float, ...]
    user_events_per_invocation: int
    user_event_json_bytes_per_invocation: int
    runtime_events_per_invocation: int
    pending_persistence_count: int
    pending_persistence_bytes: int


@dataclass(frozen=True)
class BenchmarkResult:
    case: str
    repeats: int
    concurrency: int
    invocation_count_per_repeat: int
    chunk_count: int
    chunk_payload_bytes: int
    median_invoke_ms: float
    p95_invoke_ms: float
    invocations_per_second: float
    user_events_per_invocation: int
    user_event_json_bytes_per_invocation: int
    runtime_events_per_invocation: int
    pending_persistence_count: int
    pending_persistence_bytes: int
    latency_change_vs_none_percent: float
    throughput_change_vs_none_percent: float


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int((len(ordered) - 1) * percentile))
    return ordered[index]


def _user_event_json_bytes(events: tuple[Any, ...]) -> int:
    if not events:
        return 0
    document = [event.model_dump(mode="json") for event in events]
    return len(
        json.dumps(
            document,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )


def _build_workflow(
    *,
    workflow_id: str,
    case: str,
    chunk_count: int,
    chunk_payload_bytes: int,
) -> Workflow:
    chunk = "x" * chunk_payload_bytes

    def stream() -> StreamingResult[str, dict[str, int]]:
        return streaming_result(
            (chunk for _ in range(chunk_count)),
            reducer=_ChunkSummaryReducer(),
        )

    stream_mapping = (
        UserEventMapping(
            type="message_delta",
            transform=lambda value: {"delta": value},
        )
        if case in {"stream", "stream_and_completed"}
        else None
    )
    completed_mapping = (
        UserEventMapping(
            type="message_completed",
            transform=lambda value: value,
        )
        if case in {"completed", "stream_and_completed"}
        else None
    )

    workflow = Workflow(id=workflow_id)
    workflow.add_node(
        stream,
        node_id="stream",
        stream_user_event_mapping=stream_mapping,
        user_event_mapping=completed_mapping,
    )
    return workflow


async def _run_trial(
    *,
    case: str,
    repeat: int,
    invocation_count: int,
    concurrency: int,
    chunk_count: int,
    chunk_payload_bytes: int,
    warmup_count: int,
) -> TrialResult:
    store = RuntimeStore()
    app = AutoAgentApp(runtime_store=store)
    workflow = _build_workflow(
        workflow_id=f"user_event_{case}_{repeat}",
        case=case,
        chunk_count=chunk_count,
        chunk_payload_bytes=chunk_payload_bytes,
    )
    app.register_workflow(workflow)

    try:
        await app.astart()
        for index in range(warmup_count):
            invocation = await app.ainvoke(
                workflow,
                session_id=f"{case}-{repeat}-warmup-{index}",
                event_mode="minimal",
            )
            if invocation.state != "completed":
                raise RuntimeError(
                    f"{case} warmup did not complete: {invocation.state}"
                )

        semaphore = asyncio.Semaphore(concurrency)
        latencies_ms: list[float] = []
        invocations: list[Any] = []

        async def invoke_one(index: int) -> None:
            async with semaphore:
                started = perf_counter()
                invocation = await app.ainvoke(
                    workflow,
                    session_id=f"{case}-{repeat}-measured-{index}",
                    event_mode="minimal",
                )
                latencies_ms.append((perf_counter() - started) * 1_000)
                if invocation.state != "completed":
                    raise RuntimeError(
                        f"{case} Invocation did not complete: "
                        f"{invocation.state}"
                    )
                invocations.append(invocation)

        measured_started = perf_counter()
        await asyncio.gather(
            *(invoke_one(index) for index in range(invocation_count))
        )
        measured_elapsed = perf_counter() - measured_started

        sample_invocation = invocations[-1]
        user_events = store.list_user_events(
            invocation_id=sample_invocation.id,
            limit=chunk_count + 10,
        )
        runtime_events = await store.alist_runtime_events(
            invocation_id=sample_invocation.id,
            limit=100_000,
        )

        return TrialResult(
            case=case,
            repeat=repeat,
            concurrency=concurrency,
            invocation_count=invocation_count,
            chunk_count=chunk_count,
            chunk_payload_bytes=chunk_payload_bytes,
            elapsed_seconds=measured_elapsed,
            invocation_latencies_ms=tuple(latencies_ms),
            user_events_per_invocation=len(user_events),
            user_event_json_bytes_per_invocation=_user_event_json_bytes(
                user_events
            ),
            runtime_events_per_invocation=len(runtime_events),
            pending_persistence_count=store.pending_persistence_count,
            pending_persistence_bytes=store.pending_persistence_bytes,
        )
    finally:
        await app.aclose()


def _aggregate(
    trials: list[TrialResult],
) -> list[BenchmarkResult]:
    by_case = {
        case: [trial for trial in trials if trial.case == case]
        for case in _CASES
    }
    baseline_trials = by_case["none"]
    baseline_latencies = [
        latency
        for trial in baseline_trials
        for latency in trial.invocation_latencies_ms
    ]
    baseline_median = median(baseline_latencies)
    baseline_throughput = sum(
        trial.invocation_count for trial in baseline_trials
    ) / sum(trial.elapsed_seconds for trial in baseline_trials)

    results: list[BenchmarkResult] = []
    for case in _CASES:
        case_trials = by_case[case]
        latencies = [
            latency
            for trial in case_trials
            for latency in trial.invocation_latencies_ms
        ]
        case_median = median(latencies)
        case_throughput = sum(
            trial.invocation_count for trial in case_trials
        ) / sum(trial.elapsed_seconds for trial in case_trials)
        sample = case_trials[-1]
        results.append(
            BenchmarkResult(
                case=case,
                repeats=len(case_trials),
                concurrency=sample.concurrency,
                invocation_count_per_repeat=sample.invocation_count,
                chunk_count=sample.chunk_count,
                chunk_payload_bytes=sample.chunk_payload_bytes,
                median_invoke_ms=case_median,
                p95_invoke_ms=_percentile(latencies, 0.95),
                invocations_per_second=case_throughput,
                user_events_per_invocation=sample.user_events_per_invocation,
                user_event_json_bytes_per_invocation=(
                    sample.user_event_json_bytes_per_invocation
                ),
                runtime_events_per_invocation=(
                    sample.runtime_events_per_invocation
                ),
                pending_persistence_count=sample.pending_persistence_count,
                pending_persistence_bytes=sample.pending_persistence_bytes,
                latency_change_vs_none_percent=(
                    (case_median / baseline_median) - 1
                )
                * 100,
                throughput_change_vs_none_percent=(
                    (case_throughput / baseline_throughput) - 1
                )
                * 100,
            )
        )
    return results


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    trials: list[TrialResult] = []
    for repeat in range(args.repeats):
        # Rotate the order so warm caches or host drift do not always favor the
        # same case while preserving deterministic runs.
        offset = repeat % len(_CASES)
        ordered_cases = _CASES[offset:] + _CASES[:offset]
        for case in ordered_cases:
            trials.append(
                await _run_trial(
                    case=case,
                    repeat=repeat,
                    invocation_count=args.invocations,
                    concurrency=args.concurrency,
                    chunk_count=args.chunks,
                    chunk_payload_bytes=args.chunk_bytes,
                    warmup_count=args.warmups,
                )
            )

    results = _aggregate(trials)
    persistence_verification = await _verify_persistence_boundary(
        chunk_count=args.chunks,
        chunk_payload_bytes=args.chunk_bytes,
    )
    return {
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "processor": platform.processor(),
        },
        "configuration": {
            "repeats": args.repeats,
            "warmups_per_repeat": args.warmups,
            "invocations_per_repeat": args.invocations,
            "concurrency": args.concurrency,
            "chunks_per_invocation": args.chunks,
            "chunk_payload_bytes": args.chunk_bytes,
            "event_mode": "minimal",
            "backend": "memory",
        },
        "results": [asdict(result) for result in results],
        "persistence_verification": persistence_verification,
    }


async def _verify_persistence_boundary(
    *,
    chunk_count: int,
    chunk_payload_bytes: int,
) -> dict[str, Any]:
    """Prove that UserEvents do not create durable rows in the current design."""

    with tempfile.TemporaryDirectory() as directory:
        database_path = Path(directory) / "runtime.db"
        store = RuntimeStore(
            backend=DatabaseBackend.from_path(
                database_path,
                batch_max_delay_ms=0,
            )
        )
        app = AutoAgentApp(runtime_store=store)
        workflow = _build_workflow(
            workflow_id="user_event_persistence_boundary",
            case="stream_and_completed",
            chunk_count=chunk_count,
            chunk_payload_bytes=chunk_payload_bytes,
        )
        app.register_workflow(workflow)
        try:
            await app.astart()
            invocation = await app.ainvoke(
                workflow,
                session_id="persistence-boundary",
                event_mode="minimal",
            )
            user_event_count_before_close = len(
                store.list_user_events(invocation_id=invocation.id, limit=100_000)
            )
            await store.aflush()
        finally:
            await app.aclose()

        with sqlite3.connect(database_path) as database:
            tables = {
                str(row[0])
                for row in database.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            invocation_rows = int(
                database.execute("SELECT COUNT(*) FROM invocations").fetchone()[0]
            )
            runtime_event_rows = int(
                database.execute(
                    "SELECT COUNT(*) FROM runtime_events"
                ).fetchone()[0]
            )

    return {
        "user_events_in_memory_before_close": user_event_count_before_close,
        "invocation_rows": invocation_rows,
        "runtime_event_rows": runtime_event_rows,
        "user_event_table_exists": "user_events" in tables,
        "pending_persistence_count_after_flush": store.pending_persistence_count,
        "pending_persistence_bytes_after_flush": store.pending_persistence_bytes,
    }


def _print_text(document: dict[str, Any]) -> None:
    configuration = document["configuration"]
    print(
        "UserEvent benchmark: "
        f"{configuration['repeats']} repeats x "
        f"{configuration['invocations_per_repeat']} Invocations, "
        f"concurrency={configuration['concurrency']}, "
        f"{configuration['chunks_per_invocation']} chunks x "
        f"{configuration['chunk_payload_bytes']} B"
    )
    for result in document["results"]:
        print(
            f"{result['case']:>20}  "
            f"median={result['median_invoke_ms']:8.3f} ms  "
            f"p95={result['p95_invoke_ms']:8.3f} ms  "
            f"throughput={result['invocations_per_second']:8.2f} inv/s  "
            f"events/inv={result['user_events_per_invocation']:4d}  "
            f"event-json/inv="
            f"{result['user_event_json_bytes_per_invocation']:7d} B  "
            f"latency-vs-none="
            f"{result['latency_change_vs_none_percent']:+7.2f}%"
        )
    persistence = document["persistence_verification"]
    print(
        "persistence boundary: "
        f"{persistence['user_events_in_memory_before_close']} UserEvents in "
        "memory, "
        f"{persistence['runtime_event_rows']} RuntimeEvent rows, "
        f"user_events table={persistence['user_event_table_exists']}, "
        f"pending={persistence['pending_persistence_count_after_flush']} items/"
        f"{persistence['pending_persistence_bytes_after_flush']} B"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compare identical streaming Workflow execution with no UserEvent, "
            "one completion Event, per-chunk Events, and both Event forms."
        )
    )
    parser.add_argument("--invocations", type=int, default=100)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=5)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--chunks", type=int, default=64)
    parser.add_argument("--chunk-bytes", type=int, default=32)
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of the compact table.",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Also write the complete JSON result to this path.",
    )
    args = parser.parse_args()
    for name in (
        "invocations",
        "repeats",
        "warmups",
        "concurrency",
        "chunks",
        "chunk_bytes",
    ):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be positive")

    document = asyncio.run(_run(args))
    encoded = json.dumps(document, ensure_ascii=False, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(encoded + "\n", encoding="utf-8")
    if args.json:
        print(encoded)
    else:
        _print_text(document)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
