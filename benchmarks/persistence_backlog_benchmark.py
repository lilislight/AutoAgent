from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence
import gc
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
from time import perf_counter
import tracemalloc
from typing import Any

from autoagent import AutoAgentApp, DatabaseBackend, RuntimeStore, Workflow
from autoagent.core.runtime.backends.database import _PersistenceItem


def _build_payload_chain(
    *,
    workflow_id: str,
    node_count: int,
    payload_bytes: int,
) -> Workflow:
    payload = "x" * payload_bytes if payload_bytes else 0

    def initial() -> str | int:
        return payload

    def passthrough(value: str | int) -> str | int:
        return value

    workflow = Workflow(id=workflow_id)
    workflow.add_node(initial, node_id="node_0")
    for index in range(1, node_count):
        node_id = f"node_{index}"
        previous_id = f"node_{index - 1}"
        workflow.add_node(
            passthrough,
            node_id=node_id,
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        workflow.add_edge(previous_id, node_id)
    return workflow


class _BlockedEventBackend(DatabaseBackend):
    def __init__(self, *args: Any, release_events: threading.Event, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.release_events = release_events

    async def _persist_batch(
        self,
        batch: Sequence[_PersistenceItem],
    ) -> None:
        if any(item.kind == "event" for item in batch):
            while not self.release_events.is_set():
                await asyncio.sleep(0.001)
        await super()._persist_batch(list(batch))


def _database_sizes(path: Path) -> dict[str, Any]:
    with sqlite3.connect(path) as database:
        event_count, event_bytes = database.execute(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(payload_json)), 0) "
            "FROM runtime_events"
        ).fetchone()
        invocation_count, genesis_bytes, recovery_count, recovery_bytes = (
            database.execute(
                "SELECT COUNT(*), "
                "COALESCE(SUM(LENGTH(genesis_state_json)), 0), "
                "COUNT(recovery_state_json), "
                "COALESCE(SUM(LENGTH(recovery_state_json)), 0) "
                "FROM invocations"
            ).fetchone()
        )
        artifact_count, artifact_bytes = database.execute(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(payload_blob)), 0) "
            "FROM artifacts"
        ).fetchone()
        page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
        event_bytes_by_type = {
            str(event_type): {
                "count": int(count),
                "json_bytes": int(json_bytes),
            }
            for event_type, count, json_bytes in database.execute(
                "SELECT type, COUNT(*), "
                "COALESCE(SUM(LENGTH(payload_json)), 0) "
                "FROM runtime_events GROUP BY type ORDER BY type"
            ).fetchall()
        }
    wal_path = path.with_suffix(".db-wal")
    shm_path = path.with_suffix(".db-shm")
    wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
    shm_bytes = shm_path.stat().st_size if shm_path.exists() else 0
    return {
        "database_event_count": int(event_count),
        "database_event_json_bytes": int(event_bytes),
        "database_invocation_count": int(invocation_count),
        "database_genesis_json_bytes": int(genesis_bytes),
        "database_recovery_state_count": int(recovery_count),
        "database_recovery_state_json_bytes": int(recovery_bytes),
        "database_artifact_count": int(artifact_count),
        "database_artifact_payload_bytes": int(artifact_bytes),
        "database_page_bytes": page_size * page_count,
        "database_file_bytes": path.stat().st_size,
        "database_wal_bytes": wal_bytes,
        "database_shm_bytes": shm_bytes,
        "database_event_json_bytes_by_type": event_bytes_by_type,
    }


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    release_events = threading.Event()
    if args.database_mode == "normal":
        release_events.set()
    with tempfile.TemporaryDirectory() as directory:
        database_path = Path(directory) / "runtime.db"
        backend = _BlockedEventBackend.from_path(
            database_path,
            release_events=release_events,
            queue_low_watermark_bytes=args.queue_high_bytes // 2,
            queue_high_watermark_bytes=args.queue_high_bytes,
            queue_hard_watermark_bytes=args.queue_hard_bytes,
            batch_max_delay_ms=0,
            recovery_event_interval=args.recovery_event_interval,
            queue_admission_timeout_ms=100,
        )
        store = RuntimeStore(backend=backend)
        app = AutoAgentApp(runtime_store=store)
        workflow = _build_payload_chain(
            workflow_id="persistence_backlog_benchmark",
            node_count=args.nodes,
            payload_bytes=args.payload_bytes,
        )

        try:
            await app.astart()
            admitted = [
                await app._aadmit_invocation(
                    workflow,
                    session_id=f"session-{index}",
                )
                for index in range(args.invocations)
            ]

            gc.collect()
            tracemalloc.start()
            heap_before = tracemalloc.get_traced_memory()[0]
            started = perf_counter()
            execution = asyncio.gather(
                *(
                    app._aexecute_admitted(item, input=None)
                    for item in admitted
                ),
            )
            peak_queue_count = 0
            peak_queue_bytes = 0
            high_crossed_at: float | None = None
            hard_crossed_at: float | None = None
            while not execution.done():
                elapsed = perf_counter() - started
                peak_queue_count = max(
                    peak_queue_count,
                    store.pending_persistence_count,
                )
                peak_queue_bytes = max(
                    peak_queue_bytes,
                    store.pending_persistence_bytes,
                )
                if (
                    high_crossed_at is None
                    and store.pending_persistence_bytes
                    >= backend.queue_high_watermark_bytes
                ):
                    high_crossed_at = elapsed
                if (
                    hard_crossed_at is None
                    and store.pending_persistence_bytes
                    >= backend.queue_hard_watermark_bytes
                ):
                    hard_crossed_at = elapsed
                await asyncio.sleep(0.001)
            invocations = await execution
            production_elapsed = perf_counter() - started
            heap_at_backlog, peak_heap = tracemalloc.get_traced_memory()
            peak_queue_count = max(
                peak_queue_count,
                store.pending_persistence_count,
            )
            peak_queue_bytes = max(
                peak_queue_bytes,
                store.pending_persistence_bytes,
            )
            if (
                high_crossed_at is None
                and store.pending_persistence_bytes
                >= backend.queue_high_watermark_bytes
            ):
                high_crossed_at = production_elapsed
            if (
                hard_crossed_at is None
                and store.pending_persistence_bytes
                >= backend.queue_hard_watermark_bytes
            ):
                hard_crossed_at = production_elapsed

            event_count = sum(
                len(store.runtime_events[invocation.id])
                for invocation in invocations
            )
            queue_count = store.pending_persistence_count
            queue_bytes = store.pending_persistence_bytes
            admission_paused = store.admission_paused
            rejection: str | None = None
            if admission_paused:
                try:
                    await app.ainvoke(
                        workflow,
                        session_id="backpressure-probe",
                    )
                except (RuntimeError, TimeoutError) as exc:
                    rejection = str(exc)

            release_events.set()
            flush_started = perf_counter()
            await store.aflush()
            flush_elapsed = perf_counter() - flush_started
            gc.collect()
            heap_after_flush = tracemalloc.get_traced_memory()[0]
            tracemalloc.stop()
        finally:
            release_events.set()
            await app.aclose()

        database_sizes = _database_sizes(database_path)

    return {
        "configuration": {
            "database_mode": args.database_mode,
            "invocations": args.invocations,
            "nodes_per_invocation": args.nodes,
            "payload_bytes_per_node_output": args.payload_bytes,
            "recovery_event_interval": args.recovery_event_interval,
            "queue_low_watermark_bytes": args.queue_high_bytes // 2,
            "queue_high_watermark_bytes": args.queue_high_bytes,
            "queue_hard_watermark_bytes": args.queue_hard_bytes,
        },
        "execution_backlog": {
            "boundary_event_count": event_count,
            "peak_persistence_item_count": peak_queue_count,
            "peak_accounted_queue_bytes": peak_queue_bytes,
            "persistence_item_count_after_execution": queue_count,
            "accounted_queue_bytes_after_execution": queue_bytes,
            "average_accounted_bytes_per_item": (
                peak_queue_bytes / peak_queue_count
                if peak_queue_count
                else 0
            ),
            "production_elapsed_seconds": production_elapsed,
            "peak_accounted_queue_bytes_per_second": (
                peak_queue_bytes / production_elapsed
            ),
            "high_watermark_crossed_at_seconds": high_crossed_at,
            "hard_watermark_crossed_at_seconds": hard_crossed_at,
            "traced_heap_delta_bytes": heap_at_backlog - heap_before,
            "traced_peak_heap_delta_bytes": peak_heap - heap_before,
            "admission_paused": admission_paused,
            "new_admission_rejected": rejection is not None,
            "new_admission_error": rejection,
        },
        "after_release": {
            "flush_seconds": flush_elapsed,
            "pending_persistence_count": store.pending_persistence_count,
            "pending_persistence_bytes": store.pending_persistence_bytes,
            "traced_heap_delta_after_flush_bytes": heap_after_flush - heap_before,
        },
        "database": database_sizes,
    }


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Measure persistence backlog growth while SQLite event writes are "
            "blocked, then measure the durable JSON and database file sizes."
        )
    )
    parser.add_argument("--invocations", type=int, default=20)
    parser.add_argument("--nodes", type=int, default=10)
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument(
        "--recovery-event-interval",
        type=int,
        default=200,
    )
    parser.add_argument(
        "--database-mode",
        choices=("blocked", "normal"),
        default="blocked",
    )
    parser.add_argument(
        "--queue-high-bytes",
        type=int,
        default=1024 * 1024,
    )
    parser.add_argument(
        "--queue-hard-bytes",
        type=int,
        default=256 * 1024 * 1024,
    )
    args = parser.parse_args()
    if (
        args.invocations < 1
        or args.nodes < 1
        or args.payload_bytes < 0
        or args.recovery_event_interval < 1
        or args.queue_high_bytes < 2
        or args.queue_hard_bytes <= args.queue_high_bytes
    ):
        parser.error("Invalid positive counts or queue watermark ordering.")

    print(json.dumps(await _run(args), indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
