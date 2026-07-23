from __future__ import annotations

import asyncio
from collections.abc import Sequence
import os
from pathlib import Path
import threading
from time import perf_counter
import tempfile
import unittest

from sqlalchemy import text

from autoagent import AutoAgentApp, DatabaseBackend, RuntimeStore, Workflow
from autoagent.core.runtime.backends.database import _PersistenceItem


MEMORY_CHAIN_MAX_SECONDS = float(
    os.getenv("AUTOAGENT_PERF_MEMORY_CHAIN_MAX_SECONDS", "5")
)
SQLITE_CHAIN_MAX_SECONDS = float(
    os.getenv("AUTOAGENT_PERF_SQLITE_CHAIN_MAX_SECONDS", "10")
)


def _start_value() -> int:
    return 0


def _increment(value: int) -> int:
    return value + 1


def _large_output() -> str:
    return "x" * 100_000


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


class RuntimePerformanceRegressionTests(unittest.TestCase):
    """Low-variance guards for the most important runtime cost boundaries.

    These are intentionally smoke budgets rather than microbenchmarks. Exact
    latency and throughput measurements live in ``benchmarks/`` so normal CI
    catches hangs or order-of-magnitude regressions without depending on one
    machine's timing.
    """

    def test_memory_runtime_completes_thirty_node_chain_within_smoke_budget(
        self,
    ) -> None:
        app = AutoAgentApp()
        workflow = _build_chain("memory_performance_chain", node_count=30)

        try:
            started = perf_counter()
            invocation = app.invoke(workflow, session_id="performance")
            elapsed = perf_counter() - started
        finally:
            app.close()

        self.assertEqual("completed", invocation.state)
        self.assertLess(
            elapsed,
            MEMORY_CHAIN_MAX_SECONDS,
            "Memory runtime exceeded its smoke budget. Run "
            "`uv run python -m benchmarks.runtime_store_benchmark` to profile.",
        )

    def test_large_output_is_not_repeated_by_later_node_boundaries(self) -> None:
        store = RuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        workflow = Workflow(id="event_payload_performance")
        workflow.add_node(_large_output, node_id="large")

        try:
            invocation = app.invoke(workflow, session_id="performance")
            events = asyncio.run(
                store.alist_runtime_events(
                    invocation_id=invocation.id,
                    limit=100,
                )
            )
        finally:
            app.close()

        encoded_sizes = {
            event.boundary: len(
                store.serializer.dumps(event.model_dump(mode="python"))
            )
            for event in events
        }
        output_ready_size = encoded_sizes["node.output_ready"]
        self.assertGreater(output_ready_size, 100_000)
        self.assertLess(
            encoded_sizes["node.committed"],
            output_ready_size // 10,
        )
        self.assertLess(
            encoded_sizes["routing.committed"],
            output_ready_size // 10,
        )


class DatabasePerformanceRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_sqlite_runtime_returns_and_flushes_thirty_node_chain_within_budget(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            backend = DatabaseBackend.from_path(
                Path(directory) / "runtime.db",
                batch_max_delay_ms=0,
            )
            store = RuntimeStore(backend=backend)
            app = AutoAgentApp(runtime_store=store)
            workflow = _build_chain("sqlite_performance_chain", node_count=30)

            try:
                await app.astart()
                started = perf_counter()
                invocation = await app.ainvoke(
                    workflow,
                    session_id="performance",
                )
                invoke_elapsed = perf_counter() - started

                flush_started = perf_counter()
                await store.aflush()
                flush_elapsed = perf_counter() - flush_started
            finally:
                await app.aclose()

        self.assertEqual("completed", invocation.state)
        self.assertLess(
            invoke_elapsed,
            SQLITE_CHAIN_MAX_SECONDS,
            "SQLite-backed runtime exceeded its invoke smoke budget. Run "
            "`uv run python -m benchmarks.runtime_store_benchmark` to profile.",
        )
        self.assertLess(
            flush_elapsed,
            SQLITE_CHAIN_MAX_SECONDS,
            "SQLite persistence exceeded its flush smoke budget. Run "
            "`uv run python -m benchmarks.runtime_store_benchmark` to profile.",
        )

    async def test_concurrent_invocation_backlog_is_measured_and_blocks_admission(
        self,
    ) -> None:
        release_events = threading.Event()

        class BlockedEventBackend(DatabaseBackend):
            async def _persist_batch(
                self,
                batch: Sequence[_PersistenceItem],
            ) -> None:
                if any(item.kind == "event" for item in batch):
                    while not release_events.is_set():
                        await asyncio.sleep(0.001)
                await super()._persist_batch(list(batch))

        with tempfile.TemporaryDirectory() as directory:
            backend = BlockedEventBackend.from_path(
                Path(directory) / "runtime.db",
                queue_low_watermark_bytes=8 * 1024,
                queue_high_watermark_bytes=16 * 1024,
                queue_hard_watermark_bytes=16 * 1024 * 1024,
                batch_max_delay_ms=0,
                recovery_event_interval=1_000,
            )
            store = RuntimeStore(backend=backend)
            app = AutoAgentApp(runtime_store=store)
            workflow = _build_chain("concurrent_queue_backlog", node_count=5)

            try:
                await app.astart()
                admitted = [
                    await app._aadmit_invocation(
                        workflow,
                        session_id=f"session-{index}",
                    )
                    for index in range(8)
                ]
                invocations = await asyncio.gather(
                    *(
                        app._aexecute_admitted(item, input=None)
                        for item in admitted
                    )
                )

                boundary_event_count = sum(
                    len(store.runtime_events[invocation.id])
                    for invocation in invocations
                )
                self.assertEqual(
                    boundary_event_count,
                    store.pending_persistence_count,
                )
                self.assertGreaterEqual(
                    store.pending_persistence_bytes,
                    backend.queue_high_watermark_bytes,
                )
                self.assertTrue(store.admission_paused)

                with self.assertRaisesRegex(RuntimeError, "backlog"):
                    await app.ainvoke(
                        workflow,
                        session_id="rejected-by-backpressure",
                    )

                release_events.set()
                await store.aflush()
                self.assertEqual(0, store.pending_persistence_count)
                self.assertEqual(0, store.pending_persistence_bytes)
                self.assertFalse(store.admission_paused)
            finally:
                release_events.set()
                await app.aclose()

    async def test_queue_byte_accounting_matches_persisted_json_payloads(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            backend = DatabaseBackend.from_path(
                Path(directory) / "runtime.db",
                batch_max_delay_ms=0,
                recovery_event_interval=10,
            )
            store = RuntimeStore(backend=backend)
            app = AutoAgentApp(runtime_store=store)
            workflow = _build_chain("persisted_payload_size", node_count=5)

            try:
                invocations = [
                    await app.ainvoke(
                        workflow,
                        session_id=f"session-{index}",
                    )
                    for index in range(4)
                ]
                await store.aflush()

                expected_event_bytes = sum(
                    len(store.serializer.dumps(event.payload))
                    for invocation in invocations
                    for event in store.runtime_events[invocation.id]
                )
                async def persisted_sizes() -> tuple[int, int, int]:
                    async with backend.engine.connect() as connection:
                        event_bytes = await connection.scalar(
                            text(
                                "SELECT COALESCE(SUM(LENGTH(payload_json)), 0) "
                                "FROM runtime_events"
                            )
                        )
                        genesis_bytes = await connection.scalar(
                            text(
                                "SELECT COALESCE(SUM("
                                "LENGTH(genesis_state_json)), 0) "
                                "FROM invocations"
                            )
                        )
                        recovery_bytes = await connection.scalar(
                            text(
                                "SELECT COALESCE(SUM("
                                "LENGTH(recovery_state_json)), 0) "
                                "FROM invocations"
                            )
                        )
                    return (
                        int(event_bytes or 0),
                        int(genesis_bytes or 0),
                        int(recovery_bytes or 0),
                    )

                actual_event_bytes, genesis_bytes, recovery_bytes = (
                    await backend._database_loop.arun(persisted_sizes())
                )
            finally:
                await app.aclose()

        self.assertEqual(expected_event_bytes, actual_event_bytes)
        self.assertGreater(genesis_bytes, 0)
        self.assertGreater(recovery_bytes, 0)


if __name__ == "__main__":
    unittest.main()
