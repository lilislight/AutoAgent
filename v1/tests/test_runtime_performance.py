from __future__ import annotations

import asyncio
from collections.abc import Sequence
import os
from pathlib import Path
import threading
from time import perf_counter
import tempfile
import unittest
import json

from sqlalchemy import text

from autoagent import (
    AutoAgentApp,
    DatabaseBackend,
    PersistencePolicy,
    RuntimeStore,
    Workflow,
)
from autoagent.core.runtime.backends.database import _PersistenceItem
from autoagent.debug import DebugQueryService
from tests.helpers import started_app


MEMORY_CHAIN_MAX_SECONDS = float(
    os.getenv("AUTOAGENT_PERF_MEMORY_CHAIN_MAX_SECONDS", "5")
)
SQLITE_CHAIN_MAX_SECONDS = float(
    os.getenv("AUTOAGENT_PERF_SQLITE_CHAIN_MAX_SECONDS", "10")
)
DEBUG_QUERY_MAX_SECONDS = float(
    os.getenv("AUTOAGENT_PERF_DEBUG_QUERY_MAX_SECONDS", "5")
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
        app = started_app()
        workflow = _build_chain("memory_performance_chain", node_count=30)

        try:
            started = perf_counter()
            invocation = app.invoke(
                workflow,
                session_id="performance",
            )
            elapsed = perf_counter() - started
        finally:
            app.close()

        self.assertEqual("completed", invocation.state)
        self.assertLess(
            elapsed,
            MEMORY_CHAIN_MAX_SECONDS,
            "Memory runtime exceeded its smoke budget. Run "
            "`python -m benchmarks.runtime_store_benchmark` to profile.",
        )

    def test_standard_events_do_not_copy_large_node_output(self) -> None:
        store = RuntimeStore()
        app = started_app(runtime_store=store)
        workflow = Workflow(id="event_payload_performance")
        workflow.add_node(_large_output, node_id="large")

        try:
            invocation = app.invoke(
                workflow,
                session_id="performance",
                event_mode="standard",
            )
            events = asyncio.run(
                store.alist_runtime_events(
                    invocation_id=invocation.id,
                    limit=100,
                )
            )
        finally:
            app.close()

        encoded_sizes = [
            len(store.serializer.dumps(event.model_dump(mode="python")))
            for event in events
        ]
        self.assertTrue(encoded_sizes)
        self.assertLess(max(encoded_sizes), 20_000)


class DatabasePerformanceRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_large_journal_report_and_first_page_stay_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "debug-runtime.db"
            writer = RuntimeStore(
                backend=DatabaseBackend.from_path(
                    path,
                    batch_max_delay_ms=0,
                )
            )
            app = started_app(runtime_store=writer)
            workflow = _build_chain("debug_large_journal", node_count=80)
            workflow.nodes[0].input_mapping = lambda ctx: {}
            try:
                await app.astart()
                invocation = await app.ainvoke(
                    workflow,
                    input={"large": "x" * 100_000},
                    event_mode="standard",
                )
                invocation_id = invocation.id
            finally:
                await app.aclose()

            reader = RuntimeStore(
                backend=DatabaseBackend.from_path(path, read_only=True)
            )
            try:
                await reader.ainitialize()
                service = DebugQueryService(reader, source="database")
                started = perf_counter()
                report = await service.report(invocation_id)
                page = await service.node_executions(
                    invocation_id,
                    through_sequence=report.observed_sequence,
                    limit=20,
                )
                elapsed = perf_counter() - started
            finally:
                await reader.aclose()

        report_bytes = len(
            json.dumps(report.model_dump(mode="json")).encode("utf-8")
        )
        page_bytes = len(
            json.dumps(page.model_dump(mode="json")).encode("utf-8")
        )
        self.assertEqual(80, report.node_execution_count)
        self.assertEqual(20, len(page.items))
        self.assertTrue(page.has_more)
        self.assertNotIn("x" * 100, json.dumps(report.input.preview))
        self.assertLess(report_bytes, 20_000)
        self.assertLess(page_bytes, 50_000)
        self.assertLess(
            elapsed,
            DEBUG_QUERY_MAX_SECONDS,
            "Large-journal Debug query exceeded its smoke budget.",
        )

    async def test_sqlite_runtime_returns_and_flushes_thirty_node_chain_within_budget(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            backend = DatabaseBackend.from_path(
                Path(directory) / "runtime.db",
                batch_max_delay_ms=0,
            )
            store = RuntimeStore(backend=backend)
            app = started_app(runtime_store=store)
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
            "`python -m benchmarks.runtime_store_benchmark` to profile.",
        )
        self.assertLess(
            flush_elapsed,
            SQLITE_CHAIN_MAX_SECONDS,
            "SQLite persistence exceeded its flush smoke budget. Run "
            "`python -m benchmarks.runtime_store_benchmark` to profile.",
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
                batch_max_delay_ms=0,
                recovery_event_interval=1_000,
            )
            persistence_policy = PersistencePolicy(
                queue_low_watermark_bytes=24 * 1024,
                queue_high_watermark_bytes=48 * 1024,
                queue_hard_watermark_bytes=16 * 1024 * 1024,
                admission_timeout_ms=0,
            )
            store = RuntimeStore(
                backend=backend,
                persistence_policy=persistence_policy,
            )
            app = started_app(runtime_store=store)
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

                runtime_event_count = sum(
                    len(store.runtime_events[invocation.id])
                    for invocation in invocations
                )
                self.assertGreaterEqual(
                    store.pending_persistence_count,
                    runtime_event_count,
                )
                self.assertLessEqual(
                    store.pending_persistence_count - runtime_event_count,
                    len(admitted) + 1,
                    "Only Workflow metadata and Invocation genesis records "
                    "may add queue entries beyond Runtime Events.",
                )
                self.assertGreaterEqual(
                    store.pending_persistence_bytes,
                    persistence_policy.queue_high_watermark_bytes,
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
            app = started_app(runtime_store=store)
            workflow = _build_chain("persisted_payload_size", node_count=5)

            try:
                invocations = [
                    await app.ainvoke(
                        workflow,
                        session_id=f"session-{index}",
                        event_mode="full",
                    )
                    for index in range(4)
                ]
                await store.aflush()

                async def persisted_sizes() -> tuple[int, int, int]:
                    async with backend.engine.connect() as connection:
                        event_bytes = await connection.scalar(
                            text(
                                "SELECT COALESCE(SUM("
                                "LENGTH(payload_json) + LENGTH(timing_json) + "
                                "LENGTH(COALESCE(input_json, '')) + "
                                "LENGTH(COALESCE(output_json, '')) + "
                                "LENGTH(COALESCE(operations_json, ''))"
                                "), 0) "
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
                                "LENGTH(state_json)), 0) "
                                "FROM runtime_recovery_states"
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

        self.assertGreater(actual_event_bytes, 0)
        self.assertGreater(genesis_bytes, 0)
        self.assertGreater(recovery_bytes, 0)

    async def test_persisted_bytes_increase_from_minimal_to_standard_to_full(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            backend = DatabaseBackend.from_path(
                Path(directory) / "runtime.db",
                batch_max_delay_ms=0,
            )
            store = RuntimeStore(backend=backend)
            app = started_app(runtime_store=store)
            workflow = _build_chain("event_mode_size", node_count=5)
            try:
                invocations = {
                    mode: await app.ainvoke(
                        workflow,
                        session_id=mode,
                        event_mode=mode,
                    )
                    for mode in ("minimal", "standard", "full")
                }
                await store.aflush()

                async def persisted_bytes(invocation_id: str) -> int:
                    async with backend.engine.connect() as connection:
                        invocation_bytes = await connection.scalar(
                            text(
                                "SELECT LENGTH(input_json) + "
                                "LENGTH(COALESCE(result_json, '')) + "
                                "LENGTH(COALESCE(error_json, '')) + "
                                "LENGTH(COALESCE(genesis_state_json, '')) "
                                "FROM invocations WHERE id = :id"
                            ),
                            {"id": invocation_id},
                        )
                        event_bytes = await connection.scalar(
                            text(
                                "SELECT COALESCE(SUM("
                                "LENGTH(payload_json) + LENGTH(timing_json) + "
                                "LENGTH(COALESCE(input_json, '')) + "
                                "LENGTH(COALESCE(output_json, '')) + "
                                "LENGTH(COALESCE(operations_json, ''))"
                                "), 0) FROM runtime_events "
                                "WHERE invocation_id = :id"
                            ),
                            {"id": invocation_id},
                        )
                        recovery_bytes = await connection.scalar(
                            text(
                                "SELECT COALESCE(LENGTH(state_json), 0) "
                                "FROM runtime_recovery_states "
                                "WHERE invocation_id = :id"
                            ),
                            {"id": invocation_id},
                        )
                    return int(invocation_bytes or 0) + int(
                        event_bytes or 0
                    ) + int(recovery_bytes or 0)

                sizes = {
                    mode: await backend._database_loop.arun(
                        persisted_bytes(str(invocation.id))
                    )
                    for mode, invocation in invocations.items()
                }
            finally:
                await app.aclose()

        self.assertLess(sizes["minimal"], sizes["standard"])
        self.assertLess(sizes["standard"], sizes["full"])


if __name__ == "__main__":
    unittest.main()
