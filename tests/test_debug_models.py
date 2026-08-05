from __future__ import annotations

import unittest
import tempfile
from pathlib import Path
import asyncio
from threading import Event as ThreadingEvent
import sqlite3
from uuid import UUID

from pydantic import ValidationError

from autoagent.debug import (
    DebugPage,
    DebugQueryService,
    InvocationReport,
    ValueSummary,
    summarize_value,
)
from autoagent import (
    AutoAgentApp,
    AutoAgentSettings,
    DatabaseBackend,
    RuntimeStore,
    SystemCommand,
    Workflow,
)
from autoagent.core.runtime import UserEventSpec
from autoagent.core.workflow import MapPolicy, NodePolicy, RetryPolicy
from tests.helpers import dynamic_json_callable


def _report(**updates: object) -> InvocationReport:
    values: dict[str, object] = {
        "source": "server",
        "invocation_id": "invocation-1",
        "session_id": "session-1",
        "workflow_id": "workflow-1",
        "workflow_revision_id": "revision-1",
        "event_mode": "full",
        "execution_mode": "normal",
        "state": "completed",
        "created_at_ms": 1,
        "updated_at_ms": 2,
        "observed_sequence": 8,
        "durable_sequence": 7,
        "observed_user_event_sequence": 3,
        "durable_user_event_sequence": 2,
        "persistence_status": "pending",
        "user_event_persistence_status": "pending",
        "input": ValueSummary(type="object", shape={"keys": 1}),
    }
    values.update(updates)
    return InvocationReport.model_validate(values)


class DebugModelTests(unittest.TestCase):
    def test_report_accepts_partially_durable_evidence(self) -> None:
        report = _report(
            user_event_counts={"message": 1, "tool_result": 2},
        )

        self.assertEqual(report.observed_sequence, 8)
        self.assertEqual(report.durable_sequence, 7)
        self.assertEqual(report.user_event_counts["tool_result"], 2)

    def test_report_rejects_durable_sequence_past_observed_sequence(self) -> None:
        with self.assertRaisesRegex(
            ValidationError,
            "durable_sequence cannot exceed observed_sequence",
        ):
            _report(durable_sequence=9)

    def test_page_requires_cursor_when_more_items_exist(self) -> None:
        with self.assertRaisesRegex(
            ValidationError,
            "requires next_cursor",
        ):
            DebugPage[int](through_sequence=4, items=(1,), has_more=True)

    def test_debug_models_are_immutable(self) -> None:
        report = _report()

        with self.assertRaises(ValidationError):
            report.state = "failed"  # type: ignore[misc]

    def test_value_summary_redacts_sensitive_fields(self) -> None:
        summary = summarize_value(
            {
                "city": "Shanghai",
                "credentials": {"api_key": "do-not-print"},
            },
            detail_ref="value-ref",
        )

        self.assertTrue(summary.redacted)
        self.assertEqual(
            summary.preview,
            {
                "city": "Shanghai",
                "credentials": {"api_key": "[REDACTED]"},
            },
        )
        self.assertNotIn("do-not-print", str(summary.model_dump()))

    def test_large_value_has_shape_but_no_preview(self) -> None:
        summary = summarize_value(
            {"payload": "x" * 2_000},
            detail_ref="value-ref",
        )

        self.assertIsNone(summary.preview)
        self.assertEqual(summary.shape["key_count"], 1)
        self.assertEqual(summary.detail_ref, "value-ref")


class DebugQueryServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_minimal_report_exposes_only_invocation_evidence(self) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        workflow = Workflow(id="debug_minimal_report")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="work")
        try:
            await app.astart()
            invocation = await app.ainvoke(workflow, event_mode="minimal")
            service = DebugQueryService(app.runtime_store, source="server")
            report = await service.report(invocation.id)
            nodes = await service.node_executions(invocation.id)
        finally:
            await app.aclose()

        self.assertEqual(0, report.observed_sequence)
        self.assertEqual(0, report.node_execution_count)
        self.assertEqual(("invocation", "values"), report.available_evidence)
        self.assertEqual((), nodes.items)

    async def test_report_counts_retry_fallback_and_operator_attempts(self) -> None:
        calls = 0

        def primary() -> str:
            nonlocal calls
            calls += 1
            raise RuntimeError("primary failed")

        def fallback() -> str:
            return "recovered"

        app = AutoAgentApp(settings=AutoAgentSettings())
        app.register_capability("debug_retry")
        app.register_operator(
            primary,
            operator_id="primary",
            capability_id="debug_retry",
            default=True,
        )
        app.register_operator(
            fallback,
            operator_id="fallback",
            capability_id="debug_retry",
        )
        workflow = Workflow(id="debug_retry_report")
        workflow.add_node(
            "debug_retry",
            node_id="retry",
            policy=NodePolicy(retry=RetryPolicy(max_attempts=2)),
        )
        try:
            await app.astart()
            invocation = await app.ainvoke(workflow, event_mode="full")
            service = DebugQueryService(app.runtime_store, source="server")
            report = await service.report(invocation.id)
            operator_calls = await service.operator_calls(invocation.id)
        finally:
            await app.aclose()

        self.assertEqual("completed", report.state)
        self.assertEqual(3, report.operator_call_count)
        self.assertEqual(1, report.retry_count)
        self.assertEqual(1, report.fallback_count)
        self.assertEqual(
            ("normal", "retry", "fallback"),
            tuple(item["reason"] for item in operator_calls.items),
        )

    async def test_map_is_one_logical_operator_call_with_parallel_summary(
        self,
    ) -> None:
        workflow = Workflow(id="debug_map_report")
        workflow.add_node(dynamic_json_callable(lambda: [1, 2, 3]), node_id="source")
        workflow.add_node(
            dynamic_json_callable(lambda value: value * 2),
            node_id="mapped",
            policy=NodePolicy(
                map=MapPolicy(
                    item_selector=lambda ctx: [
                        {"value": item} for item in ctx.input
                    ],
                    output_aggregator=dynamic_json_callable(
                        lambda ctx: sum(ctx.item_outputs)
                    ),
                )
            ),
        )
        workflow.add_edge(
            "source",
            "mapped",
        )
        app = AutoAgentApp(settings=AutoAgentSettings())
        try:
            await app.astart()
            invocation = await app.ainvoke(workflow, event_mode="full")
            service = DebugQueryService(app.runtime_store, source="server")
            report = await service.report(invocation.id)
            calls = await service.operator_calls(invocation.id)
        finally:
            await app.aclose()

        mapped = next(item for item in calls.items if item["kind"] == "map")
        self.assertEqual("completed", report.state)
        self.assertEqual(2, report.operator_call_count)
        self.assertEqual(3, mapped["call_count"])
        self.assertEqual(3, mapped["attempt_count"])

    async def test_loop_report_preserves_each_node_execution_occurrence(
        self,
    ) -> None:
        workflow = Workflow(id="debug_loop_report")
        workflow.add_node(dynamic_json_callable(lambda: 0), node_id="start")
        workflow.add_node(
            dynamic_json_callable(lambda value: value + 1),
            node_id="agent",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        workflow.add_node(
            dynamic_json_callable(lambda value: value),
            node_id="finish",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        workflow.add_edge("start", "agent", edge_id="enter")
        workflow.add_edge(
            "agent",
            "agent",
            edge_id="continue",
            condition=lambda ctx: ctx.source_output < 3,
        )
        workflow.add_edge(
            "agent",
            "finish",
            edge_id="exit",
            condition=lambda ctx: ctx.source_output >= 3,
        )
        app = AutoAgentApp(settings=AutoAgentSettings())
        try:
            await app.astart()
            invocation = await app.ainvoke(workflow, event_mode="full")
            service = DebugQueryService(app.runtime_store, source="server")
            report = await service.report(invocation.id)
            nodes = await service.node_executions(invocation.id)
            edges = await service.edge_evaluations(invocation.id)
        finally:
            await app.aclose()

        self.assertEqual("completed", report.state)
        self.assertEqual(5, report.node_execution_count)
        self.assertEqual(7, report.edge_evaluation_count)
        self.assertEqual(
            3,
            sum(item["node_id"] == "agent" for item in nodes.items),
        )
        self.assertEqual(
            (False, False, True),
            tuple(
                item["selected"]
                for item in edges.items
                if item["edge_id"] == "exit"
            ),
        )

    async def test_read_only_database_backend_does_not_create_runtime_schema(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "empty.db"
            path.touch()
            store = RuntimeStore(
                backend=DatabaseBackend.from_path(path, read_only=True)
            )
            try:
                with self.assertRaises(Exception):
                    await store.ainitialize()
            finally:
                await store.aclose()
            with sqlite3.connect(path) as database:
                tables = database.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()

        self.assertEqual([], tables)

    async def test_completed_memory_report_is_compact_and_counted(self) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        workflow = Workflow(id="debug_report")
        workflow.add_node(
            dynamic_json_callable(lambda value: {"value": value + 1}),
            node_id="work",
            input_mapping=lambda ctx: {
                "value": ctx.invocation_input["value"]
            },
        )
        try:
            await app.astart()
            invocation = await app.ainvoke(
                workflow,
                input={"value": 1, "api_key": "hidden"},
                event_mode="full",
            )

            report = await DebugQueryService(
                app.runtime_store,
                source="server",
            ).report(invocation.id)
        finally:
            await app.aclose()

        self.assertEqual(report.state, "completed")
        self.assertEqual(report.node_execution_count, 1)
        self.assertEqual(report.operator_call_count, 1)
        self.assertTrue(report.input.redacted)
        self.assertNotIn("hidden", str(report.model_dump()))
        self.assertIn("runtime_state", report.available_evidence)
        self.assertNotIn(
            "RUNTIME_EVENTS_NOT_FULLY_DURABLE",
            {warning.code for warning in report.warnings},
        )

    async def test_waiting_report_identifies_wait_boundary(self) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        workflow = Workflow(id="debug_wait_report")
        workflow.add_node(SystemCommand(id="wait"), node_id="approval")
        try:
            await app.astart()
            invocation = await app.ainvoke(
                workflow,
                input={"wait_key": "human_approval"},
                event_mode="standard",
            )

            report = await DebugQueryService(
                app.runtime_store,
                source="server",
            ).report(invocation.id)
        finally:
            await app.aclose()

        self.assertEqual(report.state, "waiting")
        self.assertIsNotNone(report.primary_boundary)
        assert report.primary_boundary is not None
        self.assertEqual(report.primary_boundary.kind, "wait")
        self.assertEqual(report.primary_boundary.subject_id, "human_approval")

    async def test_failed_report_identifies_failed_node_without_inference(
        self,
    ) -> None:
        def fail() -> str:
            raise RuntimeError("fixture failed")

        app = AutoAgentApp(settings=AutoAgentSettings())
        workflow = Workflow(id="debug_failed_report")
        workflow.add_node(fail, node_id="broken")
        try:
            await app.astart()
            invocation = await app.ainvoke(workflow, event_mode="full")
            report = await DebugQueryService(
                app.runtime_store,
                source="server",
            ).report(invocation.id)
        finally:
            await app.aclose()

        self.assertEqual(report.state, "failed")
        self.assertIsNotNone(report.error)
        self.assertIsNotNone(report.primary_boundary)
        assert report.primary_boundary is not None
        self.assertEqual(report.primary_boundary.kind, "node")
        self.assertEqual(report.primary_boundary.node_id, "broken")
        self.assertEqual(report.primary_boundary.status, "failed")

    async def test_database_report_uses_type_neutral_persisted_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "debug.db"
            writer_store = RuntimeStore(backend=DatabaseBackend.from_path(path))
            app = AutoAgentApp(runtime_store=writer_store)
            workflow = Workflow(id="historical_debug_report")
            workflow.add_node(dynamic_json_callable(lambda value: value + 1), node_id="work")
            try:
                await app.astart()
                invocation = await app.ainvoke(
                    workflow,
                    input={"value": 1},
                    event_mode="standard",
                )
                invocation_id = invocation.id
            finally:
                await app.aclose()

            reader_store = RuntimeStore(
                backend=DatabaseBackend.from_path(path)
            )
            try:
                await reader_store.ainitialize()
                report = await DebugQueryService(
                    reader_store,
                    source="database",
                ).report(invocation_id)
                service = DebugQueryService(reader_store, source="database")
                nodes = await service.node_executions(invocation_id)
                node = await service.node_execution(
                    invocation_id,
                    UUID(nodes.items[0]["node_execution_id"]),
                    through_sequence=nodes.through_sequence,
                )
            finally:
                await reader_store.aclose()

        self.assertEqual(report.source, "database")
        self.assertEqual(report.state, "completed")
        self.assertEqual(report.node_execution_count, 1)
        self.assertEqual(report.operator_call_count, 1)
        self.assertEqual(report.observed_sequence, report.durable_sequence)
        self.assertEqual("completed", node["state"])

    async def test_database_progressive_queries_are_type_neutral(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "debug-details.db"
            writer_store = RuntimeStore(backend=DatabaseBackend.from_path(path))
            app = AutoAgentApp(runtime_store=writer_store)
            workflow = Workflow(id="historical_debug_details")
            workflow.add_node(dynamic_json_callable(lambda: {"value": 1}), node_id="start")
            workflow.add_node(
                dynamic_json_callable(lambda value: value + 1),
                node_id="finish",
                input_mapping=lambda ctx: {
                    "value": ctx.incoming[0].value["value"]
                },
            )
            workflow.add_edge("start", "finish")
            try:
                await app.astart()
                invocation = await app.ainvoke(workflow, event_mode="full")
                invocation_id = invocation.id
            finally:
                await app.aclose()

            reader_store = RuntimeStore(
                backend=DatabaseBackend.from_path(path, read_only=True)
            )
            try:
                await reader_store.ainitialize()
                service = DebugQueryService(reader_store, source="database")
                nodes = await service.node_executions(invocation_id)
                edges = await service.edge_evaluations(invocation_id)
                calls = await service.operator_calls(invocation_id)
                node = await service.node_execution(
                    invocation_id,
                    UUID(nodes.items[0]["node_execution_id"]),
                    through_sequence=nodes.through_sequence,
                )
                edge = await service.edge_evaluation(
                    invocation_id,
                    edges.items[0]["edge_evaluation_id"],
                    through_sequence=edges.through_sequence,
                )
                call = await service.operator_call(
                    invocation_id,
                    UUID(calls.items[0]["operator_call_id"]),
                    through_sequence=calls.through_sequence,
                )
                state = await service.runtime_state(
                    invocation_id,
                    through_sequence=nodes.through_sequence,
                    path="/invocation/state",
                )
            finally:
                await reader_store.aclose()

        self.assertEqual(2, len(nodes.items))
        self.assertEqual(1, len(edges.items))
        self.assertEqual(2, len(calls.items))
        self.assertEqual("completed", node["state"])
        self.assertTrue(edge["selected"])
        self.assertEqual("completed", call["state"])
        self.assertEqual("completed", state["value"]["preview"])

    async def test_running_report_waits_on_notification_then_returns_terminal(
        self,
    ) -> None:
        started = ThreadingEvent()
        release = ThreadingEvent()

        def slow() -> str:
            started.set()
            release.wait(timeout=2)
            return "done"

        app = AutoAgentApp(settings=AutoAgentSettings())
        workflow = Workflow(id="debug_running_report")
        workflow.add_node(slow, node_id="slow")
        try:
            await app.astart()
            invocation_task = asyncio.create_task(
                app.ainvoke(workflow, event_mode="standard")
            )
            async with asyncio.timeout(1):
                while not started.is_set():
                    await asyncio.sleep(0.001)
            invocation = next(iter(app.runtime_store.invocations.values()))
            service = DebugQueryService(app.runtime_store, source="server")

            active = await service.report_when_stable(
                invocation.id,
                timeout_ms=1,
            )
            self.assertEqual(active.state, "running")
            self.assertIn(
                "INVOCATION_STILL_RUNNING",
                {warning.code for warning in active.warnings},
            )

            report_task = asyncio.create_task(
                service.report_when_stable(invocation.id, timeout_ms=1_000)
            )
            release.set()
            report = await report_task
            await invocation_task
        finally:
            release.set()
            await app.aclose()

        self.assertEqual(report.state, "completed")
        self.assertNotIn(
            "INVOCATION_STILL_RUNNING",
            {warning.code for warning in report.warnings},
        )

    async def test_report_marks_runtime_events_that_are_not_durable_yet(
        self,
    ) -> None:
        release = ThreadingEvent()

        class BlockedEventBackend(DatabaseBackend):
            async def _persist_batch(self, batch):
                if any(item.kind == "event" for item in batch):
                    await asyncio.to_thread(release.wait)
                await super()._persist_batch(batch)

        with tempfile.TemporaryDirectory() as directory:
            backend = BlockedEventBackend.from_path(
                Path(directory) / "partial.db",
                batch_max_delay_ms=0,
            )
            app = AutoAgentApp(runtime_store=RuntimeStore(backend=backend))
            workflow = Workflow(id="debug_partial_durability")
            workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="work")
            try:
                await app.astart()
                invocation = await app.ainvoke(
                    workflow,
                    event_mode="standard",
                )
                report = await DebugQueryService(
                    app.runtime_store,
                    source="server",
                ).report(invocation.id)
            finally:
                release.set()
                await app.aclose()

        self.assertGreater(report.observed_sequence, report.durable_sequence)
        self.assertIn(
            "RUNTIME_EVENTS_NOT_FULLY_DURABLE",
            {warning.code for warning in report.warnings},
        )

    async def test_runtime_event_pages_use_stable_opaque_cursor(self) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        workflow = Workflow(id="debug_event_pages")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="work")
        try:
            await app.astart()
            invocation = await app.ainvoke(workflow, event_mode="full")
            service = DebugQueryService(app.runtime_store, source="server")

            first = await service.runtime_events(invocation.id, limit=2)
            second = await service.runtime_events(
                invocation.id,
                cursor=first.next_cursor,
                limit=2,
            )
            detail = await service.runtime_event(
                invocation.id,
                first.items[0]["sequence"],
                through_sequence=first.through_sequence,
            )
        finally:
            await app.aclose()

        self.assertTrue(first.has_more)
        self.assertIsNotNone(first.next_cursor)
        self.assertEqual(first.through_sequence, second.through_sequence)
        self.assertLess(
            first.items[-1]["sequence"],
            second.items[0]["sequence"],
        )
        self.assertIn("payload", detail)
        self.assertNotIn("operations", detail)

    async def test_execution_pages_and_details_share_observed_boundary(
        self,
    ) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        workflow = Workflow(id="debug_execution_pages")
        workflow.add_node(dynamic_json_callable(lambda: {"value": 1}), node_id="start")
        workflow.add_node(
            dynamic_json_callable(lambda value: {"value": value + 1}),
            node_id="finish",
            input_mapping=lambda ctx: {
                "value": ctx.incoming[0].value["value"]
            },
        )
        workflow.add_edge("start", "finish")
        try:
            await app.astart()
            invocation = await app.ainvoke(workflow, event_mode="full")
            service = DebugQueryService(app.runtime_store, source="server")

            first_nodes = await service.node_executions(
                invocation.id,
                limit=1,
            )
            second_nodes = await service.node_executions(
                invocation.id,
                cursor=first_nodes.next_cursor,
                limit=1,
            )
            node_id = UUID(first_nodes.items[0]["node_execution_id"])
            node = await service.node_execution(
                invocation.id,
                node_id,
                through_sequence=first_nodes.through_sequence,
            )
            edges = await service.edge_evaluations(
                invocation.id,
                through_sequence=first_nodes.through_sequence,
            )
            edge = await service.edge_evaluation(
                invocation.id,
                edges.items[0]["edge_evaluation_id"],
                through_sequence=edges.through_sequence,
            )
            calls = await service.operator_calls(
                invocation.id,
                through_sequence=first_nodes.through_sequence,
            )
            call = await service.operator_call(
                invocation.id,
                UUID(calls.items[0]["operator_call_id"]),
                through_sequence=calls.through_sequence,
            )
        finally:
            await app.aclose()

        self.assertTrue(first_nodes.has_more)
        self.assertEqual(1, len(second_nodes.items))
        self.assertEqual(first_nodes.through_sequence, second_nodes.through_sequence)
        self.assertLess(
            first_nodes.items[0]["start_sequence"],
            second_nodes.items[0]["start_sequence"],
        )
        self.assertEqual("completed", node["state"])
        self.assertEqual("start", node["node_id"])
        self.assertTrue(edge["selected"])
        self.assertEqual("finish", edge["target_node_id"])
        self.assertEqual("completed", call["state"])
        self.assertIsNotNone(call["input"])
        self.assertIsNotNone(call["output"])

    async def test_full_runtime_state_is_path_addressable_and_bounded(
        self,
    ) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        workflow = Workflow(id="debug_runtime_state")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="work")
        try:
            await app.astart()
            invocation = await app.ainvoke(workflow, event_mode="full")
            service = DebugQueryService(app.runtime_store, source="server")

            summary = await service.runtime_state(invocation.id)
            state = await service.runtime_state(
                invocation.id,
                through_sequence=summary["through_sequence"],
                path="/invocation/state",
            )
        finally:
            await app.aclose()

        self.assertEqual("completed", summary["invocation_state"])
        self.assertEqual(1, summary["node_execution_count"])
        self.assertEqual("completed", state["value"]["preview"])

    async def test_standard_runtime_state_is_explicitly_unavailable(self) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        workflow = Workflow(id="debug_standard_state")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="work")
        try:
            await app.astart()
            invocation = await app.ainvoke(workflow, event_mode="standard")
            service = DebugQueryService(app.runtime_store, source="server")
            with self.assertRaisesRegex(ValueError, "only in full mode"):
                await service.runtime_state(invocation.id)
        finally:
            await app.aclose()

    async def test_user_event_page_excludes_builtin_stream_deltas_by_default(
        self,
    ) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        workflow = Workflow(id="debug_user_event_pages")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="work")
        try:
            await app.astart()
            invocation = await app.ainvoke(workflow, event_mode="standard")
            node_execution = invocation.node_executions[0]
            for event_type in (
                "message_delta",
                "message_completed",
                "custom_notice",
            ):
                app.runtime_store.record_user_event(
                    invocation_id=invocation.id,
                    spec=UserEventSpec(
                        type=event_type,
                        data={"type": event_type},
                        node_id=node_execution.node_id,
                        node_execution_id=node_execution.id,
                    ),
                )
            service = DebugQueryService(app.runtime_store, source="server")

            semantic = await service.user_events(invocation.id)
            diagnostic = await service.user_events(
                invocation.id,
                include_stream_deltas=True,
            )
            detail = await service.user_event(
                invocation.id,
                semantic.items[0]["sequence"],
            )
        finally:
            await app.aclose()

        self.assertEqual(
            ("message_completed", "custom_notice"),
            tuple(item["type"] for item in semantic.items),
        )
        self.assertEqual(3, len(diagnostic.items))
        self.assertIn("data", detail)


if __name__ == "__main__":
    unittest.main()
