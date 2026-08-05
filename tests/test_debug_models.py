from __future__ import annotations

import unittest
import tempfile
from pathlib import Path
import asyncio
from threading import Event as ThreadingEvent
import sqlite3

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
            lambda value: {"value": value + 1},
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
            workflow.add_node(lambda value: value + 1, node_id="work")
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
            finally:
                await reader_store.aclose()

        self.assertEqual(report.source, "database")
        self.assertEqual(report.state, "completed")
        self.assertEqual(report.node_execution_count, 1)
        self.assertEqual(report.operator_call_count, 1)
        self.assertEqual(report.observed_sequence, report.durable_sequence)

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
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
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

    async def test_runtime_event_pages_use_stable_opaque_cursor(self) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        workflow = Workflow(id="debug_event_pages")
        workflow.add_node(lambda: "done", node_id="work")
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

    async def test_user_event_page_excludes_builtin_stream_deltas_by_default(
        self,
    ) -> None:
        app = AutoAgentApp(settings=AutoAgentSettings())
        workflow = Workflow(id="debug_user_event_pages")
        workflow.add_node(lambda: "done", node_id="work")
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
