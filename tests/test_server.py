from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

from fastapi import HTTPException

from autoagent import (
    AutoAgentApp,
    AutoAgentSettings,
    DatabaseBackend,
    SystemCommand,
    Workflow,
)
from autoagent.core.runtime import RuntimeEvent, RuntimeStore
from autoagent.core.server import AutoAgentServer
from autoagent.core.server.trace import TraceProjectionReducer
from autoagent.core.server.app import (
    InvocationResumeRequest,
    InvocationSubmitRequest,
)


class AutoAgentServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.app = AutoAgentApp()
        self.workflow = Workflow(id="server_wait")
        self.workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        self.app.register_workflow(self.workflow)
        await self.app.astart()
        self.server = AutoAgentServer(self.app)
        self.submit = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "") == "submit_invocation"
        )
        self.resume = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "") == "resume_invocation"
        )
        self.cancel = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "") == "cancel_invocation"
        )

    async def asyncTearDown(self) -> None:
        if self.server._invocation_tasks:
            await asyncio.gather(
                *tuple(self.server._invocation_tasks.values()),
                return_exceptions=True,
            )
        await self.app.aclose()

    async def test_waiting_session_rejects_submit_before_new_admission(self) -> None:
        first = await self.submit(
            self.workflow.id,
            InvocationSubmitRequest(
                session_key="same",
                input={"wait_key": "approval"},
            ),
        )
        await self._wait_for_state(first.invocation_id, "waiting")
        invocation_count = len(self.app.runtime_store.invocations)

        with self.assertRaises(HTTPException) as captured:
            await self.submit(
                self.workflow.id,
                InvocationSubmitRequest(
                    session_key="same",
                    input={"wait_key": "other"},
                ),
            )

        self.assertEqual(409, captured.exception.status_code)
        self.assertEqual(
            invocation_count,
            len(self.app.runtime_store.invocations),
        )

    async def test_submit_response_session_key_can_resume_wait(self) -> None:
        submitted = await self.submit(
            self.workflow.id,
            InvocationSubmitRequest(
                input={"wait_key": "approval"},
            ),
        )
        await self._wait_for_state(submitted.invocation_id, "waiting")

        resumed = await self.resume(
            self.workflow.id,
            InvocationResumeRequest(
                session_key=submitted.session_key,
                wait_key="approval",
                output={"approved": True},
            ),
        )

        self.assertEqual(submitted.session_id, resumed.session_id)
        self.assertEqual(submitted.session_key, resumed.session_key)
        self.assertEqual(submitted.invocation_id, resumed.invocation_id)
        self.assertEqual("completed", resumed.state)

    async def test_submit_selects_event_mode_per_invocation(self) -> None:
        submitted = await self.submit(
            self.workflow.id,
            InvocationSubmitRequest(
                input={"wait_key": "minimal"},
                session_key="minimal",
                event_mode="minimal",
            ),
        )
        await self._wait_for_state(submitted.invocation_id, "waiting")

        invocation = self.app.runtime_store.invocations[submitted.invocation_id]
        events = await self.app.runtime_store.alist_runtime_events(
            invocation_id=invocation.id,
        )
        self.assertEqual("minimal", invocation.event_mode)
        self.assertEqual([], list(events))

    async def test_background_failure_is_retrieved_and_retained(self) -> None:
        invocation_id = uuid4()

        async def fail() -> None:
            raise RuntimeError("background failed")

        task = asyncio.create_task(fail())
        self.server._invocation_tasks[invocation_id] = task
        task.add_done_callback(
            lambda completed: self.server._finish_invocation_task(
                invocation_id,
                completed,
            )
        )
        with self.assertRaisesRegex(RuntimeError, "background failed"):
            await task
        await asyncio.sleep(0)

        self.assertNotIn(invocation_id, self.server._invocation_tasks)
        self.assertIsInstance(
            self.server._invocation_failures[invocation_id],
            RuntimeError,
        )

    async def test_shutdown_cancels_invocation_after_bounded_grace(self) -> None:
        app = AutoAgentApp(
            settings=AutoAgentSettings(shutdown_grace_timeout_ms=10)
        )
        server = AutoAgentServer(app)
        started = asyncio.Event()

        async def never_finishes() -> None:
            started.set()
            await asyncio.Event().wait()

        invocation_id = uuid4()
        task = asyncio.create_task(never_finishes())
        server._invocation_tasks[invocation_id] = task
        await started.wait()

        await asyncio.wait_for(server.ashutdown(), timeout=0.5)

        self.assertTrue(task.cancelled())

    async def test_server_exposes_embeddable_v1_router(self) -> None:
        paths = {route.path for route in self.server.router.routes}
        self.assertIn("/api/v1/workflows", paths)
        self.assertIn("/api/v1/invocations/{invocation_id}/trace", paths)
        self.assertIn("/api/v1/invocations/{invocation_id}", paths)
        self.assertIn("/api/v1/invocations/{invocation_id}/stream", paths)
        self.assertIn("/api/v1/runtime/status", paths)
        self.assertIn("/api/v1/runtime/stream", paths)
        self.assertIn("/api/v1/health/live", paths)
        self.assertIn("/api/v1/health/ready", paths)

    async def test_standalone_server_bounds_uvicorn_graceful_shutdown(
        self,
    ) -> None:
        with patch("uvicorn.run") as run:
            self.server.run(host="127.0.0.1", port=8765)

        run.assert_called_once_with(
            self.server.api,
            host="127.0.0.1",
            port=8765,
            reload=False,
            timeout_graceful_shutdown=5.0,
        )

    async def test_runtime_status_stream_stops_after_client_disconnect(
        self,
    ) -> None:
        endpoint = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "") == "stream_runtime_status"
        )

        class DisconnectingRequest:
            def __init__(self) -> None:
                self.poll_count = 0

            async def is_disconnected(self) -> bool:
                self.poll_count += 1
                return self.poll_count > 1

        request = DisconnectingRequest()
        response = await endpoint(request)
        first = await anext(response.body_iterator)

        self.assertIn("event: runtime_status", first)
        with self.assertRaises(StopAsyncIteration):
            await anext(response.body_iterator)
        self.assertEqual(2, request.poll_count)

    async def test_runtime_status_separates_execution_and_persistence(self) -> None:
        status = self.server._runtime_status()

        self.assertEqual("ok", status["service"]["status"])
        self.assertTrue(status["execution"]["accepting_invocations"])
        self.assertEqual("memory", status["store"]["kind"])
        self.assertFalse(status["persistence"]["enabled"])
        self.assertEqual("memory_only", status["persistence"]["health"])

    async def test_running_invocation_can_be_cancelled_by_server_action(self) -> None:
        async def slow() -> str:
            await asyncio.sleep(3_600)
            return "unreachable"

        workflow = Workflow(id="server_cancel")
        workflow.add_node(slow, node_id="slow")
        self.app.register_workflow(workflow)
        submitted = await self.submit(
            workflow.id,
            InvocationSubmitRequest(entry_node_id="slow"),
        )
        await self._wait_for_state(submitted.invocation_id, "running")

        response = await self.cancel(submitted.invocation_id)

        self.assertEqual(submitted.invocation_id, response.invocation_id)
        self.assertEqual("cancelled", response.state)

    async def test_waiting_invocation_can_be_cancelled_without_active_task(self) -> None:
        submitted = await self.submit(
            self.workflow.id,
            InvocationSubmitRequest(
                session_key="cancel-wait",
                input={"wait_key": "approval"},
            ),
        )
        await self._wait_for_state(submitted.invocation_id, "waiting")
        for _ in range(1_000):
            if submitted.invocation_id not in self.server._invocation_tasks:
                break
            await asyncio.sleep(0.001)

        response = await self.cancel(submitted.invocation_id)

        self.assertEqual("cancelled", response.state)
        invocation = self.app.runtime_store.invocations[submitted.invocation_id]
        self.assertEqual("cancelled", invocation.state)
        self.assertEqual("cancelled", invocation.node_executions[-1].state)

    async def test_workflow_directory_uses_stable_cursor_pages(self) -> None:
        other = Workflow(id="second_workflow")
        other.add_node(lambda: "done", node_id="done")
        self.app.register_workflow(other)

        first = await self.server.trace.list_workflows(
            cursor=None,
            limit=1,
            registered_only=True,
        )
        second = await self.server.trace.list_workflows(
            cursor=first["next_cursor"],
            limit=1,
            registered_only=True,
        )

        self.assertTrue(first["has_more"])
        self.assertNotEqual(
            first["items"][0]["workflow_id"],
            second["items"][0]["workflow_id"],
        )
        self.assertFalse(second["has_more"])

    async def test_trace_bootstrap_separates_latest_projection_from_events(
        self,
    ) -> None:
        submitted = await self.submit(
            self.workflow.id,
            InvocationSubmitRequest(
                input={"wait_key": "trace"},
                session_key="trace",
                event_mode="full",
            ),
        )
        await self._wait_for_state(submitted.invocation_id, "waiting")

        bootstrap = await self.server.trace.trace_bootstrap(
            submitted.invocation_id,
            tail_limit=2,
        )

        self.assertEqual(self.workflow.id, bootstrap["workflow"]["workflow_id"])
        self.assertEqual("full", bootstrap["invocation"]["event_mode"])
        self.assertEqual([], bootstrap["event_page"]["items"])
        self.assertTrue(bootstrap["event_page"]["has_later"])
        self.assertEqual(
            bootstrap["invocation"]["live_sequence"],
            bootstrap["checkpoint"]["through_sequence"],
        )
        active_wait = bootstrap["checkpoint"]["projection"]["active_waits"][
            "trace"
        ]
        self.assertEqual("wait", active_wait["node_id"])
        self.assertIn("node_execution_id", active_wait)
        page = await self.server.trace.event_page(
            submitted.invocation_id,
            after_sequence=0,
            before_sequence=None,
            limit=2,
        )
        self.assertEqual(2, len(page["items"]))
        self.assertEqual(
            bootstrap["invocation"]["live_sequence"],
            page["live_sequence"],
        )
        self.assertEqual("waiting", page["invocation_state"])
        for event in page["items"]:
            self.assertNotIn("operations", event)
            self.assertIn("has_operations", event)
        detail = await self.server.trace.event_detail(
            submitted.invocation_id,
            page["items"][-1]["sequence"],
        )
        self.assertIn("operations", detail)
        state = await self.server.trace.runtime_state(
            submitted.invocation_id,
            through_sequence=bootstrap["invocation"]["live_sequence"],
        )
        self.assertEqual(
            bootstrap["invocation"]["live_sequence"],
            state["through_sequence"],
        )
        self.assertIn("node_executions", state)

    async def test_event_detail_exposes_recorded_operator_input_and_output(self) -> None:
        workflow = Workflow(id="server_event_values")
        workflow.add_node(
            lambda value: {"answer": value + 1},
            node_id="answer",
            input_mapping=lambda context: {
                "value": context.invocation_input["value"],
            },
        )
        self.app.register_workflow(workflow)
        submitted = await self.submit(
            workflow.id,
            InvocationSubmitRequest(
                input={"value": 41},
                session_key="event-values",
                event_mode="full",
            ),
        )
        await self._wait_for_state(submitted.invocation_id, "completed")

        events = await self.app.runtime_store.alist_runtime_events(
            invocation_id=submitted.invocation_id,
            limit=100,
        )
        operator_event = next(
            event
            for event in events
            if event.event_name == "operator_call.completed"
        )
        summary = self.server.trace.event_view(
            operator_event,
            include_values=False,
        )
        detail = await self.server.trace.event_detail(
            submitted.invocation_id,
            operator_event.sequence,
        )

        self.assertTrue(summary["has_input"])
        self.assertTrue(summary["has_output"])
        self.assertIsNone(summary["input"])
        self.assertIsNone(summary["output"])
        self.assertEqual({"value": 41}, detail["input"])
        self.assertEqual({"answer": 42}, detail["output"])

    async def test_projection_reducer_tracks_graph_state(self) -> None:
        invocation_id = uuid4()
        execution_id = uuid4()
        projection = TraceProjectionReducer.initial(invocation_id)
        projection = TraceProjectionReducer.apply(
            projection,
            RuntimeEvent(
                invocation_id=invocation_id,
                sequence=1,
                event_type="state_change",
                event_name="node.running",
                subject_type="node",
                subject_id="worker",
                occurred_at_ms=1,
                status="running",
                payload={
                    "node_id": "worker",
                    "node_execution_id": str(execution_id),
                },
            ),
        )
        projection = TraceProjectionReducer.apply(
            projection,
            RuntimeEvent(
                invocation_id=invocation_id,
                sequence=3,
                event_type="operator_call",
                event_name="operator_call.completed",
                subject_type="operator_call",
                subject_id="primary",
                occurred_at_ms=3,
                elapsed_ns=2_000_000,
                status="failed",
                payload={
                    "node_id": "worker",
                    "node_execution_id": str(execution_id),
                    "operator_call_id": "primary",
                    "operator_id": "primary",
                    "reason": "normal",
                    "state": "failed",
                    "error": {
                        "code": "OPERATOR_TIMEOUT",
                        "message": "timed out",
                    },
                },
            ),
        )
        projection = TraceProjectionReducer.apply(
            projection,
            RuntimeEvent(
                invocation_id=invocation_id,
                sequence=4,
                event_type="operator_call",
                event_name="operator_call.completed",
                subject_type="operator_call",
                subject_id="fallback",
                occurred_at_ms=4,
                elapsed_ns=1_000_000,
                status="completed",
                payload={
                    "node_id": "worker",
                    "node_execution_id": str(execution_id),
                    "operator_call_id": "fallback",
                    "operator_id": "fallback",
                    "reason": "fallback",
                    "state": "completed",
                },
            ),
        )
        projection = TraceProjectionReducer.apply(
            projection,
            RuntimeEvent(
                invocation_id=invocation_id,
                sequence=5,
                event_type="operator_call",
                event_name="operator_call.completed",
                subject_type="operator_call",
                subject_id="mapped",
                occurred_at_ms=5,
                elapsed_ns=5_000_000,
                status="completed",
                payload={
                    "node_id": "worker",
                    "node_execution_id": str(execution_id),
                    "operator_call_id": "mapped",
                    "kind": "map",
                    "operator_ids": ["mapped"],
                    "state": "completed",
                    "summary": {"call_count": 8},
                },
            ),
        )
        projection = TraceProjectionReducer.apply(
            projection,
            RuntimeEvent(
                invocation_id=invocation_id,
                sequence=6,
                event_type="routing",
                event_name="edge.evaluated",
                subject_type="edge",
                subject_id="worker->done",
                occurred_at_ms=2,
                status="selected",
                payload={
                    "edge_id": "worker->done",
                    "selected": True,
                    "state": "selected",
                    "source_execution_id": str(execution_id),
                    "target_node_id": "done",
                },
            ),
        )

        self.assertEqual("running", projection["nodes"]["worker"]["state"])
        self.assertEqual(
            1,
            projection["node_executions"][str(execution_id)][
                "first_event_sequence"
            ],
        )
        self.assertTrue(projection["edges"]["worker->done"]["selected"])
        execution = projection["node_executions"][str(execution_id)]
        self.assertEqual(3, execution["operator_call_count"])
        self.assertEqual(1, execution["fallback_count"])
        self.assertEqual(1, execution["timeout_count"])
        self.assertEqual(8, projection["nodes"]["worker"]["parallel_call_count"])
        self.assertEqual(6, projection["through_sequence"])

    async def test_projection_preserves_executed_loop_node_after_later_skip(
        self,
    ) -> None:
        invocation_id = uuid4()
        execution_id = uuid4()
        projection = TraceProjectionReducer.initial(invocation_id)
        events = (
            RuntimeEvent(
                invocation_id=invocation_id,
                sequence=1,
                event_type="state_change",
                event_name="node.running",
                subject_type="node",
                subject_id="loop_worker",
                occurred_at_ms=1,
                status="running",
                payload={
                    "node_id": "loop_worker",
                    "node_execution_id": str(execution_id),
                },
            ),
            RuntimeEvent(
                invocation_id=invocation_id,
                sequence=2,
                event_type="state_change",
                event_name="node.completed",
                subject_type="node",
                subject_id="loop_worker",
                occurred_at_ms=2,
                status="completed",
                payload={
                    "node_id": "loop_worker",
                    "node_execution_id": str(execution_id),
                },
            ),
            RuntimeEvent(
                invocation_id=invocation_id,
                sequence=3,
                event_type="state_change",
                event_name="node.skipped",
                subject_type="node",
                subject_id="loop_worker",
                occurred_at_ms=3,
                status="skipped",
                payload={
                    "node_id": "loop_worker",
                    "node_instance_key": "loop_worker@loop:2",
                },
            ),
            RuntimeEvent(
                invocation_id=invocation_id,
                sequence=4,
                event_type="state_change",
                event_name="node.skipped",
                subject_type="node",
                subject_id="never_run",
                occurred_at_ms=4,
                status="skipped",
                payload={
                    "node_id": "never_run",
                    "node_instance_key": "never_run@loop:2",
                },
            ),
        )
        for event in events:
            projection = TraceProjectionReducer.apply(projection, event)

        loop_worker = projection["nodes"]["loop_worker"]
        self.assertEqual("completed", loop_worker["state"])
        self.assertEqual("skipped", loop_worker["latest_occurrence_state"])
        self.assertEqual(1, loop_worker["execution_count"])
        self.assertEqual(1, loop_worker["skipped_count"])
        self.assertEqual("skipped", projection["nodes"]["never_run"]["state"])
        self.assertEqual(
            0,
            projection["nodes"]["never_run"]["execution_count"],
        )

    async def test_projection_preserves_edge_traversal_across_loop_evaluations(
        self,
    ) -> None:
        invocation_id = uuid4()
        projection = TraceProjectionReducer.initial(invocation_id)
        for sequence, selected in ((1, True), (2, False)):
            projection = TraceProjectionReducer.apply(
                projection,
                RuntimeEvent(
                    invocation_id=invocation_id,
                    sequence=sequence,
                    event_type="routing",
                    event_name="edge.evaluated",
                    subject_type="edge",
                    subject_id="loop_edge",
                    occurred_at_ms=sequence,
                    status="selected" if selected else "skipped",
                    payload={
                        "edge_id": "loop_edge",
                        "selected": selected,
                        "state": "selected" if selected else "skipped",
                        "target_node_id": "worker",
                    },
                ),
            )

        edge = projection["edges"]["loop_edge"]
        self.assertEqual("selected", edge["state"])
        self.assertTrue(edge["selected"])
        self.assertEqual("skipped", edge["latest_state"])
        self.assertFalse(edge["latest_selected"])
        self.assertEqual(1, edge["selected_count"])
        self.assertEqual(1, edge["skipped_count"])

    async def _wait_for_state(
        self,
        invocation_id,
        state: str,
    ) -> None:
        for _ in range(1_000):
            invocation = self.app.runtime_store.invocations[invocation_id]
            if invocation.state == state:
                return
            await asyncio.sleep(0.001)
        self.fail(f"Invocation did not reach state {state}.")


class PersistentTraceServerTests(unittest.IsolatedAsyncioTestCase):
    async def test_historical_trace_keeps_exact_graph_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.db"
            first_store = RuntimeStore(
                backend=DatabaseBackend.from_path(path)
            )
            first = AutoAgentApp(runtime_store=first_store)
            workflow = Workflow(id="historical_trace")
            workflow.add_node(lambda: {"answer": 42}, node_id="answer")
            first.register_workflow(workflow)
            await first.astart()
            invocation = await first.ainvoke(
                workflow,
                session_id="history",
                event_mode="standard",
            )
            await first_store.aflush()
            invocation_id = invocation.id
            await first.aclose()

            second_store = RuntimeStore(
                backend=DatabaseBackend.from_path(path)
            )
            second = AutoAgentApp(runtime_store=second_store)
            await second.astart()
            server = AutoAgentServer(second)
            try:
                runtime_status = server._runtime_status()
                workflows = await server.trace.list_workflows(
                    cursor=None,
                    limit=10,
                )
                sessions = await server.trace.list_sessions(
                    "historical_trace",
                    cursor=None,
                    limit=10,
                )
                invocations = await server.trace.list_invocations(
                    UUID(sessions["items"][0]["id"]),
                    cursor=None,
                    limit=10,
                )
                bootstrap = await server.trace.trace_bootstrap(
                    invocation_id,
                    tail_limit=20,
                )
                self.assertEqual(
                    ["historical_trace"],
                    [
                        item["workflow_id"]
                        for item in workflows["items"]
                    ],
                )
                self.assertEqual(1, sessions["items"][0]["invocation_count"])
                self.assertEqual(
                    str(invocation_id),
                    invocations["items"][0]["id"],
                )
                self.assertEqual(
                    "historical_trace",
                    bootstrap["workflow"]["workflow_id"],
                )
                self.assertEqual("durable", runtime_status["store"]["kind"])
                self.assertTrue(runtime_status["persistence"]["enabled"])
                self.assertEqual(
                    "DatabaseBackend",
                    runtime_status["persistence"]["backend_kind"],
                )
                self.assertEqual(
                    "running",
                    runtime_status["persistence"]["worker_state"],
                )
                self.assertLess(
                    runtime_status["persistence"]["low_watermark_bytes"],
                    runtime_status["persistence"]["high_watermark_bytes"],
                )
                self.assertLess(
                    runtime_status["persistence"]["high_watermark_bytes"],
                    runtime_status["persistence"]["hard_watermark_bytes"],
                )
                self.assertEqual(
                    ["answer"],
                    [
                        node["id"]
                        for node in bootstrap["workflow"]["nodes"]
                    ],
                )
                self.assertEqual(
                    "completed",
                    bootstrap["invocation"]["state"],
                )
                self.assertEqual([], bootstrap["event_page"]["items"])
                page = await server.trace.event_page(
                    invocation_id,
                    after_sequence=0,
                    before_sequence=None,
                    limit=200,
                )
                self.assertGreater(len(page["items"]), 0)
            finally:
                await second.aclose()
