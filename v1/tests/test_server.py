from __future__ import annotations

import asyncio
import inspect
import tempfile
import unittest
from pathlib import Path
from threading import Event as ThreadingEvent
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

from fastapi import HTTPException
from pydantic import BaseModel

from autoagent import (
    AutoAgentApp,
    AutoAgentSettings,
    DatabaseBackend,
    SystemCommand,
    Workflow,
)
from autoagent.core.compiler import workflow_revision_id
from autoagent.core.runtime import RuntimeEvent, RuntimeStore, UserEventSpec
from autoagent.core.server import AutoAgentServer
from autoagent.core.server.trace import TraceProjectionReducer, _workflow_groups
from autoagent.core.server.app import (
    InvocationResumeRequest,
    InvocationSubmitRequest,
)
from tests.helpers import dynamic_json_callable


class HistoricalTracePayload(BaseModel):
    value: int


def historical_trace_model() -> HistoricalTracePayload:
    return HistoricalTracePayload(value=42)


class WorkflowGroupViewTests(unittest.TestCase):
    def test_nested_and_sibling_workflows_have_complete_group_trees(self) -> None:
        nodes = [
            {"id": "start", "workflow_path": []},
            {"id": "first/inner/work", "workflow_path": ["first", "inner"]},
            {"id": "second/work", "workflow_path": ["second"]},
            {"id": "end", "workflow_path": []},
        ]
        edges = [
            {
                "id": "enter_first",
                "from_node": "start",
                "to_node": "first/inner/work",
                "workflow_path": [],
            },
            {
                "id": "first/inner/inside",
                "from_node": "first/inner/work",
                "to_node": "first/inner/work",
                "workflow_path": ["first", "inner"],
            },
            {
                "id": "between",
                "from_node": "first/inner/work",
                "to_node": "second/work",
                "workflow_path": [],
            },
            {
                "id": "leave_second",
                "from_node": "second/work",
                "to_node": "end",
                "workflow_path": [],
            },
        ]

        groups = {
            value["id"]: value
            for value in _workflow_groups(nodes, edges)
        }

        self.assertEqual({"first", "first/inner", "second"}, set(groups))
        self.assertEqual("first", groups["first/inner"]["parent_group_id"])
        self.assertEqual(
            ["first/inner/work"],
            groups["first"]["node_ids"],
        )
        self.assertEqual([], groups["first"]["direct_node_ids"])
        self.assertEqual(
            ["first/inner/inside"],
            groups["first"]["edge_ids"],
        )
        self.assertEqual([], groups["first"]["direct_edge_ids"])
        self.assertEqual(
            ["first/inner/inside"],
            groups["first/inner"]["direct_edge_ids"],
        )
        self.assertEqual(
            ["first/inner/work"],
            groups["first"]["entry_node_ids"],
        )
        self.assertEqual(
            ["first/inner/work"],
            groups["first"]["exit_node_ids"],
        )


class AutoAgentServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.app = AutoAgentApp(settings=AutoAgentSettings())
        self.workflow = Workflow(id="server_wait")
        self.workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        self.app.register_workflow(self.workflow)
        self.workflow_revision_id = self._revision_id(self.workflow)
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
        self.rerun = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "") == "rerun_invocation"
        )
        self.compare = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "") == "compare_invocations"
        )
        self.artifact_value = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "")
            == "get_invocation_artifact_value"
        )

    async def asyncTearDown(self) -> None:
        if self.server._invocation_tasks:
            await asyncio.gather(
                *tuple(self.server._invocation_tasks.values()),
                return_exceptions=True,
            )
        await self.app.aclose()

    def _revision_id(self, workflow: Workflow) -> str:
        snapshot = self.app.register_workflow(workflow).workflow_snapshot
        return workflow_revision_id(
            snapshot.workflow_id,
            snapshot.definition_hash,
        )

    async def test_artifact_value_endpoint_scopes_lookup_to_invocation(self) -> None:
        invocation_id = uuid4()
        artifact_id = uuid4()
        expected = {
            "artifact": {
                "id": str(artifact_id),
                "kind": "runtime_value",
                "storage": "database",
            },
            "value": {"large": "value"},
        }
        loader = AsyncMock(return_value=expected)

        with patch.object(
            self.app.runtime_store,
            "aload_artifact_value",
            new=loader,
        ):
            observed = await self.artifact_value(invocation_id, artifact_id)

        self.assertEqual(expected, observed)
        loader.assert_awaited_once_with(
            invocation_id=invocation_id,
            artifact_id=artifact_id,
        )

    async def test_waiting_session_rejects_submit_before_new_admission(self) -> None:
        first = await self.submit(
            self.workflow_revision_id,
            InvocationSubmitRequest(
                session_key="same",
                input={"wait_key": "approval"},
            ),
        )
        await self._wait_for_state(first.invocation_id, "waiting")
        invocation_count = len(self.app.runtime_store.invocations)

        with self.assertRaises(HTTPException) as captured:
            await self.submit(
                self.workflow_revision_id,
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
            self.workflow_revision_id,
            InvocationSubmitRequest(
                input={"wait_key": "approval"},
            ),
        )
        await self._wait_for_state(submitted.invocation_id, "waiting")

        resumed = await self.resume(
            self.workflow_revision_id,
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

    async def test_server_rerun_submits_isolated_same_mode_invocation(self) -> None:
        submitted = await self.submit(
            self.workflow_revision_id,
            InvocationSubmitRequest(
                input={"wait_key": "approval"},
                session_key="rerun-source",
                event_mode="standard",
            ),
        )
        await self._wait_for_state(submitted.invocation_id, "waiting")
        await self.resume(
            self.workflow_revision_id,
            InvocationResumeRequest(
                session_key="rerun-source",
                wait_key="approval",
                output={"approved": True},
            ),
        )

        rerun = await self.rerun(
            self.workflow_revision_id,
            submitted.invocation_id,
        )
        candidate_id = UUID(rerun["candidate_invocation_id"])
        await self._wait_for_state(candidate_id, "waiting")

        self.assertEqual(str(submitted.invocation_id), rerun["source_invocation_id"])
        self.assertEqual("standard", rerun["event_mode"])
        self.assertNotEqual(str(submitted.session_id), rerun["session_id"])
        comparison = await self.compare(submitted.invocation_id, candidate_id)
        self.assertTrue(comparison["input_equal"])
        self.assertEqual("standard", comparison["evidence_mode"])

    async def test_submit_selects_event_mode_per_invocation(self) -> None:
        submitted = await self.submit(
            self.workflow_revision_id,
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

    async def test_background_failure_marks_invocation_failed_and_is_logged(
        self,
    ) -> None:
        with patch.object(
            self.app.workflow_executor,
            "ainvoke",
            new=AsyncMock(side_effect=RuntimeError("background failed")),
        ):
            submitted = await self.submit(
                self.workflow_revision_id,
                InvocationSubmitRequest(
                    session_key="background-failure",
                    input={"wait_key": "unused"},
                ),
            )
            task = self.server._invocation_tasks[submitted.invocation_id]
            with self.assertLogs(
                "autoagent.core.app.app",
                level="ERROR",
            ) as captured:
                result = await task
                await asyncio.sleep(0)

        self.assertEqual("failed", result.state)
        self.assertNotIn(
            submitted.invocation_id,
            self.server._invocation_tasks,
        )
        invocation = self.app.runtime_store.invocations[
            submitted.invocation_id
        ]
        self.assertEqual("failed", invocation.state)
        assert invocation.error is not None
        self.assertEqual(
            "INVOCATION_INFRASTRUCTURE_ERROR",
            invocation.error.code,
        )
        self.assertEqual("RuntimeError", invocation.error.detail["exception_type"])
        self.assertIn("background failed", "\n".join(captured.output))

    async def test_slow_async_operator_does_not_block_server_health(self) -> None:
        started = ThreadingEvent()

        async def slow_external_call() -> str:
            started.set()
            await asyncio.sleep(0.2)
            return "done"

        workflow = Workflow(id="server_slow_external_call")
        workflow.add_node(slow_external_call, node_id="slow_external_call")
        revision_id = self._revision_id(workflow)
        submitted = await self.submit(
            revision_id,
            InvocationSubmitRequest(entry_node_id="slow_external_call"),
        )
        for _ in range(100):
            if started.is_set():
                break
            await asyncio.sleep(0.005)
        self.assertTrue(started.is_set())
        health = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "") == "health"
        )

        response = await asyncio.wait_for(health(), timeout=0.05)

        self.assertEqual("ok", response["status"])
        self.assertIn(
            submitted.invocation_id,
            self.server._invocation_tasks,
        )
        await self._wait_for_state(submitted.invocation_id, "completed")

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
        self.assertIn(
            "/api/v1/workflow-revisions/{workflow_revision_id}/sessions",
            paths,
        )
        self.assertIn(
            "/api/v1/workflow-revisions/{workflow_revision_id}/invocations",
            paths,
        )
        self.assertIn(
            "/api/v1/workflow-revisions/{workflow_revision_id}/resume",
            paths,
        )
        self.assertIn("/api/v1/invocations/{invocation_id}/trace", paths)
        self.assertIn("/api/v1/invocations/{invocation_id}", paths)
        self.assertIn("/api/v1/invocations/{invocation_id}/stream", paths)
        self.assertIn(
            "/api/v1/invocations/{invocation_id}/user-events",
            paths,
        )
        self.assertIn(
            "/api/v1/invocations/{invocation_id}/user-events/stream",
            paths,
        )
        self.assertIn(
            "/api/v1/sessions/{session_id}/user-events/stream",
            paths,
        )
        self.assertIn("/api/v1/runtime/status", paths)
        self.assertIn("/api/v1/system/stream", paths)
        self.assertIn("/api/v1/health/live", paths)
        self.assertIn("/api/v1/health/ready", paths)

    async def test_system_stream_multiplexes_runtime_and_workflow_updates(
        self,
    ) -> None:
        endpoint = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "") == "stream_system_updates"
        )

        class ConnectedRequest:
            async def is_disconnected(self) -> bool:
                return False

        response = await endpoint(ConnectedRequest())
        first = await anext(response.body_iterator)
        await response.body_iterator.aclose()

        self.assertIn("event: runtime_status", first)
        self.assertIn("event: workflow_catalog_changed", first)
        self.assertIn("event: trace_directory_changed", first)

    async def test_system_stream_reports_new_server_invocation(self) -> None:
        endpoint = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "") == "stream_system_updates"
        )

        class ConnectedRequest:
            async def is_disconnected(self) -> bool:
                return False

        response = await endpoint(ConnectedRequest())
        await anext(response.body_iterator)
        next_chunk = asyncio.create_task(anext(response.body_iterator))
        await asyncio.sleep(0)

        await self.submit(
            self.workflow_revision_id,
            InvocationSubmitRequest(
                input={"wait_key": "remote"},
                session_key="remote-submit",
            ),
        )
        changed = await asyncio.wait_for(next_chunk, timeout=1)
        await response.body_iterator.aclose()

        self.assertIn("event: trace_directory_changed", changed)

    async def test_user_event_page_is_independent_from_runtime_events(
        self,
    ) -> None:
        submitted = await self.submit(
            self.workflow_revision_id,
            InvocationSubmitRequest(
                input={"wait_key": "approval"},
                session_key="user-events",
                event_mode="minimal",
            ),
        )
        await self._wait_for_state(submitted.invocation_id, "waiting")
        invocation = self.app.runtime_store.invocations[
            submitted.invocation_id
        ]
        execution = invocation.latest_node_execution("wait")
        session_notifications: list[str] = []
        session_id = self.app.runtime_store.invocation_sessions[invocation.id]
        unsubscribe = (
            self.app.runtime_store.subscribe_session_user_event_changes(
                session_id,
                lambda: session_notifications.append("changed"),
            )
        )
        self.app.runtime_store.record_user_event(
            invocation_id=invocation.id,
            spec=UserEventSpec(
                type="approval_requested",
                data={"wait_key": "approval"},
                node_id="wait",
                node_execution_id=execution.id,
                workflow_path=("approval_flow",),
            ),
        )
        self.app.runtime_store.record_user_event(
            invocation_id=invocation.id,
            spec=UserEventSpec(
                type="approval_status",
                data={"status": "still_waiting"},
                node_id="wait",
                node_execution_id=execution.id,
            ),
        )
        unsubscribe()

        page = await self.server.trace.user_event_page(
            invocation.id,
            after_sequence=0,
            limit=200,
        )

        self.assertEqual(page["live_sequence"], 2)
        self.assertFalse(page["has_later"])
        self.assertEqual(page["items"][0]["type"], "approval_requested")
        self.assertEqual(
            page["items"][0]["data"],
            {"wait_key": "approval"},
        )
        self.assertEqual(
            page["items"][0]["workflow_path"],
            ["approval_flow"],
        )
        self.assertEqual(
            self.app.runtime_store.runtime_events[invocation.id],
            [],
        )
        self.assertEqual(["changed"], session_notifications)

        endpoint = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "")
            == "stream_invocation_user_events"
        )

        class DisconnectingRequest:
            async def is_disconnected(self) -> bool:
                return False

        response = await endpoint(
            DisconnectingRequest(),
            invocation.id,
            0,
            None,
        )
        first = await anext(response.body_iterator)
        await response.body_iterator.aclose()

        self.assertIn("event: user_event", first)
        self.assertIn('"type":"approval_requested"', first)

    async def test_user_event_stream_waits_for_store_notification(
        self,
    ) -> None:
        submitted = await self.submit(
            self.workflow_revision_id,
            InvocationSubmitRequest(
                input={"wait_key": "approval"},
                session_key="notified-user-events",
                event_mode="minimal",
            ),
        )
        await self._wait_for_state(submitted.invocation_id, "waiting")
        invocation = self.app.runtime_store.invocations[
            submitted.invocation_id
        ]
        execution = invocation.latest_node_execution("wait")
        endpoint = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "")
            == "stream_invocation_user_events"
        )

        class ConnectedRequest:
            async def is_disconnected(self) -> bool:
                return False

        page_calls = 0
        original_page = self.server.trace.user_event_page

        async def counted_page(*args, **kwargs):
            nonlocal page_calls
            page_calls += 1
            return await original_page(*args, **kwargs)

        with patch.object(
            self.server.trace,
            "user_event_page",
            side_effect=counted_page,
        ):
            response = await endpoint(
                ConnectedRequest(),
                invocation.id,
                0,
                None,
            )
            next_event = asyncio.create_task(
                anext(response.body_iterator)
            )
            await asyncio.sleep(0.05)

            self.assertFalse(next_event.done())
            self.assertEqual(1, page_calls)

            self.app.runtime_store.record_user_event(
                invocation_id=invocation.id,
                spec=UserEventSpec(
                    type="approval_requested",
                    data={"wait_key": "approval"},
                    node_id="wait",
                    node_execution_id=execution.id,
                ),
            )
            event = await asyncio.wait_for(next_event, timeout=0.5)
            await response.body_iterator.aclose()

        self.assertIn("event: user_event", event)
        self.assertEqual(2, page_calls)

    async def test_user_event_stream_sends_terminal_event_before_stream_end(
        self,
    ) -> None:
        submitted = await self.submit(
            self.workflow_revision_id,
            InvocationSubmitRequest(
                input={"wait_key": "approval"},
                session_key="terminal-user-event",
                event_mode="minimal",
            ),
        )
        await self._wait_for_state(submitted.invocation_id, "waiting")
        invocation = self.app.runtime_store.invocations[
            submitted.invocation_id
        ]
        session_id = self.app.runtime_store.invocation_sessions[invocation.id]
        session = self.app.runtime_store.sessions[session_id]
        execution = invocation.latest_node_execution("wait")
        endpoint = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "")
            == "stream_invocation_user_events"
        )

        class ConnectedRequest:
            async def is_disconnected(self) -> bool:
                return False

        response = await endpoint(
            ConnectedRequest(),
            invocation.id,
            0,
            None,
        )
        first = asyncio.create_task(anext(response.body_iterator))
        await asyncio.sleep(0.02)

        invocation.mark_cancelled()
        await self.app.runtime_store.apersist_invocation_state(
            session,
            invocation,
        )
        self.assertFalse(
            first.done(),
            "A terminal state alone must not close the stream before final UserEvents.",
        )
        self.app.runtime_store.record_user_event(
            invocation_id=invocation.id,
            spec=UserEventSpec(
                type="message_aborted",
                data={
                    "error_type": "CancelledError",
                    "message": "cancelled",
                },
                node_id="wait",
                node_execution_id=execution.id,
            ),
        )

        terminal_event = await asyncio.wait_for(first, timeout=0.5)
        stream_end = await asyncio.wait_for(
            anext(response.body_iterator),
            timeout=0.5,
        )
        await response.body_iterator.aclose()

        self.assertIn('"type":"message_aborted"', terminal_event)
        self.assertIn("event: stream_end", stream_end)

    async def test_standalone_server_bounds_uvicorn_graceful_shutdown(
        self,
    ) -> None:
        with (
            patch("autoagent.core.server.app.uvicorn.Config") as config,
            patch(
                "autoagent.core.server.app._ShutdownAwareUvicornServer"
            ) as uvicorn_server,
        ):
            self.server.run(host="127.0.0.1", port=8765)

        config.assert_called_once_with(
            self.server.api,
            host="127.0.0.1",
            port=8765,
            reload=False,
            timeout_graceful_shutdown=5.0,
        )
        uvicorn_server.assert_called_once_with(
            config.return_value,
            self.server.request_shutdown,
        )
        uvicorn_server.return_value.run.assert_called_once_with()

    async def test_standalone_server_suppresses_post_shutdown_sigint(
        self,
    ) -> None:
        with (
            patch("autoagent.core.server.app.uvicorn.Config"),
            patch(
                "autoagent.core.server.app._ShutdownAwareUvicornServer"
            ) as uvicorn_server,
        ):
            uvicorn_server.return_value.run.side_effect = KeyboardInterrupt

            self.server.run(host="127.0.0.1", port=8765)

        uvicorn_server.return_value.run.assert_called_once_with()

    async def test_server_shutdown_callback_runs_on_serving_loop(self) -> None:
        app = AutoAgentApp()
        serving_loop = asyncio.get_running_loop()
        callback_loops: list[asyncio.AbstractEventLoop] = []

        async def close_host() -> None:
            callback_loops.append(asyncio.get_running_loop())
            await app.aclose()

        server = AutoAgentServer(app, shutdown_callback=close_host)
        await server.ashutdown()

        self.assertEqual([serving_loop], callback_loops)

    async def test_system_stream_stops_when_server_shutdown_is_requested(
        self,
    ) -> None:
        endpoint = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "") == "stream_system_updates"
        )

        class ConnectedRequest:
            async def is_disconnected(self) -> bool:
                return False

        response = await endpoint(ConnectedRequest())
        await anext(response.body_iterator)
        following = asyncio.create_task(anext(response.body_iterator))
        await asyncio.sleep(0)

        self.server.request_shutdown()

        with self.assertRaises(StopAsyncIteration):
            await asyncio.wait_for(following, timeout=0.5)
        await response.body_iterator.aclose()

    async def test_system_stream_stops_after_client_disconnect(
        self,
    ) -> None:
        endpoint = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "") == "stream_system_updates"
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
        wake_workflow = Workflow(id="wake_system_stream_disconnect_check")
        wake_workflow.add_node(dynamic_json_callable(lambda: None), node_id="node")
        self.app.register_workflow(wake_workflow)
        await asyncio.sleep(0)
        with self.assertRaises(StopAsyncIteration):
            await anext(response.body_iterator)
        self.assertEqual(2, request.poll_count)

    async def test_runtime_stream_waits_for_store_notification(self) -> None:
        submitted = await self.submit(
            self.workflow_revision_id,
            InvocationSubmitRequest(
                session_key="runtime-stream",
                input={"wait_key": "approval"},
            ),
        )
        await self._wait_for_state(submitted.invocation_id, "waiting")
        invocation = self.app.runtime_store.invocations[
            submitted.invocation_id
        ]
        endpoint = next(
            route.endpoint
            for route in self.server.router.routes
            if getattr(route, "name", "") == "stream_invocation"
        )

        class ConnectedRequest:
            async def is_disconnected(self) -> bool:
                return False

        response = await endpoint(
            ConnectedRequest(),
            submitted.invocation_id,
            after_sequence=invocation.event_sequence,
            last_event_id=None,
        )
        initial = await anext(response.body_iterator)
        self.assertIn("event: invocation_status", initial)
        following = asyncio.create_task(anext(response.body_iterator))
        await asyncio.sleep(0.02)
        self.assertFalse(following.done())

        await self.resume(
            self.workflow_revision_id,
            InvocationResumeRequest(
                session_key=submitted.session_key,
                wait_key="approval",
                output={"approved": True},
            ),
        )
        update = await asyncio.wait_for(following, timeout=0.5)
        await response.body_iterator.aclose()

        self.assertIn("event: runtime_event", update)

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
            self._revision_id(workflow),
            InvocationSubmitRequest(entry_node_id="slow"),
        )
        await self._wait_for_state(submitted.invocation_id, "running")

        response = await self.cancel(submitted.invocation_id)

        self.assertEqual(submitted.invocation_id, response.invocation_id)
        self.assertEqual("cancelled", response.state)

    async def test_waiting_invocation_can_be_cancelled_without_active_task(self) -> None:
        submitted = await self.submit(
            self.workflow_revision_id,
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
        other.add_node(dynamic_json_callable(lambda: "done"), node_id="done")
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

    async def test_trace_directory_routes_default_to_twenty_items(self) -> None:
        for route_name in (
            "list_workflows",
            "list_registered_workflows",
            "list_workflow_revisions",
            "list_sessions",
            "list_invocations",
        ):
            endpoint = next(
                route.endpoint
                for route in self.server.router.routes
                if getattr(route, "name", "") == route_name
            )
            default = inspect.signature(endpoint).parameters["limit"].default
            self.assertEqual(
                20,
                default.default,
                msg=f"{route_name} did not default to a 20-item page",
            )

    async def test_local_workflow_registration_notifies_directory_stream(
        self,
    ) -> None:
        changes: list[str] = []
        unsubscribe = self.app.runtime_store.subscribe_workflow_changes(
            lambda: changes.append("changed")
        )
        try:
            workflow = Workflow(id="registered_after_server_start")
            workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="done")
            self.app.register_workflow(workflow)
            # Re-registering the same immutable revision is not a directory
            # change and must not produce a duplicate notification.
            self.app.register_workflow(workflow)
        finally:
            unsubscribe()

        self.assertEqual(["changed"], changes)

    async def test_agent_invocation_neighbors_page_in_both_directions(
        self,
    ) -> None:
        workflow = Workflow(id="agent_neighbor_workflow")
        workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="done")
        self.app.register_workflow(workflow)
        created = [
            await self.app.ainvoke(
                workflow,
                session_id="agent-neighbors",
            )
            for _ in range(5)
        ]
        session_id = self.app.runtime_store.invocation_sessions[created[0].id]
        ordered = sorted(
            created,
            key=lambda value: (value.created_at_ms, str(value.id)),
        )
        anchor = ordered[2]

        older = await self.server.trace.list_invocation_neighbors(
            session_id,
            anchor_invocation_id=anchor.id,
            direction="older",
            limit=1,
        )
        newer = await self.server.trace.list_invocation_neighbors(
            session_id,
            anchor_invocation_id=anchor.id,
            direction="newer",
            limit=1,
        )

        self.assertEqual([str(ordered[1].id)], [
            item["id"] for item in older["items"]
        ])
        self.assertEqual([str(ordered[3].id)], [
            item["id"] for item in newer["items"]
        ])
        self.assertTrue(older["has_more"])
        self.assertTrue(newer["has_more"])

    async def test_trace_bootstrap_separates_latest_projection_from_events(
        self,
    ) -> None:
        submitted = await self.submit(
            self.workflow_revision_id,
            InvocationSubmitRequest(
                input={"wait_key": "trace"},
                session_key="trace",
                event_mode="full",
            ),
        )
        await self._wait_for_state(submitted.invocation_id, "waiting")
        for _ in range(1_000):
            events = await self.app.runtime_store.alist_runtime_events(
                invocation_id=submitted.invocation_id,
                limit=200,
            )
            if any(event.event_name == "wait.created" for event in events):
                break
            await asyncio.sleep(0.001)
        else:
            self.fail("wait.created was not recorded before trace bootstrap.")

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
            dynamic_json_callable(lambda value: {"answer": value + 1}),
            node_id="answer",
            input_mapping=lambda context: {
                "value": context.invocation_input["value"],
            },
        )
        self.app.register_workflow(workflow)
        submitted = await self.submit(
            self._revision_id(workflow),
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
                event_name="operator_call.failed",
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
                    "streaming": True,
                    "stream_chunk_count": 7,
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
                    "operator_id": "mapped",
                    "call_no": 3,
                    "unit_index": 0,
                    "unit_attempt_no": 1,
                    "state": "completed",
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
        projection = TraceProjectionReducer.apply(
            projection,
            RuntimeEvent(
                invocation_id=invocation_id,
                sequence=7,
                event_type="state_change",
                event_name="node.cancelled",
                subject_type="node",
                subject_id="worker",
                occurred_at_ms=7,
                status="cancelled",
                payload={
                    "node_id": "worker",
                    "node_execution_id": str(execution_id),
                    "state": "cancelled",
                    "operator_summary": {
                        "attempt_count": 3,
                        "success_count": 2,
                        "failure_count": 1,
                        "retry_count": 0,
                        "fallback_count": 1,
                        "timeout_count": 1,
                        "streaming_call_count": 1,
                        "stream_chunk_count": 7,
                    },
                    "parallel_summary": {"kind": "map", "call_count": 8},
                    "error": {
                        "code": "INVOCATION_FAILED_FAST",
                        "message": "Sibling branch failed.",
                    },
                },
            ),
        )

        self.assertEqual("cancelled", projection["nodes"]["worker"]["state"])
        self.assertEqual(
            "INVOCATION_FAILED_FAST",
            projection["nodes"]["worker"]["latest_error"]["code"],
        )
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
        fallback = next(
            call
            for call in execution["operator_calls"]
            if call["id"] == "fallback"
        )
        self.assertTrue(fallback["streaming"])
        self.assertEqual(7, fallback["stream_chunk_count"])
        self.assertEqual(
            1,
            projection["nodes"]["worker"]["streaming_call_count"],
        )
        self.assertEqual(
            7,
            projection["nodes"]["worker"]["stream_chunk_count"],
        )
        self.assertEqual(8, projection["nodes"]["worker"]["parallel_call_count"])
        self.assertEqual(7, projection["through_sequence"])

    async def test_projection_bounds_timeline_calls_without_losing_counts(
        self,
    ) -> None:
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
                subject_id="mapped",
                occurred_at_ms=1,
                status="running",
                payload={
                    "node_id": "mapped",
                    "node_execution_id": str(execution_id),
                },
            ),
        )
        for index in range(60):
            projection = TraceProjectionReducer.apply(
                projection,
                RuntimeEvent(
                    invocation_id=invocation_id,
                    sequence=index + 2,
                    event_type="operator_call",
                    event_name="operator_call.completed",
                    subject_type="operator_call",
                    subject_id=f"call-{index}",
                    occurred_at_ms=index + 2,
                    status="completed",
                    payload={
                        "node_id": "mapped",
                        "node_execution_id": str(execution_id),
                        "operator_call_id": f"call-{index}",
                        "operator_id": "mapped",
                        "kind": "map",
                        "call_no": index + 1,
                        "unit_index": index,
                        "unit_attempt_no": 1,
                        "started_at_ms": index + 1,
                        "state": "completed",
                    },
                ),
            )

        execution = projection["node_executions"][str(execution_id)]
        self.assertEqual(60, execution["operator_call_count"])
        self.assertEqual(50, len(execution["operator_calls"]))
        self.assertEqual("call-10", execution["operator_calls"][0]["id"])
        self.assertEqual("call-59", execution["operator_calls"][-1]["id"])

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
    async def test_workflow_directory_reads_fresh_keyset_page(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "workflow-refresh.db"
            reader_store = RuntimeStore(
                backend=DatabaseBackend.from_path(path)
            )
            reader = AutoAgentApp(runtime_store=reader_store)
            first = Workflow(id="first_revision")
            first.add_node(dynamic_json_callable(lambda: "first"), node_id="first")
            reader.register_workflow(first)
            await reader.astart()
            server = AutoAgentServer(reader)
            try:
                initial = await server.trace.list_workflows(
                    cursor=None,
                    limit=20,
                )
                self.assertEqual(
                    ["first_revision"],
                    [item["workflow_id"] for item in initial["items"]],
                )

                writer_store = RuntimeStore(
                    backend=DatabaseBackend.from_path(path)
                )
                writer = AutoAgentApp(runtime_store=writer_store)
                await writer.astart()
                second = Workflow(id="later_revision")
                second.add_node(dynamic_json_callable(lambda: "second"), node_id="second")
                writer.register_workflow(second)
                try:
                    await writer_store.aflush()
                finally:
                    await writer.aclose()

                current = await server.trace.list_workflows(
                    cursor=None,
                    limit=20,
                )
                self.assertEqual(
                    {"first_revision", "later_revision"},
                    {item["workflow_id"] for item in current["items"]},
                )
                refreshed = await server.trace.list_workflows(
                    cursor=None,
                    limit=20,
                )
                self.assertEqual(
                    {"first_revision", "later_revision"},
                    {item["workflow_id"] for item in refreshed["items"]},
                )
            finally:
                await reader.aclose()

    async def test_workflow_directory_does_not_drain_database_pages(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "workflow-pages.db"
            store = RuntimeStore(backend=DatabaseBackend.from_path(path))
            app = AutoAgentApp(runtime_store=store)
            for index in range(4):
                workflow = Workflow(id=f"workflow_{index}")
                workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="done")
                app.register_workflow(workflow)
            await app.astart()
            await store.aflush()
            server = AutoAgentServer(app)
            backend = store.backend
            assert backend is not None
            original = backend.alist_trace_workflow_versions
            calls: list[dict[str, object]] = []

            async def traced(**kwargs):
                if not backend._database_loop.is_current():
                    calls.append(dict(kwargs))
                return await original(**kwargs)

            backend.alist_trace_workflow_versions = traced
            try:
                page = await server.trace.list_workflows(
                    cursor=None,
                    limit=1,
                )
                self.assertTrue(page["has_more"])
                self.assertEqual(1, len(page["items"]))
                self.assertEqual(1, len(calls))
                self.assertEqual(6, calls[0]["limit"])
                self.assertIsNone(calls[0]["before"])
            finally:
                await app.aclose()

    async def test_workflow_graph_uses_direct_revision_lookup(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "workflow-lookup.db"
            writer_store = RuntimeStore(
                backend=DatabaseBackend.from_path(path)
            )
            writer = AutoAgentApp(runtime_store=writer_store)
            workflow = Workflow(id="direct_lookup")
            workflow.add_node(dynamic_json_callable(lambda: "done"), node_id="done")
            writer.register_workflow(workflow)
            await writer.astart()
            await writer_store.aflush()
            entry = next(iter(writer.workflow_registry.values()))
            revision_id = workflow_revision_id(
                entry.workflow_snapshot.workflow_id,
                entry.workflow_snapshot.definition_hash,
            )
            await writer.aclose()

            reader_store = RuntimeStore(
                backend=DatabaseBackend.from_path(path)
            )
            reader = AutoAgentApp(runtime_store=reader_store)
            await reader.astart()
            server = AutoAgentServer(reader)
            backend = reader_store.backend
            assert backend is not None

            async def fail_list(**kwargs):
                self.fail(
                    "Graph lookup attempted to list Workflow revisions."
                )

            backend.alist_trace_workflow_versions = fail_list
            try:
                graph = await server.trace.workflow_graph(revision_id)
                self.assertEqual("direct_lookup", graph["workflow_id"])
                self.assertEqual(["done"], [
                    node["id"] for node in graph["nodes"]
                ])
            finally:
                await reader.aclose()

    async def test_historical_trace_reads_pydantic_values_as_json(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "unregistered-model.db"
            first_store = RuntimeStore(
                backend=DatabaseBackend.from_path(path)
            )
            first = AutoAgentApp(runtime_store=first_store)
            workflow = Workflow(id="historical_model")
            workflow.add_node(
                historical_trace_model,
                node_id="model",
            )
            first.register_workflow(workflow)
            await first.astart()
            invocation = await first.ainvoke(
                workflow,
                session_id="history",
                event_mode="full",
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
                bootstrap = await server.trace.trace_bootstrap(
                    invocation_id,
                    tail_limit=20,
                )
                page = await server.trace.event_page(
                    invocation_id,
                    after_sequence=0,
                    before_sequence=None,
                    limit=200,
                )
                output_details = [
                    await server.trace.event_detail(
                        invocation_id,
                        item["sequence"],
                    )
                    for item in page["items"]
                    if item["has_output"]
                ]
                self.assertEqual(
                    "historical_model",
                    bootstrap["workflow"]["workflow_id"],
                )
                self.assertEqual(
                    {"value": 42},
                    next(
                        detail["output"]
                        for detail in output_details
                        if detail["output"] == {"value": 42}
                    ),
                )
                state = await server.trace.runtime_state(
                    invocation_id,
                    through_sequence=bootstrap["invocation"][
                        "live_sequence"
                    ],
                )
                self.assertEqual(
                    {"output": {"value": 42}},
                    state["invocation"]["result"],
                )
            finally:
                await second.aclose()

    async def test_directory_keeps_registered_and_historical_revisions_separate(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "revisions.db"

            first_store = RuntimeStore(backend=DatabaseBackend.from_path(path))
            first = AutoAgentApp(runtime_store=first_store)
            first_workflow = Workflow(id="revision_history")
            first_workflow.add_node(dynamic_json_callable(lambda: "first"), node_id="first")
            first.register_workflow(first_workflow)
            await first.astart()
            first_invocation = await first.ainvoke(
                first_workflow,
                session_id="shared-key",
            )
            await first_store.aflush()
            first_revision_id = first_invocation.workflow_revision_id
            await first.aclose()

            second_store = RuntimeStore(backend=DatabaseBackend.from_path(path))
            second = AutoAgentApp(runtime_store=second_store)
            second_workflow = Workflow(id="revision_history")
            second_workflow.add_node(dynamic_json_callable(lambda: "second"), node_id="second")
            second.register_workflow(second_workflow)
            await second.astart()
            try:
                second_invocation = await second.ainvoke(
                    second_workflow,
                    session_id="shared-key",
                )
                await second_store.aflush()
                second_revision_id = second_invocation.workflow_revision_id
                server = AutoAgentServer(second)

                directory_page = await server.trace.list_workflows(
                    cursor=None,
                    limit=10,
                )
                revisions = {
                    item["revision_id"]: item
                    for item in directory_page["items"]
                }
                self.assertEqual(
                    {first_revision_id, second_revision_id},
                    set(revisions),
                )
                self.assertFalse(revisions[first_revision_id]["registered"])
                self.assertTrue(revisions[second_revision_id]["registered"])

                first_revision_page = (
                    await server.trace.list_workflow_versions(
                        "revision_history",
                        cursor=None,
                        limit=1,
                    )
                )
                second_revision_page = (
                    await server.trace.list_workflow_versions(
                        "revision_history",
                        cursor=first_revision_page["next_cursor"],
                        limit=1,
                    )
                )
                self.assertEqual(
                    {first_revision_id, second_revision_id},
                    {
                        first_revision_page["items"][0]["revision_id"],
                        second_revision_page["items"][0]["revision_id"],
                    },
                )
                self.assertTrue(first_revision_page["has_more"])
                self.assertFalse(second_revision_page["has_more"])

                first_sessions = await server.trace.list_sessions(
                    first_revision_id,
                    cursor=None,
                    limit=10,
                )
                second_sessions = await server.trace.list_sessions(
                    second_revision_id,
                    cursor=None,
                    limit=10,
                )
                self.assertEqual(1, len(first_sessions["items"]))
                self.assertEqual(1, len(second_sessions["items"]))
                self.assertNotEqual(
                    first_sessions["items"][0]["id"],
                    second_sessions["items"][0]["id"],
                )
                self.assertEqual(
                    first_revision_id,
                    first_sessions["items"][0]["workflow_revision_id"],
                )
                self.assertEqual(
                    second_revision_id,
                    second_sessions["items"][0]["workflow_revision_id"],
                )

                historical_graph = await server.trace.workflow_graph(
                    first_revision_id
                )
                current_graph = await server.trace.workflow_graph(
                    second_revision_id
                )
                self.assertEqual(
                    ["first"],
                    [node["id"] for node in historical_graph["nodes"]],
                )
                self.assertEqual(
                    ["second"],
                    [node["id"] for node in current_graph["nodes"]],
                )
            finally:
                await second.aclose()

    async def test_historical_trace_keeps_exact_graph_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "trace.db"
            first_store = RuntimeStore(
                backend=DatabaseBackend.from_path(path)
            )
            first = AutoAgentApp(runtime_store=first_store)
            workflow = Workflow(id="historical_trace")
            workflow.add_node(dynamic_json_callable(lambda: {"answer": 42}), node_id="answer")
            first.register_workflow(workflow)
            await first.astart()
            invocation = await first.ainvoke(
                workflow,
                session_id="history",
                event_mode="standard",
            )
            first_store.record_user_event(
                invocation_id=invocation.id,
                spec=UserEventSpec(
                    type="agent_output",
                    data={"output": {"answer": 42}},
                    node_id="answer",
                    node_execution_id=uuid4(),
                ),
            )
            await first_store.aflush()
            invocation_id = invocation.id
            revision_id = invocation.workflow_revision_id
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
                    revision_id,
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
                self.assertEqual(revision_id, workflows["items"][0]["revision_id"])
                self.assertFalse(workflows["items"][0]["registered"])
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
                user_page = await server.trace.user_event_page(
                    invocation_id,
                    after_sequence=0,
                    limit=200,
                )
                self.assertEqual(
                    ["agent_output"],
                    [item["type"] for item in user_page["items"]],
                )
                self.assertEqual(
                    {"output": {"answer": 42}},
                    user_page["items"][0]["data"],
                )
            finally:
                await second.aclose()
