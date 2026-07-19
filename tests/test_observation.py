from __future__ import annotations

import asyncio
import inspect
import json
import unittest
from typing import Any
from uuid import UUID

from autoagent import AutoAgentApp, AutoAgentServer
from autoagent.core.trace import TraceQueryService, project_runtime_events
from autoagent.core.runtime import ArtifactRef, InMemoryRuntimeStore, LoggingEventSink
from autoagent.core.workflow import SystemCommand, Workflow


def prepare(message: str) -> dict[str, str]:
    return {"text": message.upper()}


def finish(text: str) -> str:
    return f"done:{text}"


def reveal_secret(password: str) -> dict[str, str]:
    return {"token": password, "status": "accepted"}


def create_artifact(uri: str) -> ArtifactRef:
    return ArtifactRef(uri=uri, media_type="image/png", size_bytes=42)


class RuntimeEventTests(unittest.TestCase):
    def test_completed_invocation_emits_ordered_timeline_events(self) -> None:
        workflow = Workflow(id="observed_chain", name="Observed chain")
        workflow.add_node(prepare, node_id="prepare")
        workflow.add_node(finish, node_id="finish")
        workflow.add_edge("prepare", "finish")
        app = AutoAgentApp()

        invocation = app.invoke(
            workflow,
            input={"message": "hello"},
            session_id="trace-session",
        )
        session = app.runtime_store.find_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="trace-session",
        )
        assert session is not None
        events = asyncio.run(
            app.runtime_store.alist_runtime_events(
                session_id=session.id,
                invocation_id=invocation.id,
                limit=10_000,
            )
        )

        self.assertGreater(len(events), 8)
        self.assertEqual(
            list(range(1, len(events) + 1)),
            [event.sequence for event in events],
        )
        self.assertTrue(all(isinstance(event.occurred_at_ms, int) for event in events))
        types = {event.type for event in events}
        self.assertIn("invocation.created", types)
        self.assertIn("node.execution_created", types)
        self.assertIn("operator.call_started", types)
        self.assertIn("operator.call_finished", types)
        operator_events_by_call: dict[str, list[str]] = {}
        for event in events:
            if event.entity_type == "operator_call" and event.entity_id is not None:
                operator_events_by_call.setdefault(event.entity_id, []).append(event.type)
        self.assertTrue(operator_events_by_call)
        for call_events in operator_events_by_call.values():
            self.assertEqual(
                ["operator.call_started", "operator.call_finished"],
                call_events,
            )
        self.assertIn("edge.evaluated", types)
        output_events = [event for event in events if event.channel == "output"]
        self.assertEqual(1, len(output_events))
        self.assertEqual("invocation.output_published", output_events[0].type)
        self.assertEqual("user", output_events[0].visibility)
        self.assertEqual({"result": invocation.result}, output_events[0].payload)

        projection = project_runtime_events(invocation.id, events)
        self.assertEqual("completed", projection.invocation_state)
        self.assertEqual("completed", projection.nodes["prepare"].state)
        self.assertEqual("completed", projection.nodes["finish"].state)
        selected_edges = [edge for edge in projection.edges.values() if edge.selected]
        self.assertEqual(1, len(selected_edges))

    def test_projection_cursor_excludes_future_node_completion(self) -> None:
        workflow = Workflow(id="projection_cursor")
        workflow.add_node(prepare, node_id="prepare")
        app = AutoAgentApp()
        invocation = app.invoke(workflow, input={"message": "hello"}, session_id="s")
        session = app.runtime_store.find_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="s",
        )
        assert session is not None
        events = asyncio.run(
            app.runtime_store.alist_runtime_events(
                session_id=session.id,
                invocation_id=invocation.id,
                limit=10_000,
            )
        )
        running = next(
            event
            for event in events
            if event.type == "node.state_changed"
            and event.payload.get("to") == "running"
        )

        projection = project_runtime_events(
            invocation.id,
            events,
            through_sequence=running.sequence,
        )

        self.assertEqual("running", projection.nodes["prepare"].state)
        self.assertIsNone(
            projection.node_executions[
                projection.nodes["prepare"].latest_execution_id
            ].output
        )


class TraceQueryServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_bootstrap_uses_bounded_tail_and_persistent_projection_checkpoint(
        self,
    ) -> None:
        store = InMemoryRuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        workflow = Workflow(id="bounded_observation")
        workflow.add_node(prepare, node_id="prepare")
        workflow.add_node(finish, node_id="finish")
        workflow.add_edge("prepare", "finish")
        invocation = await app.ainvoke(
            workflow,
            input={"message": "incident"},
            session_id="bounded",
        )
        session = await store.afind_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="bounded",
        )
        assert session is not None
        service = TraceQueryService(
            store,
            bootstrap_event_limit=3,
            event_page_size=2,
        )

        first = await service.bootstrap(
            session_id=session.id,
            invocation_id=invocation.id,
        )
        stored = await store.aload_projection_checkpoint(
            invocation_id=invocation.id
        )
        page = await service.list_event_page(
            session_id=session.id,
            invocation_id=invocation.id,
            limit=2,
        )
        second = await service.bootstrap(
            session_id=session.id,
            invocation_id=invocation.id,
        )

        self.assertLessEqual(len(first.events), 3)
        self.assertGreater(first.checkpoint.through_sequence, 0)
        self.assertEqual("completed", first.projection.invocation_state)
        self.assertIsNotNone(stored)
        self.assertEqual(first.checkpoint.through_sequence, stored[0])
        self.assertEqual(first.checkpoint, second.checkpoint)
        self.assertEqual(first.events, second.events)
        self.assertEqual(2, len(page.events))
        self.assertTrue(page.has_more)
        self.assertEqual(page.events[-1].sequence, page.next_after_sequence)

        all_events = await store.alist_runtime_events(
            session_id=session.id,
            invocation_id=invocation.id,
            limit=10_000,
        )
        reverse_page = await service.list_event_page(
            session_id=session.id,
            invocation_id=invocation.id,
            before_sequence=all_events[-1].sequence + 1,
            limit=2,
        )
        previous_page = await service.list_event_page(
            session_id=session.id,
            invocation_id=invocation.id,
            before_sequence=reverse_page.previous_before_sequence,
            limit=2,
        )
        self.assertEqual(all_events[-2:], reverse_page.events)
        self.assertEqual(
            all_events[-4:-2],
            previous_page.events,
        )

    async def test_runtime_payloads_are_redacted_without_changing_store_data(self) -> None:
        store = InMemoryRuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        workflow = Workflow(id="redacted_observation")
        workflow.add_node(reveal_secret, node_id="secret")
        invocation = await app.ainvoke(
            workflow,
            input={"password": "sensitive-value"},
            session_id="redacted",
        )
        session = await store.afind_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="redacted",
        )
        assert session is not None

        view = await TraceQueryService(store).bootstrap(
            session_id=session.id,
            invocation_id=invocation.id,
        )

        rendered = json.dumps(view.model_dump(mode="json"))
        self.assertNotIn("sensitive-value", rendered)
        self.assertEqual("[REDACTED]", view.invocation.input["password"])
        self.assertEqual(
            "[REDACTED]",
            view.invocation.node_executions[0].output["token"],
        )
        persisted = await store.aload_invocation(invocation.id)
        self.assertEqual("sensitive-value", persisted.input["password"])

    async def test_artifact_reference_keeps_ui_type_marker(self) -> None:
        store = InMemoryRuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        workflow = Workflow(id="artifact_observation")
        workflow.add_node(create_artifact, node_id="artifact")
        invocation = await app.ainvoke(
            workflow,
            input={"uri": "artifact://images/preview.png"},
            session_id="artifact",
        )
        session = await store.afind_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="artifact",
        )
        assert session is not None

        detail = await TraceQueryService(store).get_invocation(
            session_id=session.id,
            invocation_id=invocation.id,
        )

        output = detail.node_executions[0].output
        self.assertEqual(
            "artifact://images/preview.png",
            output["__autoagent_artifact__"]["uri"],
        )

    async def test_bootstrap_restores_graph_details_timeline_and_projection(self) -> None:
        store = InMemoryRuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        workflow = Workflow(id="observation_bootstrap", name="Incident workflow")
        workflow.add_node(prepare, node_id="prepare")
        workflow.add_node(finish, node_id="finish")
        workflow.add_edge("prepare", "finish")
        invocation = await app.ainvoke(
            workflow,
            input={"message": "incident"},
            session_id="incident-1",
        )
        session = await store.afind_session(
            namespace="default",
            workflow_id=workflow.id,
            session_key="incident-1",
        )
        assert session is not None
        service = TraceQueryService(store)

        view = await service.bootstrap(
            session_id=session.id,
            invocation_id=invocation.id,
        )

        self.assertEqual("Incident workflow", view.graph.name)
        self.assertEqual({"prepare", "finish"}, {node.id for node in view.graph.nodes})
        self.assertEqual(2, len(view.invocation.node_executions))
        self.assertEqual(4, len(view.timeline.spans))
        self.assertEqual("completed", view.projection.invocation_state)
        self.assertGreater(len(view.events), 0)

    async def test_autoagent_server_builds_trace_and_execution_api(self) -> None:
        app = AutoAgentApp(runtime_store=InMemoryRuntimeStore())
        workflow = Workflow(id="registered_for_trace")
        workflow.add_node(prepare, node_id="prepare")
        app.register_workflow(workflow)
        server = AutoAgentServer(app)
        routes = [route for route in server.api.routes if hasattr(route, "path")]
        paths = {route.path for route in routes}

        self.assertIn("/api/workflows/{workflow_id}/invocations", paths)
        self.assertIn("/api/workflows", paths)
        self.assertIn("/api/registered-workflows", paths)
        self.assertIn("/api/sessions", paths)
        self.assertIn(
            "/api/sessions/{session_id}/invocations/{invocation_id}/stream",
            paths,
        )
        stream_route = next(
            route
            for route in routes
            if route.path
            == "/api/sessions/{session_id}/invocations/{invocation_id}/stream"
        )
        self.assertTrue(inspect.isasyncgenfunction(stream_route.endpoint))
        self.assertTrue(stream_route.is_sse_stream)

        protected_route = next(
            route for route in routes if route.path == "/api/workflows"
        )
        health_route = next(
            route for route in routes if route.path == "/api/health"
        )
        self.assertGreater(len(protected_route.dependant.dependencies), 0)
        self.assertEqual(0, len(health_route.dependant.dependencies))

        workflow_response = await _asgi_request(server.api, "GET", "/api/workflows")
        self.assertEqual(200, workflow_response[0])
        self.assertEqual(
            "registered_for_trace",
            json.loads(workflow_response[2])[0]["workflow_id"],
        )

        registered_response = await _asgi_request(
            server.api,
            "GET",
            "/api/registered-workflows",
        )
        self.assertEqual(200, registered_response[0])
        self.assertEqual(
            ["registered_for_trace"],
            [item["workflow_id"] for item in json.loads(registered_response[2])],
        )

        with self.assertRaisesRegex(ValueError, "cannot be empty"):
            AutoAgentServer(
                AutoAgentApp(runtime_store=InMemoryRuntimeStore()),
                access_token="",
            )

    async def test_server_keeps_historical_snapshots_out_of_registered_workflows(
        self,
    ) -> None:
        store = InMemoryRuntimeStore()
        historical_app = AutoAgentApp(runtime_store=store)
        historical = Workflow(id="revision_directory", version=1)
        historical.add_node(prepare, node_id="prepare")
        historical_entry = historical_app.register_workflow(historical)
        await store.asave_workflow_snapshot(
            historical_app.namespace,
            historical_entry.workflow_snapshot,
        )

        current_app = AutoAgentApp(runtime_store=store)
        current = Workflow(id="revision_directory", version=2)
        current.add_node(prepare, node_id="prepare")
        current.add_node(finish, node_id="finish")
        current.add_edge("prepare", "finish")
        current_entry = current_app.register_workflow(current)
        server = AutoAgentServer(current_app)

        history_response = await _asgi_request(server.api, "GET", "/api/workflows")
        registered_response = await _asgi_request(
            server.api,
            "GET",
            "/api/registered-workflows",
        )

        self.assertEqual(200, history_response[0])
        self.assertEqual(200, registered_response[0])
        history = json.loads(history_response[2])
        registered = json.loads(registered_response[2])
        self.assertEqual(2, len(history))
        self.assertEqual(
            {historical_entry.workflow_snapshot.definition_hash, current_entry.workflow_snapshot.definition_hash},
            {item["definition_hash"] for item in history},
        )
        self.assertEqual(
            [current_entry.workflow_snapshot.definition_hash],
            [item["definition_hash"] for item in registered],
        )

    async def test_server_token_authentication_uses_http_cookie(self) -> None:
        server = AutoAgentServer(
            AutoAgentApp(runtime_store=InMemoryRuntimeStore()),
            access_token="trace-secret",
        )

        health = await _asgi_request(server.api, "GET", "/api/health")
        denied = await _asgi_request(server.api, "GET", "/api/workflows")
        rejected = await _asgi_request(
            server.api,
            "POST",
            "/api/auth/session",
            json_body={"token": "wrong"},
        )
        accepted = await _asgi_request(
            server.api,
            "POST",
            "/api/auth/session",
            json_body={"token": "trace-secret"},
        )
        cookie = next(
            value
            for name, value in accepted[1]
            if name.lower() == b"set-cookie"
        ).split(b";", 1)[0]
        authorized = await _asgi_request(
            server.api,
            "GET",
            "/api/workflows",
            headers=((b"cookie", cookie),),
        )

        self.assertEqual(200, health[0])
        self.assertFalse(json.loads(health[2])["authenticated"])
        self.assertEqual(401, denied[0])
        self.assertEqual(401, rejected[0])
        self.assertEqual(200, accepted[0])
        self.assertEqual(200, authorized[0])

    async def test_server_can_submit_registered_workflow_in_background(self) -> None:
        store = InMemoryRuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        workflow = Workflow(id="server_submit")
        workflow.add_node(prepare, node_id="prepare")
        app.register_workflow(workflow)
        server = AutoAgentServer(app)

        response = await _asgi_request(
            server.api,
            "POST",
            "/api/workflows/server_submit/invocations",
            json_body={"session_id": "ui-session", "input": {"message": "hello"}},
        )

        self.assertEqual(200, response[0])
        payload = json.loads(response[2])
        self.assertEqual("server_submit", payload["workflow_id"])
        invocation_id = payload["invocation_id"]
        for _ in range(50):
            invocation = await store.aload_invocation(UUID(invocation_id))
            if invocation is not None and invocation.state == "completed":
                break
            await asyncio.sleep(0.01)
        assert invocation is not None
        self.assertEqual("completed", invocation.state)

    async def test_server_can_resume_waiting_invocation(self) -> None:
        store = InMemoryRuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        workflow = Workflow(id="server_resume")
        workflow.add_node(SystemCommand(id="wait"), node_id="approval")
        workflow.add_node(finish, node_id="finish")
        workflow.add_edge("approval", "finish")
        app.register_workflow(workflow)
        server = AutoAgentServer(app)

        submitted_response = await _asgi_request(
            server.api,
            "POST",
            "/api/workflows/server_resume/invocations",
            json_body={
                "session_id": "review-session",
                "input": {
                    "wait_key": "approval:ticket-1",
                    "wait_type": "human",
                    "payload": {"ticket": "ticket-1"},
                },
            },
        )

        self.assertEqual(200, submitted_response[0])
        submitted_payload = json.loads(submitted_response[2])
        invocation_id = UUID(submitted_payload["invocation_id"])
        for _ in range(50):
            invocation = await store.aload_invocation(invocation_id)
            if invocation is not None and invocation.state == "waiting":
                break
            await asyncio.sleep(0.01)
        assert invocation is not None
        self.assertEqual("waiting", invocation.state)
        self.assertIn("approval:ticket-1", invocation.scheduler.waiting_executions)

        resumed_response = await _asgi_request(
            server.api,
            "POST",
            "/api/workflows/server_resume/resume",
            json_body={
                "session_id": "review-session",
                "wait_key": "approval:ticket-1",
                "output": {"text": "accepted"},
            },
        )

        self.assertEqual(200, resumed_response[0])
        resumed_payload = json.loads(resumed_response[2])
        self.assertEqual("server_resume", resumed_payload["workflow_id"])
        self.assertEqual(str(invocation_id), resumed_payload["invocation_id"])
        self.assertEqual("completed", resumed_payload["state"])
        completed = await store.aload_invocation(invocation_id)
        assert completed is not None
        self.assertEqual("completed", completed.state)
        self.assertEqual({"output": "done:accepted"}, completed.result)
        events = await store.alist_runtime_events(
            invocation_id=invocation_id,
            limit=10_000,
        )
        event_types = [event.type for event in events]
        self.assertIn("invocation.state_changed", event_types)
        self.assertIn("invocation.output_published", event_types)

    async def test_logging_event_sink_receives_persisted_events(self) -> None:
        class CaptureSink(LoggingEventSink):
            def __init__(self) -> None:
                super().__init__()
                self.events = []

            async def aemit(self, events):
                self.events.extend(events)

        sink = CaptureSink()
        store = InMemoryRuntimeStore(event_sinks=(sink,))
        app = AutoAgentApp(runtime_store=store)
        workflow = Workflow(id="sink_events")
        workflow.add_node(prepare, node_id="prepare")

        await app.ainvoke(workflow, input={"message": "hello"}, session_id="sink")

        self.assertTrue(sink.events)
        self.assertIn("invocation.created", {event.type for event in sink.events})


async def _asgi_request(
    application: Any,
    method: str,
    path: str,
    *,
    json_body: dict[str, Any] | None = None,
    headers: tuple[tuple[bytes, bytes], ...] = (),
) -> tuple[int, list[tuple[bytes, bytes]], bytes]:
    """Exercise FastAPI's real ASGI HTTP boundary without an HTTP client dependency."""

    body = json.dumps(json_body).encode() if json_body is not None else b""
    request_headers = list(headers)
    if json_body is not None:
        request_headers.append((b"content-type", b"application/json"))
    messages: list[dict[str, Any]] = []
    request_sent = False

    async def receive() -> dict[str, Any]:
        nonlocal request_sent
        if request_sent:
            return {"type": "http.disconnect"}
        request_sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    await application(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path,
            "raw_path": path.encode(),
            "query_string": b"",
            "root_path": "",
            "headers": request_headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "state": {},
        },
        receive,
        send,
    )
    start = next(message for message in messages if message["type"] == "http.response.start")
    response_body = b"".join(
        message.get("body", b"")
        for message in messages
        if message["type"] == "http.response.body"
    )
    return start["status"], start["headers"], response_body


if __name__ == "__main__":
    unittest.main()
