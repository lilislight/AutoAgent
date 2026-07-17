from __future__ import annotations

import asyncio
import inspect
import json
import unittest

from autoagent import AutoAgentApp, ObservationApp
from autoagent.observer import ObservationService, project_runtime_events
from autoagent.runtime import ArtifactRef, InMemoryRuntimeStore
from autoagent.workflow import Workflow


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


class ObservationServiceTests(unittest.IsolatedAsyncioTestCase):
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
        service = ObservationService(
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

        view = await ObservationService(store).bootstrap(
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

        detail = await ObservationService(store).get_invocation(
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
        service = ObservationService(store)

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

    async def test_observation_app_builds_read_only_api(self) -> None:
        observation = ObservationApp(InMemoryRuntimeStore())
        routes = [route for route in observation.api.routes if hasattr(route, "path")]
        paths = {route.path for route in routes}

        self.assertIn("/api/workflows", paths)
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

        with self.assertRaisesRegex(ValueError, "cannot be empty"):
            ObservationApp(InMemoryRuntimeStore(), access_token="")


if __name__ == "__main__":
    unittest.main()
