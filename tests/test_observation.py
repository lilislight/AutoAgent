from __future__ import annotations

import asyncio
import inspect
import unittest

from autoagent import AutoAgentApp, ObservationApp
from autoagent.observer import ObservationService, project_runtime_events
from autoagent.runtime import InMemoryRuntimeStore
from autoagent.workflow import Workflow


def prepare(message: str) -> dict[str, str]:
    return {"text": message.upper()}


def finish(text: str) -> str:
    return f"done:{text}"


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
        paths = {route.path for route in observation.api.routes}

        self.assertIn("/api/workflows", paths)
        self.assertIn("/api/sessions", paths)
        self.assertIn(
            "/api/sessions/{session_id}/invocations/{invocation_id}/stream",
            paths,
        )
        stream_route = next(
            route
            for route in observation.api.routes
            if route.path
            == "/api/sessions/{session_id}/invocations/{invocation_id}/stream"
        )
        self.assertTrue(inspect.isasyncgenfunction(stream_route.endpoint))
        self.assertTrue(stream_route.is_sse_stream)


if __name__ == "__main__":
    unittest.main()
