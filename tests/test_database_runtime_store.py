from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from sqlalchemy import text

from autoagent import (
    AutoAgentApp,
    DatabaseRuntimeStore,
    EdgePolicy,
    MapPolicy,
    NodePolicy,
    RecoveryPolicy,
    SystemCommand,
    Workflow,
)
from autoagent.core.runtime import InMemoryRuntimeStore, Invocation


class DatabaseRuntimeStoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "runtime.db"
        self.store = DatabaseRuntimeStore.from_path(self.path)

    async def asyncTearDown(self) -> None:
        await self.store.aclose()
        self.directory.cleanup()

    async def test_v1_schema_has_events_and_snapshots_without_execution_tables(self) -> None:
        await self.store.ainitialize()

        async def table_names() -> set[str]:
            async with self.store.engine.connect() as connection:
                rows = await connection.execute(
                    text("SELECT name FROM sqlite_master WHERE type = 'table'")
                )
                return set(rows.scalars())

        names = await self.store._database_loop.arun(table_names())
        self.assertIn("runtime_events", names)
        self.assertIn("runtime_snapshots", names)
        self.assertNotIn("node_executions", names)
        self.assertNotIn("operator_calls", names)

    async def test_sqlite_round_trip_rebuilds_from_genesis_and_boundary_events(self) -> None:
        workflow = Workflow(id="database_round_trip")
        workflow.add_node(lambda value: value + 1, node_id="increment")
        app = AutoAgentApp(runtime_store=self.store)
        invocation = await app.ainvoke(
            workflow,
            input={"value": 1},
            session_id="session",
        )
        await app.aclose()

        reopened = DatabaseRuntimeStore.from_path(self.path)
        self.store = reopened
        loaded = await reopened.aload_invocation(invocation.id)
        events = await reopened.alist_runtime_events(
            invocation_id=invocation.id,
            limit=10_000,
        )
        _, rebuilt = await reopened.arebuild_execution(invocation.id)
        sessions = await reopened.alist_sessions(workflow_id=workflow.id)
        workflows = await reopened.alist_workflow_snapshots(workflow_id=workflow.id)

        self.assertEqual("completed", loaded.state)
        self.assertEqual(invocation.result, rebuilt.result)
        self.assertEqual(
            list(range(1, len(events) + 1)),
            [event.sequence for event in events],
        )
        self.assertTrue(any(event.role == "boundary" for event in events))
        self.assertEqual(1, len(sessions))
        self.assertEqual(1, len(workflows))

    async def test_sequences_restart_at_one_for_each_invocation(self) -> None:
        workflow = Workflow(id="per_invocation_sequence")
        workflow.add_node(lambda: "ok", node_id="node")
        app = AutoAgentApp(runtime_store=self.store)
        first = await app.ainvoke(workflow, session_id="same")
        second = await app.ainvoke(workflow, session_id="same")
        first_events = await self.store.alist_runtime_events(
            invocation_id=first.id,
            limit=10_000,
        )
        second_events = await self.store.alist_runtime_events(
            invocation_id=second.id,
            limit=10_000,
        )
        self.assertEqual(1, first_events[0].sequence)
        self.assertEqual(1, second_events[0].sequence)

    async def test_database_app_can_mix_sync_and_async_entrypoints(self) -> None:
        workflow = Workflow(id="database_mixed_api")
        workflow.add_node(lambda value: value, node_id="node")
        app = AutoAgentApp(runtime_store=self.store)

        synchronous = app.invoke(
            workflow,
            input={"value": "sync"},
            session_id="sync",
        )
        asynchronous = await app.ainvoke(
            workflow,
            input={"value": "async"},
            session_id="async",
        )

        self.assertEqual({"output": "sync"}, synchronous.result)
        self.assertEqual({"output": "async"}, asynchronous.result)

    async def test_postgresql_url_uses_generic_async_dialect(self) -> None:
        store = DatabaseRuntimeStore("postgresql://user:pass@localhost/runtime")
        try:
            self.assertEqual("postgresql", store.engine.dialect.name)
            self.assertTrue(store.database_url.startswith("postgresql+asyncpg://"))
        finally:
            await store.aclose()

    async def test_wait_and_resume_are_durable_barriers(self) -> None:
        workflow = Workflow(id="database_wait_resume")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        app = AutoAgentApp(runtime_store=self.store)
        waiting = await app.ainvoke(
            workflow,
            input={"wait_key": "approval"},
            session_id="session",
        )
        self.assertEqual("waiting", waiting.state)
        await app.aclose()

        reopened = DatabaseRuntimeStore.from_path(self.path)
        self.store = reopened
        restarted = AutoAgentApp(runtime_store=reopened)
        resumed = await restarted.aresume(
            workflow,
            session_id="session",
            wait_key="approval",
            output={"approved": True},
        )
        self.assertEqual("completed", resumed.state)
        self.assertEqual({"output": {"approved": True}}, resumed.result)

    async def test_transient_database_failure_retries_without_losing_events(self) -> None:
        class FlakyStore(DatabaseRuntimeStore):
            failures_remaining = 1

            async def _persist(self, command) -> None:
                if command.kind == "event" and self.failures_remaining:
                    self.failures_remaining -= 1
                    raise OSError("database temporarily unavailable")
                await super()._persist(command)

        await self.store.aclose()
        flaky = FlakyStore.from_path(self.path)
        self.store = flaky
        workflow = Workflow(id="database_retry_queue")
        workflow.add_node(lambda: "ok", node_id="node")
        app = AutoAgentApp(runtime_store=flaky)
        invocation = await app.ainvoke(workflow)
        await flaky.aflush()
        await app.aclose()
        reopened = DatabaseRuntimeStore.from_path(self.path)
        self.store = reopened
        events = await reopened.alist_runtime_events(
            invocation_id=invocation.id,
            limit=10_000,
        )

        self.assertEqual(0, flaky.failures_remaining)
        self.assertEqual(
            list(range(1, len(events) + 1)),
            [event.sequence for event in events],
        )


class BoundaryEventTests(unittest.TestCase):
    def test_simple_node_emits_only_semantic_boundary_roles(self) -> None:
        store = InMemoryRuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        workflow = Workflow(id="boundary_roles")
        workflow.add_node(lambda value: value, node_id="node")
        invocation = app.invoke(workflow, input={"value": 1})
        events = asyncio.run(
            store.alist_runtime_events(invocation_id=invocation.id, limit=10_000)
        )
        boundaries = [event.boundary for event in events if event.role == "boundary"]
        self.assertEqual(
            [
                "node.activation_ready",
                "node.input_ready",
                "node.output_ready",
                "node.committed",
                "routing.committed",
                "invocation.completed",
            ],
            boundaries,
        )
        self.assertFalse(any("ready_queue" in event.type for event in events))

    def test_reducer_rebuilds_exact_node_phase_by_boundary_sequence(self) -> None:
        store = InMemoryRuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        workflow = Workflow(id="boundary_rebuild")
        workflow.add_node(lambda value: value.upper(), node_id="node")
        invocation = app.invoke(workflow, input={"value": "hello"})
        events = asyncio.run(
            store.alist_runtime_events(invocation_id=invocation.id, limit=10_000)
        )
        cursors = {
            event.boundary: event.sequence
            for event in events
            if event.role == "boundary"
        }

        _, input_ready = asyncio.run(
            store.arebuild_execution(
                invocation.id,
                through_sequence=cursors["node.input_ready"],
            )
        )
        _, output_ready = asyncio.run(
            store.arebuild_execution(
                invocation.id,
                through_sequence=cursors["node.output_ready"],
            )
        )

        self.assertEqual("running", input_ready.node_executions[0].state)
        self.assertEqual({"value": "hello"}, input_ready.node_executions[0].input)
        self.assertEqual([], input_ready.node_executions[0].operator_calls)
        self.assertEqual("running", output_ready.node_executions[0].state)
        self.assertEqual("HELLO", output_ready.node_executions[0].output)
        self.assertEqual(1, len(output_ready.node_executions[0].operator_calls))

    def test_map_input_ready_contains_materialized_item_selector_results(self) -> None:
        store = InMemoryRuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        workflow = Workflow(id="map_input_boundary")
        workflow.add_node(lambda values: values, node_id="source")
        workflow.add_node(lambda value: value * 2, node_id="target")
        workflow.add_edge(
            "source",
            "target",
            policy=EdgePolicy(
                map=MapPolicy(item_selector=lambda values: [
                    {"value": value} for value in values
                ])
            ),
        )
        invocation = app.invoke(workflow, input={"values": [1, 2]})
        events = asyncio.run(
            store.alist_runtime_events(invocation_id=invocation.id, limit=10_000)
        )
        cursor = next(
            event.sequence
            for event in events
            if event.boundary == "node.input_ready"
            and event.payload["detail"]["node_id"] == "target"
        )
        _, rebuilt = asyncio.run(
            store.arebuild_execution(invocation.id, through_sequence=cursor)
        )
        target = rebuilt.latest_node_execution("target")
        self.assertEqual(({"value": 1}, {"value": 2}), target.operator_inputs)
        self.assertEqual([], target.operator_calls)


class RecoveryExecutionModeTests(unittest.IsolatedAsyncioTestCase):
    async def test_idempotent_policy_requires_explicit_operator_key_contract(self) -> None:
        invalid = Workflow(id="invalid_idempotency")
        invalid.add_node(
            lambda value: value,
            node_id="node",
            policy=NodePolicy(recovery=RecoveryPolicy(mode="idempotent")),
        )
        result = AutoAgentApp().compiler.compile(invalid)
        self.assertFalse(result.ok)
        self.assertIn(
            "POLICY_RECOVERY_IDEMPOTENCY_KEY_REQUIRED",
            {diagnostic.code for diagnostic in result.diagnostics},
        )

    async def test_recovery_follows_selected_path_and_stops_at_first_forbidden_node(
        self,
    ) -> None:
        calls: list[str] = []

        def safe() -> str:
            calls.append("safe")
            return "safe"

        def forbidden(value: str) -> str:
            calls.append("forbidden")
            return value

        workflow = Workflow(id="recovery_gate")
        workflow.add_node(
            safe,
            node_id="safe",
            policy=NodePolicy(
                recovery=RecoveryPolicy(mode="replay_safe", max_attempts=1)
            ),
        )
        workflow.add_node(forbidden, node_id="forbidden")
        workflow.add_edge("safe", "forbidden")
        store = InMemoryRuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        entry = app.register_workflow(workflow)
        session = await store.aget_or_create_session(
            namespace=app.namespace,
            workflow_id=workflow.id,
            session_key="recovery",
        )
        invocation = Invocation(
            workflow_id=workflow.id,
            workflow_version=entry.workflow_ir.workflow_version,
            workflow_definition_hash=entry.workflow_ir.definition_hash,
            workflow_operator_manifest_hash=entry.workflow_snapshot.operator_manifest_hash,
            entry_node_id="safe",
        )
        session = await store.aadmit_invocation(session.id, invocation)
        invocation.mark_running()

        recovered = await app.workflow_executor.arecover(
            workflow_ir=entry.workflow_ir,
            workflow_snapshot=entry.workflow_snapshot,
            session=session,
            invocation=invocation,
        )
        events = await store.alist_runtime_events(
            invocation_id=invocation.id,
            limit=10_000,
        )

        self.assertEqual(["safe"], calls)
        self.assertEqual("recovery", recovered.execution_mode)
        self.assertEqual("interrupted", recovered.state)
        self.assertEqual(
            "recovery.interrupted",
            [event for event in events if event.role == "boundary"][-1].boundary,
        )
