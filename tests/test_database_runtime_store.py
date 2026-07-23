from __future__ import annotations

import asyncio
from concurrent.futures import Future
import tempfile
import threading
import unittest
from pathlib import Path

from sqlalchemy import inspect, text
from sqlalchemy.exc import OperationalError

from autoagent import (
    AutoAgentApp,
    DatabaseRuntimeStore,
    EdgePolicy,
    FailurePolicy,
    MapPolicy,
    NodePolicy,
    RecoveryPolicy,
    SystemCommand,
    Workflow,
    WorkflowPolicy,
)
from autoagent.core.runtime import (
    InMemoryRuntimeStore,
    Invocation,
    RuntimeEvent,
    capture_execution_state,
    diff_execution_state,
)
from autoagent.core.runtime.time import utc_timestamp_ms


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
        self.assertEqual(
            {
                "workflow_versions",
                "sessions",
                "invocations",
                "runtime_events",
                "runtime_snapshots",
            },
            names,
        )

        async def table_columns(table_name: str) -> set[str]:
            async with self.store.engine.connect() as connection:
                return set(
                    await connection.run_sync(
                        lambda sync_connection: {
                            column["name"]
                            for column in inspect(sync_connection).get_columns(
                                table_name
                            )
                        }
                    )
                )

        invocation_columns = await self.store._database_loop.arun(
            table_columns("invocations")
        )
        workflow_columns = await self.store._database_loop.arun(
            table_columns("workflow_versions")
        )
        self.assertNotIn("state_json", invocation_columns)
        self.assertNotIn("snapshot_json", workflow_columns)
        self.assertNotIn("operator_manifests_json", workflow_columns)

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
        events = await reopened.alist_runtime_events(
            invocation_id=invocation.id,
            limit=10_000,
        )
        _, rebuilt = await reopened.arebuild_execution(invocation.id)

        self.assertEqual("completed", rebuilt.state)
        self.assertEqual(invocation.result, rebuilt.result)
        self.assertEqual(
            list(range(1, len(events) + 1)),
            [event.sequence for event in events],
        )
        self.assertTrue(all(event.role == "boundary" for event in events))

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

        synchronous_future: Future = Future()

        def invoke_synchronously() -> None:
            try:
                synchronous_future.set_result(
                    app.invoke(
                        workflow,
                        {"value": "sync"},
                        session_id="sync",
                    )
                )
            except BaseException as exc:
                synchronous_future.set_exception(exc)

        thread = threading.Thread(target=invoke_synchronously)
        thread.start()
        asynchronous = await app.ainvoke(
            workflow,
            input={"value": "async"},
            session_id="async",
        )
        while thread.is_alive():
            await asyncio.sleep(0.001)
        thread.join()
        synchronous = synchronous_future.result()

        self.assertEqual({"output": "sync"}, synchronous.result)
        self.assertEqual({"output": "async"}, asynchronous.result)
        await app.aclose()

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

            async def _persist_batch(self, batch) -> None:
                if (
                    any(item.kind == "event" for item in batch)
                    and self.failures_remaining
                ):
                    self.failures_remaining -= 1
                    raise OperationalError(
                        "INSERT",
                        {},
                        OSError("database temporarily unavailable"),
                        connection_invalidated=True,
                    )
                await super()._persist_batch(batch)

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

    async def test_terminal_return_does_not_wait_for_event_persistence(self) -> None:
        release = threading.Event()

        class SlowStore(DatabaseRuntimeStore):
            async def _persist_batch(self, batch) -> None:
                if any(item.kind == "event" for item in batch):
                    while not release.is_set():
                        await asyncio.sleep(0.001)
                await super()._persist_batch(batch)

        await self.store.aclose()
        slow = SlowStore.from_path(self.path)
        self.store = slow
        workflow = Workflow(id="nonblocking_terminal")
        workflow.add_node(lambda: "done", node_id="node")
        app = AutoAgentApp(runtime_store=slow)

        invocation = await asyncio.wait_for(app.ainvoke(workflow), timeout=1)

        self.assertEqual("completed", invocation.state)
        self.assertEqual("pending", slow.persistence_status(invocation.id))
        self.assertGreater(slow.pending_persistence_bytes, 0)
        release.set()
        await slow.aflush()
        self.assertEqual("durable", slow.persistence_status(invocation.id))
        await app.aclose()

    async def test_byte_backpressure_rejects_new_admission_until_queue_drains(
        self,
    ) -> None:
        release = threading.Event()

        class SlowStore(DatabaseRuntimeStore):
            async def _persist_batch(self, batch) -> None:
                if any(item.kind == "event" for item in batch):
                    while not release.is_set():
                        await asyncio.sleep(0.001)
                await super()._persist_batch(batch)

        await self.store.aclose()
        slow = SlowStore.from_path(
            self.path,
            queue_low_watermark_bytes=128,
            queue_high_watermark_bytes=512,
            queue_hard_watermark_bytes=1024 * 1024,
        )
        self.store = slow
        workflow = Workflow(id="byte_backpressure")
        workflow.add_node(lambda: "done", node_id="node")
        app = AutoAgentApp(runtime_store=slow)
        first = await app.ainvoke(workflow, session_id="first")

        self.assertEqual("completed", first.state)
        self.assertTrue(slow.admission_paused)
        with self.assertRaisesRegex(RuntimeError, "backlog"):
            await app.ainvoke(workflow, session_id="second")

        release.set()
        await slow.aflush()
        second = await app.ainvoke(workflow, session_id="second")
        self.assertEqual("completed", second.state)
        await app.aclose()

    async def test_boundary_events_are_coalesced_into_database_batches(self) -> None:
        class RecordingStore(DatabaseRuntimeStore):
            event_batch_sizes: list[int]

            def __init__(self, *args, **kwargs) -> None:
                super().__init__(*args, **kwargs)
                self.event_batch_sizes = []

            async def _persist_batch(self, batch) -> None:
                event_count = sum(item.kind == "event" for item in batch)
                if event_count:
                    self.event_batch_sizes.append(event_count)
                await super()._persist_batch(batch)

        await self.store.aclose()
        recording = RecordingStore.from_path(
            self.path,
            batch_max_delay_ms=10,
        )
        self.store = recording
        workflow = Workflow(id="batch_events")
        workflow.add_node(lambda: "done", node_id="node")
        app = AutoAgentApp(runtime_store=recording)

        await app.ainvoke(workflow)
        await recording.aflush()

        self.assertGreater(max(recording.event_batch_sizes), 1)
        await app.aclose()


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
        self.assertEqual([], input_ready.node_executions[0].operator_executions)
        self.assertEqual("running", output_ready.node_executions[0].state)
        self.assertEqual("HELLO", output_ready.node_executions[0].output)
        self.assertEqual(1, len(output_ready.node_executions[0].operator_executions))

    def test_map_selector_units_are_not_persisted_at_input_boundary(self) -> None:
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
        self.assertEqual([1, 2], target.input)
        self.assertEqual([], target.operator_executions)

    def test_map_unit_count_does_not_expand_event_or_execution_records(self) -> None:
        store = InMemoryRuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        workflow = Workflow(id="bounded_map_history")
        workflow.add_node(lambda: list(range(100)), node_id="source")
        workflow.add_node(lambda value: value * 2, node_id="target")
        workflow.add_edge(
            "source",
            "target",
            policy=EdgePolicy(
                map=MapPolicy(
                    item_selector=lambda values: [
                        {"value": value} for value in values
                    ],
                    max_parallelism=20,
                )
            ),
        )

        invocation = app.invoke(workflow)
        events = asyncio.run(
            store.alist_runtime_events(
                invocation_id=invocation.id,
                limit=10_000,
            )
        )
        target = invocation.latest_node_execution("target")
        self.assertLessEqual(len(events), 12)
        self.assertEqual(1, len(target.operator_executions))
        logical = target.operator_executions[0]
        self.assertEqual("map", logical.kind)
        self.assertEqual(100, logical.summary.call_count)
        self.assertEqual(100, logical.summary.attempt_count)
        self.assertFalse(hasattr(logical, "output"))


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
        # Persist a realistic crash point: the safe node has started, but its
        # worker result has not committed. Recovery must replay this whole node.
        previous = capture_execution_state(session, invocation)
        invocation.scheduler.drain_ready()
        invocation.mark_running()
        execution = invocation.create_node_execution("safe")
        invocation.mark_node_running(execution.id)
        sequence = invocation.next_event_sequence()
        current = capture_execution_state(session, invocation)
        operations = diff_execution_state(previous, current)
        await store.aapply_event(
            session,
            invocation,
            RuntimeEvent(
                invocation_id=invocation.id,
                sequence=sequence,
                type="node.input_ready",
                occurred_at_ms=utc_timestamp_ms(),
                payload={
                    "operations": [
                        operation.model_dump(mode="python")
                        for operation in operations
                    ]
                },
            ),
        )

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

    async def test_recovery_policy_can_finish_other_active_branches(self) -> None:
        calls: list[str] = []

        def allowed() -> str:
            calls.append("allowed")
            return "done"

        def must_skip(_value: str) -> str:
            calls.append("must_skip")
            return "unexpected"

        workflow = Workflow(
            id="recovery_continue",
            policy=WorkflowPolicy(
                failure=FailurePolicy(mode="continue_active_branches")
            ),
        )
        workflow.add_node(lambda: "blocked", node_id="blocked")
        workflow.add_node(must_skip, node_id="must_skip")
        workflow.add_node(
            allowed,
            node_id="allowed",
            policy=NodePolicy(
                recovery=RecoveryPolicy(mode="replay_safe", max_attempts=1)
            ),
        )
        workflow.add_edge("blocked", "must_skip")
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
            entry_node_id="blocked",
        )
        session = await store.aadmit_invocation(session.id, invocation)
        previous = capture_execution_state(session, invocation)
        invocation.scheduler.drain_ready()
        invocation.scheduler.enqueue_ready("blocked")
        invocation.scheduler.enqueue_ready("allowed")
        invocation.mark_running()
        sequence = invocation.next_event_sequence()
        current = capture_execution_state(session, invocation)
        await store.aapply_event(
            session,
            invocation,
            RuntimeEvent(
                invocation_id=invocation.id,
                sequence=sequence,
                type="routing.committed",
                occurred_at_ms=utc_timestamp_ms(),
                payload={
                    "operations": [
                        operation.model_dump(mode="python")
                        for operation in diff_execution_state(previous, current)
                    ]
                },
            ),
        )

        recovered = await app.workflow_executor.arecover(
            workflow_ir=entry.workflow_ir,
            workflow_snapshot=entry.workflow_snapshot,
            session=session,
            invocation=invocation,
        )

        self.assertEqual(["allowed"], calls)
        self.assertEqual("interrupted", recovered.state)
        self.assertEqual(
            "completed",
            recovered.latest_node_execution("allowed").state,
        )
        self.assertIsNone(recovered.latest_node_execution("blocked"))
        self.assertIsNone(recovered.latest_node_execution("must_skip"))
