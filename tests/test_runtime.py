from __future__ import annotations

import unittest

from autoagent import AutoAgentApp, Workflow
from autoagent.core.runtime import (
    DirectOperatorExecution,
    InMemoryRuntimeStore,
    Invocation,
    ParallelExecutionSummary,
    ParallelOperatorExecution,
)


class RuntimeStoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_session_is_one_authoritative_in_memory_aggregate(self) -> None:
        store = InMemoryRuntimeStore()
        first = await store.aget_or_create_session(
            namespace="default",
            workflow_id="chat",
            session_key="user-1",
        )
        first.context.data["messages"] = [{"role": "user", "content": "hi"}]

        second = await store.aget_or_create_session(
            namespace="default",
            workflow_id="chat",
            session_key="user-1",
        )

        self.assertIs(first, second)
        self.assertEqual(second.context.data["messages"][0]["content"], "hi")

    async def test_session_identity_includes_namespace_and_workflow(self) -> None:
        store = InMemoryRuntimeStore()
        values = [
            await store.aget_or_create_session(
                namespace=namespace,
                workflow_id=workflow_id,
                session_key="same",
            )
            for namespace, workflow_id in (
                ("default", "chat"),
                ("tenant", "chat"),
                ("default", "research"),
            )
        ]
        self.assertEqual(3, len({session.id for session in values}))

    async def test_admission_creates_sequence_zero_genesis_snapshot(self) -> None:
        store = InMemoryRuntimeStore()
        session = await store.aget_or_create_session(
            namespace="default",
            workflow_id="flow",
            session_key="one",
        )
        invocation = Invocation(
            workflow_id="flow",
            workflow_version=1,
            entry_node_id="entry",
        )

        await store.aadmit_invocation(session.id, invocation)
        snapshot = await store.aload_execution_snapshot(invocation.id)
        events = await store.alist_runtime_events(invocation_id=invocation.id)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(0, snapshot.through_sequence)
        self.assertEqual(0, invocation.event_sequence)
        self.assertEqual((), events)

    async def test_events_are_invocation_local_contiguous_and_rebuildable(self) -> None:
        store = InMemoryRuntimeStore()
        app = AutoAgentApp(runtime_store=store)
        workflow = Workflow(id="reducer")
        workflow.add_node(lambda: {"value": 1}, node_id="entry")

        invocation = await app.ainvoke(workflow, session_id="session")
        events = await store.alist_runtime_events(invocation_id=invocation.id)
        rebuilt_session, rebuilt = await store.arebuild_execution(invocation.id)

        self.assertEqual(
            list(range(1, len(events) + 1)),
            [event.sequence for event in events],
        )
        self.assertEqual("completed", rebuilt.state)
        self.assertEqual(invocation.result, rebuilt.result)
        self.assertEqual(
            invocation.context.to_record(),
            rebuilt.context.to_record(),
        )
        self.assertEqual(
            rebuilt.id,
            rebuilt_session.get_current_invocation().id,
        )
        await app.aclose()

    async def test_direct_and_parallel_execution_records_round_trip(self) -> None:
        store = InMemoryRuntimeStore()
        session = await store.aget_or_create_session(
            namespace="default",
            workflow_id="flow",
            session_key="one",
        )
        invocation = Invocation(
            workflow_id="flow",
            workflow_version=1,
            entry_node_id="entry",
        )
        await store.aadmit_invocation(session.id, invocation)
        execution = invocation.create_node_execution("entry")
        direct = DirectOperatorExecution(operator_id="primary", sequence=1)
        direct.mark_completed({"value": 1})
        parallel = ParallelOperatorExecution(
            kind="map",
            operator_ids=("primary",),
            summary=ParallelExecutionSummary(
                call_count=100,
                attempt_count=101,
                success_count=100,
                retry_count=1,
            ),
            state="completed",
        )
        execution.operator_executions.extend((direct, parallel))

        record = execution.to_record(invocation.id)
        restored = type(execution).from_record(record)

        self.assertEqual("primary", restored.operator_executions[0].operator_id)
        summary = restored.operator_executions[1].summary
        self.assertEqual(100, summary.call_count)
        self.assertEqual(101, summary.attempt_count)
        self.assertNotIn("outputs", summary.to_record())


if __name__ == "__main__":
    unittest.main()
