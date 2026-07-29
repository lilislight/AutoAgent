from __future__ import annotations

import unittest

from autoagent import AutoAgentApp, RuntimeRetentionPolicy, Workflow
from autoagent.core.runtime import (
    apply_state_operations,
    capture_execution_state,
    DirectOperatorExecution,
    ExecutionSnapshot,
    RuntimeStore,
    Invocation,
    ParallelExecutionSummary,
    ParallelOperatorExecution,
    reduce_execution_state,
    RuntimeEvent,
    Session,
    StateOperation,
    UserEventSpec,
)
from autoagent.core.runtime.time import utc_timestamp_ms
from autoagent.core.runtime.snapshot import (
    capture_recovery_state,
    compact_recovery_state,
)
from tests.helpers import started_app


class RuntimeStoreTests(unittest.IsolatedAsyncioTestCase):
    def test_record_user_event_keeps_public_defensive_copy_contract(self) -> None:
        def done() -> str:
            return "done"

        workflow = Workflow(id="user_event_copy")
        workflow.add_node(done, node_id="done")
        app = started_app()
        try:
            invocation = app.invoke(workflow, event_mode="minimal")
            execution = invocation.latest_node_execution("done")
            source = {"nested": {"value": 1}}
            returned = app.runtime_store.record_user_event(
                invocation_id=invocation.id,
                spec=UserEventSpec(
                    type="done",
                    data=source,
                    node_id="done",
                    node_execution_id=execution.id,
                ),
            )
            source["nested"]["value"] = 2
            returned.data["nested"]["value"] = 3
            stored = app.runtime_store.list_user_events(
                invocation_id=invocation.id,
            )
        finally:
            app.close()

        self.assertEqual(stored[0].data, {"nested": {"value": 1}})

    def test_state_operations_copy_only_changed_aggregate_paths(self) -> None:
        first = {"id": "first", "state": "completed"}
        second = {"id": "second", "state": "running"}
        state = {
            "session": {"context": {}},
            "invocation": {"state": "running"},
            "node_executions": [first, second],
        }

        reduced = apply_state_operations(
            state,
            (
                StateOperation(
                    op="replace",
                    path=("node_executions", 1, "state"),
                    value="completed",
                ),
            ),
        )

        self.assertIsNot(state, reduced)
        self.assertIs(first, reduced["node_executions"][0])
        self.assertIsNot(second, reduced["node_executions"][1])
        self.assertEqual("completed", reduced["node_executions"][1]["state"])

    def test_app_close_from_owned_runtime_loop_stops_thread(self) -> None:
        app = started_app()
        thread = app._runtime_loop._thread
        assert thread is not None

        app._runtime_loop.run(app.aclose())
        thread.join(timeout=0.5)

        self.assertTrue(app._closed)
        self.assertFalse(thread.is_alive())

    def test_state_operations_copy_only_nested_changed_paths(self) -> None:
        untouched_session = {"context": {"data": {"tenant": "one"}}}
        previous_context = {
            "data": {
                "nested": {"old": 1},
                "untouched": {"large": [1, 2, 3]},
            }
        }
        state = {
            "session": untouched_session,
            "invocation": {"context": previous_context},
            "node_executions": [],
        }
        operation_value = {"new": [4, 5, 6]}

        reduced = apply_state_operations(
            state,
            (
                StateOperation(
                    op="replace",
                    path=("invocation", "context", "data", "nested"),
                    value=operation_value,
                ),
            ),
        )

        self.assertIs(untouched_session, reduced["session"])
        self.assertIsNot(state["invocation"], reduced["invocation"])
        self.assertIsNot(previous_context, reduced["invocation"]["context"])
        self.assertIsNot(
            previous_context["data"],
            reduced["invocation"]["context"]["data"],
        )
        self.assertIs(
            previous_context["data"]["untouched"],
            reduced["invocation"]["context"]["data"]["untouched"],
        )
        self.assertEqual({"old": 1}, previous_context["data"]["nested"])
        operation_value["new"].append(7)
        self.assertEqual(
            {"new": [4, 5, 6]},
            reduced["invocation"]["context"]["data"]["nested"],
        )

    def test_recovery_capture_matches_compacted_full_state(self) -> None:
        session = Session(
            workflow_id="flow",
            workflow_revision_id="revision-flow",
            session_key="session",
        )
        session.context.data["session_payload"] = {"value": 1}
        invocation = Invocation(
            workflow_id="flow",
            workflow_revision_id="revision-flow",
            workflow_version=1,
            entry_node_id="entry",
            event_mode="standard",
        )
        invocation.context.data["invocation_payload"] = {"value": 2}
        session.add_invocation(invocation)
        execution = invocation.create_node_execution("entry")
        execution.input = {"prompt": "input"}
        execution.output = {"answer": "output"}
        operator_call = DirectOperatorExecution(
            operator_id="primary",
            sequence=1,
            input={"operator": "input"},
        )
        operator_call.mark_completed({"operator": "output"})
        execution.operator_executions.append(operator_call)

        expected = compact_recovery_state(
            capture_execution_state(session, invocation)
        )
        captured = capture_recovery_state(session, invocation)

        self.assertEqual(expected, captured)
        self.assertIsNone(captured["node_executions"][0]["input"])
        self.assertEqual(
            {"answer": "output"},
            captured["node_executions"][0]["output"],
        )
        operator_record = captured["node_executions"][0][
            "operator_executions"
        ][0]
        self.assertNotIn("input", operator_record)
        self.assertNotIn("output", operator_record)
        execution.output["answer"] = "changed"
        session.context.data["session_payload"]["value"] = 3
        self.assertEqual(
            {"answer": "output"},
            captured["node_executions"][0]["output"],
        )
        self.assertEqual(
            1,
            captured["session"]["context"]["data"]["session_payload"]["value"],
        )

    def test_reducer_rejects_non_contiguous_event_journal(self) -> None:
        session = Session(
            workflow_id="flow",
            workflow_revision_id="revision-flow",
            session_key="session",
        )
        invocation = Invocation(
            workflow_id="flow",
            workflow_revision_id="revision-flow",
            workflow_version=1,
            entry_node_id="entry",
            event_mode="full",
        )
        session.add_invocation(invocation)
        snapshot = ExecutionSnapshot.capture(
            session,
            invocation,
            through_sequence=0,
        )
        event = RuntimeEvent(
            invocation_id=invocation.id,
            sequence=2,
            event_type="routing",
            event_name="edge.evaluated",
            subject_type="invocation",
            subject_id=str(invocation.id),
            occurred_at_ms=utc_timestamp_ms(),
            operations=(),
        )

        with self.assertRaisesRegex(ValueError, "not contiguous"):
            reduce_execution_state(snapshot, (event,))

    def test_output_view_copies_only_values_that_are_read(self) -> None:
        copies = 0

        class TrackedValue:
            def __deepcopy__(self, memo):
                nonlocal copies
                copies += 1
                return self

        invocation = Invocation(
            workflow_id="flow",
            workflow_revision_id="revision-flow",
            workflow_version=1,
            entry_node_id="entry",
            event_mode="full",
        )
        first = invocation.create_node_execution("first")
        second = invocation.create_node_execution("second")
        invocation.mark_node_completed(first.id, TrackedValue())
        invocation.mark_node_completed(second.id, TrackedValue())

        outputs = invocation.outputs
        self.assertTrue(outputs.has("first"))
        self.assertEqual(0, copies)

        outputs.latest("second")
        self.assertEqual(1, copies)

        outputs.timeline(["first"])
        self.assertEqual(2, copies)

    async def test_session_is_one_authoritative_in_memory_aggregate(self) -> None:
        store = RuntimeStore()
        first = await store.aget_or_create_session(
            workflow_id="chat",
            workflow_revision_id="chat-revision",
            session_key="user-1",
        )
        first.context.data["messages"] = [{"role": "user", "content": "hi"}]

        second = await store.aget_or_create_session(
            workflow_id="chat",
            workflow_revision_id="chat-revision",
            session_key="user-1",
        )

        self.assertIs(first, second)
        self.assertEqual(second.context.data["messages"][0]["content"], "hi")

    async def test_session_identity_includes_revision(self) -> None:
        store = RuntimeStore()
        values = [
            await store.aget_or_create_session(
                workflow_id=workflow_id,
                workflow_revision_id=revision_id,
                session_key="same",
            )
            for workflow_id, revision_id in (
                ("chat", "chat-v1"),
                ("chat", "chat-v2"),
            )
        ]
        self.assertEqual(2, len({session.id for session in values}))

    async def test_admission_creates_sequence_zero_genesis_checkpoint(self) -> None:
        store = RuntimeStore()
        session = await store.aget_or_create_session(
            workflow_id="flow",
            workflow_revision_id="revision-flow",
            session_key="one",
        )
        invocation = Invocation(
            workflow_id="flow",
            workflow_revision_id="revision-flow",
            workflow_version=1,
            entry_node_id="entry",
            event_mode="full",
        )

        await store.aadmit_invocation(session.id, invocation)
        snapshot = await store.aload_execution_snapshot(invocation.id)
        events = await store.alist_runtime_events(invocation_id=invocation.id)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(0, snapshot.through_sequence)
        self.assertEqual(0, invocation.event_sequence)
        self.assertEqual((), events)

    async def test_replay_checkpoint_limit_never_evicts_genesis(self) -> None:
        store = RuntimeStore(
            retention_policy=RuntimeRetentionPolicy(
                max_replay_checkpoints_per_invocation=2,
            )
        )
        app = started_app(runtime_store=store)
        workflow = Workflow(id="checkpoint_retention")
        workflow.add_node(lambda: "done", node_id="node")
        invocation = await app.ainvoke(workflow, event_mode="full")

        for sequence in (1, 2, 3):
            await store.arebuild_execution(
                invocation.id,
                through_sequence=sequence,
            )

        sequences = {
            sequence
            for candidate_id, sequence in store._replay_checkpoints
            if candidate_id == invocation.id
        }
        self.assertEqual({0, 2, 3}, sequences)
        _, rebuilt = await store.arebuild_execution(
            invocation.id,
            through_sequence=1,
        )
        self.assertEqual(1, rebuilt.event_sequence)
        await app.aclose()

    async def test_events_are_invocation_local_contiguous_and_rebuildable(self) -> None:
        store = RuntimeStore()
        app = started_app(runtime_store=store)
        workflow = Workflow(id="reducer")
        workflow.add_node(lambda: {"value": 1}, node_id="entry")

        invocation = await app.ainvoke(
            workflow,
            session_id="session",
            event_mode="full",
        )
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
        session = store.find_session(
            workflow_revision_id=rebuilt.workflow_revision_id,
            session_key="session",
        )
        assert session is not None
        self.assertEqual(
            capture_execution_state(session, invocation),
            store.reduced_state(invocation.id),
        )
        paged = await store._load_event_range(
            invocation_id=invocation.id,
            after_sequence=0,
            before_sequence=None,
            page_size=2,
        )
        self.assertEqual(events, paged)
        await app.aclose()

    async def test_direct_and_parallel_execution_records_round_trip(self) -> None:
        store = RuntimeStore()
        session = await store.aget_or_create_session(
            workflow_id="flow",
            workflow_revision_id="revision-flow",
            session_key="one",
        )
        invocation = Invocation(
            workflow_id="flow",
            workflow_revision_id="revision-flow",
            workflow_version=1,
            entry_node_id="entry",
        )
        await store.aadmit_invocation(session.id, invocation)
        execution = invocation.create_node_execution("entry")
        direct = DirectOperatorExecution(operator_id="primary", sequence=1)
        direct.streaming = True
        direct.stream_chunk_count = 4
        direct.resource_usage.stream_consumption_ns = 20
        direct.resource_usage.stream_reduction_ns = 5
        direct.mark_completed({"value": 1})
        parallel = ParallelOperatorExecution(
            kind="map",
            operator_ids=("primary",),
            summary=ParallelExecutionSummary(
                call_count=100,
                attempt_count=101,
                success_count=100,
                retry_count=1,
                streaming_call_count=100,
                stream_chunk_count=400,
                stream_consumption_ns=2_000,
                stream_reduction_ns=500,
            ),
            state="completed",
        )
        execution.operator_executions.extend((direct, parallel))

        record = execution.to_record(invocation.id)
        restored = type(execution).from_record(record)

        self.assertEqual("primary", restored.operator_executions[0].operator_id)
        self.assertTrue(restored.operator_executions[0].streaming)
        self.assertEqual(4, restored.operator_executions[0].stream_chunk_count)
        self.assertEqual(
            20,
            restored.operator_executions[0].resource_usage.stream_consumption_ns,
        )
        summary = restored.operator_executions[1].summary
        self.assertEqual(100, summary.call_count)
        self.assertEqual(101, summary.attempt_count)
        self.assertEqual(100, summary.streaming_call_count)
        self.assertEqual(400, summary.stream_chunk_count)
        self.assertNotIn("outputs", summary.to_record())


if __name__ == "__main__":
    unittest.main()
