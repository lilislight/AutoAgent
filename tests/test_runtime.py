from __future__ import annotations

import unittest

from autoagent.core.runtime import (
    EdgeActivation,
    InMemoryRuntimeStore,
    Invocation,
    RuntimeErrorInfo,
)


class RuntimeStoreTests(unittest.TestCase):
    def test_store_rebuilds_session_invocation_and_outputs(self) -> None:
        store = InMemoryRuntimeStore()
        session = store.get_or_create_session(
            namespace="default",
            workflow_id="chat",
            session_key="user-1",
        )
        session.context.data["messages"] = [{"role": "user", "content": "hi"}]

        invocation = Invocation(
            workflow_id="chat",
            workflow_version=1,
            entry_node_id="llm",
            input={"message": "hi"},
        )
        execution = invocation.create_node_execution(
            "llm",
            input={"messages": session.context.data["messages"]},
        )
        invocation.mark_node_running(execution.id, input=execution.input)
        operator_call = execution.add_operator_call("openai_chat")
        operator_call.mark_running(input=execution.input)
        operator_call.mark_completed({"content": "hello"})
        invocation.mark_node_completed(execution.id, {"content": "hello"})

        session.add_invocation(invocation)
        store.save_session(session)

        loaded = store.load_session(session.id)

        self.assertIsNotNone(loaded)
        assert loaded is not None
        loaded_invocation = loaded.get_invocation(invocation.id)
        self.assertIsNotNone(loaded_invocation)
        assert loaded_invocation is not None
        self.assertEqual(
            loaded.context.data["messages"],
            [{"role": "user", "content": "hi"}],
        )
        self.assertEqual(
            loaded_invocation.outputs.latest("llm"),
            {"content": "hello"},
        )
        self.assertEqual(
            loaded_invocation.node_executions[0].operator_calls[0].operator_id,
            "openai_chat",
        )

    def test_waiting_invocation_can_be_resumed_after_load(self) -> None:
        store = InMemoryRuntimeStore()
        session = store.get_or_create_session(
            namespace="default",
            workflow_id="approval_flow",
            session_key="reviewer-1",
        )
        invocation = Invocation(
            workflow_id="approval_flow",
            workflow_version=1,
            entry_node_id="approve",
            input={"request_id": "r1"},
        )
        execution = invocation.create_node_execution("approve")
        invocation.mark_node_running(execution.id)
        invocation.mark_node_waiting(
            execution.id,
            wait_key="approval:r1",
            reason="Waiting for human approval.",
        )
        invocation.mark_waiting()
        session.add_invocation(invocation)
        store.save_session(session)

        loaded_invocation = store.load_invocation(invocation.id)
        self.assertIsNotNone(loaded_invocation)
        assert loaded_invocation is not None
        resumed = loaded_invocation.resume_waiting_node(
            wait_key="approval:r1",
            output={"approved": True},
        )

        self.assertEqual(resumed.state, "completed")
        self.assertEqual(loaded_invocation.outputs.latest("approve"), {"approved": True})
        self.assertEqual(loaded_invocation.scheduler.waiting_executions, {})

    def test_recovery_marks_running_execution_interrupted(self) -> None:
        store = InMemoryRuntimeStore()
        session = store.get_or_create_session(
            namespace="default",
            workflow_id="crash_flow",
            session_key="worker-1",
        )
        invocation = Invocation(
            workflow_id="crash_flow",
            workflow_version=1,
            entry_node_id="create_repo",
            input={"repo": "demo"},
        )
        execution = invocation.create_node_execution(
            "create_repo",
            idempotency_key=f"{invocation.id}:create_repo",
        )
        invocation.mark_node_running(execution.id, input={"repo": "demo"})
        operator_call = execution.add_operator_call("github_create_repo")
        operator_call.mark_running(input={"repo": "demo"})
        session.add_invocation(invocation)
        store.save_session(session)

        recovered = store.recover_interrupted_invocations()
        loaded_invocation = store.load_invocation(invocation.id)

        self.assertEqual(len(recovered), 1)
        self.assertIsNotNone(loaded_invocation)
        assert loaded_invocation is not None
        self.assertEqual(loaded_invocation.state, "interrupted")
        loaded_execution = loaded_invocation.node_executions[0]
        self.assertEqual(loaded_execution.state, "interrupted")
        self.assertEqual(loaded_execution.error.code, "WORKER_LOST")
        self.assertEqual(loaded_execution.operator_calls[0].state, "interrupted")

    def test_operator_calls_record_map_and_replica_calls(self) -> None:
        invocation = Invocation(
            workflow_id="fanout_flow",
            workflow_version=1,
            entry_node_id="process_items",
            input={},
        )
        execution = invocation.create_node_execution("process_items")
        invocation.mark_node_running(execution.id, input={"items": ["a", "b"]})

        first = execution.add_operator_call(
            "process_item",
            kind="map_item",
            item_index=0,
        )
        first.mark_completed({"item": "a"})
        second = execution.add_operator_call(
            "process_item",
            kind="map_item",
            item_index=1,
        )
        second.mark_completed({"item": "b"})
        replica = execution.add_operator_call(
            "judge_item",
            kind="replica",
            replica_index=0,
        )
        replica.mark_completed({"score": 1})
        invocation.mark_node_completed(
            execution.id,
            [{"item": "a"}, {"item": "b"}],
        )

        store = InMemoryRuntimeStore()
        session = store.get_or_create_session(
            namespace="default",
            workflow_id="fanout_flow",
            session_key="trace",
        )
        session.add_invocation(invocation)
        store.save_session(session)
        loaded = store.load_invocation(invocation.id)

        self.assertIsNotNone(loaded)
        assert loaded is not None
        loaded_calls = loaded.node_executions[0].operator_calls
        self.assertEqual(
            [(item.kind, item.item_index, item.replica_index) for item in loaded_calls],
            [
                ("map_item", 0, None),
                ("map_item", 1, None),
                ("replica", None, 0),
            ],
        )

    def test_invocation_aggregates_node_resource_usage_by_node_id(self) -> None:
        invocation = Invocation(
            workflow_id="loop_flow",
            workflow_version=1,
            entry_node_id="llm",
            input={},
        )
        first = invocation.create_node_execution("llm")
        first.resource_usage.add(duration_ms=120)
        first.add_operator_call("openai_chat")
        first.add_operator_call("openai_chat", kind="retry")

        tool = invocation.create_node_execution("tool")
        tool.resource_usage.add(duration_ms=999)
        tool.add_operator_call("search")

        second = invocation.create_node_execution("llm")
        second.resource_usage.add(duration_ms=80)
        second.add_operator_call("openai_chat")

        self.assertEqual(invocation.count_node_executions("llm"), 2)
        self.assertEqual(invocation.count_operator_calls("llm"), 3)
        self.assertEqual(invocation.sum_node_runtime_ms("llm"), 200)
        self.assertEqual(invocation.count_node_executions("missing"), 0)
        self.assertEqual(invocation.count_operator_calls("missing"), 0)
        self.assertEqual(invocation.sum_node_runtime_ms("missing"), 0)

    def test_store_restores_edge_activation_and_fan_in_cursor(self) -> None:
        store = InMemoryRuntimeStore()
        session = store.get_or_create_session(
            namespace="default",
            workflow_id="fan_in",
            session_key="trace",
        )
        invocation = Invocation(
            workflow_id="fan_in",
            workflow_version=1,
            entry_node_id="source",
            input={},
        )
        source = invocation.create_node_execution("source")
        invocation.mark_node_running(source.id)
        invocation.mark_node_completed(source.id, {"value": 1})
        activation = EdgeActivation(
            edge_id="source_target",
            source_node_id="source",
            source_execution_id=source.id,
        )
        target = invocation.create_node_execution(
            "target",
            incoming_activations=(activation,),
        )
        invocation.scheduler.resolve_edge(
            activation.edge_id,
            state="selected",
            activation=activation,
        )
        invocation.scheduler.scheduled_node_ids.add("target")
        invocation.scheduler.entered_loop_region_ids.add("loop_1")
        session.add_invocation(invocation)
        store.save_session(session)

        loaded = store.load_invocation(invocation.id)

        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(
            loaded.get_node_execution(target.id).incoming_activations,
            (activation,),
        )
        self.assertEqual(
            loaded.scheduler.edge_resolutions[activation.edge_id].activation,
            activation,
        )
        self.assertEqual(loaded.scheduler.scheduled_node_ids, {"target"})
        self.assertEqual(loaded.scheduler.entered_loop_region_ids, {"loop_1"})


if __name__ == "__main__":
    unittest.main()
