from __future__ import annotations

import unittest
from uuid import uuid4

from autoagent.core.runtime import InMemoryRuntimeStore, Invocation, RuntimeErrorInfo


class RuntimeStoreCreationEdgeCaseTests(unittest.TestCase):
    def test_same_namespace_workflow_and_key_reuse_session(self) -> None:
        store = InMemoryRuntimeStore()
        first = store.get_or_create_session(
            namespace="default",
            workflow_id="chat",
            session_key="user-1",
        )
        first.context.data["messages"] = [{"role": "user", "content": "hi"}]
        store.save_session(first)

        second = store.get_or_create_session(
            namespace="default",
            workflow_id="chat",
            session_key="user-1",
        )

        self.assertEqual(second.id, first.id)
        self.assertIsNot(second, first)
        self.assertEqual(
            second.context.data["messages"],
            [{"role": "user", "content": "hi"}],
        )

    def test_namespace_and_workflow_are_part_of_session_identity(self) -> None:
        store = InMemoryRuntimeStore()
        default_chat = store.get_or_create_session(
            namespace="default",
            workflow_id="chat",
            session_key="same-user",
        )
        tenant_chat = store.get_or_create_session(
            namespace="tenant-a",
            workflow_id="chat",
            session_key="same-user",
        )
        default_research = store.get_or_create_session(
            namespace="default",
            workflow_id="research",
            session_key="same-user",
        )

        self.assertNotEqual(default_chat.id, tenant_chat.id)
        self.assertNotEqual(default_chat.id, default_research.id)
        self.assertNotEqual(tenant_chat.id, default_research.id)

    def test_loaded_session_is_isolated_until_saved_again(self) -> None:
        store = InMemoryRuntimeStore()
        session = store.get_or_create_session(
            namespace="default",
            workflow_id="chat",
            session_key="user-1",
        )
        session.context.data["messages"] = []
        store.save_session(session)

        loaded = store.load_session(session.id)
        self.assertIsNotNone(loaded)
        assert loaded is not None
        loaded.context.data["messages"].append({"role": "user", "content": "local"})

        reloaded_before_save = store.load_session(session.id)
        self.assertIsNotNone(reloaded_before_save)
        assert reloaded_before_save is not None
        self.assertEqual(reloaded_before_save.context.data["messages"], [])

        store.save_session(loaded)
        reloaded_after_save = store.load_session(session.id)
        self.assertIsNotNone(reloaded_after_save)
        assert reloaded_after_save is not None
        self.assertEqual(
            reloaded_after_save.context.data["messages"],
            [{"role": "user", "content": "local"}],
        )

    def test_session_and_invocation_context_metadata_are_restored(self) -> None:
        store = InMemoryRuntimeStore()
        session = store.get_or_create_session(
            namespace="default",
            workflow_id="chat",
            session_key="user-1",
        )
        session.context.data["messages"] = []
        session.context.metadata["tenant"] = "demo"
        invocation = Invocation(
            workflow_id="chat",
            workflow_version=1,
            entry_node_id="llm",
            input={"message": "hello"},
        )
        invocation.context.data["turn_type"] = "chat"
        invocation.context.metadata["source"] = "unit-test"
        session.add_invocation(invocation)
        store.save_session(session)

        loaded = store.load_session(session.id)

        self.assertIsNotNone(loaded)
        assert loaded is not None
        loaded_invocation = loaded.get_invocation(invocation.id)
        self.assertIsNotNone(loaded_invocation)
        assert loaded_invocation is not None
        self.assertEqual(loaded.context.data, {"messages": []})
        self.assertEqual(loaded.context.metadata, {"tenant": "demo"})
        self.assertEqual(loaded_invocation.context.data, {"turn_type": "chat"})
        self.assertEqual(
            loaded_invocation.context.metadata,
            {"source": "unit-test"},
        )

    def test_save_invocation_rejects_unknown_session(self) -> None:
        store = InMemoryRuntimeStore()
        invocation = Invocation(
            workflow_id="chat",
            workflow_version=1,
            entry_node_id="llm",
            input={"message": "hello"},
        )

        with self.assertRaises(KeyError):
            store.save_invocation(uuid4(), invocation)


class RuntimeStoreResumeEdgeCaseTests(unittest.TestCase):
    def test_repeated_node_outputs_are_restored_with_ordered_views(self) -> None:
        store = InMemoryRuntimeStore()
        session = store.get_or_create_session(
            namespace="default",
            workflow_id="agent_loop",
            session_key="thread-1",
        )
        invocation = Invocation(
            workflow_id="agent_loop",
            workflow_version=1,
            entry_node_id="llm",
            input={"message": "search twice"},
        )
        first = invocation.create_node_execution("llm", input={"round": 1})
        invocation.mark_node_running(first.id, input=first.input)
        invocation.mark_node_completed(first.id, {"type": "tool_calls"})
        second = invocation.create_node_execution("tool", input={"query": "autoagent"})
        invocation.mark_node_running(second.id, input=second.input)
        invocation.mark_node_completed(second.id, {"items": [1, 2]})
        third = invocation.create_node_execution("llm", input={"round": 2})
        invocation.mark_node_running(third.id, input=third.input)
        invocation.mark_node_completed(third.id, {"type": "final", "content": "done"})

        session.add_invocation(invocation)
        store.save_session(session)

        loaded = store.load_invocation(invocation.id)

        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(
            loaded.outputs.all("llm"),
            [
                {"type": "tool_calls"},
                {"type": "final", "content": "done"},
            ],
        )
        self.assertEqual(
            loaded.outputs.latest("llm"),
            {"type": "final", "content": "done"},
        )
        self.assertEqual(
            [output.node_id for output in loaded.outputs.timeline()],
            ["llm", "tool", "llm"],
        )

    def test_resume_missing_wait_key_fails_without_changing_waiting_state(self) -> None:
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

        loaded = store.load_invocation(invocation.id)
        self.assertIsNotNone(loaded)
        assert loaded is not None

        with self.assertRaises(KeyError):
            loaded.resume_waiting_node(
                wait_key="approval:missing",
                output={"approved": False},
            )

        self.assertEqual(loaded.state, "waiting")
        self.assertIn("approval:r1", loaded.scheduler.waiting_executions)
        self.assertEqual(loaded.node_executions[0].state, "waiting")

    def test_resume_output_is_not_durable_until_saved(self) -> None:
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
        invocation.mark_node_waiting(execution.id, wait_key="approval:r1")
        invocation.mark_waiting()
        session.add_invocation(invocation)
        store.save_session(session)

        loaded = store.load_invocation(invocation.id)
        self.assertIsNotNone(loaded)
        assert loaded is not None
        loaded.resume_waiting_node(
            wait_key="approval:r1",
            output={"approved": True},
        )

        before_save = store.load_invocation(invocation.id)
        self.assertIsNotNone(before_save)
        assert before_save is not None
        self.assertEqual(before_save.state, "waiting")
        self.assertTrue(before_save.outputs.has("approve") is False)

        store.save_invocation(session.id, loaded)
        after_save = store.load_invocation(invocation.id)
        self.assertIsNotNone(after_save)
        assert after_save is not None
        self.assertEqual(after_save.outputs.latest("approve"), {"approved": True})
        self.assertEqual(after_save.scheduler.waiting_executions, {})

    def test_recovery_ignores_waiting_completed_and_failed_executions(self) -> None:
        store = InMemoryRuntimeStore()
        session = store.get_or_create_session(
            namespace="default",
            workflow_id="mixed_flow",
            session_key="worker-1",
        )
        invocation = Invocation(
            workflow_id="mixed_flow",
            workflow_version=1,
            entry_node_id="start",
            input={},
        )
        completed = invocation.create_node_execution("completed")
        invocation.mark_node_running(completed.id)
        invocation.mark_node_completed(completed.id, {"ok": True})

        waiting = invocation.create_node_execution("waiting")
        invocation.mark_node_running(waiting.id)
        invocation.mark_node_waiting(waiting.id, wait_key="wait:1")

        failed = invocation.create_node_execution("failed")
        invocation.mark_node_running(failed.id)
        invocation.mark_node_failed(
            failed.id,
            RuntimeErrorInfo(code="FAILED", message="already failed"),
        )

        running = invocation.create_node_execution("running")
        invocation.mark_node_running(running.id)

        session.add_invocation(invocation)
        store.save_session(session)

        recovered = store.recover_interrupted_invocations()
        loaded = store.load_invocation(invocation.id)

        self.assertEqual(len(recovered), 1)
        self.assertIsNotNone(loaded)
        assert loaded is not None
        states = {
            execution.node_id: execution.state
            for execution in loaded.node_executions
        }
        self.assertEqual(states["completed"], "completed")
        self.assertEqual(states["waiting"], "waiting")
        self.assertEqual(states["failed"], "failed")
        self.assertEqual(states["running"], "interrupted")

    def test_recovery_is_idempotent_after_first_pass(self) -> None:
        store = InMemoryRuntimeStore()
        session = store.get_or_create_session(
            namespace="default",
            workflow_id="crash_flow",
            session_key="worker-1",
        )
        invocation = Invocation(
            workflow_id="crash_flow",
            workflow_version=1,
            entry_node_id="long_task",
            input={},
        )
        execution = invocation.create_node_execution("long_task")
        invocation.mark_node_running(execution.id)
        session.add_invocation(invocation)
        store.save_session(session)

        first = store.recover_interrupted_invocations()
        second = store.recover_interrupted_invocations()

        self.assertEqual(len(first), 1)
        self.assertEqual(second, ())


if __name__ == "__main__":
    unittest.main()
