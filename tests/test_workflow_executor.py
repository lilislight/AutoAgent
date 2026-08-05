from __future__ import annotations

import asyncio
import threading
import time
import unittest

from autoagent import AutoAgentApp
from autoagent.core.compiler import workflow_revision_id
from autoagent.core.runtime import Invocation, RuntimeStore, SessionBusyError
from autoagent.core.workflow import (
    BackoffPolicy,
    CapabilityRef,
    Edge,
    MapPolicy,
    NodePolicy,
    RecoveryPolicy,
    ReplicationPolicy,
    ResourcePolicy,
    RetryPolicy,
    SystemCommand,
    TimeoutPolicy,
    Workflow,
)
from tests.helpers import isolated_app, started_app


def start_message(message: str) -> dict[str, str]:
    return {"text": message.upper()}


def finish_message(text: str) -> str:
    return f"done:{text}"


def registered_revision_id(app: AutoAgentApp, workflow: Workflow) -> str:
    entry = app.register_workflow(workflow)
    snapshot = entry.workflow_snapshot
    return workflow_revision_id(
        snapshot.workflow_id,
        snapshot.definition_hash,
    )


class WorkflowExecutorTests(unittest.TestCase):
    def test_app_execution_requires_explicit_start(self) -> None:
        workflow = Workflow(id="explicit_app_start")
        workflow.add_node(lambda: "done", node_id="node")
        app = isolated_app()

        with self.assertRaisesRegex(RuntimeError, "not started"):
            app.invoke(workflow)
        with self.assertRaisesRegex(RuntimeError, "not started"):
            asyncio.run(app.ainvoke(workflow))
        self.assertEqual({}, app.runtime_store.invocations)

        app.start()
        invocation = app.invoke(workflow)
        self.assertEqual("completed", invocation.state)
        app.close()

    def test_start_recovers_registered_created_invocation(self) -> None:
        workflow = Workflow(id="startup_created_recovery")
        workflow.add_node(
            lambda: "recovered",
            node_id="node",
            policy=NodePolicy(
                recovery=RecoveryPolicy(mode="replay_safe", max_attempts=1)
            ),
        )
        store = RuntimeStore()
        app = isolated_app(runtime_store=store)
        entry = app.register_workflow(workflow)
        session = store.get_or_create_session(
            workflow_id=workflow.id,
            workflow_revision_id=workflow_revision_id(
                entry.workflow_snapshot.workflow_id,
                entry.workflow_snapshot.definition_hash,
            ),
            session_key="recover-on-start",
        )
        invocation = Invocation(
            workflow_id=workflow.id,
            workflow_revision_id=session.workflow_revision_id,
            workflow_version=entry.workflow_ir.workflow_version,
            workflow_definition_hash=entry.workflow_ir.definition_hash,
            entry_node_id="node",
            event_mode="full",
        )
        asyncio.run(store.aadmit_invocation(session.id, invocation))

        app.start()

        recovered = store.invocations[invocation.id]
        self.assertEqual("completed", recovered.state)
        self.assertEqual("recovery", recovered.execution_mode)
        self.assertEqual({"output": "recovered"}, recovered.result)
        app.close()

    def test_start_recovers_independent_invocations_concurrently(self) -> None:
        active = 0
        peak_active = 0

        async def recover() -> str:
            nonlocal active, peak_active
            active += 1
            peak_active = max(peak_active, active)
            await asyncio.sleep(0.03)
            active -= 1
            return "recovered"

        workflow = Workflow(id="parallel_startup_recovery")
        workflow.add_node(
            recover,
            node_id="node",
            policy=NodePolicy(
                recovery=RecoveryPolicy(mode="replay_safe", max_attempts=1)
            ),
        )
        store = RuntimeStore()
        app = isolated_app(runtime_store=store)
        entry = app.register_workflow(workflow)
        revision_id = workflow_revision_id(
            entry.workflow_snapshot.workflow_id,
            entry.workflow_snapshot.definition_hash,
        )
        invocation_ids = []
        for index in range(2):
            session = store.get_or_create_session(
                workflow_id=workflow.id,
                workflow_revision_id=revision_id,
                session_key=f"recover-{index}",
            )
            invocation = Invocation(
                workflow_id=workflow.id,
                workflow_revision_id=revision_id,
                workflow_version=entry.workflow_ir.workflow_version,
                workflow_definition_hash=entry.workflow_ir.definition_hash,
                entry_node_id="node",
                event_mode="full",
            )
            asyncio.run(store.aadmit_invocation(session.id, invocation))
            invocation_ids.append(invocation.id)

        app.start()

        self.assertEqual(2, peak_active)
        self.assertTrue(
            all(
                store.invocations[invocation_id].state == "completed"
                for invocation_id in invocation_ids
            )
        )
        app.close()

    def test_workflow_executor_is_async_only(self) -> None:
        app = started_app()
        self.assertFalse(hasattr(app.workflow_executor, "invoke"))
        self.assertFalse(hasattr(app.workflow_executor, "resume"))
        app.close()

    def test_minimal_defers_operator_records_until_node_completion(self) -> None:
        app = started_app()
        progress_messages = []
        apply_progress = app.workflow_executor._apply_progress

        async def observe_progress(**kwargs):
            progress_messages.append(kwargs["progress"])
            await apply_progress(**kwargs)

        app.workflow_executor._apply_progress = observe_progress
        workflow = Workflow(id="minimal_without_operator_progress")
        workflow.add_node(lambda value: value + 1, node_id="node")

        invocation = app.invoke(
            workflow,
            input={"value": 1},
            event_mode="minimal",
        )
        app.close()

        self.assertEqual([], progress_messages)
        self.assertEqual(0, invocation.event_sequence)
        execution = invocation.latest_node_execution("node")
        self.assertEqual("completed", execution.state)
        self.assertEqual(1, len(execution.operator_executions))
        self.assertEqual("completed", execution.operator_executions[0].state)

    def test_expanded_child_workflow_executes_with_local_hook_ids(self) -> None:
        observed_condition: list[tuple[str, str, str, int]] = []
        observed_binding: list[tuple[str, int]] = []

        def parent_start(value: int) -> int:
            return value

        def child_first(value: int) -> int:
            return value + 1

        def child_second(value: int) -> int:
            return value * 2

        def parent_finish(value: int) -> int:
            return value + 3

        def child_condition(ctx) -> bool:
            observed_condition.append(
                (
                    ctx.edge_id,
                    ctx.source_node_id,
                    ctx.target_node_id,
                    ctx.outputs.latest("first"),
                )
            )
            return True

        def child_binding(ctx) -> None:
            observed_binding.append((ctx.node_id, ctx.outputs.latest("first")))

        child = Workflow(id="child")
        child.add_node(
            child_first,
            node_id="first",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        child.add_node(
            child_second,
            node_id="second",
            input_mapping=lambda ctx: {"value": ctx.outputs.latest("first")},
            output_binding=child_binding,
        )
        child.add_edge(
            "first",
            "second",
            edge_id="continue",
            condition=child_condition,
        )

        parent = Workflow(id="expanded_child_execution")
        parent.add_node(parent_start, node_id="start")
        parent.add_node(child, node_id="child")
        parent.add_node(
            parent_finish,
            node_id="finish",
            input_mapping=lambda ctx: {"value": ctx.outputs.latest("child")},
        )
        parent.add_edge("start", "child")
        parent.add_edge("child", "finish")

        invocation = started_app().invoke(parent, input={"value": 2})

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.result, {"output": 9})
        self.assertEqual(
            ["start", "child/first", "child/second", "finish"],
            [execution.node_id for execution in invocation.node_executions],
        )
        self.assertEqual(
            [("continue", "first", "second", 3)],
            observed_condition,
        )
        self.assertEqual([("second", 3)], observed_binding)

    def test_app_invoke_executes_simple_chain(self) -> None:
        workflow = Workflow(id="simple_chain")
        workflow.add_node(start_message, node_id="start")
        workflow.add_node(finish_message, node_id="finish")
        workflow.add_edge("start", "finish")

        invocation = started_app().invoke(workflow, input={"message": "hello"})

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.outputs.latest("start"), {"text": "HELLO"})
        self.assertEqual(invocation.outputs.latest("finish"), "done:HELLO")
        self.assertEqual(invocation.result, {"output": "done:HELLO"})

    def test_same_workflow_id_can_register_distinct_revisions(self) -> None:
        first = Workflow(id="shared_workflow_id")
        first.add_node(lambda: "first", node_id="first")
        second = Workflow(id="shared_workflow_id")
        second.add_node(lambda: "second", node_id="second")
        app = started_app()

        first_entry = app.register_workflow(first)
        second_entry = app.register_workflow(second)
        first_invocation = app.invoke(first, session_id="same-session-key")
        second_invocation = app.invoke(second, session_id="same-session-key")

        self.assertEqual(2, len(app.workflow_registry))
        self.assertIsNot(first_entry, second_entry)
        self.assertNotEqual(
            first_invocation.workflow_revision_id,
            second_invocation.workflow_revision_id,
        )
        self.assertEqual({"output": "first"}, first_invocation.result)
        self.assertEqual({"output": "second"}, second_invocation.result)
        first_session_id = app.runtime_store.invocation_sessions[
            first_invocation.id
        ]
        second_session_id = app.runtime_store.invocation_sessions[
            second_invocation.id
        ]
        self.assertNotEqual(first_session_id, second_session_id)

    def test_system_wait_resumes_same_execution_and_continues_workflow(self) -> None:
        def finish(approved: bool) -> str:
            return "approved" if approved else "rejected"

        def remember_approval(ctx) -> None:
            ctx.invocation_context.data["approval"] = ctx.output

        workflow = Workflow(id="human_approval")
        workflow.add_node(
            SystemCommand(id="wait"),
            node_id="approval",
            output_binding=remember_approval,
        )
        workflow.add_node(finish, node_id="finish")
        workflow.add_edge("approval", "finish")
        app = started_app()

        waiting = app.invoke(
            workflow,
            input={
                "wait_key": "approval:request-1",
                "wait_type": "human",
                "payload": {"request_id": "request-1"},
            },
            session_id="reviewer-1",
        )

        self.assertEqual(waiting.state, "waiting")
        self.assertEqual(len(waiting.node_executions), 1)
        execution = waiting.node_executions[0]
        self.assertEqual(execution.state, "waiting")
        self.assertEqual(execution.operator_executions, [])
        wait = waiting.scheduler.waiting_executions["approval:request-1"]
        self.assertEqual(wait.node_execution_id, execution.id)
        self.assertEqual(wait.wait_type, "human")
        self.assertEqual(wait.payload, {"request_id": "request-1"})
        stored = app.runtime_store.find_session(
            workflow_revision_id=waiting.workflow_revision_id,
            session_key="reviewer-1",
        ).get_current_invocation()
        self.assertIs(stored, waiting)
        self.assertEqual(stored.state, "waiting")
        self.assertIn("approval:request-1", stored.scheduler.waiting_executions)

        resumed = app.resume(
            workflow,
            session_id="reviewer-1",
            wait_key="approval:request-1",
            output={"approved": True},
        )

        self.assertEqual(resumed.id, waiting.id)
        self.assertEqual(resumed.state, "completed")
        self.assertEqual(resumed.node_executions[0].id, execution.id)
        self.assertEqual(resumed.outputs.latest("approval"), {"approved": True})
        self.assertEqual(resumed.context.data["approval"], {"approved": True})
        self.assertEqual(resumed.result, {"output": "approved"})
        with self.assertRaisesRegex(ValueError, "waiting Invocation"):
            app.resume(
                workflow,
                session_id="reviewer-1",
                wait_key="approval:request-1",
                output={"approved": False},
            )

    def test_system_wait_uses_node_execution_id_as_default_wait_key(self) -> None:
        workflow = Workflow(id="generated_wait_key")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait")
        app = started_app()

        invocation = app.invoke(workflow, session_id="session")

        execution = invocation.node_executions[0]
        self.assertEqual(tuple(invocation.scheduler.waiting_executions), (str(execution.id),))
        resumed = app.resume(
            workflow,
            session_id="session",
            wait_key=str(execution.id),
            output={"received": True},
        )
        self.assertEqual(resumed.result, {"output": {"received": True}})

    def test_wait_resume_preserves_loop_iteration_scope(self) -> None:
        def wait_input(ctx):
            value = ctx.incoming[0].value
            return {
                "wait_key": f"loop:{value}",
                "payload": {"iteration": value},
            }

        workflow = Workflow(id="loop_wait_resume")
        workflow.add_node(lambda: 0, node_id="start")
        workflow.add_node(
            SystemCommand(id="wait"),
            node_id="wait",
            input_mapping=wait_input,
        )
        workflow.add_node(
            lambda value: value + 1,
            node_id="step",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        workflow.add_node(
            lambda value: value,
            node_id="final",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        workflow.add_edge("start", "wait", edge_id="enter")
        workflow.add_edge("wait", "step", edge_id="step")
        workflow.add_edge(
            "step",
            "wait",
            edge_id="continue",
            condition=lambda ctx: ctx.source_output < 2,
        )
        workflow.add_edge(
            "step",
            "final",
            edge_id="exit",
            condition=lambda ctx: ctx.source_output >= 2,
        )
        app = started_app()

        first_wait = app.invoke(workflow, session_id="loop-wait")
        first_state = first_wait.state
        second_wait = app.resume(
            workflow,
            session_id="loop-wait",
            wait_key="loop:0",
            output=0,
        )
        second_state = second_wait.state
        completed = app.resume(
            workflow,
            session_id="loop-wait",
            wait_key="loop:1",
            output=1,
        )

        self.assertEqual(first_state, "waiting")
        self.assertEqual(second_state, "waiting")
        self.assertEqual(completed.state, "completed")
        self.assertEqual(completed.result, {"output": 2})
        wait_scopes = [
            execution.execution_scope
            for execution in completed.node_executions
            if execution.node_id == "wait"
        ]
        self.assertEqual(
            [scope[-1].iteration for scope in wait_scopes],
            [0, 1],
        )

    def test_wait_is_returned_only_after_parallel_runnable_work_finishes(self) -> None:
        async def scenario() -> None:
            slow_started = asyncio.Event()
            release_slow = asyncio.Event()

            def start() -> dict[str, str]:
                return {"wait_key": "parallel:wait"}

            async def slow() -> str:
                slow_started.set()
                await release_slow.wait()
                return "slow-complete"

            workflow = Workflow(id="wait_barrier")
            workflow.add_node(start, node_id="start")
            workflow.add_node(SystemCommand(id="wait"), node_id="wait")
            workflow.add_node(
                slow,
                node_id="slow",
                input_mapping=lambda _ctx: {},
            )
            workflow.add_edge("start", "wait")
            workflow.add_edge("start", "slow")
            app = started_app()

            task = asyncio.create_task(
                app.ainvoke(workflow, session_id="parallel-session")
            )
            await asyncio.wait_for(slow_started.wait(), timeout=1)
            persisted = None
            for _ in range(100):
                session = app.runtime_store.find_session(
                    workflow_revision_id=registered_revision_id(app, workflow),
                    session_key="parallel-session",
                )
                persisted = session.get_current_invocation() if session is not None else None
                if persisted is not None and persisted.scheduler.waiting_executions:
                    break
                await asyncio.sleep(0.001)

            self.assertFalse(task.done())
            self.assertIsNotNone(persisted)
            self.assertEqual(persisted.state, "running")
            with self.assertRaises(SessionBusyError):
                await app.aresume(
                    workflow,
                    session_id="parallel-session",
                    wait_key="parallel:wait",
                    output={"approved": False},
                )

            release_slow.set()
            waiting = await asyncio.wait_for(task, timeout=1)
            self.assertEqual(waiting.state, "waiting")
            self.assertEqual(waiting.outputs.latest("slow"), "slow-complete")

            resumed = await app.aresume(
                workflow,
                session_id="parallel-session",
                wait_key="parallel:wait",
                output={"approved": True},
            )
            self.assertEqual(resumed.state, "completed")
            self.assertEqual(
                resumed.result,
                {
                    "outputs": {
                        "wait": {"approved": True},
                        "slow": "slow-complete",
                    }
                },
            )

        asyncio.run(scenario())

    def test_invalid_system_wait_input_fails_without_operator_executions(self) -> None:
        workflow = Workflow(id="invalid_wait_input")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait")

        invocation = started_app().invoke(
            workflow,
            input={"wait_key": 1},
            session_id="session",
        )

        self.assertEqual(invocation.state, "failed")
        execution = invocation.node_executions[0]
        self.assertEqual(execution.error.code, "SYSTEM_COMMAND_INPUT_INVALID")
        self.assertEqual(execution.operator_executions, [])

    def test_duplicate_active_wait_key_fails_invocation(self) -> None:
        def start() -> dict[str, str]:
            return {"wait_key": "duplicate"}

        workflow = Workflow(id="duplicate_wait_key")
        workflow.add_node(start, node_id="start")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait_a")
        workflow.add_node(SystemCommand(id="wait"), node_id="wait_b")
        workflow.add_edge("start", "wait_a")
        workflow.add_edge("start", "wait_b")

        app = started_app()
        invocation = app.invoke(workflow, session_id="session")

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "WAIT_KEY_CONFLICT")
        self.assertEqual(invocation.scheduler.waiting_executions, {})
        self.assertEqual(
            {"failed", "cancelled"},
            {execution.state for execution in invocation.node_executions[1:]},
        )
        cancelled = next(
            execution
            for execution in invocation.node_executions
            if execution.state == "cancelled"
        )
        self.assertEqual(
            "INVOCATION_FAILED_FAST",
            cancelled.error.code,
        )
        self.assertTrue(
            any(
                event.event_name == "node.cancelled"
                and event.subject_id == str(cancelled.id)
                and event.payload["error"]["code"]
                == "INVOCATION_FAILED_FAST"
                for event in app.runtime_store.runtime_events.get(
                    invocation.id,
                    (),
                )
            )
        )

    def test_async_callable_runs_in_native_async_path(self) -> None:
        async def async_start(message: str) -> dict[str, str]:
            await asyncio.sleep(0.01)
            return {"text": message}

        workflow = Workflow(id="async_chain")
        workflow.add_node(async_start, node_id="start")
        workflow.add_node(finish_message, node_id="finish")
        workflow.add_edge("start", "finish")

        invocation = started_app().invoke(workflow, input={"message": "async"})

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.outputs.latest("finish"), "done:async")

    def test_ainvoke_runs_async_operator_on_app_runtime_loop(self) -> None:
        async def scenario() -> None:
            caller_thread = threading.get_ident()
            operator_thread: int | None = None

            async def operator(value: str) -> str:
                nonlocal operator_thread
                operator_thread = threading.get_ident()
                await asyncio.sleep(0)
                return value.upper()

            workflow = Workflow(id="native_async_invoke")
            workflow.add_node(operator, node_id="operator")

            invocation = await started_app().ainvoke(
                workflow,
                input={"value": "async"},
            )

            self.assertEqual(invocation.result, {"output": "ASYNC"})
            self.assertIsNotNone(operator_thread)
            self.assertNotEqual(operator_thread, caller_thread)

        asyncio.run(scenario())

    def test_ainvoke_offloads_sync_operator_without_blocking_event_loop(self) -> None:
        async def scenario() -> None:
            caller_thread = threading.get_ident()
            operator_thread: int | None = None
            heartbeat_ran = False

            def operator() -> str:
                nonlocal operator_thread
                operator_thread = threading.get_ident()
                time.sleep(0.03)
                return "done"

            async def heartbeat() -> None:
                nonlocal heartbeat_ran
                await asyncio.sleep(0.005)
                heartbeat_ran = True

            workflow = Workflow(id="sync_operator_offload")
            workflow.add_node(operator, node_id="operator")

            invocation, _ = await asyncio.gather(
                started_app().ainvoke(workflow),
                heartbeat(),
            )

            self.assertTrue(heartbeat_ran)
            self.assertNotEqual(operator_thread, caller_thread)
            self.assertEqual(invocation.result, {"output": "done"})

        asyncio.run(scenario())

    def test_sync_and_async_invocation_can_be_mixed(self) -> None:
        async def scenario() -> None:
            workflow = Workflow(id="sync_in_async")
            workflow.add_node(lambda: "done", node_id="node")
            app = started_app()

            sync_result = app.invoke(workflow, session_id="sync")
            async_result = await app.ainvoke(workflow, session_id="async")
            self.assertEqual("completed", sync_result.state)
            self.assertEqual("completed", async_result.state)
            app.close()

        asyncio.run(scenario())

    def test_ainvoke_cancellation_cancels_async_operator_and_persists_state(self) -> None:
        async def scenario() -> None:
            started = asyncio.Event()
            operator_cancelled = asyncio.Event()

            async def operator() -> str:
                started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    operator_cancelled.set()
                    raise

            workflow = Workflow(id="async_cancellation")
            workflow.add_node(operator, node_id="operator")
            app = started_app()
            task = asyncio.create_task(
                app.ainvoke(workflow, session_id="session")
            )
            await asyncio.wait_for(started.wait(), timeout=1)

            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

            self.assertTrue(operator_cancelled.is_set())
            session = app.runtime_store.find_session(
                workflow_revision_id=registered_revision_id(app, workflow),
                session_key="session",
            )
            invocation = session.get_current_invocation()
            self.assertEqual(invocation.state, "cancelled")
            self.assertEqual(
                invocation.latest_node_execution("operator").state,
                "cancelled",
            )
            self.assertTrue(
                any(
                    event.event_name == "node.cancelled"
                    and event.payload["error"]["code"]
                    == "INVOCATION_CANCELLED"
                    for event in app.runtime_store.runtime_events[invocation.id]
                )
            )
            self.assertFalse(invocation.execution_mailbox.has_pending())

        asyncio.run(scenario())

    def test_infrastructure_failure_cancels_unfinished_nodes_with_events(
        self,
    ) -> None:
        async def scenario() -> None:
            workflow = Workflow(id="infrastructure_terminalization")
            workflow.add_node(lambda: "done", node_id="worker")
            app = started_app()
            entry = app.register_workflow(workflow)
            revision_id = workflow_revision_id(
                entry.workflow_snapshot.workflow_id,
                entry.workflow_snapshot.definition_hash,
            )
            session = await app.runtime_store.aget_or_create_session(
                workflow_id=workflow.id,
                workflow_revision_id=revision_id,
                session_key="session",
            )
            invocation = Invocation(
                workflow_id=workflow.id,
                workflow_revision_id=revision_id,
                workflow_version=entry.workflow_ir.workflow_version,
                workflow_definition_hash=entry.workflow_ir.definition_hash,
                entry_node_id="worker",
                event_mode="standard",
            )
            await app.runtime_store.aadmit_invocation(
                session.id,
                invocation,
            )
            execution = invocation.create_node_execution("worker")
            invocation.mark_running()
            invocation.mark_node_running(execution.id)

            await app.workflow_executor.afail_infrastructure(
                session=session,
                invocation=invocation,
                error=RuntimeError("control path failed"),
            )
            events = app.runtime_store.runtime_events[invocation.id]

            self.assertEqual("failed", invocation.state)
            self.assertEqual("cancelled", execution.state)
            self.assertEqual(
                "INVOCATION_INFRASTRUCTURE_ERROR",
                execution.error.code,
            )
            self.assertEqual(
                ["node.cancelled", "invocation.failed"],
                [event.event_name for event in events],
            )

        asyncio.run(scenario())

    def test_async_condition_mapping_and_binding_hooks_are_supported(self) -> None:
        async def condition(ctx) -> bool:
            await asyncio.sleep(0)
            return ctx.source_output["value"] > 0

        async def mapping(ctx) -> dict[str, int]:
            await asyncio.sleep(0)
            return {"value": ctx.incoming[0].value["value"]}

        async def binding(ctx) -> None:
            await asyncio.sleep(0)
            ctx.invocation_context.data["bound"] = ctx.output

        workflow = Workflow(id="async_hooks")
        workflow.add_node(lambda: {"value": 2}, node_id="start")
        workflow.add_node(
            lambda value: value * 2,
            node_id="target",
            input_mapping=mapping,
            output_binding=binding,
        )
        workflow.add_edge("start", "target", condition=condition)

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.result, {"output": 4})
        self.assertEqual(invocation.context.data, {"bound": 4})

    def test_hook_read_views_cannot_mutate_runtime_owned_data(self) -> None:
        def condition(ctx) -> bool:
            ctx.source_output["items"][0] = 9
            ctx.outputs.latest("start")["items"][0] = 9
            return True

        def mapping(ctx) -> dict[str, int]:
            ctx.invocation_input["request"]["items"][0] = 9
            with self.assertRaises(TypeError):
                ctx.invocation_context.data["invalid"] = True
            with self.assertRaises(TypeError):
                ctx.session_context.data["invalid"] = True
            incoming = ctx.incoming[0].value
            value = incoming["items"][0]
            incoming["items"][0] = 9
            return {"value": value}

        def binding(ctx) -> None:
            value = ctx.output["value"]
            ctx.output["value"] = 9
            ctx.outputs.latest("start")["items"][0] = 9
            ctx.invocation_context.data["bound"] = value
            ctx.session_context.data["saved_output"] = {"value": value}

        workflow = Workflow(id="readonly_hooks")
        workflow.add_node(lambda request: {"items": [1]}, node_id="start")
        workflow.add_node(
            lambda value: {"value": value},
            node_id="target",
            input_mapping=mapping,
            output_binding=binding,
        )
        workflow.add_edge("start", "target", condition=condition)

        app = started_app()
        invocation = app.invoke(
            workflow,
            input={"request": {"items": [1]}},
            session_id="readonly-session",
        )
        session = app.runtime_store.find_session(
            workflow_revision_id=invocation.workflow_revision_id,
            session_key="readonly-session",
        )

        self.assertEqual(invocation.result, {"output": {"value": 1}})
        self.assertEqual(
            invocation.latest_node_execution("start").output,
            {"items": [1]},
        )
        self.assertEqual(invocation.context.data, {"bound": 1})
        self.assertEqual(session.context.data["saved_output"], {"value": 1})

    def test_parallel_ready_nodes_do_not_wait_for_slowest_branch(self) -> None:
        events: list[str] = []
        lock = threading.Lock()

        def record(name: str) -> None:
            with lock:
                events.append(name)

        def start() -> dict[str, str]:
            record("start")
            return {}

        def fast() -> dict[str, str]:
            time.sleep(0.01)
            record("fast")
            return {}

        def slow() -> dict[str, str]:
            time.sleep(0.15)
            record("slow")
            return {}

        def after_fast() -> str:
            record("after_fast")
            return "done"

        workflow = Workflow(id="parallel_no_barrier")
        workflow.add_node(start, node_id="start")
        workflow.add_node(fast, node_id="fast")
        workflow.add_node(slow, node_id="slow")
        workflow.add_node(after_fast, node_id="after_fast")
        workflow.add_edge("start", "fast")
        workflow.add_edge("start", "slow")
        workflow.add_edge("fast", "after_fast")

        invocation = started_app().invoke(workflow, input={})

        self.assertEqual(invocation.state, "completed")
        self.assertLess(events.index("after_fast"), events.index("slow"))

    def test_parallel_internal_events_receive_sequence_when_they_complete(
        self,
    ) -> None:
        selector_completed = False

        def select_items(ctx):
            nonlocal selector_completed
            selector_completed = True
            return [{"value": value} for value in ctx.input]

        async def finish_after_selector(value):
            while not selector_completed:
                await asyncio.sleep(0)
            return value

        async def slow_map_item(value):
            await asyncio.sleep(0.03)
            return value

        store = RuntimeStore()
        app = started_app(runtime_store=store)
        workflow = Workflow(id="parallel_internal_event_sequence")
        workflow.add_node(lambda: [1], node_id="start")
        workflow.add_node(
            finish_after_selector,
            node_id="fast",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        workflow.add_node(
            slow_map_item,
            node_id="mapped",
            policy=NodePolicy(map=MapPolicy(item_selector=select_items)),
        )
        workflow.add_edge("start", "fast")
        workflow.add_edge(
            "start",
            "mapped",
        )

        invocation = app.invoke(workflow, event_mode="full")
        events = store.runtime_events[invocation.id]
        app.close()

        self.assertEqual(
            list(range(1, len(events) + 1)),
            [event.sequence for event in events],
        )

        def sequence(event_name: str, node_id: str) -> int:
            return next(
                event.sequence
                for event in events
                if event.event_name == event_name
                and event.payload.get("node_id") == node_id
            )

        self.assertLess(
            sequence("item_selection.completed", "mapped"),
            sequence("operator_call.completed", "fast"),
        )
        self.assertLess(
            sequence("operator_call.completed", "fast"),
            sequence("node.completed", "fast"),
        )
        self.assertLess(
            sequence("node.completed", "fast"),
            sequence("operator_call.completed", "mapped"),
        )
        self.assertLess(
            sequence("operator_call.completed", "mapped"),
            sequence("node.completed", "mapped"),
        )

    def test_dynamic_branch_skips_propagate_before_complete_fan_in(self) -> None:
        calls: list[str] = []

        def start() -> dict[str, bool]:
            return {"use_left": True}

        def left(use_left: bool) -> dict[str, str]:
            calls.append("left")
            self.assertTrue(use_left)
            return {"value": "left-result"}

        def right(use_left: bool) -> dict[str, str]:
            calls.append("right")
            self.assertFalse(use_left)
            return {"value": "right-result"}

        def join(value: str) -> str:
            calls.append("join")
            return f"joined:{value}"

        workflow = Workflow(id="dynamic_complete_fan_in")
        workflow.add_node(start, node_id="start")
        workflow.add_node(left, node_id="left")
        workflow.add_node(right, node_id="right")
        workflow.add_node(join, node_id="join")
        workflow.add_edge(
            "start",
            "left",
            edge_id="choose_left",
            condition=lambda ctx: ctx.source_output["use_left"],
        )
        workflow.add_edge(
            "start",
            "right",
            edge_id="choose_right",
            condition=lambda ctx: not ctx.source_output["use_left"],
        )
        workflow.add_edge("left", "join", edge_id="left_join")
        workflow.add_edge("right", "join", edge_id="right_join")

        invocation = started_app().invoke(workflow, input={})

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(calls, ["left", "join"])
        self.assertEqual(invocation.result, {"output": "joined:left-result"})
        self.assertEqual(
            invocation.scheduler.edge_resolutions["choose_right"].state,
            "skipped",
        )
        self.assertEqual(
            invocation.scheduler.edge_resolutions["right_join"].state,
            "skipped",
        )

    def test_parallel_branches_complete_before_fan_in_executes(self) -> None:
        def start() -> dict[str, object]:
            return {}

        def left() -> str:
            time.sleep(0.03)
            return "L"

        def right() -> str:
            return "R"

        def join(left: str, right: str) -> str:
            return left + right

        workflow = Workflow(id="parallel_complete_fan_in")
        workflow.add_node(start, node_id="start")
        workflow.add_node(left, node_id="left")
        workflow.add_node(right, node_id="right")
        workflow.add_node(join, node_id="join")
        workflow.add_edge("start", "left")
        workflow.add_edge("start", "right")
        workflow.add_edge("left", "join")
        workflow.add_edge("right", "join")

        invocation = started_app().invoke(workflow, input={})

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.outputs.latest("join"), "LR")
        join_execution = invocation.latest_node_execution("join")
        assert join_execution is not None
        self.assertEqual(len(join_execution.incoming_activations), 2)
        self.assertEqual(
            [activation.edge_id for activation in join_execution.incoming_activations],
            ["edge_left_join", "edge_right_join"],
        )

    def test_single_path_loop_repeats_and_exposes_incoming_edge(self) -> None:
        incoming_edges: list[str] = []

        def start() -> int:
            return 0

        def map_agent_input(ctx):
            incoming_edges.append(ctx.incoming[0].edge_id)
            return {"value": ctx.incoming[0].value}

        def agent(value: int) -> int:
            return value + 1

        def final(value: int) -> int:
            return value

        workflow = Workflow(id="single_path_loop")
        workflow.add_node(start, node_id="start")
        workflow.add_node(
            agent,
            node_id="agent",
            input_mapping=map_agent_input,
            policy=NodePolicy(
                resource=ResourcePolicy(max_node_executions_per_invocation=5)
            ),
        )
        workflow.add_node(
            final,
            node_id="final",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        workflow.add_edge("start", "agent", edge_id="enter_loop")
        workflow.add_edge(
            "agent",
            "agent",
            edge_id="continue_loop",
            condition=lambda ctx: ctx.source_output < 3,
        )
        workflow.add_edge(
            "agent",
            "final",
            edge_id="exit_loop",
            condition=lambda ctx: ctx.source_output >= 3,
        )

        invocation = started_app().invoke(workflow, input={})

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.result, {"output": 3})
        self.assertEqual(
            incoming_edges,
            ["enter_loop", "continue_loop", "continue_loop"],
        )
        self.assertEqual(invocation.count_node_executions("agent"), 3)

    def test_loop_fails_when_continue_and_exit_match_same_iteration(self) -> None:
        def agent(value: int = 0) -> int:
            return value + 1

        workflow = Workflow(id="ambiguous_loop")
        workflow.add_node(lambda: {}, node_id="start", entry=True)
        workflow.add_node(agent, node_id="agent")
        workflow.add_node(agent, node_id="final")
        workflow.add_edge("start", "agent")
        workflow.add_edge(
            "agent",
            "agent",
            condition=lambda ctx: True,
        )
        workflow.add_edge(
            "agent",
            "final",
            condition=lambda ctx: True,
        )

        invocation = started_app().invoke(workflow, input={})

        self.assertEqual(invocation.state, "failed")
        assert invocation.error is not None
        self.assertEqual(invocation.error.code, "LOOP_CONTINUE_EXIT_CONFLICT")

    def test_loop_parallel_tools_complete_fan_in_before_next_iteration(self) -> None:
        collect_inputs: list[tuple[int, int]] = []

        def map_incoming(ctx):
            return {"value": ctx.incoming[0].value}

        def collect(left_tool: int, right_tool: int) -> int:
            collect_inputs.append((left_tool, right_tool))
            return max(left_tool, right_tool)

        workflow = Workflow(id="parallel_tool_loop")
        workflow.add_node(lambda: 0, node_id="start")
        workflow.add_node(
            lambda value: value,
            node_id="agent",
            input_mapping=map_incoming,
        )
        workflow.add_node(
            lambda value: value + 1,
            node_id="left_tool",
            input_mapping=map_incoming,
        )
        workflow.add_node(
            lambda value: value + 1,
            node_id="right_tool",
            input_mapping=map_incoming,
        )
        workflow.add_node(collect, node_id="collect")
        workflow.add_node(
            lambda value: value,
            node_id="final",
            input_mapping=map_incoming,
        )
        workflow.add_edge("start", "agent", edge_id="enter")
        workflow.add_edge("agent", "left_tool", edge_id="agent_left")
        workflow.add_edge("agent", "right_tool", edge_id="agent_right")
        workflow.add_edge("left_tool", "collect", edge_id="left_collect")
        workflow.add_edge("right_tool", "collect", edge_id="right_collect")
        workflow.add_edge(
            "collect",
            "agent",
            edge_id="continue",
            condition=lambda ctx: ctx.source_output < 3,
        )
        workflow.add_edge(
            "collect",
            "final",
            edge_id="exit",
            condition=lambda ctx: ctx.source_output >= 3,
        )

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.result, {"output": 3})
        self.assertEqual(collect_inputs, [(1, 1), (2, 2), (3, 3)])
        self.assertEqual(invocation.count_node_executions("agent"), 3)
        for execution in (
            item for item in invocation.node_executions if item.node_id == "collect"
        ):
            self.assertEqual(len(execution.incoming_activations), 2)

    def test_nested_loop_external_entry_repeats_for_each_outer_iteration(self) -> None:
        def map_state(ctx):
            return {"state": dict(ctx.incoming[0].value)}

        workflow = Workflow(id="nested_loop")
        workflow.add_node(
            lambda: {"outer": 0, "inner": 0},
            node_id="start",
        )
        workflow.add_node(
            lambda state: {"outer": state["outer"], "inner": 0},
            node_id="outer",
            input_mapping=map_state,
        )
        workflow.add_node(
            lambda state: dict(state),
            node_id="inner",
            input_mapping=map_state,
        )
        workflow.add_node(
            lambda state: {**state, "inner": state["inner"] + 1},
            node_id="inner_body",
            input_mapping=map_state,
        )
        workflow.add_node(
            lambda state: {
                "outer": state["outer"] + 1,
                "inner": state["inner"],
            },
            node_id="after_inner",
            input_mapping=map_state,
        )
        workflow.add_node(
            lambda state: dict(state),
            node_id="final",
            input_mapping=map_state,
        )
        workflow.add_edge("start", "outer", edge_id="enter_outer")
        workflow.add_edge("outer", "inner", edge_id="enter_inner")
        workflow.add_edge("inner", "inner_body", edge_id="inner_step")
        workflow.add_edge(
            "inner_body",
            "inner",
            edge_id="inner_back",
            condition=lambda ctx: ctx.source_output["inner"] < 2,
        )
        workflow.add_edge(
            "inner_body",
            "after_inner",
            edge_id="inner_exit",
            condition=lambda ctx: ctx.source_output["inner"] >= 2,
        )
        workflow.add_edge(
            "after_inner",
            "outer",
            edge_id="outer_back",
            condition=lambda ctx: ctx.source_output["outer"] < 2,
        )
        workflow.add_edge(
            "after_inner",
            "final",
            edge_id="outer_exit",
            condition=lambda ctx: ctx.source_output["outer"] >= 2,
        )

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(
            invocation.result,
            {"output": {"outer": 2, "inner": 2}},
        )
        inner_scopes = [
            tuple((frame.loop_region_id, frame.iteration) for frame in item.execution_scope)
            for item in invocation.node_executions
            if item.node_id == "inner"
        ]
        self.assertEqual(
            inner_scopes,
            [
                (("loop_1", 0), ("loop_2", 0)),
                (("loop_1", 0), ("loop_2", 1)),
                (("loop_1", 1), ("loop_2", 0)),
                (("loop_1", 1), ("loop_2", 1)),
            ],
        )

    def test_loop_branch_selection_and_skip_are_scoped_per_iteration(self) -> None:
        incoming_counts: list[int] = []

        def map_value(ctx):
            return {"value": ctx.incoming[0].value}

        def collect(values: list[int]) -> int:
            incoming_counts.append(len(values))
            return max(values)

        workflow = Workflow(id="loop_scoped_branch")
        workflow.add_node(lambda: 0, node_id="start")
        workflow.add_node(
            lambda value: value,
            node_id="header",
            input_mapping=map_value,
        )
        workflow.add_node(
            lambda value: value + 1,
            node_id="always",
            input_mapping=map_value,
        )
        workflow.add_node(
            lambda value: value + 1,
            node_id="optional",
            input_mapping=map_value,
        )
        workflow.add_node(
            collect,
            node_id="collect",
            input_mapping=lambda ctx: {
                "values": [item.value for item in ctx.incoming]
            },
        )
        workflow.add_node(
            lambda value: value,
            node_id="final",
            input_mapping=map_value,
        )
        workflow.add_edge("start", "header", edge_id="enter")
        workflow.add_edge("header", "always", edge_id="always_branch")
        workflow.add_edge(
            "header",
            "optional",
            edge_id="optional_branch",
            condition=lambda ctx: ctx.source_output == 1,
        )
        workflow.add_edge("always", "collect", edge_id="always_collect")
        workflow.add_edge("optional", "collect", edge_id="optional_collect")
        workflow.add_edge(
            "collect",
            "header",
            edge_id="continue",
            condition=lambda ctx: ctx.source_output < 3,
        )
        workflow.add_edge(
            "collect",
            "final",
            edge_id="exit",
            condition=lambda ctx: ctx.source_output >= 3,
        )

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.result, {"output": 3})
        self.assertEqual(incoming_counts, [1, 2, 1])
        self.assertEqual(invocation.count_node_executions("optional"), 1)

    def test_loop_branch_can_join_with_parallel_acyclic_branch(self) -> None:
        def start() -> int:
            return 0

        def loop(value: int) -> int:
            return value + 1

        def side(_value: int) -> str:
            return "side"

        def join(loop: int, side: str) -> str:
            return f"{loop}:{side}"

        workflow = Workflow(id="loop_and_parallel_join")
        workflow.add_node(start, node_id="start")
        workflow.add_node(
            loop,
            node_id="loop",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        workflow.add_node(
            side,
            node_id="side",
            input_mapping=lambda ctx: {"_value": ctx.incoming[0].value},
        )
        workflow.add_node(join, node_id="join")
        workflow.add_edge("start", "loop", edge_id="start_loop")
        workflow.add_edge("start", "side", edge_id="start_side")
        workflow.add_edge(
            "loop",
            "loop",
            edge_id="repeat_loop",
            condition=lambda ctx: ctx.source_output < 2,
        )
        workflow.add_edge(
            "loop",
            "join",
            edge_id="loop_join",
            condition=lambda ctx: ctx.source_output >= 2,
        )
        workflow.add_edge("side", "join", edge_id="side_join")

        invocation = started_app().invoke(workflow, input={})

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.result, {"output": "2:side"})
        self.assertEqual(invocation.count_node_executions("loop"), 2)

    def test_edge_condition_exception_fails_invocation(self) -> None:
        def start() -> int:
            return 1

        def finish(value: int) -> int:
            return value

        def broken_condition(_ctx) -> bool:
            raise ValueError("bad condition")

        workflow = Workflow(id="condition_failure")
        workflow.add_node(start, node_id="start")
        workflow.add_node(finish, node_id="finish")
        workflow.add_edge(
            "start",
            "finish",
            edge_id="broken_edge",
            condition=broken_condition,
        )

        invocation = started_app().invoke(workflow, input={})

        self.assertEqual(invocation.state, "failed")
        assert invocation.error is not None
        self.assertEqual(invocation.error.code, "EDGE_CONDITION_FAILED")
        self.assertIn("ValueError: bad condition", invocation.error.detail["reason"])

    def test_node_execution_resource_limit_fails_loop(self) -> None:
        def loop_node() -> dict[str, bool]:
            return {"again": True}

        def keep_going(ctx) -> bool:
            return True

        workflow = Workflow(id="limited_loop")
        workflow.add_node(lambda: None, node_id="start", entry=True)
        workflow.add_node(
            loop_node,
            node_id="loop",
            input_mapping=lambda _ctx: {},
            policy=NodePolicy(
                resource=ResourcePolicy(max_node_executions_per_invocation=1)
            ),
        )
        workflow.add_edge("start", "loop")
        workflow.edges.append(
            Edge(
                from_node="loop",
                to_node="loop",
                condition=keep_going,
            )
        )

        invocation = started_app().invoke(workflow, input={})

        self.assertEqual(invocation.state, "failed")
        self.assertIsNotNone(invocation.error)
        assert invocation.error is not None
        self.assertEqual(invocation.error.code, "RESOURCE_LIMIT_EXCEEDED")

    def test_operator_attempt_resource_limit_fails_node_execution(self) -> None:
        def limited() -> str:
            return "never"

        workflow = Workflow(id="operator_limit")
        workflow.add_node(
            limited,
            node_id="limited",
            policy=NodePolicy(
                resource=ResourcePolicy(max_operator_attempts_per_invocation=0)
            ),
        )

        compile_result = started_app().compiler.compile(workflow)

        self.assertFalse(compile_result.ok)

    def test_same_session_rejects_second_running_invocation(self) -> None:
        started = threading.Event()
        release = threading.Event()
        results = []

        def slow() -> str:
            started.set()
            release.wait(timeout=2)
            return "done"

        workflow = Workflow(id="same_session_running")
        workflow.add_node(slow, node_id="slow")
        app = started_app()
        thread = threading.Thread(
            target=lambda: results.append(
                app.invoke(workflow, session_id="shared-session")
            )
        )
        thread.start()
        self.assertTrue(started.wait(timeout=1))
        before = app.runtime_store.find_session(
            workflow_revision_id=registered_revision_id(app, workflow),
            session_key="shared-session",
        )
        assert before is not None
        before_invocation = before.get_current_invocation()

        with self.assertRaises(SessionBusyError):
            app.invoke(workflow, session_id="shared-session")

        after = app.runtime_store.find_session(
            workflow_revision_id=registered_revision_id(app, workflow),
            session_key="shared-session",
        )
        self.assertIs(before, after)
        assert after is not None
        self.assertIs(before_invocation, after.get_current_invocation())
        release.set()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(results[0].state, "completed")

    def test_same_session_rejects_invoke_while_waiting(self) -> None:
        """invoke on a session with a waiting invocation raises SessionBusyError."""

        workflow = Workflow(id="busy_while_waiting")
        workflow.add_node(SystemCommand(id="wait"), node_id="approval")
        app = started_app()

        waiting = app.invoke(
            workflow,
            input={"wait_key": "step-1"},
            session_id="shared-session",
        )
        self.assertEqual(waiting.state, "waiting")

        with self.assertRaises(SessionBusyError):
            app.invoke(workflow, session_id="shared-session")

        # The session still holds the waiting invocation, unaffected.
        session = app.runtime_store.find_session(
            workflow_revision_id=waiting.workflow_revision_id,
            session_key="shared-session",
        )
        assert session is not None
        current = session.get_current_invocation()
        assert current is not None
        self.assertEqual(current.id, waiting.id)
        self.assertEqual(current.state, "waiting")

    def test_same_session_allows_invoke_after_completed(self) -> None:
        """After the waiting invocation is resumed and completed, a new invoke
        on the same session succeeds."""

        workflow = Workflow(id="reuse_session")
        workflow.add_node(SystemCommand(id="wait"), node_id="approval")
        app = started_app()

        waiting = app.invoke(
            workflow,
            input={"wait_key": "step-1"},
            session_id="shared-session",
        )
        self.assertEqual(waiting.state, "waiting")

        resumed = app.resume(
            workflow,
            session_id="shared-session",
            wait_key="step-1",
            output={"approved": True},
        )
        self.assertEqual(resumed.state, "completed")

        # Same session, new invocation — should succeed.
        second = app.invoke(
            workflow,
            input={"wait_key": "step-2"},
            session_id="shared-session",
        )
        self.assertEqual(second.state, "waiting")
        self.assertNotEqual(second.id, waiting.id)

    def test_different_sessions_have_isolated_execution_mailboxes(self) -> None:
        gate = threading.Barrier(2)
        results: dict[str, object] = {}

        def concurrent(value: str) -> str:
            if value != "warm":
                gate.wait(timeout=2)
            return value

        workflow = Workflow(id="isolated_mailboxes")
        workflow.add_node(concurrent, node_id="node")
        app = started_app()
        app.invoke(workflow, input={"value": "warm"}, session_id="warm")

        threads = [
            threading.Thread(
                target=lambda key=key: results.__setitem__(
                    key,
                    app.invoke(
                        workflow,
                        input={"value": key},
                        session_id=key,
                    ),
                )
            )
            for key in ("one", "two")
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(results["one"].result, {"output": "one"})
        self.assertEqual(results["two"].result, {"output": "two"})

    def test_fail_fast_leftover_worker_cannot_pollute_next_invocation(self) -> None:
        def start() -> dict[str, object]:
            return {}

        def fail() -> None:
            raise RuntimeError("boom")

        def slow() -> str:
            time.sleep(0.05)
            return "old"

        first = Workflow(id="isolated_failure")
        first.add_node(start, node_id="start")
        first.add_node(fail, node_id="fail")
        first.add_node(slow, node_id="slow")
        first.add_edge("start", "fail")
        first.add_edge("start", "slow")

        second = Workflow(id="after_isolated_failure")
        second.add_node(lambda: "new", node_id="new")
        app = started_app()

        failed = app.invoke(first)
        completed = app.invoke(second)

        self.assertEqual(failed.state, "failed")
        cancelled = failed.latest_node_execution("slow")
        self.assertEqual(cancelled.state, "cancelled")
        self.assertEqual(cancelled.error.code, "INVOCATION_FAILED_FAST")
        self.assertTrue(
            any(
                event.event_name == "node.cancelled"
                and event.subject_id == str(cancelled.id)
                and event.payload["error"]["code"]
                == "INVOCATION_FAILED_FAST"
                for event in app.runtime_store.runtime_events[failed.id]
            )
        )
        self.assertEqual(completed.state, "completed")
        self.assertEqual(completed.result, {"output": "new"})

    def test_retry_uses_backoff_policy_and_records_attempt_kinds(self) -> None:
        calls = 0

        def flaky() -> str:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise RuntimeError("retry")
            return "ok"

        workflow = Workflow(id="retry_execution")
        workflow.add_node(
            flaky,
            node_id="flaky",
            policy=NodePolicy(
                retry=RetryPolicy(
                    max_attempts=3,
                    backoff=BackoffPolicy(mode="fixed", initial_delay_ms=0),
                )
            ),
        )

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.state, "completed")
        execution = invocation.node_executions[0]
        self.assertEqual([call.reason for call in execution.operator_executions], [
            "normal",
            "retry",
            "retry",
        ])

    def test_retry_exhausts_primary_before_operator_fallback(self) -> None:
        order: list[str] = []

        def primary() -> str:
            order.append("primary")
            raise RuntimeError("primary failed")

        def fallback() -> str:
            order.append("fallback")
            return "ok"

        app = started_app()
        app.register_capability("retry_fallback")
        app.register_operator(
            primary,
            operator_id="primary",
            capability_id="retry_fallback",
            default=True,
        )
        app.register_operator(
            fallback,
            operator_id="fallback",
            capability_id="retry_fallback",
        )
        workflow = Workflow(id="retry_then_fallback")
        workflow.add_node(
            "retry_fallback",
            node_id="retry_fallback",
            policy=NodePolicy(retry=RetryPolicy(max_attempts=2)),
        )

        invocation = app.invoke(workflow)

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(order, ["primary", "primary", "fallback"])
        self.assertEqual(
            [call.reason for call in invocation.node_executions[0].operator_executions],
            ["normal", "retry", "fallback"],
        )

    def test_node_max_concurrency_applies_across_sessions(self) -> None:
        active = 0
        maximum = 0
        lock = threading.Lock()
        start_gate = threading.Barrier(2)

        def limited(value: str) -> str:
            nonlocal active, maximum
            if value == "warm":
                return value
            with lock:
                active += 1
                maximum = max(maximum, active)
            time.sleep(0.02)
            with lock:
                active -= 1
            return value

        workflow = Workflow(id="node_concurrency")
        workflow.add_node(
            limited,
            node_id="limited",
            policy=NodePolicy(max_concurrency=1),
        )
        app = started_app()
        app.invoke(workflow, input={"value": "warm"}, session_id="warm")

        def invoke(key: str) -> None:
            start_gate.wait(timeout=2)
            app.invoke(workflow, input={"value": key}, session_id=key)

        threads = [threading.Thread(target=invoke, args=(key,)) for key in ("a", "b")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(maximum, 1)

    def test_timeout_is_reported_as_runtime_error(self) -> None:
        def slow() -> str:
            time.sleep(0.03)
            return "late"

        workflow = Workflow(id="timeout_execution")
        workflow.add_node(
            slow,
            node_id="slow",
            policy=NodePolicy(timeout=TimeoutPolicy(timeout_ms=1)),
        )

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "OPERATOR_TIMEOUT")
        self.assertEqual(invocation.node_executions[0].operator_executions[0].state, "failed")

    def test_replication_aggregates_indexed_operator_executions(self) -> None:
        workflow = Workflow(id="replication_execution")
        workflow.add_node(
            lambda: 2,
            node_id="sample",
            policy=NodePolicy(
                replication=ReplicationPolicy(
                    count=3,
                    output_aggregator=lambda ctx: sum(ctx.replica_outputs),
                    max_parallelism=2,
                )
            ),
        )

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.result, {"output": 6})
        calls = invocation.node_executions[0].operator_executions
        self.assertEqual(1, len(calls))
        self.assertEqual("replication", calls[0].kind)
        self.assertEqual(3, calls[0].summary.call_count)
        self.assertEqual(3, calls[0].summary.success_count)

    def test_map_policy_aggregates_indexed_operator_executions(self) -> None:
        workflow = Workflow(id="map_execution")
        workflow.add_node(lambda: [1, 2, 3], node_id="source")
        workflow.add_node(
            lambda value: value * value,
            node_id="square",
            policy=NodePolicy(
                map=MapPolicy(
                    item_selector=lambda ctx: [
                        {"value": item} for item in ctx.input
                    ],
                    output_aggregator=lambda ctx: tuple(ctx.item_outputs),
                    max_parallelism=2,
                )
            ),
        )
        workflow.add_edge(
            "source",
            "square",
        )

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.result, {"output": (1, 4, 9)})
        calls = invocation.latest_node_execution("square").operator_executions
        self.assertEqual(1, len(calls))
        self.assertEqual("map", calls[0].kind)
        self.assertEqual(3, calls[0].summary.call_count)
        self.assertEqual(3, calls[0].summary.success_count)

    def test_default_map_iterates_single_incoming_value(self) -> None:
        workflow = Workflow(id="default_node_map")
        workflow.add_node(
            lambda: [{"value": 1}, {"value": 2}],
            node_id="source",
        )
        workflow.add_node(
            lambda value: value * 2,
            node_id="mapped",
            policy=NodePolicy(map=MapPolicy()),
        )
        workflow.add_edge("source", "mapped")

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.result, {"output": [2, 4]})

    def test_custom_map_selector_can_map_invocation_input_at_entry(self) -> None:
        workflow = Workflow(id="entry_node_map")
        workflow.add_node(
            lambda value: value + 1,
            node_id="mapped",
            policy=NodePolicy(
                map=MapPolicy(
                    item_selector=lambda ctx: [
                        {"value": value} for value in ctx.input["items"]
                    ]
                )
            ),
        )

        invocation = started_app().invoke(
            workflow,
            input={"items": [1, 2]},
        )

        self.assertEqual(invocation.result, {"output": [2, 3]})

    def test_map_selector_receives_complete_fan_in(self) -> None:
        seen: list[tuple[tuple[str, ...], tuple[str, ...]]] = []

        def select_items(ctx):
            seen.append(
                (
                    tuple(item.edge_id for item in ctx.incoming),
                    tuple(sorted(ctx.input)),
                )
            )
            return [
                {"value": value}
                for incoming in ctx.incoming
                for value in incoming.value
            ]

        workflow = Workflow(id="map_complete_fan_in")
        workflow.add_node(lambda: {"_value": None}, node_id="start")
        workflow.add_node(lambda _value: [1, 2], node_id="left")
        workflow.add_node(lambda _value: [10, 20], node_id="right")
        workflow.add_node(
            lambda value: value,
            node_id="mapped",
            policy=NodePolicy(map=MapPolicy(item_selector=select_items)),
        )
        workflow.add_edge("start", "left")
        workflow.add_edge("start", "right")
        workflow.add_edge("left", "mapped", edge_id="left_mapped")
        workflow.add_edge("right", "mapped", edge_id="right_mapped")

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.result, {"output": [1, 2, 10, 20]})
        self.assertEqual(
            seen,
            [
                (
                    ("left_mapped", "right_mapped"),
                    ("left", "right"),
                )
            ],
        )

    def test_map_node_can_be_natural_loop_header(self) -> None:
        incoming_edges: list[str] = []

        def select_items(ctx):
            incoming_edges.extend(item.edge_id for item in ctx.incoming)
            return [{"value": value} for value in ctx.input]

        workflow = Workflow(id="map_loop_header")
        workflow.add_node(lambda: [0], node_id="start")
        workflow.add_node(
            lambda value: value + 1,
            node_id="mapped",
            policy=NodePolicy(
                map=MapPolicy(item_selector=select_items),
                resource=ResourcePolicy(max_node_executions_per_invocation=5),
            ),
        )
        workflow.add_node(
            lambda value: value,
            node_id="final",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value[0]},
        )
        workflow.add_edge("start", "mapped", edge_id="enter_loop")
        workflow.add_edge(
            "mapped",
            "mapped",
            edge_id="continue_loop",
            condition=lambda ctx: ctx.source_output[0] < 3,
        )
        workflow.add_edge(
            "mapped",
            "final",
            edge_id="exit_loop",
            condition=lambda ctx: ctx.source_output[0] >= 3,
        )

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.result, {"output": 3})
        self.assertEqual(
            incoming_edges,
            ["enter_loop", "continue_loop", "continue_loop"],
        )
        self.assertEqual(invocation.count_node_executions("mapped"), 3)

    def test_map_uses_fixed_app_worker_pool_without_policy_limit(self) -> None:
        active = 0
        peak = 0
        worker_tasks: set[str] = set()

        async def transform(value: int) -> int:
            nonlocal active, peak
            worker_tasks.add(asyncio.current_task().get_name())
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.005)
                return value * 2
            finally:
                active -= 1

        workflow = Workflow(id="bounded_default_map_workers")
        workflow.add_node(lambda: list(range(64)), node_id="source")
        workflow.add_node(
            transform,
            node_id="target",
            policy=NodePolicy(
                map=MapPolicy(
                    item_selector=lambda ctx: [
                        {"value": value} for value in ctx.input
                    ]
                )
            ),
        )
        workflow.add_edge(
            "source",
            "target",
        )

        invocation = started_app().invoke(workflow)

        self.assertEqual("completed", invocation.state)
        self.assertEqual(8, peak)
        self.assertEqual(8, len(worker_tasks))
        self.assertTrue(
            all(name.startswith("autoagent-unit-worker-") for name in worker_tasks)
        )

    def test_input_mapping_failure_bypasses_retry_and_fallback(self) -> None:
        app = started_app()
        primary_calls = 0
        fallback_calls = 0

        @app.capability("mapping_target", operator_id="mapping_primary")
        def primary(_value: int) -> int:
            nonlocal primary_calls
            primary_calls += 1
            return 1

        @app.operator("mapping_fallback", capability="mapping_target")
        def fallback(_value: int) -> int:
            nonlocal fallback_calls
            fallback_calls += 1
            return 2

        def broken_mapping(_ctx) -> int:
            raise ValueError("bad mapping")

        workflow = Workflow(id="mapping_failure")
        workflow.add_node(lambda: 1, node_id="start")
        workflow.add_node(
            CapabilityRef(id="mapping_target"),
            node_id="target",
            input_mapping=broken_mapping,
            policy=NodePolicy(retry=RetryPolicy(max_attempts=3)),
        )
        workflow.add_edge("start", "target")

        invocation = app.invoke(workflow)

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "INPUT_MAPPING_FAILED")
        self.assertEqual(primary_calls, 0)
        self.assertEqual(fallback_calls, 0)
        execution = invocation.latest_node_execution("target")
        self.assertEqual(execution.state, "failed")
        self.assertEqual(execution.operator_executions, [])

    def test_invalid_mapping_output_bypasses_retry_and_fallback(self) -> None:
        app = started_app()
        primary_calls = 0
        fallback_calls = 0

        @app.capability("validated_mapping", operator_id="validated_primary")
        def primary(value: int) -> int:
            nonlocal primary_calls
            primary_calls += 1
            return value

        @app.operator("validated_fallback", capability="validated_mapping")
        def fallback(value: int) -> int:
            nonlocal fallback_calls
            fallback_calls += 1
            return value

        workflow = Workflow(id="invalid_mapping_output")
        workflow.add_node(lambda: 1, node_id="start")
        workflow.add_node(
            CapabilityRef(id="validated_mapping"),
            node_id="target",
            input_mapping=lambda _ctx: {"value": "wrong type"},
            policy=NodePolicy(retry=RetryPolicy(max_attempts=3)),
        )
        workflow.add_edge("start", "target")

        invocation = app.invoke(workflow)
        execution = invocation.latest_node_execution("target")

        self.assertEqual(invocation.error.code, "INPUT_MAPPING_INVALID")
        self.assertEqual(primary_calls, 0)
        self.assertEqual(fallback_calls, 0)
        self.assertEqual(execution.operator_executions, [])

    def test_non_mapping_input_mapping_result_is_rejected_before_operator(self) -> None:
        operator_executions = 0

        def target(value: int) -> int:
            nonlocal operator_executions
            operator_executions += 1
            return value

        workflow = Workflow(id="non_mapping_input")
        workflow.add_node(lambda: 1, node_id="start")
        workflow.add_node(
            target,
            node_id="target",
            input_mapping=lambda _ctx: [1],
            policy=NodePolicy(retry=RetryPolicy(max_attempts=3)),
        )
        workflow.add_edge("start", "target")

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "INPUT_MAPPING_INVALID")
        self.assertEqual(operator_executions, 0)
        self.assertEqual(
            invocation.latest_node_execution("target").operator_executions,
            [],
        )

    def test_map_selector_requires_an_iterable_of_mappings(self) -> None:
        operator_executions = 0

        def target(value: int) -> int:
            nonlocal operator_executions
            operator_executions += 1
            return value

        workflow = Workflow(id="invalid_map_collection")
        workflow.add_node(lambda: [1], node_id="source")
        workflow.add_node(
            target,
            node_id="target",
            policy=NodePolicy(map=MapPolicy(item_selector=lambda _ctx: 1)),
        )
        workflow.add_edge(
            "source",
            "target",
        )

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "MAP_ITEM_SELECTION_FAILED")
        self.assertEqual(operator_executions, 0)

    def test_map_selector_rejects_non_mapping_item_before_operator(self) -> None:
        operator_executions = 0

        def target(value: int) -> int:
            nonlocal operator_executions
            operator_executions += 1
            return value

        workflow = Workflow(id="invalid_map_item")
        workflow.add_node(lambda: [1], node_id="source")
        workflow.add_node(
            target,
            node_id="target",
            policy=NodePolicy(
                retry=RetryPolicy(max_attempts=3),
                map=MapPolicy(item_selector=lambda ctx: list(ctx.input)),
            ),
        )
        workflow.add_edge(
            "source",
            "target",
        )

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "MAP_ITEM_INPUT_INVALID")
        self.assertEqual(operator_executions, 0)
        self.assertEqual(
            invocation.latest_node_execution("target").operator_executions,
            [],
        )

    def test_map_item_arguments_are_validated_before_operator(self) -> None:
        operator_executions = 0

        def target(value: int) -> int:
            nonlocal operator_executions
            operator_executions += 1
            return value

        workflow = Workflow(id="invalid_map_arguments")
        workflow.add_node(lambda: ["wrong"], node_id="source")
        workflow.add_node(
            target,
            node_id="target",
            policy=NodePolicy(
                map=MapPolicy(
                    item_selector=lambda ctx: [
                        {"value": item} for item in ctx.input
                    ]
                )
            ),
        )
        workflow.add_edge(
            "source",
            "target",
        )

        invocation = started_app().invoke(workflow)

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "MAP_ITEM_INPUT_INVALID")
        self.assertEqual(operator_executions, 0)

    def test_async_map_hooks_and_operator_are_supported(self) -> None:
        seen_contexts: list[tuple[str, str, bool]] = []

        async def select_items(ctx):
            await asyncio.sleep(0)
            seen_contexts.append(
                ("selector", ctx.node_id, ctx.outputs.has("source"))
            )
            self.assertEqual("shared", ctx.invocation_input["request"]["id"])
            ctx.invocation_input["request"]["id"] = "selector-local"
            return [{"value": item} for item in ctx.input]

        async def square(value: int) -> int:
            await asyncio.sleep(0)
            return value * value

        async def aggregate(ctx):
            await asyncio.sleep(0)
            seen_contexts.append(
                ("aggregator", ctx.node_id, ctx.outputs.has("source"))
            )
            self.assertEqual("shared", ctx.invocation_input["request"]["id"])
            return tuple(ctx.item_outputs)

        workflow = Workflow(id="async_map_hooks")
        workflow.add_node(lambda request: [1, 2, 3], node_id="source")
        workflow.add_node(
            square,
            node_id="square",
            policy=NodePolicy(
                map=MapPolicy(
                    item_selector=select_items,
                    output_aggregator=aggregate,
                )
            ),
        )
        workflow.add_edge(
            "source",
            "square",
        )

        invocation = started_app().invoke(
            workflow,
            input={"request": {"id": "shared"}},
        )

        self.assertEqual(invocation.result, {"output": (1, 4, 9)})
        self.assertEqual(
            [
                ("selector", "square", True),
                ("aggregator", "square", True),
            ],
            seen_contexts,
        )

    def test_async_map_failure_cancels_remaining_units_and_skips_aggregation(self) -> None:
        aggregated = False

        async def process(value: int) -> int:
            if value == 0:
                raise RuntimeError("first item failed")
            await asyncio.sleep(1)
            return value

        async def aggregate(ctx):
            nonlocal aggregated
            aggregated = True
            return ctx.item_outputs

        workflow = Workflow(id="map_failure_cancellation")
        workflow.add_node(lambda: [0, 1, 2], node_id="source")
        workflow.add_node(
            process,
            node_id="process",
            policy=NodePolicy(
                map=MapPolicy(
                    item_selector=lambda ctx: [
                        {"value": item} for item in ctx.input
                    ],
                    output_aggregator=aggregate,
                    max_parallelism=3,
                )
            ),
        )
        workflow.add_edge(
            "source",
            "process",
        )

        started = time.perf_counter()
        invocation = started_app().invoke(workflow)
        elapsed = time.perf_counter() - started

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "OPERATOR_CALL_FAILED")
        self.assertFalse(aggregated)
        self.assertLess(elapsed, 0.5)

    def test_async_replication_aggregator_is_supported(self) -> None:
        async def sample(value: int) -> int:
            await asyncio.sleep(0)
            return value

        async def aggregate(ctx):
            await asyncio.sleep(0)
            return sum(ctx.replica_outputs)

        workflow = Workflow(id="async_replication_aggregator")
        workflow.add_node(
            sample,
            node_id="sample",
            policy=NodePolicy(
                replication=ReplicationPolicy(
                    count=3,
                    output_aggregator=aggregate,
                )
            ),
        )

        invocation = started_app().invoke(workflow, input={"value": 2})

        self.assertEqual(invocation.result, {"output": 6})

    def test_output_binding_failure_bypasses_retry_and_fallback(self) -> None:
        app = started_app()
        primary_calls = 0
        fallback_calls = 0

        @app.capability("binding_target", operator_id="binding_primary")
        def primary() -> str:
            nonlocal primary_calls
            primary_calls += 1
            return "output"

        @app.operator("binding_fallback", capability="binding_target")
        def fallback() -> str:
            nonlocal fallback_calls
            fallback_calls += 1
            return "fallback"

        def broken_binding(ctx) -> None:
            ctx.invocation_context.data["partial"] = True
            ctx.session_context.data["partial"] = True
            raise RuntimeError("binding failed")

        workflow = Workflow(id="binding_failure")
        workflow.add_node(
            CapabilityRef(id="binding_target"),
            node_id="node",
            output_binding=broken_binding,
            policy=NodePolicy(retry=RetryPolicy(max_attempts=3)),
        )

        invocation = app.invoke(workflow, session_id="session")
        session = app.runtime_store.find_session(
            workflow_revision_id=invocation.workflow_revision_id,
            session_key="session",
        )

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(invocation.error.code, "OUTPUT_BINDING_FAILED")
        self.assertEqual(primary_calls, 1)
        self.assertEqual(fallback_calls, 0)
        calls = invocation.latest_node_execution("node").operator_executions
        self.assertEqual([call.operator_id for call in calls], ["binding_primary"])
        self.assertEqual(invocation.context.data, {})
        self.assertEqual(session.context.data, {})

    def test_condition_and_input_mapping_mutate_only_isolated_contexts(self) -> None:
        def condition(ctx) -> bool:
            with self.assertRaises(TypeError):
                ctx.invocation_context.data["condition_leak"] = True
            with self.assertRaises(TypeError):
                ctx.session_context.data["condition_leak"] = True
            return True

        def mapping(ctx) -> dict[str, int]:
            with self.assertRaises(TypeError):
                ctx.invocation_context.data["mapping_leak"] = True
            with self.assertRaises(TypeError):
                ctx.session_context.data["mapping_leak"] = True
            return {"value": ctx.incoming[0].value}

        workflow = Workflow(id="isolated_read_hooks")
        workflow.add_node(lambda value: value, node_id="source")
        workflow.add_node(
            lambda value: value + 1,
            node_id="target",
            input_mapping=mapping,
        )
        workflow.add_edge(
            "source",
            "target",
            condition=condition,
        )
        app = started_app()

        invocation = app.invoke(
            workflow,
            input={"value": 1},
            session_id="session",
        )
        session = app.runtime_store.find_session(
            workflow_revision_id=invocation.workflow_revision_id,
            session_key="session",
        )

        self.assertEqual({"output": 2}, invocation.result)
        self.assertEqual({}, invocation.context.data)
        self.assertEqual({}, session.context.data)

    def test_parallel_output_bindings_reject_overlapping_context_paths(self) -> None:
        def bind_a(ctx) -> None:
            ctx.invocation_context.data["shared"] = "a"

        def bind_b(ctx) -> None:
            ctx.invocation_context.data["shared"] = "b"

        workflow = Workflow(id="parallel_context_conflict")
        workflow.add_node(lambda: "start", node_id="start")
        workflow.add_node(
            lambda: "a",
            node_id="a",
            input_mapping=lambda _ctx: {},
            output_binding=bind_a,
        )
        workflow.add_node(
            lambda: "b",
            node_id="b",
            input_mapping=lambda _ctx: {},
            output_binding=bind_b,
        )
        workflow.add_edge("start", "a")
        workflow.add_edge("start", "b")

        invocation = started_app().invoke(workflow)

        failed = [
            execution
            for execution in invocation.node_executions
            if execution.error is not None
            and execution.error.code == "OUTPUT_BINDING_FAILED"
        ]
        self.assertEqual("failed", invocation.state)
        self.assertEqual(1, len(failed))
        self.assertIn("Concurrent Context write", failed[0].error.message)
        self.assertIn(invocation.context.data["shared"], {"a", "b"})

    def test_parallel_output_bindings_merge_disjoint_context_paths(self) -> None:
        def bind_a(ctx) -> None:
            ctx.invocation_context.data["a"] = ctx.output

        def bind_b(ctx) -> None:
            ctx.invocation_context.data["b"] = ctx.output

        workflow = Workflow(id="parallel_context_merge")
        workflow.add_node(lambda: "start", node_id="start")
        workflow.add_node(
            lambda: 1,
            node_id="a",
            input_mapping=lambda _ctx: {},
            output_binding=bind_a,
        )
        workflow.add_node(
            lambda: 2,
            node_id="b",
            input_mapping=lambda _ctx: {},
            output_binding=bind_b,
        )
        workflow.add_edge("start", "a")
        workflow.add_edge("start", "b")

        invocation = started_app().invoke(workflow)

        self.assertEqual("completed", invocation.state)
        self.assertEqual({"a": 1, "b": 2}, invocation.context.data)

    def test_serial_output_bindings_can_overwrite_the_same_context_path(self) -> None:
        def bind(ctx) -> None:
            ctx.invocation_context.data["value"] = ctx.output

        workflow = Workflow(id="serial_context_overwrite")
        workflow.add_node(lambda: 1, node_id="first", output_binding=bind)
        workflow.add_node(
            lambda value: value + 1,
            node_id="second",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
            output_binding=bind,
        )
        workflow.add_edge("first", "second")

        invocation = started_app().invoke(workflow)

        self.assertEqual("completed", invocation.state)
        self.assertEqual({"value": 2}, invocation.context.data)

    def test_unselected_entries_are_skipped_before_fan_in(self) -> None:
        workflow = Workflow(id="selected_multi_entry")
        workflow.add_node(lambda: {"value": "A"}, node_id="a")
        workflow.add_node(lambda: {"value": "B"}, node_id="b")
        workflow.add_node(lambda value: value, node_id="join")
        workflow.add_edge("a", "join", edge_id="a_join")
        workflow.add_edge("b", "join", edge_id="b_join")

        invocation = started_app().invoke(workflow, entry_node_id="a")

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.result, {"output": "A"})
        self.assertIn("b", invocation.scheduler.skipped_node_instances)
        self.assertEqual(invocation.scheduler.edge_resolutions["b_join"].state, "skipped")


if __name__ == "__main__":
    unittest.main()
