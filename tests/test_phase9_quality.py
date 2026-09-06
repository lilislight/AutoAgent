from __future__ import annotations

import json
import itertools
import asyncio
import threading
import time
import unittest
from collections.abc import Iterator
from pydantic import BaseModel, ConfigDict
from typing_extensions import TypedDict

from autoagent import (
    AppCheckpoint,
    AutoAgentApp,
    Capability,
    ConditionContext,
    ContextOperation,
    ContextPatch,
    Edge,
    InputMappingContext,
    InvocationUpdate,
    Map,
    Node,
    Operator,
    OutputBindingContext,
    SessionCheckpoint,
    RuntimeTransitionError,
    Stream,
    StreamContext,
    UserEvent,
    UserEventMapping,
    Workflow,
    WorkflowCompiler,
)
from autoagent.core import (
    InMemoryEventJournal,
    InMemoryUserEventJournal,
    NodeExecutor,
    Scheduler,
)
from autoagent.core.context import apply_context_operation
from autoagent.core.runtime.values import freeze


class Value(TypedDict):
    value: int


class ErrorValue(TypedDict):
    message: str


class Chunk(TypedDict):
    value: int


class SumState(TypedDict):
    total: int


class Total(TypedDict):
    total: int


class ModelValue(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: int


def identity(value: Value) -> Value:
    return value


def model_increment(value: ModelValue) -> ModelValue:
    return ModelValue(value=value.value + 1)


def route(_context: ConditionContext) -> bool:
    return True


def never(_context: ConditionContext) -> bool:
    return False


def bind_route_flag(_context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(
        invocation=(ContextOperation.set("route.enabled", True),)
    )


def map_output_event(context: OutputBindingContext) -> Value:
    return context.output  # type: ignore[return-value]


def route_from_bound_context(context: ConditionContext) -> bool:
    return bool(context.invocation_context["route"]["enabled"])  # type: ignore[index]


def fail(_value: Value) -> Value:
    raise RuntimeError("failed")


def error_mapping(context: InputMappingContext) -> ErrorValue:
    error = next(iter(context.incoming.values()))
    return {"message": error["message"]}  # type: ignore[index]


def accept_error(value: ErrorValue) -> ErrorValue:
    return value


def chunks(value: Value) -> Iterator[Chunk]:
    for number in range(value["value"]):
        yield {"value": number}


class SumReducer:
    def initial(self, _context: StreamContext) -> SumState:
        return {"total": 0}

    def add(
        self, _context: StreamContext, state: SumState, chunk: Chunk
    ) -> SumState:
        return {"total": state["total"] + chunk["value"]}

    def finish(self, _context: StreamContext, state: SumState) -> Total:
        return {"total": state["total"]}


class QualityTests(unittest.TestCase):
    def test_app_operator_limit_covers_sync_async_and_distinct_workflows(self) -> None:
        """Verify one App limit covers sync and async Operators across Invocations."""

        active = 0
        peak = 0
        lock = threading.Lock()
        saturated = threading.Event()
        release = threading.Event()

        def enter() -> None:
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
                if active == 2:
                    saturated.set()

        def leave() -> None:
            nonlocal active
            with lock:
                active -= 1

        def sync_handler(value: Value) -> Value:
            enter()
            try:
                release.wait(2)
                return value
            finally:
                leave()

        async def async_handler(value: Value) -> Value:
            enter()
            try:
                while not release.is_set():
                    await asyncio.sleep(0.005)
                return value
            finally:
                leave()

        app = AutoAgentApp(max_operator_concurrency=2)
        try:
            submitted = [
                app.submit_invoke(
                    Workflow(
                        f"global-limit-{index}",
                        nodes=[
                            Node(
                                "node",
                                sync_handler if index % 2 == 0 else async_handler,
                            )
                        ],
                    ),
                    {"value": index},
                )
                for index in range(4)
            ]
            self.assertTrue(saturated.wait(1))
            time.sleep(0.03)
            self.assertEqual(peak, 2)
            release.set()
            results = [app.join(item.ref, 2) for item in submitted]
            self.assertTrue(all(item.status == "completed" for item in results))
            self.assertEqual(peak, 2)
        finally:
            release.set()
            app.close()

    def test_app_operator_limit_applies_inside_child_workflow_map(self) -> None:
        """Verify mapped Child Workflows share their App's Operator capacity."""

        active = 0
        peak = 0
        lock = threading.Lock()

        async def child_handler(value: Value) -> Value:
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                await asyncio.sleep(0.02)
                return value
            finally:
                with lock:
                    active -= 1

        def child_inputs(context: InputMappingContext) -> list[Value]:
            return context.invocation_input["items"]  # type: ignore[index,return-value]

        app = AutoAgentApp(max_operator_concurrency=2)
        try:
            child = Workflow("limited-child", nodes=[Node("work", child_handler)])
            parent = Workflow(
                "limited-parent",
                nodes=[
                    Node(
                        "children",
                        child,
                        input_mapping=child_inputs,
                        map=Map(max_parallelism=4),
                    )
                ],
            )
            result = app.invoke(
                parent,
                {"items": [{"value": index} for index in range(6)]},
            )
            self.assertEqual(result.status, "completed")
            self.assertEqual(peak, 2)
        finally:
            app.close()

    def test_cancelled_sync_operator_keeps_capacity_until_thread_exits(self) -> None:
        """Verify logical cancellation cannot oversubscribe a live sync call."""

        sync_started = threading.Event()
        release_sync = threading.Event()
        async_started = threading.Event()

        def blocking(value: Value) -> Value:
            sync_started.set()
            release_sync.wait(2)
            return value

        async def following(value: Value) -> Value:
            async_started.set()
            return value

        app = AutoAgentApp(max_operator_concurrency=1)
        try:
            first = app.submit_invoke(
                Workflow("cancelled-capacity", nodes=[Node("node", blocking)]),
                {"value": 1},
            )
            self.assertTrue(sync_started.wait(1))
            self.assertEqual(app.cancel(first.ref).status, "cancelled")
            second = app.submit_invoke(
                Workflow("following-capacity", nodes=[Node("node", following)]),
                {"value": 2},
            )
            time.sleep(0.03)
            self.assertFalse(async_started.is_set())
            release_sync.set()
            self.assertEqual(app.join(second.ref, 2).status, "completed")
            self.assertTrue(async_started.is_set())
        finally:
            release_sync.set()
            app.close()

    def test_node_output_can_emit_non_canonical_user_event(self) -> None:
        """Verify node output can emit non canonical user event."""
        journal = InMemoryEventJournal()
        app = AutoAgentApp(runtime_journal=journal)
        try:
            items = list(app.stream(
                Workflow(
                    "mapped-user-event",
                    nodes=[
                        Node(
                            "node",
                            identity,
                            user_events=(
                                UserEventMapping("node.output", map_output_event),
                            ),
                        )
                    ],
                ),
                {"value": 4},
            ))
            result = items[-1]
            events = [item.event for item in items if isinstance(item, InvocationUpdate)]
            self.assertEqual(
                [(event.kind, event.payload) for event in events],
                [("node.output", {"value": 4})],
            )
        finally:
            app.close()

    def test_app_uses_injected_core_ports(self) -> None:
        """Verify app uses injected core ports."""
        class RuntimeJournal(InMemoryEventJournal):
            appended = 0

            def append(self, event):
                self.appended += 1
                return super().append(event)

        class UserJournal(InMemoryUserEventJournal):
            emitted = 0

            def emit(self, **values):
                self.emitted += 1
                return super().emit(**values)

        class PlanningScheduler(Scheduler):
            initialized = 0

            def initialize(self, workflow, state):
                self.initialized += 1
                return super().initialize(workflow, state)

        class ExecutingNodeExecutor(NodeExecutor):
            executed = 0

            async def execute(self, *args, **kwargs):
                self.executed += 1
                return await super().execute(*args, **kwargs)

        runtime = RuntimeJournal()
        users = UserJournal()
        scheduler = PlanningScheduler()
        executor = ExecutingNodeExecutor()
        clock = itertools.count(1).__next__
        app = AutoAgentApp(
            runtime_journal=runtime,
            user_event_journal=users,
            scheduler=scheduler,
            node_executor=executor,
            clock_ns=clock,
        )
        try:
            result = app.invoke(
                Workflow(
                    "ports",
                    nodes=[Node("stream", chunks, stream=Stream(SumReducer()))],
                ),
                {"value": 2},
            )
            self.assertEqual(result.status, "completed")
            self.assertGreater(runtime.appended, 0)
            self.assertEqual(users.emitted, 2)
            self.assertEqual(scheduler.initialized, 1)
            self.assertEqual(executor.executed, 1)
        finally:
            app.close()

    def test_condition_sees_context_candidate_from_same_node_binding(self) -> None:
        """Verify condition sees context candidate from same node binding."""
        journal = InMemoryEventJournal()
        app = AutoAgentApp(runtime_journal=journal)
        try:
            workflow = Workflow(
                "binding-before-condition",
                nodes=[
                    Node("start", identity, output_binding=bind_route_flag),
                    Node("finish", identity),
                ],
                edges=[Edge("start", "finish", route_from_bound_context)],
            )
            result = app.invoke(workflow, {"value": 1})
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"value": 1})
        finally:
            app.close()

    def test_stream_chunks_are_separate_ordered_user_events(self) -> None:
        """Verify stream chunks are separate ordered user events."""
        app = AutoAgentApp()
        try:
            items = list(app.stream(
                Workflow(
                    "user-events",
                    nodes=[Node("stream", chunks, stream=Stream(SumReducer()))],
                ),
                {"value": 3},
            ))
            result = items[-1]
            events = [item.event for item in items if isinstance(item, InvocationUpdate)]
            self.assertEqual(result.output, {"total": 3})
            self.assertEqual(
                [event.payload for event in events],
                [{"value": 0}, {"value": 1}, {"value": 2}],
            )
            self.assertEqual([event.sequence for event in events], [1, 2, 3])
            for event in events:
                self.assertEqual(UserEvent.from_record(event.to_record()), event)
        finally:
            app.close()

    def test_pydantic_values_are_normalized_at_event_boundaries(self) -> None:
        """Verify pydantic values are normalized at event boundaries."""
        app = AutoAgentApp()
        try:
            result = app.invoke(
                Workflow("pydantic", nodes=[Node("node", model_increment)]),
                {"value": 1},
            )
            self.assertEqual(result.output, {"value": 2})
            json.dumps(result.output)
        finally:
            app.close()

    def test_error_edge_handles_failure_and_receives_error_mapping(self) -> None:
        """Verify error edge handles failure and receives error mapping."""
        app = AutoAgentApp()
        try:
            workflow = Workflow(
                "error-route",
                nodes=[
                    Node("start", fail),
                    Node("handled", accept_error, input_mapping=error_mapping),
                ],
                edges=[Edge("start", "handled", on="error")],
            )
            result = app.invoke(workflow, {"value": 1})
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"message": "failed"})
        finally:
            app.close()

    def test_loop_no_route_and_execution_limit_fail_deterministically(self) -> None:
        """Verify loop no route and execution limit fail deterministically."""
        no_route = AutoAgentApp()
        try:
            workflow = Workflow(
                "no-route",
                nodes=[Node(name, identity) for name in ("start", "header", "finish")],
                edges=[
                    Edge("start", "header"),
                    Edge("header", "header", never, id="back"),
                    Edge("header", "finish", never, id="exit"),
                ],
            )
            result = no_route.invoke(workflow, {"value": 1})
            self.assertEqual(result.status, "failed")
            self.assertIn("LOOP_NO_ROUTE", result.error.message)
        finally:
            no_route.close()

        limited = AutoAgentApp(max_node_executions_per_invocation=3)
        try:
            workflow = Workflow(
                "limited",
                nodes=[
                    Node("start", identity),
                    Node("header", identity),
                    Node("finish", identity),
                ],
                edges=[
                    Edge("start", "header"),
                    Edge("header", "header", route, id="back"),
                    Edge("header", "finish", never, id="exit"),
                ],
            )
            result = limited.invoke(workflow, {"value": 1})
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.error.type, "InvocationExecutionLimitExceeded")
        finally:
            limited.close()

    def test_capability_requires_explicit_resolution_when_ambiguous(self) -> None:
        """Verify capability requires explicit resolution when ambiguous."""
        one = Operator(identity, id="one")

        def second(value: Value) -> Value:
            return {"value": value["value"] + 1}

        two = Operator(second, id="two")
        workflow = Workflow(
            "capability",
            nodes=[Node("node", Capability("choice", one.contract))],
        )
        unresolved = AutoAgentApp()
        try:
            unresolved.register_operator(one, capability_id="choice")
            unresolved.register_operator(two, capability_id="choice")
            result = unresolved.invoke(workflow, {"value": 1})
            self.assertEqual(result.status, "failed")
            self.assertIn("CapabilityResolver", result.error.message)
        finally:
            unresolved.close()

        resolved = AutoAgentApp(capability_resolver=lambda _capability, _value: two)
        try:
            resolved.register_operator(one, capability_id="choice")
            resolved.register_operator(two, capability_id="choice")
            self.assertEqual(resolved.invoke(workflow, {"value": 1}).output, {"value": 2})
        finally:
            resolved.close()

    def test_registered_default_capability_operator_is_selectable_and_disableable(self) -> None:
        """Verify registered default capability operator is selectable and disableable."""
        base = Operator(identity, id="base")

        def dynamic(value: Value) -> Value:
            return {"value": value["value"] + 10}

        app = AutoAgentApp()
        try:
            app.register_operator(base, capability_id="choice")
            app.register_operator(
                dynamic,
                operator_id="dynamic",
                capability_id="choice",
                default=True,
            )
            workflow = Workflow(
                "dynamic-capability",
                nodes=[Node("node", Capability("choice", base.contract))],
            )
            self.assertEqual(app.invoke(workflow, {"value": 1}).output, {"value": 11})
            app.set_operator_enabled("dynamic", False)
            self.assertEqual(app.invoke(workflow, {"value": 1}).output, {"value": 1})
        finally:
            app.close()

    def test_dynamic_capability_operator_must_preserve_nominal_contract(self) -> None:
        """Verify dynamic capability operator must preserve nominal contract."""
        def wrong(value: ErrorValue) -> ErrorValue:
            return value

        app = AutoAgentApp()
        try:
            previous = app.register_workflow(
                Workflow(
                    "bad-dynamic-capability",
                    nodes=[Node("previous", identity)],
                )
            )
            app.register_operator(wrong, capability_id="choice", operator_id="wrong")
            rejected = Workflow(
                "bad-dynamic-capability",
                nodes=[
                    Node(
                        "node",
                        Capability(
                            "choice", Operator(identity, id="base").contract
                        ),
                    )
                ],
            )
            rejected_ir = app._compiler.compile(rejected).require_workflow_ir()
            with self.assertRaisesRegex(ValueError, "does not match Capability"):
                app.register_workflow(rejected)
            snapshot = app.workflow_definition_snapshot("bad-dynamic-capability")
            self.assertEqual(snapshot.workflow_revision_id, previous.workflow_revision_id)
            with self.assertRaisesRegex(
                RuntimeTransitionError, "WORKFLOW_NOT_REGISTERED"
            ):
                app.workflow_definition_snapshot(
                    rejected_ir.workflow_revision_id
                )
            self.assertEqual(
                app.invoke("bad-dynamic-capability", {"value": 3}).output,
                {"value": 3},
            )
        finally:
            app.close()

    def test_unique_highest_priority_capability_operator_is_selected(self) -> None:
        """Verify unique highest priority capability operator is selected."""
        def preferred(value: Value) -> Value:
            return {"value": value["value"] + 20}

        app = AutoAgentApp()
        try:
            base = Operator(identity, id="base")
            app.register_operator(base, capability_id="priority-choice")
            app.register_operator(
                preferred,
                capability_id="priority-choice",
                operator_id="preferred",
                priority=5,
            )
            workflow = Workflow(
                "priority-capability",
                nodes=[
                    Node(
                        "node",
                        Capability("priority-choice", base.contract),
                    )
                ],
            )
            self.assertEqual(app.invoke(workflow, {"value": 1}).output, {"value": 21})
        finally:
            app.close()


    def test_app_close_returns_recoverable_state_without_business_cancel(self) -> None:
        """Verify clean close quiesces tasks and returns current checkpoint state."""
        started = threading.Event()

        async def blocking(value: Value) -> Value:
            started.set()
            await asyncio.sleep(10)
            return value

        journal = InMemoryEventJournal()
        app = AutoAgentApp(runtime_journal=journal)
        submitted = app.submit_invoke(
            Workflow("close-convergence", nodes=[Node("node", blocking)]),
            {"value": 1},
        )
        self.assertTrue(started.wait(1))
        checkpoint = app.close()
        state = checkpoint.sessions[0].state
        self.assertEqual(state.invocation.status, "running")
        self.assertFalse(
            any(
                call.status == "running"
                for call in state.invocation.scheduler.operator_calls.values()
            )
        )

    def test_idle_app_does_not_precreate_operator_workers(self) -> None:
        """Verify idle app does not precreate operator workers."""
        before = sum(
            thread.name.startswith("autoagent-operator-")
            for thread in threading.enumerate()
        )
        app = AutoAgentApp(max_operator_concurrency=16)
        try:
            after = sum(
                thread.name.startswith("autoagent-operator-")
                for thread in threading.enumerate()
            )
            self.assertEqual(after, before)
        finally:
            app.close()

    def test_cancelling_ainvoke_cancels_runtime_invocation(self) -> None:
        """Verify cancelling ainvoke cancels runtime invocation."""
        started = threading.Event()
        cancelled = threading.Event()

        async def blocking(value: Value) -> Value:
            started.set()
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled.set()
                raise
            return value

        async def run() -> None:
            journal = InMemoryEventJournal()
            app = AutoAgentApp(runtime_journal=journal)
            try:
                task = asyncio.create_task(
                    app.ainvoke(
                        Workflow("caller-cancel", nodes=[Node("node", blocking)]),
                        {"value": 1},
                        session_id="caller-cancel-session",
                    )
                )
                while not started.is_set():
                    await asyncio.sleep(0.01)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                for _ in range(100):
                    if cancelled.is_set():
                        break
                    await asyncio.sleep(0.01)
                state = journal.state("caller-cancel-session")
                self.assertTrue(cancelled.is_set())
                self.assertEqual(state.invocation.status, "cancelled")
            finally:
                await app.aclose()

        asyncio.run(run())

    def test_context_path_copy_preserves_unchanged_branch_identity(self) -> None:
        """Verify context path copy preserves unchanged branch identity."""
        context = freeze(
            {
                "large": {"items": list(range(1_000))},
                "small": {"old": 1},
            }
        )
        large = context["large"]  # type: ignore[index]
        updated = apply_context_operation(
            context,
            ContextOperation.set("small.new", 2),
        )
        self.assertIs(updated["large"], large)  # type: ignore[index]

    def test_child_handle_is_valid_after_checkpoint_load_in_new_app(self) -> None:
        """Verify a Child handle is rebuilt from a loaded Runtime graph."""
        child = Workflow("durable-child", nodes=[Node("node", identity)])
        parent = Workflow(
            "durable-parent",
            nodes=[Node("child", child, execution_mode="spawn")],
        )
        first_app = AutoAgentApp()
        result = first_app.invoke(parent, {"value": 1})
        handle = result.output
        child_result = first_app.join(handle)
        checkpoint = AppCheckpoint(
            (
                first_app.unload_session(child_result.ref),
                first_app.unload_session(result.ref),
            )
        )
        first_app.close()

        second_app = AutoAgentApp()
        try:
            second_app.register_workflow(parent)
            second_app.load_checkpoint(checkpoint)
            status = second_app.status(handle)
            self.assertEqual(status.status, "completed")
            self.assertEqual(
                second_app.child_invocations(result.ref),
                (handle,),
            )
        finally:
            second_app.close()

    def test_workflow_failure_modes_control_parallel_siblings(self) -> None:
        """Verify workflow failure modes control parallel siblings."""
        async def failing(value: Value) -> Value:
            await asyncio.sleep(0.02)
            raise RuntimeError("failed")

        def workflow(mode: str, finished: threading.Event, cancelled: threading.Event):
            async def slow(value: Value) -> Value:
                try:
                    await asyncio.sleep(0.15)
                    finished.set()
                    return value
                except asyncio.CancelledError:
                    cancelled.set()
                    raise

            return Workflow(
                f"failure-{mode}",
                nodes=[
                    Node("start", identity),
                    Node("fail", failing),
                    Node("slow", slow),
                ],
                edges=[Edge("start", "fail"), Edge("start", "slow")],
                failure_mode=mode,  # type: ignore[arg-type]
            )

        fast_finished = threading.Event()
        fast_cancelled = threading.Event()
        app = AutoAgentApp()
        try:
            result = app.invoke(
                workflow("fail_fast", fast_finished, fast_cancelled),
                {"value": 1},
            )
            self.assertEqual(result.status, "failed")
            self.assertTrue(fast_cancelled.is_set())
            self.assertFalse(fast_finished.is_set())
        finally:
            app.close()

        continued_finished = threading.Event()
        continued_cancelled = threading.Event()
        app = AutoAgentApp()
        try:
            result = app.invoke(
                workflow(
                    "continue_active_branches",
                    continued_finished,
                    continued_cancelled,
                ),
                {"value": 1},
            )
            self.assertEqual(result.status, "failed")
            self.assertTrue(continued_finished.is_set())
            self.assertFalse(continued_cancelled.is_set())
        finally:
            app.close()

    def test_compiler_success_path_compiles_each_node_once(self) -> None:
        """Verify compiler success path compiles each node once."""
        class CountingCompiler(WorkflowCompiler):
            count = 0

            def _compile_node(self, node, parent_workflows):
                self.count += 1
                return super()._compile_node(node, parent_workflows)

        compiler = CountingCompiler()
        result = compiler.compile(
            Workflow("compile-once", nodes=[Node("node", identity)])
        )
        self.assertTrue(result.ok)
        self.assertEqual(compiler.count, 1)

    def test_existing_session_rejects_new_session_context(self) -> None:
        """Verify Session Context can only be supplied when opening the Session."""
        app = AutoAgentApp()
        try:
            workflow = Workflow("cursor", nodes=[Node("node", identity)])
            first = app.invoke(
                workflow,
                {"value": 1},
                session_id="cursor-session",
                session_context={"version": 1},
            )
            with self.assertRaisesRegex(
                RuntimeTransitionError, "SESSION_CONTEXT_ALREADY_OPEN"
            ):
                app.invoke(
                    workflow,
                    {"value": 2},
                    session_id="cursor-session",
                    session_context={"version": 2},
                )
            incremental = app.join(first.ref)
            self.assertFalse(hasattr(incremental, "trace_events"))
            self.assertFalse(hasattr(incremental, "user_events"))
        finally:
            app.close()

    def test_result_omits_events_and_checkpoint(self) -> None:
        """Keep events and checkpoint capture out of Invocation results."""
        app = AutoAgentApp()
        try:
            workflow = Workflow("event-batch", nodes=[Node("node", identity)])
            first = app.invoke(
                workflow, {"value": 1}, session_id="event-batch-session"
            )
            self.assertFalse(hasattr(first, "events"))
            self.assertFalse(hasattr(first, "trace_events"))
            self.assertFalse(hasattr(first, "user_events"))
            self.assertFalse(hasattr(first, "checkpoint"))
            self.assertFalse(hasattr(app, "checkpoint"))
            self.assertFalse(hasattr(app, "acheckpoint"))
            checkpoint = app.unload_session(first.ref)
            rebuilt = SessionCheckpoint.from_record(
                checkpoint.to_record()
            )
            self.assertEqual(
            rebuilt.state.invocation.output,
                {"value": 1},
            )
            recovered_app = AutoAgentApp()
            try:
                recovered_app.register_workflow(workflow)
                loaded = recovered_app.load_checkpoint(checkpoint)
                recovered = recovered_app.join(loaded.invocations[0])
                self.assertEqual(recovered.output, {"value": 1})
            finally:
                recovered_app.close()

            second = app.invoke(
                workflow, {"value": 2}, session_id=first.session_id
            )
            self.assertEqual(second.output, {"value": 2})
        finally:
            app.close()

    def test_stale_invocation_ref_cannot_control_replaced_invocation(self) -> None:
        """Verify control methods cannot silently target a newer Invocation."""
        app = AutoAgentApp()
        try:
            workflow = Workflow("ref-validation", nodes=[Node("node", identity)])
            first = app.invoke(
                workflow,
                {"value": 1},
                session_id="ref-session",
            )
            app.invoke(workflow, {"value": 2}, session_id="ref-session")
            with self.assertRaisesRegex(
                RuntimeTransitionError, "INVOCATION_REF_STALE"
            ):
                app.join(first.ref)
        finally:
            app.close()


if __name__ == "__main__":
    unittest.main()
