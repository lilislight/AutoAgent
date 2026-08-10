from __future__ import annotations

import asyncio
import threading
import unittest
from unittest.mock import patch

from autoagent.core import (
    AggregationContext,
    AutoAgentApp,
    BackoffPolicy,
    ContextPatch,
    Edge,
    EdgeConditionContext,
    EventMode,
    InputMappingContext,
    MapPolicy,
    Node,
    NodePolicy,
    Operator,
    OutputBindingContext,
    ReplicationPolicy,
    RetryPolicy,
    RuntimeEvent,
    TimeoutPolicy,
    WaitOperator,
    Workflow,
)
from autoagent.core.runtime import (
    RuntimeState,
    SerializedCheckpoint,
    SerializedEvent,
)


def identity(value: int) -> int:
    return value


def double(value: int) -> int:
    return value * 2


def increment(value: int) -> int:
    return value + 1


def add_five(value: int) -> int:
    return value + 5


class RecordingSink:
    def __init__(self) -> None:
        self.events: list[RuntimeEvent] = []

    async def wait_until_admissible(self) -> None:
        return None

    async def submit_events(self, events: tuple[SerializedEvent, ...]) -> None:
        self.events.extend(
            event.decode() for event in events if event.channel == "runtime"
        )

    def offer_checkpoint(self, checkpoint: SerializedCheckpoint) -> None:
        del checkpoint


class FullEventReplayEquivalenceTests(unittest.TestCase):
    def _create(
        self,
        workflow: Workflow,
        invocation_input: object,
        *,
        session_id: str = "session",
    ) -> tuple[AutoAgentApp, RecordingSink, object, dict[str, object]]:
        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink, max_executor_concurrency=8)
        app.register_workflow(workflow)
        execution = app._runtime.run(
            app._create_execution(
                workflow,
                invocation_input,
                session_id=session_id,
                event_mode=EventMode.FULL,
                stream=None,
            )
        )
        return app, sink, execution, execution.runtime_state.checkpoint_record()

    def _launch_to_boundary(self, app: AutoAgentApp, execution: object) -> None:
        async def launch_and_wait() -> None:
            app._launch(execution)
            await execution.boundary.wait()

        app._runtime.run(launch_and_wait())

    def _assert_replays(
        self,
        *,
        execution: object,
        sink: RecordingSink,
        genesis: dict[str, object],
    ) -> None:
        final_record = execution.runtime_state.checkpoint_record()
        replayed = RuntimeState.from_checkpoint_record(genesis)
        events = [
            event
            for event in sink.events
            if event.invocation_id == execution.invocation.id
        ]
        batches = [batch for event in events for batch in event.operation_batches]
        self.assertTrue(batches)
        versions = [batch.state_version for batch in batches]
        for batch in batches:
            self.assertEqual(
                batch.state_version,
                replayed.state_version + 1,
                f"non-contiguous Full Event batches: {versions}",
            )
            applied = replayed.apply(batch.operations)
            self.assertEqual(applied.to_record(), batch.to_record())
        self.assertEqual(replayed.checkpoint_record(), final_record)

    def _run_and_assert(self, workflow: Workflow, invocation_input: object) -> None:
        app, sink, execution, genesis = self._create(workflow, invocation_input)
        try:
            self._launch_to_boundary(app, execution)
            self.assertTrue(execution.invocation.state.terminal)
            self._assert_replays(execution=execution, sink=sink, genesis=genesis)
        finally:
            app.close()

    def test_parallel_fan_in_and_non_conflicting_context_writes_replay(self) -> None:
        def increment(value: int) -> int:
            return value + 1

        def double(value: int) -> int:
            return value * 2

        def bind_left(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(invocation={"left": context.output})

        def bind_right(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(session={"right": context.output})

        def sum_incoming(context: InputMappingContext) -> int:
            return sum(item.value for item in context.incoming)

        workflow = Workflow(
            "replay-parallel-fan-in",
            nodes=[
                Node("start", identity),
                Node("left", increment, output_binding=bind_left),
                Node("right", double, output_binding=bind_right),
                Node("join", identity, input_mapping=sum_incoming),
            ],
            edges=[
                Edge("start", "left"),
                Edge("start", "right"),
                Edge("left", "join"),
                Edge("right", "join"),
            ],
        )
        self._run_and_assert(workflow, 3)

    def test_parallel_context_conflict_failure_replays(self) -> None:
        release = threading.Barrier(2)

        def synchronized(value: int) -> int:
            release.wait(timeout=2)
            return value

        def same_path(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(invocation={"shared": context.node_id})

        workflow = Workflow(
            "replay-context-conflict",
            nodes=[
                Node("left", synchronized, output_binding=same_path),
                Node("right", synchronized, output_binding=same_path),
            ],
        )
        self._run_and_assert(workflow, 1)

    def test_loop_scope_scheduler_and_compaction_replay(self) -> None:
        def increment(value: int) -> int:
            return value + 1

        def continue_loop(context: EdgeConditionContext) -> bool:
            return context.source_output < 3

        def exit_loop(context: EdgeConditionContext) -> bool:
            return context.source_output >= 3

        workflow = Workflow(
            "replay-loop",
            nodes=[
                Node("start", identity),
                Node("header", identity),
                Node("body", increment),
                Node("finish", identity),
            ],
            edges=[
                Edge("start", "header"),
                Edge("header", "body"),
                Edge("body", "header", condition=continue_loop),
                Edge("body", "finish", condition=exit_loop),
            ],
        )
        self._run_and_assert(workflow, 0)

    def test_nested_loop_scopes_replay(self) -> None:
        def increment_inner(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(
                invocation={
                    "inner": int(context.invocation_context.get("inner", 0)) + 1
                }
            )

        def advance_outer(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(
                invocation={
                    "outer": int(context.invocation_context.get("outer", 0)) + 1,
                    "inner": 0,
                }
            )

        def inner_back(context: EdgeConditionContext) -> bool:
            return int(context.invocation_context.get("inner", 0)) < 2

        def inner_exit(context: EdgeConditionContext) -> bool:
            return int(context.invocation_context.get("inner", 0)) >= 2

        def outer_back(context: EdgeConditionContext) -> bool:
            return int(context.invocation_context.get("outer", 0)) < 2

        def outer_exit(context: EdgeConditionContext) -> bool:
            return int(context.invocation_context.get("outer", 0)) >= 2

        workflow = Workflow(
            "replay-nested-loops",
            nodes=[
                Node("entry", identity),
                Node("header", identity),
                Node("inner_latch", identity, output_binding=increment_inner),
                Node("outer_latch", identity, output_binding=advance_outer),
                Node("outside", identity),
            ],
            edges=[
                Edge("entry", "header"),
                Edge("header", "inner_latch"),
                Edge("inner_latch", "header", condition=inner_back),
                Edge("inner_latch", "outer_latch", condition=inner_exit),
                Edge("outer_latch", "header", condition=outer_back),
                Edge("outer_latch", "outside", condition=outer_exit),
            ],
        )
        self._run_and_assert(workflow, 0)

    def test_map_replication_retry_fallback_and_timeout_replay(self) -> None:
        def sum_outputs(context: AggregationContext) -> int:
            return sum(context.operator_outputs)

        mapped = Workflow(
            "replay-map",
            nodes=[
                Node(
                    "mapped",
                    double,
                    policy=NodePolicy(
                        map=MapPolicy(max_parallelism=2, output_aggregator=sum_outputs)
                    ),
                )
            ],
        )
        self._run_and_assert(mapped, [1, 2, 3])

        replicated = Workflow(
            "replay-replication",
            nodes=[
                Node(
                    "replicas",
                    increment,
                    policy=NodePolicy(replication=ReplicationPolicy(count=3)),
                )
            ],
        )
        self._run_and_assert(replicated, 1)

        calls = 0

        def primary(value: int) -> int:
            nonlocal calls
            calls += 1
            raise ValueError("primary failed")

        retry_fallback = Workflow(
            "replay-retry-fallback",
            nodes=[
                Node(
                    "call",
                    Operator(primary, id="primary"),
                    fallback_operators=(Operator(add_five, id="fallback"),),
                    policy=NodePolicy(
                        retry=RetryPolicy(
                            max_attempts=2,
                            backoff=BackoffPolicy(initial_delay_ms=0),
                        )
                    ),
                )
            ],
        )
        self._run_and_assert(retry_fallback, 2)
        self.assertEqual(calls, 2)

        async def slow(value: int) -> int:
            await asyncio.sleep(0.05)
            return value

        timeout = Workflow(
            "replay-timeout",
            nodes=[
                Node("slow", slow, policy=NodePolicy(timeout=TimeoutPolicy(2)))
            ],
        )
        self._run_and_assert(timeout, 1)

    def test_wait_resume_replays_one_contiguous_state_history(self) -> None:
        workflow = Workflow(
            "replay-wait-resume",
            nodes=[Node("approval", WaitOperator(str, str))],
        )
        app, sink, execution, genesis = self._create(workflow, "approve")
        try:
            self._launch_to_boundary(app, execution)
            self.assertEqual(execution.invocation.state.value, "waiting")
            wait_id = execution.invocation.waits[0].id
            app.resume(execution.invocation, wait_id, "approved")
            self.assertEqual(execution.invocation.state.value, "completed")
            self._assert_replays(execution=execution, sink=sink, genesis=genesis)
        finally:
            app.close()

    def test_cancellation_replays_terminal_convergence(self) -> None:
        started = threading.Event()

        async def blocked(value: int) -> int:
            started.set()
            await asyncio.sleep(60)
            return value

        workflow = Workflow("replay-cancel", nodes=[Node("node", blocked)])
        app, sink, execution, genesis = self._create(workflow, 1)
        try:
            async def launch() -> None:
                app._launch(execution)

            app._runtime.run(launch())
            self.assertTrue(started.wait(1))
            app.cancel(execution.invocation)
            self.assertEqual(execution.invocation.state.value, "cancelled")
            self._assert_replays(execution=execution, sink=sink, genesis=genesis)
        finally:
            app.close()

    def test_two_sessions_replay_independently_when_events_interleave(self) -> None:
        async def yield_once(value: int) -> int:
            await asyncio.sleep(0)
            return value + 1

        workflow = Workflow("replay-sessions", nodes=[Node("node", yield_once)])
        sink = RecordingSink()
        app = AutoAgentApp(runtime_sink=sink, max_executor_concurrency=2)
        app.register_workflow(workflow)
        executions = [
            app._runtime.run(
                app._create_execution(
                    workflow,
                    value,
                    session_id=f"session-{value}",
                    event_mode=EventMode.FULL,
                    stream=None,
                )
            )
            for value in (1, 2)
        ]
        genesis = [item.runtime_state.checkpoint_record() for item in executions]
        try:
            async def launch_all() -> None:
                for execution in executions:
                    app._launch(execution)
                await asyncio.gather(*(item.boundary.wait() for item in executions))

            app._runtime.run(launch_all())
            for execution, initial in zip(executions, genesis, strict=True):
                self._assert_replays(
                    execution=execution, sink=sink, genesis=initial
                )
        finally:
            app.close()

    def test_one_capture_gap_carries_applied_operations_to_next_event(self) -> None:
        workflow = Workflow("replay-capture-gap", nodes=[Node("node", identity)])
        app, sink, execution, genesis = self._create(workflow, 3)
        original = RuntimeEvent.detached
        calls = 0

        def fail_once(*args: object, **kwargs: object) -> RuntimeEvent:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise TypeError("synthetic capture failure")
            return original(*args, **kwargs)

        try:
            with (
                patch.object(RuntimeEvent, "detached", side_effect=fail_once),
                self.assertLogs(
                    "autoagent.core.executor.workflow_executor", level="ERROR"
                ),
            ):
                self._launch_to_boundary(app, execution)
            self._assert_replays(execution=execution, sink=sink, genesis=genesis)
        finally:
            app.close()


if __name__ == "__main__":
    unittest.main()
