from __future__ import annotations

import unittest
from typing import Any

from autoagent.core import (
    AutoAgentApp,
    ContextPatch,
    Edge,
    EventMode,
    InputMappingContext,
    EdgeConditionContext,
    OutputBindingContext,
    InvocationState,
    Node,
    NodePolicy,
    RecoveryPolicy,
    WaitOperator,
    Workflow,
)
from tests.helpers import decode_checkpoint, decode_events, identity_int, uppercase


def inner_value(context: InputMappingContext) -> int:
    return int(context.invocation_context.get("inner", 0))


def add_one(value: int) -> int:
    return value + 1


def collect_max(values: dict[str, int]) -> int:
    return max(values.values())


class _CheckpointSink:
    def __init__(self) -> None:
        self.events: list[Any] = []
        self.checkpoints: list[Any] = []

    async def wait_until_admissible(self) -> None:
        return None

    async def submit_events(self, events: tuple[Any, ...]) -> None:
        self.events.extend(decode_events(events))

    def offer_checkpoint(self, checkpoint: Any) -> None:
        self.checkpoints.append(decode_checkpoint(checkpoint))

def replay_safe() -> NodePolicy:
    return NodePolicy(recovery=RecoveryPolicy(mode="replay_safe", max_attempts=2))


class LoopAndRecoveryTests(unittest.TestCase):
    def test_nested_loops_get_independent_iteration_scopes(self) -> None:
        def inner_body(value: int) -> int:
            return value + 1

        def bind_inner(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(invocation={"inner": context.output})

        def repeat_inner(context: EdgeConditionContext) -> bool:
            return context.invocation_context["inner"] < 2

        def leave_inner(context: EdgeConditionContext) -> bool:
            return context.invocation_context["inner"] >= 2

        def outer_latch(value: int) -> int:
            return value

        def bind_outer(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(
                invocation={
                    "outer": int(context.invocation_context.get("outer", 0)) + 1,
                    "inner": 0,
                }
            )

        def repeat_outer(context: EdgeConditionContext) -> bool:
            return context.invocation_context["outer"] < 2

        def leave_outer(context: EdgeConditionContext) -> bool:
            return context.invocation_context["outer"] >= 2

        workflow = Workflow(
            "nested-loops",
            nodes=[
                Node("start", identity_int),
                Node("outer_header", identity_int),
                Node("inner_header", identity_int),
                Node(
                    "inner_body",
                    inner_body,
                    input_mapping=inner_value,
                    output_binding=bind_inner,
                ),
                Node("outer_latch", outer_latch, output_binding=bind_outer),
                Node("finish", identity_int),
            ],
            edges=[
                Edge("start", "outer_header"),
                Edge("outer_header", "inner_header"),
                Edge("inner_header", "inner_body"),
                Edge("inner_body", "inner_header", repeat_inner, id="inner_back"),
                Edge("inner_body", "outer_latch", leave_inner, id="inner_exit"),
                Edge("outer_latch", "outer_header", repeat_outer, id="outer_back"),
                Edge("outer_latch", "finish", leave_outer, id="outer_exit"),
            ],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, 0)
        self.assertEqual(invocation.result(), {"finish": 2})
        app.close()

    def test_parallel_fan_out_join_can_loop_back_to_header(self) -> None:
        def increment_iteration(context: OutputBindingContext) -> ContextPatch:
            return ContextPatch(invocation={"iteration": context.output})

        def continue_loop(context: EdgeConditionContext) -> bool:
            return context.invocation_context["iteration"] < 3

        def exit_loop(context: EdgeConditionContext) -> bool:
            return context.invocation_context["iteration"] >= 3

        workflow = Workflow(
            "parallel-loop",
            nodes=[
                Node("start", identity_int),
                Node("header", identity_int),
                Node("left", add_one),
                Node("right", add_one),
                Node(
                    "collect",
                    collect_max,
                    output_binding=increment_iteration,
                ),
                Node("finish", identity_int),
            ],
            edges=[
                Edge("start", "header"),
                Edge("header", "left"),
                Edge("header", "right"),
                Edge("left", "collect"),
                Edge("right", "collect"),
                Edge("collect", "header", continue_loop, id="back"),
                Edge("collect", "finish", exit_loop, id="exit"),
            ],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, 0, event_mode=EventMode.FULL)
        self.assertEqual(invocation.result(), {"finish": 3})
        app.close()

    def test_loop_default_input_uses_exact_activation_not_stale_static_node_output(self) -> None:
        values: list[int] = []

        def body(value: int) -> int:
            values.append(value)
            return value + 1

        def more(context: EdgeConditionContext) -> bool:
            return context.output < 4

        def done(context: EdgeConditionContext) -> bool:
            return context.output >= 4

        workflow = Workflow(
            "exact-loop-input",
            nodes=[
                Node("start", identity_int),
                Node("header", identity_int),
                Node("body", body),
                Node("finish", identity_int),
            ],
            edges=[
                Edge("start", "header"),
                Edge("header", "body"),
                Edge("body", "header", more),
                Edge("body", "finish", done),
            ],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        self.assertEqual(app.invoke(workflow, 1).result(), {"finish": 4})
        self.assertEqual(values, [1, 2, 3])
        app.close()

    def test_recover_runnable_checkpoint_obeys_node_recovery_policy(self) -> None:
        safe_workflow = Workflow(
            "recover-safe",
            nodes=[Node("run", add_one, policy=replay_safe())],
        )
        sink = _CheckpointSink()
        app = AutoAgentApp(runtime_sink=sink)
        app.register_workflow(safe_workflow)
        completed = app.invoke(safe_workflow, 1)
        initial = sink.checkpoints[0]
        self.assertEqual(completed.state, InvocationState.COMPLETED)
        app.close()

        recovered_app = AutoAgentApp()
        recovered_app.register_workflow(safe_workflow)
        recovered = recovered_app.recover(initial)
        self.assertEqual(recovered.result(), {"run": 2})
        self.assertEqual(recovered.latest_checkpoint.invocation_state, "completed")
        recovered_app.close()

        blocked_workflow = Workflow(
            "recover-blocked", nodes=[Node("run", identity_int)]
        )
        sink = _CheckpointSink()
        app = AutoAgentApp(runtime_sink=sink)
        app.register_workflow(blocked_workflow)
        app.invoke(blocked_workflow, 1)
        blocked_checkpoint = sink.checkpoints[0]
        app.close()
        recovered_app = AutoAgentApp()
        recovered_app.register_workflow(blocked_workflow)
        blocked = recovered_app.recover(blocked_checkpoint)
        self.assertEqual(blocked.state, InvocationState.FAILED)
        self.assertIn("does not permit", blocked.error.message)
        recovered_app.close()

    def test_wait_checkpoint_restores_same_invocation_and_sequence(self) -> None:
        workflow = Workflow(
            "wait-recovery",
            nodes=[
                Node("ask", WaitOperator(str, str)),
                Node("finish", uppercase, policy=replay_safe()),
            ],
            edges=[Edge("ask", "finish")],
        )
        first = AutoAgentApp()
        first.register_workflow(workflow)
        waiting = first.invoke(workflow, "question", session_id="session")
        checkpoint = waiting.latest_checkpoint
        checkpoint = type(checkpoint).from_record(checkpoint.to_record())
        first.close()

        second = AutoAgentApp()
        second.register_workflow(workflow)
        recovered = second.recover(checkpoint)
        self.assertEqual(recovered.id, waiting.id)
        self.assertEqual(recovered.state, InvocationState.WAITING)
        completed = second.resume(recovered, recovered.waits[0].id, "answer")
        self.assertEqual(completed.result(), {"finish": "ANSWER"})
        self.assertEqual(completed.latest_checkpoint.invocation_state, "completed")
        second.close()


if __name__ == "__main__":
    unittest.main()
