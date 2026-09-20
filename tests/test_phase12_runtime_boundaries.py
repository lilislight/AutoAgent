from __future__ import annotations

from tests.graph_fixtures import (
    async_resume_graph_wait,
    child_refs,
    join_observed,
    resume_graph_wait,
    status_observed,
)

import asyncio
import threading
import time
import unittest

from pydantic import BaseModel, ConfigDict
from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp,
    ConditionContext,
    ContextOperation,
    ContextPatch,
    Edge,
    InputMappingContext,
    Map,
    Node,
    OutputBindingContext,
    Wait,
    Workflow,
)


class Value(TypedDict):
    value: int


class Batch(TypedDict):
    items: list[Value]


class Count(TypedDict):
    count: int


class ModelValue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: int


def identity(value: Value) -> Value:
    return value


def count_activations(context: InputMappingContext) -> Count:
    return {"count": len(context.incoming)}


def accept_count(value: Count) -> Count:
    return value


def first_value(context: InputMappingContext) -> Value:
    return next(iter(context.incoming.values()))  # type: ignore[return-value]


def map_items(context: InputMappingContext) -> list[Value]:
    return context.invocation_input["items"]  # type: ignore[index,return-value]


def map_model(context: InputMappingContext) -> ModelValue:
    return ModelValue.model_validate(context.invocation_input)


def increment_model(value: ModelValue) -> ModelValue:
    return ModelValue(value=value.value + 1)


def always_true(_context: ConditionContext) -> bool:
    return True


def always_false(_context: ConditionContext) -> bool:
    return False


def _wait_until(predicate, timeout: float = 1.0) -> bool:  # type: ignore[no-untyped-def]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.002)
    return bool(predicate())


class RuntimeBoundaryRegressionTests(unittest.TestCase):
    def test_parallel_completion_does_not_retry_binding_or_condition(self) -> None:
        """Verify live Context contention never invokes completion Hooks twice."""

        left_binding_started = threading.Event()
        right_binding_started = threading.Event()
        binding_calls = {1: 0, 2: 0}
        condition_calls = {1: 0, 2: 0}

        def bind(context: OutputBindingContext) -> ContextPatch:
            value = context.output["value"]  # type: ignore[index]
            binding_calls[value] += 1
            if value == 1 and binding_calls[value] == 1:
                left_binding_started.set()
                # Under the old optimistic loop the right Hook entered and
                # committed while this Hook used an older Context snapshot,
                # forcing this exact Hook to run a second time.  The completion
                # lock now prevents the right Hook from entering this window.
                if right_binding_started.wait(0.2):
                    time.sleep(0.05)
            elif value == 2:
                right_binding_started.set()
            return ContextPatch(
                invocation=(ContextOperation.set(f"value_{value}", value),)
            )

        def route(context: ConditionContext) -> bool:
            value = context.output["value"]  # type: ignore[index]
            condition_calls[value] += 1
            return True

        async def left(value: Value) -> Value:
            return {"value": 1}

        async def right(value: Value) -> Value:
            await asyncio.to_thread(left_binding_started.wait, 1)
            return {"value": 2}

        workflow = Workflow(
            "parallel-hooks-exactly-once",
            nodes=[
                Node("start", identity),
                Node("left", left, output_binding=bind),
                Node("right", right, output_binding=bind),
                Node("left-end", identity),
                Node("right-end", identity),
            ],
            edges=[
                Edge("start", "left"),
                Edge("start", "right"),
                Edge("left", "left-end", route),
                Edge("right", "right-end", route),
            ],
        )
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {"value": 0})
            self.assertEqual(result.status, "completed")
            self.assertEqual(binding_calls, {1: 1, 2: 1})
            self.assertEqual(condition_calls, {1: 1, 2: 1})
        finally:
            app.close()

    def test_parallel_error_join_commits_both_failures_once(self) -> None:
        """Verify parallel Error routes cannot lose a Join input to stale state."""

        arrived = 0

        async def fail_branch(value: Value) -> Value:
            await asyncio.sleep(0)
            raise RuntimeError(f"failed-{value['value']}")

        async def synchronize_error_routes(_context: ConditionContext) -> bool:
            nonlocal arrived
            arrived += 1
            await asyncio.sleep(0)
            return True

        workflow = Workflow(
            "parallel-error-join",
            nodes=[
                Node("start", identity),
                Node("left", fail_branch),
                Node("right", fail_branch),
                Node("join", accept_count, input_mapping=count_activations),
            ],
            edges=[
                Edge("start", "left"),
                Edge("start", "right"),
                Edge("left", "join", synchronize_error_routes, on="error"),
                Edge("right", "join", synchronize_error_routes, on="error"),
            ],
        )
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {"value": 1})
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"count": 2})
            self.assertEqual(arrived, 2)
        finally:
            app.close()

    def test_waiting_child_and_parent_sibling_reach_wait_boundary(self) -> None:
        """Verify a waiting Child remains a valid parent boundary after a sibling ends."""

        slow_started = threading.Event()
        release_slow = threading.Event()

        def slow(value: Value) -> Value:
            slow_started.set()
            release_slow.wait(2)
            return value

        child = Workflow(
            "await-sibling-child",
            nodes=[Node("approval", Wait(Value, Value))],
        )
        parent = Workflow(
            "await-sibling-parent",
            nodes=[
                Node("start", identity),
                Node("child", child),
                Node("slow", slow),
            ],
            edges=[Edge("start", "child"), Edge("start", "slow")],
        )
        app = AutoAgentApp()
        try:
            submitted = app.submit_invoke(parent, {"value": 1})

            def child_is_waiting() -> bool:
                handles = child_refs(app, submitted.ref)
                return bool(handles) and status_observed(app, handles[0]).status == "waiting"

            self.assertTrue(slow_started.wait(1))
            self.assertTrue(_wait_until(child_is_waiting))
            release_slow.set()
            result = join_observed(app, submitted.ref, timeout=1)
            self.assertEqual(result.status, "waiting")
        finally:
            release_slow.set()
            app.close()

    def test_child_completion_wakes_parent_before_unrelated_sibling(self) -> None:
        """Verify Child completion wakes its parent without waiting for a sibling."""

        slow_started = threading.Event()
        release_slow = threading.Event()
        after_started = threading.Event()

        def slow(value: Value) -> Value:
            slow_started.set()
            release_slow.wait(2)
            return value

        def after(value: Value) -> Value:
            after_started.set()
            return value

        child = Workflow(
            "wake-parent-child",
            nodes=[Node("approval", Wait(Value, Value))],
        )
        parent = Workflow(
            "wake-parent",
            nodes=[
                Node("start", identity),
                Node("child", child),
                Node("slow", slow),
                Node("after", after),
            ],
            edges=[
                Edge("start", "child"),
                Edge("start", "slow"),
                Edge("child", "after"),
            ],
        )
        app = AutoAgentApp()
        try:
            submitted = app.submit_invoke(parent, {"value": 1})

            def waiting_child():  # type: ignore[no-untyped-def]
                handles = child_refs(app, submitted.ref)
                if not handles:
                    return None
                result = status_observed(app, handles[0])
                return result if result.status == "waiting" else None

            self.assertTrue(slow_started.wait(1))
            self.assertTrue(_wait_until(lambda: waiting_child() is not None))
            child_result = waiting_child()
            assert child_result is not None
            resume_graph_wait(app,
                child_result.ref,
                child_result.waits[0].id,
                {"value": 2},
            )
            self.assertTrue(
                after_started.wait(0.5),
                "Parent did not run Child continuation while its sibling was active.",
            )
            release_slow.set()
            self.assertEqual(join_observed(app, submitted.ref, timeout=1).status, "completed")
        finally:
            release_slow.set()
            app.close()

    def test_child_ready_during_parent_quiescence_restarts_drive(self) -> None:
        """Verify Child readiness cannot be lost during parent quiescence checks."""

        child = Workflow(
            "quiescence-race-child",
            nodes=[Node("approval", Wait(Value, Value))],
        )
        parent = Workflow(
            "quiescence-race-parent",
            nodes=[Node("child", child)],
        )
        app = AutoAgentApp()
        finish_entered = threading.Event()
        gate_holder: dict[str, asyncio.Event] = {}
        original_finish = app._workflow_executor._finish_if_quiescent
        paused = False

        async def pause_parent_finish(workflow, session_id):  # type: ignore[no-untyped-def]
            nonlocal paused
            if session_id != "quiescence-race-root" or paused:
                return await original_finish(workflow, session_id)
            paused = True
            gate = asyncio.Event()
            gate_holder["gate"] = gate
            finish_entered.set()
            await gate.wait()
            return await original_finish(workflow, session_id)

        app._workflow_executor._finish_if_quiescent = pause_parent_finish  # type: ignore[method-assign]

        async def release_gate() -> None:
            gate_holder["gate"].set()

        try:
            submitted = app.submit_invoke(
                parent,
                {"value": 1},
                session_id="quiescence-race-root",
            )
            self.assertTrue(finish_entered.wait(1))
            handles = child_refs(app, submitted.ref)
            self.assertEqual(len(handles), 1)
            child_result = status_observed(app, handles[0])
            self.assertEqual(child_result.status, "waiting")

            resumed = resume_graph_wait(app,
                child_result.ref,
                child_result.waits[0].id,
                {"value": 2},
            )
            self.assertEqual(resumed.status, "completed")
            app._runtime_loop.run(release_gate())

            result = join_observed(app, submitted.ref, timeout=1)
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"value": 2})
        finally:
            gate = gate_holder.get("gate")
            if gate is not None and not gate.is_set():
                app._runtime_loop.run(release_gate())
            app.close()

    def test_parent_fail_fast_cancels_awaited_child(self) -> None:
        """Verify fail-fast parent failure promptly cancels its awaited Child."""

        child_started = threading.Event()
        child_cancelled = threading.Event()

        async def long_child(value: Value) -> Value:
            child_started.set()
            try:
                await asyncio.sleep(10)
                return value
            except asyncio.CancelledError:
                child_cancelled.set()
                raise

        async def fail_after_child_started(_value: Value) -> Value:
            while not child_started.is_set():
                await asyncio.sleep(0)
            raise RuntimeError("parent branch failed")

        child = Workflow(
            "fail-fast-cancel-child",
            nodes=[Node("work", long_child)],
        )
        parent = Workflow(
            "fail-fast-cancel-parent",
            nodes=[
                Node("start", identity),
                Node("child", child),
                Node("failure", fail_after_child_started),
            ],
            edges=[Edge("start", "child"), Edge("start", "failure")],
        )
        app = AutoAgentApp()
        try:
            result = app.invoke(parent, {"value": 1})
            self.assertEqual(result.status, "failed")
            handles = child_refs(app, result.ref)
            self.assertEqual(len(handles), 1)
            self.assertTrue(
                _wait_until(
                    lambda: status_observed(app, handles[0]).status == "cancelled"
                )
            )
            self.assertTrue(child_cancelled.wait(0.5))
        finally:
            app.close()

    def test_pydantic_values_cross_wait_and_child_boundaries(self) -> None:
        """Verify Pydantic values remain durable across Wait and Child boundaries."""

        app = AutoAgentApp()
        try:
            waiting = app.invoke(
                Workflow(
                    "pydantic-wait",
                    nodes=[Node("approval", Wait(ModelValue, ModelValue))],
                ),
                {"value": 1},
            )
            self.assertEqual(waiting.status, "waiting")
            resumed = resume_graph_wait(app,
                waiting.ref,
                waiting.waits[0].id,
                ModelValue(value=2),
            )
            self.assertEqual(resumed.status, "completed")
            self.assertEqual(resumed.output, {"value": 2})

            child = Workflow(
                "pydantic-child",
                nodes=[Node("increment", increment_model)],
            )
            parent = Workflow(
                "pydantic-parent",
                nodes=[Node("child", child, input_mapping=map_model)],
            )
            result = app.invoke(parent, {"value": 3})
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"value": 4})
        finally:
            app.close()

    def test_child_map_keeps_parallel_limit_after_wait_resume(self) -> None:
        """Verify resumed Child Map units still obey Node Map parallelism."""

        active = 0
        peak = 0

        async def work(value: Value) -> Value:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.05)
                return value
            finally:
                active -= 1

        child = Workflow(
            "resumed-map-child",
            nodes=[
                Node("approval", Wait(Value, Value)),
                Node("work", work),
            ],
            edges=[Edge("approval", "work")],
        )
        parent = Workflow(
            "resumed-map-parent",
            nodes=[
                Node(
                    "children",
                    child,
                    input_mapping=map_items,
                    map=Map(max_parallelism=1),
                )
            ],
        )
        app = AutoAgentApp()
        try:
            root = app.invoke(
                parent,
                {"items": [{"value": 1}, {"value": 2}]},
            )
            self.assertEqual(root.status, "waiting")
            handles = child_refs(app, root.ref)
            self.assertEqual(len(handles), 2)
            children = [status_observed(app, handle) for handle in handles]
            self.assertTrue(all(item.status == "waiting" for item in children))

            async def resume_both() -> None:
                await asyncio.gather(
                    *(
                        async_resume_graph_wait(app,
                            item.ref,
                            item.waits[0].id,
                            {"value": index + 10},
                        )
                        for index, item in enumerate(children)
                    )
                )

            asyncio.run(resume_both())
            self.assertEqual(join_observed(app, root.ref, timeout=1).status, "completed")
            self.assertEqual(peak, 1)
        finally:
            app.close()

    def test_deferred_loop_exit_activation_keeps_original_source(self) -> None:
        """Verify a deferred Loop Exit can activate after the boundary resolves."""

        workflow = Workflow(
            "deferred-loop-activation",
            nodes=[
                Node("start", identity),
                Node("header", identity),
                Node("left", identity),
                Node("right", identity),
                Node("join", identity, input_mapping=first_value),
                Node("finish", identity),
            ],
            edges=[
                Edge("start", "header"),
                Edge("header", "left"),
                Edge("header", "right"),
                Edge("left", "finish", always_true, id="exit"),
                Edge("left", "join", always_false, id="left-join"),
                Edge("right", "join", id="right-join"),
                Edge("join", "header", always_false, id="back"),
            ],
        )
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {"value": 1})
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"value": 1})
        finally:
            app.close()

    def test_loop_header_rejects_continue_and_exit_before_side_effects(self) -> None:
        """Verify a Loop header rejects simultaneous Continue and Exit early."""

        body_ran = threading.Event()
        finish_ran = threading.Event()

        def body(value: Value) -> Value:
            body_ran.set()
            return value

        def finish(value: Value) -> Value:
            finish_ran.set()
            return value

        workflow = Workflow(
            "header-route-conflict",
            nodes=[
                Node("start", identity),
                Node("header", identity),
                Node("a", body),
                Node("b", identity),
                Node("finish", finish),
            ],
            edges=[
                Edge("start", "header"),
                Edge("header", "a", always_true, id="to-a"),
                Edge("a", "header", always_false, id="back-a"),
                Edge("header", "b", always_false, id="to-b"),
                Edge("b", "header", always_false, id="back-b"),
                Edge("header", "finish", always_true, id="exit"),
            ],
        )
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {"value": 1})
            self.assertEqual(result.status, "failed")
            self.assertFalse(body_ran.is_set())
            self.assertFalse(finish_ran.is_set())
        finally:
            app.close()

    def test_loop_routing_does_not_mask_original_node_failure(self) -> None:
        """Verify Loop finalization preserves the Node's original failure."""

        def fail(_value: Value) -> Value:
            raise ValueError("original failure")

        workflow = Workflow(
            "loop-original-failure",
            nodes=[
                Node("start", identity),
                Node("header", fail),
                Node("finish", identity),
            ],
            edges=[
                Edge("start", "header"),
                Edge("header", "header", always_false, id="back"),
                Edge("header", "finish", always_false, id="exit"),
            ],
        )
        app = AutoAgentApp()
        try:
            result = app.invoke(workflow, {"value": 1})
            self.assertEqual(result.status, "failed")
            self.assertIsNotNone(result.error)
            assert result.error is not None
            self.assertEqual(result.error.type, "ValueError")
            self.assertEqual(result.error.message, "original failure")
        finally:
            app.close()

    def test_selected_entry_skips_unreachable_loop_component(self) -> None:
        """Verify an unselected Entry does not force its Loop to resolve."""

        workflow = Workflow(
            "multi-entry-skips-loop",
            nodes=[
                Node("entry", identity),
                Node("out", identity),
                Node("loop-entry", identity),
                Node("header", identity),
                Node("loop-out", identity),
            ],
            edges=[
                Edge("entry", "out"),
                Edge("loop-entry", "header"),
                Edge("header", "header", always_false, id="back"),
                Edge("header", "loop-out", always_false, id="exit"),
            ],
        )
        app = AutoAgentApp()
        try:
            result = app.invoke(
                workflow,
                {"value": 1},
                entry_node_id="entry",
            )
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output, {"out": {"value": 1}})
        finally:
            app.close()

    def test_child_wait_failure_cancels_sibling_and_converges_parent(self) -> None:
        """Verify a resumed Child failure settles a waiting Child Map sibling."""

        def work(value: Value) -> Value:
            if value["value"] == 1:
                raise RuntimeError("child failed after wait")
            return value

        child = Workflow(
            "wait-failure-child",
            nodes=[
                Node("approval", Wait(Value, Value)),
                Node("work", work),
            ],
            edges=[Edge("approval", "work")],
        )
        parent = Workflow(
            "wait-failure-parent",
            nodes=[
                Node(
                    "children",
                    child,
                    input_mapping=map_items,
                    map=Map(max_parallelism=2),
                )
            ],
        )
        app = AutoAgentApp()
        try:
            root = app.invoke(
                parent,
                {"items": [{"value": 1}, {"value": 2}]},
            )
            self.assertEqual(root.status, "waiting")
            handles = child_refs(app, root.ref)
            children = [status_observed(app, handle) for handle in handles]
            self.assertEqual(len(children), 2)
            self.assertTrue(all(item.status == "waiting" for item in children))

            failed_child = resume_graph_wait(app,
                children[0].ref,
                children[0].waits[0].id,
                {"value": 1},
            )
            self.assertEqual(failed_child.status, "failed")
            parent_result = join_observed(app, root.ref, timeout=1)
            self.assertEqual(parent_result.status, "failed")
            self.assertCountEqual(
                [status_observed(app, handle).status for handle in handles],
                ["failed", "cancelled"],
            )
        finally:
            app.close()



    def test_cancel_awaited_child_map_converges_every_unit(self) -> None:
        """Verify Root cancellation converges every awaited Map unit."""

        child = Workflow(
            "cancel-awaited-map-child",
            nodes=[Node("approval", Wait(Value, Value))],
        )
        parent = Workflow(
            "cancel-awaited-map-parent",
            nodes=[
                Node(
                    "children",
                    child,
                    input_mapping=map_items,
                    map=Map(max_parallelism=2),
                )
            ],
        )
        app = AutoAgentApp()
        try:
            root = app.invoke(
                parent,
                {"items": [{"value": 1}, {"value": 2}]},
            )
            handles = child_refs(app, root.ref)
            self.assertEqual(len(handles), 2)
            app.cancel(root.ref)
            self.assertEqual(join_observed(app, root.ref, timeout=1).status, "cancelled")
            self.assertEqual(
                [status_observed(app, handle).status for handle in handles],
                ["cancelled", "cancelled"],
            )
        finally:
            app.close()


if __name__ == "__main__":
    unittest.main()
