"""Parallel scheduling and Wait conformance tests from ``workflow.md``."""

from __future__ import annotations

from dataclasses import dataclass
import threading
import time
import unittest

from workflow_spec_support import (
    AutoAgentApp,
    ContextPatch,
    Edge,
    InputMappingContext,
    OutputBindingContext,
    InvocationState,
    Payload,
    assert_completed,
    assert_runtime_error,
    conditional_false,
    conditional_true,
    node,
    traced_operator,
    workflow,
)
from autoagent import Node, WaitOperator


@dataclass(frozen=True, slots=True)
class ApprovalRequest:
    prompt: str


@dataclass(frozen=True, slots=True)
class ApprovalResponse:
    approved: bool


def make_approval_request(context: InputMappingContext) -> ApprovalRequest:
    del context
    return ApprovalRequest(prompt="approve")


def record_approval(
    context: OutputBindingContext,
) -> ContextPatch:
    return ContextPatch(invocation={"approved": context.output.approved})


def wait_for_state(invocation, state: InvocationState, timeout: float = 3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snapshot = invocation.snapshot()
        if snapshot.state == state:
            return snapshot
        if snapshot.state in {
            InvocationState.COMPLETED,
            InvocationState.FAILED,
            InvocationState.CANCELLED,
        }:
            break
        time.sleep(0.01)
    snapshot = invocation.snapshot()
    raise AssertionError(f"expected {state.value}, got {snapshot.state.value}: {snapshot.error}")


class IncrementalParallelSchedulingTests(unittest.TestCase):
    def test_fast_branch_schedules_downstream_without_ready_batch_barrier(self) -> None:
        trace: list[str] = []
        slow_started = threading.Event()
        release_slow = threading.Event()
        downstream_started = threading.Event()
        definition = workflow(
            "no_ready_batch_barrier",
            [],
            [
                Edge("root", "fast", id="root_fast"),
                Edge("root", "slow", id="root_slow"),
                Edge("fast", "after_fast", id="fast_after"),
            ],
            nodes=[
                node("root", traced_operator("root", trace)),
                node("fast", traced_operator("fast", trace)),
                node(
                    "slow",
                    traced_operator(
                        "slow",
                        trace,
                        started=slow_started,
                        release=release_slow,
                    ),
                ),
                node(
                    "after_fast",
                    traced_operator(
                        "after_fast",
                        trace,
                        started=downstream_started,
                    ),
                ),
            ],
        )
        app = AutoAgentApp(max_executor_concurrency=4)
        try:
            app.register_workflow(definition)
            invocation = app.submit_invoke(definition.id, {"route": 1})
            self.assertTrue(slow_started.wait(1.5), "slow branch did not start")
            self.assertTrue(
                downstream_started.wait(1.5),
                "fast branch downstream was blocked by unrelated slow branch",
            )
            release_slow.set()
            invocation.wait(timeout=3.0)
            assert_completed(self, invocation.snapshot())
        finally:
            release_slow.set()
            app.close()

    def test_explicit_join_waits_for_complete_scoped_fan_in(self) -> None:
        trace: list[str] = []
        slow_started = threading.Event()
        release_slow = threading.Event()
        join_started = threading.Event()
        definition = workflow(
            "explicit_join_waits",
            [],
            [
                Edge("root", "fast", id="root_fast"),
                Edge("root", "slow", id="root_slow"),
                Edge("fast", "join", id="fast_join"),
                Edge("slow", "join", id="slow_join"),
            ],
            nodes=[
                node("root", traced_operator("root", trace)),
                node("fast", traced_operator("fast", trace)),
                node(
                    "slow",
                    traced_operator(
                        "slow",
                        trace,
                        started=slow_started,
                        release=release_slow,
                    ),
                ),
                node(
                    "join",
                    traced_operator("join", trace, started=join_started),
                ),
            ],
        )
        app = AutoAgentApp(max_executor_concurrency=4)
        try:
            app.register_workflow(definition)
            invocation = app.submit_invoke(definition.id, {"route": 1})
            self.assertTrue(slow_started.wait(1.5))
            self.assertFalse(join_started.wait(0.15), "Join ran before slow input resolved")
            release_slow.set()
            self.assertTrue(join_started.wait(1.5), "Join did not run after fan-in resolved")
            invocation.wait(timeout=3.0)
            assert_completed(self, invocation.snapshot())
            self.assertEqual(trace.count("join"), 1)
        finally:
            release_slow.set()
            app.close()


class ParallelLoopBoundaryTests(unittest.TestCase):
    def _early_exit_workflow(
        self,
        trace: list[str],
        slow_started: threading.Event,
        release_slow: threading.Event,
        outside_a_started: threading.Event,
        *,
        join_takes_back: bool,
    ):
        back_condition = conditional_true if join_takes_back else conditional_false
        join_exit_condition = conditional_false if join_takes_back else conditional_true
        return workflow(
            f"early_exit_{'back' if join_takes_back else 'commit'}",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "a", id="header_a"),
                Edge("header", "b", id="header_b"),
                Edge("a", "outside_a", condition=conditional_true, id="a_exit"),
                Edge("a", "join", condition=conditional_false, id="a_join"),
                Edge("b", "join", id="b_join"),
                Edge("join", "header", condition=back_condition, id="back"),
                Edge("join", "outside_join", condition=join_exit_condition, id="join_exit"),
            ],
            nodes=[
                node("entry", traced_operator("entry", trace)),
                node("header", traced_operator("header", trace)),
                node("a", traced_operator("a", trace)),
                node(
                    "b",
                    traced_operator(
                        "b",
                        trace,
                        started=slow_started,
                        release=release_slow,
                    ),
                ),
                node("join", traced_operator("join", trace)),
                node(
                    "outside_a",
                    traced_operator(
                        "outside_a",
                        trace,
                        started=outside_a_started,
                    ),
                ),
                node("outside_join", traced_operator("outside_join", trace)),
            ],
        )

    def test_early_exit_remains_pending_until_other_branch_settles(self) -> None:
        trace: list[str] = []
        slow_started = threading.Event()
        release_slow = threading.Event()
        outside_a_started = threading.Event()
        definition = self._early_exit_workflow(
            trace,
            slow_started,
            release_slow,
            outside_a_started,
            join_takes_back=False,
        )
        app = AutoAgentApp(max_executor_concurrency=6)
        try:
            app.register_workflow(definition)
            invocation = app.submit_invoke(definition.id, {"route": 1})
            self.assertTrue(slow_started.wait(1.5))
            self.assertFalse(
                outside_a_started.wait(0.15),
                "Loop Exit target ran before the parallel boundary stabilized",
            )
            release_slow.set()
            invocation.wait(timeout=3.0)
            assert_completed(self, invocation.snapshot())
            self.assertEqual(trace.count("outside_a"), 1)
            self.assertEqual(trace.count("outside_join"), 1)
        finally:
            release_slow.set()
            app.close()

    def test_pending_exit_and_later_back_fail_without_partial_exit(self) -> None:
        trace: list[str] = []
        slow_started = threading.Event()
        release_slow = threading.Event()
        outside_a_started = threading.Event()
        definition = self._early_exit_workflow(
            trace,
            slow_started,
            release_slow,
            outside_a_started,
            join_takes_back=True,
        )
        app = AutoAgentApp(max_executor_concurrency=6)
        try:
            app.register_workflow(definition)
            invocation = app.submit_invoke(definition.id, {"route": 1})
            self.assertTrue(slow_started.wait(1.5))
            self.assertFalse(outside_a_started.wait(0.15))
            release_slow.set()
            invocation.wait(timeout=3.0)
            snapshot = invocation.snapshot()
            assert_runtime_error(self, snapshot, "LOOP_BACK_EXIT_CONFLICT")
            self.assertNotIn("outside_a", trace)
        finally:
            release_slow.set()
            app.close()

    def test_cancellation_discards_uncommitted_pending_exit(self) -> None:
        trace: list[str] = []
        slow_started = threading.Event()
        release_slow = threading.Event()
        outside_a_started = threading.Event()
        definition = self._early_exit_workflow(
            trace,
            slow_started,
            release_slow,
            outside_a_started,
            join_takes_back=False,
        )
        app = AutoAgentApp(max_executor_concurrency=6)
        try:
            app.register_workflow(definition)
            invocation = app.submit_invoke(definition.id, {"route": 1})
            self.assertTrue(slow_started.wait(1.5))
            self.assertFalse(outside_a_started.wait(0.15))
            app.cancel(invocation)
            release_slow.set()
            snapshot = invocation.snapshot()
            self.assertEqual(snapshot.state, InvocationState.CANCELLED)
            self.assertNotIn("outside_a", trace)
        finally:
            release_slow.set()
            app.close()

    def test_pending_exit_survives_wait_until_resume_settles_branch(self) -> None:
        trace: list[str] = []
        outside_a_started = threading.Event()
        wait_node = Node(
            id="wait",
            operator=WaitOperator(
                request_type=ApprovalRequest,
                response_type=ApprovalResponse,
            ),
            input_mapping=make_approval_request,
            output_binding=record_approval,
        )
        definition = workflow(
            "pending_exit_wait",
            [],
            [
                Edge("entry", "header", id="entry_header"),
                Edge("header", "a", id="header_a"),
                Edge("header", "wait", id="header_wait"),
                Edge("a", "outside_a", condition=conditional_true, id="a_exit"),
                Edge("a", "join", condition=conditional_false, id="a_join"),
                Edge("wait", "join", id="wait_join"),
                Edge("join", "header", condition=conditional_false, id="back"),
                Edge("join", "outside_join", condition=conditional_true, id="join_exit"),
            ],
            nodes=[
                node("entry", traced_operator("entry", trace)),
                node("header", traced_operator("header", trace)),
                node("a", traced_operator("a", trace)),
                wait_node,
                node("join", traced_operator("join", trace)),
                node(
                    "outside_a",
                    traced_operator(
                        "outside_a",
                        trace,
                        started=outside_a_started,
                    ),
                ),
                node("outside_join", traced_operator("outside_join", trace)),
            ],
        )
        app = AutoAgentApp(max_executor_concurrency=6)
        try:
            app.register_workflow(definition)
            invocation = app.submit_invoke(definition.id, {"route": 1})
            waiting = wait_for_state(invocation, InvocationState.WAITING)
            self.assertEqual(len(waiting.waits), 1)
            self.assertFalse(
                outside_a_started.wait(0.15),
                "pending Exit committed while sibling branch was waiting",
            )
            resumed = app.submit_resume(
                invocation,
                waiting.waits[0].id,
                ApprovalResponse(approved=True),
            )
            resumed.wait(timeout=3.0)
            assert_completed(self, resumed.snapshot())
            self.assertEqual(trace.count("outside_a"), 1)
            self.assertEqual(trace.count("outside_join"), 1)
        finally:
            app.close()


if __name__ == "__main__":
    unittest.main()
