"""Opt-in replay preserves fresh dispatch, committed results and durable Waits."""

from collections import Counter
from contextlib import closing
import unittest

from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp, BackoffPolicy, Edge, Node, Recovery, RecoverySource,
    RetryPolicy, RuntimeTransitionError, SubWorkflow, Wait, Workflow, WorkflowCompiler,
)
from autoagent.core.operators import OperatorCallContext
from autoagent.core.runtime import SessionCheckpoint, StateReducer
from tests.graph_fixtures import load_graph
from tests.test_phase2_runtime_state import RecordingSink


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


class RecoveryDefaultTests(unittest.TestCase):
    def test_default_never_is_part_of_node_and_revision_contracts(self):
        """Authored, nested and compiled Nodes require an explicit replay declaration."""
        node = Node("work", identity)
        workflow = Workflow("defaults", nodes=[node])
        child = SubWorkflow("nested", workflow)
        self.assertEqual(Recovery().mode, "never")
        self.assertEqual(node.recovery_mode.mode, "never")
        self.assertEqual(child.workflow.nodes[0].recovery_mode.mode, "never")
        compiler = WorkflowCompiler()
        default = compiler.compile_or_raise(workflow)
        self.assertEqual(default.node("work").recovery_mode.mode, "never")
        node.recovery_mode = Recovery("never")
        self.assertEqual(default.workflow_revision_id, compiler.compile_or_raise(workflow).workflow_revision_id)
        node.recovery_mode = Recovery("replay_safe")
        self.assertNotEqual(default.workflow_revision_id, compiler.compile_or_raise(workflow).workflow_revision_id)

    def test_unstarted_downstream_dispatches_under_both_policies_without_repeating_prefix(self):
        """A first dispatch is allowed by never; committed upstream work is reused."""
        for mode in ("never", "replay_safe"):
            with self.subTest(mode=mode):
                calls = Counter()

                def prefix(value: Value) -> Value:
                    calls["prefix"] += 1
                    return value

                def next_step(value: Value) -> Value:
                    calls["next"] += 1
                    return value

                workflow = Workflow("fresh", nodes=[Node("prefix", prefix),
                    Node("next", next_step, recovery_mode=Recovery(mode))], edges=[Edge("prefix", "next")])
                sink = RecordingSink()
                with closing(AutoAgentApp(runtime_event_sink=sink)) as source:
                    self.assertEqual(source.invoke(workflow, {"value": 7}).status, "completed")
                cut = next(index for index, event in enumerate(sink.events)
                           if event.event_name == "node_occurrence.started" and event.payload.occurrence_id == "next@root")
                checkpoint = SessionCheckpoint.from_state(StateReducer().reduce(sink.events[:cut]))
                with closing(AutoAgentApp()) as target:
                    target.register_workflow(workflow)
                    result = target.recover(load_graph(target, checkpoint).invocations[0])
                self.assertEqual(result.status, "completed", result.error)
                self.assertEqual(calls, {"prefix": 1, "next": 2})

    def test_started_node_without_admitted_call_can_dispatch_with_default_never(self):
        """A Node-start checkpoint alone does not imply an uncertain Operator effect."""
        calls = []

        def work(value: Value) -> Value:
            calls.append(1)
            return value

        workflow = Workflow("before-call", nodes=[Node("work", work)])
        sink = RecordingSink()
        with closing(AutoAgentApp(runtime_event_sink=sink)) as source:
            self.assertEqual(source.invoke(workflow, {"value": 3}).status, "completed")
        cut = next(index for index, event in enumerate(sink.events) if event.event_name == "node_occurrence.started")
        checkpoint = SessionCheckpoint.from_state(StateReducer().reduce(sink.events[:cut + 1]))
        with closing(AutoAgentApp()) as target:
            target.register_workflow(workflow)
            result = target.recover(load_graph(target, checkpoint).invocations[0])
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(len(calls), 2)

    def test_committed_call_output_is_reused_under_default_never(self):
        """Recovery finishes an interrupted Node without invoking its committed Operator again."""
        calls = []

        def work(value: Value) -> Value:
            calls.append(1)
            return value

        workflow = Workflow("after-call", nodes=[Node("work", work)])
        sink = RecordingSink()
        with closing(AutoAgentApp(runtime_event_sink=sink)) as source:
            self.assertEqual(source.invoke(workflow, {"value": 4}).status, "completed")
        cut = next(index for index, event in enumerate(sink.events) if event.event_name == "operator_call.completed")
        checkpoint = SessionCheckpoint.from_state(StateReducer().reduce(sink.events[:cut + 1]))
        with closing(AutoAgentApp()) as target:
            target.register_workflow(workflow)
            result = target.recover(load_graph(target, checkpoint).invocations[0])
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(len(calls), 1)

    def test_existing_external_wait_resumes_under_default_never(self):
        """Reloading a pending Wait keeps its identity and still accepts an ordinary response."""
        workflow = Workflow("pending-wait", nodes=[Node("approval", Wait(Value, Value))])
        source = AutoAgentApp()
        try:
            waiting = source.invoke(workflow, {"value": 1})
            checkpoint = source.unload_session(waiting.ref, capture_checkpoint=True)
        finally:
            source.close()
        with closing(AutoAgentApp()) as target:
            target.register_workflow(workflow)
            restored = target.recover(load_graph(target, checkpoint).invocations[0])
            self.assertEqual(restored.status, "waiting", restored.error)
            self.assertEqual(restored.waits[0].id, waiting.waits[0].id)
            self.assertEqual(target.resume(restored.ref, restored.waits[0].id, {"value": 2}).output, {"value": 2})

    def test_retry_is_independent_but_terminal_reopen_requires_replay_permission(self):
        """Retry runs under never; a fresh terminal recovery round must explicitly permit replay."""
        for mode in ("never", "replay_safe"):
            with self.subTest(mode=mode):
                attempts = []

                def work(value: Value) -> Value:
                    attempts.append(OperatorCallContext.current().retry_attempt)
                    if len(attempts) < 4:
                        raise OSError("temporary")
                    return value

                workflow = Workflow("retry-and-recover", nodes=[Node("work", work,
                    recovery_mode=Recovery(mode), retry=RetryPolicy(1,
                        BackoffPolicy(initial_seconds=0, max_seconds=0, jitter="none")))])
                sink = RecordingSink()
                with closing(AutoAgentApp(runtime_event_sink=sink)) as app:
                    failed = app.invoke(workflow, {"value": 1})
                    self.assertEqual(failed.status, "failed")
                    self.assertEqual(attempts, [0, 1])
                    source = RecoverySource.from_events(sink.events)
                    if mode == "never":
                        with self.assertRaises(RuntimeTransitionError):
                            app.recover(failed.ref, source=source)
                        self.assertEqual(attempts, [0, 1])
                    else:
                        result = app.recover(failed.ref, source=source)
                        self.assertEqual(result.status, "completed", result.error)
                        self.assertEqual(attempts, [0, 1, 0, 1])
