from __future__ import annotations

import asyncio
import threading
import time
import unittest
from typing import Any

from autoagent.core import (
    AggregationContext,
    AutoAgentApp,
    ContextPatch,
    Edge,
    InputMappingContext,
    ItemSelectorContext,
    MapPolicy,
    Node,
    NodePolicy,
    OutputBindingContext,
    ReplicationPolicy,
    Workflow,
)


def identity(value: list[str]) -> list[str]:
    return value


def identity_nested(value: dict[str, list[int]]) -> dict[str, list[int]]:
    return value


class HookContextIsolationTests(unittest.TestCase):
    def test_output_binding_receives_an_isolated_output(self) -> None:
        def mutate(context: OutputBindingContext) -> ContextPatch:
            context.output.append("hook-only")
            return ContextPatch()

        workflow = Workflow(
            "binding-isolation",
            nodes=[Node("node", identity, output_binding=mutate)],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, ["runtime"])
        self.assertEqual(invocation.result(), {"node": ["runtime"]})
        app.close()

    def test_context_values_are_deepcopied_without_json_roundtrip(self) -> None:
        original = {"nested": [1]}

        def mapping(context: InputMappingContext) -> dict[str, list[int]]:
            value = context.invocation_input
            value["nested"].append(2)
            return value

        workflow = Workflow(
            "input-isolation",
            nodes=[Node("node", identity_nested, input_mapping=mapping)],
        )
        app = AutoAgentApp()
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, original)
        self.assertEqual(original, {"nested": [1]})
        self.assertEqual(invocation.result(), {"node": {"nested": [1, 2]}})
        app.close()

    def test_map_inputs_are_isolated_and_aggregation_is_index_ordered(self) -> None:
        def mutate(value: list[int]) -> int:
            unit = value[0]
            time.sleep((3 - unit) * 0.005)
            value.append(unit)
            return len(value) * 10 + unit

        def aggregate(context: AggregationContext) -> list[int]:
            self.assertEqual(context.operator_outputs, [21, 22, 23])
            context.operator_outputs.append(99)
            return context.operator_outputs

        workflow = Workflow(
            "replica-isolation",
            nodes=[
                Node(
                    "node",
                    mutate,
                    policy=NodePolicy(
                        map=MapPolicy(
                            item_selector=select_units,
                            max_parallelism=3,
                            output_aggregator=aggregate,
                        )
                    ),
                )
            ],
        )
        app = AutoAgentApp(max_executor_concurrency=3)
        app.register_workflow(workflow)
        invocation = app.invoke(workflow, 1)
        self.assertEqual(invocation.result(), {"node": [21, 22, 23, 99]})
        app.close()

    def test_replication_isolates_each_physical_call_from_one_shared_baseline(self) -> None:
        original = [1]

        def mutate(value: list[int]) -> list[int]:
            value.append(2)
            return value

        workflow = Workflow(
            "replication-call-isolation",
            nodes=[
                Node(
                    "node",
                    mutate,
                    policy=NodePolicy(
                        replication=ReplicationPolicy(count=3, max_parallelism=3)
                    ),
                )
            ],
        )
        app = AutoAgentApp(max_executor_concurrency=3)
        app.register_workflow(workflow)

        invocation = app.invoke(workflow, original)

        self.assertEqual(
            invocation.result(),
            {"node": [[1, 2], [1, 2], [1, 2]]},
        )
        self.assertEqual(original, [1])
        app.close()

    def test_input_mapping_receives_only_exact_ordered_incoming_activations(self) -> None:
        def left(_value: int) -> str:
            return "left"

        def right(_value: int) -> str:
            return "right"

        def mapping(context: InputMappingContext) -> list[str]:
            self.assertEqual(
                [activation.edge_id for activation in context.incoming],
                ["left_join", "right_join"],
            )
            self.assertEqual(
                [activation.value for activation in context.incoming],
                ["left", "right"],
            )
            return [activation.value for activation in context.incoming]

        def collect(values: list[str]) -> list[str]:
            return values

        workflow = Workflow(
            "exact-incoming",
            nodes=[
                Node("entry", identity_int),
                Node("left", left),
                Node("right", right),
                Node("join", collect, input_mapping=mapping),
            ],
            edges=[
                Edge("entry", "left", id="entry_left"),
                Edge("entry", "right", id="entry_right"),
                Edge("left", "join", id="left_join"),
                Edge("right", "join", id="right_join"),
            ],
        )
        app = AutoAgentApp(max_executor_concurrency=2)
        app.register_workflow(workflow)
        self.assertEqual(
            app.invoke(workflow, 1).result(), {"join": ["left", "right"]}
        )
        app.close()


def select_units(_context: ItemSelectorContext) -> list[list[int]]:
    return [[1], [2], [3]]


def identity_int(value: int) -> int:
    return value


class GlobalConcurrencyTests(unittest.TestCase):
    def test_global_executor_concurrency_must_be_positive(self) -> None:
        with self.assertRaisesRegex(ValueError, "max_executor_concurrency"):
            AutoAgentApp(max_executor_concurrency=0)

    def test_sync_and_async_operators_share_one_app_global_limit(self) -> None:
        lock = threading.Lock()
        active = 0
        maximum = 0

        def enter() -> None:
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)

        def leave() -> None:
            nonlocal active
            with lock:
                active -= 1

        def sync_operator(value: int) -> int:
            enter()
            try:
                time.sleep(0.03)
                return value
            finally:
                leave()

        async def async_operator(value: int) -> int:
            enter()
            try:
                await asyncio.sleep(0.03)
                return value
            finally:
                leave()

        sync_workflow = Workflow("sync", nodes=[Node("node", sync_operator)])
        async_workflow = Workflow("async", nodes=[Node("node", async_operator)])
        app = AutoAgentApp(max_executor_concurrency=2)
        app.register_workflow(sync_workflow)
        app.register_workflow(async_workflow)
        invocations = [
            app.submit_invoke(sync_workflow, 1, session_id="s1"),
            app.submit_invoke(async_workflow, 2, session_id="s2"),
            app.submit_invoke(sync_workflow, 3, session_id="s3"),
            app.submit_invoke(async_workflow, 4, session_id="s4"),
        ]
        for invocation in invocations:
            invocation.wait(2)
            self.assertEqual(invocation.result()["node"], invocation.snapshot().output["node"])
        self.assertEqual(maximum, 2)
        app.close()

    def test_sync_hook_runs_in_shared_executor_pool(self) -> None:
        hook_threads: list[int] = []
        operator_threads: list[int] = []

        def mapping(context: InputMappingContext) -> int:
            hook_threads.append(threading.get_ident())
            return int(context.invocation_input)

        def operator(value: int) -> int:
            operator_threads.append(threading.get_ident())
            return value

        workflow = Workflow(
            "hook-inline",
            nodes=[Node("node", operator, input_mapping=mapping)],
        )
        app = AutoAgentApp(max_executor_concurrency=1)
        app.register_workflow(workflow)
        self.assertEqual(app.invoke(workflow, 1).result(), {"node": 1})
        self.assertEqual(hook_threads, operator_threads)
        app.close()

    def test_blocking_sync_hook_does_not_block_runtime_loop(self) -> None:
        hook_started = threading.Event()
        release_hook = threading.Event()

        def blocking_mapping(context: InputMappingContext) -> int:
            hook_started.set()
            if not release_hook.wait(2):
                raise TimeoutError("test did not release blocking Hook")
            return int(context.invocation_input)

        slow = Workflow(
            "blocking-hook",
            nodes=[Node("node", identity_int, input_mapping=blocking_mapping)],
        )
        fast = Workflow("fast-while-hook-blocks", nodes=[Node("node", identity_int)])
        app = AutoAgentApp(max_executor_concurrency=2)
        app.register_workflow(slow)
        app.register_workflow(fast)
        slow_invocation = app.submit_invoke(slow, 1, session_id="slow")
        self.assertTrue(hook_started.wait(1))

        # The second execution can enter the Runtime Loop and use the remaining
        # executor slot while the synchronous Hook is blocked in a worker thread.
        fast_invocation = app.invoke(fast, 2, session_id="fast")
        self.assertEqual(fast_invocation.result(), {"node": 2})
        self.assertFalse(slow_invocation.done())

        release_hook.set()
        slow_invocation.wait(2)
        self.assertEqual(slow_invocation.result(), {"node": 1})
        app.close()

    def test_hook_and_operator_share_one_executor_slot(self) -> None:
        hook_started = threading.Event()
        release_hook = threading.Event()
        operator_started = threading.Event()

        def blocking_mapping(context: InputMappingContext) -> int:
            hook_started.set()
            if not release_hook.wait(2):
                raise TimeoutError("test did not release blocking Hook")
            return int(context.invocation_input)

        def observed_operator(value: int) -> int:
            operator_started.set()
            return value

        hook_workflow = Workflow(
            "hook-holds-budget",
            nodes=[Node("node", identity_int, input_mapping=blocking_mapping)],
        )
        operator_workflow = Workflow(
            "operator-waits-for-budget",
            nodes=[Node("node", observed_operator)],
        )
        app = AutoAgentApp(max_executor_concurrency=1)
        app.register_workflow(hook_workflow)
        app.register_workflow(operator_workflow)
        hook_invocation = app.submit_invoke(
            hook_workflow, 1, session_id="hook-session"
        )
        self.assertTrue(hook_started.wait(1))
        operator_invocation = app.submit_invoke(
            operator_workflow, 2, session_id="operator-session"
        )

        self.assertFalse(operator_started.wait(0.05))
        release_hook.set()
        hook_invocation.wait(2)
        operator_invocation.wait(2)
        self.assertTrue(operator_started.is_set())
        self.assertEqual(operator_invocation.result(), {"node": 2})
        app.close()
