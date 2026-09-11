from __future__ import annotations

import asyncio
import json
import threading
import time
import unittest
from concurrent.futures import CancelledError, Future
from unittest.mock import patch

import autoagent as sdk_api
import autoagent.core as core_api
from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp,
    Capability,
    ContextOperation,
    ContextPatch,
    Edge,
    Map,
    Node,
    Operator,
    Recovery,
    RuntimeErrorInfo,
    UserEvent,
    UserEventMapping,
    Wait,
    Workflow,
    WorkflowCompiler,
)
from autoagent.core import (
    NodeExecutor,
    OperatorRegistry,
    RuntimeEvent,
    SessionOpened,
    TaskRuntime,
)
from autoagent.core.app.runtime_loop import RuntimeLoop
from autoagent.core.context import apply_context_operation
from autoagent.core.errors import RuntimeTransitionError
from autoagent.core.executor.future import await_concurrent_future
from autoagent.core.hosting import RuntimeEventSink
from autoagent.core.runtime.values import freeze, thaw
from autoagent.core.workflow import SubWorkflow


class Value(TypedDict):
    value: int


class OtherValue(TypedDict):
    text: str


def identity(value: Value) -> Value:
    return value


def other(value: OtherValue) -> OtherValue:
    return value


def fail(_value: Value) -> Value:
    raise RuntimeError("expected failure")


def map_user_event(context) -> Value:
    return context.output


class DefinitionBoundaryTests(unittest.TestCase):
    def test_public_exports_preserve_sdk_core_and_host_boundaries(self) -> None:
        """Verify public exports do not leak canonical State or legacy Server APIs."""
        for module in (sdk_api, core_api):
            with self.subTest(module=module.__name__):
                self.assertEqual(len(module.__all__), len(set(module.__all__)))
                self.assertFalse(
                    [name for name in module.__all__ if not hasattr(module, name)]
                )

        self.assertTrue(
            {"AutoAgentApp", "Workflow", "SessionCheckpoint", "UserEvent"}
            <= set(sdk_api.__all__)
        )
        self.assertFalse(
            {
                "RuntimeEvent",
                "RuntimeState",
                "StateOperation",
                "StateReducer",
                "RuntimeRepository",
                "RuntimeEventSink",
                "AutoAgentServer",
                "RuntimeStore",
                "AutoAgentSettings",
                "Backoff",
                "Retry",
                "OperatorPolicy",
            }
            & set(sdk_api.__all__)
        )
        self.assertTrue(
            {"RuntimeEvent", "RuntimeState", "StateOperation", "StateReducer"}
            <= set(core_api.__all__)
        )
        self.assertEqual(
            RuntimeEventSink.__module__, "autoagent.core.hosting.runtime_events"
        )

    def test_malformed_definition_members_produce_stable_diagnostics(self) -> None:
        """Verify malformed authoring objects fail through Compiler diagnostics."""
        compiler = WorkflowCompiler()
        cases = (
            (Workflow(1, nodes=[Node("node", identity)]), "WORKFLOW_ID_INVALID"),
            (Workflow("node-id", nodes=[Node(1, identity)]), "NODE_ID_INVALID"),
            (Workflow("node-object", nodes=[object()]), "NODE_DEFINITION_INVALID"),
            (
                Workflow(
                    "edge-endpoint",
                    nodes=[Node("node", identity)],
                    edges=[Edge(1, "node")],
                ),
                "EDGE_ENDPOINT_INVALID",
            ),
            (
                Workflow(
                    "node-config",
                    nodes=[Node("node", identity, recovery_mode="invalid")],
                ),
                "NODE_CONFIGURATION_INVALID",
            ),
            (
                Workflow(
                    "inline-empty-node",
                    sub_workflows=[
                        SubWorkflow(
                            "inline",
                            Workflow("child", nodes=[Node("", identity)]),
                        )
                    ],
                ),
                "NODE_ID_EMPTY",
            ),
            (
                Workflow(
                    "empty-edge-id",
                    nodes=[Node("a", identity), Node("b", identity)],
                    edges=[Edge("a", "b", id="")],
                ),
                "EDGE_ID_EMPTY",
            ),
        )
        for workflow, code in cases:
            with self.subTest(code=code):
                result = compiler.compile(workflow)  # type: ignore[arg-type]
                self.assertFalse(result.ok)
                self.assertIn(code, {item.code for item in result.diagnostics})

    def test_diagnostics_preserve_their_declared_identifier_types(self) -> None:
        """Verify malformed definitions still produce type-safe diagnostic records."""

        compiler = WorkflowCompiler()
        invalid_id = compiler.compile(
            Workflow(1, nodes=[Node("node", identity)])  # type: ignore[arg-type]
        )
        self.assertIsNone(invalid_id.workflow_id)
        diagnostic = next(
            item
            for item in invalid_id.diagnostics
            if item.code == "WORKFLOW_ID_INVALID"
        )
        self.assertIsNone(diagnostic.workflow_id)
        self.assertEqual(diagnostic.object_type, "workflow")
        self.assertEqual(diagnostic.field, "id")

        invalid_inline = compiler.compile(
            Workflow(
                "parent",
                sub_workflows=[
                    SubWorkflow(
                        "part:invalid",
                        Workflow("child", nodes=[Node("node", identity)]),
                    )
                ],
            )
        )
        inline_diagnostic = next(
            item
            for item in invalid_inline.diagnostics
            if item.code == "SUBWORKFLOW_ID_RESERVED"
        )
        self.assertEqual(inline_diagnostic.object_type, "sub_workflow")

    def test_numeric_runtime_limits_reject_bool_values(self) -> None:
        """Verify confirmed numeric limits reject bool aliases."""
        factories = (
            lambda: Map(max_parallelism=True),
            lambda: Recovery(max_attempts=True),
            lambda: NodeExecutor(max_operator_concurrency=True),
            lambda: AutoAgentApp(max_operator_concurrency=True),
            lambda: AutoAgentApp(max_node_executions_per_invocation=True),
        )
        for factory in factories:
            with self.subTest(factory=factory):
                with self.assertRaises((TypeError, ValueError)):
                    factory()

    def test_injected_executor_cannot_exceed_app_operator_limit(self) -> None:
        """Verify dependency injection cannot bypass the App-wide Operator cap."""

        executor = NodeExecutor(max_operator_concurrency=2)
        try:
            with self.assertRaisesRegex(ValueError, "cannot exceed"):
                AutoAgentApp(
                    max_operator_concurrency=1,
                    node_executor=executor,
                )
        finally:
            executor.close()

    def test_public_definition_identifiers_are_strict_strings(self) -> None:
        """Verify Operator, Wait, Capability, and UserEventMapping reject invalid IDs."""
        operator = Operator(identity)
        cases = (
            lambda: Operator(identity, id=1),
            lambda: Wait(Value, Value, id=1),
            lambda: Capability(1, operator.contract),
            lambda: UserEventMapping(1, map_user_event),
        )
        for factory in cases:
            with self.subTest(factory=factory):
                with self.assertRaises((TypeError, ValueError)):
                    factory()

    def test_operator_registry_enforces_identity_defaults_and_contracts(self) -> None:
        """Verify the Registry is the strict single source of Capability implementations."""
        registry = OperatorRegistry()
        primary = Operator(identity, id="primary")
        capability = Capability("value", primary.contract)
        registry.bind_capability(capability)
        registry.register(primary, capability_id="value", default=True)
        self.assertIs(registry.get("primary").operator, primary)
        self.assertIs(registry.default_for_capability("value"), primary)

        with self.assertRaisesRegex(ValueError, "already registered"):
            registry.register(primary, capability_id="value")
        with self.assertRaisesRegex(ValueError, "already has a default"):
            registry.register(
                Operator(identity, id="second"),
                capability_id="value",
                default=True,
            )
        with self.assertRaisesRegex(ValueError, "does not match"):
            registry.register(Operator(other), capability_id="value")
        with self.assertRaises(KeyError):
            registry.set_enabled("missing", False)

    def test_capability_closure_binding_is_atomic(self) -> None:
        """Verify a later contract conflict cannot retain earlier bindings."""

        registry = OperatorRegistry()
        registry.register(
            Operator(other, id="wrong-second"),
            capability_id="second",
        )
        first = Capability("first", Operator(identity, id="first-base").contract)
        second = Capability("second", Operator(identity, id="second-base").contract)
        with self.assertRaisesRegex(ValueError, "does not match Capability"):
            registry.bind_capabilities((first, second))

        # A mismatched implementation remains legal for the first id only if
        # the rejected multi-bind left no partial nominal contract behind.
        registered = registry.register(
            Operator(other, id="other-first"),
            capability_id="first",
        )
        self.assertEqual(registered.id, "other-first")

    def test_context_operations_validate_and_apply_path_copy(self) -> None:
        """Verify Context operations are strict and copy only the modified path."""
        original = freeze({"left": {"value": 1}, "right": {"value": 2}})
        changed = apply_context_operation(
            original, ContextOperation.set("left.value", 3)
        )
        self.assertEqual(thaw(changed), {"left": {"value": 3}, "right": {"value": 2}})
        self.assertIs(changed["right"], original["right"])  # type: ignore[index]
        deleted = apply_context_operation(changed, ContextOperation.delete("left.value"))
        self.assertEqual(thaw(deleted), {"left": {}, "right": {"value": 2}})

        invalid = (
            lambda: ContextOperation("merge", ("value",)),
            lambda: ContextOperation("set", ["value"]),
            lambda: ContextOperation.delete("left..value"),
            lambda: ContextPatch(invocation=[ContextOperation.set("value", 1)]),
        )
        for factory in invalid:
            with self.subTest(factory=factory):
                with self.assertRaises((TypeError, ValueError)):
                    factory()

    def test_runtime_and_user_event_constructors_enforce_strict_identity(self) -> None:
        """Verify Event constructors reject malformed identities, counters, and errors."""
        with self.assertRaises(ValueError):
            RuntimeErrorInfo(1, "message")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            RuntimeErrorInfo("Error", " ")
        with self.assertRaises(ValueError):
            RuntimeEvent(
                session_id=1,  # type: ignore[arg-type]
                sequence=1,
                payload=SessionOpened({}),
            )
        with self.assertRaises(ValueError):
            RuntimeEvent(
                session_id="session",
                sequence=True,
                payload=SessionOpened({}),
            )
        for values in (
            {"session_id": "", "invocation_id": "inv", "sequence": 1},
            {"session_id": "session", "invocation_id": "", "sequence": 1},
            {"session_id": "session", "invocation_id": "inv", "sequence": True},
            {"session_id": "session", "invocation_id": "inv", "sequence": 1, "kind": " "},
            {"session_id": "session", "invocation_id": "inv", "sequence": 1, "occurred_at_us": -1},
        ):
            arguments = {"kind": "kind", "payload": {}, **values}
            with self.subTest(arguments=arguments):
                with self.assertRaises(ValueError):
                    UserEvent(**arguments)  # type: ignore[arg-type]
        valid = UserEvent(
            session_id="session",
            invocation_id="invocation",
            sequence=1,
            kind="kind",
            payload=None,
        ).to_record()
        del valid["payload"]
        with self.assertRaisesRegex(KeyError, "requires payload"):
            UserEvent.from_record(valid)

    def test_emitted_runtime_event_variants_round_trip_through_json(self) -> None:
        """Verify persisted Wait, failure, and Child Event payloads decode losslessly."""
        class Collector:
            def __init__(self) -> None:
                self.events: list[RuntimeEvent] = []

            async def append(self, event: RuntimeEvent) -> None:
                self.events.append(event)

        collector = Collector()
        app = AutoAgentApp(runtime_event_sink=collector)
        try:
            failed = app.invoke(
                Workflow("codec-failure", nodes=[Node("fail", fail)]),
                {"value": 1},
            )
            waiting = app.submit_invoke(
                Workflow(
                    "codec-wait", nodes=[Node("wait", Wait(Value, Value))]
                ),
                {"value": 2},
            )
            waiting = app.join(waiting.ref, timeout=1)
            resumed = app.resume(
                waiting.ref, waiting.waits[0].id, {"value": 3}
            )
            spawned = app.invoke(
                Workflow(
                    "codec-parent",
                    nodes=[
                        Node(
                            "spawn",
                            Workflow(
                                "codec-child",
                                nodes=[Node("child", identity)],
                            ),
                            execution_mode="spawn",
                        )
                    ],
                ),
                {"value": 4},
            )
            handle = app.child_invocations(spawned.ref)[0]
            app.join(handle, timeout=1)

            events = tuple(collector.events)
            names = {event.event_name for event in events}
            self.assertTrue(
                {
                    "node_occurrence.failed",
                    "invocation.failed",
                    "node_occurrence.waiting",
                    "wait.resumed",
                    "child_invocation.planned",
                    "child_invocation.phase_changed",
                }.issubset(names)
            )
            for event in events:
                with self.subTest(event_name=event.event_name):
                    record = json.loads(json.dumps(event.to_record()))
                    self.assertEqual(RuntimeEvent.from_record(record), event)
        finally:
            app.close()


class RuntimeBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_future_bridge_uses_cross_thread_notification(self) -> None:
        """Verify worker completion wakes asyncio without a timed polling sleep."""
        future: Future[int] = Future()

        def complete() -> None:
            time.sleep(0.02)
            future.set_result(7)

        worker = threading.Thread(target=complete)
        worker.start()
        try:
            with patch(
                "autoagent.core.executor.future.asyncio.sleep",
                side_effect=AssertionError("Future bridge must not poll."),
            ):
                result = await asyncio.wait_for(
                    await_concurrent_future(future), timeout=1
                )
            self.assertEqual(result, 7)
        finally:
            worker.join()

    async def test_task_runtime_tracks_wakes_cleans_and_cancels_tasks(self) -> None:
        """Verify TaskRuntime owns only live tasks and releases transient signals."""
        runtime = TaskRuntime()
        release = asyncio.Event()

        async def work() -> None:
            await release.wait()

        task = asyncio.create_task(work())
        runtime.track("session", task)
        duplicate = asyncio.create_task(work())
        with self.assertRaisesRegex(RuntimeError, "already has a live task"):
            runtime.track("session", duplicate)
        duplicate.cancel()
        await asyncio.gather(duplicate, return_exceptions=True)

        release.set()
        await task
        await asyncio.sleep(0)
        self.assertIsNone(runtime.task("session"))
        self.assertEqual(runtime.active_sessions(), ())

        blocked = asyncio.create_task(asyncio.Event().wait())
        runtime.track("blocked", blocked)
        await runtime.cancel_all()
        self.assertTrue(blocked.cancelled())

    async def test_async_app_facade_covers_submit_resume_and_child_observation(self) -> None:
        """Verify asynchronous App wrappers preserve Wait and Child Invocation semantics."""
        app = AutoAgentApp()
        try:
            waiting = Workflow("async-wait", nodes=[Node("wait", Wait(Value, Value))])
            submitted = await app.asubmit_invoke(waiting, {"value": 1})
            boundary = await app.ajoin(submitted.ref, timeout=1)
            self.assertEqual(boundary.status, "waiting")
            resumed = await app.asubmit_resume(
                submitted.ref, boundary.waits[0].id, {"value": 2}
            )
            completed = await app.ajoin(resumed.ref, timeout=1)
            self.assertEqual(completed.status, "completed")
            self.assertEqual(completed.output, {"value": 2})

            async def child(value: Value) -> Value:
                await asyncio.sleep(0.02)
                return value

            child_workflow = Workflow("async-child", nodes=[Node("child", child)])
            parent = Workflow(
                "async-parent",
                nodes=[Node("spawn", child_workflow, execution_mode="spawn")],
            )
            parent_result = await app.ainvoke(parent, {"value": 3})
            handles = await app.achild_invocations(parent_result.ref)
            self.assertEqual(len(handles), 1)
            status = await app.astatus(handles[0])
            self.assertIn(status.status, {"running", "completed"})
            child_result = await app.ajoin(handles[0], timeout=1)
            self.assertEqual(child_result.output, {"value": 3})

            async def blocked_child(value: Value) -> Value:
                await asyncio.sleep(10)
                return value

            blocked_parent = Workflow(
                "async-parent-cancel",
                nodes=[
                    Node(
                        "spawn",
                        Workflow(
                            "async-child-cancel",
                            nodes=[Node("child", blocked_child)],
                        ),
                        execution_mode="spawn",
                    )
                ],
            )
            blocked_result = await app.ainvoke(blocked_parent, {"value": 4})
            blocked_handle = (
                await app.achild_invocations(blocked_result.ref)
            )[0]
            cancelled = await app.acancel(blocked_handle, "test cancel")
            self.assertEqual(cancelled.status, "cancelled")
        finally:
            await app.aclose()
            await app.aclose()


class RuntimeLoopTests(unittest.TestCase):
    def test_runtime_loop_does_not_schedule_an_idle_heartbeat(self) -> None:
        """Verify RuntimeLoop relies on notifications, not periodic timers."""
        scheduled: list[float] = []
        new_event_loop = asyncio.new_event_loop

        def create_loop():
            loop = new_event_loop()
            call_later = loop.call_later

            def record_call_later(delay, callback, *args, **kwargs):
                scheduled.append(delay)
                return call_later(delay, callback, *args, **kwargs)

            loop.call_later = record_call_later  # type: ignore[method-assign]
            return loop

        with patch(
            "autoagent.core.app.runtime_loop.asyncio.new_event_loop",
            side_effect=create_loop,
        ):
            loop = RuntimeLoop()
            try:
                self.assertEqual(loop.run(asyncio.sleep(0, result=3)), 3)
            finally:
                loop.close()
        self.assertEqual(scheduled, [])

    def test_runtime_loop_run_submit_cancel_and_close_are_bounded(self) -> None:
        """Verify RuntimeLoop bridges results, cancellation, and idempotent shutdown."""
        loop = RuntimeLoop()
        try:
            self.assertEqual(loop.run(asyncio.sleep(0, result=3)), 3)
            pending = loop.submit(asyncio.sleep(10))
            self.assertTrue(pending.cancel())
            with self.assertRaises(CancelledError):
                pending.result()
        finally:
            loop.close()
            loop.close()
        coroutine = asyncio.sleep(0)
        with self.assertRaisesRegex(RuntimeError, "closed"):
            loop.submit(coroutine)


if __name__ == "__main__":
    unittest.main()
