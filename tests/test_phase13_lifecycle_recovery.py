from __future__ import annotations

from autoagent import ChildHandle

from tests.graph_fixtures import (
    async_children,
    async_resume_graph_wait,
    child_refs,
    graph_bundle,
    join_observed,
    load_graph,
    resume_graph_wait,
    root_snapshot,
    session_checkpoints,
    status_observed,
)

import asyncio
import threading
import unittest
from collections.abc import Coroutine
from dataclasses import replace
from typing import TypeVar, cast, get_args

from typing_extensions import TypedDict

from autoagent import (
    AppCheckpoint,
    AutoAgentApp,
    Edge,
    InvocationRef,
    InvocationResult,
    InvocationStatus,
    InvocationUpdate,
    Node,
    Recovery,
    RuntimeInfrastructureError,
    RuntimeTransitionError,
    Wait,
    Workflow,
)
from autoagent.core import (
    RuntimeRepository,
    InvocationCancelled,
    RecoveryApplied,
    SessionCheckpoint,
    RuntimeEvent,
)
from autoagent.core.app.stream import AttachedStream
from autoagent.core.app.runtime_loop import RuntimeLoop


class Value(TypedDict):
    value: int


def identity(value: Value) -> Value:
    return value


def _cross_root_checkpoint_pair() -> tuple[
    Workflow,
    Workflow,
    SessionCheckpoint,
    SessionCheckpoint,
    str,
]:
    child = Workflow("cross-root-child", nodes=[Node("work", identity)])
    parent = Workflow("cross-root-parent", nodes=[Node("child", child)])
    source = AutoAgentApp()
    try:
        parent_result = source.invoke(
            parent, {"value": 1}, session_id="cross-root-parent"
        )
        child_ref = child_refs(source, parent_result.ref)[0]
        graph = source.unload_session(parent_result.ref, capture_checkpoint=True)
        child_bundle = next(s for s in graph.sessions if s.session_id == child_ref.session_id)
        planned = root_snapshot(graph)
    finally:
        source.close()
    parent_state = root_snapshot(planned).state
    assert parent_state.invocation is not None
    unit = next(iter(parent_state.invocation.child_plans.values())).units[0]
    return child, parent, planned, child_bundle, unit.session_id


T = TypeVar("T")


class _ThreadSignal:
    """Notify synchronous or asyncio test waiters without polling a thread pool."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]] = []

    def set(self) -> None:
        self._event.set()
        with self._lock:
            waiters = tuple(self._waiters)
            self._waiters.clear()
        for loop, waiter in waiters:
            loop.call_soon_threadsafe(self._complete, waiter)

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout)

    async def wait_async(self, timeout: float = 1.0) -> bool:
        if self._event.is_set():
            return True
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[None] = loop.create_future()
        with self._lock:
            if self._event.is_set():
                return True
            registration = (loop, waiter)
            self._waiters.append(registration)
        done, _pending = await asyncio.wait({waiter}, timeout=timeout)
        if waiter not in done:
            with self._lock:
                if registration in self._waiters:
                    self._waiters.remove(registration)
            waiter.cancel()
            return False
        return True

    @staticmethod
    def _complete(waiter: asyncio.Future[None]) -> None:
        if not waiter.done():
            waiter.set_result(None)


class _RuntimeGate:
    """Let another thread release an asyncio waiter on its owning loop."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._gate: asyncio.Event | None = None
        self._released = False

    async def wait(self) -> None:
        gate = asyncio.Event()
        with self._lock:
            self._gate = gate
            released = self._released
        if released:
            gate.set()
        await gate.wait()

    async def release(self) -> None:
        with self._lock:
            self._released = True
            gate = self._gate
        if gate is not None:
            gate.set()


async def _await_runtime(
    runtime: RuntimeLoop, coroutine: Coroutine[object, object, T]
) -> T:
    """Submit one test-control action to the RuntimeLoop without blocking."""

    return await runtime.wait(runtime.submit(coroutine))


async def _release_runtime_gate(runtime: RuntimeLoop, gate: _RuntimeGate) -> None:
    """Release a gate on its RuntimeLoop, tolerating already-finished cleanup."""

    try:
        await _await_runtime(runtime, gate.release())
    except RuntimeError:
        pass


def _release_runtime_gate_sync(runtime: RuntimeLoop, gate: _RuntimeGate) -> None:
    """Synchronously release a gate on its RuntimeLoop."""

    try:
        runtime.run(gate.release())
    except RuntimeError:
        pass


class _BlockingSink:
    """Block one selected Runtime Event without blocking the caller's loop."""

    def __init__(self) -> None:
        self.entered = _ThreadSignal()
        self.release = _RuntimeGate()
        self.target_session_id: str | None = None
        self.target_kind: str | None = None
        self.enabled = False
        self._blocked = False

    async def append(self, event: RuntimeEvent) -> None:
        if (
            self.enabled
            and not self._blocked
            and (
                self.target_session_id is None
                or event.session_id == self.target_session_id
            )
            and (
                self.target_kind is None
                or any(log.event_name == self.target_kind for log in (event,))
            )
        ):
            self._blocked = True
            self.entered.set()
            await self.release.wait()


class _CoordinatedCloseApp(AutoAgentApp):
    """Hold the shared close operation until two callers have attached."""

    def __init__(self) -> None:
        super().__init__()
        self.close_entries = 0
        self.close_entries_lock = threading.Lock()
        self.begin_close_calls = 0
        self.both_close_callers_entered = _ThreadSignal()
        self.close_operation_entered = _ThreadSignal()
        self.close_gate_released = _ThreadSignal()
        self.close_operation_completed = _ThreadSignal()
        self.release_close_operations = _RuntimeGate()

    def _begin_close(self, *, capture_checkpoint=False):  # type: ignore[no-untyped-def]
        future = super()._begin_close(capture_checkpoint=capture_checkpoint)
        with self.close_entries_lock:
            self.begin_close_calls += 1
            if self.begin_close_calls == 2:
                self.both_close_callers_entered.set()
        return future

    async def _close_operation(self, *, capture_checkpoint=False) -> AppCheckpoint | None:
        with self.close_entries_lock:
            self.close_entries += 1
        self.close_operation_entered.set()
        await self.release_close_operations.wait()
        self.close_gate_released.set()
        result = await super()._close_operation(capture_checkpoint=capture_checkpoint)
        self.close_operation_completed.set()
        return result


class _QueuedResultApp(AutoAgentApp):
    """Expose queued cancellation and Result admission for lock-order tests."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.cancel_emit_entered = _ThreadSignal()
        self.result_entered = _ThreadSignal()

    async def _emit(self, *args: object, **kwargs: object):  # type: ignore[override]
        payload = args[2] if len(args) > 2 else kwargs.get("payload")
        if isinstance(payload, InvocationCancelled):
            self.cancel_emit_entered.set()
        return await super()._emit(*args, **kwargs)  # type: ignore[arg-type]

    async def _result(self, ref: InvocationRef) -> InvocationResult:
        self.result_entered.set()
        return await super()._result(ref)


class _ChildRefThreadApp(AutoAgentApp):
    """Record which thread validates each public InvocationRef."""

    def __init__(self) -> None:
        super().__init__()
        self.child_ref_threads: list[int] = []

    def _control_ref(self, handle: ChildHandle) -> InvocationRef:
        self.child_ref_threads.append(threading.get_ident())
        return super()._control_ref(handle)


class _CloseAttemptApp(AutoAgentApp):
    """Signal immediately before a caller enters close admission."""

    def __init__(self) -> None:
        super().__init__()
        self.close_attempted = _ThreadSignal()

    def _begin_close(self, *, capture_checkpoint=False):  # type: ignore[no-untyped-def]
        self.close_attempted.set()
        return super()._begin_close(capture_checkpoint=capture_checkpoint)


class LifecycleRecoveryTests(unittest.TestCase):
    def test_workflow_registration_is_linearized_before_close(self) -> None:
        """Verify admitted compilation finishes before close can own lifecycle."""

        app = _CloseAttemptApp()
        compile_entered = _ThreadSignal()
        release_compile = threading.Event()
        original_compile = app._compiler.compile

        def blocked_compile(workflow: Workflow):  # type: ignore[no-untyped-def]
            compile_entered.set()
            release_compile.wait()
            return original_compile(workflow)

        app._compiler.compile = blocked_compile  # type: ignore[method-assign]
        order: list[str] = []
        errors: list[BaseException] = []

        def register() -> None:
            try:
                app.register_workflow(
                    Workflow("register-before-close", nodes=[Node("node", identity)])
                )
                order.append("register")
            except BaseException as error:
                errors.append(error)

        def close() -> None:
            try:
                app.close(timeout=1)
                order.append("close")
            except BaseException as error:
                errors.append(error)

        register_thread = threading.Thread(target=register)
        close_thread = threading.Thread(target=close)
        register_thread.start()
        self.assertTrue(compile_entered.wait(1))
        close_thread.start()
        close_attempted = app.close_attempted.wait(1)
        lifecycle_owned = app._close_lock.locked()
        release_compile.set()
        register_thread.join(1)
        close_thread.join(1)
        self.assertTrue(close_attempted)
        self.assertTrue(lifecycle_owned)
        self.assertFalse(register_thread.is_alive())
        self.assertFalse(close_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(order, ["register", "close"])

    def test_workflow_snapshot_read_is_linearized_before_close(self) -> None:
        """Verify an admitted Snapshot read cannot finish after App close."""

        class BlockingSnapshots(dict):
            def __init__(self, values: dict[str, object]) -> None:
                super().__init__(values)
                self.entered = _ThreadSignal()
                self.release = threading.Event()

            def get(self, key: object, default: object = None) -> object:
                self.entered.set()
                self.release.wait()
                return super().get(key, default)

        app = _CloseAttemptApp()
        compiled = app.register_workflow(
            Workflow("snapshot-before-close", nodes=[Node("node", identity)])
        )
        snapshots = BlockingSnapshots(dict(app._workflow_definition_snapshots))
        app._workflow_definition_snapshots = snapshots  # type: ignore[assignment]
        order: list[str] = []
        errors: list[BaseException] = []

        def snapshot() -> None:
            try:
                value = app.workflow_definition_snapshot(
                    compiled.workflow_revision_id
                )
                self.assertEqual(value.workflow_revision_id, compiled.workflow_revision_id)
                order.append("snapshot")
            except BaseException as error:
                errors.append(error)

        def close() -> None:
            try:
                app.close(timeout=1)
                order.append("close")
            except BaseException as error:
                errors.append(error)

        snapshot_thread = threading.Thread(target=snapshot)
        close_thread = threading.Thread(target=close)
        snapshot_thread.start()
        self.assertTrue(snapshots.entered.wait(1))
        close_thread.start()
        close_attempted = app.close_attempted.wait(1)
        lifecycle_owned = app._close_lock.locked()
        snapshots.release.set()
        snapshot_thread.join(1)
        close_thread.join(1)
        self.assertTrue(close_attempted)
        self.assertTrue(lifecycle_owned)
        self.assertFalse(snapshot_thread.is_alive())
        self.assertFalse(close_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(order, ["snapshot", "close"])

    def test_every_control_validates_invocation_ref_on_runtime_loop(self) -> None:
        """Verify generic controls validate InvocationRefs on the Runtime loop."""

        app = _ChildRefThreadApp()
        invalid = cast(InvocationRef, {})
        try:
            for operation in (
                lambda: app.status(invalid),
                lambda: app.join(invalid),
                lambda: app.cancel(invalid),
            ):
                with self.assertRaisesRegex(TypeError, "InvocationRef"):
                    operation()

            async def exercise_async() -> None:
                for operation in (
                    lambda: app.astatus(invalid),
                    lambda: app.ajoin(invalid),
                    lambda: app.acancel(invalid),
                ):
                    with self.assertRaisesRegex(TypeError, "InvocationRef"):
                        await operation()

            asyncio.run(exercise_async())
            runtime_thread = app._runtime_loop._thread
            self.assertIsNotNone(runtime_thread)
            self.assertEqual(len(app.child_ref_threads), 6)
            self.assertTrue(
                all(
                    thread_id == runtime_thread.ident
                    for thread_id in app.child_ref_threads
                )
            )
            self.assertNotEqual(threading.get_ident(), runtime_thread.ident)
        finally:
            app.close(timeout=1)

    def test_closed_stream_does_not_commit_a_queued_publisher(self) -> None:
        """Verify a failed stream cancels queued publishers before state creation."""

        async def run() -> None:
            channel = AttachedStream()
            first_entered = asyncio.Event()
            release_first = asyncio.Event()
            second_created = False

            async def fail_first() -> object:
                first_entered.set()
                await release_first.wait()
                raise RuntimeError("publish failed")

            async def create_second() -> object:
                nonlocal second_created
                second_created = True
                return object()

            first = asyncio.create_task(channel.publish_async_created(fail_first))
            second = asyncio.create_task(channel.publish_async_created(create_second))
            receive = asyncio.create_task(channel.receive())
            await first_entered.wait()
            release_first.set()

            with self.assertRaisesRegex(RuntimeError, "publish failed"):
                await receive
            with self.assertRaisesRegex(RuntimeError, "publish failed"):
                await first
            with self.assertRaises(asyncio.CancelledError):
                await second
            self.assertFalse(second_created)

        asyncio.run(run())

    def test_abandoned_stream_does_not_commit_a_waiting_publisher(self) -> None:
        """Verify caller abandonment cancels a publisher before state creation."""

        async def run() -> None:
            channel = AttachedStream()
            created = False

            async def create() -> object:
                nonlocal created
                created = True
                return object()

            publisher = asyncio.create_task(channel.publish_async_created(create))
            await asyncio.sleep(0)
            channel.abandon()
            with self.assertRaises(asyncio.CancelledError):
                await publisher
            self.assertFalse(created)

        asyncio.run(run())

    def test_terminal_delivery_gracefully_detaches_later_publisher(self) -> None:
        """Verify final Result lets later detached work commit without demand."""

        async def run() -> None:
            channel = AttachedStream()
            receive = asyncio.create_task(channel.receive())
            terminal = object()
            terminal_publish = asyncio.create_task(
                channel.publish_terminal(terminal)
            )
            self.assertIs(await receive, terminal)

            created = False

            async def create_detached() -> object:
                nonlocal created
                created = True
                return object()

            queued = asyncio.create_task(
                channel.publish_async_created(create_detached)
            )
            channel.abandon()
            await terminal_publish
            await queued
            self.assertTrue(created)

        asyncio.run(run())

    def test_completed_parent_stream_detaches_a_spawned_child_without_cancelling_it(
        self,
    ) -> None:
        """Verify normal stream completion lets a spawned Child continue detached."""

        child = Workflow("stream-detached-child", nodes=[Node("node", identity)])
        parent = Workflow(
            "stream-detached-parent",
            nodes=[Node("child", child, execution_mode="spawn")],
        )
        app = AutoAgentApp()
        try:
            items = list(
                app.stream(
                    parent,
                    {"value": 1},
                    session_id="stream-detached-parent-session",
                )
            )
            result = items[-1]
            self.assertEqual(result.status, "completed")
            child_result = join_observed(app, result.output, timeout=1.0)
            self.assertEqual(child_result.status, "completed")
            self.assertEqual(child_result.output, {"value": 1})
        finally:
            app.close()

    def test_final_stream_result_waits_for_spawn_child(self):
        """Stream cannot deliver its final result while structured work is running."""
        child_started = _ThreadSignal()
        release_child = _RuntimeGate()
        async def work(value: Value) -> Value:
            child_started.set()
            await release_child.wait()
            return value
        app = AutoAgentApp()
        stream = app.stream(Workflow("stream-root", nodes=[Node("spawn",
            Workflow("stream-child", nodes=[Node("work", work)]), execution_mode="spawn")]), {"value": 1})
        results = []
        reader = threading.Thread(target=lambda: results.extend(stream))
        try:
            reader.start()
            self.assertTrue(child_started.wait(1))
            self.assertFalse(any(isinstance(item, InvocationResult) for item in results))
            _release_runtime_gate_sync(app._runtime_loop, release_child)
            reader.join(2)
            self.assertFalse(reader.is_alive())
            self.assertEqual(results[-1].status, "completed")
        finally:
            _release_runtime_gate_sync(app._runtime_loop, release_child)
            stream.close()
            reader.join(2)
            app.close()


    def test_result_waits_behind_queued_transitions_before_reading_state(self) -> None:
        """Verify Result State and Checkpoint share one queued lock boundary."""

        sink = _BlockingSink()
        app = _QueuedResultApp(runtime_event_sink=sink)
        result_thread: threading.Thread | None = None
        try:
            waiting = app.invoke(
                Workflow(
                    "queued-result-state",
                    nodes=[Node("approval", Wait(Value, Value))],
                ),
                {"value": 1},
                session_id="queued-result-session",
            )
            self.assertEqual(waiting.status, "waiting")
            sink.target_session_id = waiting.session_id
            sink.target_kind = "recovery.applied"
            sink.enabled = True

            holder = app._runtime_loop.submit(
                app._emit(
                    waiting.session_id,
                    waiting.invocation_id,
                    RecoveryApplied(),
                )
            )
            self.assertTrue(sink.entered.wait(1))
            cancellation = app._runtime_loop.submit(
                app._emit(
                    waiting.session_id,
                    waiting.invocation_id,
                    InvocationCancelled("queued before Result"),
                )
            )
            self.assertTrue(app.cancel_emit_entered.wait(1))

            result_box: list[InvocationResult | BaseException] = []

            def observe_result() -> None:
                try:
                    result_box.append(app._run(app._result(waiting.ref)))
                except BaseException as error:
                    result_box.append(error)

            result_thread = threading.Thread(target=observe_result)
            result_thread.start()
            self.assertTrue(app.result_entered.wait(1))
            _release_runtime_gate_sync(app._runtime_loop, sink.release)
            holder.result(1)
            cancellation.result(1)
            result_thread.join(1)
            self.assertFalse(result_thread.is_alive())
            self.assertEqual(len(result_box), 1)
            if isinstance(result_box[0], BaseException):
                raise result_box[0]
            result = result_box[0]
            checkpoint_invocation = app._repository.state(result.session_id).invocation
            self.assertIsNotNone(checkpoint_invocation)
            self.assertEqual(result.status, "cancelled")
            self.assertEqual(result.status, checkpoint_invocation.status)
            self.assertEqual(result.invocation_id, checkpoint_invocation.id)
        finally:
            sink.enabled = False
            _release_runtime_gate_sync(app._runtime_loop, sink.release)
            if result_thread is not None:
                result_thread.join(1)
            app.close(timeout=1)



    def test_recover_accepts_opened_spawn_child_already_waiting(self) -> None:
        """Verify opened Child admission is reconciled before waiting recovery."""

        child = Workflow(
            "recover-opened-wait-child",
            nodes=[Node("approval", Wait(Value, Value))],
        )
        parent = Workflow(
            "recover-opened-wait-parent",
            nodes=[Node("spawn", child, execution_mode="spawn")],
        )
        source = AutoAgentApp()
        try:
            parent_result = source.invoke(
                parent,
                {"value": 1},
                session_id="recover-opened-wait-root",
            )
            handle = parent_result.output
            waiting = join_observed(source, handle, timeout=1)
            graph = source.unload_session(parent_result.ref, capture_checkpoint=True)
            child_checkpoint = next(s for s in graph.sessions if s.session_id == waiting.session_id)
            parent_checkpoint = root_snapshot(graph)
        finally:
            source.close(timeout=1)

        root_state = root_snapshot(parent_checkpoint).state
        assert root_state.invocation is not None
        creation_id, plan = next(iter(root_state.invocation.child_plans.items()))
        opened_plan = replace(
            plan,
            units=(replace(plan.units[0], phase="opened"),),
        )
        opened_root = replace(
            root_state,
            invocation=replace(
                root_state.invocation,
                child_plans={creation_id: opened_plan},
            ),
        )
        opened_checkpoint = SessionCheckpoint.from_state(opened_root)

        recovered = AutoAgentApp()
        try:
            recovered.register_workflow(parent)
            loaded = load_graph(recovered,
                graph_bundle((opened_checkpoint, child_checkpoint))
            )
            root_ref = next(
                ref for ref in loaded.invocations
                if ref.session_id == "recover-opened-wait-root"
            )
            root_result = recovered.recover(root_ref)
            self.assertEqual(root_result.status, "settling")
            root_after_recovery = recovered._repository.state(root_result.session_id)
            recovered_plan = next(
                iter(root_after_recovery.invocation.child_plans.values())
            )
            self.assertEqual(recovered_plan.units[0].phase, "accepted")

            child_waiting = status_observed(recovered, handle)
            self.assertEqual(child_waiting.status, "waiting")
            completed = resume_graph_wait(recovered,
                child_waiting.ref,
                child_waiting.waits[0].id,
                {"value": 2},
            )
            self.assertEqual(completed.status, "completed")
            root_after_completion = recovered._repository.state(root_result.session_id)
            completed_plan = next(
                iter(root_after_completion.invocation.child_plans.values())
            )
            self.assertEqual(completed_plan.units[0].phase, "terminal")
        finally:
            recovered.close(timeout=1)



    def test_cancelled_aresume_does_not_leave_running_state_without_task(self) -> None:
        """Verify caller cancellation cannot orphan resumed physical work."""

        async def run() -> None:
            started = _ThreadSignal()

            async def slow(value: Value) -> Value:
                started.set()
                await asyncio.Event().wait()
                return value

            app = AutoAgentApp()
            try:
                waiting = await app.ainvoke(
                    Workflow(
                        "cancel-resume",
                        nodes=[Node("wait", Wait(Value, Value)), Node("slow", slow)],
                        edges=[Edge("wait", "slow")],
                    ),
                    {"value": 1},
                )
                self.assertEqual(waiting.status, "waiting")
                resume = asyncio.create_task(
                    async_resume_graph_wait(app, waiting.ref, waiting.waits[0].id, {"value": 2})
                )
                self.assertTrue(await started.wait_async())
                resume.cancel()
                await asyncio.gather(resume, return_exceptions=True)

                state = app._repository.state(waiting.session_id)
                self.assertIsNotNone(state.invocation)
                orphaned = (
                    state.invocation.status == "running"
                    and not app._task_runtime.is_live(waiting.session_id)
                )
                self.assertFalse(orphaned)
            finally:
                await app.aclose(timeout=1.0)

        asyncio.run(run())

    def test_cancelled_recover_does_not_leave_running_state_without_task(self) -> None:
        """Verify caller cancellation cannot orphan crash-replayed work."""

        async def run() -> None:
            first_started = _ThreadSignal()
            replay_started = _ThreadSignal()
            calls = 0

            async def replayable(value: Value) -> Value:
                nonlocal calls
                calls += 1
                (first_started if calls == 1 else replay_started).set()
                await asyncio.Event().wait()
                return value

            workflow = Workflow(
                "cancel-recover",
                nodes=[
                    Node(
                        "work",
                        replayable,
                        recovery_mode=Recovery("replay_safe"),
                    )
                ],
            )
            source = AutoAgentApp()
            await source.asubmit_invoke(
                workflow,
                {"value": 1},
                session_id="cancel-recover-session",
            )
            self.assertTrue(await first_started.wait_async())
            source_checkpoint = (await source.aclose(timeout=1.0, capture_checkpoint=True)).graphs[0]

            recovered = AutoAgentApp()
            try:
                recovered.register_workflow(workflow)
                loaded = await recovered.aload_checkpoint(source_checkpoint)
                recovery = asyncio.create_task(recovered.arecover(loaded.invocations[0]))
                self.assertTrue(await replay_started.wait_async())
                recovery.cancel()
                await asyncio.gather(recovery, return_exceptions=True)

                state = recovered._repository.state(loaded.invocations[0].session_id)
                self.assertIsNotNone(state.invocation)
                orphaned = (
                    state.invocation.status == "running"
                    and not recovered._task_runtime.is_live(loaded.invocations[0].session_id)
                )
                self.assertFalse(orphaned)
            finally:
                await recovered.aclose(timeout=1.0)

        asyncio.run(run())

    def test_cancel_operation_survives_caller_cancellation(self) -> None:
        """Verify graph cancellation continues until every Child is terminal."""

        async def run() -> None:
            child_started = _ThreadSignal()
            child_cancelled = _ThreadSignal()
            parent_started = _ThreadSignal()
            parent_cancelled = _ThreadSignal()

            async def child_block(value: Value) -> Value:
                child_started.set()
                try:
                    await asyncio.Event().wait()
                    return value
                finally:
                    child_cancelled.set()

            async def parent_block(
                handle: ChildHandle,
            ) -> ChildHandle:
                parent_started.set()
                try:
                    await asyncio.Event().wait()
                    return handle
                finally:
                    parent_cancelled.set()

            child = Workflow("cancel-child", nodes=[Node("work", child_block)])
            parent = Workflow(
                "cancel-parent",
                nodes=[
                    Node("spawn", child, execution_mode="spawn"),
                    Node("hold", parent_block),
                ],
                edges=[Edge("spawn", "hold")],
            )
            sink = _BlockingSink()
            app = AutoAgentApp(runtime_event_sink=sink)
            try:
                submitted = await app.asubmit_invoke(
                    parent,
                    {"value": 1},
                    session_id="cancel-parent-session",
                )
                self.assertTrue(await child_started.wait_async())
                self.assertTrue(await parent_started.wait_async())
                handles = await async_children(app, submitted.ref)
                self.assertEqual(len(handles), 1)

                sink.target_session_id = submitted.session_id
                sink.target_kind = "invocation.cancelled"
                sink.enabled = True
                cancellation = asyncio.create_task(app.acancel(submitted.ref))
                self.assertTrue(await sink.entered.wait_async())
                cancellation.cancel()
                await _release_runtime_gate(app._runtime_loop, sink.release)
                done, _pending = await asyncio.wait({cancellation}, timeout=1)
                self.assertIn(cancellation, done)
                await asyncio.gather(cancellation, return_exceptions=True)

                self.assertTrue(await child_cancelled.wait_async())
                self.assertTrue(await parent_cancelled.wait_async())
                child_state = app._repository.state(handles[0].session_id)
                self.assertIsNotNone(child_state.invocation)
                self.assertEqual(child_state.invocation.status, "cancelled")
            finally:
                await _release_runtime_gate(app._runtime_loop, sink.release)
                await app.aclose(timeout=1.0)

        asyncio.run(run())




    def test_concurrent_aclose_calls_share_one_result(self) -> None:
        """Verify concurrent asynchronous close calls are one idempotent operation."""

        async def run() -> None:
            app = _CoordinatedCloseApp()
            await app.ainvoke(
                Workflow("concurrent-aclose", nodes=[Node("node", identity)]),
                {"value": 1},
            )
            first = asyncio.create_task(app.aclose(capture_checkpoint=True))
            second = asyncio.create_task(app.aclose(capture_checkpoint=True))
            try:
                self.assertTrue(
                    await app.both_close_callers_entered.wait_async()
                )
                self.assertTrue(await app.close_operation_entered.wait_async())
                await _release_runtime_gate(
                    app._runtime_loop, app.release_close_operations
                )
                self.assertTrue(await app.close_gate_released.wait_async())
                self.assertTrue(await app.close_operation_completed.wait_async())
                done, _pending = await asyncio.wait({first, second}, timeout=2)
                self.assertEqual(done, {first, second})
                results = [task.result() for task in (first, second)]
                self.assertTrue(all(isinstance(item, AppCheckpoint) for item in results))
                self.assertEqual(results[0], results[1])
                self.assertEqual(app.close_entries, 1)
            finally:
                await _release_runtime_gate(
                    app._runtime_loop, app.release_close_operations
                )
                done, pending = await asyncio.wait({first, second}, timeout=2)
                for task in pending:
                    task.cancel()
                await asyncio.gather(*done, *pending, return_exceptions=True)

        asyncio.run(run())

    def test_concurrent_close_calls_share_one_result(self) -> None:
        """Verify concurrent synchronous close calls are one idempotent operation."""

        app = _CoordinatedCloseApp()
        app.invoke(
            Workflow("concurrent-close", nodes=[Node("node", identity)]),
            {"value": 1},
        )
        barrier = threading.Barrier(3)
        results: list[AppCheckpoint] = []
        errors: list[BaseException] = []

        def close() -> None:
            barrier.wait()
            try:
                results.append(app.close(capture_checkpoint=True))
            except BaseException as error:
                errors.append(error)

        threads = [threading.Thread(target=close) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        try:
            self.assertTrue(app.both_close_callers_entered.wait(1))
            self.assertTrue(app.close_operation_entered.wait(1))
            _release_runtime_gate_sync(
                app._runtime_loop, app.release_close_operations
            )
            for thread in threads:
                thread.join(1)
            self.assertFalse(any(thread.is_alive() for thread in threads))
            self.assertEqual(errors, [])
            self.assertEqual(len(results), 2)
            self.assertEqual(results[0], results[1])
            self.assertEqual(app.close_entries, 1)
        finally:
            _release_runtime_gate_sync(
                app._runtime_loop, app.release_close_operations
            )
            for thread in threads:
                thread.join(1)
            app.close()

    def test_close_timeout_detaches_without_cancelling_shared_quiescence(self) -> None:
        """Verify a close timeout leaves one continuing operation that can be rejoined."""

        async def run() -> None:
            app = _CoordinatedCloseApp()
            await app.ainvoke(
                Workflow("close-timeout", nodes=[Node("node", identity)]),
                {"value": 1},
            )
            first = asyncio.create_task(app.aclose(timeout=0.02, capture_checkpoint=True))
            self.assertTrue(await app.close_operation_entered.wait_async())
            with self.assertRaises(TimeoutError):
                await first
            self.assertTrue(app._closing)
            self.assertFalse(app._closed)
            with self.assertRaises(RuntimeError):
                await app.ainvoke(
                    Workflow("rejected-while-closing", nodes=[Node("node", identity)]),
                    {"value": 2},
                )

            await _release_runtime_gate(
                app._runtime_loop, app.release_close_operations
            )
            checkpoint = await app.aclose(timeout=1.0, capture_checkpoint=True)
            self.assertIsInstance(checkpoint, AppCheckpoint)
            self.assertTrue(app._closed)
            self.assertEqual(app.close_entries, 1)

        asyncio.run(run())

    def test_close_rejects_an_oversized_timeout_before_shutdown(self) -> None:
        """Reject integers that cannot be represented by the timeout machinery."""

        app = AutoAgentApp()
        try:
            with self.assertRaises(ValueError):
                app.close(timeout=10**1000)
            self.assertFalse(app._closing)
        finally:
            app.close()

    def test_close_captures_a_preloaded_journal_before_runtime_loop_start(self) -> None:
        """Verify lazy App shutdown preserves an injected root/child Runtime graph."""

        source = AutoAgentApp()
        child = Workflow("preloaded-close-child", nodes=[Node("child", identity)])
        source.invoke(
            Workflow(
                "preloaded-close-parent",
                nodes=[Node("spawn", child, execution_mode="spawn")],
            ),
            {"value": 1},
            session_id="preloaded-close-root",
        )
        checkpoint = source.close(capture_checkpoint=True)
        self.assertEqual(len(session_checkpoints(checkpoint)), 2)

        journal = RuntimeRepository()
        journal.install_states(
            {item.session_id: item.state for item in session_checkpoints(checkpoint)}
        )
        restored = AutoAgentApp(runtime_repository=journal)
        captured = restored.close(capture_checkpoint=True)

        self.assertEqual(len(session_checkpoints(captured)), 2)
        self.assertEqual(
            {item.session_id: item.state for item in session_checkpoints(captured)},
            {item.session_id: item.state for item in session_checkpoints(checkpoint)},
        )

    def test_public_invocation_status_includes_child_creation(self) -> None:
        """Verify Child admission's created phase is represented by the SDK type."""

        self.assertIn("created", get_args(InvocationStatus))

    def test_app_checkpoint_keeps_related_sessions_as_separate_entries(self) -> None:
        """Verify Parent and Child snapshots coexist without graph aggregation."""

        _child, _parent, planned, child_bundle, _child_session_id = (
            _cross_root_checkpoint_pair()
        )
        checkpoint = graph_bundle((planned, child_bundle))
        self.assertEqual(len(session_checkpoints(checkpoint)), 2)


    def test_replacement_sink_failure_retires_superseded_child_graph(self) -> None:
        """Verify durable root replacement retires old Children despite sink failure."""

        class RejectReplacementSink:
            def __init__(self) -> None:
                self.enabled = False

            async def append(self, event: RuntimeEvent) -> None:
                if (
                    self.enabled
                    and event.session_id == "replacement-sink-root"
                    and any(
                        log.event_name == "invocation.started" for log in (event,)
                    )
                ):
                    raise RuntimeError("sink rejected replacement")

        journal = RuntimeRepository()
        sink = RejectReplacementSink()
        child = Workflow(
            "replacement-sink-child",
            nodes=[Node("work", identity)],
        )
        parent = Workflow(
            "replacement-sink-parent",
            nodes=[Node("spawn", child, execution_mode="spawn")],
        )
        app = AutoAgentApp(runtime_repository=journal, runtime_event_sink=sink)
        try:
            first = app.invoke(
                parent,
                {"value": 1},
                session_id="replacement-sink-root",
            )
            handle = child_refs(app, first.ref)[0]
            self.assertEqual(join_observed(app, handle, timeout=1).status, "completed")

            sink.enabled = True
            with self.assertRaises(RuntimeInfrastructureError):
                app.invoke(
                    parent,
                    {"value": 2},
                    session_id="replacement-sink-root",
                )
            sink.enabled = False

            self.assertIn(handle.session_id, journal.session_ids())
            current = journal.state("replacement-sink-root").invocation
            self.assertIsNotNone(current)
            self.assertEqual(current.id, first.invocation_id)
            self.assertEqual(current.status, "completed")

            closed = app.close(timeout=1, capture_checkpoint=True)
            self.assertEqual(len(session_checkpoints(closed)), 1)
            self.assertEqual(session_checkpoints(closed)[0].session_id, "replacement-sink-root")
        finally:
            sink.enabled = False
            if not app._closed:
                app.close(timeout=1)

    def test_replacement_waits_for_unconfirmed_spawned_child_event(self) -> None:
        """Verify old Child Events are confirmed before their graph is retired."""

        class RejectChildTerminalSink:
            def __init__(self) -> None:
                self.enabled = True
                self.rejected = _ThreadSignal()
                self.rejected_event_id: str | None = None
                self.accepted_event_ids: list[str] = []

            async def append(self, event: RuntimeEvent) -> None:
                is_child_terminal = (
                    event.session_id != "child-event-retirement-root"
                    and any(
                        log.event_name == "invocation.completed"
                        for log in (event,)
                    )
                )
                if self.enabled and is_child_terminal:
                    self.rejected_event_id = event.id
                    self.rejected.set()
                    raise RuntimeError("reject Child terminal Event")
                self.accepted_event_ids.append(event.id)

        release_child = _RuntimeGate()
        child_started = _ThreadSignal()

        async def child_work(value: Value) -> Value:
            child_started.set()
            await release_child.wait()
            return value

        journal = RuntimeRepository()
        sink = RejectChildTerminalSink()
        child = Workflow(
            "child-event-retirement-child",
            nodes=[Node("work", child_work)],
        )
        parent = Workflow(
            "child-event-retirement-parent",
            nodes=[Node("spawn", child, execution_mode="spawn")],
        )
        app = AutoAgentApp(runtime_repository=journal, runtime_event_sink=sink)
        try:
            first = app.submit_invoke(
                parent,
                {"value": 1},
                session_id="child-event-retirement-root",
            )
            self.assertEqual(first.status, "running")
            self.assertTrue(child_started.wait(1))
            handle = child_refs(app, first.ref)[0]
            _release_runtime_gate_sync(app._runtime_loop, release_child)
            self.assertTrue(sink.rejected.wait(1))

            with self.assertRaisesRegex(RuntimeTransitionError, "SESSION_INVOCATION_ACTIVE"):
                app.invoke(
                    parent,
                    {"value": 2},
                    session_id="child-event-retirement-root",
                )

            self.assertEqual(
                journal.state("child-event-retirement-root").invocation.id,
                first.invocation_id,
            )
            self.assertIn(handle.session_id, journal.session_ids())
            self.assertFalse(journal.state(handle.session_id).invocation.terminal)
            self.assertNotIn(sink.rejected_event_id, sink.accepted_event_ids)

            sink.enabled = False
            self.assertEqual(app.recover(first.ref).status, "completed")
            second = app.invoke(
                parent,
                {"value": 2},
                session_id="child-event-retirement-root",
            )
            self.assertEqual(second.status, "completed")
            self.assertIn(sink.rejected_event_id, sink.accepted_event_ids)
            self.assertNotIn(handle.session_id, journal.session_ids())

            closed = app.close(timeout=1, capture_checkpoint=True)
            self.assertEqual(len(session_checkpoints(closed)), 2)
            self.assertIn(
                "child-event-retirement-root",
                {item.session_id for item in session_checkpoints(closed)},
            )
        finally:
            sink.enabled = False
            _release_runtime_gate_sync(app._runtime_loop, release_child)
            if not app._closed:
                app.close(timeout=1)

    def test_replacement_rejects_a_terminal_child_still_settling(self) -> None:
        """Keep the old Root until its terminal Child phase is durably settled."""

        class BlockChildTerminalSink:
            def __init__(self, root_session_id: str) -> None:
                self.root_session_id = root_session_id
                self.entered = _ThreadSignal()
                self.release = _RuntimeGate()
                self.events: list[RuntimeEvent] = []
                self.blocked = False

            async def append(self, event: RuntimeEvent) -> None:
                self.events.append(event)
                if (
                    not self.blocked
                    and event.session_id != self.root_session_id
                    and any(
                        log.event_name == "invocation.completed"
                        for log in (event,)
                    )
                ):
                    self.blocked = True
                    self.entered.set()
                    await self.release.wait()

        root_session_id = "settling-replacement-root"
        child_release = _RuntimeGate()
        child_started = _ThreadSignal()

        async def child_work(value: Value) -> Value:
            child_started.set()
            await child_release.wait()
            return value

        journal = RuntimeRepository()
        sink = BlockChildTerminalSink(root_session_id)
        child = Workflow(
            "settling-replacement-child",
            nodes=[Node("work", child_work)],
        )
        parent = Workflow(
            "settling-replacement-parent",
            nodes=[Node("spawn", child, execution_mode="spawn")],
        )
        app = AutoAgentApp(runtime_repository=journal, runtime_event_sink=sink)
        replacement_done = threading.Event()
        replacement_errors: list[BaseException] = []
        replacement_results: list[InvocationResult] = []

        def replace_root() -> None:
            try:
                replacement_results.append(
                    app.invoke(
                        parent,
                        {"value": 2},
                        session_id=root_session_id,
                    )
                )
            except BaseException as error:
                replacement_errors.append(error)
            finally:
                replacement_done.set()

        replacement = threading.Thread(target=replace_root)
        try:
            first = app.submit_invoke(
                parent,
                {"value": 1},
                session_id=root_session_id,
            )
            self.assertEqual(first.status, "running")
            self.assertTrue(child_started.wait(1))
            handle = child_refs(app, first.ref)[0]

            _release_runtime_gate_sync(app._runtime_loop, child_release)
            self.assertTrue(sink.entered.wait(1))
            child_state = journal.state(handle.session_id).invocation
            parent_state = journal.state(root_session_id).invocation
            self.assertIsNotNone(child_state)
            self.assertIsNotNone(parent_state)
            assert child_state is not None and parent_state is not None
            self.assertFalse(child_state.terminal)
            self.assertEqual(
                next(iter(parent_state.child_plans.values())).units[0].phase,
                "accepted",
            )

            replacement.start()
            self.assertTrue(replacement_done.wait(1))
            self.assertEqual(replacement_results, [])
            self.assertEqual(len(replacement_errors), 1)
            self.assertIsInstance(replacement_errors[0], RuntimeTransitionError)
            self.assertEqual(
                cast(RuntimeTransitionError, replacement_errors[0]).code,
                "SESSION_INVOCATION_ACTIVE",
            )
            self.assertEqual(
                journal.state(root_session_id).invocation.id,  # type: ignore[union-attr]
                first.invocation_id,
            )

            _release_runtime_gate_sync(app._runtime_loop, sink.release)
            settled = join_observed(app, handle, timeout=1)
            self.assertEqual(settled.status, "completed")
            parent_state = journal.state(root_session_id).invocation
            assert parent_state is not None
            self.assertEqual(
                next(iter(parent_state.child_plans.values())).units[0].phase,
                "terminal",
            )
            self.assertTrue(
                any(
                    log.event_name == "child_invocation.phase_changed"
                    and getattr(log.payload, "phase", None) == "terminal"
                    for event in sink.events
                    if event.session_id == root_session_id
                    for log in (event,)
                )
            )

            second = app.invoke(
                parent,
                {"value": 2},
                session_id=root_session_id,
            )
            self.assertEqual(second.status, "completed")
            second_handle = child_refs(app, second.ref)[0]
            self.assertEqual(
                join_observed(app, second_handle, timeout=1).status,
                "completed",
            )
        finally:
            _release_runtime_gate_sync(app._runtime_loop, child_release)
            _release_runtime_gate_sync(app._runtime_loop, sink.release)
            if replacement.is_alive():
                replacement.join(1)
            if not app._closed:
                app.close(timeout=1)

    def test_runtime_loop_submit_and_close_are_linearizable(self) -> None:
        """Verify shutdown cannot strand a submission after its closed check."""

        runtime = RuntimeLoop()
        checked = threading.Event()
        release = threading.Event()
        original_ensure_started = runtime._ensure_started_locked

        def paused_ensure_started() -> None:
            original_ensure_started()
            checked.set()
            release.wait()

        runtime._ensure_started_locked = paused_ensure_started  # type: ignore[method-assign]

        async def complete() -> None:
            return None

        coroutine = complete()
        holder: dict[str, object] = {}

        def submit() -> None:
            try:
                holder["future"] = runtime.submit(coroutine)
            except BaseException as error:
                holder["error"] = error

        thread = threading.Thread(target=submit)
        thread.start()
        self.assertTrue(checked.wait(1))
        close_thread = threading.Thread(target=runtime.close)
        close_thread.start()
        release.set()
        thread.join(1)
        close_thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertFalse(close_thread.is_alive())

        future = holder.get("future")
        settled = "error" in holder or (future is not None and future.done())  # type: ignore[union-attr]
        if future is not None and not future.done():  # type: ignore[union-attr]
            future.cancel()  # type: ignore[union-attr]
            coroutine.close()
            with runtime._actions_lock:
                runtime._actions.clear()
        self.assertTrue(settled)

    def test_runtime_loop_consumes_late_error_after_bridge_cancellation(self) -> None:
        """Verify a cancelled bridge cannot leak its Task's later exception."""

        runtime = RuntimeLoop()
        started = _ThreadSignal()
        failed = _ThreadSignal()
        unhandled: list[dict[str, object]] = []

        async def fail_after_cancellation() -> None:
            loop = asyncio.get_running_loop()
            loop.set_exception_handler(
                lambda _loop, context: unhandled.append(dict(context))
            )
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                failed.set()
                raise RuntimeError("late RuntimeLoop task failure")

        future = runtime.submit(fail_after_cancellation())
        try:
            self.assertTrue(started.wait(1))
            self.assertTrue(future.cancel())
            self.assertTrue(failed.wait(1))
        finally:
            runtime.close()

        self.assertTrue(future.cancelled())
        self.assertFalse(
            any(
                item.get("message") == "Task exception was never retrieved"
                for item in unhandled
            )
        )

    def test_runtime_loop_async_wait_joins_cancelled_task_cleanup(self) -> None:
        """Verify async bridge cancellation returns after physical Task cleanup."""

        async def run() -> None:
            runtime = RuntimeLoop()
            started = _ThreadSignal()
            cleaned = _ThreadSignal()

            async def block() -> None:
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    # Force cleanup to cross an event-loop scheduling boundary.
                    await asyncio.sleep(0)
                    cleaned.set()

            future = runtime.submit(block())
            waiter = asyncio.create_task(runtime.wait(future))
            try:
                self.assertTrue(await started.wait_async())
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)
                self.assertTrue(cleaned.wait(0))
                self.assertTrue(future.cancelled())
            finally:
                runtime.close()

        asyncio.run(run())

    def test_recovery_write_ahead_boundaries_cannot_be_disabled(self) -> None:
        """Verify batching cannot remove the four mandatory recovery WAL points."""

        import inspect
        parameters = inspect.signature(RuntimeRepository).parameters
        self.assertNotIn("max_batches_per_event", parameters)
        self.assertNotIn("flush_event_names", parameters)

    def test_checkpoint_bundle_isolated_from_external_mutable_state(self) -> None:
        """Verify a checkpoint and loaded State cannot change through an old object."""

        workflow = Workflow("immutable-checkpoint", nodes=[Node("node", identity)])
        source = AutoAgentApp()
        try:
            result = source.invoke(
                workflow,
                {"value": 1},
                session_id="immutable-checkpoint-session",
            )
            checkpoint = source.unload_session(result.ref, capture_checkpoint=True)
        finally:
            source.close()

        state = root_snapshot(checkpoint).state
        self.assertIsNotNone(state.invocation)
        mutable_context: dict[str, object] = {}
        external_state = replace(
            state,
            invocation=replace(state.invocation, context=mutable_context),
        )
        try:
            external_bundle = SessionCheckpoint.from_state(external_state)
        except (TypeError, ValueError):
            return

        original_record = external_bundle.to_record()
        mutable_context["changed"] = 1
        self.assertEqual(external_bundle.to_record(), original_record)

        target = AutoAgentApp()
        try:
            target.register_workflow(workflow)
            load_graph(target, external_bundle)
            mutable_context["changed"] = 2
            installed = target._repository.state("immutable-checkpoint-session")
            self.assertNotIn("changed", installed.invocation.context)
        finally:
            target.close()


if __name__ == "__main__":
    unittest.main()
