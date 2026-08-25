from __future__ import annotations

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
    ChildInvocationHandle,
    Edge,
    InvocationRef,
    InvocationResult,
    InvocationStatus,
    InvocationUpdate,
    Node,
    Recovery,
    RuntimeInfrastructureError,
    RuntimeTransitionError,
    TraceEvent,
    Wait,
    Workflow,
)
from autoagent.core import (
    InMemoryEventJournal,
    InvocationCancelled,
    InvocationRecoveryRequested,
    RuntimeCheckpointBundle,
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
    RuntimeCheckpointBundle,
    RuntimeCheckpointBundle,
    str,
]:
    child = Workflow("cross-root-child", nodes=[Node("work", identity)])
    parent = Workflow("cross-root-parent", nodes=[Node("child", child)])
    source = AutoAgentApp()
    planned: RuntimeCheckpointBundle | None = None
    stream = source.stream(parent, {"value": 1}, session_id="cross-root-parent")
    try:
        for item in stream:
            if (
                isinstance(item, InvocationUpdate)
                and item.event.kind == "child_invocation.planned"
            ):
                planned = item.checkpoint
                break
    finally:
        stream.close()
        source.close()
    assert planned is not None
    parent_state = planned.state("cross-root-parent")
    assert parent_state.invocation is not None
    unit = next(iter(parent_state.invocation.child_plans.values())).units[0]

    child_source = AutoAgentApp()
    try:
        child_result = child_source.invoke(
            child,
            {"value": 2},
            session_id=unit.session_id,
        )
        child_state = child_result.checkpoint.state(unit.session_id)
    finally:
        child_source.close()
    assert child_state.session is not None and child_state.invocation is not None
    matching_state = replace(
        child_state,
        session=replace(
            child_state.session,
            latest_invocation_id=unit.invocation_id,
        ),
        invocation=replace(child_state.invocation, id=unit.invocation_id),
    )
    child_bundle = RuntimeCheckpointBundle.from_states(
        unit.session_id,
        {unit.session_id: matching_state},
    )
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
                or any(log.event_name == self.target_kind for log in event.logs)
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

    def _begin_close(self):  # type: ignore[no-untyped-def]
        future = super()._begin_close()
        with self.close_entries_lock:
            self.begin_close_calls += 1
            if self.begin_close_calls == 2:
                self.both_close_callers_entered.set()
        return future

    async def _close_operation(self) -> AppCheckpoint:
        with self.close_entries_lock:
            self.close_entries += 1
        self.close_operation_entered.set()
        await self.release_close_operations.wait()
        self.close_gate_released.set()
        result = await super()._close_operation()
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
    """Record which thread validates each public Child Handle."""

    def __init__(self) -> None:
        super().__init__()
        self.child_ref_threads: list[int] = []

    def _child_ref(self, handle: ChildInvocationHandle) -> InvocationRef:
        self.child_ref_threads.append(threading.get_ident())
        return super()._child_ref(handle)


class _CloseAttemptApp(AutoAgentApp):
    """Signal immediately before a caller enters close admission."""

    def __init__(self) -> None:
        super().__init__()
        self.close_attempted = _ThreadSignal()

    def _begin_close(self):  # type: ignore[no-untyped-def]
        self.close_attempted.set()
        return super()._begin_close()


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

    def test_every_child_control_validates_handle_on_runtime_loop(self) -> None:
        """Verify sync and async Child APIs never read Runtime State off-loop."""

        app = _ChildRefThreadApp()
        invalid = cast(ChildInvocationHandle, {})
        try:
            for operation in (
                lambda: app.child_status(invalid),
                lambda: app.wait_child(invalid),
                lambda: app.cancel_child(invalid),
            ):
                with self.assertRaisesRegex(
                    RuntimeTransitionError, "CHILD_HANDLE_INVALID"
                ):
                    operation()

            async def exercise_async() -> None:
                for operation in (
                    lambda: app.achild_status(invalid),
                    lambda: app.await_child(invalid),
                    lambda: app.acancel_child(invalid),
                ):
                    with self.assertRaisesRegex(
                        RuntimeTransitionError, "CHILD_HANDLE_INVALID"
                    ):
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
            child_result = app.wait_child(result.output, timeout=1.0)
            self.assertEqual(child_result.status, "completed")
            self.assertEqual(child_result.output, {"value": 1})
        finally:
            app.close()

    def test_final_stream_result_detaches_spawn_child_before_close(self) -> None:
        """Verify final Result itself releases detached Child publishers."""

        child_started = _ThreadSignal()
        release_child = _RuntimeGate()

        async def child_work(value: Value) -> Value:
            child_started.set()
            await release_child.wait()
            return value

        child = Workflow(
            "stream-break-child",
            nodes=[Node("work", child_work)],
        )
        parent = Workflow(
            "stream-break-parent",
            nodes=[Node("spawn", child, execution_mode="spawn")],
        )
        app = AutoAgentApp()
        stream = app.stream(
            parent,
            {"value": 1},
            session_id="stream-break-root",
        )
        try:
            result: InvocationResult | None = None
            for item in stream:
                if isinstance(item, InvocationResult):
                    result = item
                    break
            self.assertIsNotNone(result)
            self.assertTrue(child_started.wait(1))

            _release_runtime_gate_sync(app._runtime_loop, release_child)
            child_result = app.wait_child(result.output, timeout=1)  # type: ignore[union-attr]
            self.assertEqual(child_result.status, "completed")
            self.assertEqual(child_result.output, {"value": 1})
            self.assertIn(
                "invocation.completed",
                {event.kind for event in child_result.trace_events},
            )
            stream.close()
        finally:
            stream.close()
            _release_runtime_gate_sync(app._runtime_loop, release_child)
            app.close(timeout=1)

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
            sink.target_kind = "invocation.recovery_requested"
            sink.enabled = True

            holder = app._runtime_loop.submit(
                app._emit(
                    waiting.session_id,
                    waiting.invocation_id,
                    InvocationRecoveryRequested(),
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
            checkpoint_invocation = result.checkpoint.state(result.session_id).invocation
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

    def test_recover_terminal_root_restarts_spawn_child_without_awaiting_it(self) -> None:
        """Verify Root recovery preserves immediate-return spawn semantics."""

        first_started = _ThreadSignal()
        replay_started = _ThreadSignal()
        first_gate = _RuntimeGate()
        replay_gate = _RuntimeGate()
        calls = 0

        async def child_work(value: Value) -> Value:
            nonlocal calls
            calls += 1
            if calls == 1:
                first_started.set()
                await first_gate.wait()
            else:
                replay_started.set()
                await replay_gate.wait()
            return value

        child = Workflow(
            "recover-spawn-child",
            nodes=[
                Node(
                    "work",
                    child_work,
                    recovery_mode=Recovery("replay_safe"),
                )
            ],
        )
        parent = Workflow(
            "recover-spawn-parent",
            nodes=[Node("spawn", child, execution_mode="spawn")],
        )
        source = AutoAgentApp()
        try:
            parent_result = source.invoke(
                parent,
                {"value": 1},
                session_id="recover-spawn-root",
            )
            handle = parent_result.output
            self.assertTrue(first_started.wait(1))
            checkpoint = source.close(timeout=1).roots[0]
        finally:
            _release_runtime_gate_sync(source._runtime_loop, first_gate)
            if not source._closed:
                source.close(timeout=1)

        recovered = AutoAgentApp()
        recovery_thread: threading.Thread | None = None
        try:
            recovered.register_workflow(parent)
            loaded = recovered.load_checkpoint(checkpoint)
            result_box: list[InvocationResult | BaseException] = []
            recovery_done = _ThreadSignal()

            def recover_root() -> None:
                try:
                    result_box.append(recovered.recover(loaded.roots[0]))
                except BaseException as error:
                    result_box.append(error)
                finally:
                    recovery_done.set()

            recovery_thread = threading.Thread(target=recover_root)
            recovery_thread.start()
            self.assertTrue(replay_started.wait(1))
            self.assertTrue(recovery_done.wait(1))
            recovery_thread.join(1)
            self.assertEqual(len(result_box), 1)
            if isinstance(result_box[0], BaseException):
                raise result_box[0]
            recovered_root = result_box[0]
            self.assertEqual(recovered_root.status, "completed")
            running_child = recovered.child_status(handle)
            self.assertEqual(running_child.status, "running")
            self.assertTrue(recovered._task_runtime.is_live(handle["session_id"]))

            _release_runtime_gate_sync(recovered._runtime_loop, replay_gate)
            completed_child = recovered.wait_child(handle, timeout=1)
            self.assertEqual(completed_child.status, "completed")
            self.assertEqual(completed_child.output, {"value": 1})
        finally:
            _release_runtime_gate_sync(recovered._runtime_loop, replay_gate)
            if recovery_thread is not None:
                recovery_thread.join(1)
            recovered.close(timeout=1)

    def test_recover_terminal_root_leaves_spawn_child_waiting(self) -> None:
        """Verify a waiting spawn Child does not delay recovered Root delivery."""

        child = Workflow(
            "recover-spawn-wait-child",
            nodes=[Node("approval", Wait(Value, Value))],
        )
        parent = Workflow(
            "recover-spawn-wait-parent",
            nodes=[Node("spawn", child, execution_mode="spawn")],
        )
        source = AutoAgentApp()
        try:
            parent_result = source.invoke(
                parent,
                {"value": 1},
                session_id="recover-spawn-wait-root",
            )
            handle = parent_result.output
            waiting = source.wait_child(handle, timeout=1)
            self.assertEqual(waiting.status, "waiting")
            checkpoint = source.close(timeout=1).roots[0]
        finally:
            if not source._closed:
                source.close(timeout=1)

        recovered = AutoAgentApp()
        try:
            recovered.register_workflow(parent)
            loaded = recovered.load_checkpoint(checkpoint)
            recovered_root = recovered.recover(loaded.roots[0])
            self.assertEqual(recovered_root.status, "completed")
            waiting = recovered.child_status(handle)
            self.assertEqual(waiting.status, "waiting")
            self.assertFalse(recovered._task_runtime.is_live(handle["session_id"]))

            completed = recovered.resume(
                waiting.ref,
                waiting.waits[0].id,
                {"value": 2},
            )
            self.assertEqual(completed.status, "completed")
            self.assertEqual(completed.output, {"value": 2})
        finally:
            recovered.close(timeout=1)

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
            waiting = source.wait_child(handle, timeout=1)
            checkpoint = waiting.checkpoint
        finally:
            source.close(timeout=1)

        root_state = checkpoint.state("recover-opened-wait-root")
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
        opened_checkpoint = RuntimeCheckpointBundle.from_states(
            checkpoint.root_session_id,
            {**checkpoint.states, checkpoint.root_session_id: opened_root},
        )

        recovered = AutoAgentApp()
        try:
            recovered.register_workflow(parent)
            loaded = recovered.load_checkpoint(opened_checkpoint)
            root_result = recovered.recover(loaded.roots[0])
            self.assertEqual(root_result.status, "completed")
            root_after_recovery = root_result.checkpoint.state(
                root_result.session_id
            )
            recovered_plan = next(
                iter(root_after_recovery.invocation.child_plans.values())
            )
            self.assertEqual(recovered_plan.units[0].phase, "accepted")

            child_waiting = recovered.child_status(handle)
            self.assertEqual(child_waiting.status, "waiting")
            completed = recovered.resume(
                child_waiting.ref,
                child_waiting.waits[0].id,
                {"value": 2},
            )
            self.assertEqual(completed.status, "completed")
            root_after_completion = completed.checkpoint.state(
                root_result.session_id
            )
            completed_plan = next(
                iter(root_after_completion.invocation.child_plans.values())
            )
            self.assertEqual(completed_plan.units[0].phase, "terminal")
        finally:
            recovered.close(timeout=1)

    def test_recover_exact_spawn_child_waits_for_its_boundary(self) -> None:
        """Verify exact Child recovery waits despite a spawn ancestor edge."""

        first_started = _ThreadSignal()
        replay_started = _ThreadSignal()
        first_gate = _RuntimeGate()
        replay_gate = _RuntimeGate()
        calls = 0

        async def child_work(value: Value) -> Value:
            nonlocal calls
            calls += 1
            if calls == 1:
                first_started.set()
                await first_gate.wait()
            else:
                replay_started.set()
                await replay_gate.wait()
            return value

        child = Workflow(
            "recover-exact-spawn-child",
            nodes=[
                Node(
                    "work",
                    child_work,
                    recovery_mode=Recovery("replay_safe"),
                )
            ],
        )
        parent = Workflow(
            "recover-exact-spawn-parent",
            nodes=[Node("spawn", child, execution_mode="spawn")],
        )
        source = AutoAgentApp()
        try:
            parent_result = source.invoke(
                parent,
                {"value": 1},
                session_id="recover-exact-spawn-root",
            )
            handle = parent_result.output
            self.assertTrue(first_started.wait(1))
            checkpoint = source.close(timeout=1).roots[0]
        finally:
            _release_runtime_gate_sync(source._runtime_loop, first_gate)
            if not source._closed:
                source.close(timeout=1)

        recovered = AutoAgentApp()
        recovery_thread: threading.Thread | None = None
        try:
            recovered.register_workflow(parent)
            recovered.load_checkpoint(checkpoint)
            result_box: list[InvocationResult | BaseException] = []
            recovery_done = _ThreadSignal()

            def recover_child() -> None:
                try:
                    result_box.append(
                        recovered.recover(
                            InvocationRef(
                                handle["session_id"], handle["invocation_id"]
                            )
                        )
                    )
                except BaseException as error:
                    result_box.append(error)
                finally:
                    recovery_done.set()

            recovery_thread = threading.Thread(target=recover_child)
            recovery_thread.start()
            self.assertTrue(replay_started.wait(1))
            self.assertFalse(recovery_done.wait(0.05))
            _release_runtime_gate_sync(recovered._runtime_loop, replay_gate)
            self.assertTrue(recovery_done.wait(1))
            recovery_thread.join(1)
            self.assertEqual(len(result_box), 1)
            if isinstance(result_box[0], BaseException):
                raise result_box[0]
            child_result = result_box[0]
            self.assertEqual(child_result.status, "completed")
            self.assertEqual(child_result.output, {"value": 1})
        finally:
            _release_runtime_gate_sync(recovered._runtime_loop, replay_gate)
            if recovery_thread is not None:
                recovery_thread.join(1)
            recovered.close(timeout=1)

    def test_stream_close_converges_every_admission_stage(self) -> None:
        """Verify early stream close never leaves an admitted Invocation orphaned."""

        for boundary in (
            "session.opened",
            "invocation.opened",
            "invocation.started",
            "scheduler.initialized",
        ):
            with self.subTest(boundary=boundary):
                journal = InMemoryEventJournal()
                app = AutoAgentApp(runtime_journal=journal)
                session_id = f"stream-close-{boundary}"
                stream = app.stream(
                    Workflow(
                        f"stream-close-workflow-{boundary}",
                        nodes=[Node("node", identity)],
                    ),
                    {"value": 1},
                    session_id=session_id,
                )
                try:
                    for item in stream:
                        if (
                            isinstance(item, InvocationUpdate)
                            and isinstance(item.event, TraceEvent)
                            and item.event.kind == boundary
                        ):
                            break
                    stream.close()
                    state = journal.state(session_id)
                    if boundary == "session.opened":
                        self.assertIsNone(state.session)
                    else:
                        self.assertIsNotNone(state.invocation)
                        self.assertEqual(state.invocation.status, "cancelled")
                    self.assertFalse(app._task_runtime.is_live(session_id))
                finally:
                    stream.close()
                    app.close(timeout=1.0)

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
                    app.aresume(waiting.ref, waiting.waits[0].id, {"value": 2})
                )
                self.assertTrue(await started.wait_async())
                resume.cancel()
                await asyncio.gather(resume, return_exceptions=True)

                state = app._journal.state(waiting.session_id)
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
            source_checkpoint = (await source.aclose(timeout=1.0)).roots[0]

            recovered = AutoAgentApp()
            try:
                recovered.register_workflow(workflow)
                loaded = await recovered.aload_checkpoint(source_checkpoint)
                recovery = asyncio.create_task(recovered.arecover(loaded.roots[0]))
                self.assertTrue(await replay_started.wait_async())
                recovery.cancel()
                await asyncio.gather(recovery, return_exceptions=True)

                state = recovered._journal.state(loaded.roots[0].session_id)
                self.assertIsNotNone(state.invocation)
                orphaned = (
                    state.invocation.status == "running"
                    and not recovered._task_runtime.is_live(loaded.roots[0].session_id)
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
                handle: ChildInvocationHandle,
            ) -> ChildInvocationHandle:
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
                handles = await app.achild_handles(submitted.ref)
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
                child_state = app._journal.state(handles[0]["session_id"])
                self.assertIsNotNone(child_state.invocation)
                self.assertEqual(child_state.invocation.status, "cancelled")
            finally:
                await _release_runtime_gate(app._runtime_loop, sink.release)
                await app.aclose(timeout=1.0)

        asyncio.run(run())

    def test_cancelled_astream_receive_has_no_terminal_sentinel_deadlock(self) -> None:
        """Verify one cancelled receive cannot enqueue two terminal sentinels."""

        async def run() -> None:
            sink = _BlockingSink()
            sink.enabled = True
            sink.target_kind = "session.opened"
            app = AutoAgentApp(runtime_event_sink=sink)
            stream = app.astream(
                Workflow("cancel-astream", nodes=[Node("node", identity)]),
                {"value": 1},
                session_id="cancel-astream-session",
            )
            first = await anext(stream)
            self.assertIsInstance(first, InvocationUpdate)
            assert isinstance(first, InvocationUpdate)
            self.assertEqual(first.event.kind, "session.opened")
            # Root SessionOpened is not persisted until InvocationOpened makes
            # admission recoverable, so block and cancel the second receive.
            consumer = asyncio.create_task(anext(stream))
            try:
                self.assertTrue(await sink.entered.wait_async())
                consumer.cancel()
                await _release_runtime_gate(app._runtime_loop, sink.release)
                done, _pending = await asyncio.wait({consumer}, timeout=0.2)
                completed_without_deadlock = consumer in done
                if not completed_without_deadlock:
                    async def unblock_terminal_queue() -> None:
                        for channel in tuple(app._attached_streams.values()):
                            while not channel._queue.empty():
                                channel._queue.get_nowait()

                    await app._runtime_loop.wait(
                        app._runtime_loop.submit(unblock_terminal_queue())
                    )
                    await asyncio.wait({consumer}, timeout=0.5)
                self.assertTrue(completed_without_deadlock)
                self.assertFalse(app._attached_streams)
                self.assertFalse(app._attached_stream_tasks)
            finally:
                await _release_runtime_gate(app._runtime_loop, sink.release)
                if not consumer.done():
                    consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
                close_stream = asyncio.create_task(stream.aclose())
                done, _pending = await asyncio.wait({close_stream}, timeout=1)
                if close_stream not in done:
                    close_stream.cancel()
                await asyncio.gather(close_stream, return_exceptions=True)
                await app.aclose(timeout=1.0)

        asyncio.run(run())

    def test_external_cancelled_astream_ends_with_cancelled_result(self) -> None:
        """Verify external cancellation still delivers the stream's final Result."""

        async def run() -> None:
            started = _ThreadSignal()

            async def block(value: Value) -> Value:
                started.set()
                await asyncio.Event().wait()
                return value

            app = AutoAgentApp()
            stream = app.astream(
                Workflow("external-cancel-stream", nodes=[Node("work", block)]),
                {"value": 1},
                session_id="external-cancel-stream-session",
            )
            try:
                ref = None
                while True:
                    item = await anext(stream)
                    if (
                        isinstance(item, InvocationUpdate)
                        and isinstance(item.event, TraceEvent)
                        and item.event.kind == "operator_call.started"
                    ):
                        ref = InvocationRef(
                            item.event.session_id, item.event.invocation_id
                        )
                        break
                cancelled_update = asyncio.create_task(anext(stream))
                self.assertTrue(await started.wait_async())
                assert ref is not None
                cancellation = asyncio.create_task(app.acancel(ref, "external"))
                update = await cancelled_update
                self.assertIsInstance(update, InvocationUpdate)
                self.assertEqual(update.event.kind, "invocation.cancelled")
                self.assertIsNotNone(update.checkpoint)

                final_pull = asyncio.create_task(anext(stream))
                cancelled, final = await asyncio.gather(cancellation, final_pull)
                self.assertEqual(cancelled.status, "cancelled")
                self.assertIsInstance(final, InvocationResult)
                self.assertEqual(final.status, "cancelled")
                self.assertEqual(
                    final.checkpoint.state(ref.session_id).invocation.status,
                    "cancelled",
                )
                with self.assertRaises(StopAsyncIteration):
                    await anext(stream)
            finally:
                await stream.aclose()
                await app.aclose(timeout=1)

        asyncio.run(run())

    def test_external_cancelled_sync_stream_ends_with_cancelled_result(self) -> None:
        """Verify sync stream cancellation has the same final Result contract."""

        started = threading.Event()

        async def block(value: Value) -> Value:
            started.set()
            await asyncio.Event().wait()
            return value

        app = AutoAgentApp()
        stream = app.stream(
            Workflow("external-cancel-sync-stream", nodes=[Node("work", block)]),
            {"value": 1},
            session_id="external-cancel-sync-stream-session",
        )
        next_values: list[object] = []
        next_errors: list[BaseException] = []
        cancel_values: list[InvocationResult] = []
        cancel_errors: list[BaseException] = []

        def pull_cancel_update() -> None:
            try:
                next_values.append(next(stream))
            except BaseException as error:
                next_errors.append(error)

        def cancel_invocation(ref: InvocationRef) -> None:
            try:
                cancel_values.append(app.cancel(ref, "external"))
            except BaseException as error:
                cancel_errors.append(error)

        update_thread: threading.Thread | None = None
        cancel_thread: threading.Thread | None = None
        try:
            ref = None
            for item in stream:
                if (
                    isinstance(item, InvocationUpdate)
                    and isinstance(item.event, TraceEvent)
                    and item.event.kind == "operator_call.started"
                ):
                    ref = InvocationRef(
                        item.event.session_id, item.event.invocation_id
                    )
                    break
            assert ref is not None
            update_thread = threading.Thread(target=pull_cancel_update)
            update_thread.start()
            self.assertTrue(started.wait(1))
            cancel_thread = threading.Thread(target=cancel_invocation, args=(ref,))
            cancel_thread.start()
            update_thread.join(1)
            self.assertFalse(update_thread.is_alive())
            self.assertEqual(next_errors, [])
            self.assertEqual(len(next_values), 1)
            update = next_values[0]
            self.assertIsInstance(update, InvocationUpdate)
            self.assertEqual(update.event.kind, "invocation.cancelled")

            final = next(stream)
            cancel_thread.join(1)
            self.assertFalse(cancel_thread.is_alive())
            self.assertEqual(cancel_errors, [])
            self.assertEqual(len(cancel_values), 1)
            self.assertEqual(cancel_values[0].status, "cancelled")
            self.assertIsInstance(final, InvocationResult)
            self.assertEqual(final.status, "cancelled")
            with self.assertRaises(StopIteration):
                next(stream)
        finally:
            stream.close()
            if update_thread is not None:
                update_thread.join(1)
            if cancel_thread is not None:
                cancel_thread.join(1)
            app.close(timeout=1)

    def test_concurrent_aclose_calls_share_one_result(self) -> None:
        """Verify concurrent asynchronous close calls are one idempotent operation."""

        async def run() -> None:
            app = _CoordinatedCloseApp()
            await app.ainvoke(
                Workflow("concurrent-aclose", nodes=[Node("node", identity)]),
                {"value": 1},
            )
            first = asyncio.create_task(app.aclose())
            second = asyncio.create_task(app.aclose())
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
                results.append(app.close())
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
            first = asyncio.create_task(app.aclose(timeout=0.02))
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
            checkpoint = await app.aclose(timeout=1.0)
            self.assertIsInstance(checkpoint, AppCheckpoint)
            self.assertTrue(app._closed)
            self.assertEqual(app.close_entries, 1)

        asyncio.run(run())

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
        checkpoint = source.close()
        self.assertEqual(len(checkpoint.roots), 1)
        self.assertEqual(len(checkpoint.roots[0].states), 2)

        journal = InMemoryEventJournal()
        journal.install_states(checkpoint.roots[0].states)
        restored = AutoAgentApp(runtime_journal=journal)
        captured = restored.close()

        self.assertEqual(len(captured.roots), 1)
        self.assertEqual(
            captured.roots[0].root_session_id,
            checkpoint.roots[0].root_session_id,
        )
        self.assertEqual(captured.roots[0].states, checkpoint.roots[0].states)

    def test_public_invocation_status_includes_child_creation(self) -> None:
        """Verify Child admission's created phase is represented by the SDK type."""

        self.assertIn("created", get_args(InvocationStatus))

    def test_app_checkpoint_rejects_cross_root_planned_child_claim(self) -> None:
        """Verify one planned Child cannot also be another checkpoint Root."""

        _child, _parent, planned, child_bundle, _child_session_id = (
            _cross_root_checkpoint_pair()
        )
        with self.assertRaisesRegex(ValueError, "independent Checkpoint Root"):
            AppCheckpoint((planned, child_bundle))

    def test_checkpoint_load_cannot_claim_an_existing_root_as_child(self) -> None:
        """Verify a planned Child claim cannot hijack an installed independent Root."""

        _child, parent, planned, child_bundle, child_session_id = (
            _cross_root_checkpoint_pair()
        )
        target = AutoAgentApp()
        try:
            target.register_workflow(parent)
            loaded_child = target.load_checkpoint(child_bundle)
            child_ref = loaded_child.roots[0]
            state_before = target._journal.state(child_session_id)

            with self.assertRaisesRegex(
                RuntimeTransitionError,
                "CHECKPOINT_GRAPH_CONFLICT",
            ):
                target.load_checkpoint(planned)

            self.assertEqual(target._journal.session_ids(), (child_session_id,))
            self.assertEqual(target._journal.state(child_session_id), state_before)
            self.assertEqual(target._root_session_id(child_session_id), child_session_id)
            self.assertIsNotNone(state_before.invocation)
            self.assertEqual(state_before.invocation.id, child_ref.invocation_id)
        finally:
            target.close()

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
                        log.event_name == "invocation.opened" for log in event.logs
                    )
                ):
                    raise RuntimeError("sink rejected replacement")

        journal = InMemoryEventJournal()
        sink = RejectReplacementSink()
        child = Workflow(
            "replacement-sink-child",
            nodes=[Node("work", identity)],
        )
        parent = Workflow(
            "replacement-sink-parent",
            nodes=[Node("spawn", child, execution_mode="spawn")],
        )
        app = AutoAgentApp(runtime_journal=journal, runtime_event_sink=sink)
        try:
            first = app.invoke(
                parent,
                {"value": 1},
                session_id="replacement-sink-root",
            )
            handle = app.child_handles(first.ref)[0]
            self.assertEqual(app.wait_child(handle, timeout=1).status, "completed")

            sink.enabled = True
            with self.assertRaises(RuntimeInfrastructureError):
                app.invoke(
                    parent,
                    {"value": 2},
                    session_id="replacement-sink-root",
                )
            sink.enabled = False

            self.assertEqual(journal.session_ids(), ("replacement-sink-root",))
            current = journal.state("replacement-sink-root").invocation
            self.assertIsNotNone(current)
            self.assertEqual(current.status, "created")
            self.assertTrue(
                any(
                    log.event_name == "invocation.opened"
                    for event in journal.events("replacement-sink-root")
                    for log in event.logs
                )
            )

            closed = app.close(timeout=1)
            self.assertEqual(len(closed.roots), 1)
            self.assertEqual(closed.roots[0].root_session_id, "replacement-sink-root")
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
                        for log in event.logs
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

        journal = InMemoryEventJournal()
        sink = RejectChildTerminalSink()
        child = Workflow(
            "child-event-retirement-child",
            nodes=[Node("work", child_work)],
        )
        parent = Workflow(
            "child-event-retirement-parent",
            nodes=[Node("spawn", child, execution_mode="spawn")],
        )
        app = AutoAgentApp(runtime_journal=journal, runtime_event_sink=sink)
        try:
            first = app.invoke(
                parent,
                {"value": 1},
                session_id="child-event-retirement-root",
            )
            self.assertEqual(first.status, "completed")
            self.assertTrue(child_started.wait(1))
            handle = app.child_handles(first.ref)[0]
            _release_runtime_gate_sync(app._runtime_loop, release_child)
            self.assertTrue(sink.rejected.wait(1))

            with self.assertRaises(RuntimeInfrastructureError):
                app.invoke(
                    parent,
                    {"value": 2},
                    session_id="child-event-retirement-root",
                )

            self.assertEqual(
                journal.state("child-event-retirement-root").invocation.id,
                first.invocation_id,
            )
            self.assertIn(handle["session_id"], journal.session_ids())
            self.assertTrue(journal.events(handle["session_id"]))
            self.assertNotIn(sink.rejected_event_id, sink.accepted_event_ids)

            sink.enabled = False
            second = app.invoke(
                parent,
                {"value": 2},
                session_id="child-event-retirement-root",
            )
            self.assertEqual(second.status, "completed")
            self.assertIn(sink.rejected_event_id, sink.accepted_event_ids)
            self.assertNotIn(handle["session_id"], journal.session_ids())

            closed = app.close(timeout=1)
            self.assertEqual(len(closed.roots), 1)
            self.assertEqual(
                closed.roots[0].root_session_id,
                "child-event-retirement-root",
            )
        finally:
            sink.enabled = False
            _release_runtime_gate_sync(app._runtime_loop, release_child)
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

        journal = InMemoryEventJournal(
            max_batches_per_event=100,
            flush_event_names=frozenset(),
        )
        self.assertTrue(
            {
                "operator_call.started",
                "child_invocation.planned",
                "wait.resumed",
                "invocation.recovery_requested",
            }
            <= journal._flush_event_names
        )

    def test_checkpoint_bundle_isolated_from_external_mutable_state(self) -> None:
        """Verify a checkpoint and loaded State cannot change through an old object."""

        workflow = Workflow("immutable-checkpoint", nodes=[Node("node", identity)])
        source = AutoAgentApp()
        try:
            checkpoint = source.invoke(
                workflow,
                {"value": 1},
                session_id="immutable-checkpoint-session",
            ).checkpoint
        finally:
            source.close()

        state = checkpoint.state("immutable-checkpoint-session")
        self.assertIsNotNone(state.invocation)
        mutable_context: dict[str, object] = {}
        external_state = replace(
            state,
            invocation=replace(state.invocation, context=mutable_context),
        )
        try:
            external_bundle = RuntimeCheckpointBundle.from_states(
                "immutable-checkpoint-session",
                {"immutable-checkpoint-session": external_state},
            )
        except (TypeError, ValueError):
            return

        original_record = external_bundle.to_record()
        mutable_context["changed"] = 1
        self.assertEqual(external_bundle.to_record(), original_record)

        target = AutoAgentApp()
        try:
            target.register_workflow(workflow)
            target.load_checkpoint(external_bundle)
            mutable_context["changed"] = 2
            installed = target._journal.state("immutable-checkpoint-session")
            self.assertNotIn("changed", installed.invocation.context)
        finally:
            target.close()


if __name__ == "__main__":
    unittest.main()
