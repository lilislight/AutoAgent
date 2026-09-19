"""Session commit ordering and graph lifecycle cuts under controlled Sink ACKs."""
import asyncio
import unittest

from autoagent import AutoAgentApp, Edge, Map, Node, Wait, Workflow
from autoagent.core import StateReducer
from autoagent.core.app._graph_gate import GraphGate
from autoagent.core.errors import RuntimeInfrastructureError, RuntimeTransitionError
from autoagent.core.runtime import (
    ChildInvocationPhaseChanged, InvocationCancelled, InvocationStarted, SessionOpened, WaitResumed,
)
from tests.benchmarks.benchmark_core_audit import Value, identity, items
from tests.test_core_execution_performance import assert_index


class BlockingSink:
    def __init__(self):
        self.events = []
        self.predicate = lambda event: False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.active = set()
        self.peak = 0

    async def append(self, event):
        assert event.session_id not in self.active, 'same Session submitted concurrently'
        self.active.add(event.session_id)
        self.peak = max(self.peak, len(self.active))
        try:
            if self.predicate(event):
                self.entered.set()
                await self.release.wait()
            self.events.append(event)
        finally:
            self.active.remove(event.session_id)


class GraphGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_writer_waits_for_readers_and_blocks_new_readers(self):
        """Exclusive cuts drain current readers, prefer writers, and allow owner nesting."""
        gate = GraphGate()
        entered = asyncio.Event()
        release = asyncio.Event()
        order = []
        async def writer():
            entered.set()
            async with gate:
                order.append('writer')
                async with gate, gate.shared():
                    await release.wait()
        async def reader():
            async with gate.shared():
                order.append('reader')
        async with gate.shared():
            task = asyncio.create_task(writer())
            await entered.wait()
            await asyncio.sleep(0)
            sibling = asyncio.create_task(reader())
            # A reader already admitted may finish its nested transition.
            async with gate.shared():
                await asyncio.sleep(0)
            self.assertEqual(order, [])
        await asyncio.sleep(0)
        self.assertEqual(order, ['writer'])
        release.set()
        await asyncio.wait_for(asyncio.gather(task, sibling), 1)
        self.assertEqual(order, ['writer', 'reader'])
        self.assertTrue(gate.idle)

    async def test_cancelled_waiters_release_barrier_and_retirement(self):
        """Cancelled reader/writer waiters cannot strand a gate or retire it early."""
        gate = GraphGate()
        retired = []
        async with gate.shared():
            writer = asyncio.create_task(gate.__aenter__())
            await asyncio.sleep(0)
            async def read():
                async with gate.shared():
                    pass
            reader = asyncio.create_task(read())
            await asyncio.sleep(0)
            gate.retire(lambda: retired.append(True))
            writer.cancel()
            reader.cancel()
            await asyncio.gather(writer, reader, return_exceptions=True)
            self.assertEqual(retired, [])
            with self.assertRaises(RuntimeError):
                async with gate:
                    pass
        self.assertEqual(retired, [True])
        async with gate:
            pass
        self.assertTrue(gate.idle)


class CoreGraphConcurrencyTests(unittest.TestCase):
    def make_wait_graph(self, sink, count=4):
        app = AutoAgentApp(runtime_event_sink=sink)
        child = Workflow('graph-child', nodes=[Node('wait', Wait(Value, Value))])
        parent = Workflow('graph-root', nodes=[Node('children', child,
            input_mapping=items, map=Map(max_parallelism=count))])
        result = app.invoke(parent, {'value': count}, session_id='root')
        children = [app.join(ref) for ref in app.child_invocations(result.ref)]
        return app, result, children

    def assert_replay(self, app, sink):
        for sid in app._repository.session_ids():
            events = [event for event in sink.events if event.session_id == sid]
            self.assertEqual([e.sequence for e in events], list(range(1, len(events) + 1)))
            self.assertEqual(StateReducer().reduce(tuple(events)), app._repository.state(sid))
            assert_index(app._repository, sid)

    def test_sibling_ack_overlap_and_parent_terminal_causality(self):
        """A blocked Child ACK allows siblings to complete and preserves parent causal order."""
        sink = BlockingSink()
        app, root, children = self.make_wait_graph(sink)
        async def run():
            blocked = children[0]
            sink.predicate = lambda e: e.session_id == blocked.ref.session_id and isinstance(e.payload, WaitResumed)
            first = asyncio.create_task(app._resume(blocked.ref, blocked.waits[0].id,
                {'value': 0}, wait_for_boundary=True))
            await sink.entered.wait()
            others = await asyncio.gather(*(app._resume(c.ref, c.waits[0].id,
                {'value': i}, wait_for_boundary=True) for i, c in enumerate(children[1:])))
            self.assertTrue(all(r.status == 'completed' for r in others))
            self.assertFalse(first.done())
            # A sibling result read also must not queue behind the blocked Child.
            self.assertEqual((await app._result(children[1].ref)).status, 'completed')
            checkpoint = asyncio.create_task(app._capture_checkpoint('root'))
            await asyncio.sleep(0)
            self.assertFalse(checkpoint.done())
            sink.release.set()
            await first
            await checkpoint
        try:
            app._runtime_loop.run(asyncio.wait_for(run(), 5))
            self.assertEqual(app.join(root.ref).status, 'completed')
            self.assertGreater(sink.peak, 1)
            markers = [e for e in sink.events if isinstance(e.payload, ChildInvocationPhaseChanged)
                       and e.payload.phase == 'terminal']
            self.assertEqual(len(markers), len(children))
            plan = next(iter(app._repository.state('root').invocation.child_plans.values()))
            for marker in markers:
                sid = plan.units[marker.payload.unit_index].session_id
                terminal = next(e for e in sink.events if e.session_id == sid and e.event_name == 'invocation.completed')
                self.assertLess(sink.events.index(terminal), sink.events.index(marker))
            self.assert_replay(app, sink)
        finally:
            sink.predicate = lambda e: False
            app._runtime_loop.run(self.release(sink))
            app.close(timeout=2)

    @staticmethod
    async def release(sink):
        sink.release.set()

    def test_concurrent_same_session_resumes_use_latest_scheduler(self):
        """Concurrent Wait completions in one Session preserve all Scheduler branches."""
        sink = BlockingSink()
        app = AutoAgentApp(runtime_event_sink=sink)
        workflow = Workflow('parallel-waits', nodes=[Node('start', identity),
            Node('a', Wait(Value, Value)), Node('b', Wait(Value, Value)),
            Node('a_end', identity), Node('b_end', identity)],
            edges=[Edge('start', 'a'), Edge('start', 'b'), Edge('a', 'a_end'), Edge('b', 'b_end')])
        async def run(result):
            await asyncio.gather(*(app._resume(result.ref, wait.id, {'value': i},
                wait_for_boundary=True) for i, wait in enumerate(result.waits)))
        try:
            result = app.invoke(workflow, {'value': 1})
            app._runtime_loop.run(asyncio.wait_for(run(result), 5))
            self.assertEqual(app.join(result.ref).status, 'completed')
            self.assert_replay(app, sink)
        finally:
            app.close(timeout=2)

    def test_cancel_during_child_admission_leaves_no_live_descendant(self):
        """Cancellation drains opening/acceptance ACKs before snapshotting and stopping the graph."""
        for boundary in ('session', 'invocation', 'accepted'):
            with self.subTest(boundary=boundary):
                sink = BlockingSink()
                app = AutoAgentApp(runtime_event_sink=sink)
                child = Workflow('admission-child', nodes=[Node('wait', Wait(Value, Value))])
                workflow = Workflow('admission-root', nodes=[Node('child', child)])
                def blocked(e):
                    return ((boundary == 'session' and e.session_id != 'root' and isinstance(e.payload, SessionOpened))
                        or (boundary == 'invocation' and e.session_id != 'root' and isinstance(e.payload, InvocationStarted))
                        or (boundary == 'accepted' and isinstance(e.payload, ChildInvocationPhaseChanged) and e.payload.phase == 'accepted'))
                sink.predicate = blocked
                async def run():
                    invocation = asyncio.create_task(app._invoke(workflow, {'value': 1},
                        session_id='root', session_context=None, entry_node_id=None, wait_for_boundary=True))
                    await sink.entered.wait()
                    state = app._repository.state('root').invocation
                    ref = app._ref_for_invocation('root', state)
                    cancel = asyncio.create_task(app._cancel_graph(ref, 'test'))
                    await asyncio.sleep(0)
                    self.assertFalse(cancel.done())
                    sink.release.set()
                    await cancel
                    self.assertEqual((await invocation).status, 'cancelled')
                    for sid in app._repository.session_ids():
                        state = app._repository.state(sid).invocation
                        self.assertTrue(state.terminal)
                        self.assertFalse(app._task_runtime.is_live(sid))
                try:
                    app._runtime_loop.run(asyncio.wait_for(run(), 5))
                    self.assert_replay(app, sink)
                finally:
                    sink.predicate = lambda e: False
                    app._runtime_loop.run(self.release(sink))
                    app.close(timeout=2)

    def test_load_unload_and_recovery_respect_inflight_child_commit(self):
        """Control operations cannot replace or discard a graph during a Child ACK."""
        sink = BlockingSink()
        app, root, children = self.make_wait_graph(sink, count=1)
        child = children[0]
        checkpoint = app._runtime_loop.run(app._capture_checkpoint(child.ref.session_id))
        async def run():
            sink.predicate = lambda e: isinstance(e.payload, WaitResumed)
            commit = asyncio.create_task(app._emit(child.ref.session_id, child.ref.invocation_id,
                WaitResumed(child.waits[0].id, {'value': 1})))
            await sink.entered.wait()
            with self.assertRaisesRegex(RuntimeTransitionError, 'CHECKPOINT_SESSION_LIVE'):
                await app._load_checkpoint(checkpoint)
            with self.assertRaisesRegex(RuntimeTransitionError, 'INVOCATION_STILL_LIVE'):
                await app._recover(root.ref)
            unload = asyncio.create_task(app._unload_session(root.ref))
            await asyncio.sleep(0)
            self.assertFalse(unload.done())
            sink.release.set()
            await commit
            with self.assertRaisesRegex(RuntimeTransitionError, 'RELATED_INVOCATION_NOT_UNLOADABLE'):
                await unload
            result = await app._recover(root.ref)
            self.assertEqual(result.status, 'completed')
        try:
            app._runtime_loop.run(asyncio.wait_for(run(), 5))
            self.assert_replay(app, sink)
        finally:
            sink.predicate = lambda e: False
            app._runtime_loop.run(self.release(sink))
            app.close(timeout=2)

    def test_recovery_reservation_rejects_resume_but_allows_cancellation(self):
        """A graph recovery admits one coordinator while cancellation can still converge it."""
        sink = BlockingSink()
        app, root, children = self.make_wait_graph(sink, count=1)
        original = app._recover_session
        async def run():
            entered, release = asyncio.Event(), asyncio.Event()
            async def paused(*args, **kwargs):
                entered.set()
                await release.wait()
                await original(*args, **kwargs)
            app._recover_session = paused
            first = asyncio.create_task(app._recover(root.ref))
            await entered.wait()
            try:
                with self.assertRaisesRegex(RuntimeTransitionError, 'INVOCATION_STILL_LIVE'):
                    await app._recover(root.ref)
                child = children[0]
                with self.assertRaisesRegex(RuntimeTransitionError, 'INVOCATION_STILL_LIVE'):
                    await app._resume(child.ref, child.waits[0].id, {'value': 1}, wait_for_boundary=True)
                with self.assertRaisesRegex(RuntimeTransitionError, 'INVOCATION_RESULT_PENDING'):
                    await app._unload_session(root.ref)
                await app._cancel_graph(root.ref, 'stop recovery')
            finally:
                release.set()
            self.assertEqual((await first).status, 'cancelled')
            self.assertFalse(app._recovering)
        try:
            app._runtime_loop.run(asyncio.wait_for(run(), 5))
            self.assert_replay(app, sink)
        finally:
            app._recover_session = original
            app.close(timeout=2)

    def test_recovered_wait_can_resume_while_other_recovery_work_remains(self):
        """Recovery reservation does not block an already admitted external Wait."""
        sink = BlockingSink()
        app, root, children = self.make_wait_graph(sink, count=1)
        child = children[0]
        original = app._recover_session
        async def run():
            entered, release = asyncio.Event(), asyncio.Event()
            async def paused(sid, *args, **kwargs):
                await original(sid, *args, **kwargs)
                if sid == child.ref.session_id:
                    entered.set()
                    await release.wait()
            app._recover_session = paused
            recovery = asyncio.create_task(app._recover(root.ref))
            await entered.wait()
            try:
                result = await app._resume(child.ref, child.waits[0].id,
                    {'value': 1}, wait_for_boundary=True)
                self.assertEqual(result.status, 'completed')
            finally:
                release.set()
            self.assertEqual((await recovery).status, 'completed')
        try:
            app._runtime_loop.run(asyncio.wait_for(run(), 5))
            self.assert_replay(app, sink)
        finally:
            app._recover_session = original
            app.close(timeout=2)

    def test_duplicate_child_settlement_coalesces_parent_markers(self):
        """Concurrent finalizers submit one terminal phase and one await-ready event."""
        sink = BlockingSink()
        app, root, children = self.make_wait_graph(sink, count=1)
        child = children[0]
        async def run():
            await app._emit(child.ref.session_id, child.ref.invocation_id, InvocationCancelled('test'))
            sink.predicate = lambda e: isinstance(e.payload, ChildInvocationPhaseChanged) and e.payload.phase == 'terminal'
            tasks = [asyncio.create_task(app._settle_child(child.ref.session_id, child.ref.invocation_id))
                     for _ in range(20)]
            await sink.entered.wait()
            await asyncio.sleep(0)
            sink.release.set()
            await asyncio.gather(*tasks)
        try:
            app._runtime_loop.run(asyncio.wait_for(run(), 5))
            self.assertEqual(app.join(root.ref).status, 'failed')
            markers = [e for e in sink.events if isinstance(e.payload, ChildInvocationPhaseChanged)
                       and e.payload.phase == 'terminal']
            self.assertEqual(len(markers), 1)
            ready = [e for e in sink.events if e.payload.kind == 'child_await.ready']
            self.assertEqual(len(ready), 1)
            self.assert_replay(app, sink)
        finally:
            sink.predicate = lambda e: False
            app._runtime_loop.run(self.release(sink))
            app.close(timeout=2)

    def test_parallel_child_failures_converge_without_task_cycles(self):
        """Simultaneous failing Children cancel siblings without finalizer deadlocks."""
        class YieldingSink(BlockingSink):
            async def append(self, event):
                await asyncio.sleep(0)
                await super().append(event)
        async def fail(value: Value) -> Value:
            raise ValueError('child failed')
        for attempt in range(10):
            with self.subTest(attempt=attempt):
                sink = YieldingSink()
                app = AutoAgentApp(runtime_event_sink=sink)
                child = Workflow('failing-child', nodes=[Node('wait', Wait(Value, Value)), Node('fail', fail)],
                                 edges=[Edge('wait', 'fail')])
                parent = Workflow('failing-parent', nodes=[Node('children', child,
                    input_mapping=items, map=Map(max_parallelism=4))])
                async def run(children):
                    return await asyncio.gather(*(app._resume(c.ref, c.waits[0].id,
                        {'value': 1}, wait_for_boundary=True) for c in children))
                try:
                    root = app.invoke(parent, {'value': 4})
                    children = [app.join(ref) for ref in app.child_invocations(root.ref)]
                    app._runtime_loop.run(asyncio.wait_for(run(children), 5))
                    self.assertEqual(app.join(root.ref).status, 'failed')
                    self.assertTrue(all(not app._task_runtime.is_live(sid)
                                        for sid in app._repository.session_ids()))
                    self.assert_replay(app, sink)
                finally:
                    app.close(timeout=2)

    def test_retry_settles_exact_event_before_replanning(self):
        """An ambiguous ACK retries the same object and does not plan a duplicate completion."""
        class Sink:
            failed = None
            calls = 0
            async def append(self, event):
                if event.payload.kind == 'node_occurrence.completed':
                    self.calls += 1
                    if self.failed is None:
                        self.failed = event
                        raise OSError('lost ACK')
                    self.assert_same = event is self.failed
        sink = Sink()
        app = AutoAgentApp(runtime_event_sink=sink)
        try:
            with self.assertRaises(RuntimeInfrastructureError):
                app.invoke(Workflow('retry', nodes=[Node('work', identity)]), {'value': 1}, session_id='retry')
            event = sink.failed
            result = app._runtime_loop.run(app._emit(event.session_id, event.invocation_id, event.payload))
            self.assertIs(result, event)
            self.assertTrue(sink.assert_same)
            self.assertEqual(sink.calls, 2)
            self.assertEqual(app._repository.state('retry').sequence, event.sequence)
            assert_index(app._repository, 'retry')
        finally:
            app.close(timeout=2)
