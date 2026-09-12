"""Conflict, preview and ACK boundaries for Context/Child derived lookups."""
import asyncio
import random
import unittest
from types import MappingProxyType
from unittest.mock import patch

from autoagent import AutoAgentApp, ContextOperation, Node, Workflow
from autoagent.core.errors import RuntimeInfrastructureError, RuntimeTransitionError
from autoagent.core.runtime import RuntimeRepository
from autoagent.core.runtime._context_index import (
    ContextRevisionIndex, context_previews, planning_indexes, _previews)
from autoagent.core.runtime.transitions import _apply_context_operations, _paths_overlap
from tests.benchmarks.benchmark_core_context_child import (
    bind_hot, child_fixture, finish_children, verify)
from tests.benchmarks.benchmark_core_audit import identity
from tests.test_core_execution_performance import assert_index


class CheckingRepository(RuntimeRepository):
    async def _settle_pending(self, sid):
        await super()._settle_pending(sid)
        if sid not in self._states:
            return
        state = self.state(sid)
        assert_index(self, sid)
        for name, index in self._context_indexes.get(sid, {}).items():
            self_owner = getattr(state, name)
            assert index.revisions is self_owner.context_path_revisions
            assert index.descendants == ContextRevisionIndex(index.revisions).descendants
        expected = {key for key, value in self._states.items()
                    if value.invocation and value.invocation.status in {'failed', 'cancelled'}}
        assert self._failed_sessions == expected


class ContextIndexTests(unittest.TestCase):
    def test_random_queries_and_updates_match_full_overlap_scan(self):
        """Ancestor and descendant queries preserve the original conflict predicate."""
        rng = random.Random(12)
        paths = [tuple(rng.choice('abcd') for _ in range(rng.randrange(1, 6))) for _ in range(400)]
        revisions = {path: rng.randrange(50) for path in paths}
        index = ContextRevisionIndex(revisions)
        for sequence in range(50, 150):
            for path in rng.sample(paths, 20):
                started = rng.randrange(sequence + 1)
                expected = any(rev > started and _paths_overlap(path, key)
                               for key, rev in revisions.items())
                self.assertEqual(index.conflicts(path, started), expected)
            target = rng.choice(paths)
            revisions = {**revisions, target: sequence}
            index = index.advance(revisions, (ContextOperation.set(target, 1),))
            self.assertEqual(index.descendants, ContextRevisionIndex(revisions).descendants)

    def test_indexed_patch_matches_scan_success_and_atomic_failures(self):
        """Indexed planning preserves duplicate, overlap and operation error ordering."""
        from autoagent.core.runtime.values import freeze
        rng = random.Random(33)
        paths = [('a',), ('a', 'x'), ('a', 'y'), ('b',), ('c', 'z')]
        for _ in range(300):
            context = freeze({'a': {'x': 1}, 'b': 2})
            revisions = MappingProxyType({p: rng.randrange(10) for p in paths})
            ops = tuple(ContextOperation.set(rng.choice(paths), {'x': 2})
                        if rng.random() < .7 else ContextOperation.delete(rng.choice(paths))
                        for _ in range(rng.randrange(1, 5)))
            started = rng.randrange(12)
            def run(indexed):
                try:
                    with planning_indexes((ContextRevisionIndex(revisions),) if indexed else ()):
                        return ('ok', _apply_context_operations(context, revisions, ops, started, 12))
                except RuntimeTransitionError as error:
                    return ('error', str(error))
            self.assertEqual(run(False), run(True))
            self.assertEqual(context, {'a': {'x': 1}, 'b': 2})

    def test_preview_reuses_context_but_retimes_revisions_and_invalidates(self):
        """Unchanged inputs reuse values; changed revisions still detect conflicts."""
        from autoagent.core.runtime.values import freeze
        context = freeze({'a': 1})
        revisions = MappingProxyType({('a',): 1})
        operations = (ContextOperation.set('a', 2),)
        with context_previews():
            preview, _ = _apply_context_operations(context, revisions, operations, 2, 3)
            with patch('autoagent.core.context._ContextEdit', side_effect=AssertionError('recomputed')):
                actual, updated = _apply_context_operations(context, revisions, operations, 2, 8)
            self.assertIs(actual, preview)
            self.assertEqual(updated[('a',)], 8)
            with self.assertRaises(RuntimeTransitionError):
                _apply_context_operations(context, {('a',): 5}, operations, 2, 8)
            changed, _ = _apply_context_operations(freeze({'a': 1, 'b': 9}), revisions, operations, 2, 8)
            self.assertEqual(changed, {'a': 2, 'b': 9})
            cache = _previews.get()
        self.assertTrue(cache.closed)
        self.assertEqual(cache, [])

    def test_live_preview_reuses_owned_patch(self):
        """The normal OutputBound/NodeCompleted path computes a nonempty patch once."""
        from autoagent.core.context import _ContextEdit
        app = AutoAgentApp(runtime_repository=CheckingRepository())
        try:
            with patch('autoagent.core.context._ContextEdit', wraps=_ContextEdit) as edit:
                result = app.invoke(Workflow('preview-reuse', nodes=[Node('a', identity, output_binding=bind_hot)]), {'value': 1})
            self.assertEqual(result.status, 'completed', result.error)
            self.assertEqual(edit.call_count, 1)
        finally:
            app.close()

    def test_context_index_does_not_advance_before_lost_ack_is_retried(self):
        """Failed append keeps old queries and retries the exact Event before publishing."""
        class Sink:
            failed = None
            async def append(self, event):
                if event.payload.kind == 'node_occurrence.completed':
                    if self.failed is None:
                        self.failed = event
                        raise OSError('lost ACK')
                    assert event is self.failed
        sink = Sink()
        repository = CheckingRepository(sink=sink)
        app = AutoAgentApp(runtime_repository=repository)
        try:
            with self.assertRaises(RuntimeInfrastructureError):
                app.invoke(Workflow('ack-context', nodes=[Node('a', identity, output_binding=bind_hot)]),
                           {'value': 1}, session_id='ack-context')
            before = repository.state('ack-context')
            self.assertNotIn('hot', before.session.context)
            index = repository._context_indexes['ack-context']['session']
            self.assertFalse(index.conflicts(('hot',), 0))
            app._runtime_loop.run(repository.settle('ack-context'))
            self.assertTrue(index.conflicts(('hot',), 0))
            self.assertNotIn('hot', before.session.context)
        finally:
            app.close()

    def test_waiting_child_counts_and_replay(self):
        """Progressive resume preserves output, terminal units and replayable records."""
        with patch('tests.benchmarks.benchmark_core_context_child.AutoAgentApp',
                   side_effect=lambda **kwargs: AutoAgentApp(runtime_repository=CheckingRepository(), **kwargs)):
            app, parent, children = child_fixture(20)
        try:
            finish_children(app, parent, children)
            assert_index(app._repository, parent.session_id)
        finally:
            app.close()
        report = verify()
        self.assertEqual(report['child_replay']['sessions'], 33)

    def test_cancelled_children_rebuild_failure_and_remaining_indexes(self):
        """Cancellation converges all units; installation and discard rebuild/drop indexes."""
        with patch('tests.benchmarks.benchmark_core_context_child.AutoAgentApp',
                   side_effect=lambda **kwargs: AutoAgentApp(runtime_repository=CheckingRepository(), **kwargs)):
            app, parent, children = child_fixture(12)
        try:
            self.assertEqual(app.cancel(children[0].ref).status, 'cancelled')
            self.assertEqual(app.join(parent.ref).status, 'failed')
            repository = app._repository
            saved = {sid: repository.state(sid) for sid in repository.session_ids()}
            restored = CheckingRepository()
            restored.install_states(saved)
            self.assertEqual(restored._failed_sessions, repository._failed_sessions)
            for sid in saved:
                assert_index(restored, sid)
            plan = next(iter(restored.state(parent.session_id).invocation.child_plans.values()))
            self.assertTrue(restored.has_failed_child(plan))
            self.assertEqual(restored.execution_index(parent.session_id).child_remaining[plan.creation_id], 0)
            restored.discard_states(tuple(saved))
            self.assertEqual(restored._failed_sessions, set())
            self.assertEqual(restored._execution_indexes, {})
            self.assertEqual(restored._context_indexes, {})
        finally:
            app.close()


class PreviewLifetimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_inherited_cache_is_closed_when_owner_is_cancelled(self):
        """A task inheriting a ContextVar cannot retain active cancelled previews."""
        entered = asyncio.Event()
        release = asyncio.Event()
        async def child():
            await release.wait()
            cache = _previews.get()
            self.assertTrue(cache.closed)
            self.assertEqual(cache, [])
            _apply_context_operations({}, {}, (ContextOperation.set('a', 1),), 0, 1)
            self.assertEqual(cache, [])
        async def owner():
            with context_previews():
                cache = _previews.get()
                cache.append(object())
                entered.child = asyncio.create_task(child())
                entered.set()
                await asyncio.Event().wait()
        task = asyncio.create_task(owner())
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        release.set()
        await entered.child
