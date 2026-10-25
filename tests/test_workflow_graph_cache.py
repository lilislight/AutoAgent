"""Retention and LRU boundaries for detached Workflow routing metadata."""
import gc
import unittest
import weakref

from autoagent.host.persistence.graph_metadata import WorkflowGraphCache, outgoing_edges


class WorkflowGraphCacheTests(unittest.TestCase):
    def test_metadata_drops_definition_and_condition_bodies(self):
        """Keep routing fields without retaining full schemas or condition implementations."""
        class Body(str):
            pass
        body = Body('x' * 100_000)
        reference = weakref.ref(body)
        snapshot = {'definition': {'nodes': [body], 'edges': [
            dict(id='edge', source='a', target='b', on='complete', condition=body)]}}
        cache = WorkflowGraphCache(10_000)
        cache.put('revision', outgoing_edges(snapshot))
        del snapshot, body
        gc.collect()
        self.assertIsNone(reference())
        self.assertTrue(cache.get('revision')['a'][0].conditional)

    def test_lru_budget_oversize_and_clear(self):
        """Evict least recently used revisions and reject oversized entries without retention."""
        graph = outgoing_edges({'definition': {'edges': [
            dict(id='edge', source='a', target='b', on='error', condition=None)]}})
        cache = WorkflowGraphCache(100_000)
        cache.put('r1', graph)
        size = cache.bytes
        cache.budget = size * 2
        cache.put('r2', graph)
        self.assertIs(cache.get('r1'), graph)
        cache.put('r3', graph)
        self.assertEqual(set(cache.entries), {'r1', 'r3'})
        self.assertLessEqual(cache.bytes, cache.budget)
        cache.clear()
        cache.budget = 1
        cache.put('oversized', graph)
        self.assertEqual((cache.bytes, len(cache.entries)), (0, 0))
        self.assertIsNone(cache.get('oversized'))
