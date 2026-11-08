"""Writer metadata retention and transaction publication boundaries."""

import unittest

from autoagent.host.models import HostSettings
from autoagent.host.persistence.metadata import MetadataCache


class WriterMetadataTests(unittest.TestCase):
    def test_queue_and_host_measurement_share_exact_object_ownership(self):
        """Shared containers, backing dictionaries and cycles receive one byte charge."""
        from types import MappingProxyType
        from autoagent.core.app._residency import retained_size
        from autoagent.core.runtime import RuntimeEvent
        from autoagent.core.runtime.events import SessionOpened
        from autoagent.host.persistence.codec import retained_objects

        shared = {'values': list(range(100))}
        cycle = []
        cycle.append(cycle)
        values = [RuntimeEvent('memory', 1, SessionOpened({'shared': shared})),
                  MappingProxyType(shared), shared, cycle]
        objects = retained_objects(values)
        self.assertEqual(sum(size for _, size in objects.values()), retained_size((values,)))
        self.assertIn(id(shared), objects)
        self.assertIn(id(cycle), objects)
    def test_projection_metadata_retains_control_flags_without_business_values(self):
        """State changes expose availability and error summaries without retaining bodies."""
        from autoagent.core.runtime.events import RuntimeErrorInfo
        from autoagent.host.persistence.projections import light_record

        operations = [
            dict(op='replace', path=['invocation', 'input'], value={'secret': 'prompt'}),
            dict(op='replace', path=['invocation', 'pending_recovery'], value={'secret': 'plan'}),
            dict(op='replace', path=['invocation', 'error'], value=RuntimeErrorInfo(
                'Failure', 'private message', code='TEST', phase='operator')),
            dict(op='remove', path=['invocation', 'output']),
            dict(op='replace', path=['session', 'signals'], value={'secret': 'signal body'}),
            dict(op='replace', path=['invocation', 'resume_receipts'], value={'secret': 'response body'}),
        ]
        for availability in (False, True):
            with self.subTest(availability=availability):
                result = light_record({'operations': operations}, availability=availability)['operations']
                self.assertIs(result[0]['value'], True if availability else None)
                self.assertIs(result[1]['value'], True)
                self.assertEqual(result[2]['value'], dict(type='Failure', code='TEST', phase='operator'))
                self.assertNotIn('value', result[3])
                self.assertIs(result[4]['value'], True if availability else None)
                self.assertIs(result[5]['value'], True if availability else None)
                self.assertNotIn('secret', repr(result))
                self.assertNotIn('private message', repr(result))

    def test_cache_detaches_values_and_enforces_budget(self):
        """Isolate nested metadata and evict old keys within the byte budget."""
        cache = MetadataCache(100_000)
        row = {"input_ref": {"path": ["payload", "input"]}}
        cache.put(("invocations", ("a",)), row)
        size = cache.bytes
        cache.budget = size * 2
        row["input_ref"]["path"].append("changed")
        loaded = cache.get(("invocations", ("a",)))
        self.assertEqual(loaded["input_ref"]["path"], ["payload", "input"])
        loaded["input_ref"]["path"].clear()
        self.assertTrue(cache.get(("invocations", ("a",)))["input_ref"]["path"])
        for key in range(100):
            cache.put(("sessions", (str(key),)), {"durable_sequence": key})
            self.assertLessEqual(cache.bytes, cache.budget)
        self.assertGreater(cache.evictions, 0)
        cache.clear()
        self.assertEqual((cache.bytes, len(cache.entries)), (0, 0))

    def test_cache_does_not_remember_missing_or_oversized_values(self):
        """Missing rows and oversized entries cannot retain history or mask inserts."""
        cache = MetadataCache(1)
        cache.put("missing", None)
        cache.put("oversized", {"id": "session"})
        self.assertEqual(cache.entries, {})
        self.assertIsNone(cache.get("missing"))

    def test_host_environment_configures_metadata_budget(self):
        """Parse and validate the deployment budget through Host settings."""
        settings = HostSettings.from_environment({"AUTOAGENT_WRITER_METADATA_CACHE_BYTES": "1024"})
        self.assertEqual(settings.persistence.writer_metadata_cache_bytes, 1024)
        self.assertEqual(settings.persistence.artifact_threshold_bytes,8192)
        self.assertEqual(HostSettings().persistence.artifact_threshold_bytes,8192)
        self.assertEqual(HostSettings.from_environment({'AUTOAGENT_ARTIFACT_THRESHOLD_BYTES':'16384'}).persistence.artifact_threshold_bytes,16384)
        with self.assertRaises(ValueError):
            HostSettings.from_environment({"AUTOAGENT_WRITER_METADATA_CACHE_BYTES": "0"})
