"""Writer metadata retention and transaction publication boundaries."""

import unittest

from autoagent.host.models import HostSettings
from autoagent.host.persistence.metadata import MetadataCache


class WriterMetadataTests(unittest.TestCase):
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
        with self.assertRaises(ValueError):
            HostSettings.from_environment({"AUTOAGENT_WRITER_METADATA_CACHE_BYTES": "0"})
