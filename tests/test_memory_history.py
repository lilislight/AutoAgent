"""Long-lived numeric history and query bounds without running timed benchmarks."""

import unittest

from autoagent.host.memory_history import MemoryHistory, WIDTH, history_points


def sample(index, rss=None):
    return dict(timestamp_us=index * 1_000_000, monotonic_seconds=index, pressure='normal', budget_bytes=1024,
                process=dict(rss_bytes=index if rss is None else rss), cgroup=None, host=None)


class MemoryHistoryTests(unittest.TestCase):
    def test_wraparound_and_age_retention_bound_numeric_storage(self):
        """Capacity overwrites and time expiry preserve chronological points at fixed memory cost."""
        ring = MemoryHistory(5, 3)
        self.assertEqual(ring.storage_bytes, 0)
        for i in range(20):
            ring.append(sample(i))
        packed, retained = ring.capture(history=5, window_seconds=None, now=19)
        self.assertEqual(retained, 4)
        self.assertEqual([p['timestamp_us'] for p in history_points(packed, 5)], [i * 1_000_000 for i in range(16, 20)])
        self.assertEqual(ring.storage_bytes, 5 * WIDTH * 8)
        packed, retained = ring.capture(history=5, window_seconds=None, now=30)
        self.assertEqual((len(packed), retained), (0, 0))
        ring.append(sample(31))
        self.assertEqual(ring.count, 1)
        self.assertEqual(ring.storage_bytes, 5 * WIDTH * 8)
        ring.clear()
        self.assertEqual((ring.count, ring.storage_bytes), (0, 0))

    def test_downsampling_preserves_spikes_endpoints_and_missing_values(self):
        """A small response preserves sampled RSS extremes instead of averaging away a short spike."""
        ring = MemoryHistory(100, 86400)
        for i in range(100):
            ring.append(sample(i, 99999 if i == 47 else 100))
        packed, retained = ring.capture(history=100, window_seconds=None, now=99)
        points = history_points(packed, 12)
        self.assertEqual(retained, 100)
        self.assertLessEqual(len(points), 12)
        self.assertEqual((points[0]['timestamp_us'], points[-1]['timestamp_us']), (0, 99_000_000))
        self.assertIn(99999, [p['rss_bytes'] for p in points])
        self.assertEqual([p['timestamp_us'] for p in points], sorted({p['timestamp_us'] for p in points}))
        self.assertTrue(all(p['projection_bytes'] is None for p in points))
        self.assertTrue(all('host' not in p for p in points))

    def test_time_window_and_sample_selection_do_not_leak_unselected_history(self):
        """Window queries use monotonic time and intersect explicit count bounds after wraparound."""
        ring = MemoryHistory(100, 86400)
        for i in range(150):
            ring.append(sample(i))
        packed, _ = ring.capture(history=10, window_seconds=3, now=149)
        self.assertEqual([p['timestamp_us'] for p in history_points(packed, 600)], [146_000_000, 147_000_000, 148_000_000, 149_000_000])
        self.assertEqual(len(ring.capture(history=0, window_seconds=None, now=149)[0]), 0)

    def test_full_day_retention_and_default_response_are_bounded(self):
        """A day of one-second samples fits in six MiB and returns at most 600 points."""
        ring = MemoryHistory(86400, 86400)
        for i in range(86401):
            ring.append(sample(i, 50000 if i == 45001 else 100))
        packed, retained = ring.capture(history=0, window_seconds=86400, now=86400)
        points = history_points(packed, 600)
        self.assertEqual(retained, 86400)
        self.assertLess(ring.storage_bytes, 6 * 1024**2)
        self.assertLessEqual(len(points), 600)
        self.assertIn(50000, [p['rss_bytes'] for p in points])

