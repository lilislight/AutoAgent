"""Effective capacity detection and shared headroom without real resource pressure."""

from contextlib import contextmanager
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from autoagent.host.memory_resources import (FALLBACK_BUDGET, MemoryController,
    MemoryResources, memory_controller, system_memory)

GIB = 1024**3


class MemoryResourcesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.mount = Path(self.temp.name) / 'memory view'
        self.leaf = self.mount / 'parent' / 'worker'
        self.leaf.mkdir(parents=True)

    def files(self, directory, limit='max', used=0, unified=True):
        (directory / ('memory.max' if unified else 'memory.limit_in_bytes')).write_text(str(limit))
        (directory / ('memory.current' if unified else 'memory.usage_in_bytes')).write_text(str(used))

    @contextmanager
    def proc(self, membership, *, root='/', unified=True):
        original = Path.read_text
        escaped = str(self.mount).replace(' ', r'\040')
        filesystem = 'cgroup2 cgroup rw' if unified else 'cgroup cgroup rw,memory'
        records = {'/proc/self/cgroup': membership,
                   '/proc/self/mountinfo': f'1 0 0:1 {root} {escaped} rw - {filesystem}'}
        def read(path, *args, **kwargs):
            return records[str(path)] if str(path) in records else original(path, *args, **kwargs)
        with patch.object(Path, 'read_text', read):
            yield

    def resource(self, configured=0, *, unified=True):
        with patch('autoagent.host.memory_resources.memory_controller', return_value=MemoryController(self.leaf, self.mount, unified)):
            return MemoryResources(configured)

    def test_physical_capacity_has_no_512_mib_ceiling(self):
        """An unconfined 16 GiB machine receives 80 percent capacity, independent of free-memory changes."""
        with patch('autoagent.host.memory_resources.memory_controller', return_value=None):
            resources = MemoryResources(0)
        with patch('autoagent.host.memory_resources.system_memory', return_value=(16 * GIB, 12 * GIB)):
            before = resources.sample()
        with patch('autoagent.host.memory_resources.system_memory', return_value=(16 * GIB, GIB // 2)):
            after = resources.sample()
        self.assertEqual(before.budget_bytes, 16 * GIB * 4 // 5)
        self.assertEqual(before.budget_bytes, after.budget_bytes)
        self.assertEqual(before.budget_source, 'physical')
        self.assertGreater(after.pressure_ratio(), .9)

    def test_nested_cgroup_limits_and_shared_ancestor_usage_both_apply(self):
        """A tight parent can exhaust shared headroom despite a nearly empty leaf process group."""
        self.files(self.mount)
        self.files(self.leaf.parent, 10 * GIB, 19 * GIB // 2)
        self.files(self.leaf, 2 * GIB, GIB // 10)
        resources = self.resource()
        with patch('autoagent.host.memory_resources.system_memory', return_value=(16 * GIB, 12 * GIB)):
            sample = resources.sample()
        self.assertEqual(sample.budget_bytes, 2 * GIB * 4 // 5)
        self.assertEqual(sample.budget_source, 'cgroup')
        self.assertEqual(sample.cgroup['available_bytes'], GIB // 2)
        self.assertAlmostEqual(sample.pressure_ratio(), .95)
        self.assertGreater(sample.reclaim_bytes(.65), 0)
        self.files(self.leaf.parent, GIB, GIB // 2)
        with patch('autoagent.host.memory_resources.system_memory', return_value=(16 * GIB, 12 * GIB)):
            sample = resources.sample()
        self.assertEqual(sample.budget_bytes, GIB * 4 // 5)
        self.assertEqual(sample.cgroup['limit_path'], str(self.leaf.parent))

    def test_explicit_budget_wins_and_auto_capacity_tracks_limit_changes(self):
        """Configured budgets remain fixed while automatic budgets follow resized container capacity."""
        self.files(self.mount)
        self.files(self.leaf.parent)
        self.files(self.leaf, 4 * GIB)
        automatic, explicit = self.resource(), self.resource(3 * GIB)
        with patch('autoagent.host.memory_resources.system_memory', return_value=(16 * GIB, 12 * GIB)):
            self.assertEqual(automatic.sample().budget_bytes, 4 * GIB * 4 // 5)
            self.files(self.leaf, 8 * GIB)
            self.assertEqual(automatic.sample().budget_bytes, 8 * GIB * 4 // 5)
            self.assertEqual(explicit.sample().budget_bytes, 3 * GIB)
            self.assertEqual(explicit.sample().budget_source, 'explicit')

    def test_v1_unlimited_values_and_unavailable_capacity_fallback(self):
        """Legacy controller unlimited sentinels are ignored; unknown capacity alone uses 512 MiB."""
        self.files(self.mount, 9223372036854771712, unified=False)
        self.files(self.leaf.parent, GIB, unified=False)
        self.files(self.leaf, 9223372036854771712, unified=False)
        resources = self.resource(unified=False)
        with patch('autoagent.host.memory_resources.system_memory', return_value=(None, None)):
            self.assertEqual(resources.sample().budget_bytes, GIB * 4 // 5)
        with patch('autoagent.host.memory_resources.memory_controller', return_value=None):
            resources = MemoryResources(0)
        with patch('autoagent.host.memory_resources.system_memory', return_value=(None, None)):
            sample = resources.sample()
        self.assertEqual(sample.budget_bytes, FALLBACK_BUDGET)
        self.assertEqual(sample.budget_source, 'fallback')

    def test_membership_mount_roots_namespaces_and_escaped_paths(self):
        """Detection maps the process group into its mounted namespace and decodes escaped mount paths."""
        with self.proc('0::/tenant/parent/worker', root='/tenant'):
            self.assertEqual(memory_controller(), MemoryController(self.leaf, self.mount, True))
        with self.proc('0::/', root='/tenant/parent/worker'):
            self.assertEqual(memory_controller(), MemoryController(self.mount, self.mount, True))
        with self.proc('5:cpu,memory:/parent/worker', unified=False):
            self.assertEqual(memory_controller(), MemoryController(self.leaf, self.mount, False))
        with self.proc('0::/../../outside'):
            self.assertIsNone(memory_controller())

    def test_physical_memory_fallback_and_missing_availability(self):
        """Unsupported procfs can still expose physical capacity, without inventing available memory."""
        with patch.object(Path, 'read_text', side_effect=OSError), patch('os.sysconf', side_effect=[16 * GIB // 4096, 4096]):
            self.assertEqual(system_memory(), (16 * GIB, None))
        with patch.object(Path, 'read_text', side_effect=OSError), patch('os.sysconf', side_effect=ValueError):
            self.assertEqual(system_memory(), (None, None))

    def test_physical_capacity_caps_large_container_limit_and_zero_limit_blocks(self):
        """A container cannot increase physical capacity, and a zero limit cannot become unlimited."""
        self.files(self.mount)
        self.files(self.leaf.parent)
        self.files(self.leaf, 64 * GIB)
        resources = self.resource()
        with patch('autoagent.host.memory_resources.system_memory', return_value=(16 * GIB, 12 * GIB)):
            self.assertEqual(resources.sample().budget_bytes, 16 * GIB * 4 // 5)
            self.files(self.leaf, 0)
            sample = resources.sample()
        self.assertEqual(sample.budget_bytes, 1)
        self.assertEqual(sample.pressure_ratio(), 1)
