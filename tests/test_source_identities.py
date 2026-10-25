"""Conservative identity membership without retaining source objects."""
import unittest

from autoagent.host.persistence.identities import SourceIdentities


class SourceIdentityTests(unittest.TestCase):
    def test_uninitialized_and_disabled_filters_always_require_sql(self):
        """Incomplete startup coverage can never claim that a source is absent."""
        for budget in (0,1,1024):
            identities=SourceIdentities(budget)
            self.assertTrue(identities.may_contain('runtime_event','old'))
            identities.add('runtime_event','old')
            identities.ready=True
            self.assertTrue(identities.may_contain('runtime_event','old'))
            if budget==0:
                self.assertTrue(identities.may_contain('runtime_event','new'))
            identities.clear()
            self.assertTrue(identities.may_contain('runtime_event','old'))

    def test_saturation_preserves_all_positives_with_bounded_storage(self):
        """Hash collisions only add SQL reads and never hide a stored identity."""
        identities=SourceIdentities(31)
        for index in range(1000):
            identities.add('runtime_event',str(index))
        identities.ready=True
        self.assertLessEqual(len(identities.bits),31)
        for index in range(1000):
            self.assertTrue(identities.may_contain('runtime_event',str(index)))

    def test_complete_empty_filter_proves_absence_and_separates_source_kinds(self):
        """Identity membership includes source kind and supports exact negatives."""
        identities=SourceIdentities(4096);identities.ready=True
        self.assertFalse(identities.may_contain('runtime_event','same-id'))
        identities.add('runtime_event','same-id')
        self.assertTrue(identities.may_contain('runtime_event','same-id'))
        self.assertFalse(identities.may_contain('user_event','same-id'))
