"""Canonical driver JSON and bounded batch-local encoding work."""

import unittest

from autoagent.host.persistence.codec import EncodingMemo, canonical, canonical_text, encode
from autoagent.host.persistence.storage import source_texts


class PersistenceEncodingTests(unittest.TestCase):
    def test_driver_text_matches_canonical_bytes(self):
        """Preserve Unicode, surrogate, primitive and floating-point JSON semantics."""
        for value in ({'a': ['汉字', '\ud800', None, True, False, 1, 1.0, -0.0]}, 'x' * 10000):
            with self.subTest(value_type=type(value).__name__):
                self.assertEqual(canonical_text(value).encode(), canonical(value))
        with self.assertRaises(ValueError):
            canonical_text(float('nan'))

    def test_finalization_releases_encoded_business_containers(self):
        """Keep final text and source metadata without duplicate payload/delta trees."""
        record = {'session_id': 's', 'invocation_id': 'i', 'payload': {'output': {'text': 'x' * 1024}},
                  'delta': {'operations': [{'op': 'set', 'path': ['context'], 'value': {'text': 'z' * 1024}}]}}
        encoded = encode(record, 'runtime_event', 100_000, EncodingMemo(4096))
        payload, delta = canonical(encoded.record['payload']), canonical(encoded.record['delta'])
        texts = source_texts(encoded, 'runtime_event')
        self.assertEqual(texts['payload_text'].encode(), payload)
        self.assertEqual(texts['delta_text'].encode(), delta)
        self.assertEqual(encoded.record, {'session_id': 's', 'invocation_id': 'i'})

    def test_key_memo_is_bounded_and_digests_remain_identical(self):
        """Limit short-key retention and preserve Artifact hashes across memo hits."""
        memo = EncodingMemo(8192)
        for index in range(1000):
            key = str(index)
            self.assertEqual(memo.key_size(key), len(canonical(key)))
        self.assertLessEqual(len(memo.keys), 256)
        memo.key_size('x' * 10000)
        self.assertTrue(all(len(key) <= 128 for key in memo.keys))
        record = {'payload': {'text': 'shared' * 1024}}
        first = encode(record, 'user_event', 256, memo)
        second = encode(record, 'user_event', 256, memo)
        self.assertEqual(first, second)
        self.assertLessEqual(memo.used, memo.budget)
        self.assertLessEqual(memo.used + memo.key_bytes, memo.budget)
