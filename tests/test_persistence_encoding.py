"""Canonical driver JSON and bounded batch-local encoding work."""

import unittest
from unittest.mock import patch
from types import SimpleNamespace

from autoagent.host.persistence.codec import EncodingMemo, canonical, canonical_text, encode
from autoagent.host.persistence.storage import source_texts


class PersistenceEncodingTests(unittest.TestCase):
    def test_batch_cache_accounting_is_fused_and_roundtrips_with_eviction(self):
        """Owned conversions need no second object walk and stay bounded through eviction."""
        from autoagent.core.runtime import RuntimeEvent
        from autoagent.core.runtime.events import SessionOpened
        from autoagent.host.persistence.codec import retained_objects
        from autoagent.host.persistence.storage import prepare
        from autoagent.host.persistence import PersistenceConfig
        from autoagent.core.runtime.values import freeze, thaw, record_values
        memo = EncodingMemo(8192)
        for index in range(30):
            value = freeze({'text': '汉字' * (index + 1), 'rows': [index, True, None]})
            with record_values(memo.record_value):
                record = thaw(value)
            cached = memo.converted.get(id(value))
            if cached:
                actual = sum(size for _, size in retained_objects((value, record)).values())
                self.assertGreaterEqual(cached[2], actual)
            encode({'payload': record}, 'user_event', 128, memo)
            self.assertLessEqual(memo.used + memo.key_bytes + memo.converted_bytes + memo.packed_bytes, memo.budget)
        event = RuntimeEvent('s', 1, SessionOpened({'text': 'x' * 16384}))
        entry = SimpleNamespace(kind='runtime_event', identity=event.id, value=event, binding=None, anchor=False)
        with patch('autoagent.host.persistence.codec.retained_objects', side_effect=AssertionError('Second walk')):
            self.assertEqual(len(prepare([entry], PersistenceConfig())), 1)

    def test_small_scalar_memo_preserves_json_types_and_escaped_lengths(self):
        """Repeated metadata strings reuse canonical bytes without changing numbers or escaping."""
        memo = EncodingMemo(8192)
        for text in ('call-id', '汉字', '\ud800', 'quotes"\\'):
            self.assertIs(memo.string(text), memo.string(text))
            self.assertEqual(memo.string(text), canonical(text))
        for value in (None, True, False, 1, 1.0, -0.0, 10**100):
            encoded = encode({'payload': value}, 'user_event', 8192, memo)
            self.assertEqual(encoded.encoded_bytes, len(canonical({'payload': value})))

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
