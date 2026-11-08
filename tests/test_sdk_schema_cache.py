"""Static SDK schema reuse preserves dynamic project schemas and bounded retention."""

import json
import unittest
from unittest.mock import patch

from pydantic import ConfigDict, TypeAdapter, create_model

from autoagent.core import ValueContract
from autoagent.core.compiler.snapshot import _annotation_definition
from autoagent.core.operators import schema


class SDKSchemaTests(unittest.TestCase):
    def model(self, name, module='autoagent.schema_tests'):
        return create_model(name, __config__=ConfigDict(extra='forbid'), __module__=module, value=(int, 0))

    def test_sdk_contracts_and_snapshots_reuse_schema_without_sharing_mutable_results(self):
        """Repeated compilation reuses SDK schema generation while exported definitions stay independently editable."""
        model = self.model('CachedSDKValue')
        original = TypeAdapter.json_schema
        with patch.object(TypeAdapter, 'json_schema', autospec=True, side_effect=original) as generated:
            first = ValueContract.create(model, location='first')
            second = ValueContract.create(model, location='second')
            definition = _annotation_definition(model)
            definition['properties'].clear()
            self.assertIn('value', _annotation_definition(model)['properties'])
            self.assertEqual(generated.call_count, 1)
        self.assertEqual(first.schema, second.schema)
        self.assertEqual(second.restore({'value': 3}).value, 3)

    def test_sdk_model_rebuild_and_schema_configuration_invalidate_cached_schema(self):
        """SDK model rebuilds and changed schema configuration cannot return an older definition."""
        model = self.model('RebuiltSDKValue')
        before = schema.schema_json(model)
        model.model_config['title'] = 'Updated title'
        self.assertEqual(json.loads(schema.schema_json(model))['title'], 'Updated title')
        model.model_fields['value'].default = 9
        model.model_rebuild(force=True)
        after = schema.schema_json(model)
        self.assertNotEqual(before, after)
        self.assertEqual(json.loads(after)['properties']['value']['default'], 9)

    def test_project_schema_hooks_are_regenerated(self):
        """Dynamic user schema hooks remain observable on every read instead of inheriting SDK caching."""
        calls = []
        def describe(value):
            calls.append(True)
            value['title'] = 'Generation ' + str(len(calls))
        model = self.model('ProjectValue', module=__name__)
        model.model_config['json_schema_extra'] = describe
        self.assertNotEqual(schema.schema_json(model), schema.schema_json(model))
        self.assertEqual(len(calls), 2)

    def test_oversized_sdk_schema_is_not_retained(self):
        """Large SDK schemas remain usable without exceeding the fixed cache entry budget."""
        model = self.model('LargeSDKValue')
        model.model_config['json_schema_extra'] = {'description': 'x' * (schema._MAX_ENTRY_BYTES + 1)}
        schema.schema_json(model)
        self.assertNotIn(model, schema._cache)
        self.assertLessEqual(schema._cached_bytes, schema._MAX_BYTES)
