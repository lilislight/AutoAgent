"""Persist the last discovered MCP schemas and the versions accepted for use."""

import hashlib
import json


def schema_version(schema):
    """Treat documentation annotations as compatible with the previous schema."""
    annotations = {"title", "description", "examples", "$comment"}
    schema_maps = {"properties", "patternProperties", "$defs", "definitions", "dependentSchemas"}
    schema_lists = {"allOf", "anyOf", "oneOf", "prefixItems"}
    schema_values = {"additionalProperties", "unevaluatedProperties", "propertyNames", "items",
        "additionalItems", "unevaluatedItems", "contains", "not", "if", "then", "else"}

    def normalize(value):
        if not isinstance(value, dict):
            return value
        result = {}
        for key, item in value.items():
            if key in annotations:
                continue
            if key in schema_maps and isinstance(item, dict):
                item = {name: normalize(child) for name, child in item.items()}
            elif (key in schema_lists or key == "items") and isinstance(item, list):
                item = [normalize(child) for child in item]
            elif key in schema_values:
                item = normalize(item)
            result[key] = item
        return result

    return hashlib.sha256(json.dumps(normalize(schema), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def observed_catalog(previous, tools):
    """Keep removed names and the last accepted schema for subsequent review."""
    result = {name: {**tool, "listed": False} for name, tool in previous.items()}
    for name, definition in tools.items():
        schema = definition.parameters
        revision = schema_version(schema)
        old = previous.get(name)
        approved = old["approved_schema"] if old is not None and old["approved_version"] != revision else schema
        result[name] = dict(name=definition.name, description=definition.description, input_schema=schema,
            schema_version=revision, approved_schema=approved,
            approved_version=old["approved_version"] if old is not None else revision, listed=True)
    return result
