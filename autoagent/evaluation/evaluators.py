"""Deterministic checks; custom sync or async callables share CheckResult."""

from dataclasses import dataclass
import json

from .models import CheckResult


def _difference(actual, expected, path=''):
    """Locate the first JSON difference, keeping booleans distinct from numbers."""
    if isinstance(actual, dict) and isinstance(expected, dict):
        if actual.keys() != expected.keys():
            return path, list(actual), list(expected)
        for key in sorted(actual):
            found = _difference(actual[key], expected[key], path + '/' + key.replace('~', '~0').replace('/', '~1'))
            if found:
                return found
        return None
    if isinstance(actual, list) and isinstance(expected, list):
        if len(actual) != len(expected):
            return path + ' (length)', len(actual), len(expected)
        for index, (left, right) in enumerate(zip(actual, expected)):
            found = _difference(left, right, path + '/' + str(index))
            if found:
                return found
        return None
    if isinstance(actual, bool) != isinstance(expected, bool) or actual != expected:
        return path, actual, expected
    return None


def _preview(value):
    parts, size = [], 0
    for part in json.JSONEncoder(ensure_ascii=False).iterencode(value):
        parts.append(part[:200 - size])
        size += len(part)
        if size >= 200:
            return ''.join(parts) + '…'
    return ''.join(parts)


@dataclass(frozen=True)
class StatusIs:
    expected: str = 'completed'

    def __post_init__(self):
        if self.expected not in ('created', 'running', 'waiting', 'settling', 'completed', 'failed', 'cancelled'):
            raise ValueError('Unknown expected Invocation status.')

    async def __call__(self, context):
        actual = context.execution.status
        return CheckResult('status', actual == self.expected, f'Expected {self.expected}; observed {actual}.',
            ({'invocation_id': context.execution.invocation_id},))


@dataclass(frozen=True)
class OutputEquals:
    """Compare selected output against Case.expected (including explicit null)."""
    pointer: str | None = None

    async def __call__(self, context):
        actual = await context.execution.output(pointer=self.pointer)
        difference = _difference(actual, context.case.expected)
        reason = 'Output matches expected.' if difference is None else (
            f'Output differs at {difference[0] or "/"}: expected {_preview(difference[2])}; observed {_preview(difference[1])}.')
        return CheckResult('output', difference is None, reason,
            (dict({'invocation_id': context.execution.invocation_id}, field='output', pointer=self.pointer or ''),))


@dataclass(frozen=True)
class NodeExecuted:
    """Check a required or prohibited business phase in the complete Workflow observation."""
    node_id: str
    expected: bool = True

    def __post_init__(self):
        if not isinstance(self.node_id, str) or not self.node_id.strip() or type(self.expected) is not bool:
            raise ValueError('NodeExecuted requires a Node id and a boolean expected value.')

    async def __call__(self, context):
        actual = await context.execution.node(self.node_id).was_executed()
        return CheckResult('node:' + self.node_id, actual == self.expected,
            f'Business phase {self.node_id}: expected executed={self.expected}, observed {actual}.')


@dataclass(frozen=True)
class RouteTaken:
    route_id: str
    expected: bool = True

    def __post_init__(self):
        if not isinstance(self.route_id, str) or not self.route_id.strip() or type(self.expected) is not bool:
            raise ValueError('RouteTaken requires a route id and a boolean expected value.')

    async def __call__(self, context):
        actual = await context.execution.route(self.route_id).was_taken()
        return CheckResult('route:' + self.route_id, actual == self.expected,
            f'Business route {self.route_id}: expected taken={self.expected}, observed {actual}.')
