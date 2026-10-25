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
        actual = context.evidence.member()['status']
        return CheckResult('status', actual == self.expected, f'Expected {self.expected}; observed {actual}.',
            (context.evidence.reference(),))


@dataclass(frozen=True)
class OutputEquals:
    """Compare selected output against Case.expected (including explicit null)."""
    pointer: str | None = None

    async def __call__(self, context):
        actual = await context.evidence.value(pointer=self.pointer)
        difference = _difference(actual, context.case.expected)
        reason = 'Output matches expected.' if difference is None else (
            f'Output differs at {difference[0] or "/"}: expected {_preview(difference[2])}; observed {_preview(difference[1])}.')
        return CheckResult('output', difference is None, reason,
            (dict(context.evidence.reference(), field='output', pointer=self.pointer or ''),))


@dataclass(frozen=True)
class EventCount:
    name: str
    minimum: int = 0
    maximum: int | None = None
    family: bool = False

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name or type(self.minimum) is not int or self.minimum < 0:
            raise ValueError('EventCount requires an Event name and nonnegative minimum.')
        if self.maximum is not None and (type(self.maximum) is not int or self.maximum < self.minimum):
            raise ValueError('maximum cannot be smaller than minimum.')

    async def __call__(self, context):
        ids = [member['invocation_id'] for member in context.evidence.snapshot['members']] if self.family else [context.evidence.invocation_id]
        count = 0
        for iid in ids:
            async for _ in context.evidence.events(names=(self.name,), invocation_id=iid):
                count += 1
        passed = count >= self.minimum and (self.maximum is None or count <= self.maximum)
        return CheckResult(f'events:{self.name}', passed, f'Observed {count} matching Events.',
            tuple(context.evidence.reference(iid) for iid in ids))
