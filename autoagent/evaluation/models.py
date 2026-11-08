"""Business Evaluation definitions with immutable data and explicit comparison conditions."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import inspect
import json
import math
import types
from typing import Any, Callable


class EvaluationError(RuntimeError):
    """Evaluation cannot produce a trustworthy business verdict."""


def _immutable(*args, **kwargs):
    raise EvaluationError('Evaluation data is immutable; use copy_input()/copy_expected() to construct working data.')


class FrozenDict(dict):
    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = __ior__ = _immutable
    def __deepcopy__(self, memo): return self


class FrozenList(list):
    __setitem__ = __delitem__ = append = clear = extend = insert = pop = remove = reverse = sort = __iadd__ = __imul__ = _immutable
    def __deepcopy__(self, memo): return self


def json_copy(value):
    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))


def freeze_json(value):
    if isinstance(value, dict): return FrozenDict({key: freeze_json(item) for key, item in value.items()})
    if isinstance(value, list): return FrozenList(freeze_json(item) for item in value)
    return value


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        allow_nan=False, separators=(',', ':')).encode()).hexdigest()


def _class_dependency(target, seen):
    name = target.__module__ + ':' + target.__qualname__
    if id(target) in seen:
        return dict(reference=name, identity_complete=True)
    if len(seen) > 32:
        return dict(name=name, identity_complete=False)
    try:
        source = inspect.getsource(target)
    except (OSError, TypeError):
        source = None
    result = dict(name=name, source_hash=digest(source) if source else None,
        identity_complete=source is not None, methods={}, configuration={})
    for key, item in vars(target).items():
        if key in ('__module__', '__doc__', '__dict__', '__weakref__', '__annotations__'): continue
        if isinstance(item, (staticmethod, classmethod)): item = item.__func__
        if isinstance(item, property): item = item.fget
        if inspect.isfunction(item):
            identity = callable_identity(item, seen | {id(target)})
            result['methods'][key] = identity
            result['identity_complete'] &= identity['identity_complete']
        else:
            try:
                result['configuration'][key] = digest(json_copy(item))
            except (TypeError, ValueError):
                result['identity_complete'] = False
    return result


def callable_identity(value, _seen=None):
    """Fingerprint inspectable rule dependencies; opaque state remains explicitly unknown."""
    seen = set() if _seen is None else _seen
    target = inspect.unwrap(value) if inspect.isfunction(value) else type(value)
    name = f'{target.__module__}:{target.__qualname__}'
    if id(target) in seen:
        return dict(reference=name, identity_complete=True)
    if len(seen) > 32:
        return dict(name=name, identity_complete=False)
    seen = seen | {id(target)}
    try:
        source = inspect.getsource(target)
    except (OSError, TypeError):
        source = None
    result = dict(name=name, source_hash=digest(source) if source is not None else None, identity_complete=source is not None)
    configuration, dependencies = {}, {}
    function = target if inspect.isfunction(value) else getattr(target, '__call__', None)
    if inspect.isfunction(function):
        variables = inspect.getclosurevars(function)
        configuration.update(defaults=function.__defaults__, keyword_defaults=function.__kwdefaults__)
        for key, item in (variables.globals | variables.nonlocals).items():
            try:
                configuration[key] = json_copy(item)
            except (TypeError, ValueError):
                if inspect.isfunction(item):
                    dependencies[key] = callable_identity(item, seen)
                elif isinstance(item, type) and item.__module__.startswith('autoagent.'):
                    dependencies[key] = dict(name=item.__module__ + ':' + item.__qualname__, identity_complete=True)
                elif isinstance(item, type):
                    dependencies[key] = _class_dependency(item, seen)
                elif isinstance(item, types.ModuleType) and item.__name__ in ('json', 'math', 'operator', 'statistics'):
                    dependencies[key] = dict(module=item.__name__, identity_complete=True)
                else:
                    dependencies[key] = dict(type=type(item).__module__ + ':' + type(item).__qualname__, identity_complete=False)
    if hasattr(value, '__dataclass_fields__'):
        configuration['instance'] = {key: getattr(value, key) for key in value.__dataclass_fields__}
    elif not inspect.isfunction(value) and getattr(value, '__dict__', None):
        configuration['instance'] = vars(value)
    try:
        result['configuration_hash'] = digest(configuration)
    except (TypeError, ValueError):
        result.update(configuration_hash=None, identity_complete=False)
    result['dependencies'] = dependencies
    result['identity_complete'] &= all(item['identity_complete'] for item in dependencies.values())
    return result


def positive(value, name, *, integer=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0 or integer and type(value) is not int:
        raise ValueError(f'{name} must be a positive {"integer" if integer else "finite number"}.')


@dataclass(frozen=True)
class EvalCase:
    id: str
    input: Any
    expected: Any = None
    tags: tuple[str, ...] = ()

    def __post_init__(self):
        if not isinstance(self.id, str) or not self.id.strip() or len(self.id) > 512:
            raise ValueError('Case id must contain 1–512 characters.')
        object.__setattr__(self, 'input', freeze_json(json_copy(self.input)))
        object.__setattr__(self, 'expected', freeze_json(json_copy(self.expected)))
        object.__setattr__(self, 'tags', tuple(self.tags))
        if any(not isinstance(tag, str) or not tag for tag in self.tags):
            raise ValueError('Case tags must be nonempty strings.')

    def copy_input(self): return json_copy(self.input)
    def copy_expected(self): return json_copy(self.expected)
    @property
    def fingerprint(self): return digest(dict(input=self.input, expected=self.expected))


@dataclass(frozen=True)
class CheckResult:
    """One named business rule; null leaves the Case ungraded."""
    name: str
    passed: bool | None
    reason: str = ''
    evidence: tuple[dict, ...] = ()

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name or type(self.passed) not in (bool, type(None)):
            raise ValueError('Checks require a stable name and boolean or null verdict.')
        if not isinstance(self.reason, str): raise ValueError('Check reason must be a string.')
        object.__setattr__(self, 'evidence', tuple(freeze_json(item) for item in json_copy(self.evidence)))

    def to_record(self): return json_copy(asdict(self))


@dataclass(frozen=True)
class RunOptions:
    concurrency: int = 1
    repeats: int = 1
    timeout_seconds: float = 60
    cleanup_timeout_seconds: float = 10
    read_limit: int = 100000

    def __post_init__(self):
        for key in ('concurrency', 'repeats', 'read_limit'): positive(getattr(self, key), key, integer=True)
        for key in ('timeout_seconds', 'cleanup_timeout_seconds'): positive(getattr(self, key), key)


@dataclass(frozen=True)
class EvalSuite:
    """Whole-Workflow business cases and rules; declare conditions={} when no external conditions exist.

    Undeclared conditions permit execution but cannot establish an accepted comparison.
    """
    id: str
    cases: tuple[EvalCase, ...]
    scenario: Callable
    evaluators: tuple[Callable, ...] = ()
    version: str = '1'
    fixture: Callable | None = None
    host_factory: Callable | None = None
    conditions: dict | None = None
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        if not isinstance(self.id, str) or not self.id.strip() or not isinstance(self.version, str) or not self.version:
            raise ValueError('Suite id and rule version must be nonempty strings.')
        object.__setattr__(self, 'cases', tuple(self.cases))
        object.__setattr__(self, 'evaluators', tuple(self.evaluators))
        if not self.cases or any(not isinstance(case, EvalCase) for case in self.cases): raise ValueError('Suite requires EvalCases.')
        if len({case.id for case in self.cases}) != len(self.cases): raise ValueError('Duplicate Case ids.')
        if not inspect.iscoroutinefunction(self.scenario): raise ValueError('Scenario must be an async Python function.')
        if any(not callable(item) for item in self.evaluators): raise ValueError('Evaluators must be callable.')
        if any(item is not None and not callable(item) for item in (self.fixture, self.host_factory)):
            raise ValueError('fixture and host_factory must be callable.')
        if not isinstance(self.metadata, dict) or self.conditions is not None and not isinstance(self.conditions, dict):
            raise ValueError('metadata and conditions must be JSON objects.')
        object.__setattr__(self, 'metadata', freeze_json(json_copy(self.metadata)))
        object.__setattr__(self, 'conditions', None if self.conditions is None else freeze_json(json_copy(self.conditions)))

    def identity(self):
        return dict(id=self.id, version=self.version, scenario=callable_identity(self.scenario),
            evaluators=[callable_identity(item) for item in self.evaluators], fixture=callable_identity(self.fixture) if self.fixture else None,
            metadata=json_copy(self.metadata), conditions=json_copy(self.conditions),
            host_factory=callable_identity(self.host_factory) if self.host_factory else None)


@dataclass(frozen=True)
class EvaluationEnvironment:
    case: EvalCase
    trial: int
    directory: str
    database_url: str
    resources: dict = field(default_factory=dict, compare=False, repr=False)
