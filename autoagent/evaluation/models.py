"""Small, serializable Evaluation definitions and explicit result contracts."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import inspect
import json
import math
from typing import Any, Callable


class EvaluationError(RuntimeError):
    """Evaluation cannot produce a trustworthy result."""


class EvidenceIncomplete(EvaluationError):
    """Evidence is unavailable or exceeds the declared read budget."""


def json_copy(value):
    """Reject non-JSON values, NaN and Infinity at the public boundary."""
    return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        allow_nan=False, separators=(',', ':')).encode()).hexdigest()


def callable_identity(value):
    target = inspect.unwrap(value) if inspect.isfunction(value) else type(value)
    try:
        source = inspect.getsource(target)
    except (OSError, TypeError):
        source = None
    result = dict(name=f'{target.__module__}:{target.__qualname__}',
        source_hash=digest(source) if source is not None else None, identity_complete=source is not None)
    configuration = {}
    if inspect.isfunction(value):
        configuration = dict(defaults=target.__defaults__, keyword_defaults=target.__kwdefaults__,
            closure=inspect.getclosurevars(target).nonlocals)
    elif hasattr(value, '__dataclass_fields__'):
        configuration = {name: getattr(value, name) for name in value.__dataclass_fields__}
    elif getattr(value, '__dict__', None):
        configuration = vars(value)
    try:
        # Identity needs configuration equality, not plaintext credentials or large fixture bodies.
        result['configuration_hash'] = digest(configuration)
    except (TypeError, ValueError):
        result.update(configuration_hash=None, identity_complete=False)
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
        object.__setattr__(self, 'input', json_copy(self.input))
        object.__setattr__(self, 'expected', json_copy(self.expected))
        object.__setattr__(self, 'tags', tuple(self.tags))
        if any(not isinstance(tag, str) or not tag for tag in self.tags):
            raise ValueError('Case tags must be nonempty strings.')

    @property
    def fingerprint(self):
        return digest(dict(input=self.input, expected=self.expected))


@dataclass(frozen=True)
class CheckResult:
    """A business verdict. A null verdict leaves the Case ungraded."""
    name: str
    passed: bool | None
    reason: str = ''
    evidence: tuple[dict, ...] = ()

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name or type(self.passed) not in (bool, type(None)):
            raise ValueError('Checks require a name and a boolean or null verdict.')
        if not isinstance(self.reason, str):
            raise ValueError('Check reason must be a string.')
        json_copy(self.evidence)

    def to_record(self):
        return json_copy(asdict(self))


@dataclass(frozen=True)
class RunOptions:
    concurrency: int = 1
    repeats: int = 1
    timeout_seconds: float = 60
    cleanup_timeout_seconds: float = 10
    evidence_event_limit: int = 100000

    def __post_init__(self):
        for key in ('concurrency', 'repeats', 'evidence_event_limit'):
            positive(getattr(self, key), key, integer=True)
        for key in ('timeout_seconds', 'cleanup_timeout_seconds'):
            positive(getattr(self, key), key)


@dataclass(frozen=True)
class EvalSuite:
    id: str
    cases: tuple[EvalCase, ...]
    scenario: Callable
    evaluators: tuple[Callable, ...] = ()
    version: str = '1'
    fixture: Callable | None = None
    host_factory: Callable | None = None
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        if not isinstance(self.id, str) or not self.id.strip() or not isinstance(self.version, str) or not self.version:
            raise ValueError('Suite id and version must be nonempty strings.')
        object.__setattr__(self, 'cases', tuple(self.cases))
        object.__setattr__(self, 'evaluators', tuple(self.evaluators))
        if not self.cases or any(not isinstance(case, EvalCase) for case in self.cases):
            raise ValueError('Suite requires at least one EvalCase.')
        if len({case.id for case in self.cases}) != len(self.cases):
            raise ValueError('Duplicate Case ids.')
        if not inspect.iscoroutinefunction(self.scenario):
            raise ValueError('Scenario must be an async Python function.')
        if any(not callable(evaluator) for evaluator in self.evaluators):
            raise ValueError('Evaluators must be callable.')
        for value in (self.fixture, self.host_factory):
            if value is not None and not callable(value):
                raise ValueError('fixture and host_factory must be callable.')
        object.__setattr__(self, 'metadata', json_copy(self.metadata))

    def identity(self):
        return dict(id=self.id, version=self.version, scenario=callable_identity(self.scenario),
            evaluators=[callable_identity(item) for item in self.evaluators],
            fixture=callable_identity(self.fixture) if self.fixture else None, metadata=self.metadata,
            host_factory=callable_identity(self.host_factory) if self.host_factory else None)


@dataclass(frozen=True)
class EvaluationEnvironment:
    case: EvalCase
    trial: int
    directory: str
    database_url: str

    resources: dict = field(default_factory=dict, compare=False, repr=False)
