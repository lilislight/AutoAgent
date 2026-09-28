"""Authoring and observations for durable, composable Runtime waits."""
from dataclasses import dataclass
from types import MappingProxyType
from typing import ClassVar, Mapping
from pydantic import BaseModel, ConfigDict, JsonValue
from .models import SystemCommand, RuntimeHandle, RuntimeWait, TimerRequest


@dataclass(frozen=True, slots=True)
class AwaitSignal(SystemCommand):
    endpoint: str
    limit: int = 1
    id: ClassVar[str] = 'system_command:await_signal'

    def __post_init__(self):
        SignalCase(self.endpoint, self.limit)


@dataclass(frozen=True, slots=True)
class SignalCase:
    endpoint: str
    limit: int = 1

    def __post_init__(self):
        if not isinstance(self.endpoint, str) or not self.endpoint.strip():
            raise ValueError('Signal Endpoint cannot be empty.')
        if type(self.limit) is not int or self.limit < 1:
            raise ValueError('Signal limit must be a positive integer.')


@dataclass(frozen=True, slots=True)
class TimerCase:
    delay_us: int | None = None
    deadline_at_us: int | None = None

    def __post_init__(self):
        TimerRequest(delay_us=self.delay_us, deadline_at_us=self.deadline_at_us)


@dataclass(frozen=True, slots=True)
class ChildCase:
    handle: object
    after: object = None

    def __post_init__(self):
        if not isinstance(self.handle, RuntimeHandle) and not callable(self.handle):
            raise TypeError('ChildCase requires a RuntimeHandle or Handle resolver.')
        if self.after is not None and not isinstance(self.after, str) and not callable(self.after):
            raise TypeError('ChildCase after requires an observation version or resolver.')


@dataclass(frozen=True, slots=True)
class Select(SystemCommand):
    cases: Mapping[str, SignalCase | TimerCase | ChildCase]
    id: ClassVar[str] = 'system_command:select'

    def __post_init__(self):
        if not isinstance(self.cases, Mapping) or not self.cases:
            raise ValueError('Select requires named cases.')
        for name, case in self.cases.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError('Select case names must be nonempty strings.')
            if type(case) not in (SignalCase, TimerCase, ChildCase):
                raise TypeError('Unsupported Select case.')
        object.__setattr__(self, 'cases', MappingProxyType(dict(self.cases)))


class SelectResult(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    case: str
    value: JsonValue


class SuspensionInfo(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    can_unload: bool
    next_deadline_at_us: int | None = None
    waits: list[RuntimeWait]


class CommandSuspended(Exception):
    """Internal control flow: the durable wait owns the unfinished Call."""
