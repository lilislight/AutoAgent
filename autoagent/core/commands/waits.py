"""Authoring and observations for durable, composable Runtime waits."""
from dataclasses import dataclass
from types import MappingProxyType
from typing import Annotated, ClassVar, Literal, Mapping
from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator
from .models import SystemCommand, RuntimeHandle, RuntimeWait, TimerRequest


@dataclass(frozen=True, slots=True)
class AwaitSignal(SystemCommand):
    endpoint: str | None = None
    limit: int = 1
    id: ClassVar[str] = 'system_command:await_signal'

    def __post_init__(self):
        if self.endpoint is None:
            if type(self.limit) is not int or self.limit != 1:
                raise ValueError('Dynamic AwaitSignal takes limit in its request.')
            return
        SignalCase(self.endpoint, self.limit)


@dataclass(frozen=True, slots=True)
class SignalCase:
    endpoint: str
    limit: int = 1
    consume: bool = True

    def __post_init__(self):
        if type(self.consume) is not bool:
            raise TypeError("Signal consume must be bool.")
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
    scope: Literal["direct", "graph"] = "direct"

    def __post_init__(self):
        if self.scope not in ("direct", "graph"):
            raise ValueError("Child scope must be direct or graph.")
        if not isinstance(self.handle, RuntimeHandle) and not callable(self.handle):
            raise TypeError('ChildCase requires a RuntimeHandle or Handle resolver.')
        if self.after is not None and not isinstance(self.after, str) and not callable(self.after):
            raise TypeError('ChildCase after requires an observation version or resolver.')


@dataclass(frozen=True, slots=True)
class Select(SystemCommand):
    cases: Mapping[str, SignalCase | TimerCase | ChildCase] | None = None
    id: ClassVar[str] = 'system_command:select'

    def __post_init__(self):
        if self.cases is None:
            return
        if not isinstance(self.cases, Mapping) or not self.cases:
            raise ValueError('Select requires named cases.')
        for name, case in self.cases.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError('Select case names must be nonempty strings.')
            if type(case) not in (SignalCase, TimerCase, ChildCase):
                raise TypeError('Unsupported Select case.')
        object.__setattr__(self, 'cases', MappingProxyType(dict(self.cases)))


class SignalRequestCase(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    kind: Literal['signal'] = 'signal'
    endpoint: str = Field(min_length=1)
    limit: int = Field(default=1, ge=1)
    consume: bool = True


class TimerRequestCase(TimerRequest):
    kind: Literal['timer'] = 'timer'


class ChildRequestCase(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    kind: Literal['child'] = 'child'
    handle: RuntimeHandle
    after: str | None = None
    scope: Literal["direct", "graph"] = "direct"


class SelectRequest(BaseModel):
    """One serializable set of competitors, also usable as a Map unit."""
    model_config = ConfigDict(extra='forbid', frozen=True)
    cases: dict[str, Annotated[SignalRequestCase | TimerRequestCase | ChildRequestCase, Field(discriminator='kind')]]

    @model_validator(mode='after')
    def named_cases(self):
        if not self.cases or any(not name.strip() for name in self.cases):
            raise ValueError('Select requires nonempty named cases.')
        return self


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
