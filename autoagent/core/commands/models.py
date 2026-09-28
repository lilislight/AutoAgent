"""Core-owned command definitions and durable runtime identities."""
from __future__ import annotations

from abc import ABC, abstractmethod
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import ClassVar, Literal, TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator


if TYPE_CHECKING:
    from ..operators.contract import ValueContract
    from ..workflow.models import Workflow, WorkflowIR


class RuntimeHandle(BaseModel):
    """Identity only; each Command validates the caller/target relationship."""
    model_config = ConfigDict(extra='forbid', frozen=True, strict=True)
    session_id: str
    invocation_id: str
    workflow_id: str
    workflow_revision_id: str

    @field_validator('session_id', 'invocation_id', 'workflow_id', 'workflow_revision_id')
    @classmethod
    def non_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError('Runtime identity cannot be empty.')
        return value


class RuntimeWait(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    handle: RuntimeHandle
    wait_id: str
    request: JsonValue = None
    kind: Literal["external", "signal", "timer", "child", "any"] = "external"
    condition: JsonValue = None


class RuntimeObservation(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    handle: RuntimeHandle
    status: Literal['created', 'running', 'waiting', 'settling', 'completed', 'failed', 'cancelled']
    pending_outcome: Literal['completed', 'failed', 'cancelled'] | None = None
    output: JsonValue = None
    error: dict[str, JsonValue] | None = None
    cancel_reason: str | None = None
    waits: list[RuntimeWait] = Field(default_factory=list)
    version: str = ""


class SystemCommand(ABC):
    """Definition of a Core primitive, not a user-supplied Runtime mutator."""
    @property
    @abstractmethod
    def id(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class Wait(SystemCommand):
    request_type: object
    response_type: object
    id: ClassVar[str] = 'system_command:wait'

    input_contract: ValueContract = field(init=False, repr=False)
    output_contract: ValueContract = field(init=False, repr=False)

    def __post_init__(self) -> None:
        from ..operators.contract import ValueContract
        object.__setattr__(self, 'input_contract', ValueContract.create(self.request_type, location='Wait request'))
        object.__setattr__(self, 'output_contract', ValueContract.create(self.response_type, location='Wait response'))


@dataclass(frozen=True, slots=True)
class Spawn(SystemCommand):
    workflow: Workflow
    entry_node_id: str
    id: ClassVar[str] = 'system_command:spawn'

    def __post_init__(self) -> None:
        _entry(self.entry_node_id)


@dataclass(frozen=True, slots=True)
class Await(SystemCommand):
    workflow: Workflow | None = None
    entry_node_id: str | None = None
    id: ClassVar[str] = 'system_command:await'

    def __post_init__(self) -> None:
        if self.workflow is None:
            if self.entry_node_id is not None:
                raise ValueError('Handle Await has no entry Node.')
        else:
            _entry(self.entry_node_id)


class ResumeRequest(BaseModel):
    """A response addressed to one exact Invocation and Wait."""
    model_config = ConfigDict(extra='forbid', frozen=True)
    handle: RuntimeHandle
    wait_id: str = Field(min_length=1)
    response: JsonValue


class ResumeReceipt(BaseModel):
    """Acceptance of a response, not completion of the target Invocation."""
    model_config = ConfigDict(extra='forbid', frozen=True)
    handle: RuntimeHandle
    wait_id: str = Field(min_length=1)


@dataclass(frozen=True, slots=True)
class Resume(SystemCommand):
    id: ClassVar[str] = 'system_command:resume'


@dataclass(frozen=True, slots=True)
class Status(SystemCommand):
    """Observe the target's current acknowledged state without waiting."""
    id: ClassVar[str] = 'system_command:status'


def _entry(value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError('A creating Command requires entry_node_id.')


@dataclass(frozen=True, slots=True)
class CommandIR:
    id: str
    workflow: WorkflowIR | None = None
    entry_node_id: str | None = None
    handle: object = None
    endpoint: str | None = None
    limit: int | None = None
    cases: object = None


runtime_handles: ContextVar[tuple[RuntimeHandle | None, RuntimeHandle | None]] = ContextVar('runtime_handles', default=(None, None))

class CancelRequest(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    handle: RuntimeHandle
    reason: str | None = Field(default=None, min_length=1)


class CancelReceipt(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    handle: RuntimeHandle
    disposition: Literal['requested', 'already_stopping', 'already_terminal']


@dataclass(frozen=True, slots=True)
class Cancel(SystemCommand):
    id: ClassVar[str] = 'system_command:cancel'


class AwaitAnyRequest(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    handles: list[RuntimeHandle] = Field(min_length=1)

    @field_validator('handles')
    @classmethod
    def unique_handles(cls, handles):
        if len(set(handles)) != len(handles):
            raise ValueError('AwaitAny handles must be unique.')
        return handles


@dataclass(frozen=True, slots=True)
class AwaitAny(SystemCommand):
    id: ClassVar[str] = 'system_command:await_any'


class TimerRequest(BaseModel):
    """Specify a relative delay or an absolute Unix deadline, in microseconds."""
    model_config = ConfigDict(extra='forbid', frozen=True, strict=True)
    delay_us: int | None = Field(default=None, ge=0)
    deadline_at_us: int | None = Field(default=None, ge=0)

    @model_validator(mode='after')
    def one_time(self):
        if (self.delay_us is None) == (self.deadline_at_us is None):
            raise ValueError('Specify exactly one of delay_us and deadline_at_us.')
        return self


class TimerResult(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    deadline_at_us: int = Field(ge=0)


@dataclass(frozen=True, slots=True)
class Timer(SystemCommand):
    id: ClassVar[str] = 'system_command:timer'


SYSTEM_COMMAND_IDS = frozenset({Spawn.id, Await.id, Wait.id, Resume.id, Status.id, Cancel.id, AwaitAny.id, Timer.id, 'system_command:send_signal', 'system_command:receive_signal', 'system_command:await_signal', 'system_command:select'})
