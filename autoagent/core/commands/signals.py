"""Typed Signal contracts and bounded mailbox configuration."""
from dataclasses import dataclass
from typing import ClassVar
from pydantic import BaseModel, ConfigDict, Field, field_validator
from .models import RuntimeHandle, SystemCommand
from pydantic import JsonValue


@dataclass(frozen=True, slots=True)
class SignalEndpoint:
    name: str
    payload_type: object

    def __post_init__(self):
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError('Signal Endpoint name cannot be empty.')


@dataclass(frozen=True, slots=True)
class SignalLimits:
    """Per-Invocation bounds; bytes count canonical UTF-8 JSON payloads."""
    max_message_bytes: int = 1_048_576
    max_mailbox_bytes: int = 8_388_608
    max_messages: int = 128
    max_pending_receipts: int = 128
    max_internal_sources: int = 128
    max_external_sources: int = 64

    def __post_init__(self):
        from dataclasses import fields
        if any(type(getattr(self, f.name)) is not int or getattr(self, f.name) < 1 for f in fields(self)):
            raise ValueError('Signal limits must be positive integers.')
        if self.max_message_bytes > self.max_mailbox_bytes:
            raise ValueError('A message must fit within the mailbox byte limit.')


class SendSignalRequest(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    handle: RuntimeHandle
    endpoint: str = Field(min_length=1)
    payload: JsonValue


class SignalReceipt(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    handle: RuntimeHandle
    endpoint: str
    message_id: str
    accepted_sequence: int


class SignalMessage(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    message_id: str
    endpoint: str
    payload: JsonValue
    accepted_at_us: int
    accepted_sequence: int
    sender_id: str | None = None


class ReceiveSignalRequest(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, strict=True)
    endpoint: str = Field(min_length=1)
    limit: int = Field(default=1, ge=1)


class HasSignalRequest(BaseModel):
    """Observe whether any selected Endpoint has unread messages, without consuming."""

    model_config = ConfigDict(extra='forbid', frozen=True, strict=True)
    endpoints: list[str] = Field(min_length=1)

    @field_validator('endpoints')
    @classmethod
    def valid_endpoints(cls, values):
        if any(not value.strip() for value in values) or len(set(values)) != len(values):
            raise ValueError('Signal Endpoints must be nonempty and unique.')
        return values


class HasSignalResult(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, strict=True)
    has_messages: bool


@dataclass(frozen=True, slots=True)
class HasSignal(SystemCommand):
    """Return a point-in-time boolean; a later delivery does not change the result."""

    id: ClassVar[str] = 'system_command:has_signal'


class SignalBatch(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    messages: list[SignalMessage] = Field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SelfHandle:
    """Resolve the Invocation executing this Command."""


@dataclass(frozen=True, slots=True)
class OwnerHandle:
    """Resolve the direct Parent of the executing Invocation."""


@dataclass(frozen=True, slots=True)
class SendSignal(SystemCommand):
    handle: object = None
    endpoint: str | None = None

    def __post_init__(self):
        if self.handle is None and self.endpoint is None:
            return
        if not isinstance(self.handle, (RuntimeHandle, SelfHandle, OwnerHandle)) and not callable(self.handle):
            raise TypeError('Signal handle must be a RuntimeHandle, selector or resolver.')
        if not isinstance(self.endpoint, str) or not self.endpoint.strip():
            raise ValueError('Signal Endpoint cannot be empty.')

    id: ClassVar[str] = 'system_command:send_signal'


@dataclass(frozen=True, slots=True)
class ReceiveSignal(SystemCommand):
    endpoint: str | None = None
    limit: int = 1

    def __post_init__(self):
        if self.endpoint is None:
            if type(self.limit) is not int or self.limit != 1:
                raise ValueError('Dynamic ReceiveSignal takes limit in its request.')
            return
        if not isinstance(self.endpoint, str) or not self.endpoint.strip():
            raise ValueError('Signal Endpoint cannot be empty.')
        if type(self.limit) is not int or self.limit < 1:
            raise ValueError('Signal receive limit must be a positive integer.')

    id: ClassVar[str] = 'system_command:receive_signal'
