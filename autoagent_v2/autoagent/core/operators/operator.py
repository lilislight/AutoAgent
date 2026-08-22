"""Executable Operator and Wait definitions."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from .contract import OperatorContract, ValueContract


def callable_id(handler: Callable[..., object]) -> str:
    return str(
        getattr(handler, "__name__", None)
        or handler.__class__.__name__
    )


@dataclass(frozen=True, slots=True, init=False)
class Operator:
    id: str
    handler: Callable[..., object] = field(repr=False, compare=False)
    contract: OperatorContract
    version: str

    def __init__(
        self,
        handler: Callable[..., object],
        *,
        id: str | None = None,
        version: str | int = "1",
    ) -> None:
        if not callable(handler):
            raise TypeError("Operator handler must be callable.")
        if id is not None and not isinstance(id, str):
            raise TypeError("Operator id must be a string.")
        operator_id = id or callable_id(handler)
        if not operator_id.strip():
            raise ValueError("Operator id cannot be empty.")
        object.__setattr__(self, "id", operator_id)
        object.__setattr__(self, "handler", handler)
        object.__setattr__(self, "contract", OperatorContract.from_callable(handler))
        object.__setattr__(self, "version", str(version))


@dataclass(frozen=True, slots=True)
class Wait:
    """Suspend one NodeOccurrence with durable custom request/response values."""

    request_type: object
    response_type: object
    id: str = "autoagent.wait"

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not self.id.strip():
            raise ValueError("Wait id cannot be empty.")

    @property
    def input_contract(self) -> ValueContract:
        return ValueContract.create(self.request_type, location="Wait request")

    @property
    def output_contract(self) -> ValueContract:
        return ValueContract.create(self.response_type, location="Wait response")
