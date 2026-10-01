"""Executable Operator definition."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from .contract import OperatorContract


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
        if operator_id.startswith("system_command:"):
            raise ValueError("system_command: is reserved for Core commands.")
        if not operator_id.strip():
            raise ValueError("Operator id cannot be empty.")
        object.__setattr__(self, "id", operator_id)
        object.__setattr__(self, "handler", handler)
        object.__setattr__(self, "contract", OperatorContract.from_callable(handler))
        object.__setattr__(self, "version", str(version))
