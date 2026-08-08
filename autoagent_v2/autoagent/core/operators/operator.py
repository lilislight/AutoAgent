"""Concrete and framework-owned Operator bindings embedded in Workflow IR."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any

from .contract import OperatorContract, ValueContract


def callable_id(handler: Callable[..., object]) -> str:
    target = handler.func if isinstance(handler, partial) else handler
    name = (
        getattr(target, "__name__", None)
        or getattr(target, "__qualname__", None)
        or target.__class__.__name__
    )
    return str(name)


@dataclass(frozen=True, slots=True, init=False)
class Operator:
    handler: Callable[..., object]
    id: str
    version: str | int
    contract: OperatorContract = field(repr=False, compare=False)

    def __init__(
        self,
        handler: Callable[..., object],
        *,
        id: str | None = None,
        version: str | int = 1,
    ) -> None:
        if not callable(handler):
            raise TypeError("Operator handler must be callable.")
        operator_id = id or callable_id(handler)
        if not operator_id.strip():
            raise ValueError("Operator id cannot be empty.")
        try:
            contract = OperatorContract.from_callable(handler)
        except (TypeError, ValueError) as error:
            raise TypeError(f"Operator contract is invalid: {error}") from error
        object.__setattr__(self, "handler", handler)
        object.__setattr__(self, "id", operator_id)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "contract", contract)

    @classmethod
    def from_callable(
        cls,
        handler: Callable[..., object],
        *,
        operator_id: str | None = None,
        version: str | int = 1,
    ) -> "Operator":
        return cls(handler, id=operator_id, version=version)

    @property
    def signature(self) -> inspect.Signature:
        return inspect.signature(self.handler)


@dataclass(frozen=True, slots=True)
class WaitOperator:
    """Framework-owned suspension binding with explicit request/response types."""

    request_type: object
    response_type: object
    id: str = field(default="autoagent.wait", init=False)

    @property
    def request_contract(self) -> ValueContract:
        return ValueContract.create(
            self.request_type, location="WaitOperator request_type"
        )

    @property
    def response_contract(self) -> ValueContract:
        return ValueContract.create(
            self.response_type, location="WaitOperator response_type"
        )
