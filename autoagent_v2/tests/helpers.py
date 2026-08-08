"""Typed Workflow callables shared by V2 Core tests."""

from __future__ import annotations

from collections.abc import Iterator

from autoagent.core import ExecutionContext


def identity_int(value: int) -> int:
    return value


def identity_str(value: str) -> str:
    return value


def increment(value: int) -> int:
    return value + 1


def double(value: int) -> int:
    return value * 2


def uppercase(value: str) -> str:
    return value.upper()


def context_input_int(context: ExecutionContext) -> int:
    return int(context.invocation_input)


def always_true(_context: ExecutionContext) -> bool:
    return True


def always_false(_context: ExecutionContext) -> bool:
    return False


def text_stream(value: str) -> Iterator[str]:
    return iter(value)


class TextReducer:
    def __init__(self) -> None:
        self._parts: list[str] = []

    def add(self, chunk: str) -> None:
        self._parts.append(chunk)

    def finish(self) -> str:
        return "".join(self._parts)
