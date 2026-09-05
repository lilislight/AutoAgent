from __future__ import annotations

from collections.abc import Callable
import inspect
from typing import Any


class Evaluation:
    """Base class for one Manifest-registered Workflow evaluation.

    Every asynchronous ``eval_*`` method defines one isolated Eval Case. The
    Runner supplies its ``EvalCase`` argument; authors do not instantiate or
    register an App from evaluation code.
    """


def evaluation_case_methods(
    evaluation_type: type[Evaluation],
) -> tuple[tuple[str, Callable[..., Any]], ...]:
    """Discover Eval Cases in deterministic class-definition order."""

    methods: dict[str, Callable[..., Any]] = {}
    for base in reversed(evaluation_type.__mro__):
        for name, value in vars(base).items():
            if not name.startswith("eval_"):
                continue
            if callable(value):
                methods[name] = value
            else:
                methods.pop(name, None)
    return tuple(methods.items())
