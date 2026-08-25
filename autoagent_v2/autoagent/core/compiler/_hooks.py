"""Shared callable introspection for Workflow hooks."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import get_type_hints


@dataclass(frozen=True, slots=True)
class HookContract:
    """One bound signature and its resolved annotations."""

    target: object
    annotation_source: object
    signature: inspect.Signature
    hints: Mapping[str, object]


def resolve_hook_contract(handler: Callable[..., object]) -> HookContract:
    """Resolve callable objects and partials using their bound signature."""

    target = handler.func if isinstance(handler, partial) else handler
    annotation_source = (
        target
        if inspect.isfunction(target) or inspect.ismethod(target)
        else target.__call__
    )
    return HookContract(
        target=target,
        annotation_source=annotation_source,
        signature=inspect.signature(handler),
        hints=get_type_hints(annotation_source, include_extras=True),
    )


__all__ = ["HookContract", "resolve_hook_contract"]
