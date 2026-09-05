"""Stable semantic versions for user-defined Workflow hooks."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar


HookVersion = str | int
F = TypeVar("F", bound=Callable[..., object])
_HOOK_VERSION_ATTRIBUTE = "__autoagent_workflow_hook_version__"


def workflow_hook(*, version: HookVersion) -> Callable[[F], F]:
    """Attach an explicit semantic version without wrapping the callable."""

    resolved = _validate_version(version)

    def decorate(handler: F) -> F:
        try:
            setattr(handler, _HOOK_VERSION_ATTRIBUTE, resolved)
        except (AttributeError, TypeError) as error:
            raise TypeError(
                "workflow_hook requires a callable that supports attributes."
            ) from error
        return handler

    return decorate


def workflow_hook_version(handler: object) -> HookVersion | None:
    """Return the explicit hook version, if the callable declares one."""

    value = getattr(handler, _HOOK_VERSION_ATTRIBUTE, None)
    return _validate_version(value) if value is not None else None


def _validate_version(value: object) -> HookVersion:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise TypeError("Workflow hook version must be a string or integer.")
    if isinstance(value, str) and not value.strip():
        raise ValueError("Workflow hook version cannot be empty.")
    return value


__all__ = ["HookVersion", "workflow_hook", "workflow_hook_version"]
