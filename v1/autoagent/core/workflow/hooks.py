from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar


F = TypeVar("F", bound=Callable[..., Any])
HookVersion = str | int
_HOOK_VERSION_ATTRIBUTE = "__autoagent_workflow_hook_version__"


def workflow_hook(*, version: HookVersion) -> Callable[[F], F]:
    """Attach an explicit semantic version to an executable Workflow hook.

    The decorator returns the original callable, so its signature and async
    behavior remain unchanged. Compiler includes the version in
    ``definition_hash`` for input mappings, output bindings, edge conditions,
    map selectors, and output aggregators.
    """

    resolved_version = _validate_hook_version(version)

    def decorate(handler: F) -> F:
        try:
            setattr(handler, _HOOK_VERSION_ATTRIBUTE, resolved_version)
        except (AttributeError, TypeError) as exc:
            raise TypeError(
                "workflow_hook requires a callable that supports attributes."
            ) from exc
        return handler

    return decorate


def get_workflow_hook_version(handler: Any) -> HookVersion | None:
    """Return the version attached by ``workflow_hook``, if one exists."""

    value = getattr(handler, _HOOK_VERSION_ATTRIBUTE, None)
    return _validate_hook_version(value) if value is not None else None


def _validate_hook_version(value: HookVersion) -> HookVersion:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise TypeError("Workflow hook version must be a string or integer.")
    if isinstance(value, str) and not value.strip():
        raise ValueError("Workflow hook version cannot be empty.")
    return value
