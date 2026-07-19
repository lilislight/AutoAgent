from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from autoagent.core.operators.manifest import RecoveryMode


F = TypeVar("F", bound=Callable[..., Any])


def capability(
    capability_id: str | None = None,
    *,
    operator_id: str | None = None,
    description: str | None = None,
    version: str | int = 1,
    recovery_mode: RecoveryMode = "never",
    priority: int = 0,
    metadata: dict[str, Any] | None = None,
) -> Callable[[F], F]:
    """Register a Capability and its default Operator on the default App."""

    from autoagent.core.app.default import get_default_app

    return get_default_app().capability(
        capability_id,
        operator_id=operator_id,
        description=description,
        version=version,
        recovery_mode=recovery_mode,
        priority=priority,
        metadata=metadata,
    )


def operator(
    operator_id: str | None = None,
    *,
    capability: str | None = None,
    version: str | int = 1,
    recovery_mode: RecoveryMode = "never",
    priority: int = 0,
    enabled: bool = True,
    metadata: dict[str, Any] | None = None,
) -> Callable[[F], F]:
    """Register an Operator on the lazily created default App."""

    from autoagent.core.app.default import get_default_app

    return get_default_app().operator(
        operator_id,
        capability=capability,
        version=version,
        recovery_mode=recovery_mode,
        priority=priority,
        enabled=enabled,
        metadata=metadata,
    )
