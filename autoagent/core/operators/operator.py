from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any

from autoagent.core.operators.contract import OperatorContract, callable_contract
from autoagent.core.operators.manifest import (
    OperatorManifest,
    RecoveryMode,
    callable_operator_id,
)


class Operator:
    """One concrete callable implementation registered with an application.

    Operator is a business object rather than a Pydantic model because it owns
    a live Python callable and mutable availability state. Registry controls its
    identity and capability relationship; NodeExecutor reads the object and
    invokes it but does not modify registration metadata.
    """

    def __init__(
        self,
        *,
        id: str,
        handler: Callable[..., Any],
        capability_id: str | None = None,
        version: str | int = 1,
        recovery_mode: RecoveryMode = "never",
        priority: int = 0,
        enabled: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        resolved_id = id.strip()
        if not resolved_id:
            raise ValueError("Operator id cannot be empty.")
        if not callable(handler):
            raise TypeError("Operator handler must be callable.")

        contract, issues = callable_contract(handler)
        errors = [issue.message for issue in issues if issue.severity == "error"]
        if errors:
            raise ValueError(" ".join(errors))

        resolved_capability_id = capability_id.strip() if capability_id else None
        if capability_id is not None and not resolved_capability_id:
            raise ValueError("Operator capability_id cannot be empty.")
        if isinstance(version, str) and not version.strip():
            raise ValueError("Operator version cannot be empty.")
        if recovery_mode not in {"never", "replay_safe", "idempotent"}:
            raise ValueError(f"Unsupported Operator recovery_mode: {recovery_mode}")

        self._id = resolved_id
        self._handler = handler
        self._capability_id = resolved_capability_id
        self._version = version
        self._recovery_mode = recovery_mode
        self._priority = priority
        self._enabled = enabled
        self._contract = contract
        self._metadata = dict(metadata or {})

    @property
    def id(self) -> str:
        return self._id

    @property
    def handler(self) -> Callable[..., Any]:
        return self._handler

    @property
    def capability_id(self) -> str | None:
        return self._capability_id

    @property
    def priority(self) -> int:
        return self._priority

    @property
    def version(self) -> str | int:
        return self._version

    @property
    def recovery_mode(self) -> RecoveryMode:
        """Crash behavior consumed by durable recovery, never normal retry."""

        return self._recovery_mode

    @property
    def manifest(self) -> OperatorManifest:
        """Return the immutable compatibility record persisted with execution."""

        return OperatorManifest.from_contract(
            operator_id=self.id,
            version=self.version,
            capability_id=self.capability_id,
            contract=self.contract,
            recovery_mode=self.recovery_mode,
        )

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def contract(self) -> OperatorContract:
        """Contract inferred only from this Operator's Python handler."""

        return self._contract

    @property
    def metadata(self) -> MappingProxyType[str, Any]:
        return MappingProxyType(self._metadata)

    @property
    def is_async(self) -> bool:
        """Whether the registered handler should use the async execution lane."""

        return inspect.iscoroutinefunction(self._handler) or inspect.iscoroutinefunction(
            getattr(self._handler, "__call__", None)
        )

    def enable(self) -> None:
        """Make this Operator eligible for future runtime selection."""

        self._enabled = True

    def disable(self) -> None:
        """Exclude this Operator from future calls without deleting its identity."""

        self._enabled = False

    def invoke(self, input: Any) -> Any:
        """Invoke the handler from a mapping of parameter names to values."""

        return _call_handler(self._handler, input)

    async def ainvoke(self, input: Any) -> Any:
        """Invoke the handler and await its result when it is awaitable."""

        result = _call_handler(self._handler, input)
        if inspect.isawaitable(result):
            return await result
        return result

    @classmethod
    def from_callable(
        cls,
        handler: Callable[..., Any],
        *,
        operator_id: str | None = None,
    ) -> Operator:
        """Create a virtual Operator using the same defaults as registration.

        Direct callables are not a separate recovery mechanism. Without an
        explicit operator_id they use module/qualified-name identity. Compiler
        supplies a node-stable binding id so distinct Callable objects become
        distinct Operators while repeated use of one object can reuse it.
        Direct Operators keep version 1 and ``never`` recovery unless the
        developer binds an explicit Operator object with different metadata.
        """

        return cls(id=operator_id or callable_operator_id(handler), handler=handler)


def _call_handler(handler: Callable[..., Any], input: Any) -> Any:
    if not isinstance(input, Mapping):
        raise TypeError(
            "Operator input must be a mapping whose keys match handler parameters."
        )
    arguments = dict(input)
    signature = inspect.signature(handler)
    signature.bind(**arguments)
    return handler(**arguments)
