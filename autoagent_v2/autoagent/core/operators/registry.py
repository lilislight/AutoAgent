"""Application-local Capability and Operator registries.

The registry is a Runtime resource. Adding an implementation does not mutate a
compiled Workflow revision; every implementation must preserve the Capability's
nominal contract.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from threading import RLock

from .operator import Operator


@dataclass(frozen=True, slots=True)
class OperatorRegistration:
    operator: Operator
    capability_id: str
    priority: int = 0
    enabled: bool = True
    default: bool = False


class OperatorRegistry:
    """Thread-safe index of additional implementations for Capabilities."""

    def __init__(self) -> None:
        self._items: dict[str, OperatorRegistration] = {}
        self._by_capability: dict[str, list[str]] = {}
        self._defaults: dict[str, str] = {}
        self._contracts: dict[str, object] = {}
        self._lock = RLock()

    def bind_capability(self, capability) -> None:
        self.bind_capabilities((capability,))

    def bind_capabilities(self, capabilities) -> None:
        """Validate and bind one Workflow closure without partial mutation."""

        with self._lock:
            pending = dict(self._contracts)
            for capability in capabilities:
                contract = capability.contract
                previous = pending.get(capability.id)
                if previous is not None and not _same_contract(previous, contract):
                    raise ValueError(
                        f"Capability {capability.id!r} was rebound with another contract."
                    )
                for operator_id in self._by_capability.get(capability.id, ()):
                    registration = self._items[operator_id]
                    _require_contract(
                        capability.id,
                        contract,
                        registration.operator.contract,
                    )
                pending[capability.id] = contract
            self._contracts = pending

    def register(
        self,
        operator: Operator,
        *,
        capability_id: str,
        priority: int = 0,
        enabled: bool = True,
        default: bool = False,
    ) -> Operator:
        resolved = capability_id.strip()
        if not resolved:
            raise ValueError("Operator capability_id cannot be empty.")
        with self._lock:
            if operator.id in self._items:
                raise ValueError(f"Operator already registered: {operator.id}")
            if default and resolved in self._defaults:
                raise ValueError(f"Capability {resolved!r} already has a default Operator.")
            contract = self._contracts.get(resolved)
            if contract is not None:
                _require_contract(resolved, contract, operator.contract)
            registration = OperatorRegistration(
                operator, resolved, priority, enabled, default
            )
            self._items[operator.id] = registration
            self._by_capability.setdefault(resolved, []).append(operator.id)
            if default:
                self._defaults[resolved] = operator.id
        return operator

    def get(self, operator_id: str) -> OperatorRegistration | None:
        with self._lock:
            return self._items.get(operator_id)

    def for_capability(
        self, capability_id: str, *, include_disabled: bool = False
    ) -> tuple[OperatorRegistration, ...]:
        with self._lock:
            values = tuple(
                self._items[operator_id]
                for operator_id in self._by_capability.get(capability_id, ())
            )
        if include_disabled:
            return values
        return tuple(value for value in values if value.enabled)

    def default_for_capability(self, capability_id: str) -> Operator | None:
        with self._lock:
            operator_id = self._defaults.get(capability_id)
            registration = self._items.get(operator_id) if operator_id else None
            if registration is None or not registration.enabled:
                return None
            return registration.operator

    def set_enabled(self, operator_id: str, enabled: bool) -> None:
        with self._lock:
            registration = self._items.get(operator_id)
            if registration is None:
                raise KeyError(operator_id)
            self._items[operator_id] = replace(registration, enabled=enabled)


def _same_contract(left, right) -> bool:
    return (
        left.input.same_as(right.input)
        and _same_optional(left.output, right.output)
        and _same_optional(left.stream_chunk, right.stream_chunk)
    )


def _same_optional(left, right) -> bool:
    if left is None or right is None:
        return left is right
    return left.same_as(right)


def _require_contract(capability_id: str, expected, actual) -> None:
    if not _same_contract(expected, actual):
        raise ValueError(
            f"Operator contract does not match Capability {capability_id!r}."
        )


__all__ = ["OperatorRegistration", "OperatorRegistry"]
