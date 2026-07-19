from __future__ import annotations

import warnings
from collections import defaultdict
from threading import RLock

from autoagent.core.operators.capability import Capability
from autoagent.core.operators.contract import (
    OperatorContract,
    OperatorContractWarning,
    compare_contracts,
)
from autoagent.core.operators.operator import Operator


class CapabilityRegistry:
    """Application-local registry of abstract capability contracts.

    Registration is thread-safe and duplicate ids are rejected. Capability
    replacement is intentionally unsupported because changing a contract could
    invalidate already compiled Workflow IR and existing runtime inputs.
    """

    def __init__(self) -> None:
        self._items: dict[str, Capability] = {}
        self._lock = RLock()

    def register(self, capability: Capability) -> Capability:
        with self._lock:
            if capability.id in self._items:
                raise ValueError(f"Capability already registered: {capability.id}")
            self._items[capability.id] = capability
        return capability

    def get(self, capability_id: str) -> Capability | None:
        with self._lock:
            return self._items.get(capability_id)

    def contains(self, capability_id: str) -> bool:
        with self._lock:
            return capability_id in self._items

    def values(self) -> tuple[Capability, ...]:
        with self._lock:
            return tuple(self._items.values())

    def bind_contract(
        self,
        capability_id: str,
        contract: OperatorContract,
    ) -> None:
        """Establish a Capability contract from its first registered Operator."""

        with self._lock:
            capability = self._items.get(capability_id)
            if capability is None:
                raise ValueError(f"Unknown Capability: {capability_id}")
            capability._bind_contract(contract)


class OperatorRegistry:
    """Application-local Operator store with a capability-to-operator index.

    An Operator may be standalone, in which case only OperatorRef can select
    it. When capability_id is present, registration requires that Capability to
    exist. Registration rejects only structural contract mismatches that make
    named invocation impossible; annotation and portability uncertainty emit an
    OperatorContractWarning. Operators keep stable insertion order inside each
    Capability. Runtime selection breaks ties by stable Operator id instead of
    depending on this insertion order.
    """

    def __init__(self, capability_registry: CapabilityRegistry) -> None:
        self._capability_registry = capability_registry
        self._items: dict[str, Operator] = {}
        self._by_capability: dict[str, list[str]] = defaultdict(list)
        self._default_by_capability: dict[str, str] = {}
        self._lock = RLock()

    def register(self, operator: Operator, *, default: bool = False) -> Operator:
        with self._lock:
            if operator.id in self._items:
                raise ValueError(f"Operator already registered: {operator.id}")

            capability = None
            if operator.capability_id is not None:
                capability = self._capability_registry.get(operator.capability_id)
                if capability is None:
                    raise ValueError(
                        f"Operator references an unknown capability: {operator.capability_id}"
                    )

            if default and operator.capability_id is None:
                raise ValueError("A default Operator must implement a Capability.")
            if (
                default
                and operator.capability_id is not None
                and operator.capability_id in self._default_by_capability
            ):
                raise ValueError(
                    "Capability already has a default Operator: "
                    f"{operator.capability_id}"
                )

            if capability is not None:
                if capability.contract is None:
                    self._capability_registry.bind_contract(
                        capability.id,
                        operator.contract,
                    )
                else:
                    _validate_contract(capability, operator)

            self._items[operator.id] = operator
            if operator.capability_id is not None:
                self._by_capability[operator.capability_id].append(operator.id)
                if default:
                    self._default_by_capability[operator.capability_id] = operator.id
        return operator

    def get(self, operator_id: str) -> Operator | None:
        with self._lock:
            return self._items.get(operator_id)

    def contains(self, operator_id: str) -> bool:
        with self._lock:
            return operator_id in self._items

    def for_capability(
        self,
        capability_id: str,
        *,
        include_disabled: bool = False,
    ) -> tuple[Operator, ...]:
        with self._lock:
            operators = tuple(
                self._items[operator_id]
                for operator_id in self._by_capability.get(capability_id, ())
            )
        if include_disabled:
            return operators
        return tuple(operator for operator in operators if operator.enabled)

    def default_for_capability(self, capability_id: str) -> Operator | None:
        with self._lock:
            operator_id = self._default_by_capability.get(capability_id)
            if operator_id is None:
                return None
            return self._items.get(operator_id)

    def values(self) -> tuple[Operator, ...]:
        with self._lock:
            return tuple(self._items.values())


def _validate_contract(capability: Capability, operator: Operator) -> None:
    if capability.contract is None:
        raise ValueError(f"Capability has no established contract: {capability.id}")
    issues = compare_contracts(capability.contract, operator.contract)
    errors = [issue.message for issue in issues if issue.severity == "error"]
    if errors:
        raise ValueError(
            f"Operator {operator.id} does not match Capability "
            f"{capability.id}: {' '.join(errors)}"
        )
    for issue in issues:
        if issue.severity == "warning":
            warnings.warn(
                f"Operator {operator.id} / Capability {capability.id}: {issue.message}",
                OperatorContractWarning,
                stacklevel=3,
            )
