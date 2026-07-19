from __future__ import annotations

from typing import Any

from autoagent.core.operators.operator import Operator
from autoagent.core.operators.registry import CapabilityRegistry, OperatorRegistry
from autoagent.core.workflow.capability import CapabilityRef, OperatorRef, SystemCommand
from autoagent.core.workflow.policy import CapabilitySelectionPolicy


class OperatorResolutionError(Exception):
    """Stable resolution failure returned by NodeExecutor as a runtime error."""

    def __init__(self, code: str, message: str, detail: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = dict(detail or {})


class OperatorResolver:
    """Resolve a compiled node binding into one concrete executable Operator.

    Direct Operator and legacy Callable bindings bypass registries. OperatorRef
    resolves one exact registered implementation. CapabilityRef obtains all
    enabled implementations and applies the node's CapabilitySelectionPolicy
    for every NodeExecution, preserving late binding after compilation.
    """

    def __init__(
        self,
        capability_registry: CapabilityRegistry,
        operator_registry: OperatorRegistry,
    ) -> None:
        self.capability_registry = capability_registry
        self.operator_registry = operator_registry

    def resolve(
        self,
        binding: Any,
        policy: CapabilitySelectionPolicy | None = None,
    ) -> Operator:
        return self.resolve_candidates(binding, policy)[0]

    def resolve_candidates(
        self,
        binding: Any,
        policy: CapabilitySelectionPolicy | None = None,
    ) -> tuple[Operator, ...]:
        """Return candidates in execution order for primary/fallback calls."""

        if isinstance(binding, Operator):
            if not binding.enabled:
                raise OperatorResolutionError(
                    "OPERATOR_UNAVAILABLE",
                    f"Operator is disabled: {binding.id}",
                    {"operator_id": binding.id},
                )
            return (binding,)

        if callable(binding):
            return (Operator.from_callable(binding),)

        if isinstance(binding, OperatorRef):
            operator = self.operator_registry.get(binding.id)
            if operator is None:
                raise OperatorResolutionError(
                    "OPERATOR_NOT_REGISTERED",
                    f"Operator is not registered: {binding.id}",
                    {"operator_id": binding.id},
                )
            if not operator.enabled:
                raise OperatorResolutionError(
                    "OPERATOR_UNAVAILABLE",
                    f"Operator is disabled: {binding.id}",
                    {"operator_id": binding.id},
                )
            return (operator,)

        if isinstance(binding, CapabilityRef):
            if not self.capability_registry.contains(binding.id):
                raise OperatorResolutionError(
                    "CAPABILITY_NOT_REGISTERED",
                    f"Capability is not registered: {binding.id}",
                    {"capability_id": binding.id},
                )
            candidates = list(self.operator_registry.for_capability(binding.id))
            if not candidates:
                raise OperatorResolutionError(
                    "CAPABILITY_UNAVAILABLE",
                    f"Capability has no enabled Operator: {binding.id}",
                    {"capability_id": binding.id},
                )
            ordered = self._order(binding.id, candidates, policy)
            effective = policy or CapabilitySelectionPolicy()
            return ordered if effective.allow_fallback else ordered[:1]

        if isinstance(binding, SystemCommand):
            raise OperatorResolutionError(
                "SYSTEM_COMMAND_UNSUPPORTED",
                "SystemCommand requires a SystemCommandExecutor.",
                {"command_id": binding.id},
            )

        raise OperatorResolutionError(
            "UNSUPPORTED_CAPABILITY",
            f"Unsupported compiled capability: {type(binding).__name__}",
        )

    def _order(
        self,
        capability_id: str,
        candidates: list[Operator],
        policy: CapabilitySelectionPolicy | None,
    ) -> tuple[Operator, ...]:
        effective = policy or CapabilitySelectionPolicy()
        excluded = set(effective.excluded_operator_ids)
        candidates = [candidate for candidate in candidates if candidate.id not in excluded]
        if not candidates:
            raise OperatorResolutionError(
                "CAPABILITY_UNAVAILABLE",
                f"Selection policy excluded every Operator for: {capability_id}",
                {"capability_id": capability_id},
            )

        preferred_order = {
            operator_id: index
            for index, operator_id in enumerate(effective.preferred_operator_ids)
        }

        if effective.mode == "priority":
            candidates.sort(
                key=lambda candidate: (
                    candidate.id not in preferred_order,
                    preferred_order.get(candidate.id, 0),
                    -candidate.priority,
                    candidate.id,
                )
            )
            return tuple(candidates)

        if effective.mode == "first_available":
            candidates.sort(
                key=lambda candidate: (
                    candidate.id not in preferred_order,
                    preferred_order.get(candidate.id, 0),
                    candidate.id,
                )
            )
            return tuple(candidates)

        if effective.mode == "default":
            preferred = [
                candidate
                for operator_id in effective.preferred_operator_ids
                for candidate in candidates
                if candidate.id == operator_id
            ]
            if preferred:
                preferred_ids = {candidate.id for candidate in preferred}
                remaining = [
                    candidate for candidate in candidates
                    if candidate.id not in preferred_ids
                ]
                default = self.operator_registry.default_for_capability(capability_id)
                default_candidate = None
                if (
                    default is not None
                    and default.enabled
                    and default.id not in excluded
                    and default.id not in preferred_ids
                ):
                    default_candidate = default
                    remaining = [
                        candidate for candidate in remaining if candidate.id != default.id
                    ]
                remaining.sort(key=lambda candidate: (-candidate.priority, candidate.id))
                return tuple(
                    [
                        *preferred,
                        *([default_candidate] if default_candidate is not None else []),
                        *remaining,
                    ]
                )
            default = self.operator_registry.default_for_capability(capability_id)
            if default is not None and default.enabled and default.id not in excluded:
                remaining = [candidate for candidate in candidates if candidate.id != default.id]
                remaining.sort(key=lambda candidate: (-candidate.priority, candidate.id))
                return tuple([default, *remaining])
            candidates.sort(key=lambda candidate: (-candidate.priority, candidate.id))
            return tuple(candidates)

        raise OperatorResolutionError(
            "SELECTION_MODE_UNSUPPORTED",
            f"Operator selection mode is not implemented: {effective.mode}",
            {"capability_id": capability_id, "mode": effective.mode},
        )
