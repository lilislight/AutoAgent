"""Declarative token limits and non-retryable admission failures."""

from dataclasses import dataclass
from typing import Literal
from .money import usd_units


class UsageAccountingError(RuntimeError):
    """Admission cannot establish trustworthy usage; no request was sent."""


class ResourceBudgetExceeded(UsageAccountingError):
    """A configured scope has exhausted its reported token allowance."""

    def __init__(self, scope: str, metric: str, used: int | str, limit: int | str):
        self.scope, self.metric, self.used, self.limit = scope, metric, used, limit
        super().__init__(f"{scope} {metric} budget exhausted: used={used}, limit={limit}.")


@dataclass(frozen=True, slots=True)
class UsagePolicy:
    """Stop new requests at the settled limit; in-flight calls may exceed it."""

    max_input_tokens: int | None = None
    max_output_tokens: int | None = None
    max_total_tokens: int | None = None
    on_unknown: Literal["block", "allow"] = "block"
    max_cost_usd: str | None = None

    def __post_init__(self):
        for name in ("max_input_tokens", "max_output_tokens", "max_total_tokens"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"{name} must be a nonnegative integer or None.")
        if self.on_unknown not in ("block", "allow"):
            raise ValueError("on_unknown must be 'block' or 'allow'.")
        if self.max_cost_usd is not None:
            usd_units(self.max_cost_usd)

    @property
    def limited(self) -> bool:
        return any(value is not None for value in (self.max_input_tokens, self.max_output_tokens, self.max_total_tokens, self.max_cost_usd))


@dataclass(frozen=True, slots=True)
class WorkflowResourcePolicy:
    invocation: UsagePolicy | None = None
    session: UsagePolicy | None = None

    def __post_init__(self):
        for value in (self.invocation, self.session):
            if value is not None and not isinstance(value, UsagePolicy):
                raise TypeError("Workflow usage limits must be UsagePolicy.")


__all__ = ["UsagePolicy", "WorkflowResourcePolicy", "ResourceBudgetExceeded", "UsageAccountingError"]
