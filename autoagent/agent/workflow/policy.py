"""Versioned Context policies; callbacks run inside ordinary Agent Operators."""

from __future__ import annotations

from dataclasses import dataclass, field
import inspect
import json
import math
from typing import Callable

from autoagent.core.operators import run_sync

from .history import ContextView
from .models import ContextPreparation, TokenEstimate


class ContextLimitExceeded(ValueError):
    """The selected Context cannot fit without losing required information."""


@dataclass(frozen=True)
class ContextBudget:
    input_limit: int | None
    trigger_tokens: int | None
    target_tokens: int | None


def item_characters(item):
    """Count model content, including opaque replay text, without native duplicates."""
    value = item.model_dump(mode="json", exclude={"native", "id", "tool_metadata"}, exclude_none=True)
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")))


def fixed_characters(preparation):
    from .projection import project_items
    from .runtime_context import time_characters
    return sum(item_characters(item) for item in project_items(preparation.fixed_input, preparation.replay_target)) + (
        len(json.dumps([tool.model_dump(mode="json", exclude_none=True) for tool in preparation.tools],
                       ensure_ascii=False, separators=(",", ":"))) if preparation.tools else 0) + time_characters(preparation)


@dataclass(frozen=True)
class UsagePlusCharacters:
    chars_per_token: float = 4

    def __post_init__(self):
        if isinstance(self.chars_per_token, bool) or not math.isfinite(self.chars_per_token) or self.chars_per_token <= 0:
            raise ValueError("chars_per_token must be finite and positive.")

    def __call__(self, preparation: ContextPreparation) -> TokenEstimate:
        from .projection import project_history
        history = preparation.conversation
        anchor = history.token_anchor
        if (anchor is not None and anchor.fingerprint == preparation.fingerprint
                and anchor.view_generation == history.view.generation):
            trailing = sum(item_characters(item) for item in project_history(
                history, preparation.replay_target, start=anchor.source_count))
            from .runtime_context import time_characters
            trailing += time_characters(preparation)
            return TokenEstimate(input_tokens=anchor.input_tokens + math.ceil(trailing / self.chars_per_token), anchored=True)
        size = fixed_characters(preparation) + sum(item_characters(item) for item in project_history(history, preparation.replay_target))
        return TokenEstimate(input_tokens=math.ceil(size / self.chars_per_token))


@dataclass(frozen=True)
class ContextEvaluation:
    preparation: ContextPreparation
    estimate: TokenEstimate
    budget: ContextBudget


def over_context_limit(value: ContextEvaluation) -> bool:
    threshold = value.budget.trigger_tokens
    return threshold is not None and value.estimate.input_tokens >= threshold


def always_compact(value: ContextEvaluation) -> bool:
    return True


async def call_policy(handler, value):
    if inspect.iscoroutinefunction(handler) or inspect.iscoroutinefunction(getattr(handler, "__call__", None)):
        result = handler(value)
    else:
        result = await run_sync(handler, value)
    return await result if inspect.isawaitable(result) else result


@dataclass(frozen=True)
class CompactionResult:
    view: ContextView


@dataclass(frozen=True)
class CompactionInput:
    preparation: ContextPreparation
    policy: ContextPolicy
    budget: ContextBudget
    estimate_before: TokenEstimate

    @property
    def conversation(self):
        return self.preparation.conversation

    @property
    def view(self):
        return self.conversation.view

    async def estimate(self, view: ContextView) -> TokenEstimate:
        from .history import Conversation
        candidate = Conversation(items=self.conversation.items, view=view, origins=self.conversation.origins,
            runtime_times=self.conversation.runtime_times if view == self.view else {})
        return await self.policy.estimate_input(self.preparation.model_copy(update={"conversation": candidate}))

    def over_limit(self, estimate: TokenEstimate) -> bool:
        threshold = self.budget.trigger_tokens if self.budget.trigger_tokens is not None else self.budget.input_limit
        return threshold is not None and estimate.input_tokens >= threshold

    async def summarize(self, view: ContextView) -> CompactionResult:
        from .compaction import LLMHistoryCompactor
        from .history import Conversation
        preparation = self.preparation.model_copy(update={"conversation": Conversation(
            items=self.conversation.items, view=view, origins=self.conversation.origins,
            runtime_times=self.conversation.runtime_times if view == self.view else {})})
        return await LLMHistoryCompactor()(CompactionInput(preparation, self.policy, self.budget,
                                                        await self.policy.estimate_input(preparation)))


def _default_compactor():
    from .compaction import LLMHistoryCompactor
    return LLMHistoryCompactor()


@dataclass(frozen=True)
class ContextPolicy:
    """Absolute thresholds override ratios; targets are best-effort, not quotas.

    Custom callbacks must change id/version when their behavior or captured
    configuration changes. Runtime clients and callbacks never enter Session data.
    """

    id: str = "agent.context"
    version: str = "1"
    context_window: int | None = None
    compact_at_ratio: float = 0.8
    compact_at_tokens: int | None = None
    target_ratio: float = 0.5
    target_tokens: int | None = None
    estimate: Callable = field(default_factory=UsagePlusCharacters)
    trigger: Callable = over_context_limit
    compact: Callable = field(default_factory=_default_compactor)

    def __post_init__(self):
        if any(not isinstance(value, str) or not value.strip() for value in (self.id, self.version)):
            raise ValueError("Context policy id and version must be nonempty strings.")
        for name in ("context_window", "compact_at_tokens", "target_tokens"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value <= 0):
                raise ValueError(f"{name} must be a positive integer or None.")
        for name in ("compact_at_ratio", "target_ratio"):
            value = getattr(self, name)
            if isinstance(value, bool) or not math.isfinite(value) or not 0 < value <= 1:
                raise ValueError(f"{name} must be in (0, 1].")
        if any(not callable(getattr(self, name)) for name in ("estimate", "trigger", "compact")):
            raise TypeError("Context estimate, trigger and compact must be callable.")
        self.budget({})

    def budget(self, settings) -> ContextBudget:
        window = self.context_window
        limit = window - (settings.get("max_output_tokens") or 0) if window is not None else None
        if limit is not None and limit <= 0:
            raise ContextLimitExceeded("The configured output limit leaves no input Context space.")
        threshold = self.compact_at_tokens
        if threshold is None and window is not None:
            threshold = max(1, math.floor(window * self.compact_at_ratio))
        target = self.target_tokens
        if target is None and window is not None:
            target = max(1, math.floor(window * self.target_ratio))
        if threshold is not None and target is not None and target >= threshold:
            raise ValueError("The Context target must be below its compaction threshold.")
        if limit is not None:
            threshold = min(threshold, limit) if threshold is not None else limit
            target = min(target, max(1, threshold - 1)) if target is not None else max(1, threshold - 1)
        return ContextBudget(limit, threshold, target)

    async def estimate_input(self, preparation) -> TokenEstimate:
        value = self.estimate(preparation) if isinstance(self.estimate, UsagePlusCharacters) else await call_policy(self.estimate, preparation)
        if not isinstance(value, TokenEstimate):
            raise TypeError("Context estimator must return TokenEstimate.")
        return TokenEstimate.model_validate(value)

    def identity(self):
        def identify(callback):
            if hasattr(callback, "identity"):
                return callback.identity()
            if isinstance(callback, UsagePlusCharacters):
                return {"type": "usage_plus_characters", "chars_per_token": callback.chars_per_token}
            return {"callable": getattr(callback, "__module__", type(callback).__module__) + "." + getattr(callback, "__qualname__", type(callback).__qualname__)}
        return {**{key: getattr(self, key) for key in (
            "id", "version", "context_window", "compact_at_ratio", "compact_at_tokens", "target_ratio", "target_tokens")},
            **{key: identify(getattr(self, key)) for key in ("estimate", "trigger", "compact")}}


__all__ = ["ContextPolicy", "ContextBudget", "ContextEvaluation", "CompactionInput", "CompactionResult",
           "ContextLimitExceeded", "UsagePlusCharacters", "over_context_limit", "always_compact"]
