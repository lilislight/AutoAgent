"""Token queries and project-wide windows; all ranges are start-inclusive/end-exclusive."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone as utc_timezone
from types import MappingProxyType
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import Field

from autoagent.core.usage import UsagePolicy, UsageAccountingError, ResourceBudgetExceeded
from autoagent.core.money import usd_units
from .models._base import Model

TOKEN_FIELDS = ("input_tokens", "output_tokens", "total_tokens", "reasoning_tokens", "cache_read_tokens")


def timestamp_us(value: datetime | None) -> int | None:
    if value is None:
        return None
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Usage times must be timezone-aware datetimes.")
    delta = value.astimezone(utc_timezone.utc) - datetime(1970, 1, 1, tzinfo=utc_timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


@dataclass(frozen=True, slots=True)
class TimeRange:
    start: datetime | None = None
    end: datetime | None = None

    def __post_init__(self):
        start, end = timestamp_us(self.start), timestamp_us(self.end)
        if start is not None and end is not None and start >= end:
            raise ValueError("Usage range start must precede end.")

    @classmethod
    def day(cls, *, at: datetime | None = None, timezone: str = "UTC"):
        return UsageWindow("day", timezone).resolve(at or datetime.now(utc_timezone.utc))

    @classmethod
    def month(cls, *, at: datetime | None = None, timezone: str = "UTC"):
        return UsageWindow("month", timezone).resolve(at or datetime.now(utc_timezone.utc))

    @classmethod
    def last_24_hours(cls, *, at: datetime | None = None):
        end = at or datetime.now(utc_timezone.utc)
        timestamp_us(end)
        end = end.astimezone(utc_timezone.utc)
        return cls(end - timedelta(hours=24), end)


@dataclass(frozen=True, slots=True)
class UsageWindow:
    period: Literal["day", "month"]
    timezone: str = "UTC"

    def __post_init__(self):
        if self.period not in ("day", "month"):
            raise ValueError("Usage window period must be day or month.")
        ZoneInfo(self.timezone)

    def resolve(self, at: datetime) -> TimeRange:
        timestamp_us(at)
        current = at.astimezone(ZoneInfo(self.timezone))
        start = current.replace(hour=0, minute=0, second=0, microsecond=0)
        if self.period == "day":
            end = start + timedelta(days=1)
        else:
            start = start.replace(day=1)
            end = start.replace(year=start.year + 1, month=1) if start.month == 12 else start.replace(month=start.month + 1)
        return TimeRange(start, end)


@dataclass(frozen=True, slots=True)
class WindowUsagePolicy:
    window: UsageWindow | TimeRange
    limits: UsagePolicy

    def __post_init__(self):
        if not isinstance(self.window, (UsageWindow, TimeRange)) or not isinstance(self.limits, UsagePolicy):
            raise TypeError("Window usage requires a UsageWindow/TimeRange and UsagePolicy.")


@dataclass(frozen=True, slots=True)
class ProjectUsagePolicy:
    project: WindowUsagePolicy | None = None
    workflows: Mapping[str, WindowUsagePolicy] = field(default_factory=dict)

    def __post_init__(self):
        if self.project is not None and not isinstance(self.project, WindowUsagePolicy):
            raise TypeError("Project usage requires WindowUsagePolicy.")
        if any(not isinstance(key, str) or not key.strip() or not isinstance(value, WindowUsagePolicy)
               for key, value in self.workflows.items()):
            raise TypeError("Workflow usage maps Workflow IDs to WindowUsagePolicy.")
        object.__setattr__(self, "workflows", MappingProxyType(dict(self.workflows)))


@dataclass(frozen=True, slots=True)
class TokenUsageQuery:
    invocation_id: str | None = None
    node_id: str | None = None
    session_id: str | None = None
    workflow_id: str | None = None
    workflow_revision_id: str | None = None
    kind: Literal["generation", "embedding"] | None = None
    provider_id: str | None = None
    requested_model: str | None = None
    start: datetime | None = None
    end: datetime | None = None

    def __post_init__(self):
        TimeRange(self.start, self.end)
        for key in ("invocation_id", "node_id", "session_id", "workflow_id", "workflow_revision_id", "provider_id", "requested_model"):
            value = getattr(self, key)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{key} must be a nonempty string or None.")
        if self.node_id is not None and self.invocation_id is None:
            raise ValueError("Node usage requires invocation_id.")
        if self.kind not in (None, "generation", "embedding"):
            raise ValueError("Usage kind must be generation or embedding.")


class TokenUsageSummary(Model):
    call_count: int = Field(default=0, ge=0)
    pending_calls: int = Field(default=0, ge=0)
    unsettled_calls: int = Field(default=0, ge=0)
    missing_usage_calls: int = Field(default=0, ge=0)
    input_tokens: int | None = Field(default=0, ge=0)
    output_tokens: int | None = Field(default=0, ge=0)
    total_tokens: int | None = Field(default=0, ge=0)
    reasoning_tokens: int | None = Field(default=0, ge=0)
    cache_read_tokens: int | None = Field(default=0, ge=0)
    missing_tokens: dict[str, int] = Field(default_factory=lambda: dict.fromkeys(TOKEN_FIELDS, 0))
    cost_usd: str | None = "0"
    missing_cost_calls: int = Field(default=0, ge=0)


@dataclass(frozen=True, slots=True)
class TokenUsageCheck:
    scope: str
    policy: UsagePolicy
    query: TokenUsageQuery

    def validate(self, summary: TokenUsageSummary):
        for metric in ("input_tokens", "output_tokens", "total_tokens"):
            limit = getattr(self.policy, "max_" + metric)
            if limit is None:
                continue
            used = getattr(summary, metric)
            if (used or 0) >= limit:
                raise ResourceBudgetExceeded(self.scope, metric, used or 0, limit)
            if self.policy.on_unknown == "block" and (summary.unsettled_calls or summary.missing_tokens[metric]):
                raise UsageAccountingError(f"{self.scope} has unknown {metric} usage; admission blocked.")
        if self.policy.max_cost_usd is not None:
            used = summary.cost_usd or "0"
            if usd_units(used) >= usd_units(self.policy.max_cost_usd):
                raise ResourceBudgetExceeded(self.scope, "cost_usd", used, self.policy.max_cost_usd)
            if self.policy.on_unknown == "block" and (summary.unsettled_calls or summary.missing_cost_calls):
                raise UsageAccountingError(f"{self.scope} has unknown cost usage; admission blocked.")


def usage_checks(core, project: ProjectUsagePolicy | None, record) -> tuple[TokenUsageCheck, ...]:
    checks = []
    if core is not None:
        for scope, policy, ids in (
            ("node", core.node_usage, dict(invocation_id=record.invocation_id, node_id=record.node_id)),
            ("invocation", core.invocation_usage, dict(invocation_id=record.invocation_id)),
            ("session", core.session_usage, dict(session_id=record.session_id)),
        ):
            if policy is not None and policy.limited:
                if any(value is None for value in ids.values()):
                    raise UsageAccountingError(f"Missing {scope} identity for token admission.")
                checks.append(TokenUsageCheck(scope, policy, TokenUsageQuery(**ids)))
    if project is not None:
        at = datetime(1970, 1, 1, tzinfo=utc_timezone.utc) + timedelta(microseconds=record.started_at_us)
        for scope, policy in (("workflow", project.workflows.get(record.workflow_id)), ("project", project.project)):
            if policy is None or not policy.limits.limited:
                continue
            window = policy.window.resolve(at) if isinstance(policy.window, UsageWindow) else policy.window
            if window.start is not None and at < window.start or window.end is not None and at >= window.end:
                continue
            ids = dict(workflow_id=record.workflow_id) if scope == "workflow" else {}
            checks.append(TokenUsageCheck(scope, policy.limits, TokenUsageQuery(**ids, start=window.start, end=window.end)))
    return tuple(checks)


__all__ = ["TimeRange", "UsageWindow", "WindowUsagePolicy", "ProjectUsagePolicy", "TokenUsageQuery", "TokenUsageSummary"]
