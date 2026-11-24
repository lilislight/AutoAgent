"""Session-scoped task plans, committed by the Agent Workflow with Tool results."""

from typing import Annotated, Literal

from pydantic import Field, JsonValue, model_validator

from .models._base import Model, NonEmpty
from .tools import Tool, ToolSet, tool
from .tools.group import ToolGroup


class PlanStep(Model):
    step: Annotated[NonEmpty, Field(max_length=500)]
    status: Literal["pending", "in_progress", "completed"]


class SessionPlan(Model):
    plan: tuple[PlanStep, ...] = Field(min_length=1, max_length=64)
    explanation: str | None = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def _active_step(self):
        if sum(item.status == "in_progress" for item in self.plan) > 1:
            raise ValueError("A plan can have at most one in_progress step.")
        return self


class _PlanTool(Tool):
    """An implementation identity, never inferred from a model-supplied name."""


def is_plan_tool(value):
    while getattr(value, "_owner", None) is not None:
        value = value._owner._tools[value.definition.name]
    return isinstance(value, _PlanTool)


class PlanTools(ToolGroup):
    """Bind this group to opt into plans; SDK callers own their ToolPolicy."""

    group_id = "planning"

    def __init__(self, *, policy=None):
        @tool
        def update_plan(plan: Annotated[list[PlanStep], Field(max_length=64)],
                        explanation: Annotated[str | None, Field(max_length=2000)] = None) -> dict[str, JsonValue]:
            """Create or replace the current Session plan with the full ordered step list.

            Use for multi-step work, not simple questions. Update it at meaningful
            milestones; at most one step may be in_progress. Completed plans stay
            available until replaced. Send plan: [] to clear the plan. Submit at
            most one update_plan call per response. Planning does not authorize
            work the user has not requested.
            """
            if not plan:
                return {"plan": None}
            try:
                value = SessionPlan(plan=tuple(plan), explanation=explanation)
            except ValueError as error:
                from .tools import ToolBusinessError
                raise ToolBusinessError("A plan can have at most one in_progress step.", code="invalid_plan") from error
            return {"plan": value.model_dump(mode="json")}

        self._tools = ToolSet((_PlanTool(update_plan),), policy=policy)
        self._bind_group()


__all__ = ["PlanStep", "SessionPlan", "PlanTools"]
