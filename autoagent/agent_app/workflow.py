"""Workflow assembly and identity owned by the Agent application."""

from autoagent.agent.workflow.builder import agent_workflow
from autoagent.agent.workflow.policy import ContextPolicy
from autoagent.agent.workflow.subagents import SubagentsConfig, SubagentTarget

BUILTIN_AGENT_WORKFLOW_ID = "autoagent.agent"


def builtin_agent_workflow(*, execution=True):
    """Construct the product Agent; project resources require explicit grants."""
    workflow = agent_workflow(
        workflow_id=BUILTIN_AGENT_WORKFLOW_ID,
        context_policy=ContextPolicy(context_window=200_000),
        execution=execution,
        user_events=True,
        include_ask_user=True,
        subagents=SubagentsConfig(enabled=True, targets={
            "agent": SubagentTarget(workflow_id=BUILTIN_AGENT_WORKFLOW_ID,
                description="Delegate a task to another instance of this Agent."),
        }),
    )
    start = next(node for node in workflow.nodes if node.id == "start")
    start.executable.handler.__autoagent_builtin_chat__ = True
    return workflow


def is_builtin_agent(workflow):
    """Match both product identity and implementation on definitions or compiled IR."""
    workflow_id = getattr(workflow, "workflow_id", None) or getattr(workflow, "id", None)
    return workflow_id == BUILTIN_AGENT_WORKFLOW_ID and any(
        getattr(getattr(node.executable, "handler", None), "__autoagent_builtin_chat__", False)
        for node in workflow.nodes
    )
