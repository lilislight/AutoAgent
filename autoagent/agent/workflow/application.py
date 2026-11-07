"""SDK-owned Workflow used exclusively by the Agents application."""

from .builder import agent_workflow
from .subagents import SubagentsConfig, SubagentTarget

BUILTIN_AGENT_WORKFLOW_ID = "autoagent.agent"


def builtin_agent_workflow():
    """Construct the SDK chat Agent; project resources require explicit Host grants."""
    workflow = agent_workflow(
        workflow_id=BUILTIN_AGENT_WORKFLOW_ID,
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


def is_builtin_agent(ir):
    return ir.workflow_id == BUILTIN_AGENT_WORKFLOW_ID and any(
        getattr(getattr(node.executable, "handler", None), "__autoagent_builtin_chat__", False)
        for node in ir.nodes
    )
