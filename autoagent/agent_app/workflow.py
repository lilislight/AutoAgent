"""Workflow assembly and identity owned by the Agent application."""

from autoagent.agent.workflow.builder import agent_workflow
from autoagent.agent.workflow.policy import ContextPolicy
from autoagent.agent.workflow.subagents import SubagentsConfig, SubagentTarget
from autoagent.agent.execution import ExecutionToolsConfig
from .prompts import DEFAULT_AGENT_APP_INSTRUCTIONS
from .tool_defaults import execution_policy

BUILTIN_AGENT_WORKFLOW_ID = "autoagent.agent"


def builtin_agent_workflow(*, resources=None, execution=True, command_policy=None):
    """Construct the product Agent; project resources require explicit grants."""
    workflow = agent_workflow(
        workflow_id=BUILTIN_AGENT_WORKFLOW_ID,
        instructions=DEFAULT_AGENT_APP_INSTRUCTIONS,
        resources=resources,
        max_rounds=None,
        context_policy=ContextPolicy(context_window=200_000),
        user_events=True,
        include_ask_user=True,
        inject_current_time=True,
        subagents=SubagentsConfig(enabled=True, targets={
            "agent": SubagentTarget(workflow_id=BUILTIN_AGENT_WORKFLOW_ID,
                description="Delegate a task to another instance of this Agent."),
        }),
    )
    start = next(node for node in workflow.nodes if node.id == "start")
    start.executable.handler.__autoagent_builtin_chat__ = True
    start.executable.handler.__agent_app_execution__ = ExecutionToolsConfig() if execution is True else execution or None
    start.executable.handler.__agent_app_command_policy__ = command_policy if command_policy is not None else execution_policy()
    from .application import AgentApplication
    start.executable.handler.__runtime_application__ = AgentApplication
    return workflow


def is_builtin_agent(workflow):
    """Match both product identity and implementation on definitions or compiled IR."""
    workflow_id = getattr(workflow, "workflow_id", None) or getattr(workflow, "id", None)
    return workflow_id == BUILTIN_AGENT_WORKFLOW_ID and any(
        getattr(getattr(node.executable, "handler", None), "__autoagent_builtin_chat__", False)
        for node in workflow.nodes
    )
