"""Application composition; Agent execution stays in the shared Workflow builder."""

from contextlib import asynccontextmanager
from pathlib import Path
import sys

from autoagent.agent_app import BUILTIN_AGENT_WORKFLOW_ID, builtin_agent_workflow
from autoagent.agent import (
    AgentResources, CommandTools, CommandRule, ContextPolicy, ExecutionToolsConfig, SubagentsConfig, SubagentTarget,
    Tool, ToolSet, ToolDecision, SkillDirectory, agent_workflow,
)
from autoagent.host import workflow

from .tools import add_numbers, get_demo_weather, save_demo_note


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def demo_execution_config():
    """Deliberately small allowlist; approval/deny probes perform no writes."""
    rules = [CommandRule(program=name, decision="allow") for name in ("pwd", "ls", "printf")]
    rules.extend((
        CommandRule(program="git", args_prefix=["status"], decision="allow"),
        CommandRule(program="git", args_prefix=["diff"], decision="allow"),
        CommandRule(program="git", args_prefix=["push"], decision="ask"),
        CommandRule(program="git", args_prefix=["reset", "--hard"], decision="deny"),
    ))
    for program in dict.fromkeys((sys.executable, "python3")):
        prefix = ["-u", "-m", "agent_demo.cli_probe"]
        rules.extend((
            CommandRule(program=program, args=["--version"], decision="allow"),
            CommandRule(program=program, args_prefix=prefix, decision="allow"),
            CommandRule(program=program, args_prefix=[*prefix, "approval"], decision="ask"),
            CommandRule(program=program, args_prefix=[*prefix, "deny"], decision="deny"),
        ))
    return ExecutionToolsConfig(default_decision="ask", rules=rules)


def demo_tool_policy(call, context):
    return ToolDecision(action="ask" if call.name == "save_demo_note" else "allow")


def demo_tools():
    return ToolSet([Tool(handler) for handler in (add_numbers, get_demo_weather, save_demo_note)],
        policy=demo_tool_policy)


def demo_resources():
    tools = demo_tools()
    commands = CommandTools(PROJECT_ROOT, config=demo_execution_config())
    return AgentResources(tools=(*(tools[item.name] for item in tools.definitions), *commands.tools()),
        skills=(SkillDirectory(PROJECT_ROOT / "skills", include=("demo-guide",)),))


@asynccontextmanager
async def lifespan(setup):
    """Register demo resources explicitly in the product application."""
    # /agent talks only to the product Workflow. Keep that implementation and
    # marker, applying the same demo command policy as the execution Workflows.
    for selected in setup.workflows:
        if selected.id == BUILTIN_AGENT_WORKFLOW_ID:
            selected.nodes[0].executable.handler.__agent_app_execution__ = demo_execution_config()
    application = setup.applications.get("agent")
    if application is None or not application.enabled:
        yield
        return
    context = application.context
    for handler in (add_numbers, get_demo_weather, save_demo_note):
        context.resources.register_tool(Tool(handler), version="1", policy=demo_tool_policy)
    async with context.resources.skill_directories([PROJECT_ROOT / "skills"], include=("demo-guide",), workflows={BUILTIN_AGENT_WORKFLOW_ID}):
        yield



INSTRUCTIONS = """你是 AutoAgent 的交互测试助手，默认使用中文回答。
按用户要求选择工具；天气数据仅为模拟，回答时必须说明。
用户要求澄清或测试人机交互时使用 ask_user，等待真实回答。
保存笔记必须调用 save_demo_note，审批由系统处理，不能自行声称批准或保存成功。
用户要求委派、并行研究或测试子 Agent 时，使用 researcher 类型。
spawn_agent 立即返回；run_agent 最多等待一段时间，返回时子任务可能仍在运行。
用 get_agents 查看可见任务及结果。需要回复时在 send_message 中设置
reply_requested=true，并在正文明确需要回答什么；回复时填写 reply_to。
程序等待都必须有限，使用 wait_for，不能无限轮询或反复重发消息。
消息会自动出现在 Context 中，不需要调用接收工具。
"""


@workflow
def main_agent():
    return agent_workflow(
        user_events=True,
        resources=demo_resources(),
        instructions=INSTRUCTIONS,
        include_ask_user=True,
        user_signal_endpoint="user.messages",
        subagents=SubagentsConfig(
            enabled=True,
            signal_endpoint="agent.messages",
            targets={"researcher": SubagentTarget(
                workflow_id="researcher",
                description="Analyze a delegated question, use the demo Tools, and report findings.")},
            include_siblings=True,
            default_wait_seconds=30,
            max_wait_seconds=120,
        ),
        context_policy=ContextPolicy(context_window=8192),
        max_rounds=40,
        max_parallelism=4,
    )


@workflow
def research_agent():
    return agent_workflow(
        user_events=True,
        workflow_id="researcher",
        resources=demo_resources(),
        instructions=INSTRUCTIONS + "\n你是研究子 Agent。独立完成任务并清楚返回结果。"
            "需要联系其他 Agent 时先用 get_agents 查询实际 ID，不能猜测 ID。",
        include_ask_user=True,
        user_signal_endpoint="user.messages",
        subagents=SubagentsConfig(enabled=True, signal_endpoint="agent.messages", include_siblings=True),
        context_policy=ContextPolicy(context_window=8192),
        max_rounds=30,
        max_parallelism=4,
    )
