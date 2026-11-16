"""Product access defaults; reusable SDK Tools carry no product permission rules."""

from autoagent.agent.files import FileToolsConfig
from autoagent.agent.tools import ToolPolicy


def file_defaults():
    return FileToolsConfig(policy=ToolPolicy(rules=[
        dict(id="external", when=dict(field="operation.attributes.scope", op="equals", value="external"),
            decision="ask", reason="Access outside the current Workspace"),
    ]))


def execution_policy():
    return ToolPolicy(rules=[
        dict(when=dict(field="context.tool_name", op="equals", value="execute_command"), decision="ask"),
        dict(when={"all": [
            dict(field="context.tool_name", op="equals", value="manage_process"),
            dict(field="args.request.action", op="equals", value="write"),
        ]}, decision="ask"),
    ])
