"""Product access defaults; reusable SDK Tools carry no product permission rules."""

from autoagent.agent.files import FileToolsConfig
from autoagent.agent.tools import ToolPolicy
from autoagent.agent.tools.tool_policy import AllCondition, AnyCondition, FieldCondition, NotCondition


BUILTIN_READ_FILE = "builtin/tool/read_file"


def shared_file_reads(policy):
    """Inherit target-based read rules while keeping call arguments tool-specific."""
    def fields(condition):
        if isinstance(condition, NotCondition):
            yield from fields(condition.not_)
        elif isinstance(condition, (AllCondition, AnyCondition)):
            for child in condition.all if isinstance(condition, AllCondition) else condition.any:
                yield from fields(child)
        else:
            yield condition._tokens

    read = FieldCondition(field="operation.action", op="equals", value="file.read")
    rules = []
    for rule in policy.rules:
        referenced = tuple(fields(rule.when))
        if any(path[:1] == ("operation",) for path in referenced) and all(
                path[:1] == ("operation",) or path[:2] in {
                    ("context", "user_id"), ("context", "session_id"), ("context", "attributes")}
                for path in referenced):
            rules.append(rule.model_copy(update={"when": AllCondition(all=(read, rule.when))}))
    return policy.model_copy(update={"default": "allow", "rules": tuple(rules)})


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
