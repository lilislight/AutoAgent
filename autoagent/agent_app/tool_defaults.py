"""Product access defaults; reusable SDK Tools carry no product permission rules."""

from autoagent.agent.files import FileToolsConfig
from autoagent.agent.execution import ExecutionToolsConfig
from autoagent.agent.tools import ToolPolicy


def file_defaults():
    # Preserve existing product defaults while using the SDK's single rule model.
    sample = {"all": [dict(field="operation.action", op="in", value=["file.read", "file.write", "file.delete"]),
        {"any": [dict(field="operation.attributes.path", op="glob", value=pattern)
            for pattern in ("**/.env.example", "**/.env.sample")]}]}
    return FileToolsConfig(policy=ToolPolicy(rules=[
        dict(id="external", when={"all": [dict(field="operation.attributes.scope", op="equals", value="external"),
            {"not": sample}]}, decision="ask", reason="Access outside the current Workspace"),
        dict(id="sensitive", when={"all": [dict(field="operation.action", op="in",
            value=["file.read", "file.write", "file.delete"]), {"not": sample},
            {"any": [dict(field="operation.attributes.path", op="glob", value=pattern)
                for pattern in ("**/.env", "**/.env.*", "**/*.key", "**/*.pem", "**/id_rsa", "**/id_ed25519")]}]},
            decision="ask", reason="Sensitive file"),
    ]))


def execution_defaults(config=None):
    defaults = ExecutionToolsConfig(default_decision="ask", process_write_decision="ask", file_policy=file_defaults())
    if config is None:
        return defaults
    return defaults.model_copy(update={name: getattr(config, name) for name in config.model_fields_set})
