"""The product's one registration tool over the generic current Host API."""

from pathlib import Path
from typing import Annotated

from pydantic import Field
from autoagent.agent.models import ToolOutput
from autoagent.core.errors import WorkflowCompileError

from autoagent.agent.tools import Tool, ToolSet, ToolBusinessError, ToolPolicy, ToolContext, tool
from autoagent.agent.tools.group import ToolGroup
from autoagent.host import HostError


class WorkflowRegistrationTools(ToolGroup):
    group_id = "workflows"

    def __init__(self, host, root):
        self.root = Path(root).resolve()

        @tool
        async def register_workflow(entrypoint: Annotated[str, Field(min_length=3, max_length=512)],
                                    source_dir: Annotated[str, Field(min_length=1, max_length=4096)] = ".") -> ToolOutput:
            """Register a Python Workflow into this running Host and its project TOML.

            source_dir is relative to the current Workspace working directory.
            entrypoint uses module:object syntax: a Workflow or a zero-argument
            synchronous factory. Existing code stays active until registration
            succeeds. Source edits take effect only after registering again or
            restarting the Server. Registration imports Python but does not run
            the Workflow. Use the existing CLI with --server to execute/inspect.
            """
            source = (self.root / source_dir).resolve()
            if not source.is_relative_to(self.root):
                raise ToolBusinessError("Workflow source_dir must remain inside the current Workspace working directory.", code="invalid_source")
            try:
                context = ToolContext.current()
                result = await host.register_workflow(source_dir=source, entrypoint=entrypoint,
                    expected_source_digest=context.attributes.get("workflow_source_digest") if context else None)
                return ToolOutput(structured_content=result)
            except WorkflowCompileError as error:
                return ToolOutput(is_error=True, structured_content={"error": {"kind": "workflow_compile_failed", "message": str(error)},
                    "diagnostics": [item.to_record() for item in error.diagnostics]})
            except (HostError, ValueError, TypeError, OSError) as error:
                raise ToolBusinessError(str(error), code=(getattr(error, "code", None) or "workflow_registration_failed")) from error

        self._tools = ToolSet((Tool(register_workflow),), policy=ToolPolicy(default="ask"))
        self._bind_group()

    def source_fingerprint(self, arguments):
        from autoagent.host.source_loading import capture_sources
        source = (self.root / arguments.get("source_dir", ".")).resolve()
        if source.is_relative_to(self.root) and source.is_dir():
            return capture_sources(source)[1]
        return None
