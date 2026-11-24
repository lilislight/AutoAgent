# Core Workflow projects

Use this reference when creating a project, registering a Workflow, or locating
the definition in an existing project.

## Select the SDK and the project

Use the project's Python environment and its selected AutoAgent V2 version.
Check it with `autoagent --version`. When starting from a supplied wheel,
install that wheel with the `cli` extra to use the project commands.

When authoring from Agent App, use [live registration](live-registration.md)
to publish into its current Host. The bundled Skills are already available;
they do not need to be copied into `.agents/skills`. Explicit user Skills of the
same name take precedence over the bundled guide.

For an existing project, find its `auto-agent.toml` or decorated Workflow
factories and follow the project's layout. Run CLI commands from the project
directory or use `autoagent --project PATH`. An explicit manifest path can
also be passed as `PATH`.

| Task | Public import |
| --- | --- |
| Define Workflows, Nodes, Operators, Hooks, policies and native commands | `autoagent` |
| Register a Workflow by decorator | `autoagent.host.workflow` |
| Compile a definition from Python | `autoagent.core.compiler.WorkflowCompiler` |
| Run a definition in an embedded Python application | `autoagent.core.app.AutoAgentApp` |

Keep the definition importable. Export a Workflow object or a synchronous
zero-argument factory that constructs one. Create clients and other live
resources in the application's lifecycle or inside the Operator that uses them.
For local CLI execution, the Host reads the project's `.env`. Process
environment values take precedence. Use `.env.example` for configuration
names and placeholders.

## Minimal manifest project

Create `workflows.py` and `auto-agent.toml` in the same directory. The
Workflow uses a module-level `TypedDict` as its Entry and Exit contract.

```python
from typing_extensions import TypedDict

from autoagent import Node, Workflow


class Message(TypedDict):
    text: str


def normalize(value: Message) -> Message:
    return {"text": value["text"].strip().lower()}


def normalize_workflow() -> Workflow:
    return Workflow("normalize", nodes=[Node("normalize", normalize)])
```

```toml
schema_version = 1

[project]
name = "text-normalizer"

[[workflows]]
entrypoint = "workflows:normalize_workflow"
```

For a Workflow in another source directory, a Workflow table may also contain
`source_dir = "path/to/source"`. Relative paths resolve beside the manifest.
Agent App's registration Tool writes the appropriate source entry automatically.

An entrypoint uses `module:attribute`. The CLI uses the Workflow ID
`normalize` and the Entry Node ID `normalize` from the returned definition.
The factory name is the import target. See [validation.md](validation.md)
for the commands that compile and invoke this project.

## Decorator discovery

In a project using decorator discovery, add `from autoagent.host import
workflow` to `workflows.py` and decorate `normalize_workflow` with
`@workflow`. Pass the project's directory to the CLI. A manifest with
explicit `[[workflows]]` entries selects those entries.

Use [workflow.md](workflow.md) to extend the single Node with business
branches, data mappings, or parallel work.
