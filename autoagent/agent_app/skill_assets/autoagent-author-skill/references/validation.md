# Compile and run a Core Workflow

Use this reference to check registration, inspect the compiled graph, and
exercise concrete inputs. The commands below use the
[minimal manifest project](project.md).

## Choose local or Server CLI

Pass `--project PATH` to select local project configuration and
`--server API_URL` to send a supported command to an existing Server.
Choose one source per command.

| Task | Source |
| --- | --- |
| Check or compile source definitions, or run a business Eval | Local project. The compiler checks do not start the Host. |
| Invoke, inspect registered inputs, or reply to a Wait with no running writer | Local project. These commands open a Host for the command's lifetime. |
| Invoke or control an execution on a running Host | Server. Use its API base URL, including the configured API prefix. |
| Read saved Invocation history and scoped Input/Output values | Local database or Server, depending on where the execution was recorded. |

For the default local Server, use for example:

```bash
autoagent --server http://127.0.0.1:8765/api/v2 workflow inputs normalize --entry-node normalize
autoagent --server http://127.0.0.1:8765/api/v2 invoke normalize \
  --entry-node normalize --input-json '{"text":"  Hello  "}' --wait terminal
```

Use the same source for follow-up commands. `--wait none` is available
through a Server that keeps the execution alive after the command returns.
`--wait-timeout` limits observation. When a mutation has an unknown
outcome, inspect its state before retrying. See [hosting.md](hosting.md)
for lifecycle and integration.

## Check static structure first

```bash
autoagent project check
autoagent workflow check normalize
autoagent workflow compile normalize --node-id normalize
```

`project check` loads the project's Workflow definitions and compiles them.
`workflow check` targets a single Workflow. `workflow compile` shows the
plan, including Entries, Edges and enclosing loops where applicable.
`--node-id` and `--edge-id` narrow the compiled plan to one object.

Read a diagnostic's code and affected object. For import or registration
errors, start with the module, factory name and selected project. For contract
errors, compare the source output, the target input, and any mapping's return
type. For a join or error target, check which Edge values the mapping receives.

## Run a representative input

```bash
autoagent workflow inputs normalize --entry-node normalize
autoagent invoke normalize --entry-node normalize \
  --input-json '{"text":"  Hello  "}' --wait terminal --format json
autoagent invocation value NORMALIZE_INVOCATION_ID --field output --format json
```

`workflow inputs` describes the entry contract from the Host catalog.
It opens local project resources. `invoke` takes `--input-json` or
`--input-file`. A new Session is the default. For intentional Session
reuse, pass `--session-mode existing --session-id SESSION_ID`.

Read the execution status and Invocation ID from the `invoke` response.
Pass that ID to `invocation value`. The output should be
`{"text":"hello"}`. For each business branch added by a change, run a
representative input and check the expected outcome.

## Verify an external Wait

Register the `approval_workflow` from
[interaction-recovery.md](interaction-recovery.md) as a project Workflow.
For example, save it as `approval.py` and add
`entrypoint = "approval:approval_workflow"` under another `[[workflows]]`
section in the manifest.

```bash
autoagent invoke approval --entry-node approve \
  --input-json '{"order_id":"A-1"}' --wait boundary --format json

autoagent invocation respond INVOCATION_ID --wait-id WAIT_ID \
  --input-json '{"approved":true}' --wait terminal --format json
```

Use the IDs returned by the first command. Keep both commands pointed at the
same Host or local database. Use `--wait boundary` when the caller must
observe and respond to the pending Wait. `--wait-timeout` limits how long
the CLI observes an execution.

## Run directly from Python

Use `WorkflowCompiler` and `AutoAgentApp` when the project embeds Core.
The same business definition can also be registered with the CLI.

```python
from autoagent.core.app import AutoAgentApp
from autoagent.core.compiler import WorkflowCompiler
from workflows import normalize_workflow

workflow = normalize_workflow()
compiled = WorkflowCompiler().compile(workflow)
assert compiled.ok, compiled.diagnostics

app = AutoAgentApp()
try:
    result = app.invoke(workflow, {"text": "  Hello  "}, entry_node_id="normalize")
    assert result.status == "completed", result
    assert result.output == {"text": "hello"}
finally:
    app.close()
```

Report the Workflow ID and Entry Node, the checks run, and the actual result
for the inputs exercised. See [Workflow business Evaluation](evaluation.md)
when the requirement needs assertions on the execution path, intermediate
data, or downstream effects.
