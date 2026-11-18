# Build an Agent Workflow

Use `agent_workflow(...)` from `autoagent.agent` for a Workflow that
lets the model choose Tools, consume their results, and continue until
it has a final response or reaches its execution limit. Register the
returned Workflow through the [normal project entrypoint](project.md).

## Configure the Workflow and its resources

| Contract | Purpose |
| --- | --- |
| `AgentWorkflowOptions` | Pick a Workflow ID, define instructions and model request options, and set round and parallelism limits. |
| `AgentHostSettings` and `AgentModelSettings` | Set the runnable Workflow's model defaults and access boundary through `AgentWorkflowOptions(runtime=...)`. |
| `AgentResources` | Declare the Tools, MCP sources, and Skills available to this Workflow with `tools`, `mcp`, and `skills`. |
| `ContextPolicy` | Configure estimation and compaction of the conversation used for model requests. Test the behavior with a representative multi-turn case. |
| `AgentWorkflowPolicies` | Adjust supported Node execution policies and Workflow resource limits for the application. |

Register a named Provider with `setup.providers.register(name, provider)`
in the project [lifespan](hosting.md#run-a-project-from-python).
Use the same name in the Workflow's model settings. Keep provider
configuration external to the Workflow input. A local Tool can be a
typed callable or a `Tool`. Use `MCPSource` for a declared MCP
connection and `SkillDirectory` for a declared folder of Skills.
The resource registry belongs to the application and is installed on
the owning runtime loop.

The builder's Entry is `start`. Send an `AgentInput` from Python or
`{"message": "..."}` as JSON. The final `AgentResult` contains
content blocks and exposes `text` for text consumers. For a multi-turn
conversation, reuse its Session after the current Invocation settles.
Use the same Workflow and Session deliberately when continuing that
conversation.

## Handle approval and questions

Use `AgentAccess` for the Workflow's access boundary and a
`ToolPolicy` when authorization depends on arguments or context.
An approval requirement creates a native Wait before executing
the approved Tool.

Read the pending `AgentApprovalRequest`, review its requests, and
create one decision per request with `request.respond(approved=...)`.
Submit `AgentApprovalResponse(decisions=..., revision=batch.revision)`
to the exact Invocation and Wait through the Host or CLI. Serialize
the response in JSON mode when crossing the CLI or API boundary.
Treat the model's Tool call as a request for execution. The
application supplies the approval decision.

Use `include_ask_user=True` when the model may need explicit
clarification. Reply to an `AskUserRequest` with an `AskUserResponse`
that addresses the requested question IDs. Handle Tool approvals
and user questions according to the Wait request's `kind`.

## Coordinate children and observe results

Use `SubagentsConfig` and named `SubagentTarget` declarations for
delegation. Register the target Workflows with their Entry Nodes and
set a bounded wait policy. Inspect Child results through the
Workflow's business execution view.

Enable `AgentWorkflowOptions(user_events=True)` when the client consumes
Agent progress and committed messages. Transient updates are suitable
for live display. Use the durable result and persisted business
messages for [Evaluation](evaluation.md).

Choose offline Provider responses that exercise success, refusal,
and errors at the Tool boundary. In a business Eval, verify what the
Tool actually did and whether a denied request left the fixture state
unchanged.
