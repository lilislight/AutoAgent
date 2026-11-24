# Use Agent capabilities in a custom Workflow

Use this approach when the project defines the order of model calls,
validation, Tools, and business decisions. Keep the graph and its
portable input and output contracts in the Workflow definition.
Use `autoagent[agent]` for the Agent SDK, and add `mcp` for MCP.

## Connect a model Node

Import `LLM_CALL`, `LLMRequest`, and `LLMResponse` from
`autoagent.agent`. Use `Node("model", LLM_CALL)` and map the upstream
business data to an `LLMRequest`. Build its input from typed
`LLMMessage` items. Set the model and generation options on the request,
or resolve a model through the project's configured Provider.

In the [project lifespan](hosting.md#run-a-project-from-python), register
an implementation with
`setup.register_operator(llm_call_operator(provider), capability_id=LLM_CALL.id, default=True)`.
Enter and close the Provider in the same lifespan when it owns a
connection. Use the Host's Provider configuration when the project
already defines the appropriate binding.

- Set `LLMRequest.streaming=True` to consume Provider chunks. The
  `LLM_CALL` capability provides its own reducer to produce a final
  `LLMResponse`. Use the Node's User Event Mapper for application
  progress. See [streaming and User Events](streaming-user-events.md).
- Use `LLMOutputOptions(text_format=LLMJsonSchemaFormat.from_type(Model))`
  for a structured response. Handle provider and validation errors as
  part of the Workflow's failure behavior. Use
  `parse_response(response, Model)` to obtain the application value
  from validated response text.
- Keep clients, credentials, and connections in application resources.
  Use portable request and result values across Workflow boundaries.

## Decide where Tools execute

Describe allowed Tools using typed Python callables wrapped with
`Tool` or exposed through a `ToolSet`. Advertise their definitions to
the model. The model returns an `LLMToolCall`; select and authorize the
call before executing it. A `ToolSet` can handle the call with
`ainvoke(call)` inside an Operator or be used as a Node executable
through `as_operator()`. Forward the `LLMToolResult` with its
corresponding call ID if the model needs a follow-up request.

For MCP, enter the `MCPClient` in the owning lifespan, then use the
chosen MCP Tools or registry through their public interfaces. Use
`MCPOperations` when the Workflow needs an explicit resource or prompt
read. Bound Tool selection to the project's declared capabilities.
Apply Tool policy to validated arguments before effects, and use
a native [Wait](interaction-recovery.md#external-approval) when the
business requires a human decision.

Use a fixed offline Provider and isolated Tool resources to
[evaluate](evaluation.md) the selected branch, structured response, and
any effects. Use [Agent Workflows](agent-workflows.md) when the model
should choose Tools and continue through multiple rounds.
