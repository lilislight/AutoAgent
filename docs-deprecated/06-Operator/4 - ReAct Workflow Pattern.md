# ReAct Workflow Pattern

This document defines how ReAct-style execution is represented as Workflow
nodes.

AutoAgent OS should not require ReAct to be hidden inside a black-box Agent
Operator. LLM reasoning, tool execution, observation, and loop continuation can
be modeled as visible Workflow structure.

## Principle

```text
Graph edges define control flow.
ToolSet defines tools visible to an LLM node.
Tool nodes execute tools as ordinary Workflow nodes.
```

The LLM node decides which tools to call. Scheduler routes to the selected tool
nodes. NodeExecutor executes those tool nodes through the Operator registry.

## Graph Shape

Example:

```text
reason
  toolset: [search_web, fetch_issue, read_file]

reason -> search_web  if reason.output.tool_calls contains "search_web"
reason -> fetch_issue if reason.output.tool_calls contains "fetch_issue"
reason -> read_file   if reason.output.tool_calls contains "read_file"

search_web  -> reason
fetch_issue -> reason
read_file   -> reason

reason -> final_answer if reason.output.final_answer != null
```

There is no required ToolDispatcher node. Tool nodes are normal Workflow nodes.

## YAML Sketch

```yaml
nodes:
  - id: reason
    capability: operator:llm.reason
    toolset:
      - name: search_web
        capability: operator:search.web
      - name: fetch_issue
        capability: operator:github.fetch_issue

  - id: search_web
    capability: operator:search.web

  - id: fetch_issue
    capability: operator:github.fetch_issue

  - id: final_answer
    capability: operator:answer.format

edges:
  - id: reason_to_search
    from: reason
    to: search_web
    condition: nodes.reason.output.tool_calls contains "search_web"

  - id: reason_to_fetch_issue
    from: reason
    to: fetch_issue
    condition: nodes.reason.output.tool_calls contains "fetch_issue"

  - id: search_to_reason
    from: search_web
    to: reason

  - id: fetch_to_reason
    from: fetch_issue
    to: reason

  - id: reason_to_final
    from: reason
    to: final_answer
    condition: nodes.reason.output.final_answer != null
```

## Parallel Tool Calls

If the LLM returns multiple tool calls, multiple outgoing tool edges may be
satisfied.

```text
reason.output.tool_calls = ["search_web", "fetch_issue"]
```

Scheduler can mark both `search_web` and `fetch_issue` ready. WorkflowExecutor
can dispatch both nodes in the same batch when execution policy allows it.

## Tool Results

Tool nodes write their outputs into the Runtime Run namespace.

```text
run.nodes.search_web.output
run.nodes.fetch_issue.output
```

The next LLM reasoning step reads those outputs through its input mapping. The
Workflow controls how tool observations are summarized or appended to messages.

## Benefits

Visible ReAct structure gives AutoAgent OS:

- node-level observability for LLM and tool calls
- retry, timeout, and resource policy per tool
- fallback paths for failed tools
- parallel tool execution
- runtime evidence for Optimizer
- graph-level debugging in Observability UI

## Black-Box Agent Operators

Agent Operators are still allowed when the developer wants to hide internal
reasoning and tool loops behind one node.

Use explicit ReAct Workflow structure when tool choice, tool execution, and
iteration should be visible to Scheduler, Observability, and Optimizer.
