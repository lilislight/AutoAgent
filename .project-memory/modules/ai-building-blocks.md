---
code_paths:
  - autoagent/ai/
tags:
  - ai
  - llm
  - tools
  - react-workflow
---

# AI Building Blocks

## Responsibility

Provide provider-neutral LLM contracts, concrete Provider adapters, typed Tool definitions, the built-in `llm_call` Capability/Operator, and reusable ReAct Workflow construction on top of the ordinary Workflow runtime.

## Current Design

Normalized Pydantic models represent LLM messages, requests, responses, streaming chunks, tool calls, usage, and response formats. LLMProvider defines invoke, stream, and close boundaries; Chat Completions and DeepSeek configuration adapt external protocols into those models. Typed Tools derive definitions and schemas from Python callables. `react_workflow` builds a bounded child Workflow for conversation preparation, LLM calls, Tool validation/execution, repair, output validation, and session-scoped message history.

## Boundaries and Rules

- Provider-specific transport and errors stay behind the provider-neutral LLMProvider interface.
- AI execution uses normal Capability registration, Workflow compilation, Node policies, Map, loops, Runtime Events, and User Events.
- Tool parse repair, output repair, and transport retry are separate bounded mechanisms.
- Streaming deltas are live User Events and are reduced to an authoritative final output; transient chunks are not Runtime replay state.
- ReAct conversation history is scoped by Session and expanded Workflow path so sibling child Workflows remain isolated.

## Relationships

ProjectHost installs an LLM Provider only when selected Workflows require `llm_call`. Operator System exposes the Capability/Operator abstraction. Runtime Execution supplies streaming, Map, loop, persistence, and session behavior without an AI-specific executor.
