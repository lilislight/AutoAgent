# AutoAgent

[中文](README_ZH.md)

AutoAgent is a code-first framework for building durable, observable AI
Workflows. It combines explicit graph orchestration, typed Python functions,
LLM and Tool integration, persistence, recovery, and tracing in one runtime.

The framework is designed for a development model in which a Coding Agent
translates business requirements into Workflow code, while AutoAgent provides
the stable authoring contract, deterministic compiler diagnostics, execution
runtime, and debugging evidence needed to validate that code.

## Why AutoAgent

AI applications often need more than a single model call. They need branching,
parallel work, loops, external approvals, retries, Tools, structured output,
durable state, and enough execution evidence to explain what happened.

AutoAgent provides these capabilities without hiding control flow inside an
agent loop:

- **Workflow as code** — define Nodes, Edges, Conditions, mappings, policies,
  and child Workflows with typed Python.
- **Compiled execution** — validate graph structure and contracts before an
  Invocation runs.
- **One runtime model** — use the same Scheduler and Executor for ordinary
  automation, LLM calls, Tools, ReAct Workflows, Map, Replication, and Wait.
- **Durable operation** — optionally persist Sessions, Invocations, recovery
  state, Events, and large-value Artifact references to SQLite or PostgreSQL.
- **Built-in observability** — inspect graph movement, timings, retries,
  fallback, Wait/Resume, errors, and detailed state changes.
- **Coding-Agent tooling** — use a stable public API, packaged examples,
  compiler diagnostics, CLI commands, and an authoring Skill.

## How it fits together

```mermaid
flowchart LR
    USER["User requirement"] --> AGENT["Coding Agent"]
    AGENT --> SOURCE["Workflow source"]
    SOURCE --> COMPILER["Compiler"]
    COMPILER --> APP["AutoAgent App"]
    APP --> RUNTIME["Workflow Runtime"]
    RUNTIME --> STORE["Memory / Database Store"]
    RUNTIME --> TRACE["Tracing Server and UI"]
```

Workflow source describes business behavior. The host environment owns App
lifecycle, persistence, concurrency, Provider credentials, and Server
configuration. This separation lets the same Workflow run locally, through the
CLI, inside another service, or eventually on a hosted platform.

## Core capabilities

### Workflow authoring

- sequential, conditional, and parallel graph execution;
- natural Loops, fan-in, dynamic Map, and Replication;
- typed Input Mapping, Output Binding, Conditions, selectors, and aggregators;
- Retry, fallback, timeout, recovery, resource, and failure policies;
- explicit sync/async `StreamingResult` execution with one typed final output;
- independent `UserEvent` streams configured by snake_case `UserEventMapping`,
  with standard ReAct message and Tool events, live SSE delivery, and durable
  semantic history when a Database Store is configured;
- process-local and durable Wait/Resume;
- reusable child Workflows.

### AI building blocks

- a provider-neutral `llm_call` Capability;
- a Chat Completions Provider adapted to the built-in `llm_call` Operator;
- Provider streaming reduced by NodeExecutor without persisting transient chunks;
- typed Tools generated from Python functions;
- structured model output;
- bounded ReAct Workflows with Tool and output repair.

### Runtime and persistence

The in-memory RuntimeStore is the authoritative latest state. A database backend
persists runtime records asynchronously so normal execution does not wait for
every SQL operation. AutoAgent supports explicit startup recovery, cross-process
Wait/Resume, persistence backpressure, configurable retention, and large-value
externalization through Artifact references.

Each Invocation selects one observation level:

| Mode | Intended use |
| --- | --- |
| `minimal` | Final Invocation state and result with the lowest recording cost |
| `standard` | Production tracing, durable Wait/Resume, and crash recovery |
| `full` | Detailed phase data, state replay, debugging, and future Fork support |

UserEvent history is orthogonal to these modes: semantic events are durable in
all three modes, while built-in token/reasoning/Tool-call deltas remain
memory/SSE-only and are replaced by their authoritative completed events.

### Tracing

The embedded Server can run standalone or as a FastAPI Router. Its UI combines
the Workflow graph, Invocation state, Inspector, Timeline, replay, Event detail,
and persistence health. Historical data is paged and detailed Event values are
loaded on demand.

## Start with an authored project

AutoAgent projects export one or more Workflow objects from Python modules and
list them in `auto-agent.toml`. The CLI discovers the project, creates the App,
applies runtime configuration, and uses the same compiler and execution path as
an embedded host.

```bash
autoagent project check
autoagent workflow list
autoagent workflow check <workflow-id>
autoagent workflow preview <workflow-id>
autoagent workflow preview <workflow-id> --format mermaid --output workflow.mmd
autoagent workflow preview <workflow-id> --format json
autoagent invocation run <workflow-id> --input-file input.json
autoagent invocation report <invocation-id>
autoagent invocation query <invocation-id> nodes
autoagent invocation query <invocation-id> node <node-execution-id>
autoagent eval list
autoagent eval check <suite-id>
autoagent eval run <suite-id>
autoagent server --host 127.0.0.1 --port 8765
```

Project-owned Evaluations are declared with ``[[eval_suites]]`` in
``auto-agent.toml``. ``eval list`` reads only those locators, ``eval check``
validates the selected ``Evaluation`` class and Workflow without Provider
credentials, and ``eval run`` executes its ``eval_*`` Cases through the normal
App path in Full event mode. Business mismatches exit with code 1; loading,
configuration, or Evaluator infrastructure errors exit with code 2.

`invocation report` returns a compact diagnostic index for one observed
execution. `invocation query` progressively loads bounded Node, Edge, Operator
Call, RuntimeEvent, UserEvent, or Full Runtime State evidence. Both commands
prefer the matching running Server and can use an explicitly configured
durable database without recovering or executing the Workflow.

For an unregistered Workflow exported directly from a Python file, `check`,
`preview`, `invocation run`, and local `invocation resume` also accept `--file`.
The exported object defaults to `workflow`; use `--object` when the file exports
another name:

```bash
autoagent workflow check --file ./draft_workflow.py
autoagent workflow preview --file ./draft_workflow.py --object review_workflow
autoagent invocation run --file ./draft_workflow.py --input-file input.json
autoagent invocation resume --file ./draft_workflow.py \
  --session <session-key> --wait-key <wait-key>
```

`invocation run` and `invocation resume` execute locally by default. Pass
`--server` to execute through a running AutoAgent Server, or use `submit` when
the CLI should return immediately after admission:

```bash
autoagent invocation run <workflow-id> --server --input-file input.json
autoagent invocation submit <workflow-id> --input-file input.json
autoagent invocation resume <workflow-id> --server \
  --session <session-key> --wait-key <wait-key> \
  --response-file response.json
```

The client uses `AUTOAGENT_SERVER_URL` when configured and otherwise derives
`http://127.0.0.1:<AUTOAGENT_SERVER_PORT>`. `--server-url` overrides both. A
missing or unreachable Server is an error and never falls back to local
execution. Standalone file loading is intentionally not supported by `server`
or remote execution.

See the packaged [authoring examples](examples/authoring/README.md) for complete
conditional, Wait/Resume, and LLM/Tool/ReAct Workflows. The
[authoring Skill](skills/autoagent-author-workflow/SKILL.md) describes
the recommended Coding-Agent workflow.

Runtime configuration is supplied through CLI options and environment
variables. [.env.example](.env.example) documents every supported framework
setting.

## Project status

The current foundation includes the public Workflow authoring API, compiler,
runtime execution, SQLite/PostgreSQL persistence, recovery, Runtime Events,
CLI, tracing Server/UI, AI building blocks, packaged examples, and the
authoring Skill.

The next focus is validating Coding-Agent authoring in independent projects,
then building Agent-friendly Invocation reports, progressive trace queries,
rerun/Eval, and Fork-based debugging. [MVP.md](MVP.md) defines the product
stages and acceptance boundaries; [TODO.md](TODO.md) tracks current work.

## Development

AutoAgent requires Python 3.12 or newer. Run Python commands through the
repository environment:

```bash
UV_CACHE_DIR=/tmp/autoagent-uv-cache uv run python -m unittest discover -v
```

Build the tracing UI and Wheel with:

```bash
./build.sh
```

Windows PowerShell users can run `./build.ps1`.

Repository areas:

- [`autoagent/`](autoagent/) — Python framework, runtime, persistence, CLI, and
  embedded Server;
- [`ui/`](ui/) — tracing UI source;
- [`examples/`](examples/) — normative authoring examples;
- [`tests/`](tests/) — framework regression and integration tests;
- [`benchmarks/`](benchmarks/) — runtime and persistence measurements;
- [`docs-deprecated/`](docs-deprecated/) — archived design material, not current
  documentation.
