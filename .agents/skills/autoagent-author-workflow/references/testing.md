# Workflow Testing

This reference owns deterministic validation of authored Workflow projects. It
does not define the public API, graph design, or Diagnostic semantics.

## Contents

- Test layers
- Coverage matrix
- Fixtures
- Wait and Resume
- AI and Tool tests
- Commands
- Acceptance

## Test layers

### Project load

Verify:

- `auto-agent.toml` parses;
- every entrypoint imports;
- every object is a Workflow;
- Workflow IDs are unique.

### Compile

Compile every exported Workflow and assert:

- no error Diagnostics;
- expected entry and exit IDs;
- expected Node, Edge, and Loop counts;
- warnings are either absent or explicitly justified.

Prefer testing through the same Project Compiler used by the CLI rather than
constructing internal Compiler registries in authoring tests.

### Invocation

Run the Workflow through the public host or CLI path and assert:

- terminal or waiting state;
- typed result;
- expected business values;
- expected error code for negative cases.

Do not assert internal Scheduler queue layout or database rows in an authoring
test.

## Coverage matrix

For each material feature, cover:

- every conditional branch;
- the combination of selected parallel branches;
- fan-in after predecessors finish in different orders;
- Loop continuation and exit;
- Loop resource limit on a non-terminating case;
- Map empty, single-item, multi-item, and item failure;
- Replication aggregation and replica failure;
- Retry exhaustion and fallback when configured;
- Timeout;
- parallel Output Binding conflict when Context is written;
- Wait result and Resume validation;
- invalid Invocation input;
- invalid Operator output.

Test only the rows relevant to the Workflow; do not add artificial framework
coverage to every project.

## Fixtures

Keep runnable JSON objects under descriptive paths such as:

```text
inputs/high-risk.json
expected/high-risk.json
```

Fixtures must be:

- deterministic;
- free of secrets;
- serializable;
- small enough to review;
- representative of the public Invocation contract.

Compare stable public results, not generated UUIDs or timestamps.

## Wait and Resume

For process-local behavior, one host may invoke and resume.

For durable behavior:

1. configure a temporary SQLite database;
2. run until waiting in `standard` or `full` mode;
3. close the first host;
4. create a new host with the same namespace and Workflow;
5. resume with the same Session and wait key;
6. assert the terminal result.

Do not claim cross-process Resume from an in-memory or minimal-mode test.

## AI and Tool tests

Default tests must not call a paid model or depend on model randomness.

Prefer a local OpenAI-compatible mock Provider driven by deterministic
responses. Configure the ordinary CLI host to use it, keeping Provider
registration out of Workflow source.

Use a fake `llm_call` Operator only in test-only host code that already owns
App construction. Do not introduce App or Operator registration into the
authored Workflow module solely for testing.

Cover:

- normalized request construction;
- valid Tool sequence;
- parallel Tool calls where supported;
- unknown Tool repair;
- invalid Tool arguments;
- Tool execution exception returned to the model;
- structured output repair;
- repair exhaustion;
- max-step termination.

Use typed mocked Tool outputs.

## Commands

At minimum run:

```bash
autoagent project check
autoagent workflow list
autoagent workflow check <workflow-id>
python -m unittest <relevant-test-module>
```

Use the project's documented Python test runner when it differs. Do not impose
a package manager solely because the AutoAgent repository uses one in
development.

## Acceptance

Do not finish until:

- static checks pass;
- tests cover every new business path;
- failure behavior is explicit;
- no secret is committed;
- no test depends on a live paid Provider by default;
- the handoff states any external behavior that could not be verified.
