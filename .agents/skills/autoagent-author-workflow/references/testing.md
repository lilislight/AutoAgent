# Workflow Testing

This reference owns deterministic validation of authored Workflow projects. It
does not define the public API, graph design, or Diagnostic semantics.

## Minimum validation

Use the smallest test set that proves the requested behavior:

1. run Project Check and Workflow Check for every exported Workflow;
2. run one successful Invocation;
3. add one case for each materially different business branch or failure the
   requester named;
4. add one boundary test only when the project relies on durability, Loop
   termination, Retry/fallback, Map/Replication, or another nontrivial policy.

Do not test AutoAgent internals, Scheduler queues, database rows, or framework
features the project does not use. Do not repeat one behavior through sync and
async, memory and database, or CLI and direct-host paths unless that difference
is itself a requirement. Prefer table-driven cases and shared fixtures.

## Fixtures

Keep runnable JSON objects under descriptive paths such as:

```text
inputs/high-risk.json
expected/high-risk.json
```

Keep fixtures deterministic, secret-free, serializable, and small. Compare
stable public results rather than generated IDs or timestamps.

## Wait and Resume

For process-local behavior, one host may invoke and resume.

For durable behavior:

1. configure a temporary SQLite database;
2. run until waiting in `standard` or `full` mode;
3. close the first host;
4. create a new host with the same Workflow;
5. resume with the same Session and wait key;
6. assert the terminal result.

Do not claim cross-process Resume from an in-memory or minimal-mode test.

## AI and Tool tests

Default tests must not call a paid model or depend on model randomness.

Prefer a local Chat Completions mock Provider driven by deterministic
responses. Configure the ordinary CLI host to use it, keeping Provider
registration out of Workflow source.

Use a fake `llm_call` Operator only in test-only host code that already owns
App construction. Do not introduce App or Operator registration into the
authored Workflow module solely for testing.

Only when a required test cannot be expressed through the CLI, host it with
the existing public APIs instead of framework internals:

```python
from autoagent.app import AutoAgentApp, AutoAgentSettings
from autoagent.project import ProjectCompiler, ProjectLoader
```

Workflow source still uses only `autoagent` and `autoagent.ai`. Do not import
`autoagent.core.*` from either source or tests.

For ReAct behavior, select only the cases the request actually uses from this
matrix:

- one successful Tool and structured-output sequence;
- one parallel Tool turn when parallel calls are required;
- one correction case for each required error family (invalid Tool call, Tool
  execution exception, or invalid structured output);
- one exhaustion case for each distinct configured repair counter, not one for
  every error subtype sharing that counter;
- one `max_steps` termination case for a model-driven Loop.

An invalid-arguments test must fail before the Tool handler runs: use malformed
JSON, a missing required field, or an incompatible field type. A value that
passes the input schema and is rejected inside the Tool handler tests a Tool
execution exception instead. Do not use one fixture as evidence for both
paths.

Use typed mocked Tool outputs.

Reuse one scripted fake model and a table of response sequences instead of
copying a new fake and Invocation harness into every test.

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

Require passing static checks, one successful deterministic Invocation, and
coverage of materially different requested branches or policies. Keep secrets
out of fixtures, avoid live paid Providers, and label externally dependent
behavior unverified when it could not be exercised.
