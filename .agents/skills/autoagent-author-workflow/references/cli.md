# AutoAgent CLI

This reference owns the CLI commands a Coding Agent uses to validate and run
authored Workflows. It does not document Server administration.

## Contents

- Global form
- Recommended authoring cycle
- Project and Workflow checks
- Invocation Run and Resume
- Relevant runtime overrides
- Exit status

## Global form

```bash
autoagent \
  --project <project-directory-or-auto-agent.toml> \
  [--env-file <path> | --no-env-file] \
  [--log-level debug|info|warning|error] \
  <command>
```

Global options must appear before the command group.

## Recommended authoring cycle

```bash
autoagent --project . project check
autoagent --project . workflow list
autoagent --project . workflow check <workflow-id>
autoagent --project . invocation run <workflow-id> --input-file input.json
```

Run `project check` before requiring Provider secrets. Static compilation
registers abstract built-in capabilities without contacting external services.

## Project Check

```bash
autoagent project check \
  [--warnings-as-errors] \
  [--report-file report.txt]
```

Checks every exported Workflow. `--report-file` writes the same report that is
still printed to stdout.

## Workflow List

```bash
autoagent workflow list [--report-file report.txt]
```

Shows project identity, entrypoints, source Node/Edge counts, and compile
status.

## Workflow Check

```bash
autoagent workflow check <workflow-id> \
  [--warnings-as-errors] \
  [--report-file report.txt]
```

Shows version, compiled Node/Edge counts, entries, exits, Loops, and structured
diagnostics.

## Invocation Run

```bash
autoagent invocation run <workflow-id> \
  [--input-file input.json | --input-json '{"key":"value"}'] \
  [--session <session-id>] \
  [--entry-node <node-id>] \
  [--event-mode minimal|standard|full] \
  [--timeout-ms <milliseconds>] \
  [--trace] \
  [--report-file report.txt]
```

Input must be a JSON object or `null`. Use `--input-file -` to read stdin.

Event modes:

- `minimal`: terminal Invocation contract only; no durable recovery history;
- `standard`: graph-level events and recovery state; default;
- `full`: internal phase events and state operations for detailed debugging.

Choose Event mode per Invocation. Do not hard-code it into Workflow source.

## Invocation Resume

```bash
autoagent invocation resume <workflow-id> \
  --session <session-id> \
  --wait-key <wait-key> \
  [--response-file response.json | --response-json '{"approved":true}'] \
  [--timeout-ms <milliseconds>] \
  [--trace] \
  [--report-file report.txt]
```

Cross-process Resume requires the same Workflow, Session, wait key,
and database as the waiting Invocation. It also requires `standard` or `full`
mode on the original Invocation.

## Relevant runtime overrides

For durable Wait/Resume, `invocation run` and `invocation resume` accept:

```text
--store auto|memory|database
```

`--store database` requires `AUTOAGENT_DATABASE_URL`. `auto` uses the database
when configured and memory otherwise.

This controls the test/run host, not Workflow semantics. Consult CLI help for
other hosting limits only when a test specifically requires one; do not tune
hosting infrastructure as part of Workflow authoring.

## Exit status

- `0`: command succeeded or Invocation reached a non-failure result such as
  completed or waiting;
- `1`: compile check failed, warnings were promoted, Invocation failed, timed
  out, was interrupted, or was cancelled;
- `2`: project load or configuration error;
- `130`: user interruption.

Read [diagnostics.md](diagnostics.md) after a Project or Workflow check failure.
