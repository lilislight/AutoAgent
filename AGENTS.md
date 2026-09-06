# Repository Guidelines

## Project Structure

The repository root is the AutoAgent V2 project and the only active development
target.

- `autoagent/` contains the V2 Python package: Core, Host, hosting adapters, CLI,
  and tracing server.
- `tests/` contains the V2 Python test suite.
- `ui/` contains the V2 tracing UI.
- `examples/` contains V2 runnable examples.
- `docs/` contains V2 design notes, plans, testing notes, and the package README.
- Root packaging files build V2; `AGENTS.md` is the repository instruction file.
- `.agents/` and `.codex/` are workspace metadata, not product source trees.
- `v1/` contains the complete archived V1 project, including its source, tests,
  UI, examples, documentation, Skills, Skill Evals, build scripts, and packaging
  files.

V1 is reference material only. Do not import V1 from V2, preserve V1
compatibility in V2, or place new work in `v1/` unless the user explicitly asks
to modify the archived implementation.

## Build, Test, and Development Commands

Run V2 commands from the repository root through the root virtual environment:

- `.venv/bin/python -m unittest discover -s tests -v`
- `.venv/bin/python -m compileall -q autoagent tests`
- `.venv/bin/python -m tests.benchmarks.benchmark_full_core`
- `cd ui && npm test`
- `cd ui && npm run build`

Run an archived V1 command only when work is explicitly scoped to `v1/`, and
run it with `cwd=v1` so its same-named `autoagent` package cannot shadow V2.

## Sources of Truth

Treat current V2 source, configuration, tests, and run paths as authoritative.
Files in `docs/` are reference snapshots and may be stale. Routine source,
behavior, API, test, build, or structure changes do not require corresponding
documentation updates. Update files in `docs/` only when the user explicitly
asks for documentation work. Files below `v1/` are historical evidence and are
not current product documentation.

Keep V2 changes within the root V2 trees. Do not add compatibility layers for
the V1 API or data model. Reuse a V1 algorithm only after verifying that its
semantics match the current V2 contracts.

## Coding and Testing

Write Markdown with short sections, descriptive headings, and direct
explanations. Capitalize domain terms consistently: Workflow, Workflow IR,
Runtime, Scheduler, Operator, Runtime Session, Runtime Event, Trace Event, and
User Event.

Keep Python tests in `tests/`. Every `test_*` method must start with a short
docstring describing the behavior or failure boundary it verifies. Run focused
tests while iterating and the full V2 suite before completing broad Runtime or
repository changes.

## Implementation Authorization

Treat requests as read-only design discussion unless the user explicitly
authorizes implementation with wording such as "开始实现", "开始写代码", or
"改一下 xxx". Before authorization, do not create, edit, move, delete, format,
or commit repository files. After authorization, modify only the requested
scope.

## Commits and Pull Requests

Use concise imperative commit messages that describe the visible change. Pull
requests should summarize the behavior, affected areas, validation, and any
architecture or terminology decisions reviewers should check.
