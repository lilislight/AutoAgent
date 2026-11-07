# Repository Guidelines

## Project Structure

The repository root is the AutoAgent V2 project and the only active development
target.

- `autoagent/` contains the V2 Core Python package and public exports. External
  integration protocols, including event sinks, remain in `autoagent/core/`.
- `tests/` contains the V2 Python test suite.
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
- `.venv/bin/python -m tests.postgres` starts the installed local PostgreSQL test
  cluster and runs the full V2 suite with both database backends enabled.
- `.venv/bin/python -m compileall -q autoagent tests`
- `.venv/bin/python -m tests.benchmarks.benchmark_full_core`

Run PostgreSQL tests for Persistence, Host, Server, and Agent recording changes.
Do not treat skipped PostgreSQL tests as completed verification. Tests use
`AUTOAGENT_TEST_POSTGRES_URL` when set, otherwise the ignored local
`.autoagent/postgres/test-url`; each test creates and removes its own database.

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

## UI Design and Loading

Keep UI text in English. Reuse the shared shell, header, navigation, sidebar,
controls, typography, spacing, and theme tokens across all Tabs. Scope feature
styles to their own content; opening a Tab must not restyle shared chrome.
Use a shared Markdown typography scale for message headings, body text, emphasis,
inline code, code blocks, and tables. Keep inline code subtle and tables readable
with contained horizontal scrolling; do not let message content restyle chrome.

Keep navigation, headings, search inputs, filters, dropdowns, and actions in
their real positions while data or code loads. Place lazy-loading boundaries
inside the content area so they do not replace the surrounding controls.

Show skeletons only for pending data elements, such as list rows or messages.
Match the real element's width, height, padding, gaps, icon size, and text-line
layout by reusing its layout classes. Use separate row variants for Workflows,
Sessions, Invocations, and Conversations. Do not cover an entire sidebar or
replace ready inputs and dropdowns with skeletons. Keep unknown values neutral
until loaded, and use the shared spinner where a content skeleton is unsuitable.

Choose loading feedback by behavior, not request duration:

| Behavior | Feedback |
| --- | --- |
| First read of structured content | Skeleton matching the list row, facts, message, card, option, Timeline track, chart, or JSON value. |
| App bootstrap or Graph/layout initialization | Centered shared spinner, confined to the unready app or canvas. |
| Background refresh or search with existing content | Retain content and show a small indicator beside the search, heading, or refresh action. No blocking overlay or blur. |
| Mutation or explicit command | Spinner plus action text inside a stable-size button; disable conflicting submissions. |
| Load more / append pagination | Keep existing rows/messages; use a tail skeleton or loading-more button indicator. |
| Streaming, execution, approval wait, or reconnect | Real partial content and explicit lifecycle/connection status, independent of data-loading placeholders. |

Use the same Inspector frame, title, close action, and known Tabs throughout
loading. Summary uses field-value skeletons; Input/Output and Definition use
JSON skeletons; Execution uses record skeletons; pending Actions use request
cards. Changing the selected object replaces only the affected content and
must not replay the panel entrance. Apply the same rule to menus and forms:
keep ready controls, skeletonize pending data, and use a small indicator for
validation. Do not show both a skeleton and a spinner for the same pending area.

Preserve rendered content during background refetches and cached Tab switches.
Do not add artificial loading delays or flash welcome/empty states before data
is ready. Loading must finish for successful, empty, and failed responses;
failures need an actionable error state.
Treat a disabled query as idle, not loading. A new object or filter scope must
not display the previous object's data as its own. Empty and failed reads must
release loading feedback; retained data remains visible on a refresh failure.
Replacing a whole page is a scope change: retain its navigation and skeletonize
the requested page instead of labelling old rows as the new page.

Use short, consistent, trigger-directional motion: side panels enter from their
opening side, and dropdowns enter downward. Do not replay panel entrance when
only its content changes. Contain animation overflow so Tab switches, chat
switches, loading, and panel entrances never create transient page scrollbars;
keep scrolling inside the intended list, conversation, or content panel.

Respect `prefers-reduced-motion`, light/dark themes, keyboard focus, and
accessible loading labels/`aria-busy`. Verify loading with delayed requests,
compare skeleton and loaded element geometry, and check desktop and narrow
viewports for layout shifts and horizontal/vertical page overflow. Run
`npm --prefix ui test` and `npm --prefix ui run build` for UI changes.

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
