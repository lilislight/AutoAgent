# Repository Guidelines

## Project Structure & Module Organization

This repository contains the AutoAgent implementation, tests, UI, runnable
examples, and an archived design snapshot.

- `autoagent/` contains the Python framework and embedded tracing server.
- `tests/` contains the automated Python test suite.
- `ui/` contains the tracing UI.
- `examples/` contains runnable examples.
- `docs-deprecated/` is an obsolete design snapshot and is not a source of
  truth for the current implementation.

Do not add current documentation to `docs-deprecated/`.

## Build, Test, and Development Commands

Run Python commands through the repository-root virtual environment:

- `python -m unittest discover -v`
- `cd ui && npm run build`

## Coding Style & Naming Conventions

Write Markdown with short sections, descriptive headings, and direct explanations. Preserve the numbered documentation directories and the `N - Title.md` filename style. Capitalize domain terms consistently: Workflow, Workflow IR, Runtime, Scheduler, Kernel, Operator, Runtime Session, and Runtime Run.

Use fenced code blocks with language tags, for example `mermaid` for diagrams and `text` for state sketches. Keep diagrams close to the section they explain.

## Testing Guidelines

Keep Python tests in `tests/`. Treat current source and tests as authoritative;
the archived documentation may be used only as historical context.

## Commit & Pull Request Guidelines

The current history uses concise, imperative commit messages, for example `Add comprehensive documentation for Workflow authoring, compilation, and runtime execution`. Follow that style: start with a verb, describe the visible change, and avoid vague messages like `update docs`.

Pull requests should include a short summary, affected documentation areas, and any terminology or architecture decisions reviewers should validate. Link related issues when available. Include screenshots only when changing rendered diagrams or other visual artifacts.

## Agent-Specific Instructions

Do not overwrite an existing `AGENTS.md`. Keep edits scoped to the requested documentation area, and avoid introducing build or test instructions that are not backed by repository files.

## AutoAgent V2 Rewrite

- Treat `autoagent_v2/` as a completely independent new project root for the V2 rewrite.
- Put all V2 source, tests, skills, documentation, packaging files, and future examples under `autoagent_v2/`.
- Do not implement V2 by modifying the existing `autoagent/`, `tests/`, `docs-deprecated/`, `skills/`, `examples/`, or `ui/` trees.
- Do not add compatibility layers for the existing AutoAgent API or data model. V2 follows `Refactor.md` and may intentionally be incompatible.
- Existing source may be inspected to understand proven algorithms. Reuse only logic that still matches the V2 design; do not copy the old architecture or preserve obsolete abstractions.
- Design and implement V2 as a complete standalone project with its own future `autoagent/`, `tests/`, `skills/`, and packaging structure. During the initial Core implementation, create only the directories and files required by the implemented code and tests.
- Run V2 tests from the repository-root environment while targeting `autoagent_v2/`; do not modify legacy tests to make V2 pass.
