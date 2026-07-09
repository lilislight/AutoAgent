# Repository Guidelines

## Project Structure & Module Organization

This repository currently contains design documentation for AutoAgent OS. Source code, tests, and build assets are not present.

- `docs/README.md` contains initial notes for Workflow IR and runtime scheduling.
- `docs/00-Foundation/` covers vision, core concepts, and architecture.
- `docs/01-Workflow/` documents workflow, node, edge, and builder concepts.
- `docs/02-Compiler/` describes compilation, validation, Workflow IR, and the compilation pipeline.
- `docs/03-Runtime/` describes invocation, runtime, sessions, runs, context, lifecycle, and concurrency.

Keep new documentation in the numbered area that best matches its subject. Use the existing filename pattern, for example `docs/02-Compiler/5 - New Topic.md`.

## Build, Test, and Development Commands

There is no configured build system, package manager, or test runner yet. Useful local checks are documentation-oriented:

- `rg "WorkflowIR" docs` searches the documentation for a concept.
- `git diff -- docs` reviews documentation changes before commit.
- `find docs -name "*.md" -print` lists all Markdown documents.

If executable code is added later, document setup, run, build, lint, and test commands here before relying on them in pull requests.

## Coding Style & Naming Conventions

Write Markdown with short sections, descriptive headings, and direct explanations. Preserve the numbered documentation directories and the `N - Title.md` filename style. Capitalize domain terms consistently: Workflow, Workflow IR, Runtime, Scheduler, Kernel, Operator, Runtime Session, and Runtime Run.

Use fenced code blocks with language tags, for example `mermaid` for diagrams and `text` for state sketches. Keep diagrams close to the section they explain.

## Testing Guidelines

No automated tests or coverage requirements are defined. For documentation changes, verify links, diagrams, and terminology against `docs/00-Foundation/1 - Core Concepts.md`. When code is introduced, place tests in a clearly named test directory and add exact test commands to this guide.

## Commit & Pull Request Guidelines

The current history uses concise, imperative commit messages, for example `Add comprehensive documentation for Workflow authoring, compilation, and runtime execution`. Follow that style: start with a verb, describe the visible change, and avoid vague messages like `update docs`.

Pull requests should include a short summary, affected documentation areas, and any terminology or architecture decisions reviewers should validate. Link related issues when available. Include screenshots only when changing rendered diagrams or other visual artifacts.

## Agent-Specific Instructions

Do not overwrite an existing `AGENTS.md`. Keep edits scoped to the requested documentation area, and avoid introducing build or test instructions that are not backed by repository files.
