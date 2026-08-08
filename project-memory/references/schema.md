# Project Memory Schema

## Contents

- [Directory](#directory)
- [Project](#project)
- [Architecture](#architecture)
- [Module](#module)
- [Change](#change)
- [Structured relations](#structured-relations)
- [Update matrix](#update-matrix)

## Directory

```text
.project-memory/
├── project.md
├── architecture.md
├── modules/
│   └── <module-slug>.md
└── changes/
    └── <change-slug>.md
```

Use lowercase kebab-case file names. Use file stems as Module and Change references. Git supplies authorship, timestamps, versions, and history; do not duplicate them in documents.

## Project

`project.md` answers why the project exists. Keep only current, project-wide information:

- purpose and users;
- scope and non-goals;
- current stage when it affects development choices;
- core concepts;
- links to the architecture and primary development entry points.

Update it only when those facts change. Do not add module details, implementation inventories, or chronological history.

Suggested headings are in `assets/templates/project.md`. Omit empty headings.

## Architecture

`architecture.md` explains how the current system is composed and routes readers to Modules:

- system overview;
- Module map with one-line responsibilities and links;
- key cross-module flows;
- global boundaries and rules;
- runtime or deployment shape when important.

Update it when the module map, cross-module relationships, global flows, or global boundaries change. Keep prior architecture and replacement reasons in Change documents, not here.

Suggested headings are in `assets/templates/architecture.md`. Omit empty headings.

## Module

Each `modules/<module-slug>.md` describes one stable logical responsibility. A Module may match a directory or span several directories; it is not a file, class, package inventory, or API list.

Describe only current information:

- responsibility and boundary;
- current conceptual design;
- important module rules;
- relationships with other Modules;
- links to only the history required to understand the current design.

Update a Module when its responsibility, boundary, core behavior, important relationships, rules, or coarse code entry paths change. Do not update it for local implementation details, ordinary method or API edits, mechanical refactors, or test-only changes.

Suggested headings and frontmatter are in `assets/templates/module.md`. Omit empty headings.

## Change

Each `changes/<change-slug>.md` is a compact explanation of one meaningful project evolution. It is not a commit log, PR summary, full design document, implementation report, or test report.

Create a Change only when at least one condition holds:

1. Project, Architecture, or Module current knowledge must change.
2. A non-obvious reason may affect future development.
3. A new approach replaces an old approach that future work should not accidentally restore.
4. A failed or reverted approach leaves a durable lesson.

One coherent intent and outcome form one Change, even across several commits. Separate independent intents. After completion, correct factual errors but do not rewrite old history for later work; create and link a new Change instead.

Keep the body minimal:

- `Intent`: the problem and desired result;
- `Outcome`: the final conceptual or behavioral change;
- `Reason`: only known, non-obvious rationale with future value;
- `Impact`: affected current knowledge and future implications.

Do not reproduce diffs, file lists, API inventories, commits, PRs, issue metadata, or detailed verification. Suggested headings and frontmatter are in `assets/templates/change.md`. Omit empty headings.

## Structured relations

Only Module and Change use YAML frontmatter. All supported values are lists.

Module fields:

```yaml
---
code_paths:
  - src/scheduler/
tags:
  - scheduling
  - workflow-execution
---
```

- `code_paths`: stable repository-relative directories or entry files; never line numbers or symbols.
- `tags`: a small set of reusable lowercase kebab-case topics. Tags are keywords; do not add a separate keyword field.

Change fields:

```yaml
---
modules:
  - scheduler
  - runtime
tags:
  - workflow-loop
  - execution-model
related_changes:
  - runtime-node-insertion
supersedes:
  - dynamic-runtime-expansion
---
```

- `modules`: existing Module file stems.
- `tags`: a small set of reusable lowercase kebab-case topics.
- `related_changes`: only Changes required to understand this Change.
- `supersedes`: Changes whose approaches this Change intentionally replaces.

Omit empty fields. Reuse existing tags before creating synonyms. A new superseding Change points to the old Change; the old historical file does not require an active/inactive state.

## Update matrix

| Result of a coding task | Memory action |
|---|---|
| No durable project knowledge changed | No update |
| Current Module understanding changed | Update Module; create Change only if its gate passes |
| Module map or cross-module design changed | Update Architecture and Modules; normally create Change |
| Project purpose, scope, or core concepts changed | Update Project and affected current documents; create Change |
| An old approach was replaced | Update current documents; create a new Change with `supersedes` |
| Memory conflicts with code | Treat memory as possibly stale, verify code, then correct affected current documents |
