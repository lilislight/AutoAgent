---
name: project-memory
description: Maintain concise, repo-native project memory for coding agents and developers. Use when explicitly initializing Project Memory in a repository; when understanding, planning, or changing code in a repository that already contains `.project-memory/`; and after coding tasks to decide whether Project, Architecture, Module, or Change memory must be updated. Also use to validate or repair an existing `.project-memory/` directory. Do not infer missing intent or create commit-level change logs.
---

# Project Memory

Maintain a small current view of a software project plus selected explanations of important evolution. Use memory to narrow code exploration, then verify current behavior against code.

## Choose the workflow

- If the user explicitly asks to initialize memory and `.project-memory/` does not exist, follow **Initialize**.
- If `.project-memory/` exists and the task is read-only, follow **Read context** and do not update files.
- If `.project-memory/` exists and the task changes the project, follow **Read context**, complete the coding task, then follow **Maintain after a change**.
- If the user asks to inspect or repair memory, read the schema, run validation, and make only the requested repairs.
- Do not initialize memory implicitly in an unrelated repository.

Before creating or updating memory, read [references/schema.md](references/schema.md) completely. Use the templates in `assets/templates/` when creating files, replace placeholders, and remove empty headings and fields.

## Preserve truth and scope

- Treat current code as authoritative for implemented behavior.
- Treat conflicting memory as possibly stale; verify and correct current-state documents instead of forcing code to match memory.
- Record user intent and known rationale. Never invent missing reasons from a diff.
- Store concepts, boundaries, and reasons that affect future work. Point to code for implementation details.
- Never create file, API, method, commit, PR, or test inventories.
- Keep Project, Architecture, and Module documents current and concise. Keep important history in Change documents.
- Preserve unrelated user edits in both code and memory.

## Initialize

Initialize only after an explicit request.

1. Read repository instructions such as `AGENTS.md` and follow them.
2. Reuse any available repository-onboarding, architecture-analysis, or language-specific skill when it directly helps. Treat its output as input to verify, not as authoritative memory.
3. Inspect current source, configuration, dependency manifests, tests, and build or run paths. Use repository search and Git when useful. Do not depend on stale design documents when code can verify the current state.
4. Identify stable logical Modules from responsibilities and relationships, not from every directory or package.
5. Create `.project-memory/project.md`, `.project-memory/architecture.md`, and only the Module files justified by current evidence.
6. Do not reconstruct historical Change files from Git unless the user supplies or confirms the missing intent and rationale.
7. Remove template placeholders and empty headings.
8. Run the validator and fix errors before finishing.

Keep bootstrap conservative. Mark an unknown in plain language when it materially limits understanding; otherwise omit unsupported detail.

## Read context

1. Read `.project-memory/project.md`.
2. Read `.project-memory/architecture.md` when system structure or module routing matters.
3. Match the task and candidate code paths against Module `tags` and `code_paths`.
4. Read only the relevant Module files.
5. Use Module names and tags to shortlist Change files. Follow `related_changes` and `supersedes` only when historical rationale matters.
6. Do not load all Change files by default.
7. Inspect the relevant current code before making implementation claims or edits.

If memory is missing, broken, or stale, continue using code when safe and include the memory repair in the end-of-task maintenance decision.

## Maintain after a change

During the task, retain the user's smallest reliable Change Intent in working context. Do not create a committed Change draft merely because coding started.

Complete and verify the requested code work first. Then:

1. Decide whether durable project knowledge changed. If not, leave `.project-memory/` untouched.
2. Update affected Module documents when their current responsibilities, boundaries, conceptual behavior, relationships, rules, or coarse code paths changed.
3. Update Architecture when the Module map, cross-module flow, global boundary, runtime shape, or deployment shape changed.
4. Update Project only when purpose, users, scope, stage, core concepts, or primary entry guidance changed.
5. Create a Change only when the gate in the schema passes. Keep one coherent intent and outcome per file.
6. Fill `modules` and reuse existing `tags`. Add `related_changes` only when required for understanding. Add `supersedes` when an old approach was intentionally replaced.
7. Do not rewrite an old Change to describe later work. Create a new Change and connect it to the old one.
8. Remove duplicated explanations from current-state documents and keep only a small number of history links needed to understand the current design.
9. Run validation and fix errors.

Possible outcomes include no update, a current Module correction without a Change, or a Change plus updates to Module, Architecture, or Project.

## Validate

Run:

```bash
python3 <skill-directory>/scripts/validate_memory.py --repo <repository-root>
```

Treat validation errors as required fixes. Review warnings, but do not add content merely to silence them.

## Report

Keep the final user-facing note short:

- State which Project Memory documents changed and why.
- If nothing needed updating, omit the memory note unless the user asked about it.
- Surface unknown intent only when it materially affects future understanding.
