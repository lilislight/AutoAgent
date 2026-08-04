---
code_paths:
  - autoagent/project/
  - autoagent/cli/
  - .env.example
tags:
  - project-hosting
  - cli
  - manifest
  - environment
---

# Project and CLI Hosting

## Responsibility

Discover and validate AutoAgent projects, load explicitly declared Workflow and Evaluation objects, resolve deployment environment, own the configured App and Providers, and expose project, Workflow, Invocation, Evaluation, and Server commands.

## Current Design

`auto-agent.toml` is a strict schema-versioned manifest with project metadata, Workflow entrypoints, and optional Eval Suite locators. ProjectLoader imports only declared objects and also supports explicit standalone Workflow files for check, preview, local run, and local resume. ProjectHost creates one configured AutoAgentApp, installs required LLM Providers from the resolved environment, registers selected Workflows, and closes resources in ownership order. The CLI shares ProjectCompiler and ProjectHost paths, renders deterministic reports, and maps configuration, execution, and Server failures to distinct exit codes.

## Boundaries and Rules

- The manifest locates code; it does not contain Runtime, database, Provider-secret, or Server policy.
- Project `.env` values are resolved once per command and process environment values override them.
- Local run/resume is the default. `--server` or `--server-url` explicitly selects remote execution; submit is Server-only and returns after admission.
- Remote execution addresses come from explicit URL, project environment, or localhost defaults and never silently fall back to local execution.
- Standalone Workflow files are not accepted by Server hosting or remote execution.
- ProjectHost owns any Provider it creates and closes Providers after closing the App.

## Relationships

Workflow Compilation supplies validation and previews. Runtime Execution supplies the App path used by local CLI and Evaluation. AI Building Blocks supplies optional Provider setup. Tracing Server and UI supplies remote execution and hosting endpoints.
