---
code_paths:
  - skills/
  - skill-evals/
tags:
  - coding-agent
  - workflow-authoring
  - evaluation
  - cli
---

# Coding Agent Skills

## Responsibility

Guide Coding Agents through the supported public process for authoring AutoAgent Workflows and debugging recorded Invocations, then forward-test those instructions in isolated projects.

## Current Design

The authoring Skill translates business requirements into typed Workflow code, registered business Evaluations, and public CLI validation. The debugging Skill starts from an Invocation Report, progressively queries bounded evidence, makes a focused project repair, and verifies it through Rerun, Comparison, and the owning Eval Suite. Skill Evaluations keep the user request separate from evaluator-only acceptance rules. The debugging harness prepares a deterministic Full-mode incident and transparently records AutoAgent CLI calls for objective process checks.

## Boundaries and Rules

- Skills teach only public Workflow, Evaluation, and CLI contracts; they do not expose framework internals as authored-project dependencies.
- Skill instructions stay concise and route detailed contracts through references loaded only when needed.
- Authoring Evaluations judge end-to-end business behavior; focused unit tests cover only isolated project helpers without duplicating the same scenario.
- Debugging is Report-first and evidence-bounded. Rerun and Comparison do not themselves establish business correctness; the Eval oracle does.
- Evaluator harnesses are repository test tools, not Skills, Runtime features, or shipped user workflows. They record observable CLI behavior and never attempt to capture private reasoning.

## Relationships

Workflow Authoring, AI Building Blocks, and Evaluation provide the public code contracts taught by the authoring Skill. Project and CLI Hosting plus Runtime evidence provide the debugging surface. Framework tests validate Skill structure and keep references aligned with current source.
