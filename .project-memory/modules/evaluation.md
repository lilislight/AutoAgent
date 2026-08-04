---
code_paths:
  - autoagent/evaluation/
  - autoagent/cli/evaluation.py
tags:
  - evaluation
  - eval-suite
  - testing
  - runtime-evidence
---

# Evaluation

## Responsibility

Define project-owned, business-facing Workflow Eval Suites and execute their Cases through the real Runtime with explicit Evaluators and traceable evidence.

## Current Design

Each manifest Eval Suite binds one stable id and Evaluation class to one Workflow id. EvaluationLoader imports and validates only the requested Suite, discovering async `eval_*` methods in deterministic definition order. EvaluationRunner constructs one isolated Session per Case, executes sequential invoke/resume Steps in Full Event mode through the normal ProjectHost, applies Evaluators as strict gates, and aggregates typed Step, Case, and Suite results. Cases may run concurrently up to a configured limit, while a Suite timeout cancels unfinished work and preserves completed results.

## Boundaries and Rules

- Evaluation is a CLI-owned end-to-end contract, separate from framework unit tests.
- Evaluation code does not instantiate an App; Runner supplies an EvalCase backed by ProjectHost.
- Cases are isolated by Session, while Steps within one Case intentionally share Session state.
- Evaluators consume immutable snapshots of request, result, contexts, outputs, previous Steps, and bounded Runtime evidence.
- Business mismatches are result failures; loading, configuration, or Evaluator infrastructure failures remain distinct errors.

## Relationships

Project and CLI Hosting owns Suite discovery and environment. Runtime Execution provides invoke/resume and evidence. Workflow Compilation validates the selected Workflow before execution. Runtime Persistence may retain the Full Event evidence but is not a separate Evaluation engine.
