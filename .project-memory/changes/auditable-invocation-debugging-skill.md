---
modules:
  - coding-agent-skills
tags:
  - coding-agent
  - evaluation
  - cli
---

# Auditable Invocation Debugging Skill

## Intent

Make the local Coding-Agent debugging workflow verifiable from an existing Invocation without requiring access to the Agent's private reasoning.

## Outcome

The debugging Skill now defines a Report-first, bounded-evidence repair path through Rerun, Comparison, and business Evaluation. An isolated deterministic Skill Evaluation prepares an incorrect Full-mode Invocation, preserves its business oracle, records AutoAgent CLI commands through a transparent proxy, and checks the observable command order and resulting candidate.

## Reason

Reviewing only the final code cannot establish whether a Coding Agent used recorded Runtime evidence or merely guessed a repair. Observable CLI auditing provides reproducible process evidence while preserving the boundary that private reasoning is neither available nor required.

## Impact

Future debugging Skill changes must keep the CLI workflow, hidden acceptance contract, and harness aligned. The harness remains evaluator-only and must not become part of Runtime execution or persistence.
