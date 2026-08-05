# AutoAgent

## Purpose

AutoAgent is a code-first Python framework for building durable, observable AI Workflows. It gives Coding Agents a stable authoring contract and gives developers deterministic compilation, execution, persistence, evaluation, and debugging evidence for the generated Workflow code.

## Users

- Coding Agents that translate business requirements into Workflow definitions and focused tests.
- Developers who host, configure, inspect, and evaluate those Workflows.

## Scope and Non-goals

The framework covers explicit graph orchestration, typed Python Operators, LLM and Tool integration, Wait/Resume, recovery, optional database durability, Runtime and User Events, project-owned Evaluations, and an embedded tracing Server/UI.

Workflow source is currently authored as Python objects. Serialized YAML/JSON authoring and UI-based Workflow construction are not part of the implemented authoring surface.

## Current Stage

The package version is 0.1.0. The repository contains the end-to-end framework foundation and is actively refining Coding-Agent authoring and Evaluation workflows; public and persistence contracts should be treated as pre-release unless explicitly documented as stable.

## Core Concepts

- A **Workflow** is a static graph of Nodes and Edges plus execution policies and hooks.
- The **Compiler** expands child Workflows, validates graph and contract semantics, and produces immutable execution identity and Workflow IR.
- An **Operator** is a concrete callable; a **Capability** is an application-local abstract contract that may resolve to an Operator.
- A **Session** carries cross-Invocation context for one Workflow revision; an **Invocation** is one execution in that Session.
- The **RuntimeStore** owns the latest in-memory state. An optional durable backend persists immutable boundaries downstream.
- **Runtime Events** support tracing, replay, and recovery according to Event mode; **User Events** are a separate application/UI journal.
- An **Evaluation** runs isolated Cases through the normal ProjectHost and Runtime, then applies explicit Evaluators to captured evidence.

## Where to Start

- [Architecture](architecture.md)
- [Stable Workflow authoring API](../autoagent/__init__.py)
- [Application hosting entry point](../autoagent/core/app/app.py)
- [CLI entry point](../autoagent/cli/main.py)
- [Coding Agent Skills](../skills/)
- [Normative examples](../examples/authoring/)
- [Regression and integration tests](../tests/)
