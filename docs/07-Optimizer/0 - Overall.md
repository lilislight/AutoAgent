# Overall

This document defines the Optimizer module.

Optimizer improves Workflow definitions and policies. It uses static Workflow
structure, compiled Workflow IR, and runtime evidence to propose changes for
future Workflow versions. It does not directly mutate active Runtime Runs.

## Purpose

Optimizer turns design structure and execution evidence into Workflow
improvement proposals.

```text
Workflow / Workflow IR / runtime evidence
    -> Optimizer
    -> WorkflowPatch proposal
    -> Compiler validation
    -> new Workflow version
```

Optimizer is a proposal system first. Automatic application can be added later
through explicit approval policy.

## Evidence Sources

Optimizer may use:

- Workflow source structure
- Workflow IR graph indexes and compiled policies
- Runtime Session context snapshots
- Runtime Run node and edge states
- event logs and timelines
- node input, output, error, and retry records
- resource usage such as tokens, cost, time, and memory
- waiting and resume events
- human feedback
- Operator descriptors and historical performance
- observability summaries

Evidence should be structured before it is given to an LLM-based optimizer.

## Optimization Types

### Policy Optimization

Policy optimization adjusts execution parameters without changing graph shape.

Examples:

- increase or decrease retry attempts
- adjust timeout duration
- set resource limits such as token, cost, or invocation count
- change fail-fast versus continue-active-branches behavior
- tune concurrency limits
- replace an expensive Operator with a cheaper equivalent

This is the lowest-risk optimization path because it preserves Workflow
structure.

### Static Graph Optimization

Static graph optimization analyzes Workflow and Workflow IR without using
runtime outcomes.

Examples:

- remove unreachable nodes or edges
- simplify duplicate edges
- identify branches that can run in parallel
- suggest splitting a large node into smaller managed nodes
- suggest merging nodes that do not need separate retry, timeout, observability,
  or failure handling
- detect dead loops or branches without terminal behavior
- improve input mappings and output bindings

Static optimization should preserve developer intent. Destructive graph changes
should be emitted as reviewable patches, not applied silently.

### Runtime Evidence Optimization

Runtime evidence optimization uses observed behavior across Runtime Runs.

Examples:

- add fallback paths for frequently failing nodes
- reduce retry on nodes that rarely recover
- increase timeout for nodes that usually succeed after longer execution
- add validation before nodes that often receive bad input
- split a slow or unreliable node into visible substeps
- remove or de-prioritize branches that are rarely selected and low value
- parallelize independent slow branches
- improve prompts or input mappings for low-quality LLM outputs
- suggest human review where automatic execution is risky

LLMs may be used to interpret evidence and propose changes, but the output must
be structured as WorkflowPatch values that Compiler can validate.

### Experiment Optimization

Optimizer may propose controlled experiments rather than immediate replacement.

Examples:

- compare two Operators for the same node
- canary a new prompt or model
- evaluate alternate retry or timeout policy
- route a small percentage of runs through a candidate branch

Experiment results become new runtime evidence. The system should track which
Workflow version and patch candidate produced each run.

## WorkflowPatch

Optimizer output targets the Workflow authoring model.

Patch operations may include:

- add, remove, or replace a node
- add, remove, or replace an edge
- update a node capability reference
- update node policy or Workflow policy
- update input mapping or output binding
- add validation, preprocessing, fallback, or human review nodes
- attach optimizer rationale and evidence references

Patches should be structured, reviewable, reversible, and compilable.

## Patch Lifecycle

```text
collect evidence
    -> generate optimization proposal
    -> produce WorkflowPatch
    -> validate patch against Workflow
    -> compile patched Workflow
    -> review or approve
    -> publish new Workflow version
    -> observe future runs
```

Compiler remains the authority for structural validity. Runtime remains the
authority for executing compiled Workflow IR.

## Safety Boundary

Optimizer should not:

- mutate active Runtime Session or Runtime Run state
- change Scheduler decisions for an in-flight run
- rewrite Workflow IR directly without producing a Workflow patch
- bypass Compiler validation
- silently remove nodes with external side effects
- change security-sensitive Operators without approval

Static patching is the default model. Changes affect future Runtime Runs after a
new Workflow version is compiled.

## Objective Functions

Optimizer should optimize against explicit objectives, not vague improvement.

Common objectives:

- success rate
- latency
- token or cost usage
- output quality
- human intervention rate
- failure recovery rate
- policy compliance
- developer-supplied business metrics

When objectives conflict, Optimizer should preserve the tradeoff in the patch
rationale.
