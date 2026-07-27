from __future__ import annotations

from typing import Literal

ExecutionMode = Literal["normal", "recovery"]

# Invocation state is the coarse state of one app.invoke().
# request. WorkflowExecutor updates it around the scheduling/execution loop.
InvocationStateValue = Literal[
    "created",      # Invocation object exists; entry request may be queued.
    "running",      # WorkflowExecutor is actively advancing this invocation.
    "waiting",      # At least one NodeExecution is externally waiting.
    "completed",    # Workflow reached a successful final result.
    "failed",       # Workflow reached an unrecoverable failure.
    "cancelled",    # Caller/framework explicitly cancelled the invocation.
    "interrupted",  # Recovery found in-flight work after process loss.
]

# NodeExecution state is the logical state of one workflow node execution.
# NodeExecutor owns running/completed/failed/waiting transitions; scheduler
# consumes terminal/stable transitions and decides downstream requests.
NodeExecutionStateValue = Literal[
    "created",      # Object allocated, not yet ready for execution.
    "ready",        # Created from a ready request, not yet running.
    "running",      # NodeExecutor is executing the logical operator work.
    "waiting",      # External resume is required before graph can advance.
    "completed",    # Final logical output is available.
    "failed",       # Final logical error is available.
    "skipped",      # Scheduler determined this path should not execute.
    "cancelled",    # Caller/framework cancelled before completion.
    "interrupted",  # Process loss interrupted a running node execution.
]

# One logical OperatorExecution belongs to one NodeExecution. Direct executions
# describe retry/fallback attempts; map/replication use one parallel summary.
OperatorExecutionStateValue = Literal[
    "running",
    "completed",
    "failed",
    "interrupted",
]

DirectOperatorExecutionReason = Literal[
    "normal",
    "retry",
    "fallback",
    "recovery",
]

ParallelOperatorExecutionKind = Literal[
    "map",
    "replication",
]

# EdgeEvaluation state is written by scheduler after inspecting an outgoing edge.
EdgeEvaluationStateValue = Literal[
    "selected",  # Condition/policy selected this edge.
    "skipped",   # Condition/policy did not select this edge.
    "failed",    # Condition evaluation itself failed.
]

# Edge resolution is the invocation-level control state used only outside loop
# regions. Pending edges are absent from SchedulerContext.edge_resolutions.
EdgeResolutionStateValue = Literal[
    "selected",  # Source completed and this edge condition evaluated true.
    "skipped",   # Source/path was skipped or this edge condition evaluated false.
]
