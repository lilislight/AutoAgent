from __future__ import annotations

from typing import Literal

ExecutionMode = Literal["normal", "recovery"]

# Invocation state is the coarse state of one app.invoke()/workflow.invoke()
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
    "running",      # NodeExecutor is executing OperatorCalls.
    "waiting",      # External resume is required before graph can advance.
    "completed",    # Final logical output is available.
    "failed",       # Final logical error is available.
    "skipped",      # Scheduler determined this path should not execute.
    "cancelled",    # Caller/framework cancelled before completion.
    "interrupted",  # Process loss interrupted a running node execution.
]

# OperatorCall state is the concrete operator-call state inside one
# NodeExecution. NodeExecutor updates this record for tracing and recovery.
OperatorCallStateValue = Literal[
    "created",      # Call record allocated, operator not yet called.
    "running",      # Operator call is in progress.
    "completed",    # This concrete call returned output.
    "failed",       # This concrete call raised/returned an error.
    "interrupted",  # Process loss happened while this call was running.
]

# OperatorCallKind explains why NodeExecutor created a concrete operator
# call. It affects aggregation/retry behavior but is not visible as graph state.
OperatorCallKind = Literal[
    "normal",    # First ordinary call for a NodeExecution.
    "retry",     # Repeated call after a failure under RetryPolicy.
    "fallback",  # Call using a fallback selected operator.
    "map_item",  # Per-item call created by EdgePolicy.map.
    "replica",   # Parallel/sample call created by ReplicationPolicy.
    "recover",   # Recovery-specific call after an interruption.
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
