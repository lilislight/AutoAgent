"""Stable public surface for the standalone V2 Core."""

from .app import AutoAgentApp
from .compiler import WorkflowCompiler
from .errors import *
from .operators import Operator, StreamReducer, WaitOperator
from .runtime import (
    Event,
    EventMode,
    Invocation,
    InvocationSnapshot,
    InvocationState,
    RuntimeErrorInfo,
    RuntimeEvent,
    RuntimeSink,
    SinkPressure,
    StateOperation,
    Session,
    UserEvent,
    AsyncInvocationStream,
    InvocationStream,
    RecoveryCheckpoint,
)
from .workflow import (
    ContextPatch,
    Edge,
    EdgeIR,
    ExecutionContext,
    Node,
    NodeIR,
    UserEventMapping,
    Workflow,
    WorkflowIR,
    SubworkflowIR,
    BackoffPolicy,
    FailurePolicy,
    MapPolicy,
    NodePolicy,
    RecoveryPolicy,
    ReplicationPolicy,
    ResourcePolicy,
    RetryPolicy,
    StreamPolicy,
    TimeoutPolicy,
    WorkflowPolicy,
)

_default_app = AutoAgentApp()


def get_default_app() -> AutoAgentApp:
    """Return the process-local V2 App without starting execution resources."""

    return _default_app


__all__ = [
    "AdmissionRejectedError",
    "AsyncInvocationStream",
    "AutoAgentError",
    "AutoAgentApp",
    "ContextPatch",
    "Edge",
    "EdgeIR",
    "Event",
    "EventMode",
    "ExecutionContext",
    "Invocation",
    "InvocationConflictError",
    "InvocationSnapshot",
    "InvocationState",
    "InvocationStateError",
    "InvocationStream",
    "LoopControlError",
    "NodeExecutionLimitExceededError",
    "Operator",
    "Node",
    "NodeIR",
    "RecoveryCheckpoint",
    "RecoveryError",
    "RuntimeErrorInfo",
    "RuntimeEvent",
    "RuntimeSink",
    "Session",
    "SinkPressure",
    "SinkDeliveryError",
    "StateOperation",
    "StreamReducer",
    "UserEvent",
    "UserEventMapping",
    "WaitOperator",
    "Workflow",
    "WorkflowCompiler",
    "WorkflowCompileError",
    "WorkflowIR",
    "WorkflowNotRegisteredError",
    "WorkflowRegistrationError",
    "SubworkflowIR",
    "BackoffPolicy",
    "FailurePolicy",
    "MapPolicy",
    "NodePolicy",
    "RecoveryPolicy",
    "ReplicationPolicy",
    "ResourcePolicy",
    "RetryPolicy",
    "StreamPolicy",
    "TimeoutPolicy",
    "WorkflowPolicy",
    "get_default_app",
]
