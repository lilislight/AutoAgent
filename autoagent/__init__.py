"""Stable authoring and invocation API for AutoAgent V2."""

from .core.commands import Wait
from .core.app import (
    AppCheckpoint,
    AutoAgentApp,
    CheckpointLoadResult,
    InvocationRef,
    InvocationResult,
    InvocationStatus,
    InvocationStream,
    InvocationSubmission,
    InvocationUpdate,
    InvocationWait,
    StreamItem,
)
from .core.compiler import (
    CompileResult,
    Diagnostic,
    WorkflowCompiler,
    WorkflowDefinitionSnapshot,
)
from .core.context import ContextOperation, ContextPatch
from .core.errors import (
    AutoAgentError,
    RuntimeInfrastructureError,
    RuntimeTransitionError,
    WorkflowCompileError,
)
from .core.operators import Operator, StreamReducer, ValueContract
from .core.runtime import SessionCheckpoint, RuntimeErrorInfo, UserEvent
from .core.workflow import (
    AggregationContext,
    Capability,
    ConditionContext,
    Context,
    Edge,
    ErrorInfo,
    InputMappingContext,
    Map,
    Node,
    OutputBindingContext,
    Recovery,
    Stream,
    StreamContext,
    SubWorkflow,
    UserEventMapping,
    Workflow,
    workflow_hook,
)

__all__ = [
    "AggregationContext",
    "AppCheckpoint",
    "AutoAgentApp",
    "AutoAgentError",
    "Capability",
    "CheckpointLoadResult",
    "CompileResult",
    "ConditionContext",
    "Context",
    "ContextOperation",
    "ContextPatch",
    "Diagnostic",
    "Edge",
    "ErrorInfo",
    "InputMappingContext",
    "InvocationRef",
    "InvocationResult",
    "InvocationStatus",
    "InvocationStream",
    "InvocationSubmission",
    "InvocationUpdate",
    "InvocationWait",
    "StreamItem",
    "Map",
    "Node",
    "Operator",
    "OutputBindingContext",
    "Recovery",
    "SessionCheckpoint",
    "RuntimeErrorInfo",
    "RuntimeInfrastructureError",
    "RuntimeTransitionError",
    "Stream",
    "StreamContext",
    "StreamReducer",
    "SubWorkflow",
    "UserEvent",
    "UserEventMapping",
    "ValueContract",
    "Wait",
    "Workflow",
    "WorkflowCompileError",
    "WorkflowCompiler",
    "WorkflowDefinitionSnapshot",
    "workflow_hook",
]

from .core import RuntimeGraphCheckpoint
__all__ += ["RuntimeGraphCheckpoint"]

from .core.commands import SystemCommand, Spawn, Await, RuntimeHandle, RuntimeObservation, RuntimeWait
__all__ += ["SystemCommand", "Spawn", "Await", "RuntimeHandle", "RuntimeObservation", "RuntimeWait"]

from .core.commands import Resume, ResumeRequest, ResumeReceipt, Status
__all__ += ["Resume", "ResumeRequest", "ResumeReceipt", "Status"]

from .core.commands import Cancel, CancelRequest, CancelReceipt, AwaitAny, AwaitAnyRequest, Timer, TimerRequest, TimerResult
__all__ += ["Cancel", "CancelRequest", "CancelReceipt", "AwaitAny", "AwaitAnyRequest", "Timer", "TimerRequest", "TimerResult"]

from .core.commands import SignalEndpoint, SignalLimits, SendSignal, SignalReceipt, SignalMessage, ReceiveSignal, SignalBatch
__all__ += ['SignalEndpoint', 'SignalLimits', 'SendSignal', 'SignalReceipt', 'SignalMessage', 'ReceiveSignal', 'SignalBatch']

from .core.commands import SelfHandle, OwnerHandle
__all__ += ["SelfHandle", "OwnerHandle"]

from .core.commands import AwaitSignal, SignalCase, TimerCase, ChildCase, Select, SelectResult, SuspensionInfo
__all__ += ["AwaitSignal", "SignalCase", "TimerCase", "ChildCase", "Select", "SelectResult", "SuspensionInfo"]
