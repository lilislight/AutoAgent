"""Stable Workflow-authoring API.

The names in ``__all__`` are the root-package contract intended for Workflow
authors and Coding Agents. Runtime hosting, persistence, Server, compiler, and
execution objects remain available from their owning modules, but are not part
of this authoring contract.

Some historical root attributes are still imported below so existing
feature-testing examples keep running. Their presence does not make them part
of the stable public API; only ``__all__`` defines that contract.
"""

from autoagent.core.app import AutoAgentApp, AutoAgentSettings
from autoagent.core.operators import (
    Capability,
    Operator,
    OperatorContract,
    OperatorContractWarning,
    ParameterContract,
    SchemaContract,
    StreamReducer,
    StreamingResult,
    streaming_result,
)
from autoagent.core.workflow import (
    BackoffPolicy,
    CapabilityRef,
    CapabilitySelectionPolicy,
    Edge,
    FailurePolicy,
    InputMapping,
    HookVersion,
    MapPolicy,
    Node,
    NodePolicy,
    OutputBinding,
    OperatorRef,
    ResourcePolicy,
    ReplicationPolicy,
    RecoveryPolicy,
    RetryPolicy,
    SystemCommand,
    TimeoutPolicy,
    UserEventMapping,
    Workflow,
    WorkflowPolicy,
    workflow_hook,
)
from autoagent.core.runtime import (
    ArtifactPolicy,
    ArtifactRef,
    ConditionContext,
    DatabaseBackend,
    InputMappingContext,
    JsonRuntimeSerializer,
    MapAggregationContext,
    MapItemSelectionContext,
    OutputBindingContext,
    PersistenceAdmissionError,
    PersistenceHealth,
    PersistencePolicy,
    ReplicationAggregationContext,
    RuntimeRetentionPolicy,
    RuntimeStore,
)
from autoagent.core.server import AutoAgentServer

__all__ = [
    "ArtifactRef",
    "BackoffPolicy",
    "CapabilityRef",
    "CapabilitySelectionPolicy",
    "ConditionContext",
    "FailurePolicy",
    "InputMapping",
    "InputMappingContext",
    "MapPolicy",
    "MapAggregationContext",
    "MapItemSelectionContext",
    "NodePolicy",
    "OutputBinding",
    "OutputBindingContext",
    "ReplicationAggregationContext",
    "ResourcePolicy",
    "ReplicationPolicy",
    "RecoveryPolicy",
    "RetryPolicy",
    "SystemCommand",
    "StreamReducer",
    "StreamingResult",
    "TimeoutPolicy",
    "UserEventMapping",
    "Workflow",
    "WorkflowPolicy",
    "workflow_hook",
    "streaming_result",
]
