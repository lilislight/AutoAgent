"""Core exceptions. V2 intentionally exposes no legacy compatibility errors."""


class AutoAgentError(Exception):
    """Base class for V2 Core failures."""


class WorkflowCompileError(AutoAgentError):
    """The Workflow cannot be compiled into executable IR."""

    def __init__(self, message: str, *, code: str | None = None) -> None:
        self.code = code
        super().__init__(f"{code}: {message}" if code else message)


class LoopControlError(AutoAgentError):
    """Selected graph transitions are incompatible for the active Loop scope."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


class NodeExecutionLimitExceededError(AutoAgentError):
    """A Node exceeded its Invocation-local execution safety limit."""

    code = "NODE_EXECUTION_LIMIT_EXCEEDED"

    def __init__(
        self,
        *,
        node_id: str,
        attempted: int,
        allowed: int,
        scope: str,
    ) -> None:
        self.node_id = node_id
        self.attempted = attempted
        self.allowed = allowed
        self.scope = scope
        super().__init__(
            f"{self.code}: Node {node_id!r} attempted execution {attempted}, "
            f"exceeding the Node execution limit {allowed} in iteration scope {scope}."
        )


class WorkflowRegistrationError(AutoAgentError):
    """The Workflow conflicts with the App registry."""


class WorkflowNotRegisteredError(AutoAgentError):
    """The requested Workflow is not registered in this App."""


class InvocationConflictError(AutoAgentError):
    """A Session already owns an active Invocation."""


class InvocationStateError(AutoAgentError):
    """The requested operation is invalid for the Invocation state."""


class AdmissionRejectedError(AutoAgentError):
    """The RuntimeSink rejected or timed out new-execution admission."""


class SinkDeliveryError(AutoAgentError):
    """The RuntimeSink failed before accepting Event ownership."""


class RecoveryError(AutoAgentError):
    """A RecoveryCheckpoint is invalid for the current App."""
