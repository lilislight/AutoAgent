"""Core exceptions. V2 intentionally exposes no legacy compatibility errors."""


class AutoAgentError(Exception):
    """Base class for V2 Core failures."""


class WorkflowCompileError(AutoAgentError):
    """The Workflow cannot be compiled into executable IR."""

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        diagnostics: tuple[object, ...] = (),
        object_type: str | None = None,
        object_id: str | None = None,
        field: str | None = None,
        hint: str | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.diagnostics = diagnostics
        self.object_type = object_type
        self.object_id = object_id
        self.field = field
        self.hint = hint
        super().__init__(f"{code}: {message}" if code else message)


class LoopControlError(AutoAgentError):
    """Selected graph transitions are incompatible for the active Loop scope."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


class RuntimeTransitionError(AutoAgentError):
    """A Runtime Event cannot be atomically applied to the current State."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


class RuntimeInfrastructureError(AutoAgentError):
    """A hosting dependency failed after Core produced canonical progress."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
