"""HTTP contracts for the Workflow definition browser."""

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator
from typing import Literal, Annotated


class ExecutionAction(BaseModel):
    model_config=ConfigDict(extra='forbid',strict=True)
    invocation_id: str=Field(min_length=1,max_length=4096)
    request_id: str=Field(min_length=1,max_length=128,pattern='^[A-Za-z0-9_-]+$')


class PauseExecution(ExecutionAction):
    action: Literal['pause']


class RecoverExecution(ExecutionAction):
    action: Literal['recover']
    expected_version: int=Field(ge=0,le=9007199254740991)


class CancelExecution(ExecutionAction):
    action: Literal['cancel']
    reason: str | None=Field(None,max_length=4096)


class RespondToWait(ExecutionAction):
    action: Literal['respond']
    target_invocation_id: str=Field(min_length=1,max_length=4096)
    wait_id: str=Field(min_length=1,max_length=4096)
    payload: JsonValue


class SendExecutionSignal(ExecutionAction):
    action: Literal['signal']
    endpoint: str=Field(min_length=1,max_length=512)
    source_id: str=Field(min_length=1,max_length=128)
    sequence: int=Field(ge=1,le=9007199254740991)
    payload: JsonValue


ExecutionActionRequest=Annotated[PauseExecution|RecoverExecution|CancelExecution|RespondToWait|SendExecutionSignal,Field(discriminator='action')]


class RunWorkflowRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    workflow_id: str = Field(min_length=1, max_length=512)
    revision_id: str = Field(min_length=1, max_length=4096)
    entry_node_id: str = Field(min_length=1, max_length=4096)
    input: JsonValue
    session_mode: Literal["new", "existing"] = "new"
    session_id: str | None = Field(None, min_length=1, max_length=512)
    session_context: dict[str, JsonValue] | None = None

    @model_validator(mode="after")
    def validate_session(self):
        if self.session_mode == "existing" and not self.session_id:
            raise ValueError("Choose a Session ID when reusing a Session.")
        if self.session_mode == "existing" and self.session_context is not None:
            raise ValueError("Session context can only be set for a new Session.")
        return self


class WorkflowSummary(BaseModel):
    model_config = ConfigDict(extra="ignore")

    workflow_id: str
    workflow_version: str
    revision_id: str
    runnable: bool
    definition_hash: str | None = None
    registered_at_us: int | None = None


class WorkflowPage(BaseModel):
    items: list[WorkflowSummary]
    next_cursor: str | None
    frontier: int | None = None


class WorkflowCatalogEntry(BaseModel):
    workflow_id: str
    workflow_version: str
    revision_id: str
    current_revision_id: str | None
    matched_revision_id: str | None

    model_config = ConfigDict(extra="ignore")


class WorkflowCatalogPage(BaseModel):
    items: list[WorkflowCatalogEntry]
    next_cursor: str | None


class WorkflowDetail(BaseModel):
    schema_version: int
    workflow_id: str
    workflow_version: str
    workflow_revision_id: str
    definition_hash: str
    definition: dict[str, object]
    runnable: bool


class GraphNode(BaseModel):
    id: str
    executable: dict[str, str]
    mapped: bool


class GraphEdge(BaseModel):
    id: str
    source: str
    target: str
    on: Literal["complete", "error"]
    conditional: bool


class GraphDefinition(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]
    entry_node_ids: list[str]
    exit_node_ids: list[str]


class WorkflowGraph(WorkflowDetail):
    definition: GraphDefinition
    registered_at_us: int | None = None


class TracePage(BaseModel):
    items: list[dict[str, object]]
    next_cursor: str | None
    frontier: int
    total_count: int | None = None


class CreateCheckpointRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    session_id: str = Field(min_length=1, max_length=4096)
    sequence: int | None = Field(None, ge=1, le=9007199254740991)
    pinned: bool = False


class PinCheckpointRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    pinned: bool


class ForkRequest(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    invocation_id: str = Field(min_length=1, max_length=4096)
    sequence: int = Field(ge=1, le=9007199254740991)
    session_id: str | None = Field(None, min_length=1, max_length=4096)
