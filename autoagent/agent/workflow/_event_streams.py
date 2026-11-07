"""Progress wrappers retain the existing Map results and commit boundaries."""

from collections.abc import AsyncIterator

from autoagent.core import StreamContext

from ..models import LLMToolCall, LLMToolResult
from ..models._base import Model
from .models import ToolBranchInput, ToolBranchResult, ToolTask
from .subagents import WaitPreparation
from .tools import branch_tools, execute_tool, prepare_tool_wait


class ToolExecutionChunk(Model):
    call: LLMToolCall
    result: LLMToolResult | None = None


class ToolExecutionState(Model):
    result: LLMToolResult | None = None


async def execute_tool_stream(task: ToolTask) -> AsyncIterator[ToolExecutionChunk]:
    yield ToolExecutionChunk(call=task.call)
    yield ToolExecutionChunk(call=task.call, result=await execute_tool(task))


async def execute_retryable_tool_stream(task: ToolTask) -> AsyncIterator[ToolExecutionChunk]:
    # Core intentionally prohibits Retry after a stream has emitted. A progress
    # wrapper must not change that policy for an otherwise ordinary Tool call.
    yield ToolExecutionChunk(call=task.call, result=await execute_tool(task))


class ToolExecutionReducer:
    def initial(self, context: StreamContext) -> ToolExecutionState:
        return ToolExecutionState()

    def add(self, context: StreamContext, state: ToolExecutionState, chunk: ToolExecutionChunk) -> ToolExecutionState:
        if state.result is not None:
            raise ValueError("Tool stream emitted progress after its result.")
        return ToolExecutionState(result=chunk.result)

    def finish(self, context: StreamContext, state: ToolExecutionState) -> LLMToolResult:
        if state.result is None:
            raise ValueError("Tool stream ended without a result.")
        return state.result


async def branch_stream(value: ToolBranchInput) -> AsyncIterator[ToolBranchResult]:
    yield await branch_tools(value)


class BranchState(Model):
    result: ToolBranchResult | None = None


class BranchReducer:
    def initial(self, context: StreamContext) -> BranchState:
        return BranchState()

    def add(self, context: StreamContext, state: BranchState, chunk: ToolBranchResult) -> BranchState:
        if state.result is not None:
            raise ValueError("Tool classification emitted more than one result.")
        return BranchState(result=chunk)

    def finish(self, context: StreamContext, state: BranchState) -> ToolBranchResult:
        if state.result is None:
            raise ValueError("Tool classification ended without a result.")
        return state.result


class WaitPreparationChunk(Model):
    call: LLMToolCall
    preparation: WaitPreparation


async def prepare_wait_stream(task: ToolTask) -> AsyncIterator[WaitPreparationChunk]:
    yield WaitPreparationChunk(call=task.call, preparation=await prepare_tool_wait(task))


class WaitPreparationState(Model):
    result: WaitPreparation | None = None


class WaitPreparationReducer:
    def initial(self, context: StreamContext) -> WaitPreparationState:
        return WaitPreparationState()

    def add(self, context: StreamContext, state: WaitPreparationState, chunk: WaitPreparationChunk) -> WaitPreparationState:
        if state.result is not None:
            raise ValueError("Wait preparation emitted more than one result.")
        return WaitPreparationState(result=chunk.preparation)

    def finish(self, context: StreamContext, state: WaitPreparationState) -> WaitPreparation:
        if state.result is None:
            raise ValueError("Wait preparation ended without a result.")
        return state.result
