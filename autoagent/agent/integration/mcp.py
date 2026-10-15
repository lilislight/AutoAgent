"""Reduce MCP progress without retaining a history in Runtime State."""

from __future__ import annotations

from autoagent.core.workflow import StreamContext

from ..models import LLMToolResult
from ..models._base import Model
from ..mcp.streaming import MCPToolStreamChunk


class MCPToolStreamState(Model):
    result: LLMToolResult | None = None


class MCPToolStreamReducer:
    def initial(self, context: StreamContext) -> MCPToolStreamState:
        return MCPToolStreamState()

    def add(
        self, context: StreamContext, state: MCPToolStreamState, chunk: MCPToolStreamChunk,
    ) -> MCPToolStreamState:
        if state.result is not None:
            raise ValueError("MCP stream emitted data after its final result.")
        return MCPToolStreamState(result=chunk.result) if chunk.type == "completed" else state

    def finish(self, context: StreamContext, state: MCPToolStreamState) -> LLMToolResult:
        if state.result is None:
            raise ValueError("MCP stream ended without its final result.")
        return state.result


__all__ = ["MCPToolStreamReducer", "MCPToolStreamState"]
