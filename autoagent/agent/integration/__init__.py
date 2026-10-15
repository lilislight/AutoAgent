"""Thin adapters to the Core's existing execution contracts."""

from .llm import LLMStreamReducer, LLMStreamState
from .tools import tool_operator
from .mcp import MCPToolStreamReducer, MCPToolStreamState

__all__ = ["LLMStreamReducer", "LLMStreamState", "tool_operator", "MCPToolStreamReducer", "MCPToolStreamState"]
