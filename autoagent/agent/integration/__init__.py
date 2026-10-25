"""Thin adapters to the Core's existing execution contracts."""

from .llm import LLMStreamReducer, LLMStreamState
from .tools import tool_operator
from .mcp import MCPToolStreamReducer, MCPToolStreamState
from .capabilities import LLM_CALL, EMBEDDING, LLMSelection, LLMResolver, llm_call_operator, embedding_operator

__all__ = [
    "LLMStreamReducer", "LLMStreamState", "tool_operator",
    "MCPToolStreamReducer", "MCPToolStreamState",
    "LLM_CALL", "EMBEDDING", "LLMSelection", "LLMResolver", "llm_call_operator", "embedding_operator",
]
