"""Typed MCP resource/prompt reads, independent of model-facing Tool calls."""

from __future__ import annotations

from typing import Literal

from autoagent.core import Operator

from ..models._base import Model, NonEmpty
from .client import MCPClient
from .content import MCPResourceResponse, MCPPromptResponse


class MCPReadResourceRequest(Model):
    uri: NonEmpty


class MCPGetPromptRequest(Model):
    name: NonEmpty
    arguments: dict[str, str] | None = None


class MCPOperations:
    """Borrow the client's existing lifecycle, authentication and error semantics."""

    def __init__(self, client: MCPClient):
        self.client = client

    async def aread_resource(self, request: MCPReadResourceRequest) -> MCPResourceResponse:
        request = MCPReadResourceRequest.model_validate(request)
        return MCPResourceResponse.model_validate(await self.client.read_resource_content(request.uri))

    async def aget_prompt(self, request: MCPGetPromptRequest) -> MCPPromptResponse:
        request = MCPGetPromptRequest.model_validate(request)
        return MCPPromptResponse.model_validate(await self.client.get_prompt_messages(request.name, request.arguments))

    def as_operator(self, operation: Literal["read_resource", "get_prompt"], *, id: str | None = None) -> Operator:
        if operation not in {"read_resource", "get_prompt"}:
            raise ValueError("Unknown MCP operation.")
        return Operator(getattr(self, "a" + operation), id=id or "mcp." + operation)


__all__ = ["MCPReadResourceRequest", "MCPGetPromptRequest", "MCPOperations"]
