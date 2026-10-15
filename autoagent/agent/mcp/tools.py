"""Adapt discovered MCP Tools and content to the existing ToolSet boundary."""

from __future__ import annotations

from copy import deepcopy
import asyncio
from collections.abc import AsyncIterator
import json
from typing import TYPE_CHECKING

from ..models import (
    LLMToolCall, LLMToolDefinition, LLMToolResult,
)
from ..models._base import _check_finite
from ..tools import ToolExecutionError, ToolOutputError
from ..tools.execution import _unique_object, error_result
from .client import MCPClient, MCPClientError
from .streaming import MCPToolStreamChunk
from .content import mcp_content, content_metadata

if TYPE_CHECKING:
    from autoagent.core import Operator
    from mcp_types import CallToolResult, Tool
    from mcp.shared.dispatcher import ProgressFnT


def _no_remote_schema(uri: str):
    from referencing.exceptions import NoSuchResource

    raise NoSuchResource(ref=uri)


def _content(block: dict):
    return mcp_content(block)


def _content_metadata(block: dict) -> dict:
    return content_metadata(block)


class MCPTool:
    """One connection-bound tool; its advertised name may be aliased.

    Never executes or fetches linked resources during conversion. AssetSource
    references need an application media resolver when used as model input.
    Remote input schemas are checked locally without fetching external $refs;
    SDK negotiation and output-schema checks remain the SDK's responsibility.
    """

    def __init__(self, client: MCPClient, description: Tool, *, name: str | None = None) -> None:
        from jsonschema.validators import validator_for
        from referencing import Registry

        self._client = client
        self._generation = client.generation
        self.remote_name = description.name
        self._metadata = description.model_dump(mode="json", by_alias=True, exclude_none=True)
        self._definition = LLMToolDefinition(
            name=description.name if name is None else name,
            parameters=deepcopy(description.input_schema),
            description=description.description,
        )
        schema = self._definition.parameters
        validator_type = validator_for(schema)
        validator_type.check_schema(schema)
        self._validator = validator_type(schema, registry=Registry(retrieve=_no_remote_schema))

    @property
    def definition(self) -> LLMToolDefinition:
        return LLMToolDefinition.model_validate(self._definition.model_dump(mode="python"))

    @property
    def metadata(self) -> dict:
        """Keep MCP output schemas, annotations and metadata available separately."""
        return deepcopy(self._metadata)

    def _result(self, call: LLMToolCall, result: CallToolResult) -> LLMToolResult:
        try:
            raw = result.model_dump(mode="json", by_alias=True, exclude_none=True)
            blocks = raw.get("content", [])
            content = tuple(_content(block) for block in blocks)
            return LLMToolResult(
                call_id=call.call_id, content=content,
                structured_content=result.structured_content, is_error=result.is_error,
                structured_content_present="structured_content" in result.model_fields_set,
                tool_metadata={"mcp": {
                    "server_id": self._client.config.server_id,
                    "tool_name": self.remote_name,
                    "result": {key: value for key, value in raw.items() if key not in {"content", "structuredContent", "isError"}},
                    "content": [_content_metadata(block) for block in blocks],
                }},
            )
        except Exception as error:
            raise ToolOutputError(
                f"MCP Tool {call.name!r} returned invalid content: {error}",
                tool_name=call.name, call_id=call.call_id,
            ) from error

    async def ainvoke(
        self, call: LLMToolCall, *, progress_callback: ProgressFnT | None = None,
    ) -> LLMToolResult:
        call = LLMToolCall.model_validate(call)
        if call.name != self._definition.name:
            return error_result(call, "unknown_tool", f"This Tool is named {self._definition.name!r}.")
        try:
            arguments = json.loads(call.arguments, object_pairs_hook=_unique_object)
            _check_finite(arguments)
            if not isinstance(arguments, dict):
                raise ValueError("Tool arguments must be a JSON object.")
            errors = list(self._validator.iter_errors(arguments))
        except (ValueError, TypeError) as error:
            return error_result(call, "invalid_arguments", str(error))
        except Exception as error:
            raise ToolExecutionError(
                f"MCP Tool {call.name!r} cannot validate its schema: {error}",
                tool_name=call.name, call_id=call.call_id,
            ) from error
        if errors:
            message = "; ".join(
                f"{'.'.join(str(part) for part in error.absolute_path) or 'arguments'}: {error.message}"
                for error in errors
            )
            return error_result(call, "invalid_arguments", message)
        try:
            await self._client.aprepare()
            if self._generation != self._client.generation:
                return error_result(call, "stale_tool", "Tool definition belongs to a previous connection; rediscover before invoking.")
            result = await self._client.call_tool(self.remote_name, arguments, progress_callback=progress_callback)
        except MCPClientError as error:
            if error.code == -32602:
                return error_result(call, "invalid_arguments", str(error))
            raise ToolExecutionError(
                f"MCP Tool {call.name!r} failed: {error}", tool_name=call.name, call_id=call.call_id,
            ) from error
        return self._result(call, result)

    async def astream(self, call: LLMToolCall) -> AsyncIterator[MCPToolStreamChunk]:
        """Yield recent progress, then one authoritative result.

        Closing the generator cancels its RPC and releases lifecycle accounting.
        Progress is an observation; it does not commit remote side effects.
        Keep at most 32 updates, discarding the oldest when consumers are slow.
        SDK notification tasks never wait on a stopped stream consumer.
        """
        call = LLMToolCall.model_validate(call)
        queue = asyncio.Queue(maxsize=32)
        accepting_progress = True
        identity = dict(server_id=self._client.config.server_id,
                        tool_name=call.name, call_id=call.call_id)

        async def progress(value: float, total: float | None, message: str | None):
            if not accepting_progress:
                return
            chunk = MCPToolStreamChunk(
                type="progress", progress=value, total=total, message=message, **identity,
            )
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(chunk)

        task = asyncio.create_task(self.ainvoke(call, progress_callback=progress))
        pending = None
        try:
            while not task.done() or not queue.empty():
                pending = asyncio.create_task(queue.get())
                await asyncio.wait((pending, task), return_when=asyncio.FIRST_COMPLETED)
                if pending.done():
                    yield pending.result()
                else:
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                pending = None
            accepting_progress = False
            yield MCPToolStreamChunk(type="completed", result=task.result(), **identity)
        finally:
            accepting_progress = False
            if pending is not None:
                pending.cancel()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, *(() if pending is None else (pending,)), return_exceptions=True)

    def as_operator(self, *, id: str | None = None, streaming: bool = False) -> Operator:
        from autoagent.core import Operator

        async def invoke(call: LLMToolCall) -> LLMToolResult:
            return await self.ainvoke(call)

        return Operator(self.astream if streaming else invoke, id=id or self.definition.name)


__all__ = ["MCPTool"]
