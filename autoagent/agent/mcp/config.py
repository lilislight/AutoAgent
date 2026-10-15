"""MCP connection settings; never part of LLM inputs or Runtime State."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..models import URLSource
from ..models._base import NonEmpty


class _Config(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    server_id: NonEmpty
    request_timeout_seconds: float = Field(default=60, gt=0, allow_inf_nan=False)
    connect_timeout_seconds: float = Field(default=180, gt=0, allow_inf_nan=False)
    mode: Literal["auto", "legacy", "2026-07-28"] = "auto"
    input_required_max_rounds: int = Field(default=10, gt=0)
    max_listing_pages: int = Field(default=100, gt=0)


class MCPStdioConfig(_Config):
    type: Literal["stdio"] = "stdio"
    command: NonEmpty
    args: tuple[str, ...] = ()
    env: dict[str, str] | None = Field(default=None, repr=False)
    cwd: str | None = None
    encoding: str = "utf-8"


class _HTTPConfig(_Config):
    url: NonEmpty
    headers: dict[str, str] = Field(default_factory=dict, repr=False)
    timeout_seconds: float = Field(default=30, gt=0, allow_inf_nan=False)
    read_timeout_seconds: float = Field(default=300, gt=0, allow_inf_nan=False)

    @field_validator("url")
    @classmethod
    def _http_url(cls, value: str) -> str:
        URLSource(url=value)
        return value


class MCPSSEConfig(_HTTPConfig):
    type: Literal["sse"] = "sse"


class MCPStreamableHTTPConfig(_HTTPConfig):
    type: Literal["streamable_http"] = "streamable_http"
    terminate_on_close: bool = True
    max_sse_event_size: int | None = Field(default=1024 * 1024, gt=0)


MCPTransportConfig = MCPStdioConfig | MCPSSEConfig | MCPStreamableHTTPConfig

__all__ = [
    "MCPSSEConfig", "MCPStdioConfig", "MCPStreamableHTTPConfig", "MCPTransportConfig",
]
