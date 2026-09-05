from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr


StructuredOutputMode = Literal["auto", "json_schema", "json_object", "prompt"]


class ChatCompletionsConfig(BaseModel):
    """Connection settings for an OpenAI-style Chat Completions endpoint."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    base_url: str = "https://api.openai.com/v1"
    api_key: SecretStr
    default_model: str
    timeout_ms: int = Field(default=60_000, gt=0)
    structured_output_mode: StructuredOutputMode = "auto"
    headers: dict[str, str] = Field(default_factory=dict)
