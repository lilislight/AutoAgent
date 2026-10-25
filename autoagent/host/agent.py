"""Optional environment-driven Agent resources owned by Host, outside Core."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr


class AgentSettings(BaseModel):
    """Connection and request defaults; credentials never enter requests."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    provider: str = Field(default="openai", min_length=1, pattern=r"\S")
    backend: Literal["openai", "litellm"] = "openai"
    model: str | None = Field(default=None, min_length=1, pattern=r"\S")
    api_key: SecretStr | None = Field(default=None, repr=False)
    base_url: str | None = None
    protocol: Literal["openai.chat_completions", "openai.responses"] = "openai.chat_completions"
    timeout_seconds: float = Field(default=60, gt=0, allow_inf_nan=False)
    streaming: bool = False
    max_output_tokens: int | None = Field(default=None, ge=1)

    @classmethod
    def from_environment(cls, environment, *, embedding=False):
        prefix = "AUTOAGENT_EMBEDDING_" if embedding else "AUTOAGENT_LLM_"
        # Embeddings still require a default model. Generation may choose its
        # model in each Node, independently of Host connection setup.
        model = environment.get(prefix + "MODEL", "").strip()
        if embedding and not model:
            return None
        fields = {"PROVIDER": "provider", "BACKEND": "backend", "MODEL": "model",
                  "API_KEY": "api_key", "BASE_URL": "base_url", "TIMEOUT_SECONDS": "timeout_seconds"}
        if not embedding:
            fields.update(PROTOCOL="protocol", STREAMING="streaming", MAX_OUTPUT_TOKENS="max_output_tokens")
        values = {field: environment[prefix + name].strip() for name, field in fields.items()
                  if environment.get(prefix + name, "").strip()}
        return cls(**values) if values else None

    def create_provider(self, *, embedding=False):
        """Construct lazily; Host enters and closes the resource on its Runtime loop."""
        from autoagent.agent import (
            DeepSeekProvider, DeepSeekProviderConfig, LiteLLMEmbeddingProvider,
            LiteLLMProvider, LiteLLMProviderConfig, OpenAIChatCompletionsProvider,
            OpenAIEmbeddingProvider, OpenAIProviderConfig, OpenAIResponsesProvider,
        )

        if self.api_key is None or not self.api_key.get_secret_value().strip():
            name = "AUTOAGENT_EMBEDDING_API_KEY" if embedding else "AUTOAGENT_LLM_API_KEY"
            raise ValueError(f"Set {name} in the project's .env or process environment.")
        connection = dict(provider_id=self.provider, api_key=self.api_key, timeout_seconds=self.timeout_seconds)
        if self.base_url is not None:
            connection["base_url"] = self.base_url
        if self.backend == "litellm":
            config = LiteLLMProviderConfig(model_prefix=self.provider, **connection)
            return LiteLLMEmbeddingProvider(config) if embedding else LiteLLMProvider(config, protocol=self.protocol)
        if not embedding and self.provider == "deepseek":
            return DeepSeekProvider(DeepSeekProviderConfig(**connection), protocol=self.protocol)
        if self.provider != "openai" and self.base_url is None:
            raise ValueError("Set an explicit BASE_URL for an OpenAI-compatible vendor, or use BACKEND=litellm.")
        config = OpenAIProviderConfig(**connection)
        if embedding:
            return OpenAIEmbeddingProvider(config)
        provider = OpenAIResponsesProvider if self.protocol == "openai.responses" else OpenAIChatCompletionsProvider
        return provider(config)


_UNSET = object()


def llm_request(*, input, model=_UNSET, options=None, **kwargs):
    """Build a complete LLMRequest from Host's merged environment and explicit overrides.

    Call in Input Mapping or an Operator, while Host's environment scope is open.
    Passing model explicitly also works without environment configuration.
    Explicit model=None requests resolver selection instead of the env default.
    Explicit streaming=False and max_output_tokens=None override env defaults.
    """
    from autoagent.agent import LLMProviderOptions, LLMRequest

    settings = AgentSettings.from_environment(os.environ)
    selected = (settings.model if settings else None) if model is _UNSET else model
    if selected is None and model is _UNSET:
        raise ValueError("Set AUTOAGENT_LLM_MODEL, pass model, or pass model=None for resolver selection.")
    if settings:
        kwargs.setdefault("streaming", settings.streaming)
        if settings.max_output_tokens is not None:
            kwargs.setdefault("max_output_tokens", settings.max_output_tokens)
    if options is not None:
        if settings is None:
            raise ValueError("Protocol options require Agent settings; otherwise pass provider_options explicitly.")
        if "provider_options" in kwargs:
            raise ValueError("Use options or provider_options, not both.")
        kwargs["provider_options"] = LLMProviderOptions(
            protocol=settings.protocol, provider_id=settings.provider, values=options)
    return LLMRequest(model=selected, input=input, **kwargs)


def embedding_request(*, input, model=None, **kwargs):
    """Build a complete EmbeddingRequest without exposing connection credentials."""
    from autoagent.agent import EmbeddingRequest

    settings = AgentSettings.from_environment(os.environ, embedding=True)
    selected = model or (settings.model if settings else None)
    if selected is None:
        raise ValueError("Set AUTOAGENT_EMBEDDING_MODEL or pass model to embedding_request().")
    return EmbeddingRequest(model=selected, input=input, **kwargs)


@asynccontextmanager
async def agent_resources(setup):
    """Bind configured defaults after project setup, preserving custom implementations."""
    from contextlib import AsyncExitStack
    from autoagent.agent import LLM_CALL, EMBEDDING, embedding_operator, llm_call_operator

    async with AsyncExitStack() as stack:
        for capability, embedding in ((LLM_CALL, False), (EMBEDDING, True)):
            if any(item.capability_id == capability.id and item.enabled for item in setup.operators):
                continue
            settings = AgentSettings.from_environment(setup.environment, embedding=embedding)
            if settings is None or (settings.model is None and settings.api_key is None):
                continue
            provider = await stack.enter_async_context(settings.create_provider(embedding=embedding))
            identity = f"{capability.id}.{settings.provider}.{settings.backend}"
            operator = (embedding_operator(provider, id=identity) if embedding else
                        llm_call_operator(provider, id=identity))
            setup.register_operator(operator, capability_id=capability.id, default=True)
        yield
