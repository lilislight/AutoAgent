from __future__ import annotations

from collections.abc import Mapping

from autoagent.ai.providers.base import LLMProvider
from autoagent.ai.providers.chat_completions import (
    ChatCompletionsConfig,
    ChatCompletionsProvider,
)
from autoagent.ai.providers.deepseek import DeepSeekConfig, DeepSeekProvider


LLM_PROVIDER_ENV_KEYS = frozenset(
    {
        "AUTOAGENT_LLM_PROVIDER",
        "AUTOAGENT_LLM_MODEL",
        "AUTOAGENT_LLM_TIMEOUT_MS",
        "AUTOAGENT_LLM_STRUCTURED_OUTPUT_MODE",
        "AUTOAGENT_LLM_API_KEY",
        "AUTOAGENT_LLM_BASE_URL",
    }
)


def llm_provider_from_environment(
    environment: Mapping[str, str],
) -> LLMProvider:
    """Build the configured default LLM Provider from one environment snapshot."""

    provider_name = _optional_text(
        environment,
        "AUTOAGENT_LLM_PROVIDER",
    ) or "chat_completions"
    if provider_name not in {"chat_completions", "deepseek"}:
        raise ValueError(
            "AUTOAGENT_LLM_PROVIDER must be 'chat_completions' or 'deepseek'."
        )
    api_key = _required_text(environment, "AUTOAGENT_LLM_API_KEY")
    base_url = _optional_text(environment, "AUTOAGENT_LLM_BASE_URL")
    default_model = _required_text(environment, "AUTOAGENT_LLM_MODEL")
    timeout_ms = _positive_int(
        environment,
        "AUTOAGENT_LLM_TIMEOUT_MS",
        60_000,
    )
    structured_output_mode = _optional_text(
        environment,
        "AUTOAGENT_LLM_STRUCTURED_OUTPUT_MODE",
    ) or "auto"

    if provider_name == "deepseek":
        return DeepSeekProvider(
            DeepSeekConfig(
                api_key=api_key,
                base_url=base_url or "https://api.deepseek.com",
                default_model=default_model,
                timeout_ms=timeout_ms,
                structured_output_mode=(
                    "json_object"
                    if structured_output_mode == "auto"
                    else structured_output_mode
                ),
            )
        )
    if provider_name == "chat_completions":
        return ChatCompletionsProvider(
            ChatCompletionsConfig(
                api_key=api_key,
                base_url=base_url or "https://api.openai.com/v1",
                default_model=default_model,
                timeout_ms=timeout_ms,
                structured_output_mode=structured_output_mode,
            )
        )
    raise AssertionError("Unreachable LLM Provider selection.")


def _optional_text(
    environment: Mapping[str, str],
    key: str,
) -> str | None:
    value = environment.get(key)
    if value is None or not value.strip():
        return None
    return value.strip()


def _required_text(
    environment: Mapping[str, str],
    key: str,
) -> str:
    value = _optional_text(environment, key)
    if value is None:
        raise ValueError(f"{key} is required.")
    return value


def _positive_int(
    environment: Mapping[str, str],
    key: str,
    default: int,
) -> int:
    value = _optional_text(environment, key)
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer.") from exc
    if parsed <= 0:
        raise ValueError(f"{key} must be greater than zero.")
    return parsed
