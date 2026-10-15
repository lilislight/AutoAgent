"""Connection configuration and explicit model capability profiles."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from ..models._base import Model, NonEmpty
from ..models import LLMJsonSchemaFormat, LLMRequest
from .base import LLMUnsupportedRequest


class OpenAIProviderConfig(BaseModel):
    """Runtime connection settings, never serialized into LLMRequest.

    Owned SDK clients have retries disabled. Injected clients retain their own
    transport and retry configuration. Models are selected explicitly in requests.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)
    provider_id: NonEmpty = "openai"
    api_key: SecretStr | None = Field(default=None, repr=False)
    base_url: str = "https://api.openai.com/v1"
    timeout_seconds: float = Field(default=60.0, gt=0, allow_inf_nan=False)
    headers: dict[str, str] = Field(default_factory=dict, repr=False)


class LLMModelProfile(Model):
    """Optional constraints for a particular provider/model combination.

    None means unspecified, not a promise of support. No model-name heuristics
    or automatic parameter dropping are applied when a profile is absent.
    """

    input_modalities: tuple[str, ...] | None = None
    output_modalities: tuple[str, ...] | None = None
    tools: bool | None = None
    json_schema: bool | None = None
    sampling: bool | None = None

    def check(self, request: LLMRequest, *, provider_id: str, protocol: str) -> None:
        from ..models import LLMMessage, LLMToolResult

        unsupported = set()
        if self.input_modalities is not None:
            for item in request.input:
                if isinstance(item, (LLMMessage, LLMToolResult)):
                    unsupported.update(
                        part.type for part in item.content
                        if part.type not in self.input_modalities and part.type != "refusal"
                    )
        output = request.output_options
        modalities = output.modalities if output is not None else ("text",)
        if self.output_modalities is not None:
            unsupported.update(set(modalities) - set(self.output_modalities))
        reason = None
        if unsupported:
            reason = "Model profile does not support modalities: " + ", ".join(sorted(unsupported))
        elif self.tools is False and request.tools:
            reason = "Model profile does not support Tool calling."
        elif self.json_schema is False and output is not None and isinstance(output.text_format, LLMJsonSchemaFormat):
            reason = "Model profile does not support JSON Schema output."
        elif self.sampling is False and (request.temperature is not None or request.top_p is not None):
            reason = "Model profile does not accept sampling parameters."
        if reason:
            raise LLMUnsupportedRequest(reason, provider_id=provider_id, protocol=protocol)


__all__ = ["LLMModelProfile", "OpenAIProviderConfig"]
