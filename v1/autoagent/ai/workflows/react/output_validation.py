from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import TypeAdapter, ValidationError

from autoagent.ai.models.llm import LLMResponse
from autoagent.ai.models.react import OutputValidationResult


class StructuredOutputRepairExhausted(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class OutputValidator:
    response_format: Any | None
    max_parse_retries: int

    def validate(
        self,
        response: LLMResponse,
        previous_failures: int,
    ) -> OutputValidationResult:
        content = response.message.content
        if self.response_format is None:
            return OutputValidationResult(
                response=response,
                valid=True,
                value=content,
            )
        try:
            if content is None:
                raise ValueError("Structured response content is missing.")
            adapter = TypeAdapter(self.response_format)
            value = adapter.validate_json(content)
            normalized = adapter.dump_python(value, mode="json")
            return OutputValidationResult(
                response=response,
                valid=True,
                value=normalized,
            )
        except (ValidationError, ValueError, TypeError) as exc:
            if previous_failures >= self.max_parse_retries:
                raise StructuredOutputRepairExhausted(
                    "Structured output remained invalid after "
                    f"{self.max_parse_retries} repair attempt(s): {exc}"
                ) from exc
            return OutputValidationResult(
                response=response,
                valid=False,
                error=str(exc),
            )

    def finish(self, result: OutputValidationResult) -> Any:
        return result.value
