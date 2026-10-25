"""Boundary checks for the Agent LLM models, without network or provider SDKs."""

from __future__ import annotations

import base64
import fnmatch
import json
from pathlib import Path
import tomllib
import unittest

from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError

import autoagent.agent as agent
from autoagent import AutoAgentApp, Node, Operator, ValueContract, Workflow
from autoagent.agent import (
    AssetSource, AudioContent, DataSource, FileContent, ImageContent,
    LLMAudioOutput, LLMJsonFormat, LLMJsonSchemaFormat, LLMMessage,
    LLMNamedToolChoice, LLMNativeData, LLMOutputOptions, LLMProviderItem,
    LLMProviderOptions, LLMReasoning, LLMRequest, LLMResponse, LLMSource,
    LLMTextFormat, LLMToolCall, LLMToolDefinition, LLMToolResult, LLMUsage,
    LLMVoiceReference, ProviderSource, RefusalContent, TextContent,
    URLSource, VideoContent,
)


class Answer(BaseModel):
    model_config = ConfigDict(extra="forbid")
    answer: str


def user_message(text: str = "Hello") -> LLMMessage:
    return LLMMessage(role="user", content=(TextContent(text=text),))


def function_definition() -> LLMToolDefinition:
    return LLMToolDefinition(
        name="lookup",
        parameters={
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
            "additionalProperties": False,
        },
        strict=True,
    )


def native(protocol="openai.responses", **payload) -> LLMNativeData:
    return LLMNativeData(provider_id="primary", protocol=protocol, payload=payload)


def fake_llm(request: LLMRequest) -> LLMResponse:
    """Exercise Core's actual request restoration and response serialization."""
    assert isinstance(request.input[0], LLMMessage)
    assert isinstance(request.input[0].content[0], TextContent)
    return LLMResponse(
        model=request.model,
        output=(LLMMessage(role="assistant", content=(TextContent(text="Answer"),)),),
        finish_reason="stop",
        usage=LLMUsage(input_tokens=2, output_tokens=1, total_tokens=3),
    )


class LLMSourceTests(unittest.TestCase):
    def test_all_source_variants_round_trip_without_io(self) -> None:
        """Preserve URLs, inline bytes, assets and connection-scoped file IDs."""
        sources = (
            URLSource(url="https://example.com/report.pdf"),
            DataSource(data=base64.b64encode(b"%PDF").decode(), mime_type="application/pdf"),
            AssetSource(uri="asset://report", mime_type="application/pdf"),
            ProviderSource(provider_id="primary", file_id="file_123"),
        )
        adapter = TypeAdapter(LLMSource)
        for source in sources:
            with self.subTest(source=source.type):
                self.assertEqual(adapter.validate_json(source.model_dump_json()), source)

    def test_source_discriminator_rejects_conflicting_locations(self) -> None:
        """Reject sources that mix URL, inline data and provider file fields."""
        adapter = TypeAdapter(LLMSource)
        for value in (
            {"type": "url", "url": "https://example.com/a", "file_id": "file_1"},
            {"type": "data", "data": "YQ==", "mime_type": "image/png", "url": "https://example.com/a"},
            {"type": "unknown", "uri": "asset://a"},
        ):
            with self.subTest(value=value), self.assertRaises(ValidationError):
                adapter.validate_python(value)

    def test_invalid_urls_base64_and_mime_types_are_rejected(self) -> None:
        """Catch malformed media references before a Provider tries to resolve them."""
        for url in ("relative.png", "file:///tmp/a", "data:image/png;base64,YQ==", "https:///a", "https://a:b/a", "https://a/white space"):
            with self.subTest(url=url), self.assertRaises(ValidationError):
                URLSource(url=url)
        for data in ("not base64!", "data:image/png;base64,YQ==", "☃", b"YQ=="):
            with self.subTest(data=data), self.assertRaises(ValidationError):
                DataSource(data=data, mime_type="image/png")
        with self.assertRaises(ValidationError):
            DataSource(data="YQ==", mime_type="pdf")

    def test_provider_file_reference_requires_connection_identity(self) -> None:
        """A bare provider file ID cannot lose its connection ownership."""
        for provider_id in ("", "   "):
            with self.subTest(provider_id=provider_id), self.assertRaises(ValidationError):
                ProviderSource(provider_id=provider_id, file_id="file_123")


class LLMRequestTests(unittest.TestCase):
    def test_multimodal_request_preserves_every_content_type_and_order(self) -> None:
        """Round-trip mixed text, image, audio, video and PDF without flattening."""
        parts = (
            TextContent(text="Compare the provided materials."),
            ImageContent(source=URLSource(url="https://example.com/image.png"), detail="original"),
            AudioContent(source=DataSource(data="YQ==", mime_type="audio/wav")),
            VideoContent(source=AssetSource(uri="asset://clip"), start_seconds=1.0, end_seconds=3.0),
            FileContent(source=ProviderSource(provider_id="primary", file_id="file_pdf"), filename="report.pdf", detail="high"),
        )
        request = LLMRequest(
            model="gpt-4.1",
            input=(
                LLMMessage(role="developer", content=(TextContent(text="Use the sources."),)),
                LLMMessage(role="user", content=parts),
            ),
        )
        restored = LLMRequest.model_validate_json(request.model_dump_json())
        self.assertEqual(restored, request)
        self.assertEqual(tuple(part.type for part in restored.input[1].content), ("text", "image", "audio", "video", "file"))

    def test_pdf_is_a_file_not_extracted_text(self) -> None:
        """Keep the original PDF source and filename distinct from plain text."""
        pdf = FileContent(source=DataSource(data="JVBERg==", mime_type="application/pdf"), filename="scan.pdf")
        request = LLMRequest(model="vision-model", input=(LLMMessage(role="user", content=(pdf,)),))
        record = json.loads(request.model_dump_json())
        part = record["input"][0]["content"][0]
        self.assertEqual(part["type"], "file")
        self.assertEqual(part["source"]["mime_type"], "application/pdf")
        self.assertNotIn("text", part)

    def test_instructions_and_refusals_enforce_message_roles(self) -> None:
        """Instruction roles require text; refusal content is assistant-only."""
        image = ImageContent(source=AssetSource(uri="asset://image"))
        for role in ("system", "developer"):
            with self.subTest(role=role), self.assertRaises(ValidationError):
                LLMMessage(role=role, content=(image,))
        with self.assertRaises(ValidationError):
            LLMMessage(role="user", content=(RefusalContent(text="No"),))
        self.assertEqual(user_message("").content[0].text, "")

    def test_video_intervals_reject_invalid_or_nonfinite_ranges(self) -> None:
        """Require an increasing finite interval while allowing an omitted range."""
        for values in (
            {"start_seconds": -1.0}, {"end_seconds": 0.0},
            {"start_seconds": 3.0, "end_seconds": 2.0},
            {"start_seconds": 3.0, "end_seconds": 3.0},
            {"start_seconds": float("nan")}, {"end_seconds": float("inf")},
        ):
            with self.subTest(values=values), self.assertRaises(ValidationError):
                VideoContent(source=AssetSource(uri="asset://clip"), **values)
        self.assertIsNone(VideoContent(source=AssetSource(uri="asset://clip")).end_seconds)

    def test_empty_input_and_invalid_generation_settings_are_rejected(self) -> None:
        """Reject missing prompts, boolean limits and invalid sampling parameters."""
        for values in (
            {"input": ()}, {"model": " "}, {"max_output_tokens": 0},
            {"max_output_tokens": True}, {"temperature": 2.1},
            {"streaming": "true"}, {"streaming": 1},
            {"temperature": float("nan")}, {"top_p": -0.1}, {"top_p": 1.1},
        ):
            arguments = {"model": "model", "input": (user_message(),), **values}
            with self.subTest(values=values), self.assertRaises(ValidationError):
                LLMRequest(**arguments)

    def test_tool_selection_references_unique_advertised_functions(self) -> None:
        """Named and required choices cannot select missing or duplicate Tools."""
        definition = function_definition()
        request = LLMRequest(model="model", input=(user_message(),), tools=(definition,), tool_choice=LLMNamedToolChoice(name="lookup"))
        self.assertEqual(request.tool_choice.name, "lookup")
        for values in (
            {"tools": (definition, definition)}, {"tool_choice": "required"},
            {"tools": (definition,), "tool_choice": LLMNamedToolChoice(name="absent")},
        ):
            with self.subTest(values=values), self.assertRaises(ValidationError):
                LLMRequest(model="model", input=(user_message(),), **values)

    def test_function_names_and_schema_roots_follow_openai_constraints(self) -> None:
        """Validate function names and require object-shaped parameters."""
        for name in ("bad.name", "", "x" * 65):
            with self.subTest(name=name), self.assertRaises(ValidationError):
                LLMToolDefinition(name=name, parameters={"type": "object"})
        with self.assertRaises(ValidationError):
            LLMToolDefinition(name="lookup", parameters={"type": "array"})
        self.assertEqual(LLMToolDefinition(name="1_lookup", parameters={"type": "object"}).name, "1_lookup")

    def test_structured_output_compiles_python_type_to_portable_schema(self) -> None:
        """Store output schema data rather than a Python class in the request."""
        output_format = LLMJsonSchemaFormat.from_type(Answer)
        request = LLMRequest(model="model", input=(user_message(),), output_options=LLMOutputOptions(text_format=output_format))
        restored = LLMRequest.model_validate_json(request.model_dump_json())
        self.assertEqual(restored.output_options.text_format.name, "Answer")
        self.assertEqual(restored.output_options.text_format.json_schema["properties"]["answer"]["type"], "string")
        for schema in ({}, {"type": "array"}, {"anyOf": [{"type": "object"}]}):
            with self.subTest(schema=schema), self.assertRaises(ValidationError):
                LLMJsonSchemaFormat(name="answer", json_schema=schema)

    def test_output_formats_are_discriminated_on_json_restore(self) -> None:
        """Restore text, JSON object and JSON Schema formats as distinct types."""
        for output_format in (
            LLMTextFormat(), LLMJsonFormat(), LLMJsonSchemaFormat.from_type(Answer),
        ):
            options = LLMOutputOptions(text_format=output_format)
            with self.subTest(format=output_format.type):
                restored = LLMOutputOptions.model_validate_json(options.model_dump_json())
                self.assertIsInstance(restored.text_format, type(output_format))

    def test_audio_output_requires_modality_voice_and_format(self) -> None:
        """Audio settings and requested modalities must describe the same output."""
        audio = LLMAudioOutput(voice="alloy", format="wav")
        for values in (
            {"modalities": ()}, {"modalities": ("text", "text")},
            {"modalities": ("audio",)}, {"audio": audio},
        ):
            with self.subTest(values=values), self.assertRaises(ValidationError):
                LLMOutputOptions(**values)
        options = LLMOutputOptions(modalities=("text", "audio"), audio=audio)
        self.assertEqual(LLMOutputOptions.model_validate_json(options.model_dump_json()), options)
        custom = LLMAudioOutput(voice=LLMVoiceReference(id="voice_123"), format="pcm16")
        self.assertEqual(custom.voice.id, "voice_123")

    def test_provider_options_cannot_override_normalized_request_fields(self) -> None:
        """Block nested generation, transcript and stream overrides."""
        for name in ("model", "messages", "input", "instructions", "tools", "temperature", "max_completion_tokens", "response_format", "text", "audio", "stream", "n", "extra_body"):
            with self.subTest(name=name), self.assertRaises(ValidationError):
                LLMProviderOptions(protocol="openai.responses", values={name: None})
        options = LLMProviderOptions(protocol="openai.responses", values={"reasoning": {"effort": "low"}, "store": False})
        self.assertEqual(options.values["reasoning"], {"effort": "low"})

    def test_json_payloads_reject_python_objects_and_nonfinite_numbers(self) -> None:
        """All extension containers remain safe for lossless JSON persistence."""
        for payload in ({"client": object()}, {"nested": [float("nan")]}, {"nested": {"limit": float("inf")}}):
            with self.subTest(payload=payload), self.assertRaises(ValidationError):
                native(**payload)
            with self.subTest(options=payload), self.assertRaises(ValidationError):
                LLMProviderOptions(protocol="openai.responses", values=payload)
        with self.assertRaises(ValidationError):
            LLMToolDefinition(name="lookup", parameters={"type": "object", "default": float("nan")})

    def test_fields_are_frozen_and_unknown_fields_are_rejected(self) -> None:
        """Catch field reassignment and accidental SDK fields at the model boundary."""
        request = LLMRequest(model="model", input=(user_message(),))
        with self.assertRaises(ValidationError):
            request.model = "another"
        with self.assertRaises(ValidationError):
            LLMRequest(model="model", input=(user_message(),), messages=[])
        with self.assertRaises(ValidationError):
            TextContent(text="a", content="b")


class LLMResponseTests(unittest.TestCase):
    def test_reasoning_tool_calls_and_messages_retain_protocol_order(self) -> None:
        """Preserve item identities, encrypted reasoning and malformed arguments."""
        response = LLMResponse(
            model="reasoning-model", finish_reason="tool_calls", provider_response_id="resp_1",
            output=(
                LLMReasoning(id="rs_1", summary=("Using a Tool.",), encrypted_content="opaque", native=native(group_id="turn_1")),
                LLMMessage(role="assistant", content=(TextContent(text="Checking."),)),
                LLMToolCall(id="fc_1", call_id="call_1", name="lookup", arguments='{"query":'),
            ),
        )
        restored = LLMResponse.model_validate_json(response.model_dump_json())
        self.assertEqual(restored, response)
        self.assertEqual(tuple(item.type for item in restored.output), ("reasoning", "message", "tool_call"))
        self.assertEqual(restored.output[0].encrypted_content, "opaque")
        self.assertEqual(restored.tool_calls[0].arguments, '{"query":')
        self.assertNotEqual(restored.tool_calls[0].id, restored.tool_calls[0].call_id)

    def test_response_can_be_returned_in_the_next_request_without_data_loss(self) -> None:
        """Carry reasoning and calls into a new request with a correlated result."""
        response = LLMResponse(model="model", finish_reason="tool_calls", output=(
            LLMReasoning(id="rs_1", encrypted_content="opaque", native=native()),
            LLMToolCall(call_id="call_1", name="lookup", arguments='{"query":"a"}'),
        ))
        observation = LLMToolResult(
            call_id="call_1",
            content=(TextContent(text="Found"), ImageContent(source=AssetSource(uri="asset://image"))),
            structured_content={"count": 1},
        )
        request = LLMRequest(model="model", input=(user_message(), *response.output, observation))
        restored = LLMRequest.model_validate_json(request.model_dump_json())
        self.assertEqual(restored.input[1:3], response.output)
        self.assertEqual(restored.input[-1], observation)

    def test_text_and_media_properties_exclude_reasoning_and_refusals(self) -> None:
        """Derive visible output without mixing reasoning or refusal text into it."""
        response = LLMResponse(model="model", finish_reason="stop", output=(
            LLMReasoning(text="private reasoning", summary=("summary",)),
            LLMMessage(role="assistant", content=(TextContent(text="First"), RefusalContent(text="Refusal"))),
            LLMMessage(role="assistant", content=(TextContent(text=" second"), AudioContent(source=DataSource(data="YQ==", mime_type="audio/wav"), transcript="spoken"))),
        ))
        self.assertEqual(response.text, "First second")
        self.assertEqual(len(response.media), 1)
        self.assertEqual(response.media[0].transcript, "spoken")
        self.assertNotIn("text", response.model_dump())
        self.assertNotIn("media", response.model_dump())

    def test_audio_identity_and_expiry_survive_round_trip(self) -> None:
        """Retain OpenAI audio continuation data separately from its transcript."""
        audio = AudioContent(
            source=DataSource(data="YQ==", mime_type="audio/wav"), transcript="Hello",
            native=native("openai.chat_completions", id="audio_1", expires_at=1900000000),
        )
        response = LLMResponse(model="audio-model", finish_reason="stop", output=(LLMMessage(role="assistant", content=(audio,)),))
        restored = LLMResponse.model_validate_json(response.model_dump_json())
        self.assertEqual(restored.media[0].native.payload["id"], "audio_1")
        self.assertEqual(restored.media[0].native.payload["expires_at"], 1900000000)

    def test_hosted_activity_is_not_a_local_tool_call(self) -> None:
        """Preserve provider-specific activity without accidentally executing it."""
        item = LLMProviderItem(id="ws_1", provider_type="web_search_call", native=native(status="completed"))
        response = LLMResponse(model="model", output=(item,), finish_reason="stop")
        self.assertEqual(response.tool_calls, ())
        self.assertEqual(LLMResponse.model_validate_json(response.model_dump_json()), response)
        with self.assertRaises(ValidationError):
            LLMProviderItem(provider_type="compaction")

    def test_truncated_or_filtered_response_can_preserve_partial_or_empty_output(self) -> None:
        """A terminal model result can be incomplete without fabricating content."""
        response = LLMResponse(model="model", output=(LLMMessage(role="assistant", content=(TextContent(text="Partial"),)),), finish_reason="length", provider_metadata={"status": "incomplete"})
        self.assertEqual(response.text, "Partial")
        self.assertEqual(LLMResponse(model="model", output=(), finish_reason="content_filter").text, "")

    def test_response_rejects_input_messages_and_local_tool_results(self) -> None:
        """Prevent input-only interaction items from masquerading as model output."""
        for item in (user_message(), LLMToolResult(call_id="call_1")):
            with self.subTest(item=item.type), self.assertRaises(ValidationError):
                LLMResponse(model="model", output=(item,), finish_reason="stop")

    def test_usage_preserves_missing_counts_and_rejects_negative_counts(self) -> None:
        """Do not turn missing usage into zero or accept boolean token counts."""
        self.assertIsNone(LLMUsage(input_tokens=0).output_tokens)
        for values in ({"input_tokens": -1}, {"output_tokens": True}):
            with self.subTest(values=values), self.assertRaises(ValidationError):
                LLMUsage(**values)


class LLMCoreBoundaryTests(unittest.TestCase):
    def test_rich_models_round_trip_through_core_value_contracts(self) -> None:
        """Use Core's canonical serialization for full multimodal and native data."""
        request = LLMRequest(
            model="model", input=(user_message(),), tools=(function_definition(),),
            output_options=LLMOutputOptions(text_format=LLMJsonSchemaFormat.from_type(Answer)),
            provider_options=LLMProviderOptions(protocol="openai.responses", values={"reasoning": {"effort": "low"}}),
        )
        response = LLMResponse(model="model", finish_reason="tool_calls", output=(
            LLMReasoning(encrypted_content="opaque", native=native()),
            LLMToolCall(call_id="call_1", name="lookup", arguments="{"),
        ))
        for value in (request, response):
            with self.subTest(model=type(value).__name__):
                contract = ValueContract.create(type(value), location="agent")
                record = contract.to_record(value)
                self.assertEqual(contract.restore(record), value)
                json.dumps(record, allow_nan=False)

    def test_runtime_can_execute_a_model_typed_operator(self) -> None:
        """Run the model contract through actual Node admission and completion."""
        request = LLMRequest(model="mock-model", input=(user_message(),))
        app = AutoAgentApp()
        try:
            result = app.invoke(Workflow("agent-model-boundary", nodes=[Node("llm", Operator(fake_llm))]), request)
            self.assertEqual(result.status, "completed")
            restored = ValueContract.create(LLMResponse, location="response").restore(result.output)
            self.assertEqual(restored.text, "Answer")
            self.assertEqual(restored.usage.total_tokens, 3)
        finally:
            app.close()

    def test_mutated_json_fields_are_revalidated_at_core_boundary(self) -> None:
        """Frozen field assignment does not bypass validation of mutable JSON blobs."""
        data = native(valid=True)
        response = LLMResponse(model="model", output=(LLMReasoning(native=data),), finish_reason="stop")
        response.output[0].native.payload["invalid"] = object()
        contract = ValueContract.create(LLMResponse, location="response")
        with self.assertRaises(TypeError):
            contract.to_record(response)

    def test_public_exports_and_package_discovery_include_agent_models(self) -> None:
        """Ensure the public API is packaged without requiring an OpenAI SDK."""
        self.assertNotIn("LLMResult", agent.__all__)
        for name in agent.__all__:
            with self.subTest(name=name):
                self.assertTrue(hasattr(agent, name))
        root = Path(__file__).resolve().parents[1]
        config = tomllib.loads((root / "pyproject.toml").read_text())
        include = config["tool"]["setuptools"]["packages"]["find"]["include"]
        for package in ("autoagent.agent", "autoagent.agent.models"):
            with self.subTest(package=package):
                self.assertTrue(any(fnmatch.fnmatchcase(package, pattern) for pattern in include))
                self.assertTrue((root.joinpath(*package.split(".")) / "__init__.py").is_file())


if __name__ == "__main__":
    unittest.main()
