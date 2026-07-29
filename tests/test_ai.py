from __future__ import annotations

import asyncio
import unittest
from dataclasses import dataclass
from typing import Any, Literal
from unittest.mock import patch

import httpx
from openai import APIStatusError
from pydantic import BaseModel, Field

from autoagent import (
    AutoAgentApp,
    JsonRuntimeSerializer,
    StreamingResult,
    Workflow,
    streaming_result,
)
from autoagent.ai import (
    ChatCompletionsConfig,
    ChatCompletionsProvider,
    LLM_CALL_CAPABILITY_ID,
    LLM_CALL_CONTRACT,
    LLMMessage,
    LLMProvider,
    LLMRequest,
    LLMResponse,
    LLMStreamChunk,
    LLMToolCall,
    create_llm_call_operator,
    get_tool_definition,
    llm_call_node,
    react_workflow,
    register_llm_call_operator,
    tool,
)
from autoagent.ai.providers.deepseek import DeepSeekConfig, DeepSeekProvider
from autoagent.ai.providers.factory import llm_provider_from_environment
from autoagent.ai.models.user_event import (
    AgentFailedPayload,
    AgentOutputPayload,
    MessageAbortedPayload,
    MessageCompletedPayload,
    MessageDeltaPayload,
    ReasoningDeltaPayload,
    ToolCallDeltaPayload,
    ToolCallRequestedPayload,
    ToolResultPayload,
)
from tests.helpers import started_app


class SDKModel:
    def __init__(self, value: dict[str, Any]) -> None:
        self.value = value

    def model_dump(self, *, mode: str) -> dict[str, Any]:
        assert mode == "python"
        return self.value


class FakeStream:
    def __init__(self, chunks: list[dict[str, Any]]) -> None:
        self.chunks = chunks

    async def __aiter__(self):
        for chunk in self.chunks:
            yield SDKModel(chunk)


class FakeCompletions:
    def __init__(self, handler) -> None:
        self.handler = handler
        self.calls: list[dict[str, Any]] = []

    async def create(self, **params):
        self.calls.append(params)
        result = await self.handler(params)
        if isinstance(result, FakeStream):
            return result
        return SDKModel(result)


class FakeSDKClient:
    def __init__(self, handler) -> None:
        self.chat = type("Chat", (), {})()
        self.chat.completions = FakeCompletions(handler)
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _create_test_operator(config, *, handler, provider_type=ChatCompletionsProvider):
    return create_llm_call_operator(
        provider_type(
            config,
            client=FakeSDKClient(handler),
        ),
        operator_id="chat_completions.test",
    )


def _register_test_operator(app, config, *, handler):
    return register_llm_call_operator(
        app,
        ChatCompletionsProvider(
            config,
            client=FakeSDKClient(handler),
        ),
        operator_id="chat_completions.test",
    )


class Answer(BaseModel):
    value: int


@dataclass
class DataclassAnswer:
    value: int


class LLMContractTests(unittest.TestCase):
    def test_explicit_capability_contract_exists_before_operator(self) -> None:
        app = AutoAgentApp()
        capability = app.register_capability(
            LLM_CALL_CAPABILITY_ID,
            contract=LLM_CALL_CONTRACT,
        )

        self.assertIs(capability.contract, LLM_CALL_CONTRACT)
        parameters = {
            item.name: item
            for item in LLM_CALL_CONTRACT.input.parameters
        }
        self.assertTrue(parameters["request"].required)
        self.assertFalse(parameters["mode"].required)
        self.assertEqual(parameters["mode"].default, "invoke")

    def test_response_format_accepts_dataclass_and_persists_as_schema(self) -> None:
        request = LLMRequest(
            messages=(LLMMessage(role="user", content="hello"),),
            response_format=DataclassAnswer,
        )
        serializer = JsonRuntimeSerializer()

        restored = serializer.loads(serializer.dumps(request))

        self.assertEqual(restored.response_format.name, "DataclassAnswer")
        self.assertEqual(
            restored.response_format.json_schema["properties"]["value"]["type"],
            "integer",
        )


class ToolDecoratorTests(unittest.TestCase):
    def test_tool_infers_identity_description_and_schemas(self) -> None:
        @tool()
        def search(
            query: str,
            limit: int = 5,
        ) -> list[str]:
            """Search indexed documents.

            A longer explanation is not sent as the short Tool description.
            """

            return [query] * limit

        definition = get_tool_definition(search)

        self.assertEqual(definition.name, "search")
        self.assertEqual(definition.description, "Search indexed documents.")
        self.assertTrue(definition.id.endswith(".search"))
        self.assertEqual(
            definition.contract.input.json_schema["properties"]["query"]["type"],
            "string",
        )
        self.assertEqual(
            definition.contract.output.json_schema["items"]["type"],
            "string",
        )

    def test_tool_rejects_untyped_output(self) -> None:
        with self.assertRaisesRegex(ValueError, "type annotations"):

            @tool()
            def invalid(value: str):
                return value


class ReActUserEventPayloadTests(unittest.TestCase):
    def test_semantic_payloads_are_provider_neutral_and_extensible(self) -> None:
        self.assertEqual(
            MessageDeltaPayload(delta="hello", provider_note="kept").model_dump(
                mode="json"
            ),
            {"delta": "hello", "provider_note": "kept"},
        )
        self.assertEqual(
            ReasoningDeltaPayload(delta="think").model_dump(mode="json"),
            {"delta": "think"},
        )
        completed = MessageCompletedPayload(
            message=LLMMessage(role="assistant", content="done"),
            model="model",
            provider_extension={"safe": True},
        )
        self.assertEqual(
            completed.model_dump(mode="json")["provider_extension"],
            {"safe": True},
        )
        self.assertEqual(
            ToolCallDeltaPayload(
                tool_call_index=0,
                tool_call_id="call_1",
                tool_name="weather",
                arguments_delta='{"city":',
            ).model_dump(mode="json"),
            {
                "tool_call_index": 0,
                "tool_call_id": "call_1",
                "tool_name": "weather",
                "arguments_delta": '{"city":',
            },
        )

    def test_authoritative_tool_and_output_payloads_validate(self) -> None:
        requested = ToolCallRequestedPayload.model_validate(
            {
                "calls": [
                    {
                        "tool_call_id": "call_1",
                        "name": "weather",
                        "raw_arguments": '{"city":"Paris"}',
                    }
                ],
                "reasoning_content": "I should check the weather.",
            }
        )
        result = ToolResultPayload.model_validate(
            {
                "results": [
                    {
                        "tool_call_id": "call_1",
                        "tool_id": "weather",
                        "output": {"temperature": 21},
                        "error": None,
                    }
                ]
            }
        )
        self.assertEqual(requested.calls[0].name, "weather")
        self.assertEqual(
            requested.reasoning_content,
            "I should check the weather.",
        )
        self.assertEqual(result.results[0].output, {"temperature": 21})
        self.assertEqual(
            AgentOutputPayload(output="done").model_dump(mode="json"),
            {"output": "done"},
        )

    def test_framework_failure_payloads_share_the_contract(self) -> None:
        self.assertEqual(
            MessageAbortedPayload(
                error_type="cancelled",
                message="stopped",
            ).model_dump(mode="json"),
            {"error_type": "cancelled", "message": "stopped"},
        )
        self.assertEqual(
            AgentFailedPayload(
                code="tool_failed",
                message="Tool failed.",
                detail={"tool_call_id": "call_1"},
            ).model_dump(mode="json"),
            {
                "code": "tool_failed",
                "message": "Tool failed.",
                "detail": {"tool_call_id": "call_1"},
            },
        )


class ChatCompletionsProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_factory_builds_generic_provider_from_common_environment(
        self,
    ) -> None:
        provider = llm_provider_from_environment(
            {
                "AUTOAGENT_LLM_API_KEY": "secret",
                "AUTOAGENT_LLM_MODEL": "model",
                "AUTOAGENT_LLM_STRUCTURED_OUTPUT_MODE": "json_object",
            }
        )

        try:
            self.assertIsInstance(provider, ChatCompletionsProvider)
            self.assertNotIsInstance(provider, DeepSeekProvider)
            self.assertEqual(
                provider.config.structured_output_mode,
                "json_object",
            )
        finally:
            await provider.aclose()

    async def test_factory_builds_deepseek_from_provider_environment(
        self,
    ) -> None:
        provider = llm_provider_from_environment(
            {
                "AUTOAGENT_LLM_PROVIDER": "deepseek",
                "AUTOAGENT_LLM_MODEL": "deepseek-chat",
                "AUTOAGENT_LLM_API_KEY": "secret",
            }
        )

        try:
            self.assertIsInstance(provider, DeepSeekProvider)
            self.assertEqual(
                provider.config.base_url,
                "https://api.deepseek.com",
            )
            self.assertEqual(
                provider.config.structured_output_mode,
                "json_object",
            )
        finally:
            await provider.aclose()

    async def test_provider_operates_without_operator_and_preserves_injected_client(
        self,
    ) -> None:
        async def handler(params):
            return {
                "model": "fake",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "ok"},
                    }
                ],
            }

        client = FakeSDKClient(handler)
        provider = ChatCompletionsProvider(
            ChatCompletionsConfig(
                api_key="secret",
                default_model="model",
            ),
            client=client,
        )

        response = await provider.ainvoke(
            LLMRequest(
                messages=(LLMMessage(role="user", content="hello"),),
            )
        )
        await provider.aclose()

        self.assertEqual(response.message.content, "ok")
        self.assertFalse(client.closed)

    async def test_request_omits_unset_optional_chat_completion_fields(
        self,
    ) -> None:
        captured: dict[str, Any] = {}

        async def handler(params):
            captured.update(params)
            return {
                "model": "fake",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "ok"},
                    }
                ],
            }

        provider = ChatCompletionsProvider(
            ChatCompletionsConfig(
                api_key="secret",
                default_model="model",
            ),
            client=FakeSDKClient(handler),
        )
        await provider.ainvoke(
            LLMRequest(
                messages=(LLMMessage(role="user", content="hello"),),
            )
        )

        self.assertEqual(
            {"model", "messages"},
            set(captured),
        )

    async def test_success_status_provider_error_is_not_reported_as_choices(
        self,
    ) -> None:
        async def handler(params):
            return {
                "error": "Unexpected endpoint or method.",
            }

        provider = ChatCompletionsProvider(
            ChatCompletionsConfig(
                api_key="secret",
                default_model="model",
            ),
            client=FakeSDKClient(handler),
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "Unexpected endpoint or method",
        ):
            await provider.ainvoke(
                LLMRequest(
                    messages=(LLMMessage(role="user", content="hello"),),
                )
            )

    async def test_provider_owns_sdk_client_and_disables_hidden_retries(
        self,
    ) -> None:
        async def handler(params):
            raise AssertionError("No request expected.")

        client = FakeSDKClient(handler)
        with patch(
            "autoagent.ai.providers.chat_completions.provider.AsyncOpenAI",
            return_value=client,
        ) as constructor:
            provider = ChatCompletionsProvider(
                ChatCompletionsConfig(
                    api_key="secret",
                    default_model="model",
                    base_url="https://provider.example/v1",
                    timeout_ms=2_500,
                )
            )
            await provider.aclose()

        constructor.assert_called_once_with(
            api_key="secret",
            base_url="https://provider.example/v1",
            timeout=2.5,
            max_retries=0,
            default_headers=None,
        )
        self.assertTrue(client.closed)

    async def test_registration_helper_installs_capability_and_operator(self) -> None:
        async def handler(params):
            return {
                "model": "fake",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": "ok"},
                    }
                ],
            }

        app = AutoAgentApp()
        operator = _register_test_operator(
            app,
            ChatCompletionsConfig(
                api_key="secret",
                default_model="model",
            ),
            handler=handler,
        )

        self.assertTrue(app.capability_registry.contains(LLM_CALL_CAPABILITY_ID))
        self.assertIs(
            app.operator_registry.default_for_capability(
                LLM_CALL_CAPABILITY_ID
            ),
            operator,
        )

    async def test_operator_translates_chat_completion_request_and_response(self) -> None:
        captured: dict[str, Any] = {}

        async def handler(params):
            captured.update(params)
            return {
                "id": "chatcmpl_1",
                "model": "model-used",
                "choices": [
                    {
                        "finish_reason": "tool_calls",
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "call_1",
                                    "type": "function",
                                    "function": {
                                        "name": "lookup",
                                        "arguments": '{"value":1}',
                                    },
                                }
                            ],
                        },
                    }
                ],
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 3,
                    "total_tokens": 13,
                },
            }

        operator = _create_test_operator(
            ChatCompletionsConfig(
                api_key="secret",
                default_model="default-model",
                base_url="https://provider.example/v1/",
            ),
            handler=handler,
        )
        request = LLMRequest(
            messages=(LLMMessage(role="user", content="hello"),),
            response_format=Answer,
            provider_options={"top_k": 12},
        )

        response = await operator.ainvoke({"request": request})

        self.assertEqual(
            captured["model"],
            "default-model",
        )
        self.assertEqual(
            captured["response_format"],
            {"type": "json_object"},
        )
        self.assertNotIn("secret", repr(captured))
        self.assertEqual(captured["extra_body"], {"top_k": 12})
        self.assertEqual(response.message.tool_calls[0].raw_arguments, '{"value":1}')
        self.assertEqual(response.usage.total_tokens, 13)

    async def test_deepseek_auto_mode_uses_json_object_and_schema_prompt(
        self,
    ) -> None:
        captured: dict[str, Any] = {}

        async def handler(params):
            captured["payload"] = params
            return {
                "model": "deepseek-v4-flash",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "content": '{"value":1}',
                        },
                    }
                ],
            }

        operator = _create_test_operator(
            DeepSeekConfig(
                api_key="secret",
                default_model="deepseek-v4-flash",
            ),
            handler=handler,
            provider_type=DeepSeekProvider,
        )

        await operator.ainvoke(
            {
                "request": LLMRequest(
                    messages=(LLMMessage(role="user", content="answer"),),
                    response_format=Answer,
                )
            }
        )

        payload = captured["payload"]
        self.assertEqual(payload["response_format"], {"type": "json_object"})
        self.assertIn("JSON Schema", payload["messages"][0]["content"])
        self.assertIn('"value"', payload["messages"][0]["content"])
        self.assertEqual(payload["messages"][1]["content"], "answer")

    async def test_generic_auto_mode_uses_json_object_and_schema_prompt(
        self,
    ) -> None:
        captured: dict[str, Any] = {}

        async def handler(params):
            captured["payload"] = params
            return {
                "model": "local-model",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "content": '{"value":1}',
                        },
                    }
                ],
            }

        operator = _create_test_operator(
            ChatCompletionsConfig(
                api_key="secret",
                default_model="local-model",
                base_url="http://localhost:1234/v1",
            ),
            handler=handler,
        )
        await operator.ainvoke(
            {
                "request": LLMRequest(
                    messages=(LLMMessage(role="user", content="answer"),),
                    response_format=Answer,
                )
            }
        )

        payload = captured["payload"]
        self.assertEqual(payload["response_format"], {"type": "json_object"})
        self.assertIn("JSON Schema", payload["messages"][0]["content"])
        self.assertEqual(payload["messages"][1]["content"], "answer")

    async def test_deepseek_maps_tokens_reasoning_and_extended_usage(
        self,
    ) -> None:
        captured: dict[str, Any] = {}

        async def handler(params):
            captured.update(params)
            return {
                "id": "deepseek_1",
                "model": "deepseek-v4-pro",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "reasoning_content": "I should answer briefly.",
                            "content": "done",
                        },
                    }
                ],
                "usage": {
                    "prompt_tokens": 20,
                    "completion_tokens": 12,
                    "total_tokens": 32,
                    "prompt_cache_hit_tokens": 8,
                    "prompt_cache_miss_tokens": 12,
                    "completion_tokens_details": {
                        "reasoning_tokens": 7,
                    },
                },
            }

        provider = DeepSeekProvider(
            DeepSeekConfig(
                api_key="secret",
                default_model="deepseek-v4-pro",
            ),
            client=FakeSDKClient(handler),
        )
        response = await provider.ainvoke(
            LLMRequest(
                messages=(
                    LLMMessage(role="user", content="continue"),
                    LLMMessage(
                        role="assistant",
                        content=None,
                        reasoning_content="Previous reasoning.",
                        tool_calls=(
                            LLMToolCall(
                                id="call_1",
                                name="lookup",
                                raw_arguments="{}",
                            ),
                        ),
                    ),
                    LLMMessage(
                        role="tool",
                        content="result",
                        tool_call_id="call_1",
                    ),
                ),
                max_output_tokens=512,
                provider_options={
                    "thinking": {"type": "enabled"},
                    "reasoning_effort": "high",
                },
            )
        )

        self.assertNotIn("max_completion_tokens", captured)
        self.assertEqual(captured["max_tokens"], 512)
        self.assertEqual(
            captured["messages"][1]["reasoning_content"],
            "Previous reasoning.",
        )
        self.assertEqual(
            captured["extra_body"]["thinking"],
            {"type": "enabled"},
        )
        self.assertEqual(captured["reasoning_effort"], "high")
        self.assertNotIn("reasoning_effort", captured["extra_body"])
        self.assertEqual(
            response.message.reasoning_content,
            "I should answer briefly.",
        )
        assert response.usage is not None
        self.assertEqual(response.usage.prompt_cache_hit_tokens, 8)
        self.assertEqual(response.usage.prompt_cache_miss_tokens, 12)
        self.assertEqual(response.usage.reasoning_tokens, 7)

    async def test_deepseek_omits_reasoning_from_non_tool_history(
        self,
    ) -> None:
        captured: dict[str, Any] = {}

        async def handler(params):
            captured.update(params)
            return {
                "model": "deepseek-v4-pro",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "content": "next",
                        },
                    }
                ],
            }

        provider = DeepSeekProvider(
            DeepSeekConfig(
                api_key="secret",
                default_model="deepseek-v4-pro",
            ),
            client=FakeSDKClient(handler),
        )
        await provider.ainvoke(
            LLMRequest(
                messages=(
                    LLMMessage(
                        role="assistant",
                        content="previous",
                        reasoning_content="Not required without a Tool call.",
                    ),
                    LLMMessage(role="user", content="next"),
                ),
            )
        )

        self.assertNotIn("reasoning_content", captured["messages"][0])

    async def test_json_object_mode_defers_provider_option_but_keeps_schema_prompt(
        self,
    ) -> None:
        captured: dict[str, Any] = {}

        @tool(description="Look up one value.")
        def lookup(value: int) -> int:
            return value

        async def handler(params):
            captured["payload"] = params
            return {
                "model": "deepseek-v4-flash",
                "choices": [
                    {
                        "finish_reason": "tool_calls",
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "call_1",
                                    "type": "function",
                                    "function": {
                                        "name": "lookup",
                                        "arguments": '{"value":1}',
                                    },
                                }
                            ],
                        },
                    }
                ],
            }

        operator = _create_test_operator(
            DeepSeekConfig(
                api_key="secret",
                default_model="deepseek-v4-flash",
            ),
            handler=handler,
            provider_type=DeepSeekProvider,
        )

        await operator.ainvoke(
            {
                "request": LLMRequest(
                    messages=(LLMMessage(role="user", content="use lookup"),),
                    tools=(get_tool_definition(lookup).llm_definition(),),
                    tool_choice="auto",
                    response_format=Answer,
                )
            }
        )

        payload = captured["payload"]
        self.assertNotIn("response_format", payload)
        instruction = payload["messages"][0]["content"]
        self.assertIn("exactly two permitted response forms", instruction)
        self.assertIn("tool_calls", instruction)
        self.assertIn("exactly one valid JSON value", instruction)
        self.assertIn("JSON Schema", instruction)
        self.assertIn('"value"', instruction)
        self.assertEqual(payload["messages"][1]["content"], "use lookup")

    async def test_prompt_mode_omits_provider_response_format(self) -> None:
        captured: dict[str, Any] = {}

        async def handler(params):
            captured["payload"] = params
            return {
                "model": "custom",
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "role": "assistant",
                            "content": '{"value":1}',
                        },
                    }
                ],
            }

        operator = _create_test_operator(
            ChatCompletionsConfig(
                api_key="secret",
                default_model="custom",
                base_url="https://provider.example/v1",
                structured_output_mode="prompt",
            ),
            handler=handler,
        )

        await operator.ainvoke(
            {
                "request": LLMRequest(
                    messages=(LLMMessage(role="user", content="answer"),),
                    response_format=Answer,
                )
            }
        )

        self.assertNotIn("response_format", captured["payload"])
        self.assertIn(
            "exactly one valid JSON value",
            captured["payload"]["messages"][0]["content"],
        )

    async def test_sdk_error_is_normalized_with_request_id(self) -> None:
        request = httpx.Request("POST", "https://provider.example/chat/completions")
        response = httpx.Response(
            400,
            request=request,
            headers={"x-request-id": "req_123"},
            json={"error": {"message": "unsupported response_format"}},
        )

        async def handler(params):
            raise APIStatusError(
                "unsupported response_format",
                response=response,
                body={"error": {"message": "unsupported response_format"}},
            )

        provider = ChatCompletionsProvider(
            ChatCompletionsConfig(
                api_key="secret",
                default_model="model",
            ),
            client=FakeSDKClient(handler),
        )
        with self.assertRaisesRegex(
            RuntimeError,
            "unsupported response_format",
        ) as raised:
            await provider.ainvoke(
                LLMRequest(
                    messages=(LLMMessage(role="user", content="hello"),),
                )
            )

        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.request_id, "req_123")
        self.assertFalse(raised.exception.retryable)

    async def test_stream_normalizes_text_tools_and_final_response(self) -> None:
        async def handler(params):
            self.assertTrue(params["stream"])
            return FakeStream(
                [
                    {
                        "id": "chatcmpl_1",
                        "model": "model-used",
                        "choices": [
                            {
                                "delta": {"content": "hel"},
                                "finish_reason": None,
                            }
                        ],
                    },
                    {
                        "id": "chatcmpl_1",
                        "model": "model-used",
                        "choices": [
                            {
                                "delta": {
                                    "content": "lo",
                                    "tool_calls": [
                                        {
                                            "index": 0,
                                            "id": "call_1",
                                            "function": {
                                                "name": "lookup",
                                                "arguments": '{"value":',
                                            },
                                        }
                                    ],
                                },
                                "finish_reason": None,
                            }
                        ],
                    },
                    {
                        "id": "chatcmpl_1",
                        "model": "model-used",
                        "choices": [
                            {
                                "delta": {
                                    "tool_calls": [
                                        {
                                            "index": 0,
                                            "function": {"arguments": "1}"},
                                        }
                                    ]
                                },
                                "finish_reason": "tool_calls",
                            }
                        ],
                    },
                ]
            )

        provider = ChatCompletionsProvider(
            ChatCompletionsConfig(
                api_key="secret",
                default_model="model",
            ),
            client=FakeSDKClient(handler),
        )

        chunks = [
            chunk
            async for chunk in provider.astream(
                LLMRequest(
                    messages=(LLMMessage(role="user", content="hello"),),
                )
            )
        ]

        self.assertTrue(all(isinstance(chunk, LLMStreamChunk) for chunk in chunks))
        self.assertEqual(
            [chunk.text_delta for chunk in chunks if chunk.type == "text_delta"],
            ["hel", "lo"],
        )
        completed = chunks[-1]
        self.assertEqual(completed.type, "completed")
        assert completed.response is not None
        self.assertEqual(completed.response.message.content, "hello")
        self.assertEqual(
            completed.response.message.tool_calls[0].raw_arguments,
            '{"value":1}',
        )

    async def test_llm_call_operator_selects_stream_from_mapped_mode(
        self,
    ) -> None:
        async def handler(params):
            self.assertTrue(params["stream"])
            return FakeStream(
                [
                    {
                        "id": "chatcmpl_stream",
                        "model": "model-used",
                        "choices": [
                            {
                                "delta": {"content": "streamed"},
                                "finish_reason": "stop",
                            }
                        ],
                    }
                ]
            )

        operator = _create_test_operator(
            ChatCompletionsConfig(
                api_key="secret",
                default_model="model",
            ),
            handler=handler,
        )
        result = await operator.ainvoke(
            {
                "request": LLMRequest(
                    messages=(LLMMessage(role="user", content="hello"),),
                ),
                "mode": "stream",
            }
        )
        self.assertIsInstance(result, StreamingResult)
        assert isinstance(result, StreamingResult)
        async for chunk in result.source:
            result.reducer.add(chunk)
        response = result.reducer.finish()

        self.assertEqual(response.message.content, "streamed")

    async def test_llm_call_operator_coalesces_adjacent_small_text_deltas(
        self,
    ) -> None:
        pieces = ["x"] * 100

        async def handler(params):
            self.assertTrue(params["stream"])
            return FakeStream(
                [
                    {
                        "id": "chatcmpl_coalesced",
                        "model": "model-used",
                        "choices": [
                            {
                                "delta": {"content": piece},
                                "finish_reason": (
                                    "stop" if index == len(pieces) - 1 else None
                                ),
                            }
                        ],
                    }
                    for index, piece in enumerate(pieces)
                ]
            )

        operator = _create_test_operator(
            ChatCompletionsConfig(
                api_key="secret",
                default_model="model",
            ),
            handler=handler,
        )
        result = await operator.ainvoke(
            {
                "request": LLMRequest(
                    messages=(LLMMessage(role="user", content="hello"),),
                ),
                "mode": "stream",
            }
        )
        assert isinstance(result, StreamingResult)
        chunks = [chunk async for chunk in result.source]
        for chunk in chunks:
            result.reducer.add(chunk)

        self.assertEqual(
            [
                chunk.text_delta
                for chunk in chunks
                if chunk.type == "text_delta"
            ],
            ["x" * 32, "x" * 32, "x" * 32, "x" * 4],
        )
        self.assertEqual(chunks[-1].type, "completed")
        self.assertEqual(
            result.reducer.finish().message.content,
            "".join(pieces),
        )

    async def test_llm_call_operator_bounds_first_delta_latency(self) -> None:
        source_closed = asyncio.Event()

        class PausedProvider(LLMProvider):
            provider_name = "paused"

            async def ainvoke(self, request: LLMRequest) -> LLMResponse:
                raise AssertionError("ainvoke should not be used")

            async def astream(self, request: LLMRequest):
                try:
                    yield LLMStreamChunk(
                        type="text_delta",
                        text_delta="first",
                    )
                    await asyncio.Event().wait()
                finally:
                    source_closed.set()

        operator = create_llm_call_operator(PausedProvider())
        result = await operator.ainvoke(
            {
                "request": LLMRequest(
                    messages=(LLMMessage(role="user", content="hello"),),
                ),
                "mode": "stream",
            }
        )
        assert isinstance(result, StreamingResult)
        iterator = result.source.__aiter__()

        started = asyncio.get_running_loop().time()
        with patch(
            "autoagent.ai.operators.llm_call."
            "_LLM_STREAM_DELTA_MAX_DELAY_SECONDS",
            0.010,
        ):
            first = await asyncio.wait_for(iterator.__anext__(), timeout=0.2)
        elapsed = asyncio.get_running_loop().time() - started
        await iterator.aclose()

        self.assertEqual("first", first.text_delta)
        self.assertLess(elapsed, 0.15)
        self.assertTrue(source_closed.is_set())

    async def test_llm_call_operator_bounds_each_partial_delta_interval(
        self,
    ) -> None:
        release_second = asyncio.Event()

        class GatedProvider(LLMProvider):
            provider_name = "gated"

            async def ainvoke(self, request: LLMRequest) -> LLMResponse:
                raise AssertionError("ainvoke should not be used")

            async def astream(self, request: LLMRequest):
                yield LLMStreamChunk(type="text_delta", text_delta="one")
                await release_second.wait()
                yield LLMStreamChunk(type="text_delta", text_delta="two")
                await asyncio.Event().wait()

        operator = create_llm_call_operator(GatedProvider())
        result = await operator.ainvoke(
            {
                "request": LLMRequest(
                    messages=(LLMMessage(role="user", content="hello"),),
                ),
                "mode": "stream",
            }
        )
        assert isinstance(result, StreamingResult)
        iterator = result.source.__aiter__()

        with patch(
            "autoagent.ai.operators.llm_call."
            "_LLM_STREAM_DELTA_MAX_DELAY_SECONDS",
            0.010,
        ):
            first = await asyncio.wait_for(iterator.__anext__(), timeout=0.2)
            release_second.set()
            interval_started = asyncio.get_running_loop().time()
            second = await asyncio.wait_for(iterator.__anext__(), timeout=0.2)
            visible_interval = (
                asyncio.get_running_loop().time() - interval_started
            )
        await iterator.aclose()

        self.assertEqual("one", first.text_delta)
        self.assertEqual("two", second.text_delta)
        self.assertLess(visible_interval, 0.15)

    async def test_llm_call_operator_coalesces_only_matching_tool_call_deltas(
        self,
    ) -> None:
        async def handler(params):
            self.assertTrue(params["stream"])
            return FakeStream(
                [
                    {
                        "id": "chatcmpl_tools",
                        "model": "model-used",
                        "choices": [
                            {
                                "delta": {
                                    "tool_calls": [
                                        {
                                            "index": 0,
                                            "id": "call_1",
                                            "function": {
                                                "name": "lookup",
                                                "arguments": '{"city":',
                                            },
                                        }
                                    ]
                                },
                                "finish_reason": None,
                            }
                        ],
                    },
                    {
                        "id": "chatcmpl_tools",
                        "model": "model-used",
                        "choices": [
                            {
                                "delta": {
                                    "tool_calls": [
                                        {
                                            "index": 0,
                                            "function": {
                                                "arguments": '"Tokyo"}',
                                            },
                                        }
                                    ]
                                },
                                "finish_reason": "tool_calls",
                            }
                        ],
                    },
                ]
            )

        operator = _create_test_operator(
            ChatCompletionsConfig(
                api_key="secret",
                default_model="model",
            ),
            handler=handler,
        )
        result = await operator.ainvoke(
            {
                "request": LLMRequest(
                    messages=(LLMMessage(role="user", content="hello"),),
                ),
                "mode": "stream",
            }
        )
        assert isinstance(result, StreamingResult)
        chunks = [chunk async for chunk in result.source]
        tool_chunks = [
            chunk for chunk in chunks if chunk.type == "tool_call_delta"
        ]
        for chunk in chunks:
            result.reducer.add(chunk)

        self.assertEqual(len(tool_chunks), 1)
        self.assertEqual(tool_chunks[0].tool_call_id, "call_1")
        self.assertEqual(tool_chunks[0].tool_name, "lookup")
        self.assertEqual(
            tool_chunks[0].tool_arguments_delta,
            '{"city":"Tokyo"}',
        )
        self.assertEqual(
            result.reducer.finish().message.tool_calls[0].raw_arguments,
            '{"city":"Tokyo"}',
        )

    async def test_closing_coalesced_llm_stream_closes_provider_source(
        self,
    ) -> None:
        closed = asyncio.Event()

        class BlockingProvider(LLMProvider):
            provider_name = "blocking"

            async def ainvoke(self, request: LLMRequest) -> LLMResponse:
                raise AssertionError("ainvoke should not be used")

            async def astream(self, request: LLMRequest):
                try:
                    yield LLMStreamChunk(
                        type="text_delta",
                        text_delta="x" * 32,
                    )
                    await asyncio.Event().wait()
                finally:
                    closed.set()

        operator = create_llm_call_operator(BlockingProvider())
        result = await operator.ainvoke(
            {
                "request": LLMRequest(
                    messages=(LLMMessage(role="user", content="hello"),),
                ),
                "mode": "stream",
            }
        )
        assert isinstance(result, StreamingResult)
        iterator = result.source.__aiter__()

        first = await iterator.__anext__()
        await iterator.aclose()

        self.assertEqual(first.text_delta, "x" * 32)
        self.assertTrue(closed.is_set())

    async def test_deepseek_streams_reasoning_and_preserves_usage(self) -> None:
        async def handler(params):
            self.assertTrue(params["stream"])
            return FakeStream(
                [
                    {
                        "id": "deepseek_1",
                        "model": "deepseek-v4-pro",
                        "choices": [
                            {
                                "delta": {
                                    "reasoning_content": "First ",
                                },
                                "finish_reason": None,
                            }
                        ],
                    },
                    {
                        "id": "deepseek_1",
                        "model": "deepseek-v4-pro",
                        "choices": [
                            {
                                "delta": {
                                    "reasoning_content": "think.",
                                    "content": "answer",
                                },
                                "finish_reason": "stop",
                            }
                        ],
                    },
                    {
                        "id": "deepseek_1",
                        "model": "deepseek-v4-pro",
                        "choices": [],
                        "usage": {
                            "prompt_tokens": 10,
                            "completion_tokens": 6,
                            "total_tokens": 16,
                            "prompt_cache_hit_tokens": 4,
                            "prompt_cache_miss_tokens": 6,
                            "completion_tokens_details": {
                                "reasoning_tokens": 5,
                            },
                        },
                    },
                ]
            )

        provider = DeepSeekProvider(
            DeepSeekConfig(
                api_key="secret",
                default_model="deepseek-v4-pro",
            ),
            client=FakeSDKClient(handler),
        )
        chunks = [
            chunk
            async for chunk in provider.astream(
                LLMRequest(
                    messages=(LLMMessage(role="user", content="answer"),),
                    provider_options={"thinking": {"type": "enabled"}},
                )
            )
        ]

        self.assertEqual(
            [
                chunk.reasoning_delta
                for chunk in chunks
                if chunk.type == "reasoning_delta"
            ],
            ["First ", "think."],
        )
        self.assertEqual(
            [
                chunk.text_delta
                for chunk in chunks
                if chunk.type == "text_delta"
            ],
            ["answer"],
        )
        completed = chunks[-1]
        assert completed.response is not None
        self.assertEqual(
            completed.response.message.reasoning_content,
            "First think.",
        )
        assert completed.response.usage is not None
        self.assertEqual(completed.response.usage.prompt_cache_hit_tokens, 4)
        self.assertEqual(completed.response.usage.reasoning_tokens, 5)


class ReActWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.apps: list[AutoAgentApp] = []

    def tearDown(self) -> None:
        for app in self.apps:
            app.close()

    def app_with_responses(
        self,
        responses: list[LLMResponse],
        requests: list[LLMRequest],
        modes: list[str] | None = None,
    ) -> AutoAgentApp:
        response_iterator = iter(responses)

        async def fake_llm(
            request: LLMRequest,
            mode: Literal["invoke", "stream"] = "invoke",
        ) -> LLMResponse:
            requests.append(request)
            if modes is not None:
                modes.append(mode)
            return next(response_iterator)

        app = started_app()
        self.apps.append(app)
        app.register_capability(
            LLM_CALL_CAPABILITY_ID,
            contract=LLM_CALL_CONTRACT,
        )
        app.register_operator(
            fake_llm,
            operator_id="fake_llm",
            capability_id=LLM_CALL_CAPABILITY_ID,
            default=True,
        )
        return app

    def test_llm_call_node_emits_the_standard_non_stream_event_contract(
        self,
    ) -> None:
        response = LLMResponse(
            model="test-model",
            message=LLMMessage(
                role="assistant",
                content="answer",
                reasoning_content="private reasoning",
            )
        )
        requests: list[LLMRequest] = []
        app = self.app_with_responses([response], requests)
        workflow = Workflow(id="direct_llm_call")
        workflow.add_node(
            llm_call_node(
                id="answer",
                input_mapping=lambda ctx: {
                    "request": ctx.invocation_input["request"],
                    "mode": "invoke",
                },
            )
        )

        invocation = app.invoke(
            workflow,
            input={
                "request": LLMRequest(
                    messages=(LLMMessage(role="user", content="question"),)
                )
            },
            event_mode="minimal",
        )
        events = app.runtime_store.list_user_events(
            invocation_id=invocation.id,
        )

        self.assertEqual(invocation.state, "completed")
        self.assertEqual([event.type for event in events], ["message_completed"])
        self.assertEqual(events[0].data["message"]["content"], "answer")
        self.assertEqual(
            events[0].data["message"]["reasoning_content"],
            "private reasoning",
        )

    def test_graph_has_one_entry_one_exit_and_prepare_is_loop_header(self) -> None:
        requests: list[LLMRequest] = []
        app = self.app_with_responses([], requests)
        workflow = react_workflow(
            id="shape",
            instructions="Answer.",
            response_format=Answer,
        )

        result = app.compiler.compile(workflow)

        self.assertTrue(result.ok, result.diagnostics)
        assert result.workflow_ir is not None
        self.assertEqual(result.workflow_ir.entry_node_ids, ("start",))
        self.assertEqual(result.workflow_ir.exit_node_ids, ("finish",))
        regions = tuple(result.workflow_ir.graph.loop_regions.values())
        self.assertEqual(len(regions), 1)
        self.assertEqual(regions[0].header_node_id, "prepare_conversation")

    def test_initial_input_propagates_provider_options_and_mode_across_loop(
        self,
    ) -> None:
        @tool(id="echo", description="Echo one value.")
        def echo(value: int) -> int:
            return value

        requests: list[LLMRequest] = []
        modes: list[str] = []
        app = self.app_with_responses(
            [
                _tool_response("call_1", "echo", '{"value":1}'),
                _text_response("complete"),
            ],
            requests,
            modes,
        )
        workflow = react_workflow(
            id="input_options",
            instructions="Use the tool.",
            tools=[echo],
        )

        invocation = app.invoke(
            workflow,
            input={
                "input": "echo one",
                "provider_options": {
                    "thinking": {"type": "enabled"},
                },
                "mode": "stream",
            },
        )

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(modes, ["stream", "stream"])
        self.assertEqual(
            [request.provider_options for request in requests],
            [
                {"thinking": {"type": "enabled"}},
                {"thinking": {"type": "enabled"}},
            ],
        )

    def test_react_stream_emits_message_reasoning_and_final_user_events(
        self,
    ) -> None:
        response = _text_response("hello")

        class ResponseReducer:
            def __init__(self) -> None:
                self.response: LLMResponse | None = None

            def add(self, chunk: LLMStreamChunk) -> None:
                if chunk.type == "completed":
                    self.response = chunk.response

            def finish(self) -> LLMResponse:
                assert self.response is not None
                return self.response

        async def chunks():
            yield LLMStreamChunk(type="reasoning_delta", reasoning_delta="think")
            yield LLMStreamChunk(type="text_delta", text_delta="hello")
            yield LLMStreamChunk(type="completed", response=response)

        async def fake_llm(
            request: LLMRequest,
            mode: Literal["invoke", "stream"] = "invoke",
        ) -> LLMResponse | StreamingResult[LLMStreamChunk, LLMResponse]:
            self.assertEqual(mode, "stream")
            return streaming_result(chunks(), reducer=ResponseReducer())

        app = started_app()
        self.apps.append(app)
        app.register_capability(
            LLM_CALL_CAPABILITY_ID,
            contract=LLM_CALL_CONTRACT,
        )
        app.register_operator(
            fake_llm,
            operator_id="fake_streaming_llm",
            capability_id=LLM_CALL_CAPABILITY_ID,
            default=True,
        )
        workflow = react_workflow(
            id="stream_user_events",
            instructions="Answer.",
        )

        invocation = app.invoke(
            workflow,
            input={"input": "hello", "mode": "stream"},
            event_mode="minimal",
        )
        events = app.runtime_store.list_user_events(
            invocation_id=invocation.id,
        )

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(
            [event.type for event in events],
            [
                "reasoning_delta",
                "message_delta",
                "message_completed",
                "agent_output",
            ],
        )
        self.assertEqual(events[0].data, {"delta": "think"})
        self.assertEqual(events[1].data, {"delta": "hello"})
        self.assertEqual(events[-1].data, {"output": "hello"})

    def test_tool_arguments_failure_returns_to_prepare_once(self) -> None:
        calls: list[tuple[int, int]] = []

        @tool(id="add", description="Add numbers.")
        def add(x: int, y: int) -> int:
            calls.append((x, y))
            return x + y

        requests: list[LLMRequest] = []
        app = self.app_with_responses(
            [
                _tool_response("call_bad", "add", '{"x":2}'),
                _tool_response("call_good", "add", '{"x":2,"y":3}'),
                _text_response("done"),
            ],
            requests,
        )
        workflow = react_workflow(
            id="tool_repair",
            instructions="Use tools.",
            tools=[add],
        )

        invocation = app.invoke(workflow, input={"input": "2 + 3"})

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.result, {"output": "done"})
        self.assertEqual(calls, [(2, 3)])
        self.assertEqual(len(requests), 3)
        self.assertIn(
            "tool_arguments_validation_error",
            requests[1].messages[-1].content,
        )
        self.assertEqual(
            invocation.count_node_executions("prepare_conversation"),
            3,
        )
        events = app.runtime_store.list_user_events(
            invocation_id=invocation.id,
        )
        self.assertNotIn(
            "tool_call_rejected",
            [event.type for event in events],
        )
        self.assertNotIn(
            "validate_tool_calls",
            {event.node_id for event in events},
        )
        requested = [
            event
            for event in events
            if event.type == "tool_call_requested"
        ]
        self.assertEqual(
            ["call_bad", "call_good"],
            [
                event.data["calls"][0]["tool_call_id"]
                for event in requested
            ],
        )
        tool_results = [
            event for event in events if event.type == "tool_result"
        ]
        self.assertEqual(1, len(tool_results))
        self.assertEqual(
            "call_good",
            tool_results[0].data["results"][0]["tool_call_id"],
        )

    def test_unknown_tool_is_returned_to_model_then_exhausts_retry(self) -> None:
        @tool(id="known", description="Known Tool.")
        def known(value: int) -> int:
            return value

        requests: list[LLMRequest] = []
        app = self.app_with_responses(
            [
                _tool_response("call_1", "missing", '{"value":1}'),
                _tool_response("call_2", "still_missing", '{"value":2}'),
            ],
            requests,
        )
        workflow = react_workflow(
            id="unknown_tool_repair",
            instructions="Use tools.",
            tools=[known],
        )

        invocation = app.invoke(workflow, input={"input": "use a tool"})

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(len(requests), 2)
        self.assertIn("Unknown tool: missing", requests[1].messages[-1].content)
        self.assertIn("1 repair attempt", invocation.error.message)
        self.assertIn("Unknown tool: still_missing", invocation.error.message)
        events = app.runtime_store.list_user_events(
            invocation_id=invocation.id,
        )
        self.assertEqual(events[-1].type, "agent_failed")
        self.assertEqual(
            events[-1].data["code"],
            invocation.error.code,
        )

    def test_tool_execution_error_is_returned_to_model_for_recovery(self) -> None:
        calls: list[int] = []

        @tool(id="unstable", description="Fail for negative values.")
        def unstable(value: int) -> int:
            calls.append(value)
            if value < 0:
                raise RuntimeError("negative values are unavailable")
            return value * 2

        requests: list[LLMRequest] = []
        app = self.app_with_responses(
            [
                _tool_response(
                    "call_bad",
                    "unstable",
                    '{"value":-1}',
                    reasoning_content="Try the unstable tool first.",
                ),
                _tool_response("call_good", "unstable", '{"value":3}'),
                _text_response("recovered"),
            ],
            requests,
        )
        workflow = react_workflow(
            id="tool_execution_recovery",
            instructions="Use tools.",
            tools=[unstable],
        )

        invocation = app.invoke(workflow, input={"input": "run tool"})

        self.assertEqual("completed", invocation.state)
        self.assertEqual({"output": "recovered"}, invocation.result)
        self.assertEqual([-1, 3], calls)
        self.assertEqual(3, len(requests))
        error_message = requests[1].messages[-1].content
        self.assertIn("tool_execution_error", error_message)
        self.assertIn("RuntimeError", error_message)
        self.assertIn("negative values are unavailable", error_message)
        self.assertEqual(
            requests[1].messages[-2].reasoning_content,
            "Try the unstable tool first.",
        )
        requested = next(
            event
            for event in app.runtime_store.list_user_events(
                invocation_id=invocation.id
            )
            if event.type == "tool_call_requested"
        )
        self.assertEqual(
            "Try the unstable tool first.",
            requested.data["reasoning_content"],
        )

    def test_multiple_calls_to_one_tool_use_one_generated_map_node(self) -> None:
        calls: list[int] = []

        @tool(id="double", description="Double one integer.")
        def double(value: int) -> int:
            calls.append(value)
            return value * 2

        requests: list[LLMRequest] = []
        app = self.app_with_responses(
            [
                LLMResponse(
                    message=LLMMessage(
                        role="assistant",
                        tool_calls=(
                            LLMToolCall(
                                id="call_1",
                                name="double",
                                raw_arguments='{"value":2}',
                            ),
                            LLMToolCall(
                                id="call_2",
                                name="double",
                                raw_arguments='{"value":4}',
                            ),
                        ),
                    ),
                    finish_reason="tool_calls",
                    model="fake",
                ),
                _text_response("complete"),
            ],
            requests,
        )
        workflow = react_workflow(
            id="parallel_same_tool",
            instructions="Use tools.",
            tools=[double],
        )

        invocation = app.invoke(workflow, input={"input": "double values"})

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(sorted(calls), [2, 4])
        tool_execution = invocation.latest_node_execution("tool_0_double")
        self.assertEqual(len(tool_execution.operator_executions), 1)
        self.assertEqual(
            tool_execution.operator_executions[0].summary.call_count,
            2,
        )
        events = app.runtime_store.list_user_events(
            invocation_id=invocation.id,
        )
        requested = next(
            event for event in events
            if event.type == "tool_call_requested"
        )
        result = next(
            event for event in events
            if event.type == "tool_result"
        )
        self.assertEqual(len(requested.data["calls"]), 2)
        self.assertEqual(len(result.data["results"]), 2)
        self.assertEqual("llm_call", requested.node_id)
        self.assertEqual("tool_0_double", result.node_id)
        self.assertNotIn(
            "validate_tool_calls",
            {event.node_id for event in events},
        )
        self.assertEqual(events[-1].type, "agent_output")
        self.assertEqual(events[-1].node_id, "finish")

    def test_react_workflow_can_execute_as_one_child_workflow(self) -> None:
        requests: list[LLMRequest] = []
        app = self.app_with_responses([_text_response("child result")], requests)
        child = react_workflow(
            id="child_react",
            instructions="Answer.",
        )

        def parent_start() -> dict[str, str]:
            return {"input": "hello child"}

        def parent_finish(value: str) -> str:
            return f"parent:{value}"

        from autoagent import Workflow

        parent = Workflow(id="parent")
        parent.add_node(parent_start, node_id="parent_start")
        parent.add_node(child, node_id="agent")
        parent.add_node(
            parent_finish,
            node_id="parent_finish",
            input_mapping=lambda ctx: {"value": ctx.incoming[0].value},
        )
        parent.add_edge("parent_start", "agent")
        parent.add_edge("agent", "parent_finish")

        invocation = app.invoke(parent)

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.result, {"output": "parent:child result"})
        self.assertEqual(requests[0].messages[-1].content, "hello child")
        events = app.runtime_store.list_user_events(
            invocation_id=invocation.id,
        )
        self.assertTrue(events)
        self.assertEqual(
            {("agent",)},
            {event.workflow_path for event in events},
        )

    def test_structured_output_failure_returns_to_prepare_once(self) -> None:
        requests: list[LLMRequest] = []
        app = self.app_with_responses(
            [
                _text_response('{"wrong":1}'),
                _text_response('{"value":7}'),
            ],
            requests,
        )
        workflow = react_workflow(
            id="output_repair",
            instructions="Return an answer.",
            response_format=Answer,
        )

        invocation = app.invoke(workflow, input={"input": "answer"})

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(invocation.result, {"output": {"value": 7}})
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[1].tool_choice, "none")
        self.assertIn("Validation error", requests[1].messages[-1].content)
        events = app.runtime_store.list_user_events(
            invocation_id=invocation.id,
        )
        self.assertNotIn(
            "validate_output",
            {event.node_id for event in events},
        )
        self.assertEqual(
            2,
            sum(event.type == "message_completed" for event in events),
        )
        self.assertEqual("agent_output", events[-1].type)
        self.assertEqual("finish", events[-1].node_id)

    def test_default_output_repair_limit_fails_second_invalid_response(self) -> None:
        requests: list[LLMRequest] = []
        app = self.app_with_responses(
            [
                _text_response('{"wrong":1}'),
                _text_response('{"still_wrong":2}'),
            ],
            requests,
        )
        workflow = react_workflow(
            id="output_repair_exhausted",
            instructions="Return an answer.",
            response_format=Answer,
        )

        invocation = app.invoke(workflow, input={"input": "answer"})

        self.assertEqual(invocation.state, "failed")
        self.assertEqual(len(requests), 2)
        self.assertIn("1 repair attempt", invocation.error.message)


def _tool_response(
    call_id: str,
    name: str,
    arguments: str,
    *,
    reasoning_content: str | None = None,
) -> LLMResponse:
    return LLMResponse(
        message=LLMMessage(
            role="assistant",
            reasoning_content=reasoning_content,
            tool_calls=(
                LLMToolCall(
                    id=call_id,
                    name=name,
                    raw_arguments=arguments,
                ),
            ),
        ),
        finish_reason="tool_calls",
        model="fake",
    )


def _text_response(content: str) -> LLMResponse:
    return LLMResponse(
        message=LLMMessage(role="assistant", content=content),
        finish_reason="stop",
        model="fake",
    )


if __name__ == "__main__":
    unittest.main()
