from __future__ import annotations

import asyncio
import unittest
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
from pydantic import BaseModel, Field

from autoagent import AutoAgentApp, JsonRuntimeSerializer
from autoagent.ai import (
    LLM_CALL_CAPABILITY_ID,
    LLM_CALL_CONTRACT,
    LLMMessage,
    LLMRequest,
    LLMResponse,
    LLMToolCall,
    OpenAICompatibleConfig,
    create_openai_compatible_operator,
    get_tool_definition,
    react_workflow,
    register_openai_compatible_operator,
    tool,
)
from autoagent.ai.openai_compatible import _post_json
from tests.helpers import started_app


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


class OpenAICompatibleOperatorTests(unittest.IsolatedAsyncioTestCase):
    def test_config_reads_structured_output_mode_from_env(self) -> None:
        config = OpenAICompatibleConfig.from_env(
            env_file=None,
            environ={
                "AUTOAGENT_OPENAI_API_KEY": "secret",
                "AUTOAGENT_OPENAI_MODEL": "model",
                "AUTOAGENT_OPENAI_STRUCTURED_OUTPUT_MODE": "json_object",
            },
        )

        self.assertEqual(config.structured_output_mode, "json_object")

    async def test_registration_helper_installs_capability_and_operator(self) -> None:
        async def transport(url, headers, payload, timeout):
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
        operator = register_openai_compatible_operator(
            app,
            OpenAICompatibleConfig(
                api_key="secret",
                default_model="model",
            ),
            transport=transport,
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

        async def transport(url, headers, payload, timeout):
            captured.update(
                url=url,
                headers=headers,
                payload=payload,
                timeout=timeout,
            )
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

        operator = create_openai_compatible_operator(
            OpenAICompatibleConfig(
                api_key="secret",
                default_model="default-model",
                base_url="https://provider.example/v1/",
            ),
            transport=transport,
        )
        request = LLMRequest(
            messages=(LLMMessage(role="user", content="hello"),),
            response_format=Answer,
        )

        response = await operator.ainvoke({"request": request})

        self.assertEqual(
            captured["url"],
            "https://provider.example/v1/chat/completions",
        )
        self.assertEqual(captured["payload"]["model"], "default-model")
        self.assertEqual(
            captured["payload"]["response_format"]["json_schema"]["name"],
            "Answer",
        )
        self.assertNotIn("secret", repr(captured["payload"]))
        self.assertEqual(response.message.tool_calls[0].raw_arguments, '{"value":1}')
        self.assertEqual(response.usage.total_tokens, 13)

    async def test_deepseek_auto_mode_uses_json_object_and_schema_prompt(
        self,
    ) -> None:
        captured: dict[str, Any] = {}

        async def transport(url, headers, payload, timeout):
            captured["payload"] = payload
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

        operator = create_openai_compatible_operator(
            OpenAICompatibleConfig(
                api_key="secret",
                default_model="deepseek-v4-flash",
                base_url="https://api.deepseek.com",
            ),
            transport=transport,
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

    async def test_json_object_mode_defers_provider_option_but_keeps_schema_prompt(
        self,
    ) -> None:
        captured: dict[str, Any] = {}

        @tool(description="Look up one value.")
        def lookup(value: int) -> int:
            return value

        async def transport(url, headers, payload, timeout):
            captured["payload"] = payload
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

        operator = create_openai_compatible_operator(
            OpenAICompatibleConfig(
                api_key="secret",
                default_model="deepseek-v4-flash",
                base_url="https://api.deepseek.com",
            ),
            transport=transport,
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

        async def transport(url, headers, payload, timeout):
            captured["payload"] = payload
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

        operator = create_openai_compatible_operator(
            OpenAICompatibleConfig(
                api_key="secret",
                default_model="custom",
                base_url="https://provider.example/v1",
                structured_output_mode="prompt",
            ),
            transport=transport,
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

    async def test_http_error_includes_provider_body_and_request_id(self) -> None:
        request = httpx.Request("POST", "https://provider.example/chat/completions")
        response = httpx.Response(
            400,
            request=request,
            headers={"x-request-id": "req_123"},
            json={"error": {"message": "unsupported response_format"}},
        )
        client = AsyncMock()
        client.post.return_value = response
        context = AsyncMock()
        context.__aenter__.return_value = client

        with patch(
            "autoagent.ai.openai_compatible.httpx.AsyncClient",
            return_value=context,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "req_123.*unsupported response_format",
            ):
                await _post_json(
                    "https://provider.example/chat/completions",
                    {},
                    {},
                    1,
                )


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
    ) -> AutoAgentApp:
        response_iterator = iter(responses)

        async def fake_llm(request: LLMRequest) -> LLMResponse:
            requests.append(request)
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
) -> LLMResponse:
    return LLMResponse(
        message=LLMMessage(
            role="assistant",
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
