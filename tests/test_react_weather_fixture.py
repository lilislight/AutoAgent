from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Literal

from autoagent import AutoAgentApp, DatabaseBackend, RuntimeStore
from autoagent.ai import (
    LLM_CALL_CAPABILITY_ID,
    LLM_CALL_CONTRACT,
    LLMRequest,
    LLMResponse,
    LLMMessage,
    LLMToolCall,
    ChatCompletionsConfig,
)
from autoagent.ai.models.react import (
    ConversationUpdate,
    PreparedLLMCall,
    ToolExecutionResult,
)
from tests.fixtures.react_weather_agent import (
    CityProfile,
    WeatherAnswer,
    build_app,
    build_react_workflow,
    build_workflow,
    get_city_profile,
    get_current_weather,
)


class ReactWeatherFixtureTests(unittest.TestCase):
    def test_mock_tools_return_deterministic_typed_data(self) -> None:
        city = get_city_profile(" Tokyo ")
        weather = get_current_weather("Tokyo", "fahrenheit")

        self.assertEqual(city.country, "Japan")
        self.assertEqual(weather.temperature, 80.6)
        self.assertEqual(weather.unit, "fahrenheit")

    def test_example_workflow_compiles_with_one_entry_and_exit(self) -> None:
        async def fake_llm(
            request: LLMRequest,
            mode: Literal["invoke", "stream"] = "invoke",
        ) -> LLMResponse:
            return LLMResponse(
                message=LLMMessage(
                    role="assistant",
                    content=WeatherAnswer(
                        city="Tokyo",
                        country="Japan",
                        weather="partly cloudy",
                        temperature=27,
                        unit="celsius",
                        recommendation="Wear light clothing.",
                    ).model_dump_json(),
                ),
                model="fake",
            )

        app = AutoAgentApp()
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

        result = app.compiler.compile(build_workflow())

        self.assertTrue(result.ok, result.diagnostics)
        assert result.workflow_ir is not None
        self.assertEqual(
            result.workflow_ir.entry_node_ids,
            ("weather_agent/start",),
        )
        self.assertEqual(
            result.workflow_ir.exit_node_ids,
            ("translated_output",),
        )
        self.assertEqual(
            result.workflow_ir.nodes["weather_agent/start"].workflow_path,
            ("weather_agent",),
        )

    def test_react_child_is_followed_by_chinese_translation_llm(self) -> None:
        responses = iter(
            [
                LLMResponse(
                    message=LLMMessage(
                        role="assistant",
                        content=WeatherAnswer(
                            city="Tokyo",
                            country="Japan",
                            weather="partly cloudy",
                            temperature=27,
                            unit="celsius",
                            recommendation="Wear light clothing.",
                        ).model_dump_json(),
                    ),
                    model="fake",
                ),
                LLMResponse(
                    message=LLMMessage(
                        role="assistant",
                        content="东京天气多云，气温为27摄氏度，建议穿轻便衣物。",
                    ),
                    model="fake",
                ),
            ]
        )
        requests: list[LLMRequest] = []
        modes: list[str] = []

        async def fake_llm(
            request: LLMRequest,
            mode: Literal["invoke", "stream"] = "invoke",
        ) -> LLMResponse:
            requests.append(request)
            modes.append(mode)
            return next(responses)

        app = AutoAgentApp()
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
        workflow = build_workflow()
        app.register_workflow(workflow)
        app.start()
        try:
            invocation = app.invoke(
                workflow,
                input={
                    "input": "What is the weather in Tokyo?",
                    "provider_options": {"custom_option": "fixture"},
                    "mode": "stream",
                },
                event_mode="full",
            )
        finally:
            app.close()

        self.assertEqual(invocation.state, "completed")
        self.assertEqual(
            invocation.result,
            {"output": "东京天气多云，气温为27摄氏度，建议穿轻便衣物。"},
        )
        self.assertEqual(len(requests), 2)
        self.assertEqual(modes, ["stream", "invoke"])
        self.assertEqual(
            requests[0].provider_options,
            {"custom_option": "fixture"},
        )
        self.assertEqual(requests[1].provider_options, {})
        self.assertIn('"city":"Tokyo"', requests[1].messages[-1].content)
        self.assertIn("Simplified Chinese", requests[1].messages[0].content)

    def test_react_child_remains_independently_compilable(self) -> None:
        app, _ = build_app(
            ChatCompletionsConfig(
                api_key="test-key",
                default_model="test-model",
            )
        )
        try:
            result = app.compiler.compile(build_react_workflow())
        finally:
            app.close()

        self.assertTrue(result.ok, result.diagnostics)

    def test_fresh_app_deserializes_llm_and_react_runtime_models(self) -> None:
        config = ChatCompletionsConfig(
            api_key="test-key",
            default_model="test-model",
        )
        writer, _ = build_app(config)
        try:
            payload = writer.runtime_serializer.dumps(
                {
                    "request": LLMRequest(
                        messages=(LLMMessage(role="user", content="weather"),),
                    ),
                    "response": LLMResponse(
                        message=LLMMessage(
                            role="assistant",
                            content="tool result",
                        ),
                        model="test-model",
                    ),
                    "conversation": ConversationUpdate(
                        kind="initial",
                        messages=(LLMMessage(role="user", content="weather"),),
                    ),
                    "tool_result": ToolExecutionResult(
                        tool_call_id="call_1",
                        tool_id="mock.city_profile",
                        output=get_city_profile("Tokyo"),
                    ),
                }
            )
        finally:
            writer.close()

        reader, _ = build_app(config)
        try:
            restored = reader.runtime_serializer.loads(payload)
        finally:
            reader.close()

        self.assertIsInstance(restored["request"], LLMRequest)
        self.assertIsInstance(restored["response"], LLMResponse)
        self.assertIsInstance(restored["conversation"], ConversationUpdate)
        self.assertIsInstance(restored["tool_result"], ToolExecutionResult)
        self.assertIsInstance(restored["tool_result"].output, CityProfile)
        self.assertEqual(restored["tool_result"].output.city, "Tokyo")

    def test_database_restart_rebuilds_full_react_llm_state(self) -> None:
        workflow = build_workflow()

        def database_app(
            path: Path,
            responses: list[LLMResponse],
        ) -> AutoAgentApp:
            response_iterator = iter(responses)

            async def fake_llm(
                request: LLMRequest,
                mode: Literal["invoke", "stream"] = "invoke",
            ) -> LLMResponse:
                return next(response_iterator)

            app = AutoAgentApp(
                runtime_store=RuntimeStore(
                    backend=DatabaseBackend.from_path(path)
                )
            )
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
            app.register_workflow(workflow)
            return app

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "react-runtime.sqlite3"
            first = database_app(
                path,
                [
                    LLMResponse(
                        message=LLMMessage(
                            role="assistant",
                            tool_calls=(
                                LLMToolCall(
                                    id="city",
                                    name="get_city_profile",
                                    raw_arguments='{"city":"Tokyo"}',
                                ),
                                LLMToolCall(
                                    id="weather",
                                    name="get_current_weather",
                                    raw_arguments='{"city":"Tokyo"}',
                                ),
                            ),
                        ),
                        finish_reason="tool_calls",
                        model="fake",
                    ),
                    LLMResponse(
                        message=LLMMessage(
                            role="assistant",
                            content=WeatherAnswer(
                                city="Tokyo",
                                country="Japan",
                                weather="partly cloudy",
                                temperature=27,
                                unit="celsius",
                                recommendation="Wear light clothing.",
                            ).model_dump_json(),
                        ),
                        model="fake",
                    ),
                    LLMResponse(
                        message=LLMMessage(
                            role="assistant",
                            content="东京天气多云，气温27摄氏度。",
                        ),
                        model="fake",
                    ),
                ],
            )
            first.start()
            invocation = first.invoke(
                workflow,
                input={"input": "Weather in Tokyo?"},
                event_mode="full",
            )
            first._runtime_loop.run(first.runtime_store.aflush())
            invocation_id = invocation.id
            first.close()

            second = database_app(path, [])
            second.start()
            try:
                _, rebuilt = second._runtime_loop.run(
                    second.runtime_store.arebuild_execution(invocation_id)
                )
            finally:
                second.close()

        self.assertEqual(
            rebuilt.result,
            {"output": "东京天气多云，气温27摄氏度。"},
        )
        prepared_call = rebuilt.latest_node_execution(
            "weather_agent/prepare_conversation"
        ).output
        self.assertIsInstance(prepared_call, PreparedLLMCall)
        self.assertIsInstance(prepared_call.request, LLMRequest)
        self.assertIsInstance(
            rebuilt.latest_node_execution("translate_to_chinese").output,
            LLMResponse,
        )
        tool_batch = rebuilt.latest_node_execution(
            "weather_agent/tool_0_get_city_profile"
        ).output
        self.assertIsInstance(tool_batch.results[0].output, CityProfile)

    def test_example_app_registers_workflow_and_llm_operator(self) -> None:
        app, workflow = build_app(
            ChatCompletionsConfig(
                api_key="test-key",
                default_model="test-model",
            )
        )
        try:
            registered = next(
                (
                    entry
                    for entry in app.workflow_registry.values()
                    if entry.workflow is workflow
                ),
                None,
            )

            self.assertIsNotNone(registered)
            assert registered is not None
            self.assertIs(registered.workflow, workflow)
            self.assertIsNotNone(
                app.operator_registry.default_for_capability(
                    LLM_CALL_CAPABILITY_ID
                )
            )
        finally:
            app.close()


if __name__ == "__main__":
    unittest.main()
