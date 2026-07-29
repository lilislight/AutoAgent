from __future__ import annotations

from contextlib import redirect_stdout
from io import StringIO
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from typing import Any, Literal
from unittest.mock import patch

from autoagent import (
    AutoAgentApp,
    AutoAgentSettings,
    StreamingResult,
    streaming_result,
)
from autoagent.ai import (
    LLM_CALL_CAPABILITY_ID,
    LLM_CALL_CONTRACT,
    LLMMessage,
    LLMRequest,
    LLMResponse,
    LLMStreamChunk,
    LLMToolCall,
)
from autoagent.cli import main as cli_main
from autoagent.project import ProjectCompiler, ProjectLoader


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class AuthoringExamplesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.project = ProjectLoader().load(PROJECT_ROOT)

    def test_manifest_exports_three_valid_workflows(self) -> None:
        self.assertEqual(
            {
                "release_review",
                "human_approval",
                "weather_assistant",
            },
            {
                loaded.workflow.id
                for loaded in self.project.workflows
            },
        )
        for loaded in self.project.workflows:
            result = ProjectCompiler().compile(loaded.workflow)
            self.assertTrue(result.ok, result.diagnostics)

    def test_orchestration_example_runs_parallel_loop_and_fan_in(self) -> None:
        workflow = self.project.workflow_by_id("release_review")
        input_value = self._json("inputs/orchestration.json")
        expected = self._json("expected/orchestration.json")
        app = AutoAgentApp(settings=AutoAgentSettings())
        app.register_workflow(workflow)
        app.start()
        try:
            invocation = app.invoke(
                workflow,
                input=input_value,
                event_mode="standard",
            )
            actual = app.runtime_serializer.json_view(
                app.runtime_serializer.dumps_unchecked(invocation.result)
            )
            low_risk = app.invoke(
                workflow,
                input={
                    "request": {
                        "service": "documentation-site",
                        "risk": "low",
                        "change_summary": "Correct a heading",
                    }
                },
                event_mode="standard",
            )
            user_events = app.runtime_store.list_user_events(
                invocation_id=invocation.id,
            )
        finally:
            app.close()

        self.assertEqual(expected, actual)
        self.assertEqual(2, invocation.count_node_executions("plan_review"))
        self.assertEqual(2, invocation.count_node_executions("security_review"))
        self.assertEqual(
            2,
            invocation.count_node_executions("reliability_review"),
        )
        self.assertEqual(0, invocation.count_node_executions("approve_low_risk"))
        self.assertEqual(1, low_risk.count_node_executions("approve_low_risk"))
        self.assertEqual(0, low_risk.count_node_executions("security_review"))
        self.assertEqual(
            "automatic",
            low_risk.result["output"].path,
        )
        self.assertEqual(
            ["release_review_completed"],
            [event.type for event in user_events],
        )
        self.assertEqual("finalize_report", user_events[0].node_id)
        self.assertEqual(
            {
                "service": "payments-api",
                "decision": "approved",
                "review_rounds": 2,
                "path": "specialist",
            },
            user_events[0].data,
        )

    def test_wait_example_resumes_through_a_new_cli_host(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "runtime.db"
            env_file = Path(directory) / "runtime.env"
            env_file.write_text(
                "AUTOAGENT_DATABASE_URL="
                f"sqlite+aiosqlite:///{database_path}\n",
                encoding="utf-8",
            )
            run_code, run_output = self._run_cli(
                "--project",
                str(PROJECT_ROOT),
                "--env-file",
                str(env_file),
                "invocation",
                "run",
                "human_approval",
                "--session",
                "authoring-example",
                "--input-file",
                str(PROJECT_ROOT / "inputs" / "wait-request.json"),
            )
            resume_code, resume_output = self._run_cli(
                "--project",
                str(PROJECT_ROOT),
                "--env-file",
                str(env_file),
                "invocation",
                "resume",
                "human_approval",
                "--session",
                "authoring-example",
                "--wait-key",
                "release:42",
                "--response-file",
                str(PROJECT_ROOT / "inputs" / "wait-response.json"),
            )

        self.assertEqual(0, run_code, run_output)
        self.assertIn("STATE waiting", run_output)
        self.assertEqual(0, resume_code, resume_output)
        self.assertIn("STATE completed", resume_output)
        self.assertIn('"decision": "approved"', resume_output)
        self.assertIn('"reviewer": "release-manager"', resume_output)

    def test_react_example_uses_tools_and_structured_output(self) -> None:
        workflow = self.project.workflow_by_id("weather_assistant")
        requests: list[LLMRequest] = []

        async def fake_llm(
            request: LLMRequest,
            mode: Literal["invoke", "stream"] = "invoke",
        ) -> (
            LLMResponse
            | StreamingResult[LLMStreamChunk, LLMResponse]
        ):
            requests.append(request)
            if not any(message.role == "tool" for message in request.messages):
                response = LLMResponse(
                    message=LLMMessage(
                        role="assistant",
                        tool_calls=(
                            LLMToolCall(
                                id="city-profile",
                                name="get_city_profile",
                                raw_arguments='{"city":"Tokyo"}',
                            ),
                            LLMToolCall(
                                id="current-weather",
                                name="get_current_weather",
                                raw_arguments='{"city":"Tokyo"}',
                            ),
                        ),
                    ),
                    finish_reason="tool_calls",
                    model="mock-weather-model",
                )
                chunks = (
                    LLMStreamChunk(
                        type="tool_call_delta",
                        tool_call_index=0,
                        tool_call_id="city-profile",
                        tool_name="get_city_profile",
                        tool_arguments_delta='{"city":"Tokyo"}',
                    ),
                    LLMStreamChunk(
                        type="tool_call_delta",
                        tool_call_index=1,
                        tool_call_id="current-weather",
                        tool_name="get_current_weather",
                        tool_arguments_delta='{"city":"Tokyo"}',
                    ),
                )
            else:
                content = json.dumps(
                    {
                        "city": "Tokyo",
                        "country": "Japan",
                        "condition": "partly cloudy",
                        "temperature_celsius": 27.0,
                        "recommendation": (
                            "Carry water and a light layer."
                        ),
                        "data_source": "mock",
                    }
                )
                response = LLMResponse(
                    message=LLMMessage(
                        role="assistant",
                        content=content,
                    ),
                    finish_reason="stop",
                    model="mock-weather-model",
                )
                chunks = (
                    LLMStreamChunk(
                        type="text_delta",
                        text_delta=content[:48],
                    ),
                    LLMStreamChunk(
                        type="text_delta",
                        text_delta=content[48:],
                    ),
                )
            self.assertEqual("stream", mode)
            return streaming_result(
                self._llm_chunks(chunks, response),
                reducer=_CompletedResponseReducer(),
            )

        app = AutoAgentApp(settings=AutoAgentSettings())
        app.register_capability(
            LLM_CALL_CAPABILITY_ID,
            contract=LLM_CALL_CONTRACT,
        )
        app.register_operator(
            fake_llm,
            operator_id="sample.fake_llm",
            capability_id=LLM_CALL_CAPABILITY_ID,
            default=True,
        )
        app.register_workflow(workflow)
        app.start()
        try:
            invocation = app.invoke(
                workflow,
                input=self._json("inputs/react-weather.json"),
                event_mode="full",
            )
            actual = app.runtime_serializer.json_view(
                app.runtime_serializer.dumps_unchecked(invocation.result)
            )
            user_events = app.runtime_store.list_user_events(
                invocation_id=invocation.id,
            )
        finally:
            app.close()

        self.assertEqual(self._json("expected/react-weather.json"), actual)
        self.assertEqual(2, len(requests))
        self.assertEqual(2, len(requests[0].tools))
        self.assertEqual(
            {"get_city_profile", "get_current_weather"},
            {tool.name for tool in requests[0].tools},
        )
        self.assertEqual(
            2,
            invocation.count_node_executions(
                "tool_0_get_city_profile"
            )
            + invocation.count_node_executions(
                "tool_1_get_current_weather"
            ),
        )
        event_types = [event.type for event in user_events]
        self.assertEqual(2, event_types.count("tool_call_delta"))
        self.assertEqual(1, event_types.count("tool_call_requested"))
        self.assertEqual(2, event_types.count("tool_result"))
        self.assertEqual(2, event_types.count("message_delta"))
        self.assertEqual(1, event_types.count("message_completed"))
        self.assertEqual("agent_output", event_types[-1])
        self.assertEqual(
            {"llm_call"},
            {
                event.node_id
                for event in user_events
                if event.type
                in {
                    "tool_call_delta",
                    "tool_call_requested",
                    "message_delta",
                    "message_completed",
                }
            },
        )
        self.assertEqual(
            {"finish"},
            {
                event.node_id
                for event in user_events
                if event.type == "agent_output"
            },
        )
        self.assertNotIn(
            "validate_tool_calls",
            {event.node_id for event in user_events},
        )

    def test_mock_provider_returns_tool_and_final_turns(self) -> None:
        module_path = PROJECT_ROOT / "mock_chat_completions_provider.py"
        spec = importlib.util.spec_from_file_location(
            "authoring_mock_chat_completions_provider",
            module_path,
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        first = self._run_async(
            module.chat_completions(
                {
                    "model": "mock-weather-model",
                    "messages": [{"role": "user", "content": "weather"}],
                }
            )
        )
        second = self._run_async(
            module.chat_completions(
                {
                    "model": "mock-weather-model",
                    "messages": [
                        {
                            "role": "tool",
                            "tool_call_id": "current-weather",
                            "content": "{}",
                        }
                    ],
                }
            )
        )
        streamed = self._run_async(
            module.chat_completions(
                {
                    "model": "mock-weather-model",
                    "messages": [{"role": "user", "content": "weather"}],
                    "stream": True,
                }
            )
        )
        streamed_body = self._run_async(
            self._streaming_response_body(streamed)
        )

        self.assertEqual(
            "tool_calls",
            first["choices"][0]["finish_reason"],
        )
        self.assertEqual("stop", second["choices"][0]["finish_reason"])
        self.assertIn('"finish_reason":"tool_calls"', streamed_body)
        self.assertTrue(streamed_body.endswith("data: [DONE]\n\n"))

    def _json(self, relative_path: str) -> dict[str, object]:
        return json.loads(
            (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")
        )

    def _run_cli(self, *arguments: str) -> tuple[int, str]:
        output = StringIO()
        clean_environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("AUTOAGENT_")
        }
        with patch.dict(
            os.environ,
            clean_environment,
            clear=True,
        ), redirect_stdout(output):
            code = cli_main(arguments)
        return code, output.getvalue()

    def _run_async(self, awaitable):
        import asyncio

        return asyncio.run(awaitable)

    async def _llm_chunks(
        self,
        chunks: tuple[LLMStreamChunk, ...],
        response: LLMResponse,
    ):
        for chunk in chunks:
            yield chunk
        yield LLMStreamChunk(type="completed", response=response)

    async def _streaming_response_body(self, response: Any) -> str:
        parts: list[str] = []
        async for part in response.body_iterator:
            parts.append(
                part.decode("utf-8")
                if isinstance(part, bytes)
                else part
            )
        return "".join(parts)

class _CompletedResponseReducer:
    def __init__(self) -> None:
        self.response: LLMResponse | None = None

    def add(self, chunk: LLMStreamChunk) -> None:
        if chunk.type == "completed":
            self.response = chunk.response

    def finish(self) -> LLMResponse:
        if self.response is None:
            raise RuntimeError("The sample LLM stream did not complete.")
        return self.response


if __name__ == "__main__":
    unittest.main()
