"""Local Tool schemas, execution boundaries and Core/Provider integration."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
import threading
from typing import Annotated, Any
import unittest
from unittest.mock import patch

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from autoagent import AutoAgentApp, Edge, Node, ValueContract, Workflow
from autoagent.agent import (
    ImageContent, LLMMessage, LLMRequest, LLMResponse, LLMToolCall,
    LLMToolDefinition, LLMToolResult, OpenAIChatCompletionsProvider,
    OpenAIResponsesProvider, TextContent, Tool, ToolBusinessError,
    ToolExecutionError, ToolOutput, ToolOutputError, ToolSet, URLSource,
    get_tool_definition, tool,
)

try:
    import httpx
    from openai import AsyncOpenAI
except ImportError:
    httpx = None
    AsyncOpenAI = None


class Location(BaseModel):
    model_config = ConfigDict(extra="forbid")
    city: str
    count: Annotated[int, Field(gt=0)]


class AliasedOutput(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    value: int = Field(alias="answer")


@tool
def add(a: int, b: int = 1) -> int:
    """Add two numbers."""
    return a + b


def call(name: str = "add", arguments: str = '{"a":2}', *, call_id: str = "call-1") -> LLMToolCall:
    return LLMToolCall(name=name, arguments=arguments, call_id=call_id)


class ToolDefinitionTests(unittest.TestCase):
    def test_decorator_keeps_the_original_python_function(self):
        """Keep normal function identity, signature, defaults and ordinary calls."""
        def function(value: int) -> int:
            """Describe this Tool."""
            return value

        decorated = tool(name="echo", description="Echo an integer", strict=True)(function)
        self.assertIs(decorated, function)
        self.assertEqual(decorated(3), 3)
        definition = get_tool_definition(decorated).llm_definition()
        self.assertEqual(definition.name, "echo")
        self.assertEqual(definition.description, "Echo an integer")
        self.assertTrue(definition.strict)

    def test_multi_parameter_schema_preserves_names_defaults_and_constraints(self):
        """Advertise Python parameter names while retaining Pydantic field constraints."""
        @tool
        def query(_path: str, limit: Annotated[int, Field(gt=0, description="Maximum rows")] = 2, *, verbose: bool = False) -> str:
            return _path

        definition = get_tool_definition(query).llm_definition()
        schema = definition.parameters
        self.assertEqual(set(schema["properties"]), {"_path", "limit", "verbose"})
        self.assertEqual(schema["required"], ["_path"])
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(schema["properties"]["limit"]["exclusiveMinimum"], 0)
        self.assertEqual(schema["properties"]["limit"]["description"], "Maximum rows")
        self.assertEqual(Tool(query).invoke(call("query", '{"_path":"/tmp"}')).content[0].text, "/tmp")

    def test_unsupported_signatures_and_annotations_fail_at_definition(self):
        """Reject positional-only, variadic, untyped and unconstrained declarations."""
        def positional(value: int, /) -> int:
            return value

        def variadic(*values: int) -> int:
            return 0

        def keyword(**values: int) -> int:
            return 0

        def untyped(value) -> int:
            return 0

        def missing_return(value: int):
            return value

        def unconstrained(value: dict[str, Any]) -> int:
            return 0

        def incomplete(value: dict) -> int:
            return 0

        def nonstring_keys(value: dict[int, str]) -> int:
            return 0

        def invalid_default(value: int = "wrong") -> int:
            return 0

        def generator(value: int) -> int:
            yield value

        for handler in (positional, variadic, keyword, untyped, missing_return, unconstrained, incomplete, nonstring_keys, invalid_default, generator):
            with self.subTest(handler=handler.__name__), self.assertRaises(TypeError):
                tool(handler)

    def test_names_and_repeated_decoration_are_checked(self):
        """Reject invalid model-facing names and conflicting metadata ownership."""
        def function() -> None:
            return None

        for name in ("", "with spaces", "x" * 65):
            with self.subTest(name=name), self.assertRaises(TypeError):
                tool(name=name)(function)
        tool(function)
        with self.assertRaises(ValueError):
            tool(function)
        with self.assertRaises(ValueError):
            Tool(function, name="another")

    def test_advertised_schemas_are_detached_from_executable_metadata(self):
        """Mutating a request definition cannot change later validation or definitions."""
        tools = ToolSet([add])
        definition = tools.definitions[0]
        definition.parameters["properties"]["a"]["type"] = "string"
        self.assertEqual(tools.definitions[0].parameters["properties"]["a"]["type"], "integer")
        result = tools["add"].invoke(call(arguments='{"a":"two"}'))
        self.assertTrue(result.is_error)


class ToolExecutionTests(unittest.TestCase):
    def test_direct_sync_ainvoke_offloads_user_code(self):
        """A direct asynchronous Tool call does not run synchronous code on its loop thread."""
        caller_thread = threading.get_ident()

        @tool
        def worker() -> int:
            return threading.get_ident()

        # Explicitly own the test executor. The runner's auxiliary shutdown
        # thread can stall in this sandbox after the Tool itself has completed.
        loop = asyncio.new_event_loop()
        executor = ThreadPoolExecutor(max_workers=1)
        loop.set_default_executor(executor)
        try:
            result = loop.run_until_complete(Tool(worker).ainvoke(call("worker", "{}")))
        finally:
            executor.shutdown(wait=True)
            loop.close()
        self.assertNotEqual(int(result.content[0].text), caller_thread)

    def test_sync_invocation_uses_defaults_and_call_identity(self):
        """Bind model arguments to named Python parameters without replacing call_id."""
        result = Tool(add).invoke(call())
        self.assertEqual(result.call_id, "call-1")
        self.assertEqual(result.content, (TextContent(text="3"),))
        self.assertFalse(result.is_error)

    def test_invalid_arguments_never_enter_user_code(self):
        """Reject malformed JSON, duplicate keys, extra fields and strict type violations."""
        entered = []

        @tool
        def checked(value: Annotated[int, Field(gt=0)]) -> int:
            entered.append(value)
            return value

        arguments = (
            "{", "[]", "null", '{"value":1,"value":2}',
            '{"value":1,"extra":2}', '{"value":"1"}', '{"value":true}',
            '{"value":0}', '{}', '{"value":NaN}', '{"value":Infinity}',
            '{"value":1e400}',
        )
        for value in arguments:
            with self.subTest(arguments=value):
                result = Tool(checked).invoke(call("checked", value))
                self.assertTrue(result.is_error)
                self.assertEqual(result.structured_content["error"]["kind"], "invalid_arguments")
                self.assertEqual(result.call_id, "call-1")
        self.assertEqual(entered, [])

    def test_nested_models_dates_and_json_values_validate_from_json(self):
        """Construct typed parameters in JSON mode while retaining nested constraints."""
        @tool
        def inspect_input(location: Location, day: date, payload: JsonValue) -> dict[str, JsonValue]:
            self.assertIsInstance(location, Location)
            self.assertIsInstance(day, date)
            return {"city": location.city, "day": day.isoformat(), "payload": payload}

        executor = Tool(inspect_input)
        result = executor.invoke(call("inspect_input", '{"location":{"city":"Paris","count":2},"day":"2026-10-14","payload":[1,true]}'))
        self.assertEqual(result.structured_content, {"city": "Paris", "day": "2026-10-14", "payload": [1, True]})
        bad = executor.invoke(call("inspect_input", '{"location":{"city":"Paris","count":0},"day":"2026-10-14","payload":null}'))
        self.assertTrue(bad.is_error)

    def test_scalar_container_and_model_outputs_are_normalized(self):
        """Preserve text, encode primitive containers and retain structured object data."""
        def text() -> str:
            return "hello"

        def sequence() -> list[int]:
            return [1, 2]

        def nothing() -> None:
            return None

        def mapping() -> dict[str, int]:
            return {"answer": 3}

        def model() -> AliasedOutput:
            return AliasedOutput(value=3)

        for handler, expected in ((text, "hello"), (sequence, "[1,2]"), (nothing, "null")):
            with self.subTest(handler=handler.__name__):
                self.assertEqual(Tool(handler).invoke(call(handler.__name__, "{}")).content[0].text, expected)
        for handler in (mapping, model):
            self.assertEqual(Tool(handler).invoke(call(handler.__name__, "{}")).structured_content, {"answer": 3})

    def test_explicit_output_preserves_media_and_business_error(self):
        """Carry image content and structured observations without embedding a call_id."""
        def preview() -> ToolOutput:
            return ToolOutput(
                content=(TextContent(text="Preview"), ImageContent(source=URLSource(url="https://example.com/image.png"))),
                structured_content={"width": 10}, is_error=True,
            )

        result = Tool(preview).invoke(call("preview", "{}"))
        self.assertEqual(result.call_id, "call-1")
        self.assertEqual(tuple(part.type for part in result.content), ("text", "image"))
        self.assertEqual(result.structured_content, {"width": 10})
        self.assertTrue(result.is_error)
        contract = ValueContract.create(LLMToolResult, location="tool-result")
        self.assertEqual(contract.restore(contract.to_record(result)), result)

    def test_business_failures_are_observations(self):
        """Let a Tool explicitly report an expected domain failure to the model."""
        def lookup() -> str:
            raise ToolBusinessError("No matching record", code="not_found")

        result = Tool(lookup).invoke(call("lookup", "{}"))
        self.assertTrue(result.is_error)
        self.assertEqual(result.structured_content, {"error": {"kind": "not_found", "message": "No matching record"}})

    def test_unexpected_failures_preserve_the_exception_cause(self):
        """Raise implementation failures rather than publishing a successful Tool result."""
        def broken() -> str:
            raise OSError("disk unavailable")

        with self.assertRaises(ToolExecutionError) as caught:
            Tool(broken).invoke(call("broken", "{}"))
        self.assertIsInstance(caught.exception.__cause__, OSError)
        self.assertEqual(caught.exception.tool_name, "broken")
        self.assertEqual(caught.exception.call_id, "call-1")

    def test_wrong_nonfinite_and_mutated_outputs_raise_contract_errors(self):
        """Revalidate output types, finite JSON and constraints on mutated model instances."""
        def wrong() -> int:
            return "three"

        def nonfinite() -> dict[str, float]:
            return {"value": float("nan")}

        def mutated() -> Location:
            value = Location(city="Paris", count=1)
            value.count = 0
            return value

        for handler in (wrong, nonfinite, mutated):
            with self.subTest(handler=handler.__name__), self.assertRaises(ToolOutputError):
                Tool(handler).invoke(call(handler.__name__, "{}"))

    def test_wrong_name_does_not_invoke_the_tool(self):
        """Do not execute a bound Tool for a differently named model call."""
        result = Tool(add).invoke(call("other"))
        self.assertTrue(result.is_error)
        self.assertEqual(result.structured_content["error"]["kind"], "unknown_tool")


class ToolAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_tools_run_on_the_calling_loop(self):
        """Await asynchronous handlers on the caller's loop with validated parameters."""
        expected_loop = asyncio.get_running_loop()

        @tool
        async def echo(value: int) -> int:
            self.assertIs(asyncio.get_running_loop(), expected_loop)
            return value

        executor = Tool(echo)
        self.assertEqual((await executor.ainvoke(call("echo", '{"value":3}'))).content[0].text, "3")
        with self.assertRaises(TypeError):
            executor.invoke(call("echo", '{"value":3}'))

    async def test_async_failures_distinguish_business_errors_and_cancellation(self):
        """Report domain errors while distinguishing user-raised and external cancellation."""
        async def business() -> int:
            raise ToolBusinessError("not available")

        self.assertTrue((await Tool(business).ainvoke(call("business", "{}"))).is_error)

        async def user_cancel() -> int:
            raise asyncio.CancelledError("user failure")

        with self.assertRaises(ToolExecutionError):
            await Tool(user_cancel).ainvoke(call("user_cancel", "{}"))
        entered = asyncio.Event()

        async def pending() -> int:
            entered.set()
            await asyncio.Event().wait()
            return 0

        task = asyncio.create_task(Tool(pending).ainvoke(call("pending", "{}")))
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_toolset_checks_registration_and_missing_names(self):
        """Reject duplicate or undecorated functions and report unknown model calls."""
        with self.assertRaises(ValueError):
            ToolSet([add, add])
        with self.assertRaises(TypeError):
            ToolSet([lambda: 1])
        tools = ToolSet([add])
        self.assertEqual(len(tools), 1)
        result = await tools.ainvoke(call("missing"))
        self.assertTrue(result.is_error)
        self.assertEqual(result.structured_content["error"]["kind"], "unknown_tool")
        self.assertEqual(tools["add"].invoke(call()).content[0].text, "3")

    async def test_external_tool_boundary_requires_matching_result_identity(self):
        """Support asynchronous adapters while rejecting mismatched result call_ids."""
        class External:
            definition = LLMToolDefinition(name="remote", parameters={"type": "object"})

            async def ainvoke(self, incoming: LLMToolCall) -> LLMToolResult:
                return LLMToolResult(call_id="wrong", content=(TextContent(text="remote result"),))

        tools = ToolSet([External()])
        with self.assertRaises(ToolOutputError):
            await tools.ainvoke(call("remote", "{}"))


class ToolCoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_sync_and_async_tools_use_core_execution_resources(self):
        """Dispatch sync Tools on Core workers and async Tools on Core's event loop."""
        loops = []

        @tool
        def sync_tool() -> str:
            return threading.current_thread().name

        @tool
        async def async_tool() -> str:
            loops.append(asyncio.get_running_loop())
            return threading.current_thread().name

        tools = ToolSet([sync_tool, async_tool])
        workflow = Workflow("tool-execution", nodes=[Node("tool", tools.as_operator())])
        app = AutoAgentApp()
        try:
            # The adapter must not introduce asyncio's default thread pool
            # inside Core; Core owns physical work and cancellation leases.
            with patch("asyncio.to_thread", side_effect=AssertionError("Unexpected executor")):
                sync = await app.ainvoke(workflow, call("sync_tool", "{}"))
                asynchronous = await app.ainvoke(workflow, call("async_tool", "{}"))
            self.assertEqual(sync.status, "completed")
            self.assertTrue(sync.output["content"][0]["text"].startswith("autoagent-operator-"))
            self.assertEqual(asynchronous.status, "completed")
            self.assertIsNot(loops[0], asyncio.get_running_loop())
        finally:
            await app.aclose()

    async def test_single_tool_and_unknown_calls_work_as_core_nodes(self):
        """A bound Tool exposes the canonical Core call/result contract without a ToolSet."""
        app = AutoAgentApp()
        try:
            workflow = Workflow("single-tool", nodes=[Node("add", Tool(add).as_operator())])
            result = await app.ainvoke(workflow, call())
            self.assertEqual(result.status, "completed")
            self.assertEqual(result.output["content"][0]["text"], "3")
            tools = Workflow("empty-tools", nodes=[Node("tool", ToolSet().as_operator())])
            result = await app.ainvoke(tools, call())
            self.assertEqual(result.status, "completed")
            self.assertTrue(result.output["is_error"])
        finally:
            await app.aclose()

    async def test_unexpected_tool_failure_is_a_core_failure(self):
        """Keep Tool implementation exceptions visible as failed Core executions."""
        @tool
        def broken() -> str:
            raise RuntimeError("unexpected failure")

        app = AutoAgentApp()
        try:
            result = await app.ainvoke(Workflow("tool-failure", nodes=[Node("tool", Tool(broken).as_operator())]), call("broken", "{}"))
            self.assertEqual(result.status, "failed")
            self.assertEqual(result.error.type, "ToolExecutionError")
            self.assertIn("unexpected failure", result.error.message)
        finally:
            await app.aclose()


@unittest.skipUnless(AsyncOpenAI is not None, "Install autoagent[agent] for SDK tests")
class ToolProviderIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_fixed_workflow_completes_the_tool_call_round_trip_for_both_protocols(self):
        """Run real SDK encoding, Tool execution and result replay in one Core Workflow."""
        for protocol in ("responses", "chat"):
            with self.subTest(protocol=protocol):
                captured = []
                incoming = call(arguments='{"a":2,"b":4}')

                def handle(request):
                    payload = json.loads(request.content)
                    captured.append(payload)
                    if protocol == "responses":
                        output = [{"type": "function_call", "id": "fc-1", "call_id": incoming.call_id, "name": "add", "arguments": incoming.arguments}]
                        if len(captured) == 2:
                            observation = next(item for item in payload["input"] if item["type"] == "function_call_output")
                            self.assertEqual(observation["call_id"], incoming.call_id)
                            self.assertEqual(observation["output"], [{"type": "input_text", "text": "6"}])
                            output = [{"type": "message", "id": "msg-1", "role": "assistant", "status": "completed", "content": [{"type": "output_text", "text": "The answer is 6.", "annotations": []}]}]
                        reply = {"object": "response", "id": "resp-1", "status": "completed", "model": "model", "output": output}
                    else:
                        message = {"role": "assistant", "content": None, "tool_calls": [{"id": incoming.call_id, "type": "function", "function": {"name": "add", "arguments": incoming.arguments}}]}
                        if len(captured) == 2:
                            observation = next(item for item in payload["messages"] if item["role"] == "tool")
                            self.assertEqual(observation["tool_call_id"], incoming.call_id)
                            self.assertEqual(observation["content"], "6")
                            message = {"role": "assistant", "content": "The answer is 6."}
                        reply = {"object": "chat.completion", "id": "chat-1", "created": 1, "model": "model", "choices": [{"index": 0, "message": message, "finish_reason": "tool_calls" if len(captured) == 1 else "stop"}]}
                    return httpx.Response(200, json=reply)

                client = AsyncOpenAI(api_key="test", max_retries=0, http_client=httpx.AsyncClient(transport=httpx.MockTransport(handle)))
                client._platform = "Linux"
                cls = OpenAIResponsesProvider if protocol == "responses" else OpenAIChatCompletionsProvider
                provider = cls(client=client)
                tools = ToolSet([add])
                transcript = []

                async def generate(request: LLMRequest) -> LLMToolCall:
                    response = await provider.ainvoke(request)
                    transcript.extend(request.input)
                    transcript.extend(response.output)
                    return response.tool_calls[0]

                async def finish(result: LLMToolResult) -> LLMResponse:
                    return await provider.ainvoke(LLMRequest(model="model", input=(*transcript, result), tools=tools.definitions))

                @asynccontextmanager
                async def lifespan():
                    async with provider:
                        try:
                            yield
                        finally:
                            await client.close()

                app = AutoAgentApp(runtime_lifespan=lifespan)
                try:
                    await app.aprepare()
                    workflow = Workflow("tool-round-trip", nodes=[Node("llm", generate), Node("tool", tools.as_operator()), Node("answer", finish)], edges=[Edge("llm", "tool"), Edge("tool", "answer")])
                    result = await app.ainvoke(workflow, LLMRequest(model="model", input=(LLMMessage(role="user", content=(TextContent(text="What is 2 + 4?"),)),), tools=tools.definitions))
                    self.assertEqual(result.status, "completed", result.error)
                    response = ValueContract.create(LLMResponse, location="answer").restore(result.output)
                    self.assertEqual(response.text, "The answer is 6.")
                    self.assertEqual(len(captured), 2)
                finally:
                    await app.aclose()
                self.assertTrue(client.is_closed())


if __name__ == "__main__":
    unittest.main()
