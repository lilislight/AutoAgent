"""Weather project contracts and CLI round trips through both real SDK transports."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import socket
import signal
import time
import threading
import unittest

from unittest.mock import patch

from autoagent.agent import LLMMessage, LLMReasoning, LLMResponse, LLMStreamChunk, LLMToolCall, TextContent
from examples.deepseek_weather_assistant.weather_app.assistant import Question
from examples.deepseek_weather_assistant.weather_app.project import weather_workflow
from autoagent.agent import LLM_CALL, LLMSelection, llm_call_operator
from autoagent.host import AutoAgentHost, HostSettings
from examples.deepseek_weather_assistant.weather_app.tools import weather_tools


ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "examples" / "deepseek_weather_assistant"


class ScriptedProvider:
    provider_id = "deepseek"

    def __init__(self, *responses):
        self.responses = iter(responses)
        self.requests = []

    async def ainvoke(self, request):
        self.requests.append(request)
        return next(self.responses)

    async def astream(self, request):
        yield LLMStreamChunk(type="completed", response=await self.ainvoke(request))


def completion(*items, reason="tool_calls"):
    return LLMResponse(model="deepseek-flash", output=items, finish_reason=reason)


class WeatherLoopTests(unittest.IsolatedAsyncioTestCase):
    async def run_dialogue(self, provider):
        from contextlib import asynccontextmanager

        @asynccontextmanager
        async def lifespan(setup):
            setup.register_operator(llm_call_operator(resolver=lambda request: LLMSelection(provider, "deepseek-flash")),
                capability_id=LLM_CALL.id, default=True)
            yield

        with tempfile.TemporaryDirectory() as directory:
            async with AutoAgentHost(workflows=[weather_workflow()], lifespan=lifespan,
                settings=HostSettings(database_url="sqlite+aiosqlite:///" + str(Path(directory) / "runtime.db")),
                environment={"DEEPSEEK_API_KEY": "offline"}) as host:
                return await host.join(await host.execute("weather-assistant", Question(question="北京天气？")))

    async def test_tool_errors_and_complete_output_survive_graph_round_trips(self):
        """Separate LLM and Tool Nodes retain reasoning, call IDs and error observations between rounds."""
        thinking = LLMReasoning(text="opaque reasoning")
        call = LLMToolCall(name="missing_tool", arguments="{}", call_id="unknown-1")
        provider = ScriptedProvider(completion(thinking, call), completion(
            LLMMessage(role="assistant", content=(TextContent(text="模拟天气服务暂不可用。"),)), reason="stop"))
        result = await self.run_dialogue(provider)
        self.assertEqual(result.status, "completed", result.error)
        self.assertEqual(result.output["model_calls"], 2)
        history = provider.requests[1].input
        self.assertEqual(history[2:4], (thinking, call))
        self.assertEqual(history[4].call_id, call.call_id)
        self.assertTrue(history[4].is_error)
        self.assertEqual(history[4].structured_content["error"]["kind"], "unknown_tool")

    async def test_limit_stops_before_dispatching_unconsumed_tools(self):
        """The last allowed LLM round fails before dispatching Tool calls that cannot be consumed."""
        provider = ScriptedProvider(completion(LLMToolCall(name="get_weather",
            arguments='{"city_id":"beijing"}', call_id="call-1")))
        with patch("examples.deepseek_weather_assistant.weather_app.assistant.MAX_MODEL_CALLS", 1):
            result = await self.run_dialogue(provider)
        self.assertEqual(result.status, "failed")
        self.assertIn("exceeded 1 model calls", result.error.message)
        self.assertEqual(len(provider.requests), 1)

    async def test_partial_or_empty_responses_fail_the_llm_node(self):
        """Truncation, filtering, unknown finish reasons and empty responses do not reach finish."""
        for reason, items in (("length", (LLMMessage(role="assistant", content=(TextContent(text="半截答案"),)),)),
                              ("content_filter", ()), ("other", ()), ("stop", ())):
            with self.subTest(reason=reason):
                result = await self.run_dialogue(ScriptedProvider(completion(*items, reason=reason)))
                self.assertEqual(result.status, "failed")

    async def test_city_aliases_weather_and_invalid_arguments_use_validated_tools(self):
        """City aliases resolve and invalid cities or dates return error observations."""
        tools = weather_tools()
        city = await tools.ainvoke(LLMToolCall(name="resolve_city", arguments='{"name":" Beijing "}', call_id="city"))
        self.assertEqual(city.structured_content["city_id"], "beijing")
        result = await tools.ainvoke(LLMToolCall(name="get_weather",
            arguments='{"city_id":"beijing","day":"tomorrow"}', call_id="weather"))
        self.assertEqual(result.structured_content["source"], "simulated")
        self.assertEqual(result.structured_content["high_c"], 22)
        for arguments, kind in (({"city_id": "paris"}, "city_not_found"),
                                ({"city_id": "beijing", "day": "next_week"}, "invalid_arguments")):
            observed = await tools.ainvoke(LLMToolCall(name="get_weather", arguments=json.dumps(arguments), call_id="bad"))
            self.assertTrue(observed.is_error)
            self.assertEqual(observed.structured_content["error"]["kind"], kind)


@contextmanager
def deepseek_http():
    """Only tests replace HTTP; the example has no fake-LLM runtime option."""
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append((self.path, body))
            tools = [item for item in body["messages"] if item["role"] == "tool"]
            step = len(tools)
            message = {"role": "assistant", "content": None, "reasoning_content": "opaque-test-reasoning"}
            if step < 2:
                name, arguments = (("resolve_city", {"name": "北京"}) if step == 0 else
                                   ("get_weather", {"city_id": "beijing", "day": "tomorrow"}))
                message["tool_calls"] = [{"id": "call-" + str(step), "type": "function",
                    "function": {"name": name, "arguments": json.dumps(arguments, ensure_ascii=False)}}]
            else:
                message["content"] = "模拟天气：北京明天多云，11–22°C，降雨概率15%。适合出行。"
            raw = {"id": "weather-" + str(step), "object": "chat.completion", "created": 1,
                "model": "deepseek-flash", "choices": [{"index": 0, "message": message,
                "finish_reason": "tool_calls" if step < 2 else "stop"}],
                "usage": {"prompt_tokens": 20, "completion_tokens": 10, "total_tokens": 30}}
            if body.get("stream"):
                delta = dict(message)
                if "tool_calls" in delta:
                    delta["tool_calls"][0]["index"] = 0
                first = dict(raw, object="chat.completion.chunk", choices=[{"index": 0, "delta": delta, "finish_reason": None}])
                terminal = dict(raw, object="chat.completion.chunk", choices=[{"index": 0, "delta": {},
                    "finish_reason": raw["choices"][0]["finish_reason"]}])
                first.pop("usage")
                payload = ("data: " + json.dumps(first) + "\n\ndata: " + json.dumps(terminal) + "\n\ndata: [DONE]\n\n").encode()
                content_type = "text/event-stream"
            else:
                payload, content_type = json.dumps(raw).encode(), "application/json"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield "http://127.0.0.1:" + str(server.server_port), requests
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


class WeatherCliTests(unittest.TestCase):
    def environment(self, directory, backend="openai", base_url=None, streaming=False):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("AUTOAGENT_", "DEEPSEEK_", "WEATHER_"))}
        model = 'deepseek/deepseek-flash' if backend == 'litellm' else 'deepseek-flash'
        config = {'default_model': {'provider':'deepseek', 'model':model},
            'providers':{'deepseek':{'backend':backend, 'api_key':'${DEEPSEEK_API_KEY}',
            'base_url':base_url or 'https://api.deepseek.com', 'models':[{'id':model}]}}}
        path = Path(directory) / 'providers.json'
        path.write_text(json.dumps(config))
        env.update(DEEPSEEK_API_KEY="test-key-not-a-credential", AUTOAGENT_PROVIDERS_FILE=str(path),
            LITELLM_LOCAL_MODEL_COST_MAP="True", AUTOAGENT_DATABASE_URL="sqlite+aiosqlite:///" + str(Path(directory) / "runtime.db"))
        return env

    def test_static_project_check_needs_neither_credentials_nor_optional_sdks(self):
        """Workflow compilation succeeds without starting the lifespan or importing either provider SDK."""
        script = """
import sys
class BlockSDK:
    def find_spec(self, fullname, *args):
        if fullname.split('.')[0] in {'openai', 'litellm'}:
            raise ImportError('SDK import is forbidden during project check')
sys.meta_path.insert(0, BlockSDK())
from autoagent.host import AutoAgentHost
report = AutoAgentHost.check_project(sys.argv[1], use_env_file=False)
assert report.ok
assert 'litellm' not in sys.modules and 'openai' not in sys.modules
"""
        result = subprocess.run([sys.executable, "-c", script, str(PROJECT)], cwd=ROOT,
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_server_cli_api_history_and_ui_for_both_backends(self):
        """Both SDKs run the graph through Server; API evidence exposes each Node and UI is served."""
        import httpx
        for backend, streaming in (("openai", True), ("litellm", False)):
            if backend == "litellm" and importlib.util.find_spec("litellm") is None:
                continue
            with self.subTest(backend=backend), tempfile.TemporaryDirectory() as directory, deepseek_http() as (url, requests):
                with socket.socket() as sock:
                    sock.bind(("127.0.0.1", 0))
                    port = sock.getsockname()[1]
                env = self.environment(directory, backend, url, streaming)
                with (Path(directory) / "stdout").open("w+") as stdout, (Path(directory) / "stderr").open("w+") as stderr:
                    server = subprocess.Popen([sys.executable, "-m", "autoagent", "--project", str(PROJECT),
                        "--no-env-file", "server", "--port", str(port), "--format", "json"],
                        cwd=ROOT, env=env, stdout=stdout, stderr=stderr, text=True)
                    try:
                        deadline = time.monotonic() + 45
                        ready = None
                        while time.monotonic() < deadline and server.poll() is None:
                            stdout.seek(0)
                            try:
                                ready = json.load(stdout)
                                break
                            except (ValueError, OSError):
                                time.sleep(.05)
                        stderr.seek(0)
                        self.assertIsNotNone(ready, stderr.read())
                        self.assertTrue(ready["ok"], ready)
                        self.assertEqual(ready["data"]["registered_workflows"], 2)
                        api = ready["data"]["api_url"]
                        with httpx.Client(timeout=30) as client:
                            self.assertEqual(client.get(ready["data"]["ui_url"]).status_code, 200)
                        result = subprocess.run([sys.executable, "-m", "autoagent", "--server", api,
                            "invoke", "weather-assistant", "--entry-node", "prepare", "--input-json",
                            '{"question":"北京明天天气怎么样？"}', "--wait", "terminal", "--format", "json"],
                            cwd=ROOT, env=env, capture_output=True, text=True, timeout=40)
                        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
                        state = json.loads(result.stdout)["data"]
                        self.assertEqual(state["status"], "completed")
                        iid = state["invocation_id"]
                        output = subprocess.run([sys.executable, "-m", "autoagent", "--server", api,
                            "invocation", "value", iid, "--field", "output", "--format", "json"],
                            cwd=ROOT, env=env, capture_output=True, text=True, timeout=20)
                        self.assertEqual(output.returncode, 0, output.stderr)
                        self.assertIn("模拟天气", output.stdout)
                        nodes = subprocess.run([sys.executable, "-m", "autoagent", "--server", api,
                            "invocation", "nodes", iid, "--format", "json"], cwd=ROOT, env=env,
                            capture_output=True, text=True, timeout=20)
                        self.assertEqual(nodes.returncode, 0, nodes.stderr)
                        data = json.loads(nodes.stdout)["data"]
                        self.assertEqual({item["node_id"] for item in data["items"]}, {"prepare", "llm", "tools", "finish"})
                        self.assertEqual(len(requests), 3)
                        for path, body in requests:
                            self.assertTrue(path.endswith("/chat/completions"))
                            self.assertEqual(body["model"], "deepseek-flash")
                            self.assertNotIn("thinking", body)
                            self.assertNotIn("api_key", body)
                        assistants = [m for m in requests[-1][1]["messages"] if m["role"] == "assistant"]
                        self.assertEqual(len(assistants), 2)
                        self.assertTrue(all(m.get("reasoning_content") == "opaque-test-reasoning" for m in assistants))
                        self.assertEqual([m["tool_call_id"] for m in requests[-1][1]["messages"] if m["role"] == "tool"], ["call-0", "call-1"])
                    finally:
                        server.send_signal(signal.SIGINT)
                        server.wait(timeout=20)
                    self.assertEqual(server.returncode, 0)
                    stderr.seek(0)
                    log = stderr.read()
                    self.assertIn("Server stopped.", log)
                    self.assertNotIn("HTTP Request:", log)
                    self.assertNotIn("Traceback", log)

    def test_standard_cli_runs_without_explicit_stream_reducer(self):
        """A local CLI invocation uses Host defaults and the same split Workflow without Server."""
        for backend, streaming in (("openai", False), ("litellm", True)):
            if backend == "litellm" and importlib.util.find_spec("litellm") is None:
                continue
            with tempfile.TemporaryDirectory() as directory, deepseek_http() as (url, requests):
                result = subprocess.run([sys.executable, "-m", "autoagent", "--project", str(PROJECT),
                    "--no-env-file", "invoke", "weather-assistant", "--entry-node", "prepare", "--input-json",
                    '{"question":"北京明天天气？"}', "--wait", "terminal", "--format", "json"],
                    cwd=ROOT, env=self.environment(directory, backend=backend, base_url=url, streaming=streaming), capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
                self.assertEqual(json.loads(result.stdout)["data"]["status"], "completed")
                self.assertEqual(len(requests), 3)


if __name__ == "__main__":
    unittest.main()
