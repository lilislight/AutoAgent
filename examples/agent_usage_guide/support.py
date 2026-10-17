"""Offline HTTP fixtures shared by examples; not part of AutoAgent's API.

The actual OpenAI/LiteLLM SDKs perform request encoding and response parsing.
Every httpx send is intercepted, including requests from SDK-owned clients.
"""

from __future__ import annotations

import base64
from contextlib import asynccontextmanager
import json
import struct
from unittest.mock import patch

import httpx

from autoagent.agent import (
    DeepSeekProvider, DeepSeekProviderConfig, LiteLLMProvider,
    LiteLLMProviderConfig, LLMMessage, LLMRequest, OpenAIProviderConfig,
    OpenAIResponsesProvider, OpenAIChatCompletionsProvider, TextContent,
)


def user(text: str) -> LLMMessage:
    return LLMMessage(role="user", content=(TextContent(text=text),))


def request(text: str = "Say hello.", **options) -> LLMRequest:
    return LLMRequest(model="demo-model", input=(user(text),), **options)


def show(title: str, value) -> None:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    print(f"\n{title}\n{json.dumps(value, ensure_ascii=False, indent=2)}")


class OfflineAPI:
    """Fixed successful results and one explicitly requested rate-limit error."""

    def __init__(self):
        self.requests = []
        self.uploads = 0
        self.deletions = 0

    def _chat(self, body):
        messages = body["messages"]
        has_result = any(m["role"] == "tool" for m in messages)
        message = {"role": "assistant", "content": "Hello from the offline SDK."}
        if body.get("tools") and not has_result:
            message.update(content=None, reasoning_content="A calculation is needed.", tool_calls=[{
                "id": "call_add", "type": "function",
                "function": {"name": body["tools"][0]["function"]["name"], "arguments": '{"a":2,"b":3}'},
            }])
        elif has_result:
            message.update(content="The sum is 5.", reasoning_content="The Tool returned 5.")
        elif body.get("response_format", {}).get("type") in {"json_object", "json_schema"}:
            message["content"] = '{"city":"Hangzhou","temperature":22}'
        return {"id": "chatcmpl_demo", "object": "chat.completion", "created": 1,
                "model": body["model"], "choices": [{"index": 0, "message": message,
                "finish_reason": "tool_calls" if message.get("tool_calls") else "stop"}],
                "usage": {"prompt_tokens": 8, "completion_tokens": 6, "total_tokens": 14}}

    def _responses(self, body):
        text = '{"city":"Hangzhou","temperature":22}' if body.get("text", {}).get("format", {}).get("type") in {"json_schema", "json_object"} else "Hello from the offline SDK."
        return {"id": "resp_demo", "object": "response", "created_at": 1, "status": "completed",
                "model": body["model"], "output": [{"type": "message", "id": "msg_demo",
                "role": "assistant", "status": "completed", "content": [
                    {"type": "output_text", "text": text, "annotations": []}]}],
                "usage": {"input_tokens": 8, "output_tokens": 6, "total_tokens": 14}}

    def _stream(self, body, responses):
        if responses:
            full = self._responses(body)
            message = full["output"][0]
            empty = {**message, "status": "in_progress", "content": []}
            events = [
                {"type": "response.created", "response": {**full, "status": "in_progress", "output": [], "usage": None}},
                {"type": "response.output_item.added", "output_index": 0, "item": empty},
                {"type": "response.content_part.added", "item_id": message["id"], "output_index": 0,
                 "content_index": 0, "part": {"type": "output_text", "text": "", "annotations": []}},
                {"type": "response.output_text.delta", "item_id": message["id"], "output_index": 0,
                 "content_index": 0, "delta": message["content"][0]["text"]},
                {"type": "response.output_text.done", "item_id": message["id"], "output_index": 0,
                 "content_index": 0, "text": message["content"][0]["text"]},
                {"type": "response.content_part.done", "item_id": message["id"], "output_index": 0,
                 "content_index": 0, "part": message["content"][0]},
                {"type": "response.output_item.done", "output_index": 0, "item": message},
                {"type": "response.completed", "response": full},
            ]
            return "".join(f"event: {e['type']}\ndata: {json.dumps(e)}\n\n" for e in events)
        def chunk(delta, finish=None):
            return {"id": "chatcmpl_stream", "object": "chat.completion.chunk", "created": 1,
                    "model": body["model"], "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
        events = [chunk({"role": "assistant", "content": "Hello "}),
                  chunk({"content": "from the offline SDK."}), chunk({}, "stop"),
                  {"id": "chatcmpl_stream", "object": "chat.completion.chunk", "created": 1,
                   "model": body["model"], "choices": [],
                   "usage": {"prompt_tokens": 8, "completion_tokens": 6, "total_tokens": 14}}]
        return "".join(f"data: {json.dumps(e)}\n\n" for e in events) + "data: [DONE]\n\n"

    async def send(self, client, incoming, **kwargs):
        if incoming.url.host != "offline.example":
            raise RuntimeError("Offline example blocked an unexpected network request.")
        path = incoming.url.path
        body = json.loads(incoming.content) if "application/json" in incoming.headers.get("content-type", "") and incoming.content else {}
        self.requests.append({"path": path, "method": incoming.method, "body": body})
        headers = {"x-request-id": "req_offline_demo"}
        if path.endswith("/files") and incoming.method == "POST":
            self.uploads += 1
            result = {"id": "file_demo", "object": "file", "bytes": 10, "created_at": 1,
                      "filename": "report.pdf", "purpose": "user_data", "status": "processed"}
        elif "/files/" in path:
            if incoming.method == "DELETE":
                self.deletions += 1
                result = {"id": "file_demo", "object": "file", "deleted": True}
            else:
                result = {"id": "file_demo", "object": "file", "bytes": 10, "created_at": 1,
                          "filename": "report.pdf", "purpose": "user_data", "status": "processed"}
        elif path.endswith("/embeddings"):
            values = body["input"]
            count = 1 if isinstance(values, str) or isinstance(values[0], int) else len(values)
            dimension = body.get("dimensions", 3)
            rows = []
            for index in reversed(range(count)):
                vector = [float(index + n + 1) / 4 for n in range(dimension)]
                if body.get("encoding_format") == "base64":
                    vector = base64.b64encode(struct.pack(f"<{dimension}f", *vector)).decode()
                rows.append({"object": "embedding", "index": index, "embedding": vector})
            result = {"object": "list", "model": body["model"], "data": rows,
                      "usage": {"prompt_tokens": 5, "total_tokens": 5}}
        elif path.endswith(("/responses", "/chat/completions")):
            if body["model"] == "demo-rate-limit":
                return httpx.Response(429, request=incoming, headers=headers,
                    json={"error": {"message": "Offline rate-limit demonstration", "type": "rate_limit", "code": "rate_limit"}})
            responses = path.endswith("/responses")
            if body.get("stream"):
                return httpx.Response(200, request=incoming, headers={**headers, "content-type": "text/event-stream"},
                                      content=self._stream(body, responses).encode())
            result = self._responses(body) if responses else self._chat(body)
        else:
            raise RuntimeError(f"Unimplemented offline HTTP path: {path}")
        return httpx.Response(200, request=incoming, headers=headers, json=result)


@asynccontextmanager
async def offline_http():
    api = OfflineAPI()
    async def send(client, incoming, **kwargs):
        return await api.send(client, incoming, **kwargs)
    with patch.dict("os.environ", {"LITELLM_LOCAL_MODEL_COST_MAP": "True"}), patch.object(httpx.AsyncClient, "send", send):
        yield api


def connection(backend="openai"):
    config = {"api_key": "offline-placeholder", "base_url": "https://offline.example/v1"}
    if backend == "deepseek":
        return DeepSeekProviderConfig(**config)
    if backend == "litellm":
        return LiteLLMProviderConfig(**config, model_prefix="openai")
    return OpenAIProviderConfig(**config)


@asynccontextmanager
async def offline_provider(backend="openai", protocol="openai.responses", **options):
    async with offline_http() as api:
        cls = {"deepseek": DeepSeekProvider, "litellm": LiteLLMProvider}.get(backend)
        provider = cls(connection(backend), protocol=protocol, **options) if cls else (
            OpenAIResponsesProvider if protocol == "openai.responses" else OpenAIChatCompletionsProvider
        )(connection(), **options)
        async with provider:
            yield provider, api
