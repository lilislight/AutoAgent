from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import FastAPI
from fastapi.responses import StreamingResponse


app = FastAPI(title="AutoAgent deterministic Chat Completions mock")


@app.post("/v1/chat/completions")
async def chat_completions(
    request: dict[str, Any],
) -> Any:
    """Return a fixed Tool turn followed by one fixed structured answer."""

    response = _completion_response(request)
    if request.get("stream") is True:
        return StreamingResponse(
            _stream_response(response),
            media_type="text/event-stream",
        )
    return response


def _completion_response(request: dict[str, Any]) -> dict[str, Any]:
    messages = request.get("messages") or []
    has_tool_result = any(
        isinstance(message, dict) and message.get("role") == "tool"
        for message in messages
    )
    if not has_tool_result:
        message: dict[str, Any] = {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "city-profile",
                    "type": "function",
                    "function": {
                        "name": "get_city_profile",
                        "arguments": json.dumps({"city": "Tokyo"}),
                    },
                },
                {
                    "id": "current-weather",
                    "type": "function",
                    "function": {
                        "name": "get_current_weather",
                        "arguments": json.dumps({"city": "Tokyo"}),
                    },
                },
            ],
        }
        finish_reason = "tool_calls"
    else:
        message = {
            "role": "assistant",
            "content": json.dumps(
                {
                    "city": "Tokyo",
                    "country": "Japan",
                    "condition": "partly cloudy",
                    "temperature_celsius": 27.0,
                    "recommendation": "Carry water and a light layer.",
                    "data_source": "mock",
                }
            ),
        }
        finish_reason = "stop"

    return {
        "id": "mock-chat-completion",
        "model": request.get("model") or "mock-weather-model",
        "choices": [
            {
                "index": 0,
                "message": message,
                "finish_reason": finish_reason,
            }
        ],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 10,
            "total_tokens": 20,
        },
    }


async def _stream_response(
    response: dict[str, Any],
) -> AsyncIterator[str]:
    choice = response["choices"][0]
    message = choice["message"]
    common = {
        "id": response["id"],
        "object": "chat.completion.chunk",
        "model": response["model"],
    }

    if message.get("tool_calls"):
        for index, call in enumerate(message["tool_calls"]):
            yield _sse(
                {
                    **common,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {
                                "tool_calls": [
                                    {
                                        "index": index,
                                        "id": call["id"],
                                        "type": "function",
                                        "function": call["function"],
                                    }
                                ]
                            },
                            "finish_reason": None,
                        }
                    ],
                }
            )
    else:
        content = message.get("content") or ""
        for start in range(0, len(content), 12):
            yield _sse(
                {
                    **common,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"content": content[start : start + 12]},
                            "finish_reason": None,
                        }
                    ],
                }
            )

    yield _sse(
        {
            **common,
            "choices": [
                {
                    "index": 0,
                    "delta": {},
                    "finish_reason": choice["finish_reason"],
                }
            ],
        }
    )
    yield "data: [DONE]\n\n"


def _sse(value: dict[str, Any]) -> str:
    return f"data: {json.dumps(value, separators=(',', ':'))}\n\n"
