from __future__ import annotations

import json
from typing import Any

from fastapi import FastAPI


app = FastAPI(title="AutoAgent deterministic Chat Completions mock")


@app.post("/v1/chat/completions")
async def chat_completions(request: dict[str, Any]) -> dict[str, Any]:
    """Return a fixed Tool turn followed by one fixed structured answer."""

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
