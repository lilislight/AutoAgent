"""Framework regression fixture for ReAct and LLM persistence behavior."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Literal

from dotenv import dotenv_values
from pydantic import BaseModel, Field

from autoagent import (
    AutoAgentApp,
    AutoAgentSettings,
    AutoAgentServer,
    NodePolicy,
    RecoveryPolicy,
    Workflow,
)
from autoagent.ai import (
    ChatCompletionsConfig,
    ChatCompletionsProvider,
    LLM_CALL_CAPABILITY_ID,
    LLMMessage,
    LLMRequest,
    LLMResponse,
    llm_call_node,
    react_workflow,
    register_llm_call_operator,
    tool,
)
from autoagent.ai.providers.factory import llm_provider_from_environment


class CityProfile(BaseModel):
    city: str
    country: str
    timezone: str
    known_for: list[str]


class WeatherReading(BaseModel):
    city: str
    temperature: float
    unit: Literal["celsius", "fahrenheit"]
    condition: str
    humidity_percent: int = Field(ge=0, le=100)


class WeatherAnswer(BaseModel):
    city: str
    country: str
    weather: str
    temperature: float
    unit: Literal["celsius", "fahrenheit"]
    recommendation: str


_CITY_PROFILES = {
    "beijing": CityProfile(
        city="Beijing",
        country="China",
        timezone="Asia/Shanghai",
        known_for=["Forbidden City", "hutongs", "roast duck"],
    ),
    "london": CityProfile(
        city="London",
        country="United Kingdom",
        timezone="Europe/London",
        known_for=["River Thames", "museums", "West End"],
    ),
    "san francisco": CityProfile(
        city="San Francisco",
        country="United States",
        timezone="America/Los_Angeles",
        known_for=["Golden Gate Bridge", "fog", "steep hills"],
    ),
    "tokyo": CityProfile(
        city="Tokyo",
        country="Japan",
        timezone="Asia/Tokyo",
        known_for=["rail network", "food", "neighborhoods"],
    ),
}

_WEATHER_CELSIUS = {
    "beijing": (24.0, "clear", 38),
    "london": (15.0, "light rain", 81),
    "san francisco": (17.0, "foggy", 76),
    "tokyo": (27.0, "partly cloudy", 68),
}


@tool(
    id="mock.city_profile",
    name="get_city_profile",
    description="Look up mocked country, timezone, and highlights for a city.",
)
def get_city_profile(city: str) -> CityProfile:
    """Return deterministic mocked city information."""

    key = _city_key(city)
    try:
        return _CITY_PROFILES[key]
    except KeyError as exc:
        raise ValueError(f"Mock city data is unavailable for: {city}") from exc


@tool(
    id="mock.current_weather",
    name="get_current_weather",
    description="Get deterministic mocked current weather for a supported city.",
)
def get_current_weather(
    city: str,
    unit: Literal["celsius", "fahrenheit"] = "celsius",
) -> WeatherReading:
    """Return a mocked weather reading without making a network request."""

    key = _city_key(city)
    try:
        temperature, condition, humidity = _WEATHER_CELSIUS[key]
        profile = _CITY_PROFILES[key]
    except KeyError as exc:
        raise ValueError(f"Mock weather data is unavailable for: {city}") from exc
    if unit == "fahrenheit":
        temperature = round(temperature * 9 / 5 + 32, 1)
    return WeatherReading(
        city=profile.city,
        temperature=temperature,
        unit=unit,
        condition=condition,
        humidity_percent=humidity,
    )


def build_react_workflow() -> Workflow:
    return react_workflow(
        id="mock_weather_react",
        name="Mock Weather ReAct",
        description=(
            "A ReAct Workflow using mocked city and weather Tools with a "
            "structured final response."
        ),
        instructions=(
            "You are a concise weather assistant. Use get_city_profile to resolve "
            "the city and country, and get_current_weather for the weather. The "
            "Tools return mocked example data, so never describe it as live data. "
            "Return the final answer using the required structured output."
        ),
        tools=[get_city_profile, get_current_weather],
        response_format=WeatherAnswer,
        max_tool_parse_retries=1,
        max_output_parse_retries=1,
        max_steps=12,
    )


def build_workflow() -> Workflow:
    """Build a parent Workflow containing the ReAct weather child Workflow."""

    workflow = Workflow(
        id="mock_weather_chinese",
        name="Mock Weather with Chinese Translation",
        description=(
            "Runs the mocked-weather ReAct child Workflow and translates its "
            "structured answer into Chinese with one additional LLM Node."
        ),
    )
    weather_agent = build_react_workflow()

    def map_translation_request(ctx) -> dict[str, LLMRequest]:
        weather_answer = ctx.incoming[0].value
        return {
            "request": LLMRequest(
                messages=(
                    LLMMessage(
                        role="system",
                        content=(
                            "You are a precise translator. Translate the supplied "
                            "weather answer into natural Simplified Chinese. "
                            "Preserve every fact, number, and unit. Return only the "
                            "Chinese translation without Markdown or commentary."
                        ),
                    ),
                    LLMMessage(
                        role="user",
                        content=json.dumps(
                            weather_answer,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    ),
                ),
            )
        }

    def translated_output(response: LLMResponse) -> str:
        content = response.message.content
        if content is None or not content.strip():
            raise ValueError("Translation LLM returned no text content.")
        return content.strip()

    workflow.add_node(weather_agent, node_id="weather_agent")
    workflow.add_node(
        llm_call_node(
            id="translate_to_chinese",
            input_mapping=map_translation_request,
            policy=NodePolicy(recovery=RecoveryPolicy(mode="never")),
        ),
    )
    workflow.add_node(
        translated_output,
        node_id="translated_output",
        input_mapping=lambda ctx: {"response": ctx.incoming[0].value},
    )
    workflow.add_edge("weather_agent", "translate_to_chinese")
    workflow.add_edge("translate_to_chinese", "translated_output")
    return workflow


def build_app(
    config: ChatCompletionsConfig | None = None,
) -> tuple[AutoAgentApp, Workflow]:
    """Build the App served by the mocked-weather tracing example."""

    provider = (
        ChatCompletionsProvider(config)
        if config is not None
        else llm_provider_from_environment(
            {
                key: str(value)
                for key, value in {
                    **dotenv_values(Path.cwd() / ".env"),
                    **os.environ,
                }.items()
                if value is not None
            }
        )
    )
    app = AutoAgentApp(
        settings=AutoAgentSettings() if config is not None else None
    )
    register_llm_call_operator(
        app,
        provider,
        operator_id=f"{provider.provider_name}.default",
    )
    workflow = build_workflow()
    app.register_workflow(workflow)
    return app, workflow


def main() -> None:
    app, workflow = build_app()
    compile_result = app.compiler.compile(workflow)
    if not compile_result.ok:
        diagnostics = "\n".join(
            f"{item.severity}: {item.message}"
            for item in compile_result.diagnostics
        )
        raise RuntimeError(f"Weather ReAct Workflow failed to compile:\n{diagnostics}")

    env_file = dotenv_values(Path.cwd() / ".env")

    def configured(name: str, default: str) -> str:
        value = os.environ.get(name, env_file.get(name))
        return default if value is None or not str(value).strip() else str(value)

    host = configured("AUTOAGENT_SERVER_HOST", "0.0.0.0")
    port = int(configured("AUTOAGENT_SERVER_PORT", "8765"))
    browser_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    server_url = f"http://{browser_host}:{port}"

    assert compile_result.workflow_ir is not None
    print("Workflow:", workflow.id)
    print("Compiled entries:", compile_result.workflow_ir.entry_node_ids)
    print("Compiled exits:", compile_result.workflow_ir.exit_node_ids)
    print("Expanded node count:", len(compile_result.workflow_ir.nodes))
    print(f"\nTracing UI: {server_url}")
    print(f"Tracing API: {server_url}/api/v1")
    print(
        "Invoke from the UI by selecting the Workflow and clicking its entry "
        "Node. The input accepts `input` or `messages`, plus optional "
        "`provider_options` and `mode` (`invoke` or `stream`). For example: "
        '{"input":"What is the weather in Tokyo?",'
        '"provider_options":{},"mode":"invoke"}. '
        "The ReAct child answer is translated into Chinese by the final LLM Node."
    )
    AutoAgentServer(app).run(host=host, port=port)


def _city_key(city: str) -> str:
    return " ".join(city.strip().lower().split())


if __name__ == "__main__":
    main()
