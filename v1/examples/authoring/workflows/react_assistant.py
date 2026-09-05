from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from autoagent import Workflow
from autoagent.ai import react_workflow, tool


class CityProfile(BaseModel):
    city: str
    country: str
    timezone: str


class WeatherReading(BaseModel):
    city: str
    temperature_celsius: float
    condition: str


class WeatherAnswer(BaseModel):
    city: str
    country: str
    condition: str
    temperature_celsius: float
    recommendation: str = Field(min_length=1)
    data_source: Literal["mock"]


_CITY_PROFILES = {
    "tokyo": CityProfile(
        city="Tokyo",
        country="Japan",
        timezone="Asia/Tokyo",
    ),
    "london": CityProfile(
        city="London",
        country="United Kingdom",
        timezone="Europe/London",
    ),
}

_WEATHER = {
    "tokyo": WeatherReading(
        city="Tokyo",
        temperature_celsius=27.0,
        condition="partly cloudy",
    ),
    "london": WeatherReading(
        city="London",
        temperature_celsius=15.0,
        condition="light rain",
    ),
}


@tool(
    id="sample.city_profile",
    name="get_city_profile",
    description="Return deterministic country and timezone data for a city.",
)
def get_city_profile(city: str) -> CityProfile:
    key = city.strip().lower()
    if key not in _CITY_PROFILES:
        raise ValueError(f"Unsupported sample city: {city}")
    return _CITY_PROFILES[key]


@tool(
    id="sample.current_weather",
    name="get_current_weather",
    description="Return deterministic mocked weather for a city.",
)
def get_current_weather(city: str) -> WeatherReading:
    key = city.strip().lower()
    if key not in _WEATHER:
        raise ValueError(f"Unsupported sample city: {city}")
    return _WEATHER[key]


workflow: Workflow = react_workflow(
    id="weather_assistant",
    name="Weather assistant",
    description=(
        "Demonstrates llm_call, typed Tools, parallel Tool calls, and "
        "structured ReAct output."
    ),
    instructions=(
        "You are a deterministic sample weather assistant. Use both Tools to "
        "answer the request. The Tool data is mocked, not live. Return the "
        "required structured output and set data_source to mock."
    ),
    tools=(get_city_profile, get_current_weather),
    response_format=WeatherAnswer,
    max_tool_parse_retries=1,
    max_output_parse_retries=1,
    max_steps=8,
)
