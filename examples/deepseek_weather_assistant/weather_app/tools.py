"""A small deterministic dataset: these Tools never call a weather service."""

from typing import Literal

from pydantic import BaseModel, ConfigDict

from autoagent.agent import ToolBusinessError, ToolSet, tool


class City(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    city_id: str
    name: str
    aliases: tuple[str, ...]


class Forecast(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    city_id: str
    city: str
    day: Literal["today", "tomorrow"]
    condition: str
    low_c: int
    high_c: int
    rain_probability_percent: int
    source: Literal["simulated"] = "simulated"


CITIES = (
    City(city_id="beijing", name="北京", aliases=("北京", "北京市", "Beijing", "Peking")),
    City(city_id="shanghai", name="上海", aliases=("上海", "上海市", "Shanghai")),
    City(city_id="hangzhou", name="杭州", aliases=("杭州", "杭州市", "Hangzhou")),
    City(city_id="guangzhou", name="广州", aliases=("广州", "广州市", "Guangzhou", "Canton")),
    City(city_id="shenzhen", name="深圳", aliases=("深圳", "深圳市", "Shenzhen")),
    City(city_id="chengdu", name="成都", aliases=("成都", "成都市", "Chengdu")),
)
# Each tuple is condition, minimum, maximum, rain probability; never a real forecast.
WEATHER = {
    "beijing": (("晴", 10, 21, 5), ("多云", 11, 22, 15)),
    "shanghai": (("小雨", 17, 23, 80), ("阴", 18, 24, 30)),
    "hangzhou": (("多云", 16, 25, 20), ("小雨", 17, 23, 75)),
    "guangzhou": (("晴", 23, 30, 10), ("雷阵雨", 24, 29, 85)),
    "shenzhen": (("多云", 24, 29, 25), ("阵雨", 24, 28, 65)),
    "chengdu": (("阴", 15, 21, 30), ("小雨", 14, 20, 70)),
}


@tool
def list_cities() -> list[City]:
    """List all supported simulated cities, their IDs and Chinese/English aliases."""
    return list(CITIES)


@tool
def resolve_city(name: str) -> City:
    """Resolve an exact Chinese/English city name to the city_id needed by get_weather."""
    normalized = name.strip().casefold()
    for city in CITIES:
        if normalized in {alias.casefold() for alias in (*city.aliases, city.city_id)}:
            return city
    raise ToolBusinessError("Unsupported city. Use list_cities to see available simulated cities.",
                            code="city_not_found")


@tool
def get_weather(city_id: str, day: Literal["today", "tomorrow"] = "today") -> Forecast:
    """Get SIMULATED weather for a city_id and today/tomorrow; not a real forecast."""
    city = next((city for city in CITIES if city.city_id == city_id), None)
    if city is None:
        raise ToolBusinessError("Unknown city_id. Resolve the city name first.", code="city_not_found")
    condition, low, high, rain = WEATHER[city_id][0 if day == "today" else 1]
    return Forecast(city_id=city_id, city=city.name, day=day, condition=condition,
                    low_c=low, high_c=high, rain_probability_percent=rain)


def weather_tools() -> ToolSet:
    return ToolSet((list_cities, resolve_city, get_weather))
