"""Small deterministic Tools; only saving a note performs a local write."""

import json
from pathlib import Path
from uuid import uuid4

from pydantic import JsonValue

from autoagent.agent import ToolBusinessError, ToolContext


NOTES_DIRECTORY = Path(__file__).resolve().parents[1] / ".autoagent" / "notes"


def add_numbers(a: float, b: float) -> dict[str, JsonValue]:
    """Add two numbers and return their sum."""
    return {"a": a, "b": b, "sum": a + b}


def get_demo_weather(city: str) -> dict[str, JsonValue]:
    """Read simulated weather for Beijing/北京, Shanghai/上海 or Shenzhen/深圳; this is not live data."""
    aliases = {"beijing": "北京", "shanghai": "上海", "shenzhen": "深圳"}
    city = aliases.get(city.strip().lower(), city.strip())
    weather = {"北京": ("晴", 12, 23), "上海": ("多云", 17, 25), "深圳": ("小雨", 23, 28)}
    if city not in weather:
        raise ToolBusinessError("Supported cities: 北京, 上海, 深圳.", code="city_not_found")
    condition, low, high = weather[city]
    return {"city": city, "condition": condition, "low_c": low, "high_c": high, "source": "simulated"}


def save_demo_note(title: str, content: str) -> dict[str, JsonValue]:
    """Save a UTF-8 note inside this demo's notes directory. Requires explicit Tool approval."""
    if not title.strip() or not content.strip():
        raise ToolBusinessError("Both title and content must be nonempty.", code="invalid_arguments")
    note_id = uuid4().hex
    NOTES_DIRECTORY.mkdir(parents=True, exist_ok=True)
    path = NOTES_DIRECTORY / (note_id + ".json")
    path.write_text(json.dumps({"title": title, "content": content,
        "session_id": ToolContext.current().session_id}, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"note_id": note_id, "title": title, "path": str(path), "saved": True}
