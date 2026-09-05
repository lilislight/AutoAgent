from __future__ import annotations

from collections.abc import Callable
import re
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


_USER_EVENT_TYPE = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")


def validate_user_event_type(value: str) -> str:
    resolved = value.strip()
    if not _USER_EVENT_TYPE.fullmatch(resolved):
        raise ValueError(
            "UserEvent type must use lowercase snake_case and start with "
            "a letter."
        )
    return resolved


class UserEventMapping(BaseModel):
    """Convert one framework-selected value into a UserEvent payload.

    The containing Node field determines whether ``transform`` receives a
    StreamingResult chunk or the successfully committed Node output. Returning
    ``None`` suppresses an Event for that value.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
    )

    type: str = Field(description="Stable snake_case UserEvent type.")
    transform: Callable[[Any], Any | None]

    @field_validator("type")
    @classmethod
    def validate_type(cls, value: str) -> str:
        return validate_user_event_type(value)


UserEventMappings = (
    UserEventMapping
    | tuple[UserEventMapping, ...]
    | None
)


def normalize_user_event_mappings(
    value: UserEventMappings,
) -> tuple[UserEventMapping, ...]:
    if value is None:
        return ()
    if isinstance(value, UserEventMapping):
        return (value,)
    return tuple(value)
