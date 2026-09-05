"""Small query values shared by Store, Server, and CLI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Page(Generic[T]):
    """One stable keyset page."""

    items: tuple[T, ...]
    next_cursor: str | None = None

    @property
    def has_more(self) -> bool:
        return self.next_cursor is not None

    def to_record(self) -> dict[str, object]:
        return {
            "items": list(self.items),
            "next_cursor": self.next_cursor,
            "has_more": self.has_more,
        }


@dataclass(frozen=True, slots=True)
class ResumablePage(Generic[T]):
    """One keyset page whose last consumed position is always resumable."""

    items: tuple[T, ...]
    next_cursor: str | None = None
    resume_cursor: str | None = None

    @property
    def has_more(self) -> bool:
        return self.next_cursor is not None

    def to_record(self) -> dict[str, object]:
        return {
            "items": list(self.items),
            "next_cursor": self.next_cursor,
            "resume_cursor": self.resume_cursor,
            "has_more": self.has_more,
        }


__all__ = ["Page", "ResumablePage"]
