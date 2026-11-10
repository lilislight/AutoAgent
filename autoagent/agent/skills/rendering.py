"""Render Skill context as JSON-framed user content without changing its authority."""

from __future__ import annotations

import json
from typing import Literal

from ..models import LLMMessage, TextContent
from .models import SkillDocument, SkillMetadata


def document_message(document: SkillDocument, *, role: Literal["user", "developer"] = "user", include_revisions=True) -> LLMMessage:
    return LLMMessage(role=role, content=(TextContent(text=json.dumps({
        "skill": document.metadata.model_dump(mode="json", exclude_none=True),
        "instructions": document.instructions,
        **({"revision": document.revision} if include_revisions else {}),
        **({"snapshot_revision": document.snapshot_revision} if include_revisions and document.snapshot_revision is not None else {}),
    }, ensure_ascii=False)),))


def catalog_message(metadata: tuple[SkillMetadata, ...]) -> LLMMessage | None:
    if not metadata:
        return None
    return LLMMessage(role="user", content=(TextContent(text=json.dumps({
        "available_skills": [{"name": value.name, "description": value.description} for value in metadata],
    }, ensure_ascii=False)),))
