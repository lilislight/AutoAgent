"""Serializable media locations; resolving or uploading media is separate."""

from __future__ import annotations

import base64
import binascii
from typing import Annotated, Literal
from urllib.parse import urlsplit

from pydantic import Field, field_validator

from ._base import Model, NonEmpty


class URLSource(Model):
    """An HTTP(S) media URL, not an inline data URL."""

    type: Literal["url"] = "url"
    url: NonEmpty
    mime_type: NonEmpty | None = None

    @field_validator("url")
    @classmethod
    def _http_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or any(character.isspace() for character in value)
        ):
            raise ValueError("Media URL must be an absolute HTTP(S) URL.")
        _ = parsed.port  # Reject malformed ports as well as malformed hosts.
        return value


class DataSource(Model):
    """Base64-encoded bytes with their MIME type, without a data URL prefix."""

    type: Literal["data"] = "data"
    data: NonEmpty
    mime_type: NonEmpty

    @field_validator("data")
    @classmethod
    def _base64(cls, value: str) -> str:
        try:
            base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error) as error:
            raise ValueError("Media data must be valid base64.") from error
        return value

    @field_validator("mime_type")
    @classmethod
    def _mime_type(cls, value: str) -> str:
        media_type = value.split(";", 1)[0]
        parts = media_type.split("/")
        if len(parts) != 2 or not all(parts) or any(c.isspace() for c in media_type):
            raise ValueError("Media MIME type must have a type/subtype.")
        return value


class AssetSource(Model):
    """An application-owned resource reference resolved by the caller.

    This model does not read files or create an artifact store. The application
    owns the referenced resource's availability across Runtime recovery.
    """

    type: Literal["asset"] = "asset"
    uri: NonEmpty
    mime_type: NonEmpty | None = None


class ProviderSource(Model):
    """An uploaded file scoped to a specific provider connection identity."""

    type: Literal["provider"] = "provider"
    provider_id: NonEmpty
    file_id: NonEmpty
    mime_type: NonEmpty | None = None


LLMSource = Annotated[
    URLSource | DataSource | AssetSource | ProviderSource,
    Field(discriminator="type"),
]

__all__ = ["AssetSource", "DataSource", "LLMSource", "ProviderSource", "URLSource"]
