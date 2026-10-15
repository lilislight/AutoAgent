"""Runtime authentication resources backed by the official MCP SDK."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
import asyncio
import math
import time
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

if TYPE_CHECKING:
    from mcp.client.auth import AuthorizationCodeResult, TokenStorage
    from mcp.shared.auth import OAuthClientInformationFull, OAuthClientMetadata, OAuthToken


class MCPBearerAuth(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    token: SecretStr = Field(repr=False)

    @field_validator("token")
    @classmethod
    def _token(cls, value: SecretStr) -> SecretStr:
        text = value.get_secret_value()
        if not text or any(character.isspace() for character in text):
            raise ValueError("Bearer token must be nonempty and contain no whitespace.")
        return value


class MCPMemoryTokenStorage:
    """SDK TokenStorage implementation; applications may inject persistent storage.

    Store both client registration and tokens. Copies prevent callers mutating
    stored secrets accidentally. Storage itself is a runtime resource.
    """

    def __init__(self) -> None:
        self._tokens = None
        self._client_info = None
        self._expires_at = None

    async def get_tokens(self) -> OAuthToken | None:
        if self._tokens is None:
            return None
        tokens = self._tokens.model_copy(deep=True)
        if self._expires_at is not None:
            tokens.expires_in = max(0, int(self._expires_at - time.time()))
        return tokens

    async def set_tokens(self, tokens: OAuthToken) -> None:
        self._tokens = tokens.model_copy(deep=True)
        self._expires_at = None if tokens.expires_in is None else time.time() + tokens.expires_in

    async def get_client_info(self) -> OAuthClientInformationFull | None:
        return None if self._client_info is None else self._client_info.model_copy(deep=True)

    async def set_client_info(self, client_info: OAuthClientInformationFull) -> None:
        self._client_info = client_info.model_copy(deep=True)


@dataclass(frozen=True, slots=True)
class MCPOAuth:
    """Delegate discovery, registration, PKCE, issuer/state checks and refresh.

    Handlers and storage belong to the application. Construction opens neither
    a browser nor a callback server. The callback returns the SDK's
    AuthorizationCodeResult, preserving code, state and iss from the redirect.
    """

    client_metadata: OAuthClientMetadata = field(repr=False)
    storage: TokenStorage = field(repr=False)
    redirect_handler: Callable[[str], Awaitable[None]] | None = field(default=None, repr=False)
    callback_handler: Callable[[], Awaitable[AuthorizationCodeResult]] | None = field(default=None, repr=False)
    client_metadata_url: str | None = None
    login_timeout_seconds: float = 120

    def __post_init__(self):
        if not math.isfinite(self.login_timeout_seconds) or self.login_timeout_seconds <= 0:
            raise ValueError("OAuth login timeout must be positive and finite.")

    def build(self, server_url: str):
        from mcp.client.auth import OAuthClientProvider

        async def redirect(url):
            async with asyncio.timeout(self.login_timeout_seconds):
                await self.redirect_handler(url)

        async def callback():
            async with asyncio.timeout(self.login_timeout_seconds):
                return await self.callback_handler()

        return OAuthClientProvider(
            server_url=server_url, client_metadata=self.client_metadata.model_copy(deep=True),
            storage=self.storage,
            redirect_handler=None if self.redirect_handler is None else redirect,
            callback_handler=None if self.callback_handler is None else callback,
            client_metadata_url=self.client_metadata_url,
        )


__all__ = ["MCPBearerAuth", "MCPMemoryTokenStorage", "MCPOAuth"]
