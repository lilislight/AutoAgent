"""A local OAuth redirect receiver; application code presents the login URL."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import math
from urllib.parse import parse_qs, urlsplit
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from mcp.client.auth import AuthorizationCodeResult


class MCPOAuthCallbackServer:
    """Loopback-only callback resource for SDK authorization-code OAuth.

    Enter before constructing client metadata; redirect_uri then includes the
    assigned port. Use redirect_handler and callback_handler with MCPOAuth.
    on_authorization_url can present the URL in a UI or open a browser. This
    resource itself never opens one. SDK checks PKCE, state and issuer as usual.
    """

    def __init__(
        self, *, port: int = 0, path: str = "/callback", timeout_seconds: float = 120,
        on_authorization_url: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        if not 0 <= port <= 65535 or not path.startswith("/") or any(c in path for c in "?#\r\n"):
            raise ValueError("Invalid OAuth callback port or path.")
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("Callback timeout must be positive and finite.")
        self._port, self._path, self._timeout = port, path, timeout_seconds
        self._on_url = on_authorization_url
        self._server = None
        self._loop = None
        self._result = None
        self._state = None
        self._url_queue = None
        self._handlers = set()

    def _require_open(self):
        if self._server is None or asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Use OAuth callback server inside its context on the owning loop.")

    @property
    def redirect_uri(self) -> str:
        self._require_open()
        return f"http://127.0.0.1:{self._server.sockets[0].getsockname()[1]}{self._path}"

    async def __aenter__(self) -> Self:
        if self._server is not None:
            raise RuntimeError("OAuth callback server context is already entered.")
        self._loop = asyncio.get_running_loop()
        self._url_queue = asyncio.Queue(maxsize=1)
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", self._port, limit=8192)
        return self

    async def __aexit__(self, *_args):
        self._require_open()
        self._server.close()
        await self._server.wait_closed()
        if self._result is not None and not self._result.done():
            self._result.cancel()
        for task in tuple(self._handlers):
            task.cancel()
        await asyncio.gather(*self._handlers, return_exceptions=True)
        self._server = None

    async def redirect_handler(self, url: str) -> None:
        self._require_open()
        if self._result is not None and not self._result.done():
            raise RuntimeError("An OAuth login is already pending.")
        query = parse_qs(urlsplit(url).query)
        states = query.get("state", [])
        if len(states) != 1 or not states[0]:
            raise ValueError("OAuth authorization URL must contain one state.")
        self._state = states[0]
        self._result = self._loop.create_future()
        if not self._url_queue.empty():
            self._url_queue.get_nowait()
        self._url_queue.put_nowait(url)
        if self._on_url is not None:
            await self._on_url(url)

    async def wait_for_authorization_url(self) -> str:
        self._require_open()
        async with asyncio.timeout(self._timeout):
            return await self._url_queue.get()

    async def callback_handler(self) -> AuthorizationCodeResult:
        self._require_open()
        if self._result is None:
            raise RuntimeError("Present an OAuth authorization URL before waiting for callback.")
        async with asyncio.timeout(self._timeout):
            result = await self._result
        if isinstance(result, Exception):
            raise result
        return result

    async def _handle(self, reader, writer):
        task = asyncio.current_task()
        self._handlers.add(task)
        status, message = "400 Bad Request", "Invalid OAuth callback."
        try:
            async with asyncio.timeout(min(self._timeout, 10)):
                request = (await reader.readuntil(b"\r\n\r\n")).decode("ascii")
                method, target, _version = request.split("\r\n", 1)[0].split(" ")
                parsed = urlsplit(target)
                query = parse_qs(parsed.query, keep_blank_values=True)
                if parsed.path != self._path or method != "GET":
                    status = "404 Not Found"
                elif self._result is None or self._result.done():
                    status, message = "409 Conflict", "No OAuth login is pending."
                elif query.get("state") != [self._state]:
                    message = "OAuth state did not match."
                elif "error" in query:
                    # No credentials or server-provided error details in the response.
                    self._result.set_result(ValueError("OAuth authorization was denied."))
                    status, message = "200 OK", "Authorization was denied. You may close this window."
                elif len(query.get("code", [])) == 1 and query["code"][0] and len(query.get("iss", [])) <= 1:
                    from mcp.client.auth import AuthorizationCodeResult

                    self._result.set_result(AuthorizationCodeResult(
                        code=query["code"][0], state=query["state"][0],
                        iss=query.get("iss", [None])[0],
                    ))
                    status, message = "200 OK", "Authorization received. You may close this window."
        except (ValueError, UnicodeError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, TimeoutError):
            pass
        finally:
            try:
                body = message.encode()
                writer.write((f"HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nCache-Control: no-store\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode() + body)
                await writer.drain()
            except (ConnectionError, OSError):
                pass
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionError, OSError):
                    pass
                finally:
                    self._handlers.discard(task)


__all__ = ["MCPOAuthCallbackServer"]
