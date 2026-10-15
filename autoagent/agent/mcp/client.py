"""A shared SDK connection with task-safe transport ownership."""

from __future__ import annotations

import asyncio
from collections.abc import Collection, Mapping
from contextlib import AsyncExitStack, asynccontextmanager
from typing import TYPE_CHECKING, Self
from weakref import WeakSet

from .auth import MCPBearerAuth, MCPOAuth
from .callbacks import MCPCallbacks
from .config import MCPStdioConfig, MCPSSEConfig, MCPStreamableHTTPConfig, MCPTransportConfig
from .recovery import MCPReconnectPolicy, RecoveringMCPSubscription

if TYPE_CHECKING:
    import httpx2
    from mcp import Client
    from mcp_types import CallToolResult, Tool
    from mcp.shared.dispatcher import ProgressFnT

    from ..tools import ToolSet
    from .tools import MCPTool


class MCPClientError(RuntimeError):
    """Transport/protocol failure; Tool domain failures use is_error results."""

    def __init__(
        self, message: str, *, server_id: str, operation: str,
        code: int | None = None, status_code: int | None = None,
        connection_lost: bool = False, outcome_unknown: bool = False,
    ) -> None:
        super().__init__(message)
        self.server_id = server_id
        self.operation = operation
        self.code = code
        self.status_code = status_code
        self.connection_lost = connection_lost
        self.outcome_unknown = outcome_unknown


class MCPClient:
    """Lazy MCP connection, usable from concurrent tasks on one event loop.

    The owner task enters/exits the SDK's AnyIO transport scopes. Caller tasks
    only perform requests. Use the context manager inside Core runtime_lifespan
    to create and close resources on Core's loop. Reconnection is opt-in and
    never repeats dispatched application calls. Injected clients stay borrowed.
    """

    def __init__(
        self, config: MCPTransportConfig, *,
        auth: MCPBearerAuth | MCPOAuth | httpx2.Auth | None = None,
        http_client: httpx2.AsyncClient | None = None,
        callbacks: MCPCallbacks | None = None,
        reconnect_policy: MCPReconnectPolicy | None = None,
    ) -> None:
        if not isinstance(config, (MCPStdioConfig, MCPSSEConfig, MCPStreamableHTTPConfig)):
            raise TypeError("MCPClient requires an explicit transport configuration.")
        if isinstance(config, MCPStdioConfig) and (auth is not None or http_client is not None):
            raise ValueError("stdio uses subprocess env; HTTP authentication belongs to HTTP transports.")
        if http_client is not None and (auth is not None or config.headers):
            raise ValueError("Configure headers and auth on an injected HTTP client.")
        if auth is not None and not isinstance(config, MCPStdioConfig):
            if any(key.lower() == "authorization" for key in config.headers):
                raise ValueError("Configure HTTP authentication in one place.")
        self.config = type(config).model_validate(config.model_dump(mode="python"))
        self._auth = auth
        self._http_client = http_client
        self._callbacks = callbacks or MCPCallbacks()
        self._loop = None
        self._owner = None
        self._ready = None
        self._stop = None
        self._sdk = None
        self._failure = None
        self._close_failure = None
        self._active = 0
        self._closed = False
        self._context_entered = False
        self.reconnect_policy = None if reconnect_policy is None else MCPReconnectPolicy.model_validate(reconnect_policy)
        self._connection_lock = asyncio.Lock()
        self._requests = 0
        self._requests_idle = asyncio.Event()
        self._requests_idle.set()
        self._generation = 0
        self._registries = WeakSet()
        self._legacy_resources = set()

    @property
    def generation(self) -> int:
        """Successful connection identity; tool snapshots bind to this generation."""
        return self._generation

    @property
    def is_connected(self) -> bool:
        return not self._closed and self._sdk is not None and self._failure is None and not self._owner.done() and not self._stop.is_set()

    @staticmethod
    def _is_connection_error(error: Exception) -> bool:
        if isinstance(error, BaseExceptionGroup):
            return any(MCPClient._is_connection_error(item) for item in error.exceptions)
        if isinstance(error, MCPClientError):
            return error.connection_lost
        if isinstance(error, ConnectionError):
            return True
        if isinstance(error, (ImportError, ValueError, TypeError)):
            return False
        # Optional SDK/transport imports remain lazy.
        import anyio
        import httpx2
        from mcp.client.subscriptions import SubscriptionLost

        if isinstance(error, (anyio.EndOfStream, anyio.BrokenResourceError, anyio.ClosedResourceError,
                              httpx2.NetworkError, httpx2.RemoteProtocolError, SubscriptionLost)):
            return True
        if getattr(error, "code", None) == -32600 and "Session terminated" in str(error):
            return True
        if getattr(error, "code", None) == -32000:
            return True
        return False

    def _bind_loop(self) -> None:
        loop = asyncio.get_running_loop()
        if self._loop is not None and loop is not self._loop:
            raise RuntimeError("MCPClient must be used and closed on its owning event loop.")
        self._loop = loop

    def _error(self, error: Exception, operation: str) -> MCPClientError:
        # AnyIO transports may wrap the actual HTTP/protocol failure in a group.
        leaf = error
        while isinstance(leaf, ExceptionGroup) and leaf.exceptions:
            leaf = leaf.exceptions[0]
        return MCPClientError(
            f"MCP {operation} timed out." if isinstance(leaf, TimeoutError) else str(leaf),
            server_id=self.config.server_id, operation=operation,
            code=getattr(leaf, "code", None), status_code=getattr(leaf, "status_code", None)
            or getattr(getattr(leaf, "response", None), "status_code", None),
            connection_lost=self._is_connection_error(error) or (operation == "connect" and isinstance(leaf, TimeoutError)),
        )

    async def _transport(self, stack: AsyncExitStack):
        try:
            from mcp import StdioServerParameters
            from mcp.client.stdio import stdio_client
            from mcp.client.sse import sse_client
            from mcp.client.streamable_http import streamable_http_client
            import httpx2
        except ImportError as error:
            raise ImportError("Install autoagent[mcp] to use the MCP SDK client.") from error
        config = self.config
        if isinstance(config, MCPStdioConfig):
            return stdio_client(StdioServerParameters(
                command=config.command, args=list(config.args), env=config.env,
                cwd=config.cwd, encoding=config.encoding,
            ))
        http_client = self._http_client
        if http_client is None:
            headers = dict(config.headers)
            auth = self._auth
            if isinstance(auth, MCPBearerAuth):
                headers["Authorization"] = "Bearer " + auth.token.get_secret_value()
                auth = None
            elif isinstance(auth, MCPOAuth):
                auth = auth.build(config.url)
            elif auth is not None and not isinstance(auth, httpx2.Auth):
                raise TypeError("MCP HTTP auth must be Bearer, OAuth, or an SDK-compatible httpx2.Auth.")
            http_client = await stack.enter_async_context(httpx2.AsyncClient(
                headers=headers, auth=auth,
                timeout=httpx2.Timeout(config.timeout_seconds, read=config.read_timeout_seconds),
            ))
        if isinstance(config, MCPStreamableHTTPConfig):
            return streamable_http_client(
                config.url, http_client=http_client,
                terminate_on_close=config.terminate_on_close,
                max_sse_event_size=config.max_sse_event_size,
            )

        @asynccontextmanager
        async def borrowed_http():
            yield http_client

        # SSE enters the factory's context; the owner stack, rather than SSE,
        # owns this client. This also leaves injected clients open on shutdown.
        return sse_client(
            config.url, timeout=config.timeout_seconds,
            sse_read_timeout=config.read_timeout_seconds,
            httpx_client_factory=lambda **_kwargs: borrowed_http(),
        )

    async def _serve(self) -> None:
        phase = "connect"
        try:
            async with AsyncExitStack() as stack:
                transport = await self._transport(stack)
                from mcp import Client

                options = self._callbacks.sdk_options()
                message_handler = options.get("message_handler")
                async def observe_message(message):
                    if isinstance(message, Exception) and self._is_connection_error(message):
                        self._failure = self._error(message, "connection")
                    if message_handler is not None:
                        await message_handler(message)
                options["message_handler"] = observe_message

                async with asyncio.timeout(self.config.connect_timeout_seconds):
                    sdk = await stack.enter_async_context(Client(
                        transport, read_timeout_seconds=self.config.request_timeout_seconds,
                        cache=None, mode=self.config.mode, **options,
                        input_required_max_rounds=self.config.input_required_max_rounds,
                    ))
                self._sdk = sdk
                self._generation += 1
                self._ready.set()
                phase = "connection"
                await self._stop.wait()
                phase = "close"
        except asyncio.CancelledError:
            if not self._closed:
                self._failure = MCPClientError(
                    "MCP connection owner was cancelled.",
                    server_id=self.config.server_id, operation=phase,
                    connection_lost=True,
                )
        except Exception as error:
            failure = error if isinstance(error, ImportError) else self._error(error, phase)
            failure.__cause__ = error if failure is not error else None
            if phase == "close":
                self._close_failure = failure
            else:
                self._failure = failure
        finally:
            self._sdk = None
            self._ready.set()

    async def aprepare(self) -> None:
        self._bind_loop()
        if self._closed:
            raise RuntimeError("MCPClient is closed.")
        async with self._connection_lock:
            if self._closed:
                raise RuntimeError("MCPClient is closed.")
            if self.is_connected:
                return
            if self._owner is None:
                try:
                    await self._connect_once()
                    if self.generation > 1:
                        await self._restore_connections()
                    return
                except Exception as error:
                    if self.reconnect_policy is None or not self._is_connection_error(error):
                        raise
                    await self._reconnect_locked(self.reconnect_policy, self.reconnect_policy.max_attempts - 1)
                    return
            if self.reconnect_policy is not None and self._failure is not None and self._is_connection_error(self._failure):
                await self._reconnect_locked(self.reconnect_policy, self.reconnect_policy.max_attempts)
                return
            if self._failure is not None:
                raise self._failure
            await self._reconnect_locked(self.reconnect_policy or MCPReconnectPolicy(max_attempts=1, initial_delay_seconds=0), 1)

    async def _connect_once(self):
        if self._owner is None:
            self._ready = asyncio.Event()
            self._stop = asyncio.Event()
            self._owner = asyncio.create_task(self._serve(), name=f"mcp:{self.config.server_id}")
        await self._ready.wait()
        if self._closed:
            raise RuntimeError("MCPClient is closed.")
        if self._failure is not None:
            raise self._failure
        if self._sdk is None:
            raise RuntimeError("MCP connection is unavailable.")

    async def _retire_connection(self):
        if self._owner is not None:
            self._stop.set()
            if not self._ready.is_set():
                self._owner.cancel()
            await asyncio.shield(self._owner)
        self._owner = None
        self._sdk = None
        self._failure = None
        self._close_failure = None

    async def _reconnect_locked(self, policy, attempts):
        if attempts == 0:
            raise self._failure
        # Block new admissions under the connection lock; let existing RPCs settle.
        async with asyncio.timeout(policy.drain_timeout_seconds):
            while self._requests:
                await self._requests_idle.wait()
        last = self._failure
        delay = policy.initial_delay_seconds
        for _ in range(attempts):
            if self._closed:
                raise RuntimeError("MCPClient is closed.")
            await self._retire_connection()
            if delay:
                await asyncio.sleep(delay)
            try:
                await self._connect_once()
                # Only discovery is repeated; application calls are never replayed.
                await self._restore_connections()
            except Exception as error:
                if not self._is_connection_error(error):
                    raise
                last = error
                delay = min(policy.max_delay_seconds, delay * policy.multiplier)
                continue
            return
        raise last

    async def _restore_connections(self):
        if self._legacy_resources and self.protocol_version >= "2026-07-28":
            raise MCPClientError("Legacy subscriptions cannot be restored on a modern server; use listen_resilient().",
                                 server_id=self.config.server_id, operation="restore_subscriptions")
        for uri in sorted(self._legacy_resources):
            await self._request_on_sdk(self._sdk, "subscribe_resource", uri=uri)
        failures = []
        for registry in tuple(self._registries):
            try:
                await registry._restore(self._sdk, self.generation)
            except Exception as error:
                failures.append(error)
        if failures:
            raise ExceptionGroup("MCP Tool rediscovery failed after reconnect.", failures)

    async def areconnect(self, *, policy: MCPReconnectPolicy | None = None, expected_generation: int | None = None):
        """Replace the connection, re-negotiate auth/capabilities and rediscover Tools.

        expected_generation lets concurrent recovery consumers share one reconnect.
        Active application RPCs must settle within the configured drain deadline.
        """
        self._bind_loop()
        policy = MCPReconnectPolicy.model_validate(policy or self.reconnect_policy or MCPReconnectPolicy(max_attempts=1, initial_delay_seconds=0))
        async with self._connection_lock:
            if self._closed:
                raise RuntimeError("MCPClient is closed.")
            if expected_generation is not None and expected_generation != self.generation and self.is_connected:
                return
            await self._reconnect_locked(policy, policy.max_attempts)

    async def __aenter__(self) -> Self:
        self._bind_loop()
        if self._context_entered:
            raise RuntimeError("MCPClient context is already entered.")
        self._context_entered = True
        try:
            await self.aprepare()
        except BaseException:
            self._context_entered = False
            await self.aclose()
            raise
        return self

    async def __aexit__(self, *_args) -> None:
        try:
            await self.aclose()
        finally:
            self._context_entered = False

    async def aclose(self) -> None:
        if self._closed and (self._owner is None or self._owner.done()):
            if self._close_failure is not None:
                raise self._close_failure
            return
        self._bind_loop()
        if self._active or self._connection_lock.locked():
            raise RuntimeError("Settle active MCP requests before closing the client.")
        self._closed = True
        if self._owner is not None:
            self._stop.set()
            if not self._ready.is_set():
                self._owner.cancel()
            await asyncio.shield(self._owner)
        if self._close_failure is not None:
            raise self._close_failure

    @property
    def sdk_client(self) -> Client:
        """Advanced SDK access; requests made here are caller-managed."""
        self._bind_loop()
        if self._closed or self._sdk is None:
            raise RuntimeError("Prepare MCPClient before accessing its SDK connection.")
        return self._sdk

    async def _request(self, operation: str, **parameters):
        await self.aprepare()
        return await self._request_on_sdk(self._sdk, operation, **parameters)

    async def _request_on_sdk(self, sdk, operation, **parameters):
        self._active += 1
        self._requests += 1
        self._requests_idle.clear()
        try:
            return await getattr(sdk, operation)(**parameters)
        except Exception as error:
            failure = self._error(error, operation)
            failure.outcome_unknown = operation == "call_tool" and failure.code not in {-32601, -32602} and failure.status_code not in {401, 403}
            if failure.connection_lost and sdk is self._sdk:
                self._failure = failure
            raise failure from error
        finally:
            self._active -= 1
            self._requests -= 1
            if not self._requests:
                self._requests_idle.set()

    async def _list(self, operation: str, field: str) -> tuple:
        await self.aprepare()
        return await self._list_on_sdk(self._sdk, operation, field)

    async def _list_on_sdk(self, sdk, operation, field):
        values = []
        cursor = None
        seen = set()
        for _ in range(self.config.max_listing_pages):
            page = await self._request_on_sdk(sdk, operation, cursor=cursor)
            if sdk is not self._sdk:
                raise MCPClientError("Connection changed during discovery.", server_id=self.config.server_id, operation=operation, connection_lost=True)
            values.extend(getattr(page, field))
            cursor = page.next_cursor
            if cursor is None:
                return tuple(values)
            if cursor in seen:
                break
            seen.add(cursor)
        raise MCPClientError("MCP listing did not terminate.", server_id=self.config.server_id, operation=operation)

    async def list_tools(self) -> tuple[Tool, ...]:
        """Discover every page; SDK descriptions remain available to callers."""
        return await self._list("list_tools", "tools")

    async def call_tool(
        self, name: str, arguments: dict | None = None, *,
        progress_callback: ProgressFnT | None = None,
    ) -> CallToolResult:
        """Return the SDK result; is_error is a domain observation, not an exception."""
        return await self._request("call_tool", name=name, arguments=arguments, progress_callback=progress_callback)

    async def list_resources(self) -> tuple:
        return await self._list("list_resources", "resources")

    async def list_resource_templates(self) -> tuple:
        return await self._list("list_resource_templates", "resource_templates")

    async def read_resource(self, uri: str):
        return await self._request("read_resource", uri=uri)

    async def list_prompts(self) -> tuple:
        return await self._list("list_prompts", "prompts")

    async def get_prompt(self, name: str, arguments: dict[str, str] | None = None):
        return await self._request("get_prompt", name=name, arguments=arguments)

    async def read_resource_content(self, uri: str):
        """Read and normalize content; linked assets are never fetched implicitly."""
        from .content import mcp_resource_response

        return mcp_resource_response(await self.read_resource(uri))

    async def get_prompt_messages(self, name: str, arguments: dict[str, str] | None = None):
        from .content import mcp_prompt_response

        return mcp_prompt_response(await self.get_prompt(name, arguments))

    async def complete(self, ref, argument: dict[str, str], context_arguments: dict[str, str] | None = None):
        return await self._request("complete", ref=ref, argument=argument, context_arguments=context_arguments)

    async def subscribe_resource(self, uri: str):
        """Legacy resource subscriptions; modern servers use listen()."""
        result = await self._legacy_subscription("subscribe_resource", uri)
        self._legacy_resources.add(uri)
        return result

    async def unsubscribe_resource(self, uri: str):
        result = await self._legacy_subscription("unsubscribe_resource", uri)
        self._legacy_resources.discard(uri)
        return result

    async def _legacy_subscription(self, operation, uri):
        await self.aprepare()
        if self.protocol_version >= "2026-07-28":
            raise MCPClientError("Use listen(resource_subscriptions=...) with the negotiated MCP protocol.",
                                 server_id=self.config.server_id, operation=operation)
        return await self._request(operation, uri=uri)

    async def send_roots_list_changed(self):
        return await self._request("send_roots_list_changed")

    @property
    def protocol_version(self) -> str:
        return self.sdk_client.protocol_version

    @property
    def server_info(self) -> dict | None:
        value = self.sdk_client.server_info
        return None if value is None else value.model_dump(mode="json", by_alias=True)

    @property
    def server_capabilities(self) -> dict:
        return self.sdk_client.server_capabilities.model_dump(mode="json", by_alias=True)

    @property
    def instructions(self) -> str | None:
        return self.sdk_client.instructions

    async def tools(
        self, *, prefix: str = "", names: Mapping[str, str] | None = None,
        include: Collection[str] | None = None, exclude: Collection[str] = (),
    ) -> tuple[MCPTool, ...]:
        """Filter remote names before schema compilation, then apply aliases.

        Explicit includes must exist so a typo cannot silently remove an
        intended capability. Excludes are useful across changing catalogs.
        """
        from .tools import MCPTool

        values = await self.list_tools()
        return self._tools_from_descriptions(values, prefix=prefix, names=names, include=include, exclude=exclude)

    def _tools_from_descriptions(self, values, *, prefix="", names=None, include=None, exclude=()):
        from .tools import MCPTool
        allowed = None if include is None else frozenset(include)
        denied = frozenset(exclude)
        if allowed is not None and (missing := allowed - {value.name for value in values}):
            raise ValueError(f"Unknown included MCP tools: {sorted(missing)!r}.")
        selected = tuple(value for value in values if (allowed is None or value.name in allowed) and value.name not in denied)
        tools = tuple(MCPTool(self, value, name=prefix + (names or {}).get(value.name, value.name)) for value in selected)
        from ..tools import ToolSet

        ToolSet(tools)  # Check alias collisions even when returning individual tools.
        return tools

    async def _toolset_on_sdk(self, sdk, **options):
        from ..tools import ToolSet

        values = await self._list_on_sdk(sdk, "list_tools", "tools")
        return ToolSet(self._tools_from_descriptions(values, **options))

    async def toolset(
        self, *, prefix: str = "", names: Mapping[str, str] | None = None,
        include: Collection[str] | None = None, exclude: Collection[str] = (),
    ) -> ToolSet:
        from ..tools import ToolSet

        return ToolSet(await self.tools(prefix=prefix, names=names, include=include, exclude=exclude))

    @asynccontextmanager
    async def listen_tools(self):
        """Expose modern SDK change events with client lifecycle accounting.

        Consume this on the owning loop and explicitly refresh snapshots after
        changes. Legacy notifications are delivered through message_handler.
        The SDK validates support and the subscription's honored filter.
        """
        async with self.listen(tools_list_changed=True) as subscription:
            yield subscription

    @asynccontextmanager
    async def listen(self, *, tools_list_changed: bool = False, prompts_list_changed: bool = False,
                     resources_list_changed: bool = False, resource_subscriptions: Collection[str] = ()):
        """Modern SDK subscriptions with shared client lifecycle accounting."""
        if isinstance(resource_subscriptions, str):
            raise TypeError("Resource subscriptions require a collection of URIs.")
        await self.aprepare()
        self._active += 1
        sdk = self._sdk
        try:
            async with sdk.listen(tools_list_changed=tools_list_changed, prompts_list_changed=prompts_list_changed,
                                        resources_list_changed=resources_list_changed,
                                        resource_subscriptions=tuple(resource_subscriptions)) as subscription:
                if sdk is not self._sdk:
                    raise MCPClientError("Connection changed while acknowledging the subscription.",
                                         server_id=self.config.server_id, operation="listen", connection_lost=True)
                yield subscription
        finally:
            self._active -= 1

    @asynccontextmanager
    async def listen_resilient(self, *, tools_list_changed: bool = False, prompts_list_changed: bool = False,
                               resources_list_changed: bool = False, resource_subscriptions: Collection[str] = (),
                               policy: MCPReconnectPolicy | None = None):
        """Recover modern subscriptions and announce each resync boundary.

        Legacy resource subscriptions are restored during connection recovery;
        their notifications continue through the SDK message_handler callback.
        """
        self._bind_loop()
        if self._closed:
            raise RuntimeError("MCPClient is closed.")
        options = dict(tools_list_changed=tools_list_changed, prompts_list_changed=prompts_list_changed,
                       resources_list_changed=resources_list_changed, resource_subscriptions=resource_subscriptions)
        policy = MCPReconnectPolicy.model_validate(policy or self.reconnect_policy or MCPReconnectPolicy())
        subscription = RecoveringMCPSubscription(self, options, policy)
        self._active += 1
        try:
            await subscription.start()
            yield subscription
        finally:
            try:
                await subscription.aclose()
            finally:
                self._active -= 1


__all__ = ["MCPClient", "MCPClientError"]
