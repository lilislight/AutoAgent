"""OAuth configuration/storage/callback demo and explicit live auth entry points.

Run offline: .venv/bin/python -m examples.agent_usage_guide.mcp_auth
Run live:    ...mcp_auth --mode oauth --url https://your-server.example/mcp
"""

import argparse
import asyncio
from pathlib import Path
import os
from tempfile import TemporaryDirectory
from urllib.parse import urlencode
import webbrowser

import httpx
from mcp.shared.auth import OAuthClientMetadata, OAuthToken

from autoagent.agent import (
    MCPBearerAuth, MCPClient, MCPFileTokenStorage, MCPMemoryTokenStorage,
    MCPOAuth, MCPOAuthCallbackServer, MCPStreamableHTTPConfig,
)
from .support import show


def oauth(callback, storage):
    return MCPOAuth(client_metadata=OAuthClientMetadata(
        client_name="AutoAgent usage example", redirect_uris=[callback.redirect_uri],
        grant_types=["authorization_code", "refresh_token"], response_types=["code"],
        token_endpoint_auth_method="none"), storage=storage,
        redirect_handler=callback.redirect_handler, callback_handler=callback.callback_handler)


async def offline():
    # Exercise token persistence and callback validation without an authorization server.
    # These are test values, never usable credentials.
    with TemporaryDirectory() as directory:
        path = Path(directory) / "tokens.json"
        storage = MCPFileTokenStorage(path)
        await storage.set_tokens(OAuthToken(access_token="offline-test-token", token_type="Bearer", expires_in=60))
        restored = await MCPFileTokenStorage(path).get_tokens()
        assert restored.access_token == "offline-test-token" and restored.expires_in <= 60
        if os.name == "posix":
            assert path.stat().st_mode & 0o777 == 0o600
        async with MCPOAuthCallbackServer(timeout_seconds=5) as callback:
            settings = oauth(callback, storage)
            # Construct the real SDK OAuth handler, but do not invoke discovery/token exchange.
            assert settings.build("https://authorization.example/mcp") is not None
            state = "local-demo-state"
            await callback.redirect_handler("https://authorization.example/authorize?" + urlencode({"state": state}))
            async with httpx.AsyncClient(trust_env=False) as http:
                wrong = await http.get(callback.redirect_uri, params={"state": "wrong", "code": "test-code"})
                assert wrong.status_code == 400
                accepted = await http.get(callback.redirect_uri, params={"state": state, "code": "test-code", "iss": "https://authorization.example"})
                assert accepted.status_code == 200
            result = await callback.callback_handler()
            assert result.state == state and result.iss == "https://authorization.example"
        show("OAuth local checks", {"token_storage_reopened": True, "wrong_state_rejected": True,
                                     "callback_received": True, "provider_token_exchange_tested": False})


async def main(options):
    if options.mode == "offline":
        await offline()
        return
    if not options.url:
        raise SystemExit("Live modes require --url.")
    config = MCPStreamableHTTPConfig(server_id="authenticated-server", url=options.url)
    if options.mode == "bearer":
        token = os.environ.get("MCP_BEARER_TOKEN")
        if not token:
            raise SystemExit("Set MCP_BEARER_TOKEN before using Bearer mode.")
        async with MCPClient(config, auth=MCPBearerAuth(token=token)) as client:
            show("authenticated tools", [t.name for t in await client.list_tools()])
        return

    async def present(url):
        # The application explicitly selects browser-based interactive login.
        print("Opening the OAuth authorization page in the default browser.")
        if not await asyncio.to_thread(webbrowser.open, url):
            raise RuntimeError("No browser is available; supply an application authorization UI.")

    storage = MCPFileTokenStorage(options.token_file) if options.token_file else MCPMemoryTokenStorage()
    async with MCPOAuthCallbackServer(on_authorization_url=present) as callback:
        async with MCPClient(config, auth=oauth(callback, storage)) as client:
            show("OAuth authenticated tools", [t.name for t in await client.list_tools()])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("offline", "bearer", "oauth"), default="offline")
    parser.add_argument("--url")
    parser.add_argument("--token-file", type=Path, help="Optional credential file, separate from Workflow state")
    asyncio.run(main(parser.parse_args()))
