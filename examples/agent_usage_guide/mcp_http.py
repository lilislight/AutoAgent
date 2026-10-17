"""Self-contained Streamable HTTP + legacy SSE with actual Bearer verification.

Run: .venv/bin/python -m examples.agent_usage_guide.mcp_http
Starts loopback-only MCP servers and terminates them after the checks.
"""

import asyncio
from contextlib import asynccontextmanager
import os
from pathlib import Path
import socket
import sys

import httpx

from autoagent.agent import MCPBearerAuth, MCPClient, MCPSSEConfig, MCPStreamableHTTPConfig
from .support import show


@asynccontextmanager
async def local_http_server(transport):
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    token = "local-example-token"
    process = await asyncio.create_subprocess_exec(sys.executable, "-m", "examples.agent_usage_guide.mcp_server",
        "--transport", transport, "--port", str(port), cwd=str(Path(__file__).resolve().parents[2]),
        env={**os.environ, "GUIDE_MCP_BEARER_TOKEN": token},
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
    try:
        async with httpx.AsyncClient(timeout=.5, trust_env=False) as http:
            async with asyncio.timeout(15):
                while True:
                    if process.returncode is not None:
                        raise RuntimeError((await process.stderr.read()).decode())
                    try:
                        ready = await http.get(base + "/health")
                        assert ready.status_code == 401
                        break
                    except httpx.TransportError:
                        await asyncio.sleep(.05)
        yield base, token
    finally:
        if process.returncode is None:
            process.terminate()
        try:
            await asyncio.wait_for(process.wait(), 5)
        except TimeoutError:
            process.kill()
            await process.wait()


async def main():
    for transport, config_type, path in (
        ("streamable-http", MCPStreamableHTTPConfig, "/mcp"),
        ("sse", MCPSSEConfig, "/sse"),
    ):
        async with local_http_server(transport) as (base, token):
            config = config_type(server_id=transport, url=base + path,
                mode="legacy" if transport == "sse" else "auto", request_timeout_seconds=10)
            async with MCPClient(config, auth=MCPBearerAuth(token=token)) as client:
                result = await client.call_tool("add", {"a": 2, "b": 3})
                assert result.structured_content == {"sum": 5}
                assert (await client.read_resource_content("memory://guide")).content
                show(transport, {"protocol": client.protocol_version, "sum": result.structured_content["sum"],
                                 "unauthenticated_request": 401})


if __name__ == "__main__":
    asyncio.run(main())
