"""Local SDK MCP Server for the guide's stdio, HTTP and SSE examples.

Run directly only when testing a separately configured client:
  .venv/bin/python -m examples.agent_usage_guide.mcp_server --transport streamable-http --port 8765
"""

import argparse
import asyncio
import os

from mcp.server import MCPServer
from mcp.server.mcpserver import Context


server = MCPServer("autoagent-usage-guide", instructions="Local deterministic example server.", log_level="WARNING")


@server.tool()
async def add(a: int, b: int, ctx: Context) -> dict[str, int]:
    """Add two integers and report progress."""
    await ctx.report_progress(1, 2, "inputs received")
    await asyncio.sleep(.01)
    await ctx.report_progress(2, 2, "calculation finished")
    return {"sum": a + b}


@server.resource("memory://guide")
def guide() -> str:
    return "Read the resource before generating a report."


@server.resource("memory://items/{name}")
def item(name: str) -> str:
    return f"Example item: {name}"


@server.prompt()
def summarize(topic: str) -> str:
    return f"Summarize {topic} using the supplied sources."


class BearerApplication:
    """Example-only ASGI check for a local, static test credential."""

    def __init__(self, app, token):
        self.app, self.expected = app, f"Bearer {token}".encode()

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and dict(scope["headers"]).get(b"authorization") != self.expected:
            await send({"type": "http.response.start", "status": 401, "headers": [(b"www-authenticate", b"Bearer")]})
            await send({"type": "http.response.body", "body": b"Unauthorized"})
            return
        await self.app(scope, receive, send)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transport", choices=("stdio", "streamable-http", "sse"), default="stdio")
    parser.add_argument("--port", type=int, default=8765)
    options = parser.parse_args()
    if options.transport == "stdio":
        server.run()
    else:
        import uvicorn
        app = server.sse_app() if options.transport == "sse" else server.streamable_http_app()
        token = os.environ.get("GUIDE_MCP_BEARER_TOKEN")
        if token:
            app = BearerApplication(app, token)
        uvicorn.run(app, host="127.0.0.1", port=options.port, log_level="warning")
