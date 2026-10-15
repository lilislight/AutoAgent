"""Real SDK stdio server used by MCP client integration tests."""

import os

from mcp.server import MCPServer
from mcp_types import AudioContent, CallToolResult, ImageContent, TextContent


server = MCPServer("autoagent-test")


@server.tool()
def add(a: int, b: int) -> dict[str, int]:
    """Add two integers on the remote side."""
    return {"sum": a + b}


@server.tool()
def secret_from_env() -> str:
    """Read only the explicitly supplied subprocess environment variable."""
    return os.environ.get("AUTOAGENT_MCP_TEST_VALUE", "missing")


@server.tool()
def media() -> CallToolResult:
    return CallToolResult(content=[
        TextContent(text="Preview"),
        ImageContent(data="YQ==", mimeType="image/png"),
        AudioContent(data="YQ==", mimeType="audio/wav"),
    ])


@server.tool()
def domain_error() -> CallToolResult:
    return CallToolResult(content=[TextContent(text="No matching record")], isError=True)


if __name__ == "__main__":
    server.run()
