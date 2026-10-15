"""Optional MCP SDK clients, authentication resources and Tool adapters."""

from .auth import MCPBearerAuth, MCPMemoryTokenStorage, MCPOAuth
from .client import MCPClient, MCPClientError
from .config import (
    MCPSSEConfig, MCPStdioConfig, MCPStreamableHTTPConfig, MCPTransportConfig,
)
from .tools import MCPTool
from .callbacks import MCPCallbacks
from .oauth_callback import MCPOAuthCallbackServer
from .registry import MCPToolRegistry
from .storage import MCPFileTokenStorage
from .streaming import MCPToolStreamChunk
from .recovery import MCPReconnectPolicy, MCPSubscriptionUpdate, MCPChangeEvent
from .content import (
    MCPContentError, MCPResourceContent, MCPResourceResponse, MCPPromptResponse,
    mcp_content, mcp_resource_response, mcp_prompt_response,
)

__all__ = [
    "MCPReconnectPolicy", "MCPSubscriptionUpdate", "MCPChangeEvent",
    "MCPContentError", "MCPResourceContent", "MCPResourceResponse", "MCPPromptResponse",
    "mcp_content", "mcp_resource_response", "mcp_prompt_response",
    "MCPBearerAuth", "MCPClient", "MCPClientError", "MCPMemoryTokenStorage",
    "MCPOAuth", "MCPSSEConfig", "MCPStdioConfig", "MCPStreamableHTTPConfig",
    "MCPTool", "MCPTransportConfig",
    "MCPCallbacks", "MCPFileTokenStorage", "MCPOAuthCallbackServer",
    "MCPToolRegistry", "MCPToolStreamChunk",
]
