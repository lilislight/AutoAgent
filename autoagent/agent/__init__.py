"""Agent capabilities built alongside the Core Runtime.

Importing this module does not require a provider SDK or create clients,
connections or Runtime resources. Providers load the optional SDK lazily.
"""

from .models import (
    EmbeddingProviderOptions,
    EmbeddingInput, EmbeddingRequest, EmbeddingResponse, EmbeddingUsage,
    EmbeddingVector,
    AssetSource, AudioContent, DataSource, FileContent, ImageContent,
    LLMAudioOutput, LLMContent, LLMFinishReason, LLMItem, LLMJsonFormat,
    LLMJsonSchemaFormat, LLMMediaContent, LLMMessage, LLMNamedToolChoice,
    LLMNativeData, LLMOutputItem, LLMOutputOptions, LLMProviderItem,
    LLMProviderOptions, LLMReasoning, LLMRequest, LLMResponse, LLMSource,
    LLMTextFormat, LLMTextOutputFormat, LLMToolCall, LLMToolChoice,
    LLMToolDefinition, LLMToolResult, LLMUsage, LLMVoiceReference,
    ProviderSource, RefusalContent, TextContent, URLSource, VideoContent,
)
from .models import LLMStreamChunk
from .models import ToolOutput
from .models import LLMStructuredOutputError, parse_response, validate_response
from .models import __all__ as _model_exports
from .providers import (
    DeepSeekProviderConfig, DeepSeekProvider, DeepSeekChatCompletionsAdapter, DeepSeekResponsesAdapter,
    LiteLLMProviderConfig, LiteLLMProvider, LiteLLMEmbeddingProvider, LiteLLMChatCompletionsAdapter,
    OpenAIFileClient,
    EmbeddingProvider, EmbeddingProviderError, OpenAIEmbeddingAdapter,
    OpenAIEmbeddingProvider, ProviderError,
    ChatCompletionsAdapter, LLMModelProfile, LLMProvider, LLMProviderError,
    LLMUnsupportedRequest, MediaResolver, OpenAIChatCompletionsProvider,
    OpenAIProtocolAdapter, OpenAIProvider, OpenAIProviderConfig,
    OpenAIResponsesProvider, ResponsesAdapter,
)
from .providers import __all__ as _provider_exports
from .tools import (
    ToolContext, ToolHooks,
    ExecutableTool, Tool, ToolBusinessError, ToolDefinition, ToolError,
    ToolExecutionError, ToolOutputError, ToolSet, get_tool_definition, tool,
)
from .tools import __all__ as _tool_exports
from .mcp import (
    MCPReadResourceRequest, MCPGetPromptRequest, MCPOperations,
    MCPReconnectPolicy, MCPSubscriptionUpdate, MCPChangeEvent,
    MCPContentError, MCPResourceContent, MCPResourceResponse, MCPPromptResponse,
    mcp_content, mcp_resource_response, mcp_prompt_response,
    MCPBearerAuth, MCPClient, MCPClientError, MCPMemoryTokenStorage, MCPOAuth,
    MCPSSEConfig, MCPStdioConfig, MCPStreamableHTTPConfig, MCPTool, MCPTransportConfig,
    MCPCallbacks, MCPFileTokenStorage, MCPOAuthCallbackServer, MCPToolRegistry, MCPToolStreamChunk,
)
from .mcp import __all__ as _mcp_exports
from .integration import LLMStreamReducer, LLMStreamState, tool_operator
from .integration import MCPToolStreamReducer, MCPToolStreamState
from .skills import (
    SkillLoadRequest, SkillListResourcesRequest, SkillReadResourceRequest, SkillReadTextRequest, SkillRunScriptRequest, SkillOperations,
    SkillSnapshotFile, SkillSnapshot, SnapshotSkillSource,
    Skill, SkillSet, SkillMetadata, SkillLimits, SkillResource,
    SkillDocument, SkillResourceContent, SkillError, SkillFormatError,
    SkillNotFoundError, SkillResourceError, SkillLimitError, SkillChangedError,
    SkillAccessError, SkillScriptError, SkillResourcePage, SkillTextContent,
    SkillDiagnostic, SkillScriptSpec, SkillScriptResult, SkillSource,
    LocalSkillSource, MemorySkillSource, PackageSkillSource, SkillScriptRunner,
    SubprocessSkillRunner,
)
from .skills import __all__ as _skill_exports
from .media import MediaError, MediaAsset, MemoryMediaStore, LocalMediaStore, media_content, AssetMediaResolver
from .media import __all__ as _media_exports
from .recording import LLMCallContext, LLMCallRecord, LLMCallRecorder, llm_recording_context, runtime_llm_recorder

__all__ = [
    "LLMCallContext", "LLMCallRecord", "LLMCallRecorder", "llm_recording_context", "runtime_llm_recorder",
    *_media_exports,
    *_model_exports, *_provider_exports, *_tool_exports, *_mcp_exports,
    "LLMStreamReducer", "LLMStreamState", "tool_operator",
    "MCPToolStreamReducer", "MCPToolStreamState",
    *_skill_exports,
]
