# Agent 模块使用指南

这组示例使用当前 V2 的公开接口，集中演示 LLM、Embedding、Tool、MCP、Skill、媒体和调用记录。
这些能力可以独立调用，也可以成为 Node，或者在自定义 Operator 内组合。
示例中的一次 Tool 往返由应用明确安排，没有实现自主 Agent 循环。

## 安装与运行

在仓库根目录运行，使用根虚拟环境：

```bash
uv pip install --python .venv/bin/python -e '.[agent,skills,mcp,host]'

# 基础示例，不导入 LiteLLM，不启动 HTTP 监听，不创建 Host 数据库
.venv/bin/python -m examples.agent_usage_guide.run_all

# HTTP/SSE/Bearer、本地 OAuth 回调、Host/SQLite 示例
.venv/bin/python -m examples.agent_usage_guide.run_all --http --recording

# 可选：安装 LiteLLM，并验证所有示例分支
uv pip install --python .venv/bin/python -e '.[agent,skills,mcp,host,litellm]'
.venv/bin/python -m examples.agent_usage_guide.run_all --litellm --http --recording
```

`agent` 提供 OpenAI SDK 与 JSON Schema 验证；`skills` 提供 YAML 解析；
`mcp` 提供 MCP SDK；`host` 提供 Host 与数据库依赖。LiteLLM 始终单独安装。
只运行一个示例时，可以仅安装它实际需要的 extra。

默认示例不需要模型密钥。LLM、Embedding、文件上传使用真实 OpenAI/LiteLLM SDK，
由 `support.py` 拦截所有 `httpx.AsyncClient.send` 并返回固定 HTTP 响应。
遇到非示例域名会直接报错。这能验证接入、编码、流处理和生命周期，不能验证真实模型效果。
MCP 使用真实 SDK，通过本地子进程或 `127.0.0.1` HTTP 服务通信。

统一入口为每条命令启动独立进程，执行断言并检查退出码；失败即停止。
使用 `--live` 或认证示例的非 `offline` 模式，才会访问你配置的外部服务。

## 示例索引

下面的模块名均位于 `examples.agent_usage_guide` 下，例如运行 LLM 示例：

```bash
.venv/bin/python -m examples.agent_usage_guide.llm
```

| 文件 | 内容 | 额外运行参数 |
|---|---|---|
| [llm.py](llm.py) | 两种协议、非流式/流式、结构化输出、429、一次 Tool 往返和 reasoning 回放 | 无 |
| [providers.py](providers.py) | OpenAI、DeepSeek 直连、LiteLLM，以及真实服务入口 | `--backend deepseek` 等 |
| [media.py](media.py) | 图片、音频、视频、PDF、Asset、上传缓存、查询和清理 | 无 |
| [embeddings.py](embeddings.py) | 四种输入、批量顺序、维度、base64 到浮点向量 | 可选 `--litellm` |
| [tools.py](tools.py) | 同步/异步 Tool、Schema、Context、Hooks、Policy、错误、媒体输出 | 无 |
| [permissions.py](permissions.py) | Tool 权限、审批、Core Wait/Resume、批准与拒绝 | 无 |
| [mcp.py](mcp.py) | stdio、远端 Tool、混合 ToolSet、进度、资源、模板、Prompt、订阅边界、重连 | 无 |
| [mcp_http.py](mcp_http.py) | 自动启动 HTTP/SSE Server，验证 Bearer 后调用 Tool | 无，需要本地端口 |
| [mcp_auth.py](mcp_auth.py) | OAuth 配置、存储与回调验证，以及外部 Bearer/OAuth 入口 | 默认离线；真实认证见下文 |
| [skills.py](skills.py) | 发现、目录、正文、分页、资源、Snapshot、权限收窄、脚本、模型侧 Tools | 无 |
| [nodes.py](nodes.py) | LLM/Embedding 标准 Capability、12 种 Node 用法、Operator 内组合、超时 | 无 |
| [capabilities.py](capabilities.py) | Host 默认注册契约、Provider 绑定、普通/流式共用 LLM Capability | 需要 `host` extra |
| [recording.py](recording.py) | 无数据库调用、自定义 Recorder、Host 自动记录 LLM 与 Embedding | 需要 `host` extra |
| [token_usage.py](token_usage.py) | 五种作用域 token/cost 查询、时间窗口、调用前限额检查 | 需要 `host` extra |

## LLM 的基本用法

```python
from autoagent.agent import (
    LLMMessage, LLMRequest, OpenAIProviderConfig,
    OpenAIResponsesProvider, TextContent,
)

request = LLMRequest(
    model="your-model",
    input=(LLMMessage(role="user", content=(TextContent(text="你好"),)),),
    max_output_tokens=256,
)

async with OpenAIResponsesProvider(OpenAIProviderConfig(api_key=key)) as provider:
    response = await provider.ainvoke(request)
    print(response.text)
    print(response.usage)
```

`LLMRequest` / `LLMResponse` 是 AutoAgent 的可序列化模型。
Provider 决定 API 协议和连接，`model` 决定本次调用的模型名称。
SDK 对象、密钥、服务连接不放入请求模型或 Runtime State。

同一份普通文本请求可以交给 `OpenAIResponsesProvider` 或
`OpenAIChatCompletionsProvider`。协议专属能力通过
`LLMProviderOptions(protocol=..., provider_id=..., values=...)` 显式传递。
`provider_id` 是应用定义的连接身份；两个账号或两条连接可以使用不同 ID。
专属 options 不能覆盖 `model`、`input`、密钥、SDK client、重试等受管理字段。

`response.text` 只读取可见文本。Tool Call、媒体、reasoning 和协议回放数据位于
`response.output`，使用下一轮请求时应保留原始顺序和 `native` 信息。
这尤其影响 DeepSeek thinking + Tool 历史，以及带签名或加密内容的历史回放。

### 流式输出

```python
from contextlib import aclosing

async with aclosing(provider.astream(request)) as stream:
    async for chunk in stream:
        if chunk.type == "delta" and chunk.kind == "text":
            print(chunk.delta, end="", flush=True)
        elif chunk.type == "completed":
            response = chunk.response
```

`delta` 用于增量展示，`item` 是完整输出项，`completed.response` 是权威的完整结果。
需要读取完整 usage 时，应消费到 `completed`。提前退出时使用 `aclosing` 关闭生成器。
不要把 delta 文本拼接后当作完整的多模态、reasoning 或 Tool 结果。

### 结构化输出与异常

使用 `LLMJsonSchemaFormat.from_type(MyModel)` 设置
`LLMOutputOptions.text_format`，然后调用 `response.parse(MyModel)` 得到 Pydantic 对象。
声明格式会触发 Provider 的本地验证，`parse` 还会验证调用方指定的 Python 类型。
拒答、截断、无文本、非法 JSON、Schema 不符都会有明确错误，不会自动修复或额外发起请求。

`LLMProviderError` 提供连接身份、协议、`status_code`、`request_id` 和 `retryable` 等信息。
是否重试、何时重试由应用/Core 策略决定；`retryable=True` 本身不会执行重试。
应用注入自己的 SDK client 时，该 client 的内部重试配置仍由应用负责。

## 接入真实 Provider

以下命令会发送真实请求。先在运行环境中设置相应凭证，再执行。
模型名称必须是目标服务实际支持的名称，示例不会自动选择、切换或回退模型。

```bash
# OPENAI_API_KEY；可选 OPENAI_BASE_URL
.venv/bin/python -m examples.agent_usage_guide.providers \
  --live --backend openai --protocol responses --model YOUR_MODEL --stream

# DEEPSEEK_API_KEY；可选 DEEPSEEK_BASE_URL
.venv/bin/python -m examples.agent_usage_guide.providers \
  --live --backend deepseek --protocol chat --model YOUR_DEEPSEEK_MODEL

# LiteLLM 自己支持的路由与对应凭证，例如 DEEPSEEK_API_KEY
.venv/bin/python -m examples.agent_usage_guide.providers \
  --live --backend litellm --prefix deepseek --protocol chat --model YOUR_DEEPSEEK_MODEL

# OpenAI Embedding；LiteLLM Embedding 可将 backend/prefix/model 改为目标配置
.venv/bin/python -m examples.agent_usage_guide.providers \
  --live --backend openai --embedding --model YOUR_EMBEDDING_MODEL
```

LiteLLM 只使用一层通用转换：`LLMRequest → LiteLLM → LLMResponse`。
应用不需要为 LiteLLM 的每家上游额外写转换器。上游是否支持某个媒体、参数或协议，
仍取决于上游服务及 LiteLLM 的对应适配。

`--prefix deepseek --model model-name` 会组合成 LiteLLM 的 `deepseek/model-name`。
也可以省略 `--prefix`，直接给 `--model vendor/model-name`。
示例还支持 `LLM_API_KEY`、`LLM_BASE_URL`，用于显式配置 LiteLLM 连接。

当目标是原生 OpenAI 兼容 Responses 端点时，可使用 `--prefix openai` 加
`LLM_BASE_URL` 指定目标服务。不同 LiteLLM 路由可能把 Responses 桥接到 Chat，
并非选择 `aresponses` 就一定请求上游的 `/responses`；应按所需语义选择路由。
DeepSeek 直连示例的真实模式显式关闭 thinking，方便最小文本调用。

当前验证环境的 `litellm==1.105.1` 存在 Responses 流式收尾限制：其迭代器没有
`close` / `aclose`，当前 AutoAgent Provider 关闭流时会抛出 `AttributeError`。
因此本组验证 LiteLLM 的 Chat 流式和 Responses 非流式；后者输出 `stream_verified=false`。
真实入口也会明确拒绝 `--backend litellm --protocol responses --stream`。
这项缺陷没有在示例中通过 monkeypatch 掩盖，也没有修改 SDK 源码。

## 多模态、Asset 和 Files

内容使用有序的 `TextContent`、`ImageContent`、`AudioContent`、`VideoContent`、
`FileContent` 表达；媒体来源有四种：

| Source | 适用场景 | 应用负责的部分 |
|---|---|---|
| `URLSource` | HTTP(S) 图片、视频或文件地址 | URL 有效性与上游可访问性 |
| `DataSource` | 已读到内存的媒体 | base64 字节与 MIME，data 不含 data URL 前缀 |
| `AssetSource` | 应用管理的持久化媒体 | Store 可用性和恢复后的资源解析 |
| `ProviderSource` | 已上传的 provider 文件 | 连接身份匹配、文件有效期和清理 |

`LocalMediaStore` 保存 Asset 字节和清单，重开 Store 后仍可解析。
`AssetMediaResolver(store)` 转换成 inline data；加 `files=OpenAIFileClient(...)`
则显式上传，得到 `ProviderSource`。将 resolver 传入 Provider 的 `media_resolver`，
Provider 在编码前解析请求中的 Asset。

`OpenAIFileClient` 支持上传缓存、retrieve、invalidate、delete 和 `acleanup`。
关闭客户端不会删除文件，示例通过 `finally: await files.acleanup()` 明确清理上传。
上传和推理必须使用相同的连接身份和兼容的上游文件服务。

模型能表达视频，不代表每个 API 都接受视频。示例实际验证了基础 Chat adapter
拒绝视频、LiteLLM adapter 编码 `video_url`；没有调用真实视频模型。
示例的音频/PDF/图片占位字节用于传输演示，不能用于真实多模态质量验证。

## Embedding

`EmbeddingRequest.input` 接受：一个字符串、字符串 tuple、一个 token-ID tuple、
token-ID tuple 的 tuple。token IDs 必须与所选模型的 tokenizer 一致。
`dimensions` 是否支持由上游决定。

无论 wire format 是 `float` 还是 `base64`，`EmbeddingResponse.vectors` 都返回浮点向量 tuple。
Provider 会按原始输入 index 排序并验证批次、索引和向量维度。
Embedding 不产生流式 delta；Token usage 只有上游报告时才存在。

OpenAI 使用 `OpenAIEmbeddingProvider`，LiteLLM 使用
`LiteLLMEmbeddingProvider`。额外 API 字段使用 `EmbeddingProviderOptions`。
DeepSeek 直连适配没有虚构一个 Embedding API。

## Tool 的定义和执行

`@tool` 从函数参数、返回值注解和 docstring 生成定义，同时保留普通 Python 调用方式。
`Tool(function)` 提供单个函数的校验和执行；`ToolSet` 管理多个本地或 MCP Tool。

```python
@tool
def add(a: int, b: int) -> dict[str, int]:
    """Add two integers."""
    return {"sum": a + b}

tools = ToolSet((add,))
request = LLMRequest(model=model, input=messages, tools=tools.definitions)
first = await provider.ainvoke(request)
observations = tuple([await tools.ainvoke(call) for call in first.tool_calls])
followup = LLMRequest(
    model=model, tools=tools.definitions,
    input=(*request.input, *first.output, *observations),
)
second = await provider.ainvoke(followup)
```

LLM 返回的 Tool Call 不会自动执行。`call_id` 关联调用和结果，不是业务幂等键。
有写入副作用的 Tool，应由应用定义幂等和重试语义。

`ToolContext` 注入服务、用户和允许的 Tool，Context 参数不会出现在给模型的 Schema 中。
可以使用 `context.restrict(...)` 收窄权限。`ToolSet.select(...)` 创建独立的能力快照。
`ToolHooks` 提供 before、after、on_error；`policy(call, context)` 返回
`ToolDecision(action="allow" | "deny" | "ask", reason=...)`。
策略读取 `context.arguments` 中校验、补齐默认值后的只读参数；不通过异常表达普通权限拒绝。
Hooks 用于观察，不通过返回值替换调用或结果。

```python
from autoagent.agent import ToolDecision

def policy(call, context):
    if context.user_id != "alice":
        return ToolDecision(action="deny", reason="User cannot publish")
    return ToolDecision(action="ask", reason="Confirm publication")

tools = ToolSet((publish,), policy=policy, context=ToolContext(user_id="alice"))
decision = await tools.acheck(call)  # 同步策略也可使用 tools.check(call)
if decision.action == "ask":
    request = decision.approval_request
    # 应用通过 Wait(ToolApprovalRequest, ToolApproval) 等待认证客户端响应。
    # 下行仅模拟客户端批准；不能从 LLM 的 Tool 参数获得 approval。
    approval = request.respond(approved=True)
    result = await tools.ainvoke(call, approval=approval)
```

执行前重新检查当前策略，审批绑定参数、调用 ID、用户/会话、Tool Schema；
在 Core 内还绑定 Workflow Revision 和 Invocation。拒绝返回错误 ToolResult；
缺少必需审批抛出 `ToolApprovalRequired`，其 `request` 可序列化。
审批仅是应用输入，不是密码学凭证；客户端认证、业务幂等由应用负责。
`tools[name]`、`select(...)` 和组合 ToolSet 均保留原有权限。
完整 Workflow 路由、Wait 和执行代码见 [permissions.py](permissions.py)。

错误分为两类：

- 参数错误、未知 Tool、权限拒绝、`ToolBusinessError`：返回 `is_error=True` 的 `LLMToolResult`。
- 意外代码异常、返回值违反类型约束：抛出 `ToolExecutionError` / `ToolOutputError`，让应用或 Core 处理。

`ToolOutput` 可同时返回结构化数据、文本或媒体。同步 `Tool.invoke` 只适用于同步 Tool；
异步/远端 Tool 使用 `ainvoke`。在 async Operator 中调用同步 Tool 的 `ainvoke`，
会使用 Core 的工作线程池，不会在 Runtime 线程中直接执行阻塞函数。

## MCP：三种传输、发现和认证

`MCPStdioConfig` 启动子进程，通过 stdin/stdout 交换 MCP 数据；子进程自己的凭证使用 `env`。
`MCPStreamableHTTPConfig` 和 `MCPSSEConfig` 使用 HTTP，支持 headers、Bearer 或 OAuth。
SSE 示例明确使用 legacy 协议模式；`mode` 是 MCP 协议协商配置，与传输方式分别配置。

```python
async with MCPClient(
    MCPStreamableHTTPConfig(server_id="documents", url="https://your-server.example/mcp"),
    auth=MCPBearerAuth(token=token),
) as client:
    tools = await client.toolset(prefix="docs_")
    definitions = tools.definitions
```

发现后可以重命名、加前缀、include/exclude，再与本地 Tool 放入同一个 `ToolSet`。
Tool 调用复用 `LLMToolCall` / `LLMToolResult`；资源和 Prompt 可转换为可直接放入
`LLMRequest.input` 的内容。转换不会自动下载 MCP 返回的链接资源。

`MCPTool.astream` 给出进度和最终结果；进度本身不代表远端副作用已经提交。
`MCPToolRegistry` 保存完整 Tool 快照，refresh 后原子替换。
同一个 LLM turn 应捕获同一 snapshot，用它提供 definitions 并执行后续 Tool Call。

`areconnect()` 重新协商并恢复 Registry。之前连接的 Tool 会被标记为 stale，必须重新发现。
重连不会重放已经发送的业务 Tool Call。订阅恢复后的 connected 更新要求应用重新同步状态，
不能把它当成离线通知的自动补发。

### Bearer 和 OAuth

`mcp_http.py` 实际验证了未认证请求得到 401，以及 Bearer 认证后的 HTTP/SSE Tool 调用。
Server 的静态 Token 校验只用于本地教学。

OAuth 使用 `MCPOAuth`，委托 MCP SDK 处理发现、注册、PKCE、state/issuer 验证和刷新。
`MCPOAuthCallbackServer` 接收 loopback 回调，应用决定如何展示授权链接。
`MCPMemoryTokenStorage` 只保存在内存；`MCPFileTokenStorage` 可跨进程重启恢复，
凭证文件与 Workflow/Runtime State 分开保存。

```bash
# 读取 MCP_BEARER_TOKEN；连接真实服务
.venv/bin/python -m examples.agent_usage_guide.mcp_auth \
  --mode bearer --url https://YOUR_SERVER/mcp

# 连接真实 OAuth 服务并打开浏览器；token-file 可省略，省略后仅存内存
.venv/bin/python -m examples.agent_usage_guide.mcp_auth \
  --mode oauth --url https://YOUR_SERVER/mcp --token-file /YOUR_PRIVATE_DIRECTORY/mcp-tokens.json
```

默认 OAuth 示例只验证配置、真实 SDK handler 构造、Token 文件和本地回调，
包括拒绝错误 state；没有模拟成已完成真实 OAuth 服务的授权、Token 交换和刷新。
需要服务端接受应用的客户端注册方式和 loopback redirect URI，才能使用真实入口完成登录。

## Skill：从发现到执行

本目录附带可直接运行的 `skills/stock-guide/`：

```text
stock-guide/
├── SKILL.md
├── references/rules.md
├── assets/template.md
└── scripts/total.py
```

`SkillSet.discover([root])` 扫描根目录下的 Skill 子目录。
`catalog_message()` 只暴露目录描述；`load(name)` 加载正文；
`list_resources`、`read_resource`、`read_text` 按需读取资源，支持分页或按行继续读取。
`errors="skip"` 可保留可用 Skill，并在 `diagnostics` 中查看跳过的错误。

Skill 有两种激活方式：

- 应用选择：加载文档，通过 `document.to_message()` 放入输入，再显式读取所需资源。
- 模型选择：放入 `catalog_message()` 并暴露 `skills.as_tools()`，让模型请求加载和读取。

`disable-model-invocation: true` 让 Skill 不出现在模型目录和 Skill Tools 中，
应用仍可以通过直接方法或 `SkillOperations` 读取。

`allowed-tools` 不会自动授权或执行。应用调用 `document.restrict_tools(context)`
将其与现有权限取交集，再用该 Context 执行 Tool。
本例中的 `stock-guide` 只允许 `stock`；它不会因为写了某个命令就获得 OS 权限。

`snapshot()` 生成带资源字节、清单和内容摘要的 `SkillSnapshot`，可 JSON 序列化后
`to_skill()` 恢复；`freeze()` 可冻结整个 SkillSet。这样运行期间的正文和资源来自同一版本。
应用负责保存、恢复所需 snapshot，模块不会自动将整个文件树写入数据库。

脚本执行必须显式创建 `SubprocessSkillRunner`，通过 `SkillScriptSpec` 绑定
Skill 名称、脚本路径、解释器和需要暂存的资源，然后交给 `SkillOperations` 或 `as_tools(runner=...)`。
只读取脚本不会执行。Runner 提供超时和进程清理，但执行权限仍是应用的 OS 权限；它不是沙箱。

## 作为 Node，或嵌入 Operator

`nodes.py` 将这些能力分别变成单 Node Workflow，便于观察各自输入和输出：

| 能力 | Node executable |
|---|---|
| LLM | `LLM_CALL` 默认聚合为 `LLMResponse`，无需配置 Stream；普通实现只产生完成 chunk |
| 流式 LLM | 同一个 `LLM_CALL` 和 reducer；流式实现额外产生 delta/item |
| Embedding | `EMBEDDING` |
| ToolSet | `tools.as_operator()` |
| MCP Tool Registry | `registry.as_operator()` |
| 单个流式 MCP Tool | `mcp_tool.as_operator(streaming=True)`，加 `Stream(MCPToolStreamReducer())` |
| MCP Resource / Prompt | `MCPOperations(client).as_operator("read_resource" / "get_prompt")` |
| Skill | `SkillOperations(skills).as_operator("load" / "read_text" / "run_script" 等)` |

`LLM_CALL` 的 ID 是 `llm.call`，`EMBEDDING` 的 ID 是 `embedding.call`。
Host 启动时自动向 Core App 注册这两个契约；Provider 实现仍在 lifespan 中绑定：

```python
from contextlib import asynccontextmanager
from autoagent.agent import (
    LLM_CALL, EMBEDDING, OpenAIResponsesProvider, OpenAIEmbeddingProvider,
    llm_call_operator, embedding_operator,
)

@asynccontextmanager
async def resources(setup):
    async with OpenAIResponsesProvider() as llm, OpenAIEmbeddingProvider() as embeddings:
        setup.register_operator(
            llm_call_operator(llm),
            capability_id=LLM_CALL.id, default=True,
        )
        setup.register_operator(
            embedding_operator(embeddings),
            capability_id=EMBEDDING.id, default=True,
        )
        yield
```

将 `resources` 传给 `AutoAgentHost(lifespan=resources, ...)`。
这个片段使用 OpenAI 配置；本地离线完整版本见 `capabilities.py`。
每次请求的 `LLMRequest.streaming=False`（默认）调用 Provider 的 `ainvoke`，
`True` 调用 `astream`；同一实现支持两种模式，由 Capability 的默认 Reducer 得到完整 `LLMResponse`。
各 Node 分别在请求中指定 `model`、`max_output_tokens`、`temperature` 和 `streaming`。
普通 `Node("call", LLM_CALL)` 即可使用，无需在 Node 上配置 `LLMStreamReducer`。
Host 还可以通过 `AUTOAGENT_LLM_*`、`AUTOAGENT_EMBEDDING_*` 创建和绑定默认 Provider；
LLM 连接可以只配置 API_KEY，将模型留给每次调用；MODEL 和 API_KEY 均为空时不自动创建连接。
`llm_request()` 为省略的参数补上环境默认值，显式参数优先。项目注册的自定义实现优先。

可通过 `llm_call_operator(resolver=...)` 注入同步或异步的 `LLMResolver`，接收 `LLMRequest`，
返回运行时对象 `LLMSelection(provider=..., model=...)`。未内置路由策略。
`model=None` 允许回调选择模型；显式模型不能被回调替换。`routing` 是不会发送给模型 API 的 JSON 提示。
Provider 的创建和关闭仍由应用负责，Provider 专用选项和历史必须与选中的目标兼容。

单独使用 Core App 时，用 `app.register_capability(...)` 注册契约，
再用 `app.register_operator(...)` 绑定实现。`nodes.py` 在同一个实现上使用不同请求的
`streaming` 设置，分别演示普通与流式调用，不需要 Resolver。

Provider 和 MCP client 使用 `AutoAgentApp(runtime_lifespan=...)` 创建和关闭，
确保资源与 Core 的 Runtime loop 一致。不要先在调用方 event loop 连接好 client，再交给 Core。
Skill 和 Tool 的普通定义可以在外部创建，活跃资源及服务仍由应用注入。

自定义 Operator 内可直接 `await skills.aload(...)`、`await tools.ainvoke(...)`、
`await provider.ainvoke(...)`，不必把每次底层调用都拆成 Node。
示例也验证了其中的同步 Tool 使用 Core 工作线程。

超时配置在 `Node(..., timeout=TimeoutPolicy(seconds))`。
计时从 Operator 执行准入后开始，覆盖执行和流消费，不包含排队、输入映射或输出绑定。
它不等于 SDK 的网络超时；同步 Python 线程
无法被强制杀死。MCP 和 SDK 仍可配置自己的网络超时。
本例没有添加字段或整体内容大小限制，也没有自动 Provider/模型切换。

Runtime 的 `InvocationResult.output` 是可移植记录。需要业务类型时，使用
`ValueContract.create(LLMResponse, location="output").restore(result.output)` 等方式恢复。
`app.ainvoke` 会消费并归约流。只有 Node 配置了 `user_events` Mapper 才会产生 User Event；
同一个 Mapper 接收 Chunk 和最终 Output，返回 `UserEventSpec` 或 `None`。
Reducer 与 User Event 相互独立；Host 会持久化显式产生的事件。

## 调用记录

普通 Provider 调用在没有 Recorder 或 Host 数据库时照常工作，不会创建记录表。
可以实现 `record_llm_call(record)` 自定义 Recorder，将每次调用的 running/terminal 状态
按 `llm_call_id` 更新。`llm_recording_context` 可为独立调用补充应用上下文。

通过 Host 执行时，自动附加真实 Workflow、revision、Node、Operator、Session、Invocation 等身份。
`recording.py` 在同一次 Invocation 的两个 Capability Node 中依次执行 Embedding 和 LLM，
随后检查临时 SQLite 的两行记录，验证它们具有相同 Workflow/revision/Invocation 身份、
不同的 Node 和 Operator Call 身份。入口 Node 有 Input Mapping 时，示例显式传入
`request.model_dump(mode="json")` 作为 Invocation 输入，再在映射中恢复类型。
自定义 Operator 内直接调用 Provider 的方式见 `nodes.py`。
`recording.py` 直接使用诊断 SQL；公开 Token 汇总查询见下文 `token_usage.py`。

记录包含模型、状态、耗时和上游报告的 Token usage，不包含 Prompt、生成正文、媒体或凭证。
未报告 usage 时保持未知，不估算为 0。未配置限额时，Provider 的 `record_calls=False` 可关闭该 Provider 的记录。

## Token 统计与限额

运行 `.venv/bin/python -m examples.agent_usage_guide.token_usage`，或使用
`run_all --recording`。示例使用模拟 HTTP，验证两次调用耗尽 Session 额度，第三次不发请求。

`await host.token_usage(...)` 支持以下过滤：

- `invocation_id, node_id`：一个 Node 在一次 Invocation 中的全部调用。
- `invocation_id`：该 Invocation 的直接调用，不隐式包含 Spawn 子执行。
- `session_id`：该 Session 的全部调用，跨 Workflow Revision。
- `workflow_id`：该 Workflow ID 的全部 Revision。
- 无身份过滤：当前项目数据库中的全部调用。

可额外传入 `kind="generation" / "embedding"`、`provider_id`、`requested_model`、
`workflow_revision_id`、`start` 和 `end`。数据库是项目边界，不支持在一个数据库中混放多个项目。
时间按调用开始时间查询 `[start, end)`，必须带时区；省略边界表示不限制该边。
`TimeRange.day/month(timezone="Asia/Hong_Kong")` 生成自然日/月边界，
`TimeRange.last_24_hours()` 生成最近 24 小时。UTC 保存时间；跨午夜调用归入开始时的窗口。
历史窗口可能随着未结束调用结算而补齐，不是冻结报表。

汇总返回 token 已知部分、`pending_calls`、`unsettled_calls`、`missing_usage_calls` 和
各字段的 `missing_tokens`。全部缺失的指标为 `None`；无调用为 0。
Reasoning/cache token 是明细，不能再加到 total。失败和取消只要收到 usage 也累计。
统计仅覆盖应用记录到的调用及上游报告值，不估算缺失 usage。

限额示例见 `token_usage.py`：Node 使用 `ResourcePolicy(usage=UsagePolicy(...))`；
Workflow 使用 `WorkflowResourcePolicy(invocation=..., session=...)`；
Project 和跨 Revision 的 Workflow 周期限额通过 `AutoAgentHost(usage_policy=ProjectUsagePolicy(...))` 配置，
其 `WindowUsagePolicy` 可使用 `UsageWindow("day" / "month", timezone)` 或明确的 `TimeRange`。
`UsagePolicy` 支持 `max_input_tokens`、`max_output_tokens`、`max_total_tokens`，省略表示不限，0 表示禁止调用。
这里的输出额度是累计值，`LLMRequest.max_output_tokens` 仍是单次输出上限。

每次 Provider 请求前在同一数据库事务内检查全部适用额度并登记调用；耗尽抛出
`ResourceBudgetExceeded`，默认不触发 Core 重试。已经发出的并发请求可能导致超额。
查询/准入失败阻止发送；未知用量默认阻止后续受限调用，可显式设置 `on_unknown="allow"`。
结算写入失败保留已获得的响应，不重新发送请求，并保留未结算状态；重启后也不会把旧 running 记录当成零。
配置限额后 `record_calls=False` 不能绕过必要的计量。
Standalone Core 可在 Runtime lifespan 中使用 `runtime_llm_recorder(database_sink)`；
限额要求提供 `LLMUsageLedger`，普通观测 Recorder 无法承担准入职责。

## Cost 统计与限额

Cost 复用 `llm_calls`：终态入库时计算，保存金额、价格版本及计费明细。
默认读取已安装 LiteLLM 包内附带的模型价格 JSON，不导入 SDK、不联网更新价格。
常见模型已有价格；未安装 LiteLLM、模型未收录或自定义收费时可覆盖本地价格：

```python
from autoagent.agent import LocalTokenPricing, TokenPrice

pricing = LocalTokenPricing(overrides={
    "openai/my-model": TokenPrice("1", "3"),  # 输入/输出：美元 / 百万 token
    "lm_studio/google/gemma-4-e4b": TokenPrice("0", "0"),
})
# AutoAgentHost(..., pricing=pricing)
# WorkflowResourcePolicy(session=UsagePolicy(max_cost_usd="1.50"))
```

`pricing.catalog_info` 返回本地价格表版本及条目数。连接的 Provider ID 与计价厂商不一致时，
可在 Provider config 上设置 `pricing_provider="openai"`；它不进入 API 请求。
`LocalTokenPricing(catalog_path="/path/prices.json")` 可替换整个本地价格表；
高级计费可注入实现 `CostCalculator.calculate(record) -> CostEstimate` 的计算器。

`host.token_usage(...)` 同时返回 `cost_usd` 和 `missing_cost_calls`，沿用全部作用域和时间过滤。
`UsagePolicy(max_cost_usd="...")` 沿用 Token 的所有限额位置、未知值策略和并发语义。
每次准入使用一次条件聚合查询检查全部适用限额，随后一次插入；首次建表及事务控制不计入这两条业务 SQL。

金额使用美元十进制字符串；数据库保存整数，每单位为 `1e-12 USD`，单次费用向上舍入一次。
历史记录固定使用结算时的价格，不随价格表升级重算。它是本地价格估算，不代表厂商账单。
当前支持文本/Embedding 的线性 Token 价格，以及已提供充分明细的缓存、Reasoning、服务等级和长上下文价格。
多模态、内置收费工具、未支持的缓存时长价格等保留为未知；未知价格不会记成零，默认阻止后续受 cost 限额的调用。
本地免费模型需显式填写零价格；调用尚未结束和结算失败仍按未结算处理。
本次增加了 `llm_calls` 字段；已有旧表的开发数据库需要重建该表，不提供旧结构迁移。
