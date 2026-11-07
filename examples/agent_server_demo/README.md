# Agent Server Demo

使用真实 LLM Provider，通过现有 Server 和 UI 测试通用 Agent。包含：

- `agent` 主 Workflow 和 `researcher` 子 Workflow。
- `ask_user` 人机交互、普通 Tool 审批、子 Agent 创建及消息通信。
- 两个相同 contract、不同名称的 Signal Endpoints：`user.messages`、`agent.messages`。
- 自动同步的 `demo-guide` Skill；默认 8192 Token Context window 和压缩策略。
- 独立的 SQLite 数据库 `.autoagent/runtime.db`；获批笔记写入 `.autoagent/notes/`。

## 启动

在仓库根目录执行；首次运行前复制本目录 `.env.example` 为 `.env` 并填写 LLM 连接配置。
已构建 UI 时只需一个进程；UI 与 API 使用同一个端口。

```bash
npm --prefix ui run build
.venv/bin/python -m autoagent.server --project examples/agent_server_demo
```

打开 <http://127.0.0.1:8765/>，进入 **Executions → New Session and Run**，
Workflow 选择 `agent`，入口为 `start`。Session ID 可以留空，系统会自动创建。
输入只有一个 `message` 字段，例如：

```json
{"message":"请比较北京和上海的模拟天气。"}
```

不需要预先创建数据库或执行 SQL；Server 首次启动会创建并初始化它。
`.env` 的数据库地址应保留为此示例专用地址，避免与其他运行中的 Host 共用。

## 建议测试

| 功能 | message 示例 |
| --- | --- |
| 普通 Tools | 请计算 12.5 + 37.5，再查北京的模拟天气。 |
| 人机交互 | 请用 ask_user 问我想查询哪个城市，收到回答后再查天气。 |
| 审批 | 请调用 save_demo_note 保存标题为“测试”的笔记，内容为“审批后才保存”。 |
| 并行子任务 | 请 spawn 两个 researcher，分别研究北京和上海的模拟天气，收集结果后比较。 |
| 子任务通信 | 请创建 researcher，要求它先用 wait_for 等待消息；然后 send_message 调整目标并明确要求回复，最后收集结果。 |
| 等待与 Signal | 请用 wait_for 等待用户消息，最多 60 秒，然后根据消息继续。 |
| Skill | 请读取 demo-guide Skill，介绍如何测试审批与子任务。 |

模型是否遵循这些请求取决于模型的 Tool calling 能力；实际行为可以在图和 Trace 中查看。
`run_agent` 遇到子任务等待、消息或超时可能提前返回，不保证子任务已完成。
`spawn_agent` 创建的子任务仍归父任务管理，父任务结束后可能进入 settling 等待子任务结束。

等待时，在 UI 的执行操作中选择 Respond，按 Wait 的响应 Schema 填写。
`ask_user` 的文本回答格式示例：

```json
{"answers":[{"question_id":"以实际请求中的问题 ID 为准","status":"answered","selected_option_ids":[],"text":"北京"}]}
```

Tool 审批响应使用实际审批请求生成的 ID 和 digest，不能填写 LLM 的 call_id 代替。
UI 提供响应 Schema；完整请求在 `wait_approval` 的 Wait/Trace 中可见。

向等待中的主 Agent 发消息，在 UI 执行操作中选择 Signal，Endpoint 为 `user.messages`，
同一个 source_id 的 sequence 从 1 开始递增。Payload 可以按界面 Schema 填写，例如：

```json
{"message":"请改为查询深圳天气。","reply_requested":false,"reply_to":null,"wait_timeout_seconds":null}
```

等待只检查消息是否到达；`prepare_context` 随后读取并加入消息。已完成的 Invocation 不能接收 Signal。
继续聊天请在同一个 Session 下新建 Invocation，历史会保留。

模型、stream 和输出 Token 默认值来自 `.env`；审批与 Tool 规则在 `agent_demo/project.py` 中配置。
子任务默认继承创建它时父 Agent 的模型和权限设置。
