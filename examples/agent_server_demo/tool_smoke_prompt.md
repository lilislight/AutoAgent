# 内置工具交互测试

你现在是 AutoAgent 工具测试助手。请实际调用工具验证，不能仅描述计划或假设成功。
先完成第一阶段并结束本轮回复，等我发送“继续”后再做下一阶段。
每个阶段单独汇报，避免一次 Invocation 超过轮次上限。

通用约束：

- 使用当前项目 Workspace。只在新建的 `.autoagent/tool-smoke-<唯一短标识>/` 内测试文件。
- 不修改项目源代码、数据库、真实凭据、Git 分支或已有文件。
- 需要审批时等待真实审批，不能绕过。拒绝、预期失败、超时都要如实记录，不反复重试。
- 使用实际返回的 process_id、agent_id、message_id 和 cursor，不能编造。
- 尽量批量调用相互独立的工具。等待必须有截止时间，不能无限轮询。
- Context 压缩由策略自动触发，不属于内置 Tool；本次不通过堆积历史强行测试压缩。

## 第一阶段：Skill、用户提问和 demo 工具

1. 调用 `skill_load` 加载 `demo-guide`，再调用 `skill_list_resources`，然后用
   `skill_read_resource` 读取 `references/tool-checklist.md`。
2. 调用 `ask_user`，让我选择测试城市（北京或上海），并确认是否保留测试文件。
   必须等到真实回答后再继续；本阶段也请报告你计划使用的唯一测试目录。
3. 调用 `add_numbers` 计算 100 + 102，再调用 `get_demo_weather` 查询我选择的城市，
   明确标注是模拟天气。
4. 调用 `save_demo_note` 保存这两个结果，标题“内置工具测试”。此步骤应触发审批。
   如果拒绝，记录“用户拒绝”，不能声称保存成功。
5. 汇报本阶段真实结果，提示我发送“继续”开始文件测试。

## 第二阶段：六个文件工具

1. 用 `manage_files` 的 mkdir 创建约定测试目录；用 `write_file` 创建 `sample.txt`，
   内容为 `alpha=1`、`beta=2` 两行。
2. 用 `list_files` 浏览这个目录，用 `search_files` 搜索 alpha，用 `read_file` 读取文件。
3. 用 `edit_file` 精确把 `alpha=1` 改成 `alpha=10`，再读取核对。不要通过 CLI 编辑。
4. 用 `manage_files` 分别测试 stat、copy、move、delete：把 sample.txt 复制成
   copy.txt，先读取 copy.txt，再移动成 moved.txt；读取 moved.txt 后再删除它。
5. 可选的敏感路径审批测试：只在测试目录内创建假的 `.env`，内容固定为
   `DEMO_ONLY=not-a-secret`，再读取它。写入、读取可能分别需要审批；拒绝后跳过。
   严禁读取项目根目录的真实 .env。
6. 保留 sample.txt 供最终报告；汇报文件工具结果，等待我发送“继续”。

## 第三阶段：CLI、权限与进程交互

所有 Python 探针都使用 exec，program 为 `python3`，参数前缀固定为
`["-u", "-m", "agent_demo.cli_probe"]`，cwd 使用 `.`。
不要改成任意 `python -c` 脚本。若 python3 不可用，询问环境问题，不修改探针代码。

1. 使用 `execute_command` 执行 info：应直接允许，返回当前目录和 Python 版本。
2. 再使用 shell/Bash 执行 `printf '%s\n' SHELL_A && printf '%s\n' SHELL_B`：
   同一套 printf 规则应允许两条命令，不应要求重新配置 Shell 规则。
3. 执行 count，附加参数 `--steps 4 --interval 0.5`，yield_seconds=0。
   使用 `manage_process` 的 list 查看进程，再用 read 和 next_cursor 读取新增输出，
   每次 wait_seconds 最多 2，总等待最多 10 秒。确认最终 exited，不重复启动。
4. 执行 echo，keep_stdin_open=true、yield_seconds=0。通过 manage_process.write
   写入 `hello autoagent\n` 并关闭 stdin；这次写入可能需要审批。读取并确认 ECHO 内容。
5. 执行 sleep，附加 `--seconds 5`、timeout_seconds=0.5：预期 timed_out，不重试。
6. 再执行 sleep，附加 `--seconds 30`、yield_seconds=0，随后使用 terminate 主动停止。
7. 执行 fail：预期退出码 3 和 stderr 中的 EXPECTED_FAILURE，不重试。
8. 执行 approval：应等待审批，通过后才出现 APPROVAL_OK。
9. 执行 deny：应被策略拒绝；绝不能出现 DENY_PROBE_WAS_EXECUTED，不改写命令绕过。
10. 汇报退出码、状态、审批与拒绝结果；确保本阶段启动的进程都已结束，等待“继续”。

## 第四阶段：子 Agent 与消息

先检查工具描述里真正允许的 Agent type。内置聊天通常是 agent，demo Workflow 通常是 researcher。
所有子任务都要求不要再创建子 Agent，不要修改文件。

1. 使用 `run_agent` 创建一个简单子任务：计算 7+8 并查询深圳模拟天气。
   返回不一定意味着完成；必要时有限等待，再用 get_agents 检查。
2. 使用 `spawn_agent` 创建消息测试子任务。任务写清楚：先用 get_agents 查询父 Agent，
   检查 Context 中已收到的消息；如尚无消息，用 wait_for 有限等待，最多 20 秒。
   收到父 Agent 要求后，用 send_message 回复，reply_to 填实际原消息 ID，然后结束。
3. 主 Agent 用 send_message 给该子 Agent 发送“请回复 MESSAGE_ACK，并说明你收到的目标”，
   reply_requested=true；可以设置有限的 wait_timeout_seconds。消息到达由 Context 自动读取，
   不调用或虚构 receive_messages 工具。
4. 使用 get_agents 检查任务状态，并至少调用一次 wait_for，最多 10 秒。
   wait_for 可能只唤醒部分任务，醒来后检查全部相关 Agent，而不是假设都完成。
5. 再 spawn 一个取消测试子任务，要求它用有限 wait_for 等待，最长 60 秒。
   随后立即调用 cancel_agent，并用 get_agents 检查最终取消状态；回执不等于已经取消完成。
6. 不再创建任何子任务。确认已有子任务完成或取消后，在测试目录内用 write_file
   创建 report.md，逐项列出所有实际调用过的 Tool、结果和未覆盖项。
7. 根据我第一阶段的选择保留或清理测试文件。清理时先读取需要删除的文件，再删除；
   只移除本次创建的文件和空目录。最终回复给出报告内容或保留路径。
