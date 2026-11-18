# Child 终态压缩验证

基线为本轮开始时的工作区，包含上一轮 settling 改动；不是单独的 Git HEAD。基线源码复制到 /tmp/autoagent-child-compact-baseline，与修改后实现使用相同 benchmark 脚本，未创建分支或 worktree。source_hashes.json 记录前后源码指纹。

## 实现与边界

- 终态 Child 使用 ChildResult 替换完整 InvocationState；保留 Session 身份和事件序号的薄封装，以及最终 output/error/cancel_reason、必要时间和后代归属。Root 不压缩。
- ChildResult 不保存输入、Invocation/Session Context、Scheduler 历史或执行索引。内部公共读取视图返回共享的空 Scheduler/Context，不为每个结果分配执行结构。
- waiting、running、settling 不压缩；终态后还必须完成业务清理和后代结算。ChildCompacted 得到 Sink ACK 后才发布精简结果；失败保留完整 State，重试同一 Event。
- Parent 对应 Node 提交 NodeCompleted 后释放计划输入；Parent 进入结束/停止边界时也释放不再需要的计划输入。await Map 未完成时保留聚合和恢复所需输入。input_released 区分真实 None 输入与已释放输入。
- 子任务终态的 input_digest 保留固定长度输入证明，加载部分 await 图时仍可校验 Parent 保留的输入；父子身份与归属也必须一致。
- 压缩后的 Parent Child 仍保留后代归属元数据；不能因自身压缩而删除后代 Handle 的结果。读取结果不会释放它，结果保留至整图卸载或替换。
- checkpoint 在原有 Session 封装内用 kind=child_result 编码轻量结果，不包含完整执行 State；load 不为它建立执行索引，recover 不重跑 Child。缺失结果、伪造归属或把压缩 Child 当 Root 都会拒绝。
- 新增的是内部结果形态，未新增公开 await_child 或独立 Child 卸载 API。RuntimeState/Event schema 升至 8，SessionCheckpoint schema 升至 6，旧格式不兼容。

## 正确性验证

- 411 项全量测试通过；compileall、git diff --check 和 Core Workflow 示例通过。
- 新增 9 项测试覆盖：部分 Spawn Wait、await 聚合输入与结果、嵌套 Handle、压缩 ACK 失败、压缩前后事件前缀恢复、缺失和伪造 checkpoint、释放 Context 且不复制 output、部分 await 输入摘要校验，以及 await Child 等待后代 Wait 的恢复。
- 现有执行索引检查改为验证 ChildResult 没有执行索引；完整运行 State 仍逐事件比较增量索引和重建索引。

## 内存测试

每组保留最后一个 Child 运行，其余已完成，Root Graph 仍驻留。Map max_parallelism=8，Sink 只计数、不保留 Event。tracemalloc 测量 Python 跟踪的分配量，GC 后读取；不是 RSS，也未施加 Docker 512 MiB/1 GiB 限额。

| 场景 | 前/后保留 MiB | 前/后峰值 MiB | 已压缩 Child |
|---|---:|---:|---:|
| text_64x1MiB_small_output | 64.44 / 1.21 | 65.68 / 66.66 | 63 |
| rows_32x2048_small_output | 38.44 / 2.01 | 79.05 / 79.05 | 31 |
| text_32x1MiB_retained_output | 32.22 / 32.11 | 33.37 / 34.36 | 31 |

输入为独立大文本或 list/dict 容器，输出保留场景则直接返回大文本。保留内存显著降低，但 Map 一次性创建全部输入时的峰值没有降低；输出仍引用大输入时也不会释放那些数据。原始 traced_elapsed_ms 开启了 tracemalloc，不能用作正常执行耗时对比。

## 正常执行耗时

同机顺序运行，预热 1 次后取 3 次中位数，计时不启用 tracemalloc。两版本均等待整图完成；使用无保留的计数 Sink。

| 场景 | 修改前 ms | 修改后 ms | 变化 |
|---|---:|---:|---:|
| no_child | 1.670 | 1.806 | +8.2% |
| map_32 | 40.439 | 51.120 | +26.4% |
| map_100 | 124.281 | 146.088 | +17.5% |
| nested_4 | 6.777 | 6.804 | +0.4% |
| five_roots_large_outputs | 73.025 | 74.228 | +1.6% |

本次是以内存回收换取额外计算与确认成本，不是纯执行加速。每个终结 Child 增加一个压缩 Event，并计算输入摘要；100 Child 样本增加约 22 ms（约 18%）。小样本存在调度噪声，真实 Sink 延迟会影响额外事件成本。压缩 Event 的结果与终态 State 共享不可变 output；外部 Sink 如果保留历史 Event，仍会持有历史数据，不计入这组 Core 无保留 Sink 测量。

## 原始记录

- memory_before.json / memory_after.json：长期驻留及峰值内存。
- execution_before.json / execution_after.json：整图执行耗时和现有 benchmark 附带指标。
- tests.log：411 项测试日志。
- source_hashes.json：本轮前后 autoagent 源码指纹。
