# V2 Core 基准记录

运行命令：

```bash
../.venv/bin/python -m tests.benchmarks.benchmark_full_core
```

基准通过注入的 Host `RuntimeEventSink` 统计 canonical RuntimeEvent；公开
`InvocationResult` 只用于统计 Trace/User Event 与最终 Checkpoint。
`runtime_event_bytes` 和 `checkpoint_bytes` 都是 compact JSON 大小，`tracemalloc`
峰值不是 CI 阈值。

2026-08-25 本地参考结果：

| 场景 | 时间 | RuntimeEvent | Event 大小 | Trace | Checkpoint | 峰值内存 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 Node 串行链 | 2.44 s | 405 | 679 KB | 405 | 73 KB | 5.19 MB |
| 500 单元 Map | 3.34 s | 1007 | 1.44 MB | 1007 | 171 KB | 3.73 MB |
| 1 MB Session Context、单 Node | 35 ms | 9 | 1.92 MB | 9 | 979 KB | 3.90 MB |

RuntimeEvent 保存 StateOperationBatch，不保存完整 RuntimeState。大 Context 在 Session
打开的恢复 Operation 和最终 Checkpoint 中各出现一次；公开 Trace 投影不复制完整
Context。Checkpoint 通过不可变 RuntimeState 的浅引用构建，表中大小只在序列化时产生。

后续 Standard/Minimal capture profile 应通过聚合 Event 和收窄 Trace 投影降低观察成本，
但必须保持相同 StateOperationBatch、最终 RuntimeState 与恢复结果。

## Reducer 前缀重建

`StateReducer.reduce` 面向 Core 产生且 Host 已接纳的 sealed Event 前缀：复用一份
canonical record，StateOperation 只复制修改路径，最后在返回边界执行一次完整
`RuntimeState.from_record`。不可信 Event 需要用 `apply` / `apply_batch` 逐边界验收。

| Event 前缀 | 中位时间 | 完整 State decode |
| ---: | ---: | ---: |
| 50 | 4.2 ms | 1 |
| 100 | 7.5 ms | 1 |
| 200 | 14.0 ms | 1 |
| 400 | 29.9 ms | 1 |

上述 decode 次数是结构指标，不使用脆弱的 CI 时间阈值。优化前同机单次参考值约为
24 / 82 / 316 / 1223 ms。

## Live transition 扩展性

基准先注册 Workflow，再计时完整 `invoke`，从而排除编译成本：

| Node | Trace Event | 中位时间 |
| ---: | ---: | ---: |
| 50 | 205 | 107 ms |
| 100 | 405 | 260 ms |
| 200 | 805 | 707 ms |
| 400 | 1605 | 2.16 s |

`ValueContract` 复用编译后的 Pydantic `TypeAdapter`，避免每次 validate、encode 和
restore 重建同一 schema。400 Node 的一次采样 profile 中，`StateReducer.plan` 约占
86%，其中完整 `validate_runtime_state` 约占 74%，identity-aware diff 约占 8%；DAG
Scheduler 的 complete/propagate 不到 0.5%，Reducer 内应用 Scheduler delta 不到 0.2%。
比例只用于定位，不是性能阈值。

超线性来自明确的正确性选择：每个 transition 都完整校验持续增长的 Occurrence 与
OperatorCall 历史。只要同时保留完整历史 State 和逐 transition 全量验收，这一复杂度
就不会线性。当前不通过跳过校验、缓存“已验证”对象或维护第二份增量镜像来换取速度；
后续若要继续优化，应先决定终态历史是否仍属于恢复所需的当前 RuntimeState，还是只由
RuntimeEvent 保留。
