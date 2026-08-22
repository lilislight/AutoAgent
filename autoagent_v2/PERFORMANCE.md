# Full Core 基准记录

运行命令：

```bash
../.venv/bin/python -m tests.benchmarks.benchmark_full_core
```

2026-08-21 两层 StateOperation/Event 模型本地参考结果（启用
`tracemalloc`，不是 CI 阈值）：

| 场景 | 时间 | Event | Event 大小 | 峰值内存 |
| --- | ---: | ---: | ---: | ---: |
| 100 Node 串行链 | 2.86 s | 405 | 641 KB | 4.25 MB |
| 500 单元 Map | 3.43 s | 1007 | 1.39 MB | 3.96 MB |
| 1 MB Session Context、单 Node | 50 ms | 9 | 2.01 MB | 4.06 MB |

Full 模式为每个物理 OperatorCall 记录 started/completed Event，并同时保存可重放
StateOperation，因此相较改造前增加了编码量。Session 初始 Context 在 Full Trace
Payload 与恢复 Operation 中各保存一次，不保存完整 RuntimeState；这是当前 Full
观察信息的明确空间成本，不应误称为零重复。

热执行路径使用不可变 candidate State 和对象身份增量 diff，不在每次转换时重建完整
State；通用 record 应用仅用于 replay/recovery。后续 Standard/Minimal 应通过调整
Runtime Event flush 边界和 Trace Payload 投影降低 Event 数、路径编码与观察 Payload，
但必须保持相同的 StateOperationBatch 和最终 RuntimeState。
