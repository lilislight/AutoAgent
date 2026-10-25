"""Render the measured comparison as a reviewable Markdown report."""

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path

from .report import load, aggregate, soak_summary, diagnostics


RUNS = ('baseline', 'baseline-pinned', 'monitor-final-ab', 'postgres', 'extended',
        'artifact-threshold', 'cpu-corrected', 'eviction', 'stress-final', 'long-chain',
        'trace-reader', 'monitor-join-ab', 'soak-v1-full', 'soak-v2', 'soak-monitor-final',
        'soak-v1-standard', 'soak-v1-lru')


def number(value, digits=1):
    return '—' if value is None else f'{value:,.{digits}f}'


def table(headers, rows):
    return '\n'.join(['| ' + ' | '.join(headers) + ' |', '| ' + ' | '.join('---' for _ in headers) + ' |',
                      *['| ' + ' | '.join(str(value) for value in row) + ' |' for row in rows]])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    options = parser.parse_args()
    results, expected = load([options.data_root / name for name in RUNS])
    summaries = aggregate(results)
    diagnostic = diagnostics(results)
    regression = json.loads((options.data_root / 'regression/results.json').read_text())
    failed_modules = [row['module'] for row in regression if row['exit_code']]
    if failed_modules:
        raise RuntimeError('Regression failures remain: ' + ', '.join(failed_modules))
    tested = sum(row['tests'] for row in regression)
    postgres = sum(row['postgres'] for row in regression)
    skipped = sum(row['skipped'] for row in regression)
    baseline = [row for row in summaries if row['suite'] == 'baseline-pinned']
    soak = [soak_summary(row) for row in results if row['case']['name'] == 'soak']
    generated = datetime.now(timezone.utc).isoformat(timespec='seconds')
    blocks = [f'''# V1 / V2 原生 Server 性能与内存评估

生成时间：{generated}。完整运行 {len(results)} 组独立 Server 用例，核验 {len(expected)} 类业务输出指纹。
主要结论：V2 在多次调用和大中间结果下的内存驻留明显改善；默认 durable 调用与 SQLite 持久化成本较高；大图编译和 buffered 队列仍有内存风险。

这次没有修改归档 V1，也没有为改善数字改动 Core 执行语义。

## 测试口径

两代均启动原生 FastAPI/Uvicorn Server，使用 HTTP 提交业务，并在调用完成后等待持久化屏障。
V1 使用 minimal、standard、full；另外测试 full 加已有 LRU。V2 对照 durable、buffered、监控开关、直接 join 观察和 Artifact 阈值。
两边使用相同 Python 环境、相同业务函数与结果指纹，执行并发上限均为 32。关闭访问日志与 UI 资源，无真实 LLM 网络调用。

计时分开保存 HTTP 提交、完成观察、flush、启动和关闭；内存从外部每 20 ms 采样整个 Server 的 RSS，包含其线程；没有用 Runtime Event 列表收集器人为保留历史，也没有在计时中启用 tracemalloc。
SQLite 文件大小包含 DB/WAL/SHM；关闭后还包含 Checkpoint。PostgreSQL 测量独立数据库的 relation、索引和 TOAST 分配，随后删除测试数据库。
共享 PostgreSQL 服务、HTTP 控制器、浏览器和 GPU 显存不计入应用 RSS。

主机为 4 个逻辑 CPU、约 7.75 GiB RAM 的 Linux 环境。它不是专用压测机；重型套件顺序执行，但早期基线与长时调用、统计收尾存在重叠。
因此绝对延迟与尾延迟不能直接当作生产 SLO。原始跨轮结果、源文件哈希与软件版本均保留。

初始 156 组广覆盖测试期间存在并行源码开发，差异记录在 `baseline-source-drift.json`。主要 Runtime、Executor、驻留策略及持久化 Writer/Codec 未改变；变化主要涉及监控、CLI/Eval 和嵌套图展示。
主表另用固定源码完成 60 组复测；后续受控套件也使用固定源码。最终监控稳态开销又使用包含启动/关闭覆盖修正的固定快照复测。

V1 默认观察内存状态约每 1 ms 检查，V2 默认 `Host.wait_execution` 包含 100 ms 轮询与状态查询。
直接 `Host.join` 是独立对照，因此不能把小请求的全部延迟差异归因于 Operator 本身。

## 一小时持续调用

工作负载：4 个工作 Node，另加 Entry/Finish；每步生成 16 KiB 文本，最终只返回校验摘要。每次新建 Runtime Session，目标 1 次/秒，单个 worker 只保留一个未完成请求。
实际吞吐受执行耗时和共享主机负载影响。下图只显示 running 阶段，表格峰值包含 Server 完整生命周期。

![RSS 时间线](soak-rss.png)
''']
    blocks.append(table(['模式', '结果', '完成调用', '峰值 RSS MiB', '最后运行 RSS MiB', '末 10 分钟斜率 MiB/min', '最终文件 MiB'],
        [(row['profile'], row['status'], row['calls'], number(row['peak_rss_mib'], 2), number(row['last_running_rss_mib'], 2),
          number(row['tail_slope_mib_per_minute'], 3), number(row['storage_mib'])) for row in soak]))
    blocks.append('''
V1 full 触及外部 640 MiB 保护线后被测试控制器终止，属于保护终止，没有把它记录成内核 OOM。
V1 standard 仍持续增长。full + LRU 的曲线明显收敛，说明历史驻留策略是关键因素。
V2 对这组跨 Session 负载表现稳定，完整持久化没有要求将全部 Event 历史留在进程中。
这不构成任意 Workflow 的硬内存上限保证。

V2 durable 两组启动窗口、背景负载与完成调用数不同，不能用这两个一小时吞吐直接估算监控开销。

## 固定源码基线

每组 3 次独立进程运行；表内延迟从成功的原始调用重算中位数。链图名称只计算工作 Node，另有 2 个辅助 Node。
rows 是每步约 3.5 万条字典记录，不能仅按文本字节估计其 Python 堆成本。失败组不混入成功延迟。

![基线比较](server-comparison.png)
''')
    blocks.append(table(['场景', '模式', '成功/运行', 'HTTP+落盘 p50 ms', '峰值 RSS MiB', '关闭后文件 MiB'],
        [(row['case'], row['profile'], f"{row['successful_runs']}/{row['runs']}", number(row['p50_ms']), number(row['peak_rss_mib']), number(row['closed_storage_mib'])) for row in baseline]))
    blocks.append('''
V1 minimal/standard 保存的信息量与 full 不同，这些模式展示采集成本，完整历史场景应重点对照 V1 full。
V2 的小图基础 RSS 略高，而 rows 等大对象和多次调用的驻留更低。内存、速度和磁盘三项并未同时改善。

## 存储与提交成本

128 次内容不重复的调用，每步 16 KiB、4 个工作 Node，额外有一次预热。8 KiB 对照仅修改现有 `AUTOAGENT_ARTIFACT_THRESHOLD_BYTES`，不删除 Event：
''')
    artifacts = [row for row in summaries if row['suite'] == 'artifact-threshold']
    blocks.append(table(['模式', '调用 p50 ms', '最终文件 MiB', '峰值 RSS MiB'],
        [(row['profile'], number(row['p50_ms']), number(row['closed_storage_mib']), number(row['peak_rss_mib'])) for row in artifacts]))
    blocks.append('''
V1 的默认 Artifact 阈值为 8 KiB，V2 默认 1 MiB。阈值差异能解释一部分写放大，但降低阈值后 V2 仍有额外元数据和引用成本。
长期数据的历史 Event 与 Checkpoint 不会因为 RAM 驻留卸载而自动删除；持久化保留期需要单独设计，并说明历史状态回放的可用边界。

相同 10 工作 Node、5 次调用的 SQL 诊断（包括 Host 观察和后台调度，已扣除启动与预热）：
''')
    sql = [row for row in diagnostic if 'sql' in row]
    blocks.append(table(['模式', 'SQL 语句', '提交', 'cursor 墙钟累计 ms'],
        [(row['profile'], row['sql']['statements'], row['sql']['commits'], number(row['sql']['cursor_ms'])) for row in sql]))
    blocks.append('''
V2 默认模式的 SQL 交互与提交次数明显更多。buffered 能减少提交，却不具有相同的逐转换 durable 确认语义。
独立 cProfile 诊断已排除最初的 profiler 冲突试运行；函数调用统计见 `diagnostics.json`。
多线程/异步 profiler 的阻塞函数归因不能直接当作忙等证据，正常延迟取未启用 profiler 的测试。

## PostgreSQL

每组两次独立数据库。数字包含数据库初始化分配，不能与 SQLite 文件大小逐字节类比；测试文本高度重复，存储格式与压缩也会影响结果。
''')
    pg = [row for row in summaries if row['suite'] == 'postgres']
    blocks.append(table(['场景', '模式', 'p50 ms', '应用峰值 RSS MiB', '数据库 MiB'],
        [(row['case'], row['profile'], number(row['p50_ms']), number(row['peak_rss_mib']), number(row['closed_storage_mib'])) for row in pg]))
    blocks.append('''
## 大图与大数据边界

初始测试中，V1 的 1,000 工作 Node 链在默认 Python 递归深度下编译失败。
V2 能启动该图，但首次调用超过 120 秒观察窗口；另以 600 秒窗口和单次冷调用复测。
注册测试仅衡量 Server 启动/编译/登记，不代表执行过该图。
''')
    boundaries = [row for row in results if row['suite'] in ('stress-final', 'long-chain')]
    blocks.append(table(['场景', '模式', '结果', '完成调用', '峰值 RSS MiB', 'p50 ms'],
        [(row['case']['name'], row['profile'], row['status'], row['completed'], number(row['rss_bytes']/1024**2), number(row['latency_ms']['p50'])) for row in boundaries]))
    blocks.append('''
源码检查发现 `core/compiler/graph.py::_dominators` 为每个非 Entry 初始化完整 Node 集合，具有平方级初始空间。
这与一万节点启动触发 RSS 保护一致，但并未把全部编译内存精确归因于该函数。DAG 快速路径、紧凑集合/位图和编译复用值得优先验证。

buffered 千节点链的保护终止也说明：队列配置和 admission budget 不等于进程 RSS 硬限额。
执行中的大对象、编码临时副本、待提交数据及编译数据都可能提高峰值；单个活跃图无法靠卸载已完成 Session 完全解决。

## 其他场景

宽图、同一 Session 连续调用、慢存储、故障注入、Wait/Resume、在 durable Wait 杀进程并重启均使用原生执行/响应路由。
故障注入的正确结果是 `storage_fault_observed`，不计作业务成功，也不计作 SDK 意外性能失败。
Trace reader 场景对已完成调用读取原生 Trace API，单独记录响应字节、耗时和整个 Server RSS；未测浏览器内存或持续 SSE 客户端。
详细结果见 JSON/CSV，Trace 与 SQL 统计见 `diagnostics.json`。

## Host 内存监控

实现位于 `autoagent/host/memory.py`，由 Host 生命周期管理。默认关闭，开启后从 Workflow 编译前开始采样，覆盖运行及 Core 关闭/Checkpoint 保存阶段。
同一进程的多个 Host 共用一个进程采样线程，每个 Host 保留自己的有界缓存和轻量指标刷新任务。
监控不采集业务输入输出、不捕获 Checkpoint、不深遍历 Runtime State，也不逐秒写数据库或 Runtime Event。

```ini
AUTOAGENT_MEMORY_MONITOR_ENABLED=true
AUTOAGENT_MEMORY_MONITOR_INTERVAL_SECONDS=1
AUTOAGENT_MEMORY_MONITOR_HISTORY_SAMPLES=300
AUTOAGENT_MEMORY_MONITOR_TOP_SESSIONS=10
```

```bash
autoagent --project examples/local_ui_demo server
autoagent --server http://127.0.0.1:8765/api/v2 memory
autoagent --server http://127.0.0.1:8765/api/v2 memory --history 60 --format json
curl http://127.0.0.1:8765/api/v2/memory/metrics
```

SDK：`await host.memory_snapshot(history=60)`。JSON API：`GET /api/v2/memory?history=60`。
Prometheus 使用固定指标名，不将 Session ID 作为高基数标签。

指标含义：

- `process.rss_bytes` 是当前整个 Server RSS，`peak_rss_bytes` 是进程生存期高水位；不支持的测量返回 null。
- `cgroup` 是共享控制组的用量/限制/OOM 事件，不能当作单个 Host 独占内存。
- `persistence.retained_bytes`、`queued_count` 描述待提交队列，frontier 与 fault 用于核对其状态。
- `residency.estimated_bytes` 复用已有缓存估算，Top N 也只保留这些估算。它们可能滞后，不能加总成 RSS 的精确分解。
- `tail_bytes`/`tail_events` 是距 Checkpoint 的累计序列化量，可能已经落盘，不代表还留在 RAM 的 Event 列表。
- `sample_age_seconds`/`stale` 标识进程样本新鲜度；Host 缓存另有 `sampled_at_us`。

历史环最多 3,600 条、Top N 最多 100；默认只保留最近 300 条。压力日志按既有 Host 阈值并带回落滞后触发，不逐秒刷日志。
它是观测系统；admission、驻留卸载和 Node ResourcePolicy 仍各自负责原有策略。

一小时运行后实际验证历史仍为 300 条。采样函数耗时不包含 Host 指标刷新；完整开销采用相同 join 方式、每组五次独立 Server、每次 30 次调用复测：
''')
    joined = [row for row in summaries if row['suite'] == 'monitor-join-ab']
    blocks.append(table(['模式', '调用样本', 'p50 ms', 'p95 ms', '峰值 RSS MiB'],
        [(row['profile'], row['latency_samples'], number(row['p50_ms']), number(row['p95_ms']), number(row['peak_rss_mib'], 2)) for row in joined]))
    blocks.append(f'''
## 验证与证据

完整 V2 回归采用逐模块独立进程，避免大数据测试遗留 RSS 干扰后续 Host admission：{len(regression)} 个模块、{tested} 个测试通过，其中 PostgreSQL {postgres} 个，跳过 {skipped} 个；没有跳过 PostgreSQL 用例。
监控专测覆盖早期编译、采样独立性、缓存上限、共享线程代际、关闭阶段、只读/禁用模式、HTTP/CLI、故障降级和两个数据库。

- [汇总 JSON](summary.json)、[CSV](summary.csv)、[诊断](diagnostics.json)
- 完整原始数据与数据库：仓库忽略目录 `.autoagent/performance/`；每组含 manifest、源码哈希、逐调用、RSS 时间线和 Server 日志。
- 复现命令与测量边界：[benchmark README](../../../tests/benchmarks/server_comparison/README.md)。绘图额外使用开发依赖 matplotlib，监控本身只使用标准库。

## 下一步优先级

1. 优化大图编译的平方级集合与重复编译成本，先让大 Workflow 的启动空间可控。
2. 优化完整记录下的 SQL 校验/投影/提交交互和重复数据编码，分别保持 durable 与 buffered 的明确语义。
3. 用真实生产 payload 选取 Artifact 阈值，审计大输入/Context 的引用覆盖，并单独制定历史保留期；上述测试不代表真实 LLM、Streaming、Spawn 或完整 harness 的专项性能。
''')
    options.output.mkdir(parents=True, exist_ok=True)
    (options.output / 'README.md').write_text('\n\n'.join(blocks))
    print(json.dumps(dict(runs=len(results), tests=tested, postgres=postgres,
                          statuses=dict(Counter(row['status'] for row in results))), indent=2))


if __name__ == '__main__':
    main()
