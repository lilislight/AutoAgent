"""Report query-only measurements with historical and same-round controls apart."""
import argparse
from collections import Counter
import json
import math
from pathlib import Path
import statistics as st


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    args=parser.parse_args();base=args.directory
    summary=json.loads((base/'summary.json').read_text())
    details=json.loads((base/'details.json').read_text())
    comparisons=json.loads((base/'comparisons.json').read_text())
    raw=[json.loads(line)for line in (base/'results.jsonl').read_text().splitlines()]
    errors=json.loads((base/'audit-errors.json').read_text())
    provenance=json.loads((base/'provenance.json').read_text())
    verification=json.loads((base/'verification/summary.json').read_text())if(base/'verification/summary.json').exists()else None
    lookup={(r['phase'],r['backend'],r['case'],r['version'],r['mode']):r for r in summary}
    get=lambda backend,case,version,mode:lookup['formal',backend,case,version,mode]
    formal=[r for r in comparisons if r['phase']=='formal']
    text='''# 查询优化：Server 实测与上一轮最终版本对比

## 比较条件

“优化前”是上一轮步骤 9 的最终版本，不是最初尚未优化的 V2。逐文件 hash 已确认两者一致。
本轮结构化 JSON 中的 b0/final 标签分别表示查询优化前/后。
正式前值复用上一轮五进程测量；后值重新测量。都启动原生 Server，通过相同 HTTP 调用，包含完成观察、输出读取与强持久化屏障。
两种数据库、durable/buffered、六个场景；每组五个独立进程，每进程预热一次后测量 20 次，unique-history 为 128 次。Artifact 阈值 8192 bytes，buffered 等待 100 ms、durable 等待 0 ms，内存监控关闭。
使用相同冻结工具测量前后源码，并对 Chain、Map 的 buffered 两后端额外进行三轮交替对照。共享主机无独占 CPU，历史组与新组分时运行，不能把所有差异都归因于代码；同轮结果分列。
历史工具之后增加了无数据库测试入口；本轮持久化 HTTP 路径与业务保持相同，工具差异另存 measurement-tools.patch/provenance.json。同轮补测前后使用完全相同的当前冻结工具。
正式 comparisons.json 的 pairs 仅按历史轮号对齐，不代表同轮交替实验；真正的同轮对照在 paired 小节单列。主要运行依赖版本核对保存在 provenance.json。
本轮没有改变 Event、投影、数据保留或持久化确认语义；没有修改 Core、V1 或 UI。

## 实现范围

- 在现有 Writer 元数据预算内划分最多 512 KiB，保存保守的源 ID membership 过滤器；启动时分批读取五类源 ID。
- 确定不存在的源 ID 不再 SELECT；可能存在的 ID 继续查询并核对内容与历史绑定。写入前先加入过滤器，回滚与删除保留位，保证只增加误报、不隐藏既有记录。
- 复用已确认的不可变定义元数据，减少 Workflow/Operator 定义查询。
- 单 Writer 在正常提交时使用已知 store_order；异常、维护和不确定提交后重新读取。
- User Event 复用提交后的序号元数据；已在入队时验证的序号不再在提交时重复查 MAX。

过滤器饱和时逐渐退回 SQL，正确性不依赖它命中。首次打开大型已有数据库需要扫描源 ID，这项启动成本尚未做百万级历史量化；不能把它描述成无代价优化。未新增数据库表、索引、配置项或无界 Event/业务值缓存。

本轮保留 accepted 读路径的临时投影和现有控制依赖；它们随下一阶段“控制脱离投影、独立内存读模型”重构，未通过改变观察语义美化本轮结果。Artifact 去重批量预查询也保持原样。

## 正式速度与资源

每格为五个进程指标的中位数。延迟是请求 p50，CPU 是 Server 累计 CPU/调用（跨线程），不能与请求 p50 相减得到 I/O 等待；PostgreSQL 服务自身 CPU/RSS 未归入 Server。RSS 是正式运行期外部采样峰值，关闭后的物理存储单列。

| 后端 | 模式 | 场景 | p50 ms 前→后 | 耗时降低 | CPU ms/调用 前→后 | RSS MiB 前→后 | 存储 MiB 前→后 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
'''
    for c in sorted(formal,key=lambda r:(r['backend'],r['mode'],r['case'])):
        a=get(c['backend'],c['case'],'b0',c['mode']);z=get(c['backend'],c['case'],'final',c['mode'])
        text+=f"| {c['backend']} | {c['mode']} | {c['case']} | {a['p50_ms']:.1f}→{z['p50_ms']:.1f} | {c['p50_reduction_pct']:+.1f}% | {a['cpu_ms_call']:.1f}→{z['cpu_ms_call']:.1f} | {a['rss_mib']:.1f}→{z['rss_mib']:.1f} | {a['storage_mib']:.2f}→{z['storage_mib']:.2f} |\n"
    aggregates={}
    for mode in ('buffered','durable'):
        rows=[r for r in formal if r['mode']==mode]
        if rows:
            speed=math.exp(st.mean(math.log(r['b0']['p50_ms']/r['final']['p50_ms'])for r in rows))
            aggregates[mode]=dict(groups=len(rows),speedup=speed,normalized_time_reduction_pct=(1-1/speed)*100)
            text+=f"\n{mode}：两后端六场景等权几何平均加速 {speed:.3f}×，对应归一化耗时降低 {(1-1/speed)*100:.1f}%。\n"
    text+='''
## 同轮交替复核

这部分不复用历史结果。相邻前后进程执行相同 10 次调用，三轮交替先后顺序；与正式 20 次调用的表分开。

| 后端 | 模式 | 场景 | 前/后进程数 | p50 ms 前→后 | 降低 | CPU ms/调用 前→后 |
| --- | --- | --- | ---: | ---: | ---: | ---: |
'''
    for a in summary:
        if a['phase']!='paired'or a['version']!='b0':continue
        z=lookup['paired',a['backend'],a['case'],'final',a['mode']]
        text+=f"| {a['backend']} | {a['mode']} | {a['case']} | {a['runs']}/{z['runs']} | {a['p50_ms']:.1f}→{z['p50_ms']:.1f} | {(1-z['p50_ms']/a['p50_ms'])*100:+.1f}% | {a['cpu_ms_call']:.1f}→{z['cpu_ms_call']:.1f} |\n"
    text+='''
## SQL 独立诊断

每组 5 次 Chain 调用，诊断与正式延迟分开。SQL 包含 BEGIN 等语句；driver COMMIT 计数包含只读事务，不等于 fsync 次数。嵌套/并行阶段耗时不能相加。

| 后端 | 模式 | 版本 | SQL | SELECT | driver COMMIT | 写批次 | Runtime Event |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
'''
    for r in sorted([r for r in details if r['phase']=='diagnostic'],key=lambda r:(r['backend'],r['mode'],r['version'])):
        d=r['diagnostic']
        label='前'if r['version']=='b0'else'后'
        text+=f"| {r['backend']} | {r['mode']} | {label} | {d['statements']} | {d['selects']} | {d['driver_commits']} | {d['batches']['count']} | {d['batches']['runtime_events']} |\n"
    text+='\nbuffered 在原有批量策略下，实测写批次由 10 降到 5/6；生产速度变化使更多 Event 在同一批中写入。批量等待配置没有调整，Runtime Event 数量保持 310。串行 durable 的写批次保持 310。\n'
    new=[r for r in raw if not r.get('baseline_source')]
    text+=f"\n实际新增 {len(new)} 组运行、{sum(r['completed']for r in new)} 次测量调用；历史复用 {len(raw)-len(new)} 组。新运行状态：{dict(Counter(r['status']for r in new))}。审计问题 {len(errors)} 项。事实/投影规范行数配平、关闭后 SQLite/PG 审计和跨版本业务输出核对见 audit-errors.json；Checkpoint 等可变行数见 variable-counts.json。\n"
    if verification:
        text+=f"\n完整回归 {verification['modules']} 个模块、{verification['tests']} 项，其中 PostgreSQL {verification['postgres_tests']} 项，跳过 {verification['skipped']} 项。\n"
        if verification['failed_modules']:
            text+='\n完整回归并非全绿，失败项如下：\n\n'
            for r in verification['failed_modules']:
                text+=f"- {r['module']}："+'；'.join(r['failures'])+'。\n'
            text+='\n既有天气示例 __main__ 入口缺失的失败与上一轮相同，本轮没有修改示例或兼容入口。\n'
    text+='''
## 边界与复现

本轮没有再做一小时长测；前一轮长测不替代新代码的长期稳定性验收。ID 过滤器预算、源对象释放、低预算饱和、重启、跨 Stream 身份冲突、重复提交、丢失提交确认、维护与 Fork 由回归覆盖。SQL 启动扫描的长期数据库代价需独立测量。

```bash
.venv/bin/python -m tests.postgres start
.venv/bin/python -m tests.benchmarks.server_comparison.query_optimization --phase all
.venv/bin/python -m tests.benchmarks.server_comparison.acceptance_report --directory .autoagent/performance/query-optimization-2026-10-25
.venv/bin/python -m tests.benchmarks.server_comparison.query_report .autoagent/performance/query-optimization-2026-10-25
```

上述复现控制器会检查冻结源码与工具 hash，并续跑未完成的实验。新目录须先创建 before 快照与 provenance；禁止把已有结果覆盖成另一版源码。原始配置留在忽略目录内，可能包含本地数据库地址，不应直接公开。
'''
    (base/'aggregate.json').write_text(json.dumps(aggregates,indent=2))
    (base/'report.md').write_text(text)
    print(json.dumps(dict(new_runs=len(new),formal_groups=len(formal),audit_errors=len(errors),aggregate=aggregates)))


if __name__=='__main__':main()
