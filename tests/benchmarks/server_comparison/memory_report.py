"""Audit and summarize the standalone Core HTTP comparison without a database."""
from collections import defaultdict
import argparse
import json
from pathlib import Path
import statistics as st


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    parser.add_argument('--persistent-summary',type=Path)
    args=parser.parse_args();base=args.directory.resolve()
    raw=[json.loads(line)for line in (base/'results.jsonl').read_text().splitlines()]
    groups=defaultdict(list);outputs={};errors=[]
    for r in raw:
        case=r['case']['name'];folder=base/f"{case}-{r['profile']}-{r['repeat']}"
        if r['status']!='completed' or r['completed']!=r['case']['calls']:
            errors.append(dict(run=folder.name,status=r['status'],error=r.get('error')));continue
        if r['backend']!='none' or r['catalog']['database_mode']!='none' or r['closed_storage']['total_bytes'] or list(folder.glob('runtime.db*')):
            errors.append(dict(run=folder.name,error='Persistence was not completely disabled'))
        calls=[json.loads(line)for line in (folder/'calls.jsonl').read_text().splitlines()]
        for c in calls:
            key=case,c['index']
            if key in outputs and outputs[key]!=c['output']:
                errors.append(dict(run=folder.name,index=c['index'],error='Business output mismatch'))
            outputs.setdefault(key,c['output'])
        groups[case,r['profile']].append(dict(
            turn=r['repeat'],p50_ms=r['latency_ms']['p50'],p95_ms=r['latency_ms']['p95'],
            core_p50_ms=st.median(c['core_ms']for c in calls),
            cpu_ms_call=(r['drained_memory']['cpu_seconds']-r['warm_memory']['cpu_seconds'])*1000/r['completed'],
            lifetime_rss_mib=r['rss_bytes']/1048576,calls=r['completed']))
    summary=[]
    for (case,profile),runs in sorted(groups.items()):
        summary.append(dict(case=case,profile=profile,runs=len(runs),calls=sum(r['calls']for r in runs),
            **{key:st.median(r[key]for r in runs)for key in ('p50_ms','p95_ms','core_p50_ms','cpu_ms_call','lifetime_rss_mib')},
            process_samples=runs))
    for name,value in (('summary',summary),('audit-errors',errors)):
        (base/(name+'.json')).write_text(json.dumps(value,indent=2))
    report='''# 无持久化 Server 对比

两版 Core 都启动独立 Uvicorn/FastAPI Server，使用同一 HTTP 包装和相同业务。
V1 为 RuntimeStore(backend=None)，分别保留 standard/full 的内存事件语义；
V2 为 AutoAgentApp(runtime_event_sink=None)。不是 SQLite 内存数据库，也不是写入后删除文件。
V2 Host 必须配置数据库，因此本表是 Core + 相同 HTTP 包装，不是原生 Host/Server。
每进程一次预热后 20 次正式调用，每场景每配置五个独立进程，串行随机顺序。
所有过程默认保留各自的内存状态，没有手动卸载、清空历史或禁止 GC。
外部每 20 ms 采样进程内存和累计 CPU；CPU 只包含 Server 进程，精度受系统时钟 tick 影响。
core p50 为 HTTP handler 内 await 公共 ainvoke 的耗时，包含 App Runtime 线程调度。
无数据库文件和输出一致性按每组检查；独立回归开启 SQL 监听，验证三类业务两版 SQL 执行数为零。

| 场景 | 配置 | 进程数 | HTTP p50 ms | HTTP p95 ms | Core p50 ms | CPU ms/调用 | 生命周期峰值 RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
'''
    for r in summary:
        report+=f"| {r['case']} | {r['profile']} | {r['runs']} | {r['p50_ms']:.2f} | {r['p95_ms']:.2f} | {r['core_p50_ms']:.2f} | {r['cpu_ms_call']:.2f} | {r['lifetime_rss_mib']:.1f} |\n"
    if args.persistent_summary:
        previous=json.loads(args.persistent_summary.read_text())
        report+='''
## 上一轮原生 Server 持久化结果参考

以下保留上一轮五进程中位数，Artifact 阈值 8 KiB、完成后包含强 flush。
这是不同 HTTP/Host 路径和不同轮次的参考；不能将与上表的差值当作持久化各函数的严格耗时分解。
unique-history 上轮为每进程 128 次，本轮为 20 次，不能忽略不同历史长度。

| 后端 | 场景 | V1 standard p50 ms | V1 full p50 ms | V2 buffered p50 ms | V1 full CPU ms/调用 | V2 buffered CPU ms/调用 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
'''
        for backend in ('sqlite','postgres'):
            for case in sorted({r['case']for r in summary}):
                def get(version,mode):
                    return next(r for r in previous if r['phase']=='formal' and r['backend']==backend and r['case']==case and r['version']==version and r['mode']==mode)
                standard,full,v2=get('v1','standard'),get('v1','full'),get('final','buffered')
                report+=f"| {backend} | {case} | {standard['p50_ms']:.1f} | {full['p50_ms']:.1f} | {v2['p50_ms']:.1f} | {full['cpu_ms_call']:.1f} | {v2['cpu_ms_call']:.1f} |\n"
    report+=f"\n完成 {len(raw)} 组、{sum(r['completed']for r in raw)} 次调用；审计问题 {len(errors)} 项。完整逐进程值见 summary.json，来源 hash 见 manifest.json。\n"
    (base/'report.md').write_text(report)
    print(json.dumps(dict(runs=len(raw),calls=sum(r['completed']for r in raw),audit_errors=errors)))
    if errors:raise SystemExit(1)


if __name__=='__main__':main()
