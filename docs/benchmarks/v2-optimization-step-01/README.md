# V2 优化步骤 1：新增事实直接插入与 Node 去重写入

状态：步骤 1 已完成，保留本步修改；停止等待步骤 2 指令。

本次 232 组原生 Server 运行、4,952 次正式调用全部通过。完整 Python 回归 98 个独立模块、1,682 项测试通过，其中 PostgreSQL 298 项，0 跳过。

## 验收结论

保留本步。确定新增 Event 的事实读前查询消失，重复 Node 写入减少；链式、循环、Context 和大文本场景的 CPU 下降，多个场景的端到端耗时也有改善。全部配置中核对的五类记录（Runtime Event、图变化、求值、Node、Operator Call）数量中位数一致，逐调用校验输出、queued/retained 为零，最终 accepted/durable frontier 相等。

主目标正式验证（每配置 5 个进程 × 20 次调用）的 p50 中位数：

| 后端 | 模式 | A → B | 耗时降低 | CPU 降低 |
| --- | --- | --- | ---: | ---: |
| SQLite | durable | 944.0 → 825.5 ms | 12.6% | 13.9% |
| SQLite | buffered | 356.6 → 316.1 ms | 11.4% | 11.7% |
| PostgreSQL | durable | 685.2 → 629.7 ms | 8.1% | 14.4% |
| PostgreSQL | buffered | 260.3 → 240.2 ms | 7.7% | 9.4% |

四个配置的五轮配对 p50 都有改善；SQLite durable 的单轮降低范围为 12.3%–13.5%，buffered 为 10.6%–12.4%；PG durable 为 1.3%–15.9%，buffered 为 6.2%–9.6%。PG durable 波动较大，不将其中位数推广为每次调用固定提升。逐轮数据见 [formal-pairs.json](formal-pairs.json)。

Map 的初筛延迟变化约 1%–4%，部分短 durable 调用的变化不足 1%；这些不作为已证明的端到端收益。该优化组合不能消除逐 Event durable 提交及其余元数据查询开销，也不表示已经接近 V1 的速度。

RSS 中位数未触发方案规定的回退审查线，所有配置中最大的增加约 1.8 MiB；没有引入跨历史增长的缓存。关闭后物理存储存在页分配波动，例如 PG 正式 chain durable 为 14.78 → 15.03 MiB，不将本步描述为统一的存储缩减。完整指标与前后分布保留在表和 JSON 中。

本结果评价两项修改的组合；没有通过消融实验把具体毫秒收益拆给某一项。三进程初筛及共享主机数据不构成生产 p95 SLA，长期内存仍留待方案最终验收的 soak。

## 修改范围

- 新增 Event 的 graph_changes/edge_evaluations 直接构造事实行并批量 INSERT；只有 Storage 事务确认来源 Event 不存在时才启用。
- 重建、Checkpoint 和 accepted overlay 保留保守读取/更新路径；重复来源 Event 仍先进行身份与序列校验并跳过二次投影。
- 已按父子依赖顺序写过的 Node 从剩余集合移除，避免末尾再次 UPSERT。
- 修改产品文件仅 projections.py、storage.py；队列、缓存、batch、durable 与 flush 语义未调整。

## 实验口径

A 为本步修改前的实际源码，与 B0 产品 hash 一致；B 为本步修改后固定源码。同一版基准工具，SQLite/PG、durable/buffered、8192 bytes Artifact。每轮交替 A/B，第二轮 B/A，标准场景每配置 3 个独立 Server。另对 chain 主目标进行每配置 5 个独立 Server、每进程 20 次测量调用的正式验证；表中 `chain-10-16k-formal` 是该统计分组标签，业务负载和配置相同，不能与 5 次调用初筛直接混组。延迟、CPU 和 RSS 为独立进程指标中位数；p95 样本较少，不能作为生产尾延迟 SLO。诊断运行与正常速度分开。

## 增量效果 A → B

耗时降低率为 (A-B)/A，正数表示改善；CPU 降低率同理；RSS 为 B-A，正数表示增加。

| 后端 | 模式 | 场景 | A p50 ms | B p50 ms | 耗时降低 | CPU 降低 | RSS 变化 MiB | A/B p95 ms |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| postgres | durable | chain-10-16k | 684.2 | 680.5 | +0.6% | +12.5% | -0.4 | 689.1/691.4 |
| postgres | durable | chain-10-16k-formal | 685.2 | 629.7 | +8.1% | +14.4% | -1.3 | 788.1/693.5 |
| postgres | durable | context-append | 1819.6 | 1669.2 | +8.3% | +13.8% | +0.3 | 1821.7/1678.3 |
| postgres | durable | loop-100-16k | 6771.4 | 5699.1 | +15.8% | +16.6% | +1.8 | 6863.1/5859.1 |
| postgres | durable | map-100-16k | 2114.0 | 2026.7 | +4.1% | +5.0% | -0.1 | 2125.8/2099.9 |
| postgres | durable | text-overwrite | 2363.8 | 2189.2 | +7.4% | +10.7% | -0.3 | 2434.0/2192.9 |
| postgres | durable | unique-history | 360.3 | 358.2 | +0.6% | +11.3% | +0.1 | 479.1/368.9 |
| postgres | buffered | chain-10-16k | 274.9 | 246.4 | +10.4% | +9.2% | -0.0 | 289.9/262.5 |
| postgres | buffered | chain-10-16k-formal | 260.3 | 240.2 | +7.7% | +9.4% | +0.3 | 352.3/327.8 |
| postgres | buffered | context-append | 777.8 | 746.7 | +4.0% | +4.6% | -1.6 | 805.5/772.1 |
| postgres | buffered | loop-100-16k | 2955.6 | 2651.0 | +10.3% | +8.6% | -1.8 | 3078.1/2715.6 |
| postgres | buffered | map-100-16k | 917.1 | 896.0 | +2.3% | +1.8% | -0.5 | 942.7/940.7 |
| postgres | buffered | text-overwrite | 1278.5 | 1113.7 | +12.9% | +11.4% | -5.1 | 1294.4/1133.8 |
| postgres | buffered | unique-history | 187.4 | 173.4 | +7.5% | +8.1% | -0.6 | 214.8/201.9 |
| sqlite | durable | chain-10-16k | 942.8 | 819.2 | +13.1% | +13.4% | -0.1 | 1032.3/842.8 |
| sqlite | durable | chain-10-16k-formal | 944.0 | 825.5 | +12.6% | +13.9% | +0.1 | 964.7/872.6 |
| sqlite | durable | context-append | 2451.9 | 2196.7 | +10.4% | +13.3% | -0.1 | 2457.0/2197.5 |
| sqlite | durable | loop-100-16k | 9328.5 | 7985.4 | +14.4% | +15.5% | -0.2 | 9422.0/8138.1 |
| sqlite | durable | map-100-16k | 2793.4 | 2757.2 | +1.3% | +3.3% | -0.2 | 2844.4/2831.1 |
| sqlite | durable | text-overwrite | 3030.8 | 2689.1 | +11.3% | +11.8% | +1.5 | 3038.6/2786.4 |
| sqlite | durable | unique-history | 497.0 | 492.3 | +0.9% | +11.3% | +0.1 | 614.6/510.7 |
| sqlite | buffered | chain-10-16k | 366.5 | 305.0 | +16.8% | +17.7% | -0.2 | 433.2/331.0 |
| sqlite | buffered | chain-10-16k-formal | 356.6 | 316.1 | +11.4% | +11.7% | -0.4 | 437.9/369.0 |
| sqlite | buffered | context-append | 1083.8 | 890.8 | +17.8% | +14.3% | -0.8 | 1103.3/946.7 |
| sqlite | buffered | loop-100-16k | 4081.2 | 3816.4 | +6.5% | +5.8% | -0.3 | 4136.0/3868.1 |
| sqlite | buffered | map-100-16k | 1155.6 | 1136.4 | +1.7% | +1.5% | +0.6 | 1173.5/1149.8 |
| sqlite | buffered | text-overwrite | 1571.9 | 1392.5 | +11.4% | +11.3% | +1.2 | 1584.9/1411.8 |
| sqlite | buffered | unique-history | 216.0 | 196.2 | +9.2% | +9.2% | -0.5 | 247.5/227.6 |

## 累计效果 B0 → B

B0 为前一阶段历史测量，未与本轮交错；累计列用于参考，本步因果判断优先看本轮 A/B。

| 后端 | 模式 | 场景 | B0 p50 ms | B p50 ms | 耗时降低 |
| --- | --- | --- | ---: | ---: | ---: |
| postgres | durable | chain-10-16k | 682.2 | 680.5 | +0.3% |
| postgres | durable | context-append | 1818.2 | 1669.2 | +8.2% |
| postgres | durable | loop-100-16k | 6746.9 | 5699.1 | +15.5% |
| postgres | durable | map-100-16k | 2115.7 | 2026.7 | +4.2% |
| postgres | durable | text-overwrite | 2442.6 | 2189.2 | +10.4% |
| postgres | durable | unique-history | 361.3 | 358.2 | +0.9% |
| postgres | buffered | chain-10-16k | 276.5 | 246.4 | +10.9% |
| postgres | buffered | context-append | 794.4 | 746.7 | +6.0% |
| postgres | buffered | loop-100-16k | 2949.6 | 2651.0 | +10.1% |
| postgres | buffered | map-100-16k | 931.6 | 896.0 | +3.8% |
| postgres | buffered | text-overwrite | 1287.0 | 1113.7 | +13.5% |
| postgres | buffered | unique-history | 190.7 | 173.4 | +9.1% |
| sqlite | durable | chain-10-16k | 938.3 | 819.2 | +12.7% |
| sqlite | durable | context-append | 2453.1 | 2196.7 | +10.5% |
| sqlite | durable | loop-100-16k | 9082.5 | 7985.4 | +12.1% |
| sqlite | durable | map-100-16k | 2879.1 | 2757.2 | +4.2% |
| sqlite | durable | text-overwrite | 2926.7 | 2689.1 | +8.1% |
| sqlite | durable | unique-history | 493.1 | 492.3 | +0.2% |
| sqlite | buffered | chain-10-16k | 345.5 | 305.0 | +11.7% |
| sqlite | buffered | context-append | 986.3 | 890.8 | +9.7% |
| sqlite | buffered | loop-100-16k | 4044.4 | 3816.4 | +5.6% |
| sqlite | buffered | map-100-16k | 1151.7 | 1136.4 | +1.3% |
| sqlite | buffered | text-overwrite | 1547.2 | 1392.5 | +10.0% |
| sqlite | buffered | unique-history | 215.0 | 196.2 | +8.8% |

## SQL 机制验证

各值为 3 个独立进程中位数。下表仅统计 Writer 投影的事实存在性查询，不混入 accepted observer 重放中的保守查询。Node 参数组表示写入行尝试，不能当作逻辑 Node 数。诊断 elapsed 有嵌套，不用于正常速度判断。

| 后端 | 模式 | 场景 | graph 读 A/B | 求值事实读 A/B | Node 参数组 A/B | 总 SQL A/B | SELECT/Event A/B |
| --- | --- | --- | --- | --- | --- | --- | --- |
| postgres | durable | conditional-history | 3280/0 | 256/0 | 2880/1440 | 32115/27238 | 14.40/11.90 |
| postgres | durable | diagnostics-on | 950/0 | 0/0 | 1180/590 | 13342/11761 | 13.45/11.86 |
| postgres | buffered | conditional-history | 3280/0 | 256/0 | 780/392 | 17116/13827 | 10.00/7.82 |
| postgres | buffered | diagnostics-on | 950/0 | 0/0 | 244/121 | 6179/5232 | 7.99/6.49 |
| sqlite | durable | conditional-history | 3280/0 | 256/0 | 2880/1440 | 35751/30747 | 14.66/12.09 |
| sqlite | durable | diagnostics-on | 950/0 | 0/0 | 1180/590 | 14864/13229 | 13.72/12.05 |
| sqlite | buffered | conditional-history | 3280/0 | 256/0 | 876/438 | 19529/16028 | 10.54/8.21 |
| sqlite | buffered | diagnostics-on | 950/0 | 0/0 | 260/130 | 7128/6264 | 8.30/6.96 |

## 记录完整性、存储及专项

每次调用仍校验业务输出，读取前先到 durable 屏障，读取后确认队列排空。以下记录数取各进程中位数；详情及各轮差异见 summary.json。

| 后端 | 模式 | 场景 | Runtime Event A/B | 图变化 A/B | 求值 A/B | Node A/B | 关闭存储 MiB A/B |
| --- | --- | --- | --- | --- | --- | --- | --- |
| postgres | durable | chain-10-16k | 372/372 | 570/570 | 0/0 | 72/72 | 11.40/11.18 |
| postgres | durable | chain-10-16k-formal | 1302/1302 | 1995/1995 | 0/0 | 252/252 | 14.78/15.03 |
| postgres | durable | conditional-history | 1565/1565 | 3485/3485 | 272/272 | 306/306 | 17.40/17.46 |
| postgres | durable | context-append | 608/608 | 1100/1100 | 160/160 | 88/88 | 13.57/13.54 |
| postgres | durable | diagnostics-on | 682/682 | 1045/1045 | 0/0 | 132/132 | 12.47/12.47 |
| postgres | durable | loop-100-16k | 2448/2448 | 4460/4460 | 800/800 | 408/408 | 20.81/20.88 |
| postgres | durable | map-100-16k | 1296/1296 | 150/150 | 0/0 | 18/18 | 13.90/13.83 |
| postgres | durable | text-overwrite | 608/608 | 1100/1100 | 160/160 | 88/88 | 13.76/14.00 |
| postgres | durable | unique-history | 4128/4128 | 6063/6063 | 0/0 | 774/774 | 25.82/25.63 |
| postgres | buffered | chain-10-16k | 372/372 | 570/570 | 0/0 | 72/72 | 11.22/11.16 |
| postgres | buffered | chain-10-16k-formal | 1302/1302 | 1995/1995 | 0/0 | 252/252 | 14.92/14.78 |
| postgres | buffered | conditional-history | 1565/1565 | 3485/3485 | 272/272 | 306/306 | 17.19/17.11 |
| postgres | buffered | context-append | 608/608 | 1100/1100 | 160/160 | 88/88 | 13.58/13.54 |
| postgres | buffered | diagnostics-on | 682/682 | 1045/1045 | 0/0 | 132/132 | 12.53/12.42 |
| postgres | buffered | loop-100-16k | 2448/2448 | 4460/4460 | 800/800 | 408/408 | 21.00/20.52 |
| postgres | buffered | map-100-16k | 1296/1296 | 150/150 | 0/0 | 18/18 | 13.74/13.73 |
| postgres | buffered | text-overwrite | 608/608 | 1100/1100 | 160/160 | 88/88 | 13.71/13.72 |
| postgres | buffered | unique-history | 4128/4128 | 6063/6063 | 0/0 | 774/774 | 25.58/25.18 |
| sqlite | durable | chain-10-16k | 372/372 | 570/570 | 0/0 | 72/72 | 6.78/6.74 |
| sqlite | durable | chain-10-16k-formal | 1302/1302 | 1995/1995 | 0/0 | 252/252 | 12.25/12.29 |
| sqlite | durable | conditional-history | 1565/1565 | 3485/3485 | 272/272 | 306/306 | 15.21/15.35 |
| sqlite | durable | context-append | 608/608 | 1100/1100 | 160/160 | 88/88 | 14.35/14.21 |
| sqlite | durable | diagnostics-on | 682/682 | 1045/1045 | 0/0 | 132/132 | 8.54/8.78 |
| sqlite | durable | loop-100-16k | 2448/2448 | 4460/4460 | 800/800 | 408/408 | 22.38/22.81 |
| sqlite | durable | map-100-16k | 1296/1296 | 150/150 | 0/0 | 18/18 | 11.32/11.17 |
| sqlite | durable | text-overwrite | 608/608 | 1100/1100 | 160/160 | 88/88 | 107.90/107.89 |
| sqlite | durable | unique-history | 4128/4128 | 6063/6063 | 0/0 | 774/774 | 35.03/35.26 |
| sqlite | buffered | chain-10-16k | 372/372 | 570/570 | 0/0 | 72/72 | 6.75/6.76 |
| sqlite | buffered | chain-10-16k-formal | 1302/1302 | 1995/1995 | 0/0 | 252/252 | 13.19/13.16 |
| sqlite | buffered | conditional-history | 1565/1565 | 3485/3485 | 272/272 | 306/306 | 15.64/15.58 |
| sqlite | buffered | context-append | 608/608 | 1100/1100 | 160/160 | 88/88 | 14.23/14.39 |
| sqlite | buffered | diagnostics-on | 682/682 | 1045/1045 | 0/0 | 132/132 | 8.31/8.30 |
| sqlite | buffered | loop-100-16k | 2448/2448 | 4460/4460 | 800/800 | 408/408 | 24.17/25.10 |
| sqlite | buffered | map-100-16k | 1296/1296 | 150/150 | 0/0 | 18/18 | 11.70/11.77 |
| sqlite | buffered | text-overwrite | 608/608 | 1100/1100 | 160/160 | 88/88 | 108.31/107.92 |
| sqlite | buffered | unique-history | 4128/4128 | 6063/6063 | 0/0 | 774/774 | 35.86/36.09 |

## 原始资料

- 本地原始结果、前后源码、工具快照、manifest、验证日志和执行脚本：`.autoagent/performance/v2-optimization/step-01/`。含连接信息的 config 不导出。
- [summary.json](summary.json) 保存各配置的分布、计数和诊断差值；[comparisons.json](comparisons.json) 保存增量变化。
- 本步骤没有运行一小时 soak，没有调整 Artifact 默认值，也没有实现步骤 2。

## 短调用的观察粒度

SQLite unique-history durable 的端到端 p50 仅从 497.0 降至 492.3 ms，而 CPU/调用降低 11.3%。已有数据库中，去除预热后，各进程 Invocation 开始至完成时间间隔的中位数再取中位数，为 465.8 → 407.5 ms。这不是完整 HTTP 或最终提交耗时；它与现有 100 ms 观察轮询可能掩盖短调用收益的解释相符，但不能通过相减中位数得到精确观察耗时。本步没有修改观察语义。

## 正确性与执行记录

- 全部持久化合同测试 71 项通过；新增测试模拟真实 COMMIT 成功后确认丢失，验证重试不重复生成事实，并在 rebuild 后精确比较全部图变化与求值行。
- 新增 Parent/Child 同批写入测试，验证父子外键顺序及同一 batch 每个 Node 只写一次；既有 accepted overlay、Fork、历史检查、Checkpoint、取消和容量边界测试继续通过。
- 全部 98 个 Python 测试模块逐模块独立进程运行，1,682 项通过，PostgreSQL 298 项，0 跳过；避免跨模块 RSS 累积污染 Host admission。
- 开发新测试时修正了“False 分支无输出仍应完成”的负载假设，改为有输出的 True 条件；没有因此更改产品分支语义。
- 逐条核对 4,952 条结果、232 个最终屏障，确认无拒绝、超时或 RSS guard；前后核对的五张表记录数中位数一致。产品源码差异仅为 `projections.py`、`storage.py`，当前源码与 B 快照一致。
- 实际源码、工具 hash、完整模块验证清单及内部时间统计见 [provenance.json](provenance.json)。配置含本地数据库连接的原始文件仅保存在忽略目录。

主要复现入口（仓库根目录、根 venv）：

```bash
.venv/bin/python -m unittest tests.test_host_persistence -v
.venv/bin/python .autoagent/performance/v2-optimization/step-01/verify.py
.venv/bin/python .autoagent/performance/v2-optimization/step-01/execute.py
.venv/bin/python .autoagent/performance/v2-optimization/step-01/execute-formal.py
.venv/bin/python .autoagent/performance/v2-optimization/step-01/report.py
.venv/bin/python .autoagent/performance/v2-optimization/step-01/validate.py
.venv/bin/python -m compileall -q autoagent tests
git diff --check
```

执行器写入已有结果路径时会覆盖文件；正式复测先为脚本配置新的实验根目录，并保存同样的 before/after 源码及工具快照，不能覆盖本次证据。原始执行命令与 manifest 位于本地 step-01 目录。上述入口属于本次实验产物，未作为产品命令发布。

步骤 2 尚未开始，等待用户下一步指令。
