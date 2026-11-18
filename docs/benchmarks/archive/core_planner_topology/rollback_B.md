# B 回退记录

按用户要求回退 WorkflowIR 的 Loop 预计算查询表、成员集合，以及 LoopScheduler 的对应调用。两个生产文件已恢复为本轮 A/B 改动前的 HEAD 内容。

A 的 Repository/TransitionPlanner 计数优化及四项相关测试保留。移除两项仅用于 B 的索引测试；Benchmark 脚本和此前所有测量数据保留。REPORT.md 已标明历史结果，after.json 和 source_manifest.json 仍描述回退前的 A+B 版本，不代表当前代码。

C 未修改。用户的两个设计文档未修改。

回退后验证：

- 全量发现 350 项：342 通过，8 个既有外围模块导入错误，没有新增失败。
- Core 与相关测试/Benchmark 编译通过。
- git diff --check 通过，生产代码差异仅剩 A 的两个文件。

完整测试输出见 rollback_B_tests.log。本轮未重新测量 A-only 性能，不能将此前 A+B 的端到端结果当成回退后实测值。
