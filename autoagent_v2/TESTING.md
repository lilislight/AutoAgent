# V2 Core 测试结构

测试按 Core 从静态定义到公共入口的依赖方向分层。下层失败时先修复下层，不通过
在上层重复模拟来绕过问题。

| 测试文件 | 主要验证内容 |
| --- | --- |
| `test_phase1_contracts.py` | TypedDict/Pydantic 契约、Operator、Wait |
| `test_phase1_compiler.py` | Workflow 编译、诊断、稳定 Revision、Portable Snapshot、SubWorkflow、Map、Stream、Child Workflow |
| `test_phase1_loops.py` | 自然 Loop 的编译、嵌套和非法图结构 |
| `test_phase2_runtime_state.py` | Runtime Event、Reducer、Journal、任意前缀重放 |
| `test_phase3_scheduler.py` | DAG、分支、Fan-out、完整 Fan-in、不可达传播 |
| `test_phase4_loop_scheduler.py` | scoped NodeOccurrence、Loop 边界和序列化重放 |
| `test_phase5_executor.py` | NodeExecutor、Map、Retry、Fallback、Timeout、Stream |
| `test_phase6_context.py` | Mapping、Binding、Condition、Context 原子提交和冲突 |
| `test_phase7_wait_recovery.py` | Wait、Resume、Cancel 和进程恢复状态转换 |
| `test_phase8_app.py` | 端到端 DAG/Loop、Child Map 四种组合、Recovery 与同步异步入口 |
| `test_phase9_quality.py` | Port、Capability、失败策略、预算、取消、增量 Event |
| `test_phase10_boundaries.py` | 严格配置边界、TaskRuntime、RuntimeLoop 和持久化往返 |

## 测试说明规则

每个 `test_*` 方法必须在方法体第一行包含一句简短 docstring，直接说明它验证的
行为或失败边界。测试名称负责定位，docstring 负责解释契约；不能只写“测试正常
工作”。新增回归测试应先证明旧实现会失败，再锁定修复后的外部可观察行为。

## 验证命令

```bash
../.venv/bin/python -m unittest discover -s tests -v
../.venv/bin/python -m compileall -q autoagent tests
git diff --check
```

Full 模式性能基准单独运行：

```bash
../.venv/bin/python -m tests.benchmarks.benchmark_full_core
```
