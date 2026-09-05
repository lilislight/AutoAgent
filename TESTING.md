# V2 Core 测试结构

测试按 Core 从静态定义到公共入口的依赖方向分层。下层失败时先修复下层，不通过
在上层重复模拟来绕过问题。

| 测试文件 | 主要验证内容 |
| --- | --- |
| `test_phase1_contracts.py` | TypedDict/Pydantic 契约、Operator、Wait |
| `test_phase1_compiler.py` | Workflow 编译、诊断、稳定 Revision、Portable Snapshot、SubWorkflow、Map、Stream、Child Workflow |
| `test_phase1_loops.py` | 自然 Loop 的编译、嵌套和非法图结构 |
| `test_phase2_runtime_state.py` | StateOperation、Reducer、RuntimeEvent 捕获、前缀重建 |
| `test_phase3_scheduler.py` | DAG、分支、Fan-out、完整 Fan-in、不可达传播 |
| `test_phase4_loop_scheduler.py` | scoped NodeOccurrence、Loop 边界和序列化重放 |
| `test_phase5_executor.py` | NodeExecutor、Map、有序聚合、OperatorCall、StreamReducer |
| `test_phase6_context.py` | Mapping、Binding、Condition、Context 原子提交和冲突 |
| `test_phase7_wait_recovery.py` | Wait、Resume、Cancel 和进程恢复状态转换 |
| `test_phase8_app.py` | 同步/异步 App、严格流背压、Loop、Child 四组合、Checkpoint Recovery |
| `test_phase9_quality.py` | 全局并发、Port、Capability、UserEvent、关闭、失败模式与公开边界 |
| `test_phase10_boundaries.py` | 严格定义、codec、TaskRuntime、RuntimeLoop、Host RuntimeEvent sink |
| `test_phase11_app_correctness.py` | 流式安全 Checkpoint、sink 顺序、Child Root 与 Session 互斥回归 |
| `test_phase12_runtime_boundaries.py` | 并行/Loop/Child 竞态、失败收敛和 Pydantic durable 边界 |
| `test_phase13_lifecycle_recovery.py` | Stream/close 生命周期、RuntimeLoop 线性化、sink 失败和图恢复 |
| `test_phase14_contract_roundtrip.py` | Enum/tuple 契约在 Edge、Wait、Child 与 Checkpoint 间往返 |
| `test_phase14_runtime_state_validation.py` | RuntimeState 数值、身份、状态和引用不变量 |
| `test_runtime_checkpoint_trace.py` | RuntimeEvent 链、Trace 投影、Checkpoint 图校验与原子加载 |
| `test_executor_hardening.py` | Pydantic 边界、回调失败、同步 Stream 取消与并发名额收敛 |
| `test_host_project.py` | `autoagent.toml`、`.env`、Settings、Workflow entrypoint 与稳定诊断 |
| `test_host_runtime_store.py` | SQLite Event 原子写入、hash chain、Trace、父子重建与 HTTP Sink |
| `test_host_lifecycle.py` | Host 装配、同步/异步执行、恢复、revision 和 App→Sink 关闭顺序 |
| `test_tracing_server.py` | 只读 API、Host/静态响应安全、cursor/tail、历史 State、SSE 恢复、通知与 heartbeat |
| `test_cli.py` | compile 无副作用、invoke JSON 边界、tracing server 装配与退出清理 |

## 测试说明规则

每个 `test_*` 方法必须在方法体第一行包含一句简短 docstring，直接说明它验证的
行为或失败边界。测试名称负责定位，docstring 负责解释契约；不能只写“测试正常
工作”。新增回归测试应先证明旧实现会失败，再锁定修复后的外部可观察行为。

## 验证命令

```bash
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m compileall -q autoagent tests
git diff --check
```

Tracing UI 独立验证：

```bash
cd ui
npm test
npm run build
```

性能基准单独运行。它通过 Host sink 统计 canonical RuntimeEvent，并分别统计 Trace、
Checkpoint 大小，不依赖公开结果中的内部状态日志：

```bash
.venv/bin/python -m tests.benchmarks.benchmark_full_core
```
