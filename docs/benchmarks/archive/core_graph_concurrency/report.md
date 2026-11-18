# Parent/Child Session concurrency benchmark

## Scope and method

Measured on 2026-09-16. The baseline is the source snapshot taken before this refactor, including the already accepted Planner A change and the B rollback. C remains unchanged. `metadata.json` records the environment and source hashes.

`before.json` / `after.json`: three samples per case, median wall time in milliseconds. Graph creation and close are outside the timed region. `children` resumes independent waiting Child Sessions in one Root and waits for the parent boundary; `roots` resumes independent Root Sessions; `nodes` maps Operators inside one Session. Delays are seconds per Sink append, simulated using `asyncio.sleep`. The Sink retains no Events. All before/after cases have the same Event counts. Timings exclude tracemalloc and were run without concurrent test suites.

Sub-millisecond sleeps are affected by event-loop/OS timer resolution. These are local workload measurements, not database throughput predictions. Parent Event streams and same-Session Map Events remain serialized.

## Graph timings

| Case (kind_count_delay_seconds) | Before ms | After ms | Before/after | Peak overlapping ACKs |
| --- | ---: | ---: | ---: | ---: |
| roots_1_0 | 2.26 | 2.25 | 1.01x | 1 → 1 |
| roots_1_0.0005 | 5.82 | 5.61 | 1.04x | 1 → 1 |
| roots_1_0.002 | 8.84 | 8.64 | 1.02x | 1 → 1 |
| roots_1_0.01 | 33.71 | 33.08 | 1.02x | 1 → 1 |
| roots_32_0 | 68.69 | 67.76 | 1.01x | 1 → 1 |
| roots_32_0.0005 | 73.83 | 65.65 | 1.12x | 32 → 32 |
| roots_32_0.002 | 67.17 | 63.59 | 1.06x | 32 → 32 |
| roots_32_0.01 | 80.96 | 83.85 | 0.97x | 32 → 32 |
| nodes_32_0 | 7.52 | 7.93 | 0.95x | 1 → 1 |
| nodes_32_0.0005 | 105.22 | 87.83 | 1.20x | 1 → 1 |
| nodes_32_0.002 | 177.90 | 158.86 | 1.12x | 1 → 1 |
| nodes_32_0.01 | 785.52 | 767.30 | 1.02x | 1 → 1 |
| children_32_0 | 66.15 | 67.76 | 0.98x | 1 → 1 |
| children_32_0.0005 | 199.15 | 103.56 | 1.92x | 1 → 32 |
| children_32_0.002 | 366.91 | 154.71 | 2.37x | 1 → 32 |
| children_32_0.01 | 1516.49 | 449.90 | 3.37x | 1 → 32 |
| children_100_0 | 205.04 | 202.23 | 1.01x | 1 → 1 |
| children_100_0.0005 | 629.66 | 353.06 | 1.78x | 1 → 100 |
| children_100_0.002 | 1097.34 | 428.78 | 2.56x | 1 → 100 |
| children_100_0.01 | 4436.66 | 1261.08 | 3.52x | 1 → 100 |

## No-delay execution overhead

`execution_paired.json`: three alternating before/after process pairs. Each measurement warms up, takes five timing samples, then separately measures allocations. The table uses the median of the three per-process timing medians. These costs must not be described as a no-I/O speedup.

| Workload | Before ms | After ms | Time change |
| --- | ---: | ---: | ---: |
| chain_50000 | 63.39 | 64.49 | +1.74% |
| repeat_100 | 80.22 | 83.72 | +4.37% |
| loop_100 | 95.75 | 99.29 | +3.70% |

The extra Session lane and graph admission checks cost approximately 1.7–4.4% in these no-delay workloads. The reusable shared barrier removes per-transition context-manager allocation; wake Events are allocated only when contention actually requires them.

## Graph memory

`graph_memory_before.json` / `graph_memory_after.json`: separate tracemalloc runs, after warmup and ten sequential replacements of the same Root Invocation. Figures include current Runtime State and coordination structures, not just lock objects. GC runs before retained memory is sampled. Both versions retain exactly one current graph; old Child Sessions do not accumulate.

| Children | Before retained bytes | After retained bytes | Before peak bytes | After peak bytes | Resident Sessions |
| --- | ---: | ---: | ---: | ---: | ---: |
| 32 | 208,937 | 211,598 | 953,174 | 956,058 | 33 |
| 100 | 656,399 | 660,529 | 2,297,890 | 2,249,536 | 101 |

Retained memory increased by about 2.6 KiB / 4.0 KiB for 32 / 100 Children in these runs. Peak allocation is similar; the lower 100-Child peak is not claimed as a general memory optimization.

## Correctness validation

- 11 new tests cover shared admission, writer preference, cancelled waiters, exact Event retry, same-Session Scheduler planning, independent sibling ACKs, parent/child terminal ordering, admission versus cancellation, duplicate finalizers, parallel Child failures, checkpoint/load/unload boundaries, and recovery versus resume/cancel.
- Recorded Event streams replay to the acknowledged Runtime State; derived execution indexes match reconstructed state.
- Full discovery: 361 entries, 353 passed, 8 pre-existing peripheral import errors. See `tests_after.log`. Before refactoring: 350 entries, 342 passed, the same 8 import errors.
- Existing failures: `test_child_persistence_integrity`, `test_cli`, `test_host_lifecycle`, `test_host_project`, `test_host_runtime_store`, `test_runtime_checkpoint_trace`, `test_tracing_server`, `test_user_event_persistence`. They still import removed Core APIs such as `InvocationOpened` / `InMemoryEventJournal`; those modules are outside this Core-only change.
- `python -m compileall -q autoagent tests` and `git diff --check` passed.

## Reproduction

From the repository root, use the root virtual environment:

```bash
.venv/bin/python -m tests.benchmarks.benchmark_core_graph_concurrency
.venv/bin/python -m tests.benchmarks.benchmark_core_graph_concurrency --memory
.venv/bin/python -m tests.benchmarks.benchmark_core_execution
.venv/bin/python -m unittest tests.test_core_graph_concurrency -v
```

The baseline measurements loaded `autoagent/` from the pre-refactor snapshot recorded in `metadata.json`, while using the same benchmark drivers. No branch, worktree, persistence schema, Event format, or Host/Tracing implementation was changed.
