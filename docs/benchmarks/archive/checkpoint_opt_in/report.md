# Lifecycle checkpoint opt-in

Automatic Core capture has two lifecycle entry points: `unload_session`/`aunload_session` and `close`/`aclose`. All now default to `capture_checkpoint=False` and return `None`. Passing `capture_checkpoint=True` returns a SessionCheckpoint or AppCheckpoint. The first admitted close operation selects the option; concurrent/repeated callers share its result, including None. Capture cannot be requested retroactively after closing without it.

Default unload bypasses `_capture_checkpoint_locked` and Repository capture. Default close returns before checkpoint iteration or AppCheckpoint construction. Both retain pending Event settlement and lifecycle cleanup. Explicit Repository capture and the hosting SQLite checkpoint-rebuild path are separate explicit operations; they were not changed.

## Measurement

`benchmark.json`: five alternating enabled/disabled pairs, same code and workloads; preparation excluded from timing. The enabled option preserves prior capture behavior. Allocations are measured separately with tracemalloc, with GC before retained-memory sampling. Public API timings include runtime-loop dispatch and, for close, shutdown. This isolates the capture option rather than comparing unrelated commits. Absolute times are local measurements and include scheduling noise.

| Workload | Capture enabled ms | Default disabled ms | Time reduction | Repository capture calls |
| --- | ---: | ---: | ---: | ---: |
| close_100 | 2.866 | 0.477 | 83.4% | 100 → 0 |
| close_1000 | 24.020 | 0.937 | 96.1% | 1000 → 0 |
| unload_100 | 13.789 | 9.835 | 28.7% | 100 → 0 |

The reduction applies to lifecycle calls, not Workflow execution throughput. Checkpoints already share immutable Runtime State, so this removes snapshot wrappers, validation and collection work rather than eliminating a deep copy of all data.

## Validation

Seven new tests verify sync/async defaults, bypass of all capture methods and empty bundle construction, exact pending Event ACK retry, enabled load/resume, cached close results and concurrent mixed options. Existing checkpoint tests and the Core example now request capture explicitly.

Full discovery: 368 entries, 360 passed, 8 unchanged peripheral import errors (`InvocationOpened` / `InMemoryEventJournal` imports in Host/Tracing-related modules). Full log: `tests.log`. Compilation and `git diff --check` pass.

```python
app.unload_session(ref)  # None
checkpoint = app.unload_session(other_ref, capture_checkpoint=True)
app.close()  # None
# Or, on the first close call:
# checkpoint = app.close(capture_checkpoint=True)
```

Reproduce from the repository root:

```bash
.venv/bin/python -m tests.benchmarks.benchmark_checkpoint_opt_in
.venv/bin/python -m unittest tests.test_checkpoint_opt_in -v
```
