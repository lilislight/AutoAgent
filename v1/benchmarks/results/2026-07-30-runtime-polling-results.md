# Runtime polling benchmark

This comparison measures the 2026-07-30 replacement of two active 1 ms polling
paths:

- `RuntimeEventLoop` previously kept a 1 ms compatibility pulse active for the
  complete lifetime of submitted Workflow work.
- `RuntimeConcurrencyController` previously retried every waiting slot at 1 ms
  intervals.

Both runs used the same checkout, host, Python environment, and command:

```bash
python -m benchmarks.runtime_polling_benchmark \
  --repeats 5 \
  --operation-ms 1000 \
  --waiters 250 \
  --hold-ms 1 \
  --dispatches 500 \
  --json
```

## Results

| Case | Metric | Before | After | Change |
| --- | ---: | ---: | ---: | ---: |
| Long Runtime operation | Median wall time | 1064.11 ms | 1006.22 ms | -5.44% |
| Long Runtime operation | Median CPU time | 81.43 ms | 5.45 ms | -93.30% |
| Slot contention | Median wall time | 441.33 ms | 287.95 ms | -34.75% |
| Slot contention | Median CPU time | 176.44 ms | 16.06 ms | -90.90% |
| Cross-thread dispatch | Median wall time, 500 calls | 25261.07 ms | 25210.24 ms | -0.20% |
| Cross-thread dispatch | Median CPU time | 364.30 ms | 198.98 ms | -45.38% |
| Cross-thread dispatch | p95 per-call latency | 51.01 ms | 50.56 ms | -0.88% |

The optimized implementation uses an `asyncio.Semaphore` to notify slot
waiters. The Runtime compatibility watchdog now runs at 5 ms only until a
cross-thread submission is accepted, then returns to its 50 ms lost-wakeup
fallback while the submitted coroutine runs.

Synchronous Operator and synchronous stream completion use a separate 5 ms
fallback only while an executor Future is outstanding. This preserves correct
completion latency on hosts that lose thread-pool wakeups without re-enabling
the Runtime-wide active-work pulse. Database persistence uses the same scoped
5 ms interval only while a database batch is waiting on driver callbacks.

The cross-thread dispatch case is intentionally unchanged: it isolates the
separate 50 ms caller-side lost-wakeup fallback in
`_await_concurrent_future()`. Its stable result shows that this change removes
active-work polling without claiming an unrelated cross-thread latency
improvement.
