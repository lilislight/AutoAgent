# Runtime benchmarks

The normal test suite contains generous smoke budgets and structural payload
checks. Use this benchmark for actual latency and throughput measurements:

```bash
python -m benchmarks.runtime_store_benchmark
```

The default case executes ten 30-node chain Invocations against both the
memory-only RuntimeStore and a temporary SQLite DatabaseBackend. It reports
median and p95 Invocation latency, throughput, post-return flush time, and
events per Invocation.

Use JSON output when recording comparable results from the same host:

```bash
python -m benchmarks.runtime_store_benchmark \
  --nodes 30 --invocations 50 --json
```

Absolute results vary by hardware and system load. Compare commits on the same
host and configuration; the unittest smoke budgets are meant to catch hangs or
order-of-magnitude regressions, not small timing changes.

## Runtime scheduling overhead

To measure Runtime compatibility-pulse CPU use, concurrency-slot contention,
and cross-thread dispatch latency, run:

```bash
python -m benchmarks.runtime_polling_benchmark
```

Keep `--operation-ms`, `--waiters`, `--hold-ms`, and `--dispatches` identical
for before/after comparisons. The cross-thread case also measures the separate
lost-wakeup fallback, so it should not be used as a proxy for long Operator
execution overhead.

## UserEvent overhead

To compare the same streaming Node with no UserEvent, one final UserEvent,
per-chunk UserEvents, and both stream/final UserEvents, run:

```bash
python -m benchmarks.user_event_benchmark
```

The benchmark always uses the memory RuntimeStore and `minimal` Runtime Event
mode so Runtime tracing and database persistence do not obscure UserEvent
cost. It reports Invocation latency, throughput, UserEvents per Invocation,
retained JSON bytes, Runtime Event count, and persistence-queue size.

Use `--concurrency` to measure competing Invocations and `--output` to retain
the complete machine-readable result:

```bash
python -m benchmarks.user_event_benchmark \
  --invocations 100 --repeats 5 --concurrency 16 \
  --chunks 64 --chunk-bytes 32 \
  --output benchmarks/results/user-event.json
```

UserEvents are process-local in this implementation. The benchmark verifies
that they add neither Runtime Events nor persistence-queue items; its byte
measurement is the compact JSON representation retained for one Invocation,
not Python object heap size.

To measure queue growth when the database cannot consume events, run:

```bash
python -m benchmarks.persistence_backlog_benchmark \
  --invocations 20 --nodes 10 --payload-bytes 0
```

This benchmark durably admits every Invocation first, blocks SQLite event
writes, runs all Invocations, and reports queued item count, accounted queue
bytes, production rate, traced Python heap growth, admission backpressure, and
the final SQLite JSON/file sizes after releasing the writer.

Pass `--database-mode normal` to measure the peak queue while SQLite consumes
events normally. Pass `--event-mode minimal`, `standard`, or `full` to compare
the three Invocation-level persistence profiles with the same workload.
