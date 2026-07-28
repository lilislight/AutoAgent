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
