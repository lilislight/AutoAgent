# Two-Loop One-Way Persistence Results

Recorded on 2026-07-23 on branch
`experiment/one-way-persistence-queue`, after replacing ordinary per-Event
cross-loop request/response with one-way callback submission. Durability
barriers still wait for the persistence worker.

- Baseline commit: `7960d95`
- Python environment: repository-root virtual environment
- Full test suite: 226 tests passed, 1 PostgreSQL integration test skipped
- Full test elapsed time: 29.903 seconds

Absolute timings depend on the host and current system load. These results used
the same host and commands as
`2026-07-23-two-loop-awaited-baseline.md`.

## Invocation latency

Command:

```bash
python -m benchmarks.runtime_store_benchmark \
  --nodes 30 --invocations 50 --json
```

```json
[
  {
    "backend": "memory",
    "node_count": 30,
    "invocation_count": 50,
    "median_invoke_ms": 101.58982052234933,
    "p95_invoke_ms": 141.5095250122249,
    "invocations_per_second": 9.352885656040185,
    "flush_ms": 0.0016450067050755024,
    "events_per_invocation": 151
  },
  {
    "backend": "sqlite",
    "node_count": 30,
    "invocation_count": 50,
    "median_invoke_ms": 274.89055148907937,
    "p95_invoke_ms": 326.53718697838485,
    "invocations_per_second": 3.5904640699056762,
    "flush_ms": 62.8309590392746,
    "events_per_invocation": 151
  }
]
```

Compared with the awaited two-loop baseline:

| Metric | Awaited baseline | One-way | Change |
| --- | ---: | ---: | ---: |
| SQLite median Invocation | 419.57 ms | 274.89 ms | -34.5% |
| SQLite p95 Invocation | 473.80 ms | 326.54 ms | -31.1% |
| SQLite throughput | 2.37 inv/s | 3.59 inv/s | +51.4% |
| Post-return SQLite flush | 46.34 ms | 62.83 ms | +35.6% |
| Memory median Invocation | 101.68 ms | 101.59 ms | -0.1% |

The larger final flush is expected: ordinary execution no longer waits for
Event preparation on the persistence loop, so more accepted work may remain
when the last Invocation returns.

## Normally consuming SQLite backlog

Command:

```bash
python -m benchmarks.persistence_backlog_benchmark \
  --database-mode normal --invocations 20 --nodes 10 --payload-bytes 1024
```

```json
{
  "configuration": {
    "database_mode": "normal",
    "invocations": 20,
    "nodes_per_invocation": 10,
    "payload_bytes_per_node_output": 1024,
    "recovery_event_interval": 200,
    "queue_low_watermark_bytes": 524288,
    "queue_high_watermark_bytes": 1048576,
    "queue_hard_watermark_bytes": 268435456
  },
  "execution_backlog": {
    "boundary_event_count": 1020,
    "peak_persistence_item_count": 817,
    "peak_accounted_queue_bytes": 2262340,
    "persistence_item_count_after_execution": 817,
    "accounted_queue_bytes_after_execution": 2262340,
    "average_accounted_bytes_per_item": 2769.0820073439413,
    "production_elapsed_seconds": 1.8688738680211827,
    "peak_accounted_queue_bytes_per_second": 1210536.4833397935,
    "high_watermark_crossed_at_seconds": 0.7597666840301827,
    "hard_watermark_crossed_at_seconds": null,
    "traced_heap_delta_bytes": 11544567,
    "traced_peak_heap_delta_bytes": 11787389,
    "admission_paused": true,
    "new_admission_rejected": true,
    "new_admission_error": "Runtime persistence backlog is above the admission watermark."
  },
  "after_release": {
    "flush_seconds": 0.416732456011232,
    "pending_persistence_count": 0,
    "pending_persistence_bytes": 0,
    "traced_heap_delta_after_flush_bytes": 7606563
  },
  "database": {
    "database_event_count": 1020,
    "database_event_json_bytes": 2392074,
    "database_invocation_count": 20,
    "database_genesis_json_bytes": 27770,
    "database_recovery_state_count": 0,
    "database_recovery_state_json_bytes": 0,
    "database_artifact_count": 0,
    "database_artifact_payload_bytes": 0,
    "database_page_bytes": 3563520,
    "database_file_bytes": 3563520
  }
}
```

The normal-consumer peak changed from 833 to 817 items and from 2,292,577 to
2,262,340 accounted bytes. Both versions crossed the deliberately small 1 MiB
admission watermark. Production elapsed time was 1.697 seconds in the baseline
and 1.869 seconds in this single run; this benchmark enables `tracemalloc` and
runs concurrent Invocations, so repeated sampling is required before treating
the difference as a stable regression.

## Blocked SQLite backlog

Command:

```bash
python -m benchmarks.persistence_backlog_benchmark \
  --database-mode blocked --invocations 20 --nodes 10 --payload-bytes 1024
```

```json
{
  "configuration": {
    "database_mode": "blocked",
    "invocations": 20,
    "nodes_per_invocation": 10,
    "payload_bytes_per_node_output": 1024,
    "recovery_event_interval": 200,
    "queue_low_watermark_bytes": 524288,
    "queue_high_watermark_bytes": 1048576,
    "queue_hard_watermark_bytes": 268435456
  },
  "execution_backlog": {
    "boundary_event_count": 1020,
    "peak_persistence_item_count": 1020,
    "peak_accounted_queue_bytes": 2653213,
    "persistence_item_count_after_execution": 1020,
    "accounted_queue_bytes_after_execution": 2653213,
    "average_accounted_bytes_per_item": 2601.1892156862746,
    "production_elapsed_seconds": 1.596364633005578,
    "peak_accounted_queue_bytes_per_second": 1662034.4407183626,
    "high_watermark_crossed_at_seconds": 0.6777794259833172,
    "hard_watermark_crossed_at_seconds": null,
    "traced_heap_delta_bytes": 10437963,
    "traced_peak_heap_delta_bytes": 10678335,
    "admission_paused": true,
    "new_admission_rejected": true,
    "new_admission_error": "Runtime persistence backlog is above the admission watermark."
  },
  "after_release": {
    "flush_seconds": 0.6681305580423214,
    "pending_persistence_count": 0,
    "pending_persistence_bytes": 0,
    "traced_heap_delta_after_flush_bytes": 8244030
  },
  "database": {
    "database_event_count": 1020,
    "database_event_json_bytes": 2392093,
    "database_invocation_count": 20,
    "database_genesis_json_bytes": 27770,
    "database_recovery_state_count": 0,
    "database_recovery_state_json_bytes": 0,
    "database_artifact_count": 0,
    "database_artifact_payload_bytes": 0,
    "database_page_bytes": 3514368,
    "database_file_bytes": 3514368
  }
}
```

With writes blocked, both versions retained all 1,020 Events and approximately
2.65 MB of accounted queue data. The one-way transport changes execution
latency, not the amount of durable information produced.
