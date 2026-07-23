# Two-Loop Awaited Persistence Baseline

Recorded on 2026-07-23 before replacing per-Event cross-loop request/response
with one-way persistence submission.

- Git commit before recording: `2b1f95e`
- Python environment: repository-root `uv` environment
- Full test suite: 225 tests passed, 1 PostgreSQL integration test skipped
- Full test elapsed time: 29.506 seconds

Absolute timings depend on the host and current system load. Compare these
results with the same commands on the same host.

## Invocation latency

Command:

```bash
UV_CACHE_DIR=/tmp/autoagent-uv-cache \
  uv run python -m benchmarks.runtime_store_benchmark \
  --nodes 30 --invocations 50 --json
```

```json
[
  {
    "backend": "memory",
    "node_count": 30,
    "invocation_count": 50,
    "median_invoke_ms": 101.6802144877147,
    "p95_invoke_ms": 133.7404089863412,
    "invocations_per_second": 9.414374716123652,
    "flush_ms": 0.0015569967217743397,
    "events_per_invocation": 151
  },
  {
    "backend": "sqlite",
    "node_count": 30,
    "invocation_count": 50,
    "median_invoke_ms": 419.5743735181168,
    "p95_invoke_ms": 473.80236198659986,
    "invocations_per_second": 2.372123588963307,
    "flush_ms": 46.342451008968055,
    "events_per_invocation": 151
  }
]
```

## Normally consuming SQLite backlog

Command:

```bash
UV_CACHE_DIR=/tmp/autoagent-uv-cache \
  uv run python -m benchmarks.persistence_backlog_benchmark \
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
    "peak_persistence_item_count": 833,
    "peak_accounted_queue_bytes": 2292577,
    "persistence_item_count_after_execution": 833,
    "accounted_queue_bytes_after_execution": 2292577,
    "average_accounted_bytes_per_item": 2752.1932773109243,
    "production_elapsed_seconds": 1.6970262420363724,
    "peak_accounted_queue_bytes_per_second": 1350937.860129368,
    "high_watermark_crossed_at_seconds": 0.7526638780254871,
    "hard_watermark_crossed_at_seconds": null,
    "traced_heap_delta_bytes": 10912827,
    "traced_peak_heap_delta_bytes": 11329059,
    "admission_paused": true,
    "new_admission_rejected": true,
    "new_admission_error": "Runtime persistence backlog is above the admission watermark."
  },
  "after_release": {
    "flush_seconds": 0.41471139597706497,
    "pending_persistence_count": 0,
    "pending_persistence_bytes": 0,
    "traced_heap_delta_after_flush_bytes": 7667776
  },
  "database": {
    "database_event_count": 1020,
    "database_event_json_bytes": 2392125,
    "database_invocation_count": 20,
    "database_genesis_json_bytes": 27770,
    "database_recovery_state_count": 0,
    "database_recovery_state_json_bytes": 0,
    "database_artifact_count": 0,
    "database_artifact_payload_bytes": 0,
    "database_page_bytes": 3543040,
    "database_file_bytes": 3543040,
    "database_event_json_bytes_by_type": {
      "invocation.completed": {
        "count": 20,
        "json_bytes": 26660
      },
      "node.activation_ready": {
        "count": 200,
        "json_bytes": 455820
      },
      "node.committed": {
        "count": 200,
        "json_bytes": 405380
      },
      "node.input_ready": {
        "count": 200,
        "json_bytes": 298180
      },
      "node.output_ready": {
        "count": 200,
        "json_bytes": 740665
      },
      "routing.committed": {
        "count": 200,
        "json_bytes": 465420
      }
    }
  }
}
```

## Blocked SQLite backlog

Command:

```bash
UV_CACHE_DIR=/tmp/autoagent-uv-cache \
  uv run python -m benchmarks.persistence_backlog_benchmark \
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
    "peak_accounted_queue_bytes": 2653092,
    "persistence_item_count_after_execution": 1020,
    "accounted_queue_bytes_after_execution": 2653092,
    "average_accounted_bytes_per_item": 2601.070588235294,
    "production_elapsed_seconds": 1.4474211449851282,
    "peak_accounted_queue_bytes_per_second": 1832978.6110919775,
    "high_watermark_crossed_at_seconds": 0.6035702929948457,
    "hard_watermark_crossed_at_seconds": null,
    "traced_heap_delta_bytes": 10306600,
    "traced_peak_heap_delta_bytes": 10620606,
    "admission_paused": true,
    "new_admission_rejected": true,
    "new_admission_error": "Runtime persistence backlog is above the admission watermark."
  },
  "after_release": {
    "flush_seconds": 0.6768674000049941,
    "pending_persistence_count": 0,
    "pending_persistence_bytes": 0,
    "traced_heap_delta_after_flush_bytes": 8218334
  },
  "database": {
    "database_event_count": 1020,
    "database_event_json_bytes": 2391972,
    "database_invocation_count": 20,
    "database_genesis_json_bytes": 27770,
    "database_recovery_state_count": 0,
    "database_recovery_state_json_bytes": 0,
    "database_artifact_count": 0,
    "database_artifact_payload_bytes": 0,
    "database_page_bytes": 3510272,
    "database_file_bytes": 3510272,
    "database_event_json_bytes_by_type": {
      "invocation.completed": {
        "count": 20,
        "json_bytes": 26660
      },
      "node.activation_ready": {
        "count": 200,
        "json_bytes": 455820
      },
      "node.committed": {
        "count": 200,
        "json_bytes": 405380
      },
      "node.input_ready": {
        "count": 200,
        "json_bytes": 298180
      },
      "node.output_ready": {
        "count": 200,
        "json_bytes": 740512
      },
      "routing.committed": {
        "count": 200,
        "json_bytes": 465420
      }
    }
  }
}
```
