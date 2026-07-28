# UserEvent performance benchmark

This benchmark measures the incremental cost of UserEvent generation on the
same streaming Workflow. It was run from the current working tree based on
commit `acaab8b`.

## Test structure

Every case executes one Node that consumes the same number and size of stream
chunks through `StreamingResult`. Its reducer retains only the final chunk and
byte counts. The only variable is the Node's UserEvent configuration:

| Case | Stream mapping | Completion mapping | Expected Events |
| --- | --- | --- | ---: |
| `none` | No | No | 0 |
| `completed` | No | Yes | 1 |
| `stream` | Yes | No | chunks |
| `stream_and_completed` | Yes | Yes | chunks + 1 |

```text
identical chunk source
        |
        v
StreamingResult -> reducer -> final Node output
        |
        +-- none: no UserEvent
        +-- completed: final output -> one UserEvent
        +-- stream: every chunk -> one UserEvent
        +-- stream_and_completed: both paths
```

All cases use:

- the memory RuntimeStore;
- `minimal` Runtime Event mode;
- one warm App and compiled Workflow per trial;
- a unique Session for every Invocation;
- rotated case order across repeats;
- 32-byte chunk payloads;
- compact JSON size as the retained-size estimate.

This isolates UserEvent transformation, serialization, sequencing, and
in-memory retention from Runtime tracing and database latency.

## Environment

- Python 3.12.13
- Linux 5.15 x86-64
- 4 logical CPUs
- 8 GiB memory

## 64-chunk results

### Sequential Invocations

Configuration: 5 repeats, 30 measured Invocations per repeat, 3 warmups per
case, concurrency 1, 64 chunks per Invocation.

| Case | Median | p95 | Throughput | Change vs none | Events/Invocation | Event JSON/Invocation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `none` | 50.610 ms | 101.526 ms | 17.89 inv/s | baseline | 0 | 0 B |
| `completed` | 50.546 ms | 100.790 ms | 18.27 inv/s | +2.17% throughput | 1 | 372 B |
| `stream` | 50.541 ms | 102.781 ms | 16.87 inv/s | -5.69% throughput | 64 | 24,056 B |
| `stream_and_completed` | 50.496 ms | 104.370 ms | 16.41 inv/s | -8.24% throughput | 65 | 24,428 B |

The current Runtime has an approximately 50 ms scheduling floor for these
small sequential Invocations. That floor hides the UserEvent CPU cost in the
median, so throughput is the more useful sequential metric.

### Concurrent Invocations

Configuration: 5 repeats, 96 measured Invocations per repeat, 3 warmups per
case, concurrency 16, 64 chunks per Invocation.

| Case | Median | p95 | Throughput | Change vs none | Events/Invocation | Event JSON/Invocation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `none` | 51.857 ms | 109.101 ms | 232.77 inv/s | baseline | 0 | 0 B |
| `completed` | 51.799 ms | 107.181 ms | 233.09 inv/s | +0.14% throughput | 1 | 372 B |
| `stream` | 121.565 ms | 181.114 ms | 115.66 inv/s | -50.31% throughput | 64 | 24,056 B |
| `stream_and_completed` | 152.762 ms | 181.554 ms | 110.70 inv/s | -52.44% throughput | 65 | 24,428 B |

The difference between `stream` and `stream_and_completed` should not be
interpreted as one final Event adding 31 ms. The `completed` case shows that a
single Event is below measurement noise; concurrent scheduling changes which
approximately 50 ms polling interval contains the completion. Throughput and
p95 are more stable for comparing these two cases.

## Event-count scaling

The concurrent `stream` case was also run with 1 and 256 chunks. The 1- and
256-chunk cases use 3 repeats of 64 Invocations; the 64-chunk case uses the
larger run above.

| Chunks/Invocation | Median without Events | Median with stream Events | Stream throughput | Throughput change | Event JSON/Invocation |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 52.420 ms | 52.086 ms | 242.00 inv/s | +0.91% | 376 B |
| 64 | 51.857 ms | 121.565 ms | 115.66 inv/s | -50.31% | 24,056 B |
| 256 | 51.614 ms | 329.986 ms | 46.73 inv/s | -80.58% | 96,405 B |

Retained compact JSON is approximately 376 bytes per Event for a 32-byte
payload. The remaining approximately 344 bytes are Event identity, sequence,
node/operator identity, timestamp, field names, and JSON structure. Actual
Python heap use is higher because this measurement intentionally does not
pretend that JSON byte length equals nested-object heap size.

## Persistence boundary

A SQLite verification run generated 65 UserEvents in memory for one
`stream_and_completed` Invocation and flushed the RuntimeStore:

| Check | Result |
| --- | ---: |
| UserEvents available before App close | 65 |
| `runtime_events` rows | 0 |
| `user_events` table exists | No |
| Pending persistence items after flush | 0 |
| Pending persistence bytes after flush | 0 B |

UserEvents therefore do not currently increase database writes or persistence
backlog. Their cost is on the execution path and in process-local retained
memory.

## Conclusions

1. One completion UserEvent per Node or Agent response has negligible measured
   cost.
2. Per-chunk Event creation scales approximately linearly in retained bytes and
   becomes CPU/scheduling significant under concurrent load.
3. Raw provider tokens should not automatically become one UserEvent each in
   production. A later optimization should coalesce deltas by a small time or
   byte window before serialization and RuntimeStore insertion.
4. Until batching exists, Workflow authors should emit semantically useful
   chunks rather than provider-token-sized chunks.
5. The current benchmark measures process-local retention, not long-lived UI
   subscribers or durable UserEvent storage. Those require separate benchmarks
   if either behavior is introduced.

## Reproduction

```bash
python -m benchmarks.user_event_benchmark \
  --invocations 30 --repeats 5 --warmups 3 \
  --concurrency 1 --chunks 64 --chunk-bytes 32

python -m benchmarks.user_event_benchmark \
  --invocations 96 --repeats 5 --warmups 3 \
  --concurrency 16 --chunks 64 --chunk-bytes 32

python -m benchmarks.user_event_benchmark \
  --invocations 64 --repeats 3 --warmups 2 \
  --concurrency 16 --chunks 1 --chunk-bytes 32

python -m benchmarks.user_event_benchmark \
  --invocations 64 --repeats 3 --warmups 2 \
  --concurrency 16 --chunks 256 --chunk-bytes 32
```

Use `--json` for machine-readable stdout or `--output PATH` to write the
complete JSON document while retaining the compact terminal report.
