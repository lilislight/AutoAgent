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

## Internal batching optimization follow-up

The first implementation above published one mailbox message, acquired the
RuntimeStore lock, validated a Pydantic model, and returned a defensive deep
copy for every stream Event. The optimized working tree changes only internal
transport:

- adjacent small LLM text, reasoning, or matching Tool Call deltas are combined
  to a target of 32 characters before generic stream execution;
- arbitrary user-defined stream chunks are not combined;
- generated UserEventSpecs are transported in batches of at most 32;
- a partial batch flushes after at most approximately 20 ms;
- stream completion, failure, and cancellation force a flush;
- RuntimeStore serializes a whole batch before one lock acquisition and applies
  it atomically;
- serialization failure falls back to per-spec isolation without losing valid
  sibling Events or changing their order;
- repeated Event-type Pydantic construction and unused internal result deep
  copies were removed.

No public `StreamingResult`, `UserEventMapping`, Node, RuntimeStore paging, or
Server API changed. Provider `astream()` output also remains raw; LLM delta
coalescing is applied only by the framework `llm_call` Operator.

### 64 chunks, concurrency 16

The optimized run used the same 5 repeats, 96 measured Invocations per repeat,
3 warmups, 64 chunks, 32-byte payload, memory backend, and Minimal Event mode.

| Case | Original throughput | Optimized throughput | Direct change | Original median | Optimized median |
| --- | ---: | ---: | ---: | ---: | ---: |
| `none` | 232.77 inv/s | 249.81 inv/s | +7.32% host/run variance | 51.857 ms | 51.506 ms |
| `completed` | 233.09 inv/s | 239.20 inv/s | +2.62% | 51.799 ms | 52.153 ms |
| `stream` | 115.66 inv/s | 181.64 inv/s | **+57.05%** | 121.565 ms | 102.128 ms |
| `stream_and_completed` | 110.70 inv/s | 182.13 inv/s | **+64.53%** | 152.762 ms | 102.386 ms |

Normalized to the no-Event case in each run, 64 stream Events previously
reduced throughput by 50.31%. After batching they reduce it by 27.29%.
The retained Event count and 24,056-byte compact JSON representation are
unchanged because transport batching deliberately preserves individual Event
semantics.

### 256 chunks, concurrency 16

This run used the same 3 repeats and 64 measured Invocations per repeat:

| Case | Original throughput | Optimized throughput | Direct change | Original median | Optimized median |
| --- | ---: | ---: | ---: | ---: | ---: |
| `none` | 240.69 inv/s | 226.25 inv/s | -6.00% host/run variance | 51.614 ms | 51.924 ms |
| `stream` | 46.73 inv/s | 116.76 inv/s | **+149.86%** | 329.986 ms | 153.026 ms |
| `stream_and_completed` | 43.68 inv/s | 109.77 inv/s | **+151.31%** | 351.185 ms | 156.955 ms |

Normalized stream throughput rises from 19.41% to 51.61% of the corresponding
no-Event case. Retained bytes remain unchanged, so LLM-specific coalescing is
still required to reduce memory rather than only transport cost.

### LLM delta count

A deterministic Operator test feeds 100 consecutive one-character text deltas
through a real `llm_call` Operator. The Provider still yields 100 deltas, while
the Operator's `StreamingResult` yields four text deltas with lengths
`32, 32, 32, 4`, followed by the unchanged completed response. Type changes,
different Tool Call indexes, completed responses, errors, and cancellation are
coalescing boundaries.
