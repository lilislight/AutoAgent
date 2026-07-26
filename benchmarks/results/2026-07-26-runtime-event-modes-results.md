# Runtime Event modes benchmark

Environment: 4 logical CPUs, 8 GiB memory, repository `uv` Python
environment. Results describe the current uncommitted working tree.

## Normal execution

Configuration: 30-node chain, 30 measured Invocations per case, one warmup,
SQLite WAL with the default durability profile.

| Backend | Event mode | Median | p95 | Throughput | Final flush | Events/invocation |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Memory | Minimal | 71.76 ms | 74.99 ms | 13.69 inv/s | 0 ms | 0 |
| Memory | Standard | 75.48 ms | 77.89 ms | 12.98 inv/s | 0 ms | 121 |
| Memory | Full | 182.18 ms | 214.96 ms | 5.36 inv/s | 0 ms | 151 |
| SQLite | Minimal | 84.96 ms | 98.02 ms | 11.52 inv/s | 92.98 ms | 0 |
| SQLite | Standard | 212.71 ms | 234.43 ms | 4.65 inv/s | 80.95 ms | 121 |
| SQLite | Full | 606.85 ms | 681.43 ms | 1.64 inv/s | 375.30 ms | 151 |

## Persistence backlog with database writes blocked

Configuration: 20 concurrent Invocations, 10 nodes each. The queue was
measured after execution completed while runtime Event/invocation-state writes
were blocked, then the writer was released and flushed.

| Payload | Mode | Events | Peak queue | Queue/invocation | Execution | Flush | SQLite file |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 B | Minimal | 0 | 105,403 B | 5,270 B | 0.330 s | 0.916 s | 114,688 B |
| 0 B | Standard | 820 | 3,299,991 B | 164,999 B | 1.766 s | 1.705 s | 716,800 B |
| 0 B | Full | 1,020 | 6,556,771 B | 327,839 B | 2.567 s | 5.567 s | 2,154,496 B |
| 1 KiB | Minimal | 0 | 126,523 B | 6,326 B | 0.331 s | 0.909 s | 143,360 B |
| 1 KiB | Standard | 820 | 3,618,291 B | 180,915 B | 1.847 s | 1.770 s | 802,816 B |
| 1 KiB | Full | 1,020 | 8,668,712 B | 433,436 B | 2.427 s | 6.716 s | 4,005,888 B |
| 10 KiB | Minimal | 0 | 303,736 B | 15,187 B | 0.335 s | 0.948 s | 376,832 B |
| 10 KiB | Standard | 820 | 5,559,831 B | 277,992 B | 1.839 s | 1.750 s | 966,656 B |
| 10 KiB | Full | 1,020 | 18,568,360 B | 928,418 B | 2.743 s | 9.361 s | 3,416,064 B |

The 10 KiB values crossed the ArtifactRef threshold. Each Invocation stored one
deduplicated 10 KiB artifact, so durable database growth stayed bounded, but
the persistence queue still owned the pre-serialization values and therefore
grew to 18.6 MiB in Full mode.

## Default backpressure projection

The project defaults are a 64 MiB high watermark, 128 MiB hard watermark, and
a 5 second admission wait. If database consumption is completely stopped and
the measured workload scales approximately linearly, 64 MiB corresponds to:

| Payload | Minimal | Standard | Full |
| --- | ---: | ---: | ---: |
| 0 B | 12,734 Invocations | 407 Invocations | 205 Invocations |
| 1 KiB | 10,608 Invocations | 371 Invocations | 155 Invocations |
| 10 KiB | 4,419 Invocations | 241 Invocations | 72 Invocations |

These are workload-specific projections, not fixed framework limits. Workflow
width, node count, Context changes, retries, and output shape change the bytes
per Invocation.

## Commands

```bash
UV_CACHE_DIR=/tmp/autoagent-uv-cache \
  uv run python -m benchmarks.runtime_store_benchmark \
  --nodes 30 --invocations 30 --backend both --event-mode all --json

UV_CACHE_DIR=/tmp/autoagent-uv-cache \
  uv run python -m benchmarks.persistence_backlog_benchmark \
  --invocations 20 --nodes 10 --payload-bytes 10240 \
  --event-mode full --database-mode blocked \
  --queue-high-bytes 67108864 --queue-hard-bytes 134217728
```
