# Persistence Coordinator Results

Recorded on 2026-07-25 on branch
`experiment/one-way-persistence-queue`, after moving queue ownership,
backpressure, durable cursors, and failure state out of `DatabaseBackend` into
the sink-independent `PersistenceCoordinator`.

- Python environment: repository-root `uv` environment
- Final full suite: 257 tests passed, 1 PostgreSQL integration test skipped
- Final full-suite elapsed time: about 23.9 seconds
- Before the last three edge cases were added, the 245-test suite also passed
  twice (32.657 and 32.884 seconds)

Absolute timings depend on host load. The comparison baseline is
`2026-07-23-two-loop-one-way-results.md` from the same branch and host.

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
    "median_invoke_ms": 109.2411195004388,
    "p95_invoke_ms": 148.91062800234067,
    "invocations_per_second": 8.667453259892799,
    "flush_ms": 0.0017139973351731896,
    "events_per_invocation": 151
  },
  {
    "backend": "sqlite",
    "node_count": 30,
    "invocation_count": 50,
    "median_invoke_ms": 212.06299849836796,
    "p95_invoke_ms": 274.4771209981991,
    "invocations_per_second": 4.524655908269818,
    "flush_ms": 66.35795699912705,
    "events_per_invocation": 151
  }
]
```

SQLite median Invocation latency improved from the 274.89 ms baseline to
212.06 ms (-22.9%), p95 improved from 326.54 ms to 274.48 ms (-15.9%), and
throughput increased from 3.59 to 4.52 Invocation/s (+26.0%). Compared with
the first coordinator run, asynchronously queuing Workflow metadata and
Invocation genesis reduced median invoke latency by another 16.5%. Final
flush was 66.36 ms. The current default is the safer WAL+FULL profile, while
the reviewed implementation used WAL+NORMAL.

The memory-only median moved by roughly 5% even though the new persistence
path is not constructed for a memory-only Store, so that difference should be
treated as run-to-run host noise rather than a coordinator cost.

## Backlog and memory accounting

Commands:

```bash
UV_CACHE_DIR=/tmp/autoagent-uv-cache \
  uv run python -m benchmarks.persistence_backlog_benchmark \
  --database-mode normal --invocations 20 --nodes 10 --payload-bytes 1024

UV_CACHE_DIR=/tmp/autoagent-uv-cache \
  uv run python -m benchmarks.persistence_backlog_benchmark \
  --database-mode blocked --invocations 20 --nodes 10 --payload-bytes 1024
```

| Metric | Normal SQLite | Blocked SQLite |
| --- | ---: | ---: |
| Boundary Events | 1,020 | 1,020 |
| Peak outstanding envelopes | 998 | 1,022 |
| Peak accounted memory | 10,536,777 B | 10,855,899 B |
| Traced heap at backlog | 12,174,921 B | 12,703,850 B |
| Event JSON stored | 2,392,442 B | 2,392,267 B |
| Invocation genesis JSON stored | 27,770 B | 27,770 B |
| Production time | 2.189 s | 1.841 s |
| Flush after release | 0.622 s | 1.109 s |
| Pending after flush | 0 | 0 |

The new queue number is intentionally not the serialized JSON size. It
accounts the detached in-memory Event graph before the persistence loop has
serialized it, then replaces the estimate with the exact persistence item
size. This closes the old unaccounted preparation window and tracks actual
heap pressure much more closely: the blocked run estimated 10.86 MB while
`tracemalloc` observed 12.70 MB. The two extra blocked envelopes beyond the
1,020 boundary Events are asynchronous Workflow/admission records still
waiting in the same bounded queue.

Both runs crossed the deliberately small 1 MiB admission watermark. The
production default is 64 MiB high, 32 MiB low, and 128 MiB hard. With the
blocked workload's measured rate of about 5.90 MB/s, those defaults correspond
to roughly 11.4 seconds before new admission pauses and 22.8 seconds before
active producers reach the hard bound, assuming this workload and no draining.

## SQLite durability profiles

Command:

```bash
UV_CACHE_DIR=/tmp/autoagent-uv-cache \
  uv run python -m benchmarks.sqlite_write_benchmark \
  --events 10000 --batch-size 256 --payload-bytes 1024 --runs 3
```

| Profile | Mean Events/s |
| --- | ---: |
| WAL + FULL | 65,930 |
| DELETE + FULL | 40,115 |
| WAL + NORMAL | 104,643 |
| DELETE + NORMAL | 51,627 |

At the same FULL setting, WAL was 1.64x faster than DELETE journal. Within WAL,
NORMAL was 1.59x faster than FULL in this raw benchmark. AutoAgent therefore
keeps WAL+FULL as the default and exposes NORMAL only as an explicit durability
tradeoff.
