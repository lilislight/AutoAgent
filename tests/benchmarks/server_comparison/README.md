# Native Server comparison

Both versions run their native FastAPI Server in an isolated process. V1 runs
with `cwd=v1`; V2 runs with `cwd` at the repository root. Each worker asserts
the imported package path. The controller never imports either Runtime.
Archived V1 source is unchanged.

## Run

From the repository root, using the root environment with Server dependencies:

```bash
.venv/bin/python -m tests.benchmarks.server_comparison.run \
  --suite baseline --repeat 3 \
  --profiles v1-minimal v1-standard v1-full v2-default \
  --output .autoagent/performance/baseline

.venv/bin/python -m tests.benchmarks.server_comparison.run \
  --suite smoke --repeat 5 \
  --profiles v2-default v2-monitored v2-buffered v2-join \
  --output .autoagent/performance/monitor-final-ab

.venv/bin/python -m tests.benchmarks.server_comparison.run \
  --suite soak --duration 3600 --guard-mib 640 \
  --profiles v2-monitored --output .autoagent/performance/soak-monitor-final

.venv/bin/python -m tests.postgres start
.venv/bin/python -m tests.benchmarks.server_comparison.run \
  --suite baseline --backend postgres --cases chain-10 loop-10 map-100 context-append \
  --profiles v1-full v2-default v2-buffered --repeat 2 \
  --output .autoagent/performance/postgres
```

Profiles in one command run sequentially. Use separate output directories.
The `artifact` suite compares text-producing chains, loops, Map, Context
updates and 128 unique calls. Use `--profiles v1-full v2-artifacts-8k
v2-buffered-artifacts-8k` to compare all three with an 8 KiB Artifact threshold.
This changes benchmark workers only; the SDK default remains unchanged.
Each Server catalog records the effective threshold and persistence mode.
`--source-root PATH` pins V2 imports to a copied package tree containing
`autoagent/`. Its source hashes and actual imported path are recorded; V1
continues to use the archive. This permits fixed-source comparisons while
development continues in the shared checkout.
Run heavy suites sequentially; concurrent continuous workloads and unrelated
host load must be disclosed when interpreting timing results. Long tests
target one request per second with one outstanding request per worker.
Execution and flush time can reduce the achieved rate.

`smoke`, `baseline`, `extended`, `stress` and `soak` select workloads.
`--cases` selects exact case names. The baseline covers chain depth, loops,
Map width, text, lists of records, Context updates, concurrent Sessions and
many registered definitions. Extended cases cover wide graphs, Session reuse,
slow/failing storage, Wait/Resume, process death at a durable Wait, Artifact
thresholds, long deadlines and a separate CPU diagnostic. Stress includes
registration of large graphs, large text and a 10,000-item Map.

## Measurement boundaries

- All business submissions use native `/api/v1/.../invocations` or
  `/api/v2/executions` routes; responses use native Resume/action routes.
- Test-only routes expose a small catalog, wait for completion, flush
  persistence and return scalar health counters. They do not execute a
  substitute Workflow engine. Fault arming and SQL instrumentation affect
  diagnostic cases only.
- V1 completion includes `RuntimeStore.aflush()`. V2 includes terminal
  observation and the Host persistence flush barrier. This compares flushed outcomes;
  V1 buffered event publication and V2 per-transition durable acknowledgments
  still have different crash semantics.
  A flush captures a frontier; the V2 fixture repeats it when later records
  arrive, until the queue is empty and accepted/durable frontiers agree.
- The default V2 observer uses `Host.wait_execution`, including its 100 ms
  polling interval and database queries. V1 observes native in-memory status
  every 1 ms. `v2-join` uses `Host.join` to isolate observation overhead.
  These are application timings, not isolated Operator timings.
- V1 and V2 use concurrency 32. Map cases independently set parallelism.
  Access logs and UI assets are disabled. Business Operators are deterministic;
  no external LLM API, credentials or network latency is involved.
- Chain names count work Nodes; Entry and Finish add two more Nodes.
  Row payloads contain approximately 35,000 dictionaries per work Node.
  Text payloads contain the requested number of bytes.
- Every call validates output. The report generator additionally checks output
  parity across profiles and database backends. Unique-payload cases validate
  a per-request checksum rather than requiring identical output.
- External Linux `/proc` samples include the entire Server process and its
  threads. They exclude the HTTP controller and shared PostgreSQL service.
  RSS, lifetime high-water RSS, and (newer runs) CPU time are recorded without
  `tracemalloc`. Sampling is every 20 ms, so short spikes can be missed.
- The external RSS guard terminates only the benchmark Server. It is a sampled
  test guard, not a hard cgroup limit or evidence of an actual kernel OOM kill.
- SQLite measurements include database, WAL and SHM files; relation/page counts
  are separate. PostgreSQL uses disposable databases and records relation,
  index and TOAST allocation before removal. Shared PostgreSQL WAL and RSS
  are not attributed to individual calls.
- Warmup, running, drain, idle and shutdown are separate phases. Peak RSS
  includes the whole Server lifecycle. Storage after shutdown includes saved
  Checkpoints. Guarded processes can leave WAL files, which remain counted.
- CPU profiling is a separate `cpu-diagnostic` case, enabled after warmup.
  It uses one interpreter-wide profiler with the process CPU clock on the
  tested Python version. Its latency values must not be compared with
  uninstrumented timing runs.
- Manifests record source hashes and package versions. Results distinguish
  successful runs, rejections, timeouts, injected storage faults and RSS guards.
  Failed runs never contribute to successful latency percentiles.

## Optimization baseline diagnostics

The `optimization` suite adds matched `diagnostics-off` / `diagnostics-on`
10-Node calls, `conditional-history` (16 conditional branches and Session
reuse), and `revision-session-churn` (64 registered definitions selected in
rotation, with new Sessions). The churn case is a baseline for future cache
tests; it does not prove that any proposed cache capacity has been exceeded.
These additional fixtures target V2 profiles.

```bash
.venv/bin/python -m tests.benchmarks.server_comparison.run \
  --suite optimization --backend sqlite --repeat 3 \
  --profiles v2-artifacts-8k v2-buffered-artifacts-8k \
  --output .autoagent/performance/optimization-diagnostics
```

`instrument_sql` installs test-process-only hooks. Counters retain bounded,
normalized SQL fingerprints without parameters, classify admission, projection,
write and observation queries, and distinguish executions from parameter sets.
Phase elapsed times include nested waits and must not be summed. In particular,
`admission_and_ack` includes durable acknowledgment waits. `driver_commit` times
the actual driver commit. Write-batch counters count attempts, not guaranteed
successful transactions. Export snapshots before/after measured calls; subtract
the warmup snapshot. Normal performance runs leave all hooks disabled.

Completion observation uses an accepted view while trace values use durable
SQL. The V2 fixture drains persistence before reading output, then drains again
to include later records. Both barriers belong to measured completion time.

## Result files

Each case writes `config.json`, `server.log`, `memory.jsonl`, `calls.jsonl` and
`result.json`; the suite writes `manifest.json` and `results.jsonl`.
Configuration files contain the local disposable database URL and belong in
the ignored output directory.

For optional static report figures, install the development plotting tool
`matplotlib`; it is not an AutoAgent runtime dependency.

```bash
.venv/bin/python -m tests.benchmarks.server_comparison.report \
  --runs .autoagent/performance/baseline .autoagent/performance/soak-v2 \
  --output .autoagent/performance/report
```

The report exports JSON/CSV and standalone PNG figures. Percentiles are
recomputed from successful raw calls, using linear interpolation; small
samples do not establish production tail latency.
