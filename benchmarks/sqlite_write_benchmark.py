"""Benchmark SQLite write throughput: DELETE journal vs WAL mode.

This benchmark directly measures raw SQLite insert throughput in batches
comparable to RuntimeEvent persistence.  It tests both journal modes on
identical workloads.

Usage::

    UV_CACHE_DIR=/tmp/autoagent-uv-cache \\
      uv run python -m benchmarks.sqlite_write_benchmark \\
      --events 10000 --batch-size 256
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import tempfile
import time
from contextlib import closing
from pathlib import Path
from statistics import mean, median


def _create_table(db: sqlite3.Connection) -> None:
    db.execute(
        "CREATE TABLE IF NOT EXISTS runtime_events ("
        "  id TEXT PRIMARY KEY,"
        "  invocation_id TEXT NOT NULL,"
        "  sequence INTEGER NOT NULL,"
        "  type TEXT NOT NULL,"
        "  payload_json TEXT NOT NULL,"
        "  occurred_at_ms INTEGER NOT NULL,"
        "  created_at_ms INTEGER NOT NULL"
        ")"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_invocation "
        "ON runtime_events(invocation_id, sequence)"
    )


def _run_mode(
    mode: str,
    *,
    event_count: int,
    batch_size: int,
    payload_bytes: int,
) -> dict:
    """Run one journal mode in a fresh temporary database."""

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "bench.db"
        with closing(sqlite3.connect(str(db_path))) as db:
            db.execute(f"PRAGMA journal_mode={mode}")
            if mode.upper() == "WAL":
                db.execute("PRAGMA synchronous=NORMAL")
            _create_table(db)
            db.commit()

        batch_times_ms: list[float] = []
        total_start = time.perf_counter()
        payload = "x" * payload_bytes if payload_bytes else ""

        with closing(sqlite3.connect(str(db_path))) as db:
            db.execute(f"PRAGMA journal_mode={mode}")
            if mode.upper() == "WAL":
                db.execute("PRAGMA synchronous=NORMAL")

            for batch_start in range(0, event_count, batch_size):
                batch_end = min(batch_start + batch_size, event_count)

                t0 = time.perf_counter()
                db.execute("BEGIN")
                for i in range(batch_start, batch_end):
                    db.execute(
                        "INSERT INTO runtime_events "
                        "(id, invocation_id, sequence, type, "
                        "payload_json, occurred_at_ms, created_at_ms) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            f"event-{i:08d}",
                            "inv-bench-001",
                            i + 1,
                            "node.committed",
                            json.dumps(
                                {"detail": {"i": i}, "blob": payload}
                            ),
                            int(time.time() * 1000),
                            int(time.time() * 1000),
                        ),
                    )
                db.commit()
                batch_times_ms.append(
                    (time.perf_counter() - t0) * 1000
                )

        total_ms = (time.perf_counter() - total_start) * 1000

        db_bytes = db_path.stat().st_size
        wal_path = Path(str(db_path) + "-wal")
        shm_path = Path(str(db_path) + "-shm")
        journal_path = Path(str(db_path) + "-journal")
        wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
        shm_bytes = shm_path.stat().st_size if shm_path.exists() else 0
        journal_bytes = (
            journal_path.stat().st_size
            if journal_path.exists()
            else 0
        )

        with closing(sqlite3.connect(str(db_path))) as db:
            row_count = db.execute(
                "SELECT COUNT(*) FROM runtime_events"
            ).fetchone()[0]

    return {
        "journal_mode": mode,
        "event_count": event_count,
        "batch_size": batch_size,
        "payload_bytes": payload_bytes,
        "total_ms": total_ms,
        "events_per_second": event_count / (total_ms / 1000),
        "num_batches": len(batch_times_ms),
        "mean_batch_ms": mean(batch_times_ms),
        "median_batch_ms": median(batch_times_ms),
        "p95_batch_ms": sorted(batch_times_ms)[
            max(0, int(len(batch_times_ms) * 0.95))
        ],
        "db_file_bytes": db_bytes,
        "wal_file_bytes": wal_bytes,
        "shm_file_bytes": shm_bytes,
        "journal_file_bytes": journal_bytes,
        "actual_row_count": row_count,
    }


async def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure raw SQLite write throughput: DELETE vs WAL."
    )
    parser.add_argument("--events", type=int, default=10000)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()

    all_results: dict[str, list[dict]] = {"wal": [], "delete": []}
    for run_i in range(args.runs):
        # Alternate order each run to reduce bias
        modes = ("wal", "delete") if run_i % 2 == 0 else ("delete", "wal")
        for mode in modes:
            result = await asyncio.to_thread(
                _run_mode,
                mode,
                event_count=args.events,
                batch_size=args.batch_size,
                payload_bytes=args.payload_bytes,
            )
            all_results[mode].append(result)

    def _summary(results: list[dict]) -> dict:
        eps = [r["events_per_second"] for r in results]
        return {
            "mean_events_per_second": mean(eps),
            "median_events_per_second": median(eps),
            "min_events_per_second": min(eps),
            "max_events_per_second": max(eps),
            "mean_total_ms": mean(r["total_ms"] for r in results),
            "mean_median_batch_ms": mean(
                r["median_batch_ms"] for r in results
            ),
            "mean_p95_batch_ms": mean(
                r["p95_batch_ms"] for r in results
            ),
            "events_per_second_list": eps,
            "db_file_bytes": [r["db_file_bytes"] for r in results],
            "wal_file_bytes": [r["wal_file_bytes"] for r in results],
            "journal_file_bytes": [
                r["journal_file_bytes"] for r in results
            ],
        }

    wal = _summary(all_results["wal"])
    dell = _summary(all_results["delete"])

    speedup = wal["mean_events_per_second"] / dell["mean_events_per_second"]

    print(
        json.dumps(
            {
                "configuration": {
                    "events": args.events,
                    "batch_size": args.batch_size,
                    "payload_bytes": args.payload_bytes,
                    "runs": args.runs,
                },
                "wal": wal,
                "delete": dell,
                "speedup": {
                    "events_per_second": speedup,
                    "total_ms": dell["mean_total_ms"]
                    / wal["mean_total_ms"],
                },
                "interpretation": (
                    f"WAL is {speedup:.2f}x faster than DELETE journal "
                    f"for {args.events} rows in batches of "
                    f"{args.batch_size}."
                ),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(_main())
