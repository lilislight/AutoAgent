"""Benchmark SQLite journal mode and durability settings independently.

This benchmark directly measures raw SQLite insert throughput in batches
comparable to RuntimeEvent persistence. It never presents WAL+NORMAL versus
DELETE+FULL as a journal-mode comparison because that changes two variables.

Usage::

    python -m benchmarks.sqlite_write_benchmark \\
      --events 10000 --batch-size 256
"""

from __future__ import annotations

import argparse
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
    synchronous: str,
    event_count: int,
    batch_size: int,
    payload_bytes: int,
) -> dict:
    """Run one journal mode in a fresh temporary database."""

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "bench.db"
        with closing(sqlite3.connect(str(db_path))) as db:
            db.execute(f"PRAGMA journal_mode={mode}")
            db.execute(f"PRAGMA synchronous={synchronous}")
            _create_table(db)
            db.commit()

        batch_times_ms: list[float] = []
        total_start = time.perf_counter()
        payload = "x" * payload_bytes if payload_bytes else ""

        with closing(sqlite3.connect(str(db_path))) as db:
            db.execute(f"PRAGMA journal_mode={mode}")
            db.execute(f"PRAGMA synchronous={synchronous}")

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
                            "node.completed",
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
            # WAL may be checkpointed and removed when the final connection
            # closes, so measure sidecar files while this connection is open.
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
            row_count = db.execute(
                "SELECT COUNT(*) FROM runtime_events"
            ).fetchone()[0]

    return {
        "journal_mode": mode,
        "synchronous": synchronous,
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


def _main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure SQLite journal mode and sync level separately."
    )
    parser.add_argument("--events", type=int, default=10000)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()

    profiles = (
        ("wal_full", "wal", "FULL"),
        ("delete_full", "delete", "FULL"),
        ("wal_normal", "wal", "NORMAL"),
        ("delete_normal", "delete", "NORMAL"),
    )
    all_results: dict[str, list[dict]] = {
        name: [] for name, _, _ in profiles
    }
    for run_i in range(args.runs):
        ordered = profiles if run_i % 2 == 0 else tuple(reversed(profiles))
        for name, mode, synchronous in ordered:
            result = _run_mode(
                mode,
                synchronous=synchronous,
                event_count=args.events,
                batch_size=args.batch_size,
                payload_bytes=args.payload_bytes,
            )
            all_results[name].append(result)

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

    summaries = {
        name: _summary(results)
        for name, results in all_results.items()
    }
    comparisons = {
        "wal_vs_delete_full": (
            summaries["wal_full"]["mean_events_per_second"]
            / summaries["delete_full"]["mean_events_per_second"]
        ),
        "wal_vs_delete_normal": (
            summaries["wal_normal"]["mean_events_per_second"]
            / summaries["delete_normal"]["mean_events_per_second"]
        ),
        "wal_normal_vs_full": (
            summaries["wal_normal"]["mean_events_per_second"]
            / summaries["wal_full"]["mean_events_per_second"]
        ),
    }

    print(
        json.dumps(
            {
                "configuration": {
                    "events": args.events,
                    "batch_size": args.batch_size,
                    "payload_bytes": args.payload_bytes,
                    "runs": args.runs,
                },
                "profiles": summaries,
                "comparisons": comparisons,
                "interpretation": {
                    "journal_mode_at_full": (
                        "WAL/DELETE with synchronous=FULL: "
                        f"{comparisons['wal_vs_delete_full']:.2f}x"
                    ),
                    "journal_mode_at_normal": (
                        "WAL/DELETE with synchronous=NORMAL: "
                        f"{comparisons['wal_vs_delete_normal']:.2f}x"
                    ),
                    "durability_cost_in_wal": (
                        "NORMAL/FULL in WAL mode: "
                        f"{comparisons['wal_normal_vs_full']:.2f}x"
                    ),
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    _main()
