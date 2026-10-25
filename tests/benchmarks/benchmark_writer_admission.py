"""Measure real pending-duplicate admission at increasing queue depths.

This diagnostic uses a real SQLite sink with a gated commit. Setup and drain
are excluded from duplicate timing; it is not an HTTP latency benchmark.
Run each source root in its own process to compare implementations.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import deque
import json
from pathlib import Path
import statistics
import sys
import tempfile
import time


async def measure(args):
    from autoagent.core.runtime.user_events import UserEvent
    from autoagent.host.persistence import DatabaseSink, PersistenceConfig

    results = []
    with tempfile.TemporaryDirectory() as directory:
        sink = await DatabaseSink.open(
            "sqlite+aiosqlite:///" + str(Path(directory) / "runtime.db"),
            config=PersistenceConfig(mode="buffered", batch_delay_ms=0,
                                     queue_count=max(args.depths) + 1024),
        )
        writer = sink._writer

        async def scenario(depth):
            original = writer.storage.write
            gate, entered = asyncio.Event(), asyncio.Event()

            class QueueProbe(deque):
                iterations = 0

                def __iter__(self):
                    self.iterations += 1
                    return super().__iter__()

            assert not writer.pending
            writer.pending = QueueProbe()

            async def delayed(entries):
                entered.set()
                await gate.wait()
                return await original(entries)

            writer.storage.write = delayed
            stream = f"depth-{depth}"
            try:
                for sequence in range(1, depth + 1):
                    event = UserEvent("scale", stream, sequence, "progress", {})
                    await writer.accept("user_event", event.id, event)
                await entered.wait()
                previous_iterations = writer.pending.iterations
                times = []
                for _ in range(args.chunks):
                    started = time.perf_counter_ns()
                    for _ in range(args.calls):
                        await writer.accept("user_event", event.id, event)
                    times.append((time.perf_counter_ns() - started) / args.calls)
                self_count = writer.pending_count
                assert self_count == depth
                duplicate_iterations = writer.pending.iterations - previous_iterations
                previous_iterations = writer.pending.iterations
                fresh_times = []
                for sequence in range(depth + 1, depth + 65):
                    event = UserEvent("scale", stream, sequence, "progress", {})
                    started = time.perf_counter_ns()
                    await writer.accept("user_event", event.id, event)
                    fresh_times.append((time.perf_counter_ns() - started) / 1000)
                fresh_iterations = writer.pending.iterations - previous_iterations
                retained = writer.retained
            finally:
                started = time.perf_counter()
                gate.set()
                await writer.flush()
                drain_ms = (time.perf_counter() - started) * 1000
                writer.storage.write = original
            assert writer.pending_count == writer.retained == 0
            assert not writer.objects
            # Baseline has no indices. On the optimized path neither index may
            # retain committed Entries or grow with the lifetime of this sink.
            assert not getattr(writer, "pending_by_identity", {})
            assert not getattr(writer, "pending_tails", {})
            return dict(depth=depth, duplicate_ns=statistics.median(times),
                        chunk_ns=times, retained_bytes=retained,
                        duplicate_queue_iterations=duplicate_iterations,
                        fresh_accept_us=statistics.median(fresh_times),
                        fresh_queue_iterations=fresh_iterations,
                        drain_ms=drain_ms, drained_count=writer.pending_count)

        try:
            for depth in args.depths:
                results.append(await writer.run(scenario, depth))
        finally:
            await sink.close()
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--depths", type=int, nargs="+", default=[1, 64, 1024, 8192])
    parser.add_argument("--chunks", type=int, default=10)
    parser.add_argument("--calls", type=int, default=400)
    args = parser.parse_args()
    if args.source_root:
        sys.path.insert(0, str(args.source_root.resolve()))
    import autoagent
    if args.source_root:
        assert Path(autoagent.__file__).resolve().is_relative_to(args.source_root.resolve())
    result = dict(package=str(autoagent.__file__), measurements=asyncio.run(measure(args)))
    args.output.write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
