"""Isolate framework dispatch from user work; comparison is not a production change."""
import asyncio
import json
import os
import statistics
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch
from autoagent.core.executor.node_executor import _BurstThreadPool, _run_sync
from autoagent.core.executor.future import await_concurrent_future

COUNT=500

def noop():
    return 1

async def invoke(kind):
    if kind=='completed_future':
        for _ in range(COUNT):
            future=Future();future.set_result(1)
            assert await await_concurrent_future(future)==1
        return
    pool=_BurstThreadPool(1) if kind=='burst_pool' else ThreadPoolExecutor(1)
    try:
        for _ in range(COUNT):
            assert await _run_sync(pool,noop)==1
    finally:pool.shutdown(wait=True)

def measure(kind):
    asyncio.run(invoke(kind))
    samples=[]
    for _ in range(5):
        start=time.perf_counter_ns();asyncio.run(invoke(kind));samples.append(time.perf_counter_ns()-start)
    counts={'thread_starts':0,'pipes':0}
    thread_start=threading.Thread.start
    pipe=os.pipe
    def counted_start(self):
        counts['thread_starts']+=1
        return thread_start(self)
    def counted_pipe():
        counts['pipes']+=1
        return pipe()
    with patch.object(threading.Thread,'start',counted_start), patch.object(os,'pipe',counted_pipe):
        asyncio.run(invoke(kind))
    return {'median_ns':int(statistics.median(samples)),**counts}

def main():
    report={'calls':COUNT,'method':'5 timing samples without counters; separate instrumentation run; noop handler; 1 worker; pool creation/shutdown included',
        **{kind:measure(kind) for kind in ('burst_pool','persistent_pool_reference','completed_future')}}
    Path('artifacts/core_performance_audit/dispatch.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':main()
