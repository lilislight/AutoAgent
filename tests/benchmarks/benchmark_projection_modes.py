"""Compare projection modes through fresh native HTTP Servers and durable SQL facts.

Run with the root venv. Results and a frozen V2 package are retained below
.autoagent/performance; the benchmark never invokes Providers or archived V1.
Execution time includes HTTP terminal observation and the fact-flush barrier,
excluding Server startup. Projection readiness is observed through HTTP status
at 100 ms intervals; transactional readiness has no extra background wait.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import time
from uuid import uuid4

import httpx

ROOT = Path(__file__).resolve().parents[2]
WORKER = Path(__file__).with_name('server_comparison') / 'worker.py'


def request(client, method, path, **kwargs):
    response = client.request(method, path, **kwargs)
    response.raise_for_status()
    return response.json()


async def row_counts(url):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            return {name: int(await conn.scalar(text('SELECT COUNT(*) FROM ' + name))) for name in (
                'runtime_events', 'node_executions', 'operator_calls', 'waits', 'edge_evaluations', 'graph_changes')}
    finally:
        await engine.dispose()


def run_one(args, source, backend, persistence_mode, projection_mode, repeat, output):
    from tests.postgres import PostgreSQLDatabase
    pg = PostgreSQLDatabase() if backend == 'postgres' else None
    output.mkdir(parents=True)
    url = asyncio.run(pg.create()) if pg else 'sqlite+aiosqlite:///' + str(output / 'runtime.db')
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    config = dict(repository_root=str(ROOT), source_root=str(source), directory=str(output),
        database_url=url, port=port, persistence_mode=persistence_mode, timeout_seconds=args.timeout,
        case=dict(family='chain', nodes=args.nodes - 2, payload_bytes=args.payload_bytes),
        environment={'AUTOAGENT_PROJECTION_MODE': projection_mode,
                     'AUTOAGENT_ARTIFACT_THRESHOLD_BYTES': '8192',
                     'AUTOAGENT_HOST_MEMORY_BUDGET_BYTES': '2147483648'})
    (output / 'config.json').write_text(json.dumps(config, indent=2))
    result = dict(backend=backend, persistence_mode=persistence_mode, projection_mode=projection_mode,
                  repeat=repeat, nodes=args.nodes, calls=args.calls, payload_bytes=args.payload_bytes)
    process = None
    try:
        with (output / 'server.log').open('w') as log, httpx.Client(
                base_url=f'http://127.0.0.1:{port}', timeout=args.timeout, trust_env=False) as client:
            process = subprocess.Popen([sys.executable, str(WORKER), '--version', 'v2',
                '--config', str(output / 'config.json')], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + args.timeout
            while True:
                if process.poll() is not None:
                    raise RuntimeError('Native Server stopped during startup: ' + str(output / 'server.log'))
                try:
                    response = client.get('/__bench/catalog', timeout=.5)
                    if response.status_code == 200:
                        catalog = response.json()
                        break
                except httpx.TransportError:
                    pass
                if time.monotonic() >= deadline:
                    raise TimeoutError('Server startup timed out')
                time.sleep(.02)
            initial = request(client, 'GET', '/api/v2/query-projections')
            if initial['mode'] != projection_mode or catalog['database_mode'] != persistence_mode:
                raise AssertionError('The Server did not apply the requested modes.')
            if catalog['artifact_threshold_bytes'] != 8192:
                raise AssertionError('Artifact threshold differs from the comparison contract.')
            started = time.perf_counter()
            samples, outputs = [], []
            for _ in range(args.calls):
                call_started = time.perf_counter()
                receipt = request(client, 'POST', '/api/v2/executions', json=dict(
                    workflow_id='comparison', revision_id=catalog['revision'], entry_node_id='entry',
                    input=dict(step=0, blob='', rows=[]), session_mode='new'))
                completed = request(client, 'GET', '/__bench/wait', params={'iid': receipt['invocation_id']})
                if completed['status'] != 'completed' or completed['queued']:
                    raise AssertionError(completed)
                samples.append((time.perf_counter() - call_started) * 1000)
                outputs.append(completed['output'])
            health = request(client, 'POST', '/__bench/flush')['health']
            execution_end = time.perf_counter()
            status = request(client, 'GET', '/api/v2/query-projections')
            target = status['durable_order']
            result['status_after_execution'] = status
            result['health_after_execution'] = health
            deadline = time.monotonic() + args.timeout
            while status['processed_order'] is None or status['processed_order'] < target:
                if time.monotonic() >= deadline:
                    raise TimeoutError('Projection did not catch up: ' + str(status))
                time.sleep(.1)
                status = request(client, 'GET', '/api/v2/query-projections')
                target = max(target, status['durable_order'])
            ready = time.perf_counter()
            result.update(execution_ms=(execution_end - started) * 1000,
                call_ms=samples, projection_ready_ms=(ready - started) * 1000,
                projection_wait_after_execution_ms=(ready - execution_end) * 1000,
                projection_complete_at_execution=result['status_after_execution']['processed_order'] == target,
                final_status=status, output=outputs[0])
            if any(value != outputs[0] for value in outputs):
                raise AssertionError('Business output changed within a run')
            counts = asyncio.run(row_counts(url))
            if counts['node_executions'] != args.nodes * args.calls or counts['operator_calls'] != args.nodes * args.calls:
                raise AssertionError(counts)
            if status['worker'].get('failures', 0):
                raise AssertionError(status)
            result['rows'] = counts
            process.send_signal(signal.SIGTERM)
            process.wait(timeout=15)
            if process.returncode not in (0, -signal.SIGTERM):
                raise RuntimeError('Native Server failed to shut down: ' + str(process.returncode))
        (output / 'result.json').write_text(json.dumps(result, indent=2))
        return result
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        if pg:
            asyncio.run(pg.close())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backends', nargs='+', choices=('sqlite', 'postgres'), default=['sqlite', 'postgres'])
    parser.add_argument('--persistence-modes', nargs='+', choices=('durable', 'buffered'), default=['durable', 'buffered'])
    parser.add_argument('--nodes', type=int, default=10)
    parser.add_argument('--calls', type=int, default=5)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--payload-bytes', type=int, default=16384)
    parser.add_argument('--timeout', type=float, default=90)
    parser.add_argument('--directory', type=Path)
    args = parser.parse_args()
    if min(args.calls, args.repeats) < 1 or args.nodes < 3 or args.payload_bytes < 0:
        parser.error('Use nodes >= 3, positive calls/repeats and nonnegative payload bytes.')
    directory = (args.directory or ROOT / '.autoagent/performance' / ('projection-modes-' + uuid4().hex[:8])).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    source = directory / 'source'
    shutil.copytree(ROOT / 'autoagent', source / 'autoagent', ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    provenance = {str(path.relative_to(source)): hashlib.sha256(path.read_bytes()).hexdigest()
                  for path in source.rglob('*.py')}
    (directory / 'source-hashes.json').write_text(json.dumps(provenance, indent=2))
    if 'postgres' in args.backends:
        from tests.postgres import POSTGRES_TEST_URL, control, check_connection
        import os
        if not os.environ.get('AUTOAGENT_TEST_POSTGRES_URL') and POSTGRES_TEST_URL:
            control('start')
        asyncio.run(check_connection())
    results = []
    for repeat in range(args.repeats):
        for backend in args.backends:
            for persistence in args.persistence_modes:
                order = ('transactional', 'background') if repeat % 2 == 0 else ('background', 'transactional')
                paired = []
                for projection in order:
                    name = f'{backend}-{persistence}-{projection}-{repeat + 1}'
                    print('RUN', name, flush=True)
                    row = run_one(args, source, backend, persistence, projection, repeat + 1, directory / name)
                    paired.append(row)
                    results.append(row)
                    (directory / 'results.json').write_text(json.dumps(results, indent=2))
                    print(json.dumps({key: row[key] for key in ('backend', 'persistence_mode', 'projection_mode',
                        'execution_ms', 'projection_ready_ms', 'projection_wait_after_execution_ms')}), flush=True)
                if paired[0]['rows'] != paired[1]['rows'] or paired[0]['output'] != paired[1]['output']:
                    raise AssertionError('The two modes produced different facts, projections or business output.')
    summary = []
    for backend in args.backends:
        for persistence in args.persistence_modes:
            for projection in ('transactional', 'background'):
                rows = [row for row in results if (row['backend'], row['persistence_mode'], row['projection_mode']) ==
                        (backend, persistence, projection)]
                summary.append(dict(backend=backend, persistence_mode=persistence, projection_mode=projection,
                    repeats=len(rows), **{key: statistics.median(row[key] for row in rows) for key in (
                        'execution_ms', 'projection_ready_ms', 'projection_wait_after_execution_ms')}))
    (directory / 'summary.json').write_text(json.dumps(summary, indent=2))
    print('RESULTS', directory, flush=True)
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == '__main__':
    main()
