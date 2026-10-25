"""Freeze one V2 package and compare durable group commit through native HTTP.

Run with the root virtual environment. Results and the frozen package live in
the ignored .autoagent tree. SQL diagnostics run separately from latency trials.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[3]


def freeze(directory, wait_mode):
    manifest = directory / 'provenance.json'
    if manifest.exists():
        recorded = json.loads(manifest.read_text())
        if recorded.get('wait_mode', 'observation') != wait_mode:
            raise ValueError('Use another directory for a different observation method.')
        for name, digest in recorded['hashes'].items():
            if hashlib.sha256((directory/name).read_bytes()).hexdigest() != digest:
                raise RuntimeError('Frozen source changed: ' + name)
        return
    directory.mkdir(parents=True, exist_ok=True)
    shutil.copytree(ROOT/'autoagent', directory/'source/autoagent',
                    ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    (directory/'harness').mkdir()
    for name in ('run.py', 'worker.py', 'business.py', 'diagnostics.py', 'group_commit.py'):
        shutil.copy2(Path(__file__).with_name(name), directory/'harness'/name)
    hashes = {str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
              for folder in ('source', 'harness') for path in (directory/folder).rglob('*.py')}
    manifest.write_text(json.dumps(dict(hashes=hashes,
        git_head=subprocess.check_output(['git','rev-parse','HEAD'], cwd=ROOT, text=True).strip(),
        git_status=subprocess.check_output(['git','status','--short'], cwd=ROOT, text=True),
        python=sys.version, wait_mode=wait_mode, artifact_threshold_bytes=8192, delays_ms=[0,1,5],
        concurrency=[1,8,32], timing_repetitions=3,
        notes=['Shared host; no private CPU/cgroup isolation.',
               'Captured-frontier flush per request, global drain only at final barrier.',
               '32 Session case is one finite wave; not a steady-state soak.']), indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, default=ROOT/'.autoagent/performance/v2-optimization/step-08/join')
    parser.add_argument('--phase', choices=('screen','repeat','diagnostic','all'), default='all')
    parser.add_argument('--wait-mode', choices=('observation','join'), default='join')
    args = parser.parse_args()
    directory = args.directory.resolve()
    freeze(directory, args.wait_mode)
    spec = importlib.util.spec_from_file_location('group_commit_harness', directory/'harness/run.py')
    harness = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(harness)
    harness.ROOT = ROOT
    results = directory/'results.jsonl'
    done = {json.loads(line)['job'] for line in results.read_text().splitlines()} if results.exists() else set()
    jobs = []
    for turn in range(3):
        if args.phase == 'diagnostic' or args.phase == 'screen' and turn != 0 or args.phase == 'repeat' and turn == 0:
            continue
        for backend in (('sqlite','postgres') if turn % 2 == 0 else ('postgres','sqlite')):
            for concurrency in (1,8,32):
                for delay in ((0,1,5),(5,0,1),(1,5,0))[turn]:
                    jobs.append((turn,backend,concurrency,delay,False))
    if args.phase in ('diagnostic','all'):
        for backend in ('sqlite','postgres'):
            for concurrency in (1,8,32):
                for delay in (0,1,5):
                    jobs.append((0,backend,concurrency,delay,True))
    for turn, backend, concurrency, delay, diagnostic in jobs:
        job = f'{backend}-c{concurrency}-d{delay}-r{turn}-' + ('diagnostic' if diagnostic else 'timing')
        if job in done:
            continue
        output = directory/'runs'/job
        if output.exists():
            raise RuntimeError('Incomplete run retained; move aside before retry: ' + str(output))
        case = dict(name=job, family='chain', nodes=10, payload_bytes=16384,
                    calls={1:5,8:16,32:32}[concurrency], concurrency=concurrency,
                    captured_frontier_barrier=True, instrument_sql=diagnostic)
        profile = 'v2-artifacts-8k'
        original = harness.PROFILES[profile]
        harness.PROFILES[profile] = dict(original, wait_mode=args.wait_mode, environment=dict(original['environment'],
            AUTOAGENT_DURABLE_BATCH_DELAY_MS=str(delay)))
        print('RUN', job, flush=True)
        try:
            result = harness.run_case(profile, case, output, backend=backend,
                                      source_root=directory/'source', repeat=turn)
        finally:
            harness.PROFILES[profile] = original
        result.update(job=job, delay_ms=delay, diagnostic=diagnostic, turn=turn, directory=str(output))
        with results.open('a') as stream:
            stream.write(json.dumps(result)+'\n')
        print(json.dumps(dict(job=job,status=result['status'],completed=result['completed'],
            p50=result['latency_ms']['p50'],p95=result['latency_ms']['p95'],seconds=result.get('run_seconds'))), flush=True)
        health = result.get('barrier',{}).get('health',{})
        if (result['status'] != 'completed' or result['completed'] != case['calls'] or result['exit_code'] != 0
            or result.get('shutdown_timeout') or health.get('queued_count') != 0
            or health.get('retained_bytes') != 0 or health.get('fault') is not None
            or health.get('accepted_frontier') != health.get('durable_frontier')):
            raise RuntimeError('Incomplete or non-durable result; inspect ' + str(output))


if __name__ == '__main__':
    main()
