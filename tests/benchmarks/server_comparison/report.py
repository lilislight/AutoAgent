"""Validate business parity and export compact tables and standalone figures."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import json
from pathlib import Path
import statistics
import pstats

from .run import percentile

MIB = 1024**2


def canonical_output(value):
    if isinstance(value, dict) and set(value) == {'output'}:
        return value['output']
    return value


def load(runs):
    results = []
    expected = {}
    for run in runs:
        for line in (run / 'results.jsonl').read_text().splitlines():
            row = json.loads(line)
            row['suite'] = run.name
            row['backend'] = row.get('backend', 'sqlite')
            directory = run / f"{row['case']['name']}-{row['profile']}-{row['repeat']}"
            row['directory'] = str(directory.resolve())
            output = canonical_output(row.get('expected_output'))
            if output is not None:
                key = row['case']['name']
                if key in expected and expected[key] != output:
                    raise AssertionError(f'Business output mismatch: {run}/{key}/{row["profile"]}')
                expected[key] = output
            results.append(row)
    return results, expected


def aggregate(results):
    groups = defaultdict(list)
    for row in results:
        groups[row['suite'], row['backend'], row['case']['name'], row['profile']].append(row)
    summaries = []
    for (suite, backend, case, profile), rows in sorted(groups.items()):
        completed = [row for row in rows if row['status'] == 'completed']
        calls = []
        for row in completed:
            path = Path(row['directory']) / 'calls.jsonl'
            if path.exists():
                calls.extend(json.loads(line) for line in path.read_text().splitlines())
        times = [row['latency_ms'] for row in calls if not row.get('rejected')]
        sizes = [row['closed_storage']['total_bytes'] for row in completed]
        summaries.append(dict(suite=suite, backend=backend, case=case, profile=profile,
            runs=len(rows), successful_runs=len(completed), statuses=dict(Counter(row['status'] for row in rows)),
            calls=sum(row['completed'] for row in rows), latency_samples=len(times),
            p50_ms=percentile(times, .5), p95_ms=percentile(times, .95), p99_ms=percentile(times, .99),
            peak_rss_mib=statistics.median(row['rss_bytes'] / MIB for row in rows),
            closed_storage_mib=statistics.median(sizes) / MIB if sizes else None,
            startup_seconds=statistics.median(row['startup_seconds'] for row in rows if 'startup_seconds' in row) if any('startup_seconds' in row for row in rows) else None,
            cpu_seconds=statistics.median(row['cpu_seconds'] for row in completed if 'cpu_seconds' in row) if any('cpu_seconds' in row for row in completed) else None))
    return summaries


def timeline(directory):
    points = []
    last = -5
    with (directory / 'memory.jsonl').open() as source:
        for line in source:
            row = json.loads(line)
            if row['seconds'] - last >= 5:
                points.append(row)
                last = row['seconds']
    return points


def soak_summary(row):
    points = timeline(Path(row['directory']))
    running = [point for point in points if point['phase'] == 'running']
    tail = [point for point in running if point['seconds'] >= running[-1]['seconds'] - 600] if running else []
    slope = None
    if len(tail) > 1:
        x = [point['seconds'] / 60 for point in tail]
        y = [point['rss_bytes'] / MIB for point in tail]
        center_x, center_y = statistics.mean(x), statistics.mean(y)
        denominator = sum((value - center_x)**2 for value in x)
        slope = sum((a - center_x) * (b - center_y) for a, b in zip(x, y)) / denominator if denominator else None
    return dict(profile=row['profile'], status=row['status'], calls=row['completed'],
                elapsed_seconds=points[-1]['seconds'] if points else None,
                first_running_rss_mib=running[0]['rss_bytes'] / MIB if running else None,
                last_running_rss_mib=running[-1]['rss_bytes'] / MIB if running else None,
                peak_rss_mib=row['rss_bytes'] / MIB,
                tail_slope_mib_per_minute=slope,
                storage_mib=row['closed_storage']['total_bytes'] / MIB)


def diagnostics(results):
    output = []
    for row in results:
        record = dict(suite=row['suite'], case=row['case']['name'], profile=row['profile'], status=row['status'])
        found = False
        if 'sql_before' in row and 'sql_after' in row:
            found = True
            before, after = row['sql_before'], row['sql_after']
            record['sql'] = {key: after[key] - before[key] for key in ('statements', 'cursor_ms', 'commits')}
            record['sql']['verbs'] = {key: value - before['verbs'].get(key, 0) for key, value in after['verbs'].items()}
        warm, drained = row.get('warm_memory', {}), row.get('drained_memory', {})
        if 'cpu_seconds' in warm and 'cpu_seconds' in drained:
            found = True
            record['running_cpu_seconds'] = drained['cpu_seconds'] - warm['cpu_seconds']
            record['cpu_seconds_per_call'] = record['running_cpu_seconds'] / row['completed'] if row['completed'] else None
        if row['case'].get('profile_cpu'):
            found = True
            profiles = sorted(Path(row['directory']).glob('profile-*.pstats'))
            if profiles:
                stats = pstats.Stats(*[str(path) for path in profiles])
                functions = [dict(file=key[0], line=key[1], function=key[2], calls=value[1], self_cpu_seconds=value[2], cumulative_cpu_seconds=value[3])
                             for key, value in stats.stats.items()]
                record['cpu_profile'] = dict(total_seconds=stats.total_tt,
                    top_self=sorted(functions, key=lambda item: item['self_cpu_seconds'], reverse=True)[:25],
                    top_autoagent_cumulative=sorted((item for item in functions if '/autoagent/' in item['file']), key=lambda item: item['cumulative_cpu_seconds'], reverse=True)[:25])
        if row['case'].get('trace_reads'):
            found = True
            calls = [json.loads(line) for line in (Path(row['directory']) / 'calls.jsonl').read_text().splitlines()]
            reads = [read for call in calls for read in call.get('trace_reads', [])]
            record['trace'] = dict(samples=len(reads), p50_ms=percentile([item['milliseconds'] for item in reads], .5),
                                   p95_ms=percentile([item['milliseconds'] for item in reads], .95),
                                   median_response_bytes=statistics.median(item['response_bytes'] for item in reads) if reads else None)
        if found:
            output.append(record)
    return output


def figures(summaries, results, output):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np

    plt.rcParams.update({'font.size': 10, 'axes.spines.top': False, 'axes.spines.right': False})
    profiles = ['v1-minimal', 'v1-standard', 'v1-full', 'v2-default']
    colors = ['#94a3b8', '#60a5fa', '#f59e0b', '#14b8a6']
    selected = ['chain-10', 'loop-100', 'map-100', 'rows']
    preferred = 'baseline-pinned' if any(row['suite'] == 'baseline-pinned' for row in summaries) else 'baseline'
    baseline = {(row['case'], row['profile']): row for row in summaries if row['suite'] == preferred}
    fig, axes = plt.subplots(1, 3, figsize=(16, 4.8), layout='constrained')
    for ax, field, title, unit in zip(axes,
        ('p50_ms', 'peak_rss_mib', 'closed_storage_mib'),
        ('HTTP completion + durable barrier', 'Whole Server peak RSS', 'Database after Server closes'),
        ('Milliseconds (log scale)', 'MiB', 'MiB (log scale)')):
        for index, (profile, color) in enumerate(zip(profiles, colors)):
            positions = np.arange(len(selected)) + (index - 1.5) * .19
            values = [baseline.get((name, profile), {}).get(field) for name in selected]
            ax.bar(positions, [value if value is not None else 0 for value in values], .18, color=color, label=profile)
            for position, name in zip(positions, selected):
                row = baseline.get((name, profile))
                if row and row['successful_runs'] != row['runs']:
                    ax.text(position, 1 if field != 'peak_rss_mib' else row[field] + 10, '×', color='#b91c1c', ha='center', fontsize=14)
        if field != 'peak_rss_mib':
            ax.set_yscale('log')
        ax.set_xticks(range(len(selected)), selected, rotation=15)
        ax.set_ylabel(unit)
        ax.set_title(title)
        ax.grid(axis='y', alpha=.2)
    axes[0].legend(fontsize=8)
    fig.suptitle('V1 / V2 native Servers · median of independent runs · × = incomplete run(s)')
    fig.savefig(output / 'server-comparison.png', dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(11, 5), layout='constrained')
    for row in results:
        if row['case']['name'] != 'soak':
            continue
        points = [point for point in timeline(Path(row['directory'])) if point['phase'] == 'running']
        ax.plot([point['seconds'] / 60 for point in points], [point['rss_bytes'] / MIB for point in points],
                label=f"{row['profile']} ({row['status']})", linewidth=1.7)
    ax.axhline(640, color='#b91c1c', linestyle='--', linewidth=1, label='External test guard (640 MiB)')
    ax.set_xlabel('Minutes since each Server started')
    ax.set_ylabel('Whole Server RSS (MiB)')
    ax.set_title('Running phase only · target 1 HTTP call/s · fresh Session per call')
    ax.grid(alpha=.2)
    ax.legend(fontsize=8)
    fig.savefig(output / 'soak-rss.png', dpi=160)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runs', nargs='+', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    options = parser.parse_args()
    options.output.mkdir(parents=True, exist_ok=True)
    results, expected = load(options.runs)
    summaries = aggregate(results)
    soak = [soak_summary(row) for row in results if row['case']['name'] == 'soak']
    (options.output / 'summary.json').write_text(json.dumps(dict(
        business_parity_verified=True, expected_outputs=expected, summaries=summaries, soak=soak), indent=2))
    (options.output / 'diagnostics.json').write_text(json.dumps(diagnostics(results), indent=2))
    with (options.output / 'summary.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    figures(summaries, results, options.output)
    print(json.dumps(dict(runs=len(results), groups=len(summaries), verified_business_cases=len(expected),
                          statuses=dict(Counter(row['status'] for row in results)), soak=soak), indent=2))


if __name__ == '__main__':
    main()
