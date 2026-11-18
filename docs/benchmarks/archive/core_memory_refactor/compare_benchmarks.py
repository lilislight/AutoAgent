"""Build the comparison from isolated baseline and current benchmark outputs."""
import json
from pathlib import Path

root = Path(__file__).resolve().parent
report = {
    'baseline_commit': '91f0c98c5baed8d23ce917f4631db1d43a2cfab0',
    'baseline_source': 'git archive of baseline commit, run from /tmp/autoagent-memory-before',
    'interpreter': '/home/chengqian/projects/AutoAgent/.venv/bin/python',
    'comparisons': {},
}
for suite in ('focused', 'full'):
    before = json.loads((root / f'{suite}_before_isolated.json').read_text())
    after = json.loads((root / f'{suite}_after.json').read_text())
    comparisons = {}
    def collect(left, right, path=''):
        for key, value in left.items():
            if key not in right:
                continue
            if isinstance(value, dict):
                collect(value, right[key], path + key + '/')
            elif isinstance(value, (int, float)) and value > 0 and (key.endswith('duration_ns') or key == 'peak_bytes'):
                comparisons[path + key] = {
                    'before': value, 'after': right[key],
                    'reduction_percent': round((1 - right[key] / value) * 100, 2),
                    'before_divided_by_after': round(value / right[key], 3),
                }
    collect(before, after)
    report['comparisons'][suite] = comparisons
(root / 'comparison.json').write_text(json.dumps(report, indent=2) + '\n')
