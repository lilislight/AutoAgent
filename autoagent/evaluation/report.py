"""Atomic JSON reports, incremental Trial records and conservative Run comparisons."""

from collections import Counter
import json
import os
from pathlib import Path
import tempfile

from .models import EvaluationError


SCHEMA_VERSION = 1


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            json.dump(value, stream, ensure_ascii=False, allow_nan=False, indent=2)
            stream.write('\n')
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def summary(results, planned):
    counts = Counter(result['verdict'] for result in results)
    return dict(planned=planned, finished=len(results), remaining=planned - len(results),
        **{name: counts[name] for name in ('passed', 'failed', 'error', 'ungraded', 'interrupted')})


def save_run(directory, report):
    manifest = dict(report, results=[{key: result[key] for key in
        ('case_id', 'trial', 'verdict', 'result_file')} for result in report['results']])
    write_json(Path(directory) / 'report.json', manifest)


def load_report(path):
    path = Path(path).expanduser().resolve()
    if path.is_dir():
        path /= 'report.json'
    record = json.loads(path.read_text(encoding='utf-8'))
    if record.get('schema_version') != SCHEMA_VERSION or record.get('kind') != 'autoagent.evaluation.run':
        raise EvaluationError('Unsupported Evaluation report.')
    results = []
    for entry in record['results']:
        selected = (path.parent / entry['result_file']).resolve()
        if not selected.is_relative_to(path.parent):
            raise EvaluationError('Trial result path escapes the Run directory.')
        result = json.loads(selected.read_text(encoding='utf-8'))
        if any(result[key] != entry[key] for key in ('case_id', 'trial', 'verdict')):
            raise EvaluationError('Trial result does not match its Run manifest.')
        results.append(result)
    record['results'] = results
    return record


def compare_runs(baseline, candidate):
    """Changed expectations/checkers and incomplete Trials never masquerade as improvements."""
    if not isinstance(baseline, dict):
        baseline = load_report(baseline)
    if not isinstance(candidate, dict):
        candidate = load_report(candidate)
    left, right = baseline['suite'], candidate['suite']
    contract_keys = ('id', 'version', 'scenario', 'evaluators', 'fixture')
    contract_equal = all(left.get(key) == right.get(key) for key in contract_keys)
    def complete_identity(suite):
        identities = [suite['scenario'], *suite['evaluators']]
        if suite.get('fixture'):
            identities.append(suite['fixture'])
        return all(identity.get('identity_complete', False) for identity in identities)
    identity_known = complete_identity(left) and complete_identity(right)
    cases = []
    for case_id in sorted(set(baseline['cases']) | set(candidate['cases'])):
        old, new = baseline['cases'].get(case_id), candidate['cases'].get(case_id)
        before = [item for item in baseline['results'] if item['case_id'] == case_id]
        after = [item for item in candidate['results'] if item['case_id'] == case_id]
        a, b = Counter(item['verdict'] for item in before), Counter(item['verdict'] for item in after)
        comparable = contract_equal and old is not None and old == new
        complete = (len(before) == baseline['options']['repeats'] and len(after) == candidate['options']['repeats']
            and all(item['verdict'] in ('passed', 'failed') for item in before + after))
        if not comparable:
            outcome = 'not_comparable'
        elif not complete or not identity_known or baseline['status'] != 'completed' or candidate['status'] != 'completed':
            outcome = 'inconclusive'
        elif b['failed'] and not a['failed']:
            outcome = 'regressed'
        elif a['failed'] and not b['failed']:
            outcome = 'improved'
        elif not a['failed'] and not b['failed']:
            outcome = 'unchanged_passed'
        else:
            outcome = 'still_failing'
        cases.append(dict(case_id=case_id, outcome=outcome, baseline=dict(a), candidate=dict(b)))
    return dict(schema_version=SCHEMA_VERSION, kind='autoagent.evaluation.comparison',
        baseline_run_id=baseline['run_id'], candidate_run_id=candidate['run_id'], cases=cases,
        environment_changed=baseline.get('environment') != candidate.get('environment'),
        metadata_changed=left.get('metadata') != right.get('metadata'),
        host_factory_changed=left.get('host_factory') != right.get('host_factory'),
        ok=bool(cases) and all(item['outcome'] in ('unchanged_passed', 'improved') for item in cases))
