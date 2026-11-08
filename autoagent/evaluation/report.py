"""Portable business reports and comparisons that keep target code separate from test conditions."""

from collections import Counter
from copy import deepcopy
import json
import os
from pathlib import Path
import tempfile

from .models import EvaluationError

SCHEMA_VERSION = 2


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
        if os.path.exists(temporary): os.unlink(temporary)


def summary(results, planned):
    counts = Counter(result['verdict'] for result in results)
    return dict(planned=planned, finished=len(results), remaining=planned - len(results),
        **{name: counts[name] for name in ('passed', 'failed', 'error', 'ungraded', 'interrupted')})


def save_run(directory, report):
    manifest = dict(report, results=[{key: result[key] for key in ('case_id', 'trial', 'verdict', 'result_file')}
        for result in report['results']])
    write_json(Path(directory) / 'report.json', manifest)


def _validate(record):
    if not isinstance(record, dict) or record.get('schema_version') != SCHEMA_VERSION or record.get('kind') != 'autoagent.evaluation.run':
        raise EvaluationError('Unsupported Evaluation report.')
    required = ('suite', 'cases', 'options', 'results', 'status', 'conditions', 'run_id')
    if any(key not in record for key in required): raise EvaluationError('Incomplete Evaluation report.')
    seen = set()
    for result in record['results']:
        identity = result['case_id'], result['trial']
        if identity in seen or identity[0] not in record['cases'] or type(identity[1]) is not int or not 1 <= identity[1] <= record['options']['repeats']:
            raise EvaluationError('Trial identity is inconsistent with its Run.')
        seen.add(identity)
        if result.get('case_fingerprint') != record['cases'][identity[0]]:
            raise EvaluationError('Trial ground truth does not match its Run.')
        names = [check['name'] for check in result['checks']]
        if len(names) != len(set(names)): raise EvaluationError('Duplicate business rule names in a Trial.')
        verdict = result.get('verdict')
        if verdict not in ('passed', 'failed', 'error', 'ungraded', 'interrupted'):
            raise EvaluationError('Unknown Trial verdict.')
        if any(type(check.get('passed')) not in (bool, type(None)) for check in result['checks']):
            raise EvaluationError('Business rule verdict must be true, false or null.')
        if verdict == 'passed' and (not result['checks'] or not all(check['passed'] is True for check in result['checks'])
                or result.get('errors') or not result.get('cleanup', {}).get('ok') or not result.get('roots')):
            raise EvaluationError('Passed Trial contradicts its checks, execution or cleanup evidence.')
    return record


def load_report(path):
    path = Path(path).expanduser().resolve()
    if path.is_dir(): path /= 'report.json'
    record = json.loads(path.read_text(encoding='utf-8'))
    # CLI documents carry the same current report model, with expanded Trial records.
    if isinstance(record, dict) and str(record.get('command', '')).startswith('eval.'):
        record = record.get('data')
    if not isinstance(record, dict) or record.get('schema_version') != SCHEMA_VERSION or record.get('kind') != 'autoagent.evaluation.run':
        raise EvaluationError('Unsupported Evaluation report.')
    results = []
    for entry in record['results']:
        if 'checks' in entry:
            results.append(entry)
            continue
        selected = (path.parent / entry['result_file']).resolve()
        if not selected.is_relative_to(path.parent): raise EvaluationError('Trial result path escapes the Run directory.')
        result = json.loads(selected.read_text(encoding='utf-8'))
        if any(result[key] != entry[key] for key in ('case_id', 'trial', 'verdict')):
            raise EvaluationError('Trial result does not match its Run manifest.')
        results.append(result)
    record['results'] = results
    return _validate(record)


def _known(suite):
    dependencies = [suite['scenario'], *suite['evaluators']]
    if suite.get('fixture'): dependencies.append(suite['fixture'])
    return all(item.get('identity_complete', False) for item in dependencies)


def _rule_counts(trials):
    rules = {}
    for trial in trials:
        for check in trial['checks']:
            rules.setdefault(check['name'], Counter())['passed' if check['passed'] is True else 'failed' if check['passed'] is False else 'ungraded'] += 1
    return rules


def compare_runs(baseline, candidate):
    baseline = _validate(deepcopy(baseline)) if isinstance(baseline, dict) else load_report(baseline)
    candidate = _validate(deepcopy(candidate)) if isinstance(candidate, dict) else load_report(candidate)
    left, right = baseline['suite'], candidate['suite']
    contract_equal = all(left.get(key) == right.get(key) for key in ('id', 'version', 'scenario', 'evaluators', 'fixture'))
    condition_known = baseline['conditions'].get('declared') is not None and candidate['conditions'].get('declared') is not None
    condition_equal = baseline['conditions'] == candidate['conditions']
    cases = []
    for case_id in sorted(set(baseline['cases']) | set(candidate['cases'])):
        old, new = baseline['cases'].get(case_id), candidate['cases'].get(case_id)
        before = [item for item in baseline['results'] if item['case_id'] == case_id]
        after = [item for item in candidate['results'] if item['case_id'] == case_id]
        a, b = Counter(item['verdict'] for item in before), Counter(item['verdict'] for item in after)
        previous, current = _rule_counts(before), _rule_counts(after)
        rules = []
        for name in sorted(previous.keys() | current.keys()):
            pa, pb = previous.get(name, Counter()), current.get(name, Counter())
            outcome = ('not_observed' if not pb else 'new_coverage' if not pa else
                'inconclusive' if pa['ungraded'] or pb['ungraded'] else
                'new_failure' if pb['failed'] and not pa['failed'] else 'fixed' if pa['failed'] and not pb['failed'] else
                'still_failing' if pa['failed'] or pb['failed'] else 'unchanged_passed')
            rules.append(dict(name=name, outcome=outcome, baseline=dict(pa), candidate=dict(pb)))
        complete = (len(before) == baseline['options']['repeats'] and len(after) == candidate['options']['repeats']
            and all(item['verdict'] in ('passed', 'failed') for item in before + after)
            and not (previous.keys() - current.keys()))
        if not contract_equal or old is None or old != new:
            outcome = 'not_comparable'
        elif condition_known and not condition_equal:
            outcome = 'not_comparable'
        elif not condition_known or not complete or not (_known(left) and _known(right)) or baseline['status'] != 'completed' or candidate['status'] != 'completed':
            outcome = 'inconclusive'
        elif b['failed'] and not a['failed']: outcome = 'regressed'
        elif a['failed'] and not b['failed']: outcome = 'improved'
        elif not a['failed'] and not b['failed']: outcome = 'unchanged_passed'
        else: outcome = 'still_failing'
        cases.append(dict(case_id=case_id, outcome=outcome, baseline=dict(a), candidate=dict(b), rules=rules))
    return dict(schema_version=SCHEMA_VERSION, kind='autoagent.evaluation.comparison',
        baseline_run_id=baseline['run_id'], candidate_run_id=candidate['run_id'], cases=cases,
        conditions_known=condition_known, conditions_changed=not condition_equal,
        environment_changed=baseline.get('environment') != candidate.get('environment'),
        metadata_changed=left.get('metadata') != right.get('metadata'), host_factory_changed=left.get('host_factory') != right.get('host_factory'),
        ok=bool(cases) and all(item['outcome'] in ('unchanged_passed', 'improved') for item in cases))
