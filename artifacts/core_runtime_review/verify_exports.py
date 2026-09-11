"""Verify exported files without executing Workflow code; build combined views."""
import json
import sys
from pathlib import Path

OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(OUT.parents[1]))
from autoagent.core.runtime import RuntimeEvent, RuntimeState, StateReducer, SessionCheckpoint

cases = ('approved_recovery_child', 'approval_rejected', 'manual_review')
all_records = []
timelines = []
for case in cases:
    records = json.loads((OUT / case / 'runtime_events.json').read_text())
    line_records = [json.loads(line) for line in (OUT / case / 'runtime_events.jsonl').read_text().splitlines()]
    assert records == line_records
    all_records.extend(records)
    timelines.append(f'=== {case}: {len(records)} Events ===\n' + (OUT / case / 'timeline.txt').read_text())

records = json.loads((OUT / cases[0] / 'runtime_events.json').read_text())
events = [RuntimeEvent.from_record(record) for record in records]
parent = [event for event in events if event.session_id == 'core-api-demo-session']
state = RuntimeState()
ready_sequence = None
start_sequences = {}
for event in parent:
    state = StateReducer().apply(state, event)
    if event.event_name == 'node_occurrence.completed' and event.payload.occurrence_id == 'prepare.approval@root':
        assert state.invocation.scheduler.occurrences['risk@root'].status == 'ready'
        assert state.invocation.scheduler.occurrences['inventory@root'].status == 'ready'
        ready_sequence = event.sequence
    if event.event_name == 'node_occurrence.started':
        start_sequences.setdefault(event.payload.occurrence_id, []).append(event.sequence)
assert ready_sequence is not None
assert len(start_sequences['prepare.approval@root']) == 1, 'Resume must not start the same Node twice'
assert all(len(start_sequences[node]) == 1 and start_sequences[node][0] > ready_sequence for node in ('risk@root', 'inventory@root'))
assert start_sequences['risk@root'] != start_sequences['inventory@root']
assert len([event for event in parent if event.event_name == 'operator_call.started' and event.payload.occurrence_id == 'prepare.normalize@root']) == 1
checkpoint = SessionCheckpoint.from_record(json.loads((OUT / cases[0] / 'parent_checkpoint.json').read_text()))
suffix_state = StateReducer().reduce([event for event in parent if event.sequence > checkpoint.sequence], state=checkpoint.state)
assert suffix_state == state
assert state.invocation.status == 'completed'
children = {event.session_id for event in events} - {'core-api-demo-session'}
assert len(children) == 1
for sid in children:
    child = StateReducer().reduce([event for event in events if event.session_id == sid])
    assert child.invocation.status == 'completed'
    assert child.invocation.output['stored'] is True

summary = {
    'total_events': len(all_records),
    'json_equals_jsonl': True,
    'approval_completion_makes_both_branches_ready_at_sequence': ready_sequence,
    'separate_node_start_sequences': {node: start_sequences[node] for node in ('risk@root', 'inventory@root')},
    'wait_resume_does_not_repeat_node_start': True,
    'checkpoint_recovery_does_not_repeat_normalize_call': True,
    'checkpoint_plus_suffix_equals_full_replay': True,
    'parent_and_child_completed': True,
}
(OUT / 'runtime_events.json').write_text(json.dumps(all_records, ensure_ascii=False, indent=2) + '\n')
(OUT / 'runtime_events.jsonl').write_text(''.join(json.dumps(record, ensure_ascii=False) + '\n' for record in all_records))
(OUT / 'timeline.txt').write_text('\n'.join(timelines))
(OUT / 'semantic_checks.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
print(json.dumps(summary, ensure_ascii=False, indent=2))
