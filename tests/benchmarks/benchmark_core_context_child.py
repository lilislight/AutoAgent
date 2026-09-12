"""Read-only Core measurements; correctness/instrumentation run outside timings.

Run: .venv/bin/python -m tests.benchmarks.benchmark_core_context_child
"""
import gc
import json
import platform
import statistics
import sys
import time
import tracemalloc
from collections import defaultdict
from dataclasses import replace
from types import CodeType
from unittest.mock import patch

from autoagent import (AutoAgentApp, ContextOperation, ContextPatch, Map, Node,
                       SessionCheckpoint, Wait, Workflow, OutputBindingContext)
from autoagent.core.runtime import RuntimeEvent, RuntimeState, StateReducer
from tests.benchmarks.benchmark_core_audit import Value, identity, items


def bind_hot(_context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(session=(ContextOperation.set('hot', 1),))


def bind_empty(_context: OutputBindingContext) -> ContextPatch:
    return ContextPatch()


class Sink:
    def __init__(self):
        self.events = []

    async def append(self, event):
        self.events.append(event)


def context_fixture(count, sink=None, empty=False):
    workflow = Workflow('context-history', nodes=[Node('write', identity, output_binding=bind_empty if empty else bind_hot)])
    source = AutoAgentApp()
    try:
        result = source.invoke(workflow, {'value': 1}, session_id='context-bench')
        seed = source._repository.state(result.session_id)
    finally:
        source.close()
    # Synthetic history of deleted independent keys; fixed live Context size.
    state = replace(seed, session=replace(seed.session,
        context_path_revisions={(f'old{i}',): 1 for i in range(count)}))
    app = AutoAgentApp(runtime_event_sink=sink)
    app.register_workflow(workflow)
    app.load_checkpoint(SessionCheckpoint.from_state(state))
    return app, workflow, state


def context_case(count, empty=False):
    app, workflow, seed = context_fixture(count, empty=empty)
    def run():
        result = app.invoke(workflow.id, {'value': 1}, session_id=seed.session.id)
        assert result.status == 'completed' and result.output == {'value': 1}
    try:
        run()  # Exclude cold representation conversion.
        samples = []
        for _ in range(7):
            start = time.perf_counter_ns()
            run()
            samples.append((time.perf_counter_ns() - start) / 1e6)
        gc.collect()
        tracemalloc.start()
        try:
            run()
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        assert len(seed.session.context_path_revisions) == count
        return {'samples_ms': samples, 'median_ms': statistics.median(samples),
                'incremental_peak_bytes': peak}
    finally:
        app.close()


def child_fixture(count, sink=None):
    child = Workflow('wait-child', nodes=[Node('approval', Wait(Value, Value))])
    parent = Workflow('wait-parent', nodes=[Node('children', child, input_mapping=items,
                                               map=Map(max_parallelism=32))])
    app = AutoAgentApp(max_operator_concurrency=32, runtime_event_sink=sink)
    result = app.invoke(parent, {'value': count})
    assert result.status == 'waiting'
    children = [app.join(ref) for ref in app.child_invocations(result.ref)]
    assert len(children) == count and all(c.status == 'waiting' for c in children)
    return app, result, children


def finish_children(app, parent, children):
    for index, child in enumerate(children):
        result = app.resume(child.ref, child.waits[0].id, {'value': index})
        assert result.status == 'completed' and result.output == {'value': index}
    result = app.join(parent.ref)
    assert result.status == 'completed', result.error
    assert result.output == [{'value': i} for i in range(len(children))]
    state = app._repository.state(parent.session_id)
    assert all(unit.phase == 'terminal' for plan in state.invocation.child_plans.values()
               for unit in plan.units)


def child_case(count):
    samples = []
    peak = None
    # Independent Apps: warmup, 5 timing rounds, separate memory round.
    for round_index in range(7):
        app, parent, children = child_fixture(count)
        try:
            if round_index == 6:
                gc.collect()
                tracemalloc.start()
            start = time.perf_counter_ns()
            finish_children(app, parent, children)
            elapsed = (time.perf_counter_ns() - start) / 1e6
            if 1 <= round_index <= 5:
                samples.append(elapsed)
            if round_index == 6:
                peak = tracemalloc.get_traced_memory()[1]
        finally:
            if tracemalloc.is_tracing():
                tracemalloc.stop()
            app.close()
    return {'samples_ms': samples, 'median_ms': statistics.median(samples),
            'incremental_peak_bytes': peak}


def check_replay(app, sink, initial=None):
    groups = defaultdict(list)
    for event in sink.events:
        restored = RuntimeEvent.from_record(event.to_record())
        assert restored.to_record() == event.to_record()
        groups[event.session_id].append(restored)
    for sid, events in groups.items():
        actual = app._repository.state(sid)
        replayed = StateReducer().reduce(tuple(events), (initial or {}).get(sid))
        assert replayed.to_record() == actual.to_record()
        assert RuntimeState.from_record(actual.to_record()).to_record() == actual.to_record()
    return {'sessions': len(groups), 'events': len(sink.events)}


def verify():
    sink = Sink()
    app, workflow, seed = context_fixture(10000, sink)
    try:
        baseline = app._repository.state(seed.session.id)
        result = app.invoke(workflow.id, {'value': 1}, session_id=seed.session.id)
        assert result.status == 'completed'
        context = check_replay(app, sink, {seed.session.id: baseline})
        assert len(baseline.session.context_path_revisions) == 10000
    finally:
        app.close()
    sink = Sink()
    app, parent, children = child_fixture(32, sink)
    try:
        finish_children(app, parent, children)
        child = check_replay(app, sink)
    finally:
        app.close()
    return {'context_replay': context, 'child_replay': child}


def count_child_scans(count):
    app, parent, children = child_fixture(count)
    reads = 0
    # Only generator expressions directly inside _settle_child; currently the
    # full child-state snapshot. No instrumentation in latency/memory rounds.
    codes = {code for code in app._settle_child.__func__.__code__.co_consts
             if isinstance(code, CodeType) and code.co_name == '<genexpr>'}
    original = app._repository.state
    def counted(sid):
        nonlocal reads
        if sys._getframe(1).f_code in codes:
            reads += 1
        return original(sid)
    try:
        with patch.object(app._repository, 'state', counted):
            finish_children(app, parent, children)
        return reads
    finally:
        app.close()


def main():
    report = {'python': sys.version, 'platform': platform.platform(),
              'method': 'Public synchronous API; null sink; setup/close excluded; tracemalloc separate; no Core edits.'}
    report['correctness'] = verify()
    report['context'] = {}
    for count in (0, 1000, 10000, 50000):
        report['context'][count] = context_case(count)
        print(f'context {count} finished', file=sys.stderr, flush=True)
    report['empty_context_control'] = {count: context_case(count, empty=True)
                                       for count in (0, 1000, 10000, 50000)}
    report['children'] = {}
    for count in (10, 100, 300, 1000):
        report['children'][count] = child_case(count)
        report['children'][count]['settle_child_snapshot_state_reads'] = count_child_scans(count)
        print(f'children {count} finished', file=sys.stderr, flush=True)
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
