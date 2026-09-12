"""Repeatable live Core timings and allocation peaks, excluding file persistence.

Run from the repository root with the same interpreter before and after changes.
Timing uses five samples without tracemalloc; allocation runs are separate.
"""
from __future__ import annotations

import gc
import json
import platform
import statistics
import time
import tracemalloc
from dataclasses import replace
from types import MappingProxyType
from typing_extensions import TypedDict

from autoagent import AutoAgentApp, ContextOperation, ContextPatch, Edge, Node, OutputBindingContext, Workflow
from autoagent.core.runtime import StateDelta, StateOperation
from autoagent.core.runtime.operations import apply_runtime_delta


class Row(TypedDict):
    index: int
    tags: list[str]


class Document(TypedDict):
    rows: list[Row]


def passthrough(value: Document) -> Document:
    return value


def remember(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(invocation=(ContextOperation.set('document', context.output),))


def _elapsed(action, repetitions=5):
    samples=[]
    for _ in range(repetitions):
        start=time.perf_counter_ns()
        action()
        samples.append(time.perf_counter_ns()-start)
    return int(statistics.median(samples))


def _peak(action):
    gc.collect()
    tracemalloc.start()
    try:
        action()
        return tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()


def pipeline():
    workflow=Workflow('memory-pipeline', nodes=[Node('a',passthrough,output_binding=remember),Node('b',passthrough)],edges=[Edge('a','b')])
    value={'rows':[{'index':i,'tags':['one','two','three']} for i in range(1500)]}
    app=AutoAgentApp()
    app.register_workflow(workflow)
    def run():
        result=app.invoke(workflow.id,value,session_id='bench')
        assert result.status=='completed',result.error
    try:
        run()  # Warm up compilation caches and worker pool.
        elapsed=_elapsed(run)
        peak=_peak(run)
        return {'median_duration_ns':elapsed,'peak_bytes':peak,'rows':len(value['rows'])}
    finally:app.close()


def wide_updates():
    app=AutoAgentApp()
    try:
        result=app.invoke(Workflow('wide-update',nodes=[Node('a',passthrough)]),{'rows':[]})
        state=app._repository.state(result.session_id)
        call=next(iter(state.invocation.scheduler.operator_calls.values()))
        report={}
        for count in (100,10000,50000):
            calls=MappingProxyType({str(i):replace(call,id=str(i)) for i in range(count)})
            baseline=replace(state,invocation=replace(state.invocation,scheduler=replace(state.invocation.scheduler,operator_calls=calls)))
            delta=StateDelta(tuple(StateOperation('replace',('invocation','scheduler','operator_calls','0',field),value) for field,value in (
                ('output',{'rows':[]}),('status','completed'),('completed_at_us',999),('execution_duration_ns',42))))
            def run():
                candidate=apply_runtime_delta(baseline,delta)
                assert candidate.invocation.scheduler.operator_calls['0'].output=={'rows':()}
                assert baseline.invocation.scheduler.operator_calls['0'].completed_at_us==call.completed_at_us
            run()
            report[str(count)]={'median_duration_ns':_elapsed(run),'peak_bytes':_peak(run)}
        return report
    finally:app.close()


def main():
    print(json.dumps({'python':platform.python_version(),'platform':platform.platform(),
        'method':'5 timed samples, median; separate tracemalloc peak; no persistence sink',
        'large_nested_pipeline':pipeline(),'wide_four_field_updates':wide_updates()},indent=2))


if __name__=='__main__':main()
