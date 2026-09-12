"""Read-only Core audit: synthetic scaling probes and real runtime-thread profiles."""
from __future__ import annotations
import cProfile
import gc
import json
import pstats
import statistics
import time
import tracemalloc
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing_extensions import TypedDict
from autoagent import AutoAgentApp, ConditionContext, ContextOperation, Edge, InputMappingContext, Map, Node, Workflow
from autoagent.core.runtime import NodeCompleted, OccurrencePlan, SchedulerDelta, StateDelta, StateOperation, TransitionPlanner, freeze
from autoagent.core.runtime.operations import apply_runtime_delta
from autoagent.core.runtime.transitions import _apply_context_operations
from autoagent.core.runtime.state import ChildInvocationPlan, ChildUnitState
from autoagent.core.runtime.events import ChildInvocationPhaseChanged
from autoagent.core.operators.contract import ValueContract

OUT=Path('artifacts/core_performance_audit')
class Value(TypedDict):
    value: int
class Document(TypedDict):
    items: list[int]
async def identity(value: Value) -> Value:
    return value
async def increment(value: Value) -> Value:
    return {'value':value['value']+1}
def again(context: ConditionContext) -> bool:
    return context.output['value'] < 300
def finished(context: ConditionContext) -> bool:
    return context.output['value'] >= 300
def items(context: InputMappingContext) -> list[Value]:
    return [{'value':i} for i in range(context.invocation_input['value'])]

def measure(action):
    action()
    samples=[]
    for _ in range(7):
        start=time.perf_counter_ns(); action(); samples.append(time.perf_counter_ns()-start)
    gc.collect();tracemalloc.start()
    try:
        action();peak=tracemalloc.get_traced_memory()[1]
    finally:tracemalloc.stop()
    return {'median_ns':int(statistics.median(samples)), 'peak_bytes':peak}

def micro():
    app=AutoAgentApp()
    try:
        result=app.invoke(Workflow('seed',nodes=[Node('a',identity)]),{'value':1})
        seed=app._repository.state(result.session_id)
    finally:app.close()
    inv=replace(seed.invocation,status='running')
    occ=next(iter(inv.scheduler.occurrences.values()))
    call=next(iter(inv.scheduler.operator_calls.values()))
    planner=TransitionPlanner()
    report={}
    for count in (100,10000,50000):
        scheduler=replace(inv.scheduler,operator_calls=MappingProxyType({str(i):replace(call,id=str(i)) for i in range(count)}))
        state=replace(seed,invocation=replace(inv,scheduler=scheduler))
        delta=StateDelta((StateOperation('replace',('invocation','scheduler','operator_calls','0','execution_duration_ns'),42),))
        def update():
            candidate=apply_runtime_delta(state,delta)
            assert candidate.invocation.scheduler.operator_calls['0'].execution_duration_ns==42
            assert state.invocation.scheduler.operator_calls['0'].execution_duration_ns==call.execution_duration_ns
        report[f'call_field_width_{count}']=measure(update)
        occurrences={str(i):replace(occ,id=str(i)) for i in range(count-1)}
        occurrences[occ.id]=replace(occ,status='running')
        state=replace(seed,invocation=replace(inv,scheduler=replace(inv.scheduler,occurrences=MappingProxyType(occurrences))))
        report[f'node_complete_history_{count}']=measure(lambda:planner.plan(state,NodeCompleted(occ.id,{'value':1}),
            occurred_at_us=999,invocation_id=inv.id,session_id=seed.session.id,scheduler_delta=SchedulerDelta()))
        report[f'node_complete_forward_history_{count}']=measure(lambda:planner.plan(state,NodeCompleted(occ.id,{'value':1}),
            occurred_at_us=999,invocation_id=inv.id,session_id=seed.session.id,
            scheduler_delta=SchedulerDelta(ready=(OccurrencePlan('next@root','next',()),))))
        context=freeze({str(i):i for i in range(count)})
        revisions=MappingProxyType({(str(i),):1 for i in range(count)})
        report[f'empty_patch_revisions_{count}']=measure(lambda:_apply_context_operations(context,revisions,(),2,3))
        report[f'one_patch_revisions_{count}']=measure(lambda:_apply_context_operations(context,revisions,(ContextOperation.set('0',-1),),2,3))
        units=tuple(ChildUnitState(i,f's{i}',f'i{i}',None) for i in range(count))
        plan=ChildInvocationPlan('p',occ.id,'await','child','revision',units)
        state=replace(seed,invocation=replace(inv,child_plans=MappingProxyType({'p':plan})))
        report[f'child_phase_units_{count}']=measure(lambda:planner.plan(state,ChildInvocationPhaseChanged('p',0,'opened'),
            occurred_at_us=999,session_id=seed.session.id,invocation_id=inv.id))
    context=freeze({str(i):i for i in range(10000)})
    for count in (1,10,100):
        operations=tuple(ContextOperation.set(str(i),-1) for i in range(count))
        report[f'patch_10000keys_{count}ops']=measure(lambda:_apply_context_operations(context,{},operations,2,3))
    contract=ValueContract.create(Document,location="audit")
    data={'items':list(range(50000))}
    report['restore_list_50000']=measure(lambda:contract.restore(data))
    report['validate_python_list_50000_reference_only']=measure(lambda:contract.validate(data))
    return report

def profile_workflow(name,workflow,value):
    app=AutoAgentApp(max_operator_concurrency=32)
    app.register_workflow(workflow)
    async def run():
        profiler=cProfile.Profile()
        profiler.enable()
        try:
            result=await app._invoke(workflow.id,value,session_id=name,session_context=None,entry_node_id=None,wait_for_boundary=True)
            assert result.status=='completed',result.error
        finally:profiler.disable()
        profiler.dump_stats(str(OUT/f'{name}.prof'))
        with (OUT/f'{name}_profile.txt').open('w') as output:
            stats=pstats.Stats(profiler,stream=output)
            stats.strip_dirs().sort_stats('cumulative').print_stats(40)
            stats.sort_stats('tottime').print_stats(30)
        return {'profile_total_seconds':sum(v[2] for v in pstats.Stats(profiler).stats.values()),
            'occurrences':len(app._repository.state(name).invocation.scheduler.occurrences)}
    try:return app._runtime_loop.run(run())
    finally:app.close()

def main():
    OUT.mkdir(parents=True,exist_ok=True)
    report={'method':'Synthetic micro probes: median of 7, separate tracemalloc peak. Profiles run on actual runtime thread; no sink; async trivial operators.', 'micro':micro()}
    print('micro complete',flush=True)
    chain=Workflow('audit-chain',nodes=[Node(str(i),identity) for i in range(400)],edges=[Edge(str(i),str(i+1)) for i in range(399)])
    loop=Workflow('audit-loop',nodes=[Node('entry',identity),Node('step',increment),Node('exit',identity)],edges=[Edge('entry','step'),Edge('step','step',again,id='back'),Edge('step','exit',finished,id='exit')])
    mapped=Workflow('audit-map',nodes=[Node('map',identity,input_mapping=items,map=Map(max_parallelism=32))])
    report['profiles']={name:profile_workflow(name,workflow,value) for name,workflow,value in [('chain400',chain,{'value':1}),('loop300',loop,{'value':0}),('map2000',mapped,{'value':2000})]}
    (OUT/'results.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
if __name__=='__main__':main()
