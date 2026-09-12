"""Comparable pre/post measurements: steady immutable updates and real RuntimeLoop dispatch."""
import json
from dataclasses import replace
from types import MappingProxyType
from autoagent import AutoAgentApp, ContextOperation, Map, Node, Workflow
from autoagent.core.runtime import StateDelta, StateOperation, TransitionPlanner
from autoagent.core.runtime.operations import apply_runtime_delta
from autoagent.core.runtime.state import ChildInvocationPlan, ChildUnitState
from autoagent.core.runtime.events import ChildInvocationPhaseChanged
from autoagent.core.runtime.transitions import _apply_context_operations
from autoagent.core.runtime.values import freeze
from autoagent.core.operators.contract import ValueContract
from autoagent.core.executor.node_executor import _run_sync
from tests.benchmarks.benchmark_core_audit import Value, Document, identity, items, measure


def main():
    app=AutoAgentApp()
    try:
        result=app.invoke(Workflow('seed',nodes=[Node('a',identity)]),{'value':1})
        seed=app._repository.state(result.session_id)
    finally:app.close()
    inv=replace(seed.invocation,status='running')
    call=next(iter(inv.scheduler.operator_calls.values()))
    occ=next(iter(inv.scheduler.occurrences.values()))
    planner=TransitionPlanner();report={}
    for count in (100,10000,50000):
        state=replace(seed,invocation=replace(inv,scheduler=replace(inv.scheduler,
            operator_calls=MappingProxyType({str(i):replace(call,id=str(i)) for i in range(count)}))))
        delta=StateDelta((StateOperation('replace',('invocation','scheduler','operator_calls','0','execution_duration_ns'),42),))
        report[f'cold_call_update_{count}']=measure(lambda:apply_runtime_delta(state,delta))
        state=apply_runtime_delta(state,delta) # Pay representation conversion once, outside steady measurement.
        delta=StateDelta((StateOperation('replace',('invocation','scheduler','operator_calls','1','execution_duration_ns'),43),))
        report[f'steady_call_update_{count}']=measure(lambda:apply_runtime_delta(state,delta))
        units=tuple(ChildUnitState(i,f's{i}',f'i{i}',None) for i in range(count))
        plan=ChildInvocationPlan('p',occ.id,'await','w','r',units)
        state=replace(seed,invocation=replace(inv,child_plans=MappingProxyType({'p':plan})))
        delta=planner.plan(state,ChildInvocationPhaseChanged('p',0,'opened'),occurred_at_us=999,
            session_id=seed.session.id,invocation_id=inv.id)
        state=apply_runtime_delta(state,delta)
        report[f'steady_child_phase_{count}']=measure(lambda:planner.plan(state,ChildInvocationPhaseChanged('p',1,'opened'),
            occurred_at_us=1000,session_id=seed.session.id,invocation_id=inv.id))
    context=freeze({str(i):i for i in range(10000)})
    ops=tuple(ContextOperation.set(str(i),-1) for i in range(100))
    report['context_10000keys_100ops']=measure(lambda:_apply_context_operations(context,{},ops,2,3))
    revisions=MappingProxyType({(str(i),):1 for i in range(50000)})
    report['empty_context_50000revisions']=measure(lambda:_apply_context_operations(context,revisions,(),2,3))
    contract=ValueContract.create(Document,location='benchmark')
    restore=getattr(contract,'_restore_internal',contract.restore)
    report['internal_restore_50000']=measure(lambda:restore({'items':list(range(50000))}))
    app=AutoAgentApp()
    async def dispatch():
        for _ in range(500):
            assert await _run_sync(app._node_executor._pool,lambda:1)==1
    try:report['runtime_dispatch_500']=measure(lambda:app._runtime_loop.run(dispatch()))
    finally:app.close()
    for size in (100,1000,5000):
        app=AutoAgentApp(max_operator_concurrency=32)
        workflow=Workflow(f'map-{size}',nodes=[Node('map',identity,input_mapping=items,map=Map(max_parallelism=32))])
        app.register_workflow(workflow)
        def run():
            result=app.invoke(workflow.id,{'value':size},session_id='bench')
            assert result.status=='completed',result.error
            assert len(result.output)==size
        try:report[f'live_map_{size}']=measure(run)
        finally:app.close()
    print(json.dumps(report,indent=2))
if __name__=='__main__':main()
