"""Compare fixed-fact Graph/Timeline/Inspector contracts across source snapshots."""
import argparse
import asyncio
from dataclasses import asdict
import json
from pathlib import Path
from typing_extensions import TypedDict

from autoagent import AutoAgentApp, AggregationContext, ConditionContext, Edge, InputMappingContext, Map, Node, Wait, Workflow
from autoagent.core.compiler.snapshot import WorkflowDefinitionSnapshot
from autoagent.core.runtime.events import RuntimeEvent
from autoagent.host.persistence import DatabaseSink, PersistenceConfig


class Value(TypedDict):
    text: str


def identity(value: Value) -> Value: return value

def step(value: Value) -> Value: return {'text':value['text'][1:]}

def again(ctx: ConditionContext) -> bool: return bool(ctx.output['text'])

def finished(ctx: ConditionContext) -> bool: return not ctx.output['text']

def mapped(ctx: InputMappingContext) -> list[Value]: return [ctx.incoming['finish']]*3

def aggregate(ctx: AggregationContext) -> Value: return ctx.outputs[0]


def normalize(value):
    if isinstance(value,dict):
        return {k:normalize(v) for k,v in value.items() if k not in ('observed_at_us','store_id','source_id')}
    if isinstance(value,(list,tuple)): return [normalize(v) for v in value]
    return value


async def seed(path):
    class Capture:
        def __init__(self):self.events=[]
        async def append(self,event):self.events.append(event.to_record())
    capture=Capture();app=AutoAgentApp(runtime_event_sink=capture)
    try:
        wf=Workflow('contract-loop-map',nodes=[Node('entry',identity),Node('step',step),
            Node('mapped',identity,input_mapping=mapped,map=Map(aggregate=aggregate))],
            edges=[Edge('entry','step'),Edge('step','step',again,id='again'),Edge('step','mapped',finished,id='finish')])
        result=await app.ainvoke(wf,{'text':'abcd'},entry_node_id='entry')
        wait=Workflow('contract-wait',nodes=[Node('approval',Wait(Value,Value))])
        pending=await app.ainvoke(wait,{'text':'approve'},entry_node_id='approval')
        await app.aresume(pending.ref,pending.waits[0].id,{'text':'approved'})
        path.write_text(json.dumps(dict(definitions=[app.workflow_definition_snapshot(w.id).to_record() for w in (wf,wait)],
            events=capture.events,invocations=[result.invocation_id,pending.invocation_id])))
    finally:app.close()


async def read(path,output):
    data=json.loads(path.read_text());url='sqlite+aiosqlite:///'+str(output.with_suffix('.db').resolve())
    async with await DatabaseSink.open(url,config=PersistenceConfig(artifact_threshold_bytes=8192)) as sink:
        for definition in data['definitions']:
            await sink.register_workflow(WorkflowDefinitionSnapshot.from_record(definition))
        for event in data['events']:await sink.append(RuntimeEvent.from_record(event))
        results={}
        for iid in data['invocations']:
            nodes=await sink.list_nodes(iid);calls=await sink.list_calls(iid);waits=await sink.list_waits(iid)
            headers=[event for event in data['events'] if event['invocation_id']==iid]
            selected=[event['sequence'] for event in headers if event['event_name'] in ('node_occurrence.started','node_occurrence.waiting','routing.resolved','invocation.completed')]
            trace=await sink.execution_trace(iid)
            item=dict(nodes=nodes.items,calls=calls.items,waits=waits.items,
                changes=(await sink.graph_changes(iid,limit=1000)).items,trace=trace,
                overview=await sink.execution_timeline(iid),
                frames=[await sink.timeline_frame(iid,sequence=sequence) for sequence in selected],
                replay=await sink.timeline_replay(iid,limit=1000),
                node_values=[await sink.node_detail(iid,row['occurrence_id']) for row in nodes.items],
                call_values=[await sink.call_detail(iid,row['call_id']) for row in calls.items])
            # Exercise both frozen manifests and incremental queries, comparing
            # metadata content rather than opaque per-store snapshot identities.
            manifest=await sink.trace_dataset(iid,replay=True)
            item['dataset']={section:[row for page in range(info['pages']) for row in
                (await sink.trace_dataset_page(manifest['snapshot_id'],section,page))['items']]
                for section,info in manifest['sections'].items()}
            results[iid]=item
        output.write_text(json.dumps(normalize(results),sort_keys=True,indent=2))


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('mode',choices=('seed','read'))
    parser.add_argument('fixture',type=Path);parser.add_argument('--output',type=Path)
    args=parser.parse_args();asyncio.run(seed(args.fixture) if args.mode=='seed' else read(args.fixture,args.output))


if __name__=='__main__':main()
