"""Core execution timing and retained memory; sink acknowledges without retention."""
import gc
import json
import statistics
import time
import tracemalloc
from typing_extensions import TypedDict
from autoagent import AutoAgentApp, ConditionContext, Edge, Node, Workflow

class Document(TypedDict):
    rows: list[int]

class Value(TypedDict):
    value: int

async def shrink(value: Document) -> Value:
    return {'value': len(value['rows'])}

async def identity(value: Value) -> Value:
    return value

async def increment(value: Value) -> Value:
    return {'value': value['value'] + 1}

def again(context: ConditionContext) -> bool:
    return context.output['value'] < 100

def finished(context: ConditionContext) -> bool:
    return context.output['value'] >= 100

def loop_workflow():
    return Workflow('loop', nodes=[Node('entry',identity), Node('step',increment), Node('exit',identity)],
        edges=[Edge('entry','step'), Edge('step','step',again,id='back'), Edge('step','exit',finished,id='exit')])

class Sink:
    async def append(self, event):
        pass

def measure(workflow, value, repeats):
    app = AutoAgentApp(runtime_event_sink=Sink())
    app.register_workflow(workflow)
    def run():
        for _ in range(repeats):
            result = app.invoke(workflow.id, value, session_id='bench')
            assert result.status == 'completed', result.error
    try:
        run()
        samples=[]
        for _ in range(5):
            start=time.perf_counter_ns(); run(); samples.append(time.perf_counter_ns()-start)
        gc.collect(); tracemalloc.start()
        run(); gc.collect()
        retained, peak=tracemalloc.get_traced_memory(); tracemalloc.stop()
        return {'median_ns': int(statistics.median(samples)), 'retained_bytes':retained, 'peak_bytes':peak}
    finally:
        app.close()

def main():
    chain=Workflow('large-input-chain', nodes=[Node('entry',shrink)]+[Node(str(i),identity) for i in range(20)],
        edges=[Edge('entry','0')]+[Edge(str(i),str(i+1)) for i in range(19)])
    small=Workflow('repeat',nodes=[Node('entry',identity)])
    print(json.dumps({'method':'5 timing samples; separate allocation run; same session; non-retaining ACK sink',
        'chain_50000':measure(chain,{'rows':list(range(50000))},1),
        'repeat_100':measure(small,{'value':1},100),
        'loop_100':measure(loop_workflow(),{'value':0},1)},indent=2))

if __name__=='__main__': main()
