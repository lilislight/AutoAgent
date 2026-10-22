"""Launch one real native Server in its own process and package namespace."""
import argparse
import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict
import json
from pathlib import Path
import sys
import time
from uuid import UUID

parser = argparse.ArgumentParser()
parser.add_argument('--version', choices=('v1', 'v2'), required=True)
parser.add_argument('--config', required=True)
args = parser.parse_args()
ROOT = Path(__file__).resolve().parents[3]
VERSION_ROOT = ROOT / 'v1' if args.version == 'v1' else ROOT
sys.path.insert(0, str(VERSION_ROOT))
import autoagent
assert Path(autoagent.__file__).resolve().is_relative_to(VERSION_ROOT)
if args.version == 'v2':
    assert not Path(autoagent.__file__).resolve().is_relative_to(ROOT / 'v1')
from business import Payload, Batch, Summary, operations, summarize
from fastapi import HTTPException
import uvicorn

config = json.loads(Path(args.config).read_text())
case = config['case']
entry, advance, finish, batch = operations(case)
family = case.get('family', 'chain')
workflows = []

if args.version == 'v2':
    from autoagent import (Workflow, Node, Edge, Map, ResourcePolicy, InputMappingContext,
                           OutputBindingContext, ConditionContext, ContextPatch, ContextOperation)
    from autoagent.host import AutoAgentHost, HostSettings
    from autoagent.host.persistence import PersistenceConfig
    from autoagent.server import create_app, ServerSettings

    def incoming(context: InputMappingContext) -> Payload:
        value = next(iter(context.incoming.values()))
        return Payload.model_validate(dict(value), strict=False)

    def map_inputs(context: InputMappingContext) -> list[Payload]:
        value = next(iter(context.incoming.values()))
        return [Payload.model_validate(dict(item), strict=False) for item in value['items']]

    from autoagent import AggregationContext
    def aggregate(context: AggregationContext) -> Summary:
        return summarize(context.outputs)

    async def summary_identity(value: Summary) -> Summary:
        return value

    def summary_input(context: InputMappingContext) -> Summary:
        return Summary.model_validate(dict(next(iter(context.incoming.values()))), strict=False)

    def again(context: ConditionContext) -> bool:
        return context.output['step'] < case.get('iterations', 1)

    def done(context: ConditionContext) -> bool:
        return not again(context)

    def bind(context: OutputBindingContext) -> ContextPatch:
        value = dict(context.output)
        old = context.invocation_context.get('history', ())
        history = [*old, value] if case.get('context') == 'append' else [value]
        return ContextPatch(invocation=(ContextOperation.set('history', history),))

    def build(identity):
        workflow = Workflow(identity, nodes=[Node('entry', batch if family == 'map' else entry)])
        if family == 'loop':
            workflow.nodes += [Node('work', advance, input_mapping=incoming,
                output_binding=bind if case.get('context') else None,
                resource=ResourcePolicy(max_executions_per_invocation=case['iterations']+1)),
                Node('finish', finish, input_mapping=incoming)]
            workflow.edges += [Edge('entry', 'work'), Edge('work', 'work', condition=again), Edge('work', 'finish', condition=done)]
        elif family == 'map':
            workflow.nodes += [Node('work', advance, input_mapping=map_inputs,
                map=Map(aggregate=aggregate, max_parallelism=case.get('parallelism', 8))),
                Node('finish', summary_identity, input_mapping=summary_input)]
            workflow.edges += [Edge('entry', 'work'), Edge('work', 'finish')]
        else:
            previous = 'entry'
            for i in range(case.get('nodes', 1)):
                name = f'work-{i}'
                workflow.nodes.append(Node(name, advance, input_mapping=incoming))
                workflow.edges.append(Edge(previous, name))
                previous = name
            workflow.nodes.append(Node('finish', finish, input_mapping=incoming))
            workflow.edges.append(Edge(previous, 'finish'))
        return workflow

    workflows = [build('comparison' if i == 0 else f'definition-{i}') for i in range(case.get('definitions', 1))]
    env = {key: value for key, value in config.get('environment', {}).items()}
    settings = HostSettings.from_environment(dict(env, AUTOAGENT_DATABASE_URL=config['database_url']))
    settings = __import__('dataclasses').replace(settings, persistence=PersistenceConfig(mode=config.get('persistence_mode', 'durable')))
    host = AutoAgentHost(workflows=workflows, settings=settings, environment=env, root=Path(config['directory']))
    # Owned Host and native execution routes; no substitute executor.
    app = create_app(project='.', host=host, settings=ServerSettings(access_log=False), ui=False)
    original = app.router.lifespan_context
    @asynccontextmanager
    async def lifespan(app):
        async with host:
            async with original(app):
                yield
    app.router.lifespan_context = lifespan

    @app.get('/__bench/catalog')
    async def catalog():
        page = await host.list_workflows(workflow_id='comparison')
        return dict(revision=page.items[0]['revision_id'], package=str(autoagent.__file__), database_mode=settings.persistence.mode)

    @app.get('/__bench/wait')
    async def wait(iid: str):
        started = time.perf_counter()
        state = await host.wait_execution(iid, until='terminal', timeout=config.get('timeout_seconds', 120))
        terminal = time.perf_counter()
        await host._persistence.flush()
        health = await host._persistence.health()
        value = await host.get_execution_value(iid, 'invocation', iid, 'output') if state['status'] == 'completed' else {'value': None}
        return dict(status=state['status'], output=value['value'], error=state.get('error_type'),
                    observed_ms=(terminal-started)*1000, flush_ms=(time.perf_counter()-terminal)*1000,
                    queued=health.queued_count, retained_bytes=health.retained_bytes)

    @app.post('/__bench/flush')
    async def flush():
        await host._persistence.flush()
        return dict(health=asdict(await host._persistence.health()), resident=len(host._runtime.resident))

else:
    from autoagent import AutoAgentApp, AutoAgentSettings, DatabaseBackend, RuntimeStore, Workflow
    from autoagent.core.server import AutoAgentServer
    from autoagent.core.workflow.policy import MapPolicy, NodePolicy
    from autoagent.core.runtime.retention import RuntimeRetentionPolicy

    def incoming(context):
        return {'value': context.incoming[0].value}

    def again(context):
        output = context.output
        step = output.step if isinstance(output, Payload) else output['step']
        return step < case.get('iterations', 1)

    def done(context):
        return not again(context)

    def select_items(context):
        value = context.input['value']
        items = value.items if isinstance(value, Batch) else value['items']
        return [{'value': item} for item in items]

    def aggregate(context):
        return summarize(context.item_outputs)

    def bind(context):
        value = context.output.model_dump(mode='json') if isinstance(context.output, Payload) else context.output
        old = context.invocation.data.get('history', [])
        context.invocation.data['history'] = [*old, value] if case.get('context') == 'append' else [value]

    async def summary_identity(value: Summary) -> Summary:
        return value

    def build(identity):
        workflow = Workflow(id=identity)
        workflow.add_node(batch if family == 'map' else entry, node_id='entry')
        if family == 'loop':
            workflow.add_node(advance, node_id='work', input_mapping=incoming, output_binding=bind if case.get('context') else None)
            workflow.add_node(finish, node_id='finish', input_mapping=incoming)
            workflow.add_edge('entry','work')
            workflow.add_edge('work','work',condition=again)
            workflow.add_edge('work','finish',condition=done)
        elif family == 'map':
            workflow.add_node(advance, node_id='work', input_mapping=incoming,
                policy=NodePolicy(map=MapPolicy(item_selector=select_items, output_aggregator=aggregate,
                                               max_parallelism=case.get('parallelism', 8))))
            workflow.add_node(summary_identity,node_id='finish',input_mapping=incoming)
            workflow.add_edge('entry','work')
            workflow.add_edge('work','finish')
        else:
            previous='entry'
            for i in range(case.get('nodes',1)):
                name=f'work-{i}'
                workflow.add_node(advance,node_id=name,input_mapping=incoming)
                workflow.add_edge(previous,name)
                previous=name
            workflow.add_node(finish,node_id='finish',input_mapping=incoming)
            workflow.add_edge(previous,'finish')
        return workflow

    backend = DatabaseBackend(config['database_url'])
    store = RuntimeStore(backend=backend, retention_policy=RuntimeRetentionPolicy(mode=config.get('retention', 'retain_all')))
    agent = AutoAgentApp(runtime_store=store, settings=AutoAgentSettings(executor_max_thread_workers=32, executor_max_parallel_units=32))
    for i in range(case.get('definitions',1)):
        agent.register_workflow(build('comparison' if i==0 else f'definition-{i}'))
    server = AutoAgentServer(agent)
    app = server.api

    @app.get('/__bench/catalog')
    async def catalog():
        record=server._registered_workflow_records()[0] if hasattr(server,'_registered_workflow_records') else None
        entry = next(e for e in agent.workflow_registry.list() if e.workflow_ir.workflow_id == 'comparison')
        return dict(revision=entry.workflow_ir.workflow_revision_id,package=str(autoagent.__file__),database_mode='asynchronous coordinator; completion includes aflush')

    @app.get('/__bench/wait')
    async def wait(iid: str):
        identity=UUID(iid)
        deadline=time.monotonic()+config.get('timeout_seconds',120)
        while True:
            invocation=store.invocations.get(identity)
            if invocation is not None and invocation.state in {'completed','failed','cancelled'}:
                break
            if time.monotonic()>deadline:
                raise HTTPException(504,'Invocation timeout')
            await asyncio.sleep(.001)
        terminal=time.perf_counter()
        await store.aflush()
        output=invocation.result
        if hasattr(output,'model_dump'):
            output=output.model_dump(mode='json')
        return dict(status=invocation.state,output=output,error=str(invocation.error) if invocation.error else None,
                    flush_ms=(time.perf_counter()-terminal)*1000,
                    queued=store.persistence.pending_count,retained_bytes=store.persistence.pending_bytes)

    @app.post('/__bench/flush')
    async def flush():
        await store.aflush()
        return dict(queued=store.persistence.pending_count,retained_bytes=store.persistence.pending_bytes,
                    resident=len(store.invocations))

uvicorn.run(app, host='127.0.0.1',port=config['port'],log_level='warning',access_log=False)
