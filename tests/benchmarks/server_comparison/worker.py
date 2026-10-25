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
config = json.loads(Path(args.config).read_text())
ROOT = Path(config.get('repository_root') or Path(__file__).resolve().parents[3])
VERSION_ROOT = ROOT / 'v1' if args.version == 'v1' else Path(config.get('source_root') or ROOT)
sys.path.insert(0, str(VERSION_ROOT))
import autoagent
assert Path(autoagent.__file__).resolve().is_relative_to(VERSION_ROOT)
if args.version == 'v2':
    assert not Path(autoagent.__file__).resolve().is_relative_to(ROOT / 'v1')
from business import Payload, Batch, Summary, operations, summarize, finish_many
from fastapi import HTTPException
import uvicorn

case = config['case']
entry, advance, finish, batch = operations(case)
family = case.get('family', 'chain')
workflows = []
fault = {'armed': False, 'writes': 0}
if case.get('profile_cpu'):
    import cProfile
    main_profiler = cProfile.Profile(timer=time.process_time)


async def storage_probe():
    if case.get('persist_delay_ms'):
        await asyncio.sleep(case['persist_delay_ms'] / 1000)
    if fault['armed']:
        fault['writes'] += 1
        if fault['writes'] >= case['persist_fail_after']:
            raise OSError('Injected benchmark persistence failure')
diagnostics = None
if case.get('instrument_sql'):
    from diagnostics import Diagnostics
    diagnostics = Diagnostics()
    diagnostics.install_sql()
    if args.version == 'v2' and not config.get('no_persistence'):
        diagnostics.install_v2()

if args.version == 'v2':
    from autoagent import (Workflow, Node, Edge, Map, Wait, ResourcePolicy, InputMappingContext,
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

    def many_incoming(context: InputMappingContext) -> Batch:
        return Batch(items=[Payload.model_validate(dict(value), strict=False) for value in context.incoming.values()])

    def again(context: ConditionContext) -> bool:
        return context.output['step'] < case.get('iterations', 1)

    def done(context: ConditionContext) -> bool:
        return not again(context)

    def bind(context: OutputBindingContext) -> ContextPatch:
        value = dict(context.output)
        old = context.invocation_context.get('history', ())
        history = [*old, value] if case.get('context') == 'append' else [value]
        return ContextPatch(invocation=(ContextOperation.set('history', history),))

    def always(context: ConditionContext) -> bool:
        return True

    def build(identity):
        workflow = Workflow(identity, nodes=[Node('entry', batch if family == 'map' else entry)])
        if family == 'wait':
            workflow.nodes.extend([Node('approval', Wait(Payload, Payload), input_mapping=incoming),
                                   Node('finish', finish, input_mapping=incoming)])
            workflow.edges.extend([Edge('entry', 'approval'), Edge('approval', 'finish')])
        elif family == 'loop':
            workflow.nodes += [Node('work', advance, input_mapping=incoming,
                output_binding=bind if case.get('context') else None,
                resource=ResourcePolicy(max_executions_per_invocation=case['iterations']+1)),
                Node('finish', finish, input_mapping=incoming)]
            workflow.edges += [Edge('entry', 'work'), Edge('work', 'work', condition=again), Edge('work', 'finish', condition=done)]
        elif family == 'wide':
            workflow.nodes.append(Node('finish', finish_many, input_mapping=many_incoming))
            for i in range(case['width']):
                name = f'branch-{i}'
                workflow.nodes.append(Node(name, advance, input_mapping=incoming))
                workflow.edges.extend([Edge('entry', name, condition=always if case.get('conditional') else None), Edge(name, 'finish')])
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
    if config.get('no_persistence'):
        from autoagent import AutoAgentApp
        from memory_server import create_memory_app
        agent = AutoAgentApp(max_operator_concurrency=32)
        for workflow in workflows:
            agent.register_workflow(workflow)
        app = create_memory_app(agent, workflows, version='v2', config=config)
        @app.get('/__bench/sql')
        async def memory_sql():
            return diagnostics.snapshot() if diagnostics else {}
        uvicorn.run(app, host='127.0.0.1', port=config['port'], log_level='warning', access_log=False)
        raise SystemExit(0)

    env = {key: value for key, value in config.get('environment', {}).items()}
    settings = HostSettings.from_environment(dict(env, AUTOAGENT_DATABASE_URL=config['database_url']))
    settings = __import__('dataclasses').replace(settings, persistence=__import__('dataclasses').replace(settings.persistence, mode=config.get('persistence_mode', 'durable')))
    host = AutoAgentHost(workflows=workflows, settings=settings, environment=env, root=Path(config['directory']))
    if case.get('persist_delay_ms') or case.get('persist_fail_after'):
        from autoagent.host.persistence.storage import Storage
        original_write = Storage.write
        async def delayed_write(self, entries):
            await storage_probe()
            return await original_write(self, entries)
        Storage.write = delayed_write
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
        revisions = []
        if case.get('rotate_definitions'):
            for definition in workflows:
                registered = await host.list_workflows(workflow_id=definition.id)
                revisions.append(dict(workflow_id=definition.id, revision_id=registered.items[0]['revision_id']))
        return dict(revision=page.items[0]['revision_id'], revisions=revisions, package=str(autoagent.__file__), database_mode=settings.persistence.mode,
                    artifact_threshold_bytes=settings.persistence.artifact_threshold_bytes)

    async def drain_persistence():
        # flush() captures one frontier. A terminal observation can race with
        # later accepted Context/Checkpoint records, so include those too.
        deadline = time.monotonic() + config.get('timeout_seconds', 120)
        while True:
            await host._persistence.flush()
            health = await host._persistence.health()
            if health.queued_count == 0 and health.accepted_frontier == health.durable_frontier:
                return health
            if time.monotonic() >= deadline:
                raise HTTPException(504, 'Persistence drain timed out')
            await asyncio.sleep(.001)

    @app.get('/__bench/wait')
    async def wait(iid: str):
        started = time.perf_counter()
        if config.get('wait_mode') == 'join':
            try:
                result = await host.join(iid, timeout=config.get('timeout_seconds', 120))
            except TimeoutError:
                raise HTTPException(504, 'Invocation observation timed out') from None
            state = dict(status=result.status, waits=[dict(wait_id=item.id) for item in result.waits])
        else:
            state = await host.wait_execution(iid, until='boundary' if family == 'wait' else 'terminal', timeout=config.get('timeout_seconds', 120))
            if state.get('observation') == 'timeout':
                raise HTTPException(504, 'Invocation observation timed out')
        terminal = time.perf_counter()
        # wait_execution observes accepted state; trace values read durable SQL.
        # Settle that frontier before reading values, then drain later arrivals.
        if case.get('captured_frontier_barrier'):
            await host._persistence.flush()
        else:
            await drain_persistence()
        value = await host.get_execution_value(iid, 'invocation', iid, 'output') if state['status'] == 'completed' else {'value': None}
        health = await host._persistence.health() if case.get('captured_frontier_barrier') else await drain_persistence()
        if state['status'] == 'waiting':
            return dict(status='waiting', wait_id=state['waits'][0]['wait_id'])
        return dict(status=state['status'], output=value['value'], error=state.get('error_type'),
                    observed_ms=(terminal-started)*1000, flush_ms=(time.perf_counter()-terminal)*1000,
                    queued=health.queued_count, retained_bytes=health.retained_bytes)

    @app.post('/__bench/flush')
    async def flush():
        return dict(health=asdict(await drain_persistence()), resident=len(host._runtime.resident))

    @app.get('/__bench/residency')
    async def residency():
        writer = host._persistence._writer

        async def caches():
            result = {}
            for name in ('graph_cache', 'metadata_cache'):
                cache = getattr(writer.storage, name, None)
                if cache is not None:
                    result[name] = dict(bytes=cache.bytes, budget=cache.budget,
                        entries=len(cache.entries), hits=cache.hits,
                        misses=cache.misses, evictions=cache.evictions)
            return result

        return dict(health=asdict(await host._persistence.health()),
                    resident=len(host._runtime.resident),
                    residency_entries=len(host._runtime.residency.entries),
                    caches=await writer.run(caches))

else:
    from autoagent import AutoAgentApp, AutoAgentSettings, DatabaseBackend, RuntimeStore, Workflow, SystemCommand
    from autoagent.core.server import AutoAgentServer
    from autoagent.core.workflow.policy import MapPolicy, NodePolicy
    from autoagent.core.runtime.retention import RuntimeRetentionPolicy

    def incoming(context):
        return {'value': context.incoming[0].value}

    def many_incoming(context):
        return {'value': Batch(items=[item.value for item in context.incoming])}

    def wait_input(context):
        value = context.incoming[0].value
        return dict(payload=value.model_dump(mode='json') if isinstance(value, Payload) else value)

    def again(context):
        output = context.source_output
        step = output.step if isinstance(output, Payload) else output['step']
        return step < case.get('iterations', 1)

    def done(context):
        return not again(context)

    def select_items(context):
        value = context.incoming[0].value
        items = value.items if isinstance(value, Batch) else value['items']
        return [{'value': item} for item in items]

    def aggregate(context) -> Summary:
        return summarize(context.item_outputs)

    def bind(context):
        value = context.output.model_dump(mode='json') if isinstance(context.output, Payload) else context.output
        old = context.invocation_context.data.get('history', [])
        context.invocation_context.data['history'] = [*old, value] if case.get('context') == 'append' else [value]

    async def summary_identity(value: Summary) -> Summary:
        return value

    def build(identity):
        workflow = Workflow(id=identity)
        workflow.add_node(batch if family == 'map' else entry, node_id='entry')
        if family == 'wait':
            workflow.add_node(SystemCommand(id='wait'), node_id='approval', input_mapping=wait_input)
            workflow.add_node(finish, node_id='finish', input_mapping=incoming)
            workflow.add_edge('entry', 'approval')
            workflow.add_edge('approval', 'finish')
        elif family == 'loop':
            workflow.add_node(advance, node_id='work', input_mapping=incoming, output_binding=bind if case.get('context') else None)
            workflow.add_node(finish, node_id='finish', input_mapping=incoming)
            workflow.add_edge('entry','work')
            workflow.add_edge('work','work',condition=again)
            workflow.add_edge('work','finish',condition=done)
        elif family == 'wide':
            workflow.add_node(finish_many, node_id='finish', input_mapping=many_incoming)
            for i in range(case['width']):
                name = f'branch-{i}'
                workflow.add_node(advance, node_id=name, input_mapping=incoming)
                workflow.add_edge('entry', name)
                workflow.add_edge(name, 'finish')
        elif family == 'map':
            workflow.add_node(advance, node_id='work',
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

    backend = None if config.get('no_persistence') else DatabaseBackend(config['database_url'])
    if case.get('persist_delay_ms') or case.get('persist_fail_after'):
        original_write = backend._persist_batch
        async def delayed_write(entries):
            await storage_probe()
            return await original_write(entries)
        backend._persist_batch = delayed_write
    store = RuntimeStore(backend=backend, retention_policy=RuntimeRetentionPolicy(mode=config.get('retention', 'retain_all')))
    agent = AutoAgentApp(runtime_store=store, settings=AutoAgentSettings(executor_max_thread_workers=32, executor_max_parallel_units=32))
    for i in range(case.get('definitions',1)):
        definition = build('comparison' if i==0 else f'definition-{i}')
        workflows.append(definition)
        agent.register_workflow(definition)
    if config.get('no_persistence'):
        from memory_server import create_memory_app
        app = create_memory_app(agent, workflows, version='v1', config=config, store=store)
        @app.get('/__bench/sql')
        async def memory_sql():
            return diagnostics.snapshot() if diagnostics else {}
        uvicorn.run(app, host='127.0.0.1', port=config['port'], log_level='warning', access_log=False)
        raise SystemExit(0)
    server = AutoAgentServer(agent, ui_directory=Path(config['directory']) / 'no-ui')
    app = server.api

    @app.get('/__bench/catalog')
    async def catalog():
        revision = next(key for key, entry in agent.workflow_registry.items() if entry.workflow_ir.workflow_id == 'comparison')
        return dict(revision=revision,package=str(autoagent.__file__),database_mode='asynchronous coordinator; completion includes aflush',
                    artifact_threshold_bytes=backend.artifact_policy.inline_max_bytes)

    @app.get('/__bench/wait')
    async def wait(iid: str):
        identity=UUID(iid)
        deadline=time.monotonic()+config.get('timeout_seconds',120)
        while True:
            invocation=store.invocations.get(identity)
            if invocation is not None and (invocation.state in {'completed','failed','cancelled'} or family == 'wait' and invocation.state == 'waiting'):
                break
            if time.monotonic()>deadline:
                raise HTTPException(504,'Invocation timeout')
            await asyncio.sleep(.001)
        terminal=time.perf_counter()
        await store.aflush()
        if invocation.state == 'waiting':
            return dict(status='waiting', wait_id=next(iter(invocation.scheduler.waiting_executions)))
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

# V1 mounts its UI at '/', so fixture routes must precede that catch-all.
@app.get('/__bench/sql')
async def sql_counts():
    return diagnostics.snapshot() if diagnostics else {}


@app.post('/__bench/start-profile')
async def start_profile():
    if not case.get('profile_cpu'):
        raise HTTPException(400, 'CPU profiling is disabled')
    main_profiler.enable()
    return {'enabled': True}


@app.post('/__bench/arm-fault')
async def arm_fault():
    fault['armed'] = True
    return {'armed': True}


@app.get('/__bench/status')
async def status():
    return asdict(await host._persistence.health()) if args.version == 'v2' else server._runtime_status()

from starlette.routing import Mount
app.router.routes.sort(key=lambda route: isinstance(route, Mount))
if case.get('profile_cpu'):
    try:
        uvicorn.run(app, host='127.0.0.1', port=config['port'], log_level='warning', access_log=False)
    finally:
        main_profiler.disable()
        main_profiler.dump_stats(str(Path(config['directory']) / 'profile-main.pstats'))
else:
    uvicorn.run(app, host='127.0.0.1',port=config['port'],log_level='warning',access_log=False)
