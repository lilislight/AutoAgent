"""Identical HTTP envelope for standalone V1/V2 Core without a database.

This is a benchmark adapter, not the database-backed V2 Host/Server API.
Only completed response summaries are held between the two HTTP requests.
"""
from contextlib import asynccontextmanager
import time

from fastapi import FastAPI, HTTPException


def create_memory_app(agent, workflows, *, version, config, store=None):
    results = {}
    revision = 'memory-comparison'

    @asynccontextmanager
    async def lifespan(app):
        if version == 'v1':
            await agent.astart()
        try:
            yield
        finally:
            await agent.aclose()

    app = FastAPI(lifespan=lifespan)

    @app.get('/__bench/catalog')
    async def catalog():
        import autoagent
        return dict(revision=revision, package=str(autoagent.__file__),
                    database_mode='none', adapter='standalone Core; common HTTP envelope',
                    artifact_threshold_bytes=None)

    @app.post('/__bench/invoke')
    async def invoke(body: dict):
        from business import Payload
        value = Payload.model_validate(body['input'])
        started = time.perf_counter()
        if version == 'v2':
            outcome = await agent.ainvoke('comparison', value, entry_node_id='entry')
            iid, sid = outcome.invocation_id, outcome.session_id
            status, output = outcome.status, outcome.output
        else:
            outcome = await agent.ainvoke(workflows[0], input={'value': value},
                                         entry_node_id='entry', event_mode=config['event_mode'])
            iid, sid = str(outcome.id), str(store.invocation_sessions[outcome.id])
            status, output = outcome.state, outcome.result
        elapsed = (time.perf_counter() - started) * 1000
        if hasattr(output, 'model_dump'):
            output = output.model_dump(mode='json')
        if isinstance(output, dict) and set(output) == {'output'}:
            output = output['output']
        if status != 'completed':
            raise HTTPException(500, f'Core Invocation ended with {status}')
        results[iid] = dict(status=status, output=output, error=None,
                            core_ms=elapsed, flush_ms=0, queued=0, retained_bytes=0)
        return dict(invocation_id=iid, session_id=sid)

    @app.get('/__bench/wait')
    async def wait(iid: str):
        return results.pop(iid)

    @app.post('/__bench/flush')
    async def flush():
        return dict(queued=0, retained_bytes=0, persistence_enabled=False)

    @app.get('/__bench/status')
    async def status():
        return dict(persistence_enabled=False, backend=None, sink=None)

    return app
