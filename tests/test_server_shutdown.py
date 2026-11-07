"""Transport shutdown ends SSE before Uvicorn drains HTTP connections."""

import asyncio
from contextlib import AsyncExitStack
import os
from pathlib import Path
import signal
import socket
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
import httpx
from fastapi.routing import APIRoute

from autoagent.server import ServerSettings, create_app, create_router
from autoagent.server.watching import TraceWatchHub


class _Host:
    async def get_trace_head(self, iid):
        return dict(invocation_id=iid, revision=0, cursor='test', store_id='test',
                    invocation_version=0, family_version=0, active=False)

    async def wait_trace_commit(self, revision, timeout):
        await asyncio.sleep(timeout)
        return revision


class ServerShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_hub_close_wakes_all_consumers_and_cancels_producers(self):
        """Closing a Hub replaces queued updates and wakes empty subscriber queues."""
        host = _Host()
        hub = TraceWatchHub(host, ServerSettings())
        head = await host.get_trace_head('test')
        first = hub.subscribe('test', head)
        second = hub.subscribe('test', head)
        await first.get()
        waiting = asyncio.create_task(first.get())
        tasks = [group['task'] for group in hub.groups.values()]
        await hub.close()
        self.assertEqual(await asyncio.wait_for(waiting, 1), ('closed', {}))
        self.assertEqual(await second.get(), ('closed', {}))
        self.assertTrue(all(task.done() for task in tasks))
        self.assertEqual(hub.groups, {})
        self.assertEqual(hub.count, 0)
        await hub.close()
        await hub.unsubscribe('test', first)

    async def test_router_teardown_ends_an_idle_stream_without_closing_borrowed_host(self):
        """An embedded Router teardown wakes its SSE consumer and preserves Host ownership."""
        host = _Host()
        host.close = AsyncMock()
        app = FastAPI()
        router = create_router(host, settings=ServerSettings())
        app.include_router(router)
        endpoint = next(route.endpoint for route in router.routes if isinstance(route, APIRoute) and route.path.endswith('/trace-watch'))
        async with app.router.lifespan_context(app):
            response = await endpoint('test')
            stream = response.body_iterator
            await anext(stream)
            await anext(stream)
            idle = asyncio.create_task(anext(stream))
            await asyncio.sleep(0)
            self.assertFalse(idle.done())
            self.assertEqual(len(app.state._autoagent_trace_hubs), 1)
        with self.assertRaises(StopAsyncIteration):
            await asyncio.wait_for(idle, 1)
        host.close.assert_not_awaited()
        self.assertEqual(app.state._autoagent_trace_hubs, set())

    async def test_standalone_stops_streams_before_uvicorn_drain(self):
        """Standalone shutdown wakes every mounted Router before Uvicorn starts draining."""
        import uvicorn
        from autoagent.server.serving import _Server
        app = create_app(_Host(), settings=ServerSettings(), ui=False)
        app.include_router(create_router(_Host(), settings=ServerSettings(api_prefix='/other')))
        server = _Server(uvicorn.Config(app))
        async with app.router.lifespan_context(app):
            hubs = tuple(app.state._autoagent_trace_hubs)
            self.assertEqual(len(hubs), 2)
            user_hubs = tuple(app.state._autoagent_user_event_hubs)
            self.assertEqual(len(user_hubs), 2)
            async def drain(*, sockets):
                self.assertTrue(all(hub.closed for hub in hubs + user_hubs))
                self.assertEqual(sockets, ['test'])
            with patch.object(uvicorn.Server, 'shutdown', side_effect=drain) as shutdown:
                await server.shutdown(sockets=['test'])
                shutdown.assert_awaited_once()

    async def test_cli_preserves_startup_failure_exit_status(self):
        """Replacing Uvicorn's runner preserves a nonzero exit when startup never completes."""
        from autoagent.server.__main__ import main
        app = create_app(_Host(), settings=ServerSettings(), ui=False)
        with patch('sys.argv', ['autoagent.server', '--no-ui']), \
             patch('autoagent.server.app.create_app', return_value=app), \
             patch('autoagent.server.serving._Server') as server_type:
            server_type.return_value.started = False
            status = main()
        self.assertEqual(status, 3)

    async def test_cli_one_signal_closes_live_streams_and_persists_waiting_sessions(self):
        """One SIGINT or SIGTERM ends HTTP streams and completes owned Host checkpoint cleanup."""
        for signum in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=signum), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / 'workflows.py').write_text('''from typing import TypedDict
from autoagent import Node, Wait, Workflow
from autoagent.host import workflow
class Value(TypedDict):
    text: str
@workflow
def approval():
    return Workflow('shutdown-wait', nodes=[Node('approval', Wait(Value, Value))])
''')
                database = root / 'shutdown.db'
                (root / '.env').write_text(f'AUTOAGENT_DATABASE_URL=sqlite+aiosqlite:///{database}\n')
                with socket.socket() as reserved:
                    reserved.bind(('127.0.0.1', 0))
                    port = reserved.getsockname()[1]
                environment = {k: v for k, v in os.environ.items() if not k.startswith('AUTOAGENT_')}
                with (root / 'server.log').open('w+') as log:
                    proc = await asyncio.create_subprocess_exec(
                        sys.executable, '-m', 'autoagent.server', '--project', str(root),
                        '--port', str(port), '--no-ui', env=environment, stdout=log, stderr=log,
                        start_new_session=True,
                    )
                    try:
                        async with httpx.AsyncClient(base_url=f'http://127.0.0.1:{port}', timeout=5) as client:
                            for _ in range(200):
                                try:
                                    response = await client.get('/api/v2/workflows')
                                    if response.status_code == 200:
                                        break
                                except httpx.TransportError:
                                    pass
                                await asyncio.sleep(.05)
                            else:
                                self.fail('Server did not start.')
                            revision = next(item['revision_id'] for item in response.json()['items']
                                if item['workflow_id'] == 'shutdown-wait')
                            ids = []
                            for _ in range(2):
                                response = await client.post('/api/v2/executions', json=dict(
                                    workflow_id='shutdown-wait', revision_id=revision,
                                    entry_node_id='approval', input={'text': 'pending'},
                                ))
                                self.assertEqual(response.status_code, 202, response.text)
                                ids.append(response.json()['invocation_id'])
                            for iid in ids:
                                for _ in range(100):
                                    response = await client.get('/api/v2/trace-head', params={'invocation_id': iid})
                                    if response.status_code == 200 and response.json()['status'] == 'waiting':
                                        break
                                    await asyncio.sleep(.05)
                                else:
                                    self.fail('Invocation did not reach Wait.')
                            async with AsyncExitStack() as stack:
                                readers = []
                                async def drain(lines):
                                    async for _ in lines:
                                        pass
                                streams = [('/api/v2/trace-watch', iid) for iid in [ids[0], ids[0], ids[1]]]
                                streams += [('/api/v2/user-events/watch', iid) for iid in ids]
                                for path, iid in streams:
                                    response = await stack.enter_async_context(client.stream(
                                        'GET', path, params={'invocation_id': iid},
                                    ))
                                    self.assertEqual(response.status_code, 200)
                                    lines = response.aiter_lines()
                                    self.assertEqual(await anext(lines), 'retry: 1500')
                                    readers.append(asyncio.create_task(drain(lines)))
                                proc.send_signal(signum)
                                try:
                                    await asyncio.wait_for(asyncio.gather(proc.wait(), *readers), 8)
                                finally:
                                    for reader in readers:
                                        reader.cancel()
                                    await asyncio.gather(*readers, return_exceptions=True)
                        log.seek(0)
                        output = log.read()
                        self.assertIn('Server stopped.', output)
                        self.assertNotIn('timeout graceful shutdown exceeded', output)
                        self.assertNotIn('Traceback', output)
                        with sqlite3.connect(database) as connection:
                            checkpoints = connection.execute('SELECT COUNT(*) FROM checkpoints').fetchone()[0]
                            self.assertGreaterEqual(checkpoints, 2)
                    finally:
                        if proc.returncode is None:
                            proc.kill()
                        await proc.wait()
