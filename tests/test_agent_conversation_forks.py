"""Agent App Fork history references and independent continuation."""

from contextlib import asynccontextmanager
from base64 import b64decode, b64encode
import asyncio
import threading
from pathlib import Path
import tempfile
import unittest

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from autoagent.agent_app import builtin_agent_workflow
from autoagent.host import AutoAgentHost, HostRunConflict, HostSettings
from autoagent.host.persistence.models import ConflictError
from autoagent.server import create_app

from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase
from tests.test_builtin_agent import BuiltinApplicationContracts
from tests.test_agent_workflow import ScriptedProvider, answer, settings
from tests.test_agent_message_queue import PNG


class ForkContracts:
    project = BuiltinApplicationContracts.project
    database_url = BuiltinApplicationContracts.database_url

    async def send(self, host, client, sid, message):
        response = await client.post('/api/v2/agent/messages', json=dict(session_id=sid, message=message))
        self.assertEqual(response.status_code, 202, response.text)
        iid = response.json()['invocation_id']
        result = await host.join(iid)
        self.assertEqual(result.status, 'completed', result.error)
        return iid

    async def fork(self, client, sid, iid, destination):
        response = await client.post(f'/api/v2/agent/sessions/{sid}/fork', json=dict(
            new_session_id=destination, source_invocation_id=iid))
        self.assertEqual(response.status_code, 201, response.text)
        return response.json()

    async def history(self, client, sid):
        response = await client.get('/api/v2/agent/invocations', params=dict(session_id=sid))
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    async def test_multilevel_fork_keeps_exact_prefix_without_copying_executions(self):
        """Nested branches share only the selected prefix and continue with independent Context."""
        provider = ScriptedProvider(*(answer(f'Answer {n}') for n in range(8)))
        async with self.project(provider=provider) as (host, client):
            ids = [await self.send(host, client, 'original', f'Question {n}') for n in range(3)]
            fork = await self.fork(client, 'original', ids[1], 'branch')
            self.assertEqual(fork['title'], 'Question 0 · Fork')
            self.assertEqual(fork['fork_source_session_id'], 'original')
            self.assertIsNone(await host._persistence.session_information('branch'))
            page = await self.history(client, 'branch')
            self.assertEqual([row['invocation_id'] for row in page['items']], list(reversed(ids[:2])))
            self.assertTrue(all(row['inherited'] for row in page['items']))
            rows = (await client.get('/api/v2/agent/sessions')).json()['items']
            branch = next(row for row in rows if row['session_id'] == 'branch')
            self.assertIsNone(branch['invocation_id'])
            self.assertEqual(branch['invocation_count'], 0)
            self.assertEqual((await self.fork(client, 'original', ids[1], 'branch'))['title'], fork['title'])
            self.assertEqual((await self.fork(client, 'original', ids[0], 'sibling'))['title'], 'Question 0 · Fork 2')
            b1 = await self.send(host, client, 'branch', 'Branch question')
            context = str(provider.requests[-1].input)
            for text in ('Question 0', 'Question 1', 'Answer 0', 'Answer 1'):
                self.assertIn(text, context)
            self.assertNotIn('Question 2', context)
            self.assertNotIn('Answer 2', context)
            b2 = await self.send(host, client, 'branch', 'Later branch question')
            await self.fork(client, 'branch', b1, 'nested')
            await self.fork(client, 'branch', ids[0], 'early')
            self.assertEqual([row['invocation_id'] for row in (await self.history(client, 'nested'))['items']], [b1, ids[1], ids[0]])
            self.assertEqual([row['invocation_id'] for row in (await self.history(client, 'early'))['items']], [ids[0]])
            await self.send(host, client, 'nested', 'Nested question')
            self.assertIn('Branch question', str(provider.requests[-1].input))
            self.assertNotIn('Later branch question', str(provider.requests[-1].input))
            original = await self.history(client, 'original')
            self.assertEqual([row['invocation_id'] for row in original['items']], list(reversed(ids)))
            self.assertNotIn(b2, str(original))
            self.assertEqual(len((await client.get('/api/v2/agent/turn-index', params={'session_id': 'nested'})).json()['items']), 4)

    async def test_fork_rejects_unrelated_or_later_turns_and_keeps_usage_separate(self):
        """Forks cannot access unrelated or post-boundary turns or duplicate billed usage."""
        async with self.project() as (host, client):
            first = await self.send(host, client, 'source', 'First')
            later = await self.send(host, client, 'source', 'Later')
            await self.fork(client, 'source', first, 'fork')
            for sid, iid in [('fork', later), ('unrelated', first)]:
                result = await client.post(f'/api/v2/agent/sessions/{sid}/fork', json=dict(new_session_id='denied', source_invocation_id=iid))
                self.assertEqual(result.status_code, 404, result.text)
            usage = await client.get('/api/v2/agent/usage', params=dict(session_id='fork', invocation_id=first))
            self.assertEqual(usage.status_code, 200, usage.text)
            self.assertEqual(usage.json()['session']['total_tokens'], 0)
            original_usage = (await client.get('/api/v2/agent/usage', params=dict(session_id='source', invocation_id=first))).json()
            self.assertEqual(usage.json()['invocation'], original_usage['invocation'])
            await self.send(host, client, 'fork', 'Continue')
            usage = (await client.get('/api/v2/agent/usage', params=dict(session_id='fork'))).json()
            own = await host.token_usage(session_id='fork', include_descendants=True)
            self.assertEqual(usage['session']['total_tokens'], own.total_tokens)
            result = await client.post('/api/v2/agent/sessions/source/fork', json=dict(new_session_id='fork', source_invocation_id=later))
            self.assertEqual(result.status_code, 409, result.text)

    async def test_large_history_forks_and_continues_without_inspection_limit(self):
        """A Context above Inspect's 1 MiB budget still forks and reaches the next call intact."""
        large = 'Complete shared answer: ' + '界' * 370000
        provider = ScriptedProvider(answer(large), answer('Later source answer'), answer('Branch answer'))
        async with self.project(provider=provider) as (host, client):
            first = await self.send(host, client, 'large-source', 'Read the full shared answer')
            await self.send(host, client, 'large-source', 'Later source question')
            target = await host.applications['agent'].forks.target('large-source', first)
            with self.assertRaises(HostRunConflict) as error:
                await host.read_execution_context(first, sequence=target['sequence'],
                    scope='session', pointer='/agent_conversation')
            self.assertEqual(error.exception.code, 'INSPECTION_LIMIT')
            await self.fork(client, 'large-source', first, 'large-branch')
            await self.send(host, client, 'large-branch', 'Continue this large history')
            actual = str(provider.requests[-1].input)
            self.assertIn(large, actual)
            self.assertNotIn('Later source question', actual)
            self.assertEqual(len((await self.history(client, 'large-branch'))['items']), 2)

    async def test_restart_preserves_fork_and_resolves_only_inherited_attachments(self):
        """Stored ancestry survives restart and restores image pixels without copying assets."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database_url = await self.database_url(root)
            @asynccontextmanager
            async def opened(provider):
                @asynccontextmanager
                async def lifespan(setup):
                    setup.providers.register('offline', provider)
                    setup.applications['agent'].context.set_defaults(settings())
                    yield
                async with AutoAgentHost(workflows=(builtin_agent_workflow(execution=False),), root=root,
                        environment={}, lifespan=lifespan, settings=HostSettings(database_url=database_url)) as host:
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host, ui=False)), base_url='http://test') as client:
                        yield host, client
            async with opened(ScriptedProvider(answer(), answer())) as (host, client):
                result = await client.post('/api/v2/agent/messages', json=dict(session_id='images', message='Image question',
                    images=[dict(mime_type='image/png', data=PNG)]))
                self.assertEqual(result.status_code, 202, result.text)
                first = result.json()['invocation_id']
                completed = await host.join(first)
                self.assertEqual(completed.status, 'completed', completed.error)
                later = await client.post('/api/v2/agent/messages', json=dict(session_id='images', message='Later message',
                    images=[dict(mime_type='image/png', data=b64encode(b64decode(PNG) + b'x').decode('ascii'))]))
                self.assertEqual(later.status_code, 202, later.text)
                self.assertEqual((await host.join(later.json()['invocation_id'])).status, 'completed')
                page = await self.history(client, 'images')
                assets = {event['invocation_id']: part['source']['uri'] for event in page['events'] if event['kind'] == 'agent.messages'
                    for message in event['payload']['messages'] for part in message['content'] if part['type'] == 'image'}
                asset = assets[first]
            # Reopen an existing pre-Fork schema while retaining its chat history.
            engine = create_async_engine(database_url)
            async with engine.begin() as conn:
                await conn.execute(text('DROP INDEX agent_app_sessions_fork_source'))
                for name in ('fork_source_session_id', 'fork_source_invocation_id', 'fork_source_sequence'):
                    await conn.execute(text(f'ALTER TABLE agent_app_sessions DROP COLUMN {name}'))
            await engine.dispose()
            async with opened(ScriptedProvider()) as (host, client):
                await self.fork(client, 'images', first, 'image-fork')
            provider = ScriptedProvider(answer())
            async with opened(provider) as (host, client):
                self.assertEqual(len((await self.history(client, 'image-fork'))['items']), 1)
                app = host.applications['agent']
                inherited = await app.forks.image('image-fork', asset.rsplit('/', 1)[-1])
                self.assertIsNotNone(inherited)
                self.assertIsNone(await app.forks.image('unrelated', inherited.id))
                self.assertIsNone(await app.forks.image('image-fork', assets[later.json()['invocation_id']].rsplit('/', 1)[-1]))
                response = await client.post('/api/v2/agent/queue', json=dict(session_id='image-fork',
                    item_id='first-fork-message', message='Continue with the image'))
                self.assertEqual(response.status_code, 202, response.text)
                self.assertEqual(response.json()['state'], 'started', response.text)
                outcome = await host.join(response.json()['invocation_id'])
                self.assertEqual(outcome.status, 'completed', outcome.error)
                images = [part for item in provider.requests[-1].input if getattr(item, 'role', None) == 'user'
                    for part in item.content if part.type == 'image']
                self.assertEqual(len(images), 1)
                self.assertEqual(images[0].source.data, PNG)
                self.assertNotIn('Later message', str(provider.requests[-1].input))

    async def test_referenced_source_cannot_be_purged(self):
        """History maintenance refuses to delete a source while a Fork references it."""
        async with self.project() as (host, client):
            first = await self.send(host, client, 'protected', 'Keep this history')
            await self.fork(client, 'protected', first, 'dependent')
            with self.assertRaisesRegex(ConflictError, 'referenced by a Fork'):
                await host._persistence.purge_root_graph(first)
            self.assertEqual(len((await self.history(client, 'dependent'))['items']), 1)

    async def test_fork_copies_settings_and_rejects_running_reply(self):
        """Fork snapshots preferences separately from history and never branches an unfinished turn."""
        release = threading.Event()
        async def blocked(request):
            while not release.is_set():
                await asyncio.sleep(.01)
            return answer()
        self.addCleanup(release.set)
        async with self.project(provider=ScriptedProvider(answer(), blocked, answer())) as (host, client):
            first = await self.send(host, client, 'preferences', 'First')
            record = (await client.get('/api/v2/agent/settings', params=dict(session_id='preferences'))).json()
            selection = {**record['settings'], 'model': {**record['settings']['model'], 'model': 'branch-model'}, 'approval_mode': 'auto_approve'}
            changed = await client.put('/api/v2/agent/settings', json=dict(session_id='preferences',
                expected_version=record['version'], settings=selection))
            self.assertEqual(changed.status_code, 200, changed.text)
            second = await client.post('/api/v2/agent/messages', json=dict(session_id='preferences', message='Still running'))
            unfinished = await client.post('/api/v2/agent/sessions/preferences/fork', json=dict(
                new_session_id='unfinished', source_invocation_id=second.json()['invocation_id']))
            self.assertEqual(unfinished.status_code, 409, unfinished.text)
            fork = await self.fork(client, 'preferences', first, 'settings-fork')
            self.assertEqual(fork['settings'], selection)
            self.assertEqual(fork['workspace'], record['workspace'])
            reset = await client.put('/api/v2/agent/settings', json=dict(session_id='preferences',
                expected_version=changed.json()['version'], settings=record['settings']))
            self.assertEqual(reset.status_code, 200, reset.text)
            release.set()
            await host.join(second.json()['invocation_id'])
            await self.send(host, client, 'settings-fork', 'Continue independently')
            saved = (await client.get('/api/v2/agent/settings', params=dict(session_id='settings-fork'))).json()
            self.assertEqual(saved['settings'], selection)


class SQLiteForkTests(ForkContracts, unittest.IsolatedAsyncioTestCase):
    pass


@unittest.skipUnless(POSTGRES_TEST_URL, 'PostgreSQL URL not configured')
class PostgreSQLForkTests(ForkContracts, unittest.IsolatedAsyncioTestCase):
    async def database_url(self, root):
        database = PostgreSQLDatabase()
        self.addAsyncCleanup(database.close)
        return await database.create()
