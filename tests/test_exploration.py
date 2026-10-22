"""Read-only Revision and Session APIs against both real database backends."""

from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx
from sqlalchemy import select, update
from typing_extensions import TypedDict

from autoagent import Edge, Node, Operator, Workflow, WorkflowDefinitionSnapshot
from autoagent.core.compiler.snapshot import WORKFLOW_DEFINITION_SNAPSHOT_SCHEMA_VERSION, definition_digest
from autoagent.host import AutoAgentHost, HostNotFoundError, HostSettings
from autoagent.host.persistence.reading import SourceReader
from autoagent.host.persistence.schema import meta
from autoagent.host.persistence.storage import Storage
from autoagent.server import create_app
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class Value(TypedDict):
    text: str


def identity(value: Value) -> Value:
    return value


def workflow(version='1', name='review'):
    return Workflow(name, version=version, nodes=[Node('entry',identity),Node('old' if version=='1' else 'new',identity)],
        edges=[Edge('entry','old' if version=='1' else 'new', id='route')])


class ExplorationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.settings = HostSettings(database_url='sqlite+aiosqlite:///' + str(Path(self.temp.name)/'browse.db'))

    async def asyncTearDown(self):
        self.temp.cleanup()

    def host(self, workflows):
        return AutoAgentHost(workflows=workflows,settings=self.settings,environment={})

    async def saved_pair(self):
        async with self.host([workflow()]) as host:
            base = (await host.get_workflow('review'))['workflow_revision_id']
        return base

    async def test_revision_changes_are_paged_exact_and_directional(self):
        """Align by object IDs, count changed objects once, and reverse add/remove on swap."""
        base = await self.saved_pair()
        async with self.host([workflow('2')]) as host:
            target = (await host.get_workflow('review'))['workflow_revision_id']
            result = await host.compare_workflow_revisions(base,target,limit=2)
            self.assertEqual(result['total_count'],4)
            self.assertEqual(result['counts'],dict(workflow=1,node=2,edge=1,added=1,removed=1,modified=2))
            self.assertEqual(result['items'][0]['fields'],['exit_node_ids','workflow_version'])
            tail = await host.compare_workflow_revisions(base,target,limit=2,cursor=result['next_cursor'])
            self.assertIsNone(tail['next_cursor'])
            self.assertEqual(len(tail['items']),2)
            detail = await host.get_revision_comparison_object(base,target,'node','old')
            self.assertTrue(detail['base']['present'])
            self.assertFalse(detail['target']['present'])
            reverse = await host.get_revision_comparison_object(target,base,'node','old')
            self.assertFalse(reverse['base']['present'])
            self.assertTrue(reverse['target']['present'])
            identical = await host.compare_workflow_revisions(target,target)
            self.assertEqual(identical['total_count'],0)
            with self.assertRaises(HostNotFoundError):
                await host.get_revision_comparison_object(base,target,'node','missing')

    async def test_comparison_summaries_do_not_expand_large_string_artifacts(self):
        """Opaque large definitions compare by logical content and hydrate only on drill-down."""
        async with self.host([workflow()]) as host:
            original = await host.get_workflow('review')
            snapshots = []
            for version,content in (('large1','x'*1_100_000),('large2','y'*1_100_000)):
                definition = dict(original['definition'])
                definition['workflow_version'] = version
                definition['nodes'] = [dict(node) for node in definition['nodes']]
                definition['nodes'][0]['large_annotation'] = content
                digest = definition_digest(definition)
                snapshot = WorkflowDefinitionSnapshot(WORKFLOW_DEFINITION_SNAPSHOT_SCHEMA_VERSION,'review',version,'review:'+digest,digest,definition)
                await host._persistence.register_workflow(snapshot)
                snapshots.append(snapshot.workflow_revision_id)
            async def large_hashes():
                async with host._persistence._queries.view() as (conn,_,_,_):
                    table = meta.tables['artifacts']
                    return set((await conn.execute(select(table.c.hash).where(table.c.value_type=='string',table.c.logical_bytes>1_000_000))).scalars())
            hashes = await host._persistence._writer.run(large_hashes)
            self.assertEqual(len(hashes),2)
            original_row = Storage.row
            async def guarded(storage,conn,name,**keys):
                if name=='artifacts' and keys.get('hash') in hashes:
                    raise AssertionError('Large string body expanded for comparison summary')
                return await original_row(storage,conn,name,**keys)
            with patch.object(Storage,'row',guarded):
                result = await host.compare_workflow_revisions(*snapshots)
            definition['workflow_version'] = 'large3'
            definition['nodes'][0]['large_annotation'] = 'x'*1_100_000
            definition['nodes'][0]['note'] = 'small change'
            digest = definition_digest(definition)
            third = WorkflowDefinitionSnapshot(WORKFLOW_DEFINITION_SNAPSHOT_SCHEMA_VERSION,'review','large3','review:'+digest,digest,definition)
            await host._persistence.register_workflow(third)
            with patch.object(Storage,'row',guarded):
                small = await host.get_revision_comparison_object(snapshots[0],third.workflow_revision_id,'node','entry')
            self.assertEqual(small['base']['value'],{})
            self.assertEqual(small['target']['value'],{'note':'small change'})
            self.assertTrue(small['fields_only'])
            change = next(row for row in result['items'] if row['kind']=='node')
            self.assertEqual(change['id'],'entry')
            self.assertEqual(change['fields'],['large_annotation'])
            detail = await host.get_revision_comparison_object(*snapshots,'node','entry')
            self.assertEqual(detail['target']['value']['large_annotation'],'y'*1_100_000)
            workflow_detail = await host.get_revision_comparison_object(*snapshots,'workflow','review')
            self.assertNotIn('nodes',workflow_detail['base']['value'])

    async def test_session_history_is_complete_stable_and_metadata_only(self):
        """Cross-Workflow and Revision history orders by admission and ignores directory filters."""
        expected = []
        async with self.host([workflow(), workflow(name='format')]) as host:
            for name in ('review','format','review'):
                ref=await host.execute(name,{'text':name},session_id='shared',entry_node_id='entry')
                await host.join(ref)
                expected.append(ref.invocation_id)
        async with self.host([workflow('2'),workflow(name='format')]) as host:
            ref=await host.execute('review',{'text':'new'},session_id='shared',entry_node_id='entry')
            await host.join(ref)
            expected.append(ref.invocation_id)
            await host._persistence.flush()
            async def tie_timestamps(conn):
                await conn.execute(update(meta.tables['invocations']).values(started_at_us=100))
            await host._persistence._writer.run(host._persistence._maintenance.run,tie_timestamps)
            with patch.object(SourceReader,'read',side_effect=AssertionError('No business values')), patch.object(host._runtime,'_restore_graph',side_effect=AssertionError('No Runtime restoration')):
                info=await host.get_session_information('shared')
                self.assertEqual((info['invocation_count'],info['workflow_count'],info['revision_count']),(4,2,3))
                self.assertIsNotNone(info['created_at_us'])
                first=await host.list_session_history('shared',limit=2)
                second=await host.list_session_history('shared',limit=2,cursor=first.next_cursor)
                rows=first.items+second.items
                self.assertEqual([row['invocation_id'] for row in rows],expected)
                self.assertEqual([row['ordinal'] for row in rows],[1,2,3,4])
                self.assertEqual(first.total_count,4)
                memberships=await host.list_session_workflows('shared',limit=2)
                self.assertEqual(memberships.total_count,3)
                remaining=await host.list_session_workflows('shared',limit=2,cursor=memberships.next_cursor)
                all_members=memberships.items+remaining.items
                self.assertEqual(sum(row['invocation_count'] for row in all_members),4)
                self.assertEqual(sum(not row['runnable'] for row in all_members),1)
                selected=await host.list_session_history('shared',revision_ids=(rows[-1]['workflow_revision_id'],))
                self.assertEqual([row['ordinal'] for row in selected.items],[4])
                self.assertEqual(host._runtime.resident.keys(),{'shared'})

    async def test_server_scope_validation_and_missing_data(self):
        """API cursors bind to the exact Revision pair or Session and reject invalid scopes."""
        base=await self.saved_pair()
        async with self.host([workflow('2'),workflow(name='other')]) as host:
            target=(await host.get_workflow('review'))['workflow_revision_id']
            other=(await host.get_workflow('other'))['workflow_revision_id']
            ref=await host.execute('review',{'text':'a'},session_id='shared',entry_node_id='entry')
            await host.join(ref)
            await host._persistence.flush()
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host,ui=False)),base_url='http://test/api/v2/') as client:
                pair=dict(base_revision_id=base,target_revision_id=target,limit=1)
                response=await client.get('workflow-comparison',params=pair)
                self.assertEqual(response.status_code,200,response.text)
                cursor=response.json()['next_cursor']
                response=await client.get('workflow-comparison',params=dict(pair,target_revision_id=base,cursor=cursor))
                self.assertEqual(response.status_code,400)
                self.assertEqual((await client.get('workflow-comparison',params=dict(pair,target_revision_id=other))).status_code,400)
                self.assertEqual((await client.get('workflow-comparison',params=dict(pair,target_revision_id='missing'))).status_code,404)
                response=await client.get('workflow-comparison-object',params=dict(base_revision_id=base,target_revision_id=target,kind='edge',identity='route'))
                self.assertEqual(response.status_code,200,response.text)
                for name in ('session-information','session-history','session-workflows'):
                    self.assertEqual((await client.get(name,params={'session_id':'missing'})).status_code,404)
                    response=await client.get(name,params={'session_id':'shared'})
                    self.assertEqual(response.status_code,200,response.text)
                    self.assertNotIn('context',response.json())
                self.assertEqual((await client.get('session-history',params={'session_id':'shared','cursor':cursor})).status_code,400)


@unittest.skipUnless(POSTGRES_TEST_URL,'PostgreSQL is not configured')
class PostgreSQLExplorationTests(ExplorationTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.database=PostgreSQLDatabase()
        self.settings=replace(self.settings,database_url=await self.database.create())

    async def asyncTearDown(self):
        try:
            await self.database.close()
        finally:
            await super().asyncTearDown()
