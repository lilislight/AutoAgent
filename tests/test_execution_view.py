"""Workflow business observations on SQLite/PostgreSQL, including temporal and large-value boundaries."""

from pathlib import Path
import tempfile
import unittest

from typing_extensions import TypedDict
from sqlalchemy import select, update

from autoagent import (AwaitSignal, ConditionContext, ContextOperation, ContextPatch, Edge, InputMappingContext,
    Map, Node, OutputBindingContext, OwnerHandle, SendSignal, SignalEndpoint, Spawn, UserEventContext, UserEventSpec, Wait, Workflow)
from autoagent.host import AutoAgentHost, HostSettings, ViewUnavailable
from autoagent.host.persistence import PersistenceConfig
from autoagent.host.persistence.schema import meta
from tests.postgres import POSTGRES_TEST_URL, PostgreSQLDatabase


class Value(TypedDict):
    text: str


def echo(value: Value) -> Value: return value


def remember(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(session=(ContextOperation.set('last', context.output),))


class Batch(TypedDict):
    values: list[str]


def map_values(context: InputMappingContext) -> list[Value]:
    return [{'text': text} for text in context.invocation_input['values']]


class Count(TypedDict):
    value: int


def increment(value: Count) -> Count: return {'value': value['value'] + 1}
def count_identity(value: Count) -> Count: return value
def keep_count(context: InputMappingContext) -> Count:
    return next(iter(context.incoming.values())) if context.incoming else context.invocation_input
def again(context: ConditionContext) -> bool: return context.output['value'] < 200
def done(context: ConditionContext) -> bool: return context.output['value'] >= 200
def message(context: UserEventContext) -> UserEventSpec:
    return UserEventSpec(kind='business.result', payload=context.value)
def previous(context: InputMappingContext) -> Value: return dict(context.session_context['last'])


class ExecutionViewTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.database = None
        url = 'sqlite+aiosqlite:///' + str(self.root / 'runtime.db')
        if isinstance(self, PostgreSQLExecutionViewTests):
            self.database = PostgreSQLDatabase()
            url = await self.database.create()
        self.settings = HostSettings(database_url=url, memory_budget_bytes=2*1024*1024*1024,
            persistence=PersistenceConfig(mode='buffered', batch_delay_ms=10000))

    async def asyncTearDown(self):
        if self.database: await self.database.close()
        self.temporary.cleanup()

    def host(self, workflows):
        return AutoAgentHost(workflows=workflows, settings=self.settings, environment={})

    async def execute(self, host, workflow_id='business', value=None, entry_node_id='write', **options):
        ref = await host.execute_json(workflow_id, value or {'text':'before'}, revision_id=None, entry_node_id=entry_node_id, **options)
        await host.join(ref)
        return ref, await host.get_execution_view(ref.invocation_id)

    async def test_node_values_temporal_context_and_unchanged_old_observation(self):
        """Intermediate inputs/output and historical Context remain fixed after a later approval."""
        workflow = Workflow('business', nodes=[Node('write', echo, output_binding=remember),
            Node('approval', Wait(Value, Value)), Node('finish', echo, output_binding=remember)],
            edges=[Edge('write','approval',id='ask'), Edge('approval','finish',id='finish')])
        async with self.host([workflow]) as host:
            ref, waiting = await self.execute(host)
            write = await waiting.node('write').one()
            approval = await waiting.node('approval').one()
            self.assertEqual(await write.input(), {'text':'before'})
            self.assertEqual(await write.output(pointer='/text'), 'before')
            self.assertEqual(await approval.input(), {'text':'before'})
            self.assertEqual(approval.status, 'waiting')
            self.assertTrue(await waiting.route('ask').was_taken())
            self.assertTrue(await waiting.precedes(write.completed, approval.waiting))
            historic = await waiting.at(write.completed)
            self.assertEqual(await historic.context(pointer='/last/text'), 'before')
            interaction = (await waiting.interactions())[0]
            await host.perform_execution_action(ref.invocation_id, 'respond', wait_id=interaction.id,
                target_invocation_id=ref.invocation_id, payload={'text':'after'})
            await host.join(ref)
            final = await host.get_execution_view(ref.invocation_id)
            finish = await final.node('finish').one()
            with self.assertRaises(ValueError):
                await waiting.precedes(write.completed, finish.completed)
            earlier = await final.at(write.completed)
            self.assertFalse(earlier.to_record()['members'][0]['waits'])
            self.assertEqual(await final.context(pointer='/last/text'), 'after')
            self.assertEqual(await historic.context(pointer='/last/text'), 'before')
            self.assertEqual(await write.output(pointer='/text'), 'before')
            self.assertEqual(waiting.status, 'waiting')
            self.assertEqual(await interaction.request(), {'text':'before'})
            self.assertIn('waiting', [item.value for item in await final.transitions() if item.field == 'status'])
            with self.assertRaises(ViewUnavailable): await write.output(pointer='/missing')
            with self.assertRaises(ValueError): await final.node('misspelled').was_executed()
            with self.assertRaises(ValueError): await final.route('misspelled').was_taken()
            with self.assertRaises(ViewUnavailable): await historic.messages()

    async def test_map_unit_results_and_empty_map(self):
        """Map business items expose every effective result without standalone Operator evaluation."""
        workflow = Workflow('business', nodes=[Node('write', echo, map=Map(max_parallelism=2), input_mapping=map_values)])
        async with self.host([workflow]) as host:
            _, view = await self.execute(host, value={'values':['a','b','c']})
            mapped = await view.node('write').one()
            items = await mapped.items()
            self.assertEqual([item.index for item in items], [0,1,2])
            self.assertEqual([await item.output(pointer='/text') for item in items], ['a','b','c'])
            self.assertEqual(await mapped.input(pointer='/1/text'), 'b')
            self.assertTrue(all(len(item.attempts) == 1 for item in items))
            _, empty = await self.execute(host, value={'values':[]})
            self.assertEqual(await (await empty.node('write').one()).input(), [])
            self.assertEqual(await (await empty.node('write').one()).items(), ())

    async def test_route_condition_is_not_a_decision_until_node_commits(self):
        """A positive condition followed by a routing failure never counts as taking that business branch."""
        def yes(context: ConditionContext) -> bool: return True
        def broken(context: ConditionContext) -> bool: raise ValueError('cannot choose route')
        workflow = Workflow('business', nodes=[Node('write',echo),Node('yes',echo),Node('broken',echo)],
            edges=[Edge('write','yes',yes,id='yes'),Edge('write','broken',broken,id='broken')])
        async with self.host([workflow]) as host:
            _, view = await self.execute(host)
            self.assertEqual(view.status, 'failed')
            self.assertFalse(await view.route('yes').was_taken())
            self.assertEqual(await view.route('yes').state(), 'not_taken')
            self.assertFalse(await view.node('yes').was_executed())

    async def test_lazy_large_values_and_explicit_result_budget(self):
        """Metadata and small fields remain readable while oversized full business values fail explicitly."""
        workflow = Workflow('business', nodes=[Node('write',echo)])
        payload = 'a' * (2*1024*1024)
        async with self.host([workflow]) as host:
            _, view = await self.execute(host, value={'text':payload}, session_context={'small': 'yes', 'large': payload})
            node = await view.node('write').one()
            self.assertEqual(node.status,'completed')
            with self.assertRaises(ViewUnavailable) as error: await node.output()
            self.assertEqual(error.exception.code,'INSPECTION_LIMIT')
            self.assertEqual(await view.context(pointer='/small'), 'yes')
            with self.assertRaises(ViewUnavailable) as error: await view.context()
            self.assertEqual(error.exception.code,'INSPECTION_LIMIT')

    async def test_parent_child_scope_is_explicit(self):
        """Same-named parent and child Nodes never mix; unknown cross-Session order is not guessed."""
        child = Workflow('child',nodes=[Node('write',echo)])
        parent = Workflow('business',nodes=[Node('write',Spawn(child,'write'))])
        async with self.host([parent]) as host:
            _, view = await self.execute(host)
            self.assertEqual(len(await view.node('write').all()),1)
            children = view.children(workflow_id='child')
            self.assertEqual(len(children),1)
            child_node = await children[0].node('write').one()
            self.assertEqual(await child_node.output(),{'text':'before'})
            parent_node = await view.node('write').one()
            self.assertTrue(await view.precedes(parent_node.position('running'), child_node.completed))
            self.assertFalse(await view.precedes(child_node.completed, parent_node.position('running')))
            with self.assertRaises(ViewUnavailable):
                await view.precedes((await view.node('write').one()).completed, child_node.completed)

    async def test_parent_child_signal_positions_use_recorded_causality(self):
        """Cross-Session business ordering follows Signal delivery instead of comparing unrelated counters."""
        child = Workflow('child', nodes=[Node('send', SendSignal(handle=OwnerHandle(), endpoint='message'))])
        parent = Workflow('business', nodes=[Node('write', Spawn(child, 'send')), Node('receive', AwaitSignal('message'))],
            edges=[Edge('write', 'receive')], signal_endpoints=[SignalEndpoint('message', Value)])
        async with self.host([parent]) as host:
            _, view = await self.execute(host)
            self.assertEqual(view.status, 'completed', await view.error())
            send = await view.children()[0].node('send').one()
            receive = await view.node('receive').one()
            self.assertTrue(await view.precedes(send.position('running'), receive.completed))
            self.assertFalse(await view.precedes(receive.completed, send.position('running')))
            result = await receive.output()
            self.assertEqual(result['messages'][0]['payload'], {'text': 'before'})

    async def test_map_children_are_separate_business_scopes(self):
        """A mapped Spawn exposes all independent Child results without assuming their completion order."""
        child = Workflow('child', nodes=[Node('write', echo)])
        parent = Workflow('business', nodes=[Node('write', Spawn(child, 'write'),
            map=Map(max_parallelism=2), input_mapping=map_values)])
        async with self.host([parent]) as host:
            _, view = await self.execute(host, value={'values': ['a', 'b', 'c']})
            self.assertEqual(view.status, 'completed', await view.error())
            children = view.children(workflow_id='child')
            self.assertEqual(len(children), 3)
            values = [await (await child.node('write').one()).output(pointer='/text') for child in children]
            self.assertEqual(sorted(values), ['a', 'b', 'c'])
            self.assertEqual(len(await view.node('write').all()), 1)

    async def test_repeated_nodes_and_routes_cover_history_beyond_one_page(self):
        """Repeated business stages require explicit occurrence selection and include more than 1,000 Events."""
        workflow = Workflow('business', nodes=[Node('start', count_identity),
            Node('write', increment, input_mapping=keep_count), Node('finish', increment)],
            edges=[Edge('start', 'write', id='start'), Edge('write', 'write', again, id='repeat'), Edge('write', 'finish', done, id='finish')])
        async with self.host([workflow]) as host:
            _, view = await self.execute(host, value={'value': 0}, entry_node_id='start')
            self.assertEqual(view.status, 'completed')
            self.assertGreater(view.to_record()['members'][0]['position']['sequence'], 1000)
            runs = await view.node('write').all()
            self.assertEqual(len(runs), 200)
            with self.assertRaises(ValueError): await view.node('write').one()
            self.assertEqual(await runs[0].output(), {'value': 1})
            self.assertEqual(await runs[-1].output(), {'value': 200})
            decisions = await view.route('repeat').decisions()
            self.assertEqual(sum(item.status == 'taken' for item in decisions), 199)
            with self.assertRaises(ValueError): await view.route('repeat').state()

    async def test_context_and_messages_keep_their_invocation_observation(self):
        """Cross-Workflow Session Context inherits values while prior views and business messages stay fixed."""
        write = Workflow('business', nodes=[Node('write', echo, output_binding=remember, user_events=message)])
        read = Workflow('read', nodes=[Node('read', echo, input_mapping=previous)])
        async with self.host([write, read]) as host:
            ref, first = await self.execute(host)
            self.assertEqual(first.status, 'completed', await first.error())
            second_ref = await host.execute_json('read', {'text': 'ignored'}, revision_id=None,
                entry_node_id='read', session_id=ref.session_id)
            await host.join(second_ref)
            second = await host.get_execution_view(second_ref.invocation_id)
            self.assertEqual(second.status, 'completed', await second.error())
            self.assertEqual(await second.output(), {'text': 'before'})
            third_ref = await host.execute_json('business', {'text': 'new'}, revision_id=None,
                entry_node_id='write', session_id=ref.session_id)
            await host.join(third_ref)
            third = await host.get_execution_view(third_ref.invocation_id)
            self.assertEqual(await first.context(pointer='/last/text'), 'before')
            self.assertEqual(await third.context(pointer='/last/text'), 'new')
            self.assertEqual([await item.body() for item in await first.messages()], [{'text': 'before'}])
            _, independent = await self.execute(host, value={'text': 'independent'})
            self.assertNotEqual(independent.session_id, first.session_id)
            self.assertEqual(await independent.context(pointer='/last/text'), 'independent')

    async def test_missing_history_never_establishes_that_a_stage_did_not_run(self):
        """Deleting a historical fact invalidates negative business queries instead of returning false."""
        workflow = Workflow('business', nodes=[Node('write', echo)])
        async with self.host([workflow]) as host:
            ref, view = await self.execute(host)
            async def corrupt():
                async with host._persistence._writer.storage.engine.begin() as conn:
                    events = meta.tables['runtime_events']
                    identity = await conn.scalar(select(events.c.event_id).where(
                        events.c.invocation_id == ref.invocation_id).order_by(events.c.sequence).limit(1))
                    await conn.execute(update(events).where(events.c.event_id == identity).values(sequence=999999))
            await host._persistence._writer.run(corrupt)
            with self.assertRaises(ViewUnavailable) as error:
                await view.node('write').was_executed()
            self.assertEqual(error.exception.code, 'HISTORY_UNAVAILABLE')


@unittest.skipUnless(POSTGRES_TEST_URL,'PostgreSQL is not configured.')
class PostgreSQLExecutionViewTests(ExecutionViewTests):
    pass
