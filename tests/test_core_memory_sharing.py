"""Identity, ownership, ordered updates and history safety of the live Core."""
from __future__ import annotations

import asyncio
import json
import unittest
from dataclasses import replace
from types import MappingProxyType
from unittest.mock import patch
from typing_extensions import TypedDict

from autoagent import AutoAgentApp, ContextOperation, ContextPatch, Edge, Node, Workflow, OutputBindingContext, RuntimeTransitionError, RuntimeInfrastructureError
from autoagent.core.runtime import (
    RuntimeEvent, RuntimeRepository, RuntimeState, SessionState, SessionOpened,
    StateDelta, StateOperation, StateReducer, OperatorCallCompleted,
)
from autoagent.core.runtime.operations import apply_runtime_delta, _PathEdit
from autoagent.core.runtime.values import freeze, thaw


class Nested(TypedDict):
    rows: list[int]


def identity(value: Nested) -> Nested:
    return value


def bind(context: OutputBindingContext) -> ContextPatch:
    return ContextPatch(invocation=(ContextOperation.set('wrapped', {'answer':context.output}),))


class OwnedValueTests(unittest.TestCase):
    def test_external_aliases_are_detached_and_internal_aliases_shared(self):
        """External proxy/tuple containers detach once; repeated references stay shared."""
        original={'rows':[1,2]}
        external=MappingProxyType({'left':original,'right':original})
        owned=freeze(external)
        original['rows'].append(3)
        self.assertEqual(thaw(owned['left']),{'rows':[1,2]})
        self.assertIs(owned['left'],owned['right'])
        self.assertIs(freeze(owned),owned)
        self.assertIs(freeze(owned['left']['rows']),owned['left']['rows'])
        self.assertIs(freeze({'wrapper':owned})['wrapper'],owned)
        with self.assertRaises(TypeError):owned['left']['rows'][0]=9
        with self.assertRaises(AttributeError):owned._data={}
        with self.assertRaises(AttributeError):del owned._data

    def test_cycles_and_invalid_values_remain_rejected(self):
        """Memoized ownership must reject cycles, non-finite floats and invalid keys."""
        cycle=[];cycle.append(cycle)
        for value in (cycle,{'x':float('nan')},{1:'invalid'},object()):
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaises(TypeError):freeze(value)

    def test_mutable_view_cannot_modify_owned_history(self):
        """A mutable output view does not retain aliases into an owned Event value."""
        value=freeze({'rows':[1,2]})
        view=thaw(value);view['rows'].append(3)
        self.assertEqual(value['rows'],(1,2))


class BatchedPathTests(unittest.TestCase):
    def state(self, context):
        return RuntimeState(session=SessionState('s',freeze(context),0,0))

    def apply(self,state,*operations):
        return apply_runtime_delta(state,StateDelta(tuple(StateOperation(op,('session','context',*path),value) for op,path,value in operations)))

    def test_replacing_parent_discards_prior_edits_and_later_edits_use_replacement(self):
        """Overlapping operations obey original order without mutating inserted values."""
        state=self.state({'a':{'x':1},'untouched':{'value':7}})
        replacement=freeze({'x':10})
        result=self.apply(state,('replace',('a','x'),2),('replace',('a',),replacement),('replace',('a','x'),11))
        self.assertEqual(result.session.context['a']['x'],11)
        self.assertEqual(replacement['x'],10)
        self.assertEqual(state.session.context['a']['x'],1)
        self.assertIs(result.session.context['untouched'],state.session.context['untouched'])

    def test_remove_add_and_tuple_index_shifts_preserve_order(self):
        """Delete/recreate a parent and shifted tuple edits follow sequential semantics."""
        state=self.state({'a':{'x':1},'items':[{'x':1},{'x':2}]})
        result=self.apply(state,
            ('remove',('a',),None),('add',('a',),{'x':4}),('replace',('a','x'),5),
            ('replace',('items',1,'x'),20),('add',('items',0),{'x':0}),
            ('remove',('items',1),None),('replace',('items',1,'x'),21))
        self.assertEqual(thaw(result.session.context),{'a':{'x':5},'items':[{'x':0},{'x':21}]})
        self.assertEqual(thaw(state.session.context),{'a':{'x':1},'items':[{'x':1},{'x':2}]})

    def test_invalid_trailing_operation_leaves_state_and_delta_values_unchanged(self):
        """A transaction failure cannot publish earlier edits or mutate supplied values."""
        state=self.state({'a':{'x':1}});value=freeze({'x':5})
        with self.assertRaises(RuntimeTransitionError):
            self.apply(state,('replace',('a',),value),('replace',('a','x'),6),('remove',('missing',),None))
        self.assertEqual(value['x'],5)
        self.assertEqual(state.session.context['a']['x'],1)
        for index in (-1,3):
            with self.assertRaises((ValueError,RuntimeTransitionError)):
                self.apply(self.state({'items':[1]}),('replace',('items',index),2))

    def test_shared_values_are_not_mistaken_for_shared_edit_workspaces(self):
        """Editing one alias must not edit another path pointing at the same value."""
        value={'x':1};state=self.state({'a':value,'b':value})
        self.assertIs(state.session.context['a'],state.session.context['b'])
        result=self.apply(state,('replace',('a','x'),2))
        self.assertEqual(result.session.context['b']['x'],1)
        self.assertIs(result.session.context['b'],state.session.context['b'])

    def test_common_ancestors_are_copied_once_per_delta(self):
        """Multiple leaf writes share each changed ancestor within one transaction."""
        state=self.state({'a':{'x':1,'y':2}});counts={}
        original=_PathEdit.__init__
        def counted(editor,value):
            counts[id(value)]=counts.get(id(value),0)+1
            original(editor,value)
        with patch.object(_PathEdit,'__init__',counted):
            result=self.apply(state,('replace',('a','x'),3),('replace',('a','y'),4))
        self.assertEqual(max(counts.values()),1)
        self.assertEqual(result.session.context['a'],{'x':3,'y':4})

    def test_user_keys_are_not_decoded_as_domain_objects(self):
        """User data called operator_calls or execution must remain ordinary data."""
        state=self.state({'operator_calls':{'id':{}},'execution':{}})
        result=self.apply(state,('replace',('operator_calls','id'),{'output':1}),('replace',('execution',),{'phase':'user'}))
        self.assertEqual(result.session.context['operator_calls']['id'],{'output':1})


class RuntimeSharingTests(unittest.TestCase):
    def test_live_pipeline_shares_values_without_record_conversion(self):
        """Retained Events share accepted values while terminal State drops Call data."""
        events=[]
        class Sink:
            async def append(self,event):events.append(event)
        app=AutoAgentApp(runtime_event_sink=Sink())
        try:
            workflow=Workflow('sharing',nodes=[Node('a',identity,output_binding=bind)])
            with patch('autoagent.core.runtime.operations._encode_runtime',side_effect=AssertionError('live encoding')):
                result=app.invoke(workflow,{'rows':[1,2]})
            self.assertEqual(result.status,'completed',result.error)
            state=app._repository.state(result.session_id)
            completion=next(e for e in events if e.event_name=='operator_call.completed')
            value=completion.payload.output
            output_op=next(op for op in completion.delta.operations if op.path[-1]=='output')
            self.assertIs(value,output_op.value)
            call=state.invocation.scheduler.operator_calls[completion.payload.call_id]
            self.assertIsNone(call.output)
            self.assertIsNone(call.input)
            node_event=next(e for e in events if e.event_name=='node_occurrence.completed')
            self.assertIs(node_event.payload.output,value)
            self.assertIsNone(state.invocation.scheduler.occurrences['a@root'].output)
            self.assertIs(state.invocation.context['wrapped']['answer'],value)
            self.assertIs(state.invocation.output,value)
            self.assertEqual(app._repository._pending, {})
            result.output['rows'].append(99)
            self.assertEqual(value['rows'],(1,2))
            for event in events:
                decoded=RuntimeEvent.from_record(json.loads(json.dumps(event.to_record())))
                self.assertEqual(decoded,event)
            replay=StateReducer().reduce(tuple(RuntimeEvent.from_record(e.to_record()) for e in events))
            self.assertEqual(replay,state)
        finally:app.close()

    def test_user_retained_output_and_public_checkpoint_cannot_mutate_state(self):
        """User-owned output and detached result cannot rewrite accepted history."""
        retained={'rows':[1,2]}
        def returns_retained(value:Nested)->Nested:return retained
        app=AutoAgentApp()
        try:
            result=app.invoke(Workflow('retained',nodes=[Node('a',returns_retained)]),{'rows':[]})
            self.assertEqual(result.status,'completed',result.error)
            state=app._repository.state(result.session_id)
            retained['rows'].append(3)
            self.assertEqual(state.invocation.output['rows'],(1,2))
            checkpoint=app.unload_session(result.ref, capture_checkpoint=True)
            with self.assertRaises(TypeError):checkpoint.state.invocation.output['rows'][0]=42
        finally:app.close()

    def test_parallel_operator_inputs_cannot_change_each_other(self):
        """Concurrent Operators have private input containers and preserve source output."""
        async def left(value:Nested)->Nested:
            value['rows'].append(2)
            await asyncio.sleep(.01)
            return value
        async def right(value:Nested)->Nested:
            await asyncio.sleep(.005)
            self.assertEqual(value['rows'],[1])
            return value
        app=AutoAgentApp()
        try:
            workflow=Workflow('parallel-isolation',nodes=[Node('source',identity),Node('left',left),Node('right',right)],edges=[Edge('source','left'),Edge('source','right')])
            result=app.invoke(workflow,{'rows':[1]})
            self.assertEqual(result.status,'completed',result.error)
            state=app._repository.state(result.session_id)
            self.assertIsNone(state.invocation.scheduler.occurrences['source@root'].output)
            self.assertEqual(result.output['left']['rows'],[1,2])
            self.assertEqual(result.output['right']['rows'],[1])
        finally:app.close()


class PendingSharingTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_sink_retries_exact_event_and_immutable_value(self):
        """An ambiguous append retains one stable Event while external inputs change."""
        events=[]
        class Sink:
            async def append(self,event):
                events.append(event)
                with self_test.assertRaises(TypeError):event.payload.context['rows'][0]=9
                if len(events)==1:raise OSError('ambiguous append')
        self_test=self
        repository=RuntimeRepository(sink=Sink())
        original={'rows':[1]};payload=SessionOpened(original)
        with self.assertRaises(RuntimeInfrastructureError):
            await repository.commit(session_id='s',invocation_id=None,payload=payload)
        original['rows'].append(2)
        self.assertIsNone(repository.state('s').session)
        await repository.settle('s')
        self.assertIs(events[0],events[1])
        self.assertIs(repository.state('s').session.context,payload.context)
        self.assertEqual(repository.state('s').session.context['rows'],(1,))
