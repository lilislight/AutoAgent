"""Ownership, ordering and lifecycle regression boundaries for Core optimizations."""
import asyncio
import random
import threading
import unittest
from dataclasses import replace
from types import MappingProxyType
from unittest.mock import patch
from typing_extensions import TypedDict
from pydantic import BaseModel, field_validator
from autoagent import ContextOperation
from autoagent.core.context import _ContextEdit, apply_context_operation
from autoagent.core.runtime._chunked import ChunkedMap, MapEdit, ChunkedUnits
from autoagent.core.runtime.transitions import _apply_context_operations
from autoagent.core.runtime.state import ChildInvocationPlan, ChildUnitState
from autoagent.core.runtime.operations import StateOperation
from autoagent.core.operators.contract import ValueContract
from autoagent.core.executor.node_executor import _BurstThreadPool
from autoagent.core.executor.future import await_concurrent_future, _notifiers

class Document(TypedDict):
    items: list[int]

class ModeSensitive(BaseModel):
    model_config = {'extra':'forbid'}
    value: int
    @field_validator('value', mode='before')
    @classmethod
    def mode(cls, value, info):
        return value + (1 if info.mode == 'python' else 0)

class ContainerTests(unittest.TestCase):
    def test_chunked_map_preserves_order_snapshots_and_block_sharing(self):
        """Random batches match dict ordering without modifying retained snapshots."""
        rng=random.Random(42)
        reference={str(i):i for i in range(2000)}
        current=ChunkedMap(reference)
        old=current
        edit=MapEdit(current);edit['0']=-1;current=edit.finish()
        self.assertIs(old._blocks[1],current._blocks[1])
        reference['0']=-1
        snapshots=[]
        for batch in range(40):
            snapshots.append((current,dict(reference)))
            edit=MapEdit(current)
            for _ in range(40):
                key=str(rng.randrange(2400))
                if rng.random()<0.4:
                    reference.pop(key,None);edit.pop(key,None)
                else:
                    value=rng.randrange(999)
                    reference[key]=value;edit[key]=value
            current=edit.finish()
            self.assertEqual(list(current.items()),list(reference.items()))
        for state, expected in snapshots:
            self.assertEqual(list(state.items()),list(expected.items()))
        self.assertEqual(old['0'],0)
        with self.assertRaises(AttributeError):old.__init__({})
        with self.assertRaises(TypeError):old._blocks[0]['0']=99

    def test_delete_reinsert_and_compaction_preserve_order(self):
        """Deleted keys append on reinsert and empty blocks do not grow forever."""
        reference={str(i):i for i in range(1000)}
        current=ChunkedMap(reference)
        for iteration in range(20):
            edit=MapEdit(current)
            for key in tuple(reference)[:300]:
                del reference[key];del edit[key]
                reference[key]=iteration;edit[key]=iteration
            current=edit.finish()
            self.assertEqual(list(current.items()),list(reference.items()))
        self.assertLess(len(current._blocks),32)

    def test_child_units_roundtrip_and_share_unmodified_blocks(self):
        """Child unit updates preserve old plans and canonical operation records."""
        units=ChunkedUnits(ChildUnitState(i,f's{i}',f'i{i}',None) for i in range(1000))
        updated=units.replace_at(129,replace(units[129],phase='opened'))
        self.assertIs(units._blocks[0],updated._blocks[0])
        self.assertEqual(units[129].phase,'planned')
        self.assertEqual(updated[129].phase,'opened')
        self.assertEqual(tuple(updated),updated)
        plan=ChildInvocationPlan('p','a@root','await','w','r',updated)
        operation=StateOperation._from_owned('add',('invocation','child_plans','p'),plan)
        restored=StateOperation.from_record(operation.to_record())
        self.assertEqual(restored.value,plan)
        self.assertEqual(restored.to_record(),operation.to_record())
        with self.assertRaises(AttributeError):units.__init__(())

    def test_large_map_replay_and_checkpoint_preserve_canonical_state(self):
        """Live block maps and replayed/restored State encode the same records."""
        from autoagent import AutoAgentApp, Map, Node, Workflow
        from autoagent.core.runtime import StateReducer, RuntimeState
        from tests.benchmarks.benchmark_core_audit import identity, items
        class Sink:
            def __init__(self): self.events=[]
            async def append(self,event): self.events.append(event)
        sink=Sink();app=AutoAgentApp(runtime_event_sink=sink)
        try:
            result=app.invoke(Workflow('block-map',nodes=[Node('map',identity,input_mapping=items,map=Map(max_parallelism=16))]),{'value':530})
            self.assertEqual(result.status,'completed',result.error)
            state=app._repository.state(result.session_id)
            self.assertIsInstance(state.invocation.scheduler.operator_calls,ChunkedMap)
            self.assertEqual(StateReducer().reduce(tuple(sink.events)),state)
            self.assertEqual(RuntimeState.from_record(state.to_record()),state)
        finally:app.close()

    def test_large_child_plan_finishes_and_roundtrips(self):
        """Child execution across unit blocks retains stage and checkpoint semantics."""
        from autoagent import AutoAgentApp, Map, Node, Workflow
        from autoagent.core.runtime import RuntimeState
        from tests.benchmarks.benchmark_core_audit import identity, items
        child=Workflow('block-child',nodes=[Node('a',identity)])
        app=AutoAgentApp(max_operator_concurrency=16)
        try:
            result=app.invoke(Workflow('block-parent',nodes=[Node('children',child,input_mapping=items,map=Map(max_parallelism=16))]),{'value':513})
            self.assertEqual(result.status,'completed',result.error)
            self.assertEqual(len(result.output),513)
            state=app._repository.state(result.session_id)
            plan=next(iter(state.invocation.child_plans.values()))
            self.assertIsInstance(plan.units,ChunkedUnits)
            self.assertTrue(all(unit.phase=='terminal' for unit in plan.units))
            self.assertEqual(RuntimeState.from_record(state.to_record()),state)
        finally:app.close()

class ContextTests(unittest.TestCase):
    def test_batch_matches_sequential_parent_child_and_delete_semantics(self):
        """Shared workspaces preserve ordered set/delete behavior across nested paths."""
        original=MappingProxyType({'a':MappingProxyType({'x':1}), 'z':0})
        operations=(ContextOperation.set(('a','x'),2),ContextOperation.set('a',{'y':3}),
            ContextOperation.set(('a','z'),4),ContextOperation.delete('a'),
            ContextOperation.set(('a','k'),5),ContextOperation.delete(('missing','leaf')))
        sequential=original;edit=_ContextEdit(original)
        for operation in operations:
            sequential=apply_context_operation(sequential,operation);edit.apply(operation)
        self.assertEqual(edit.finish(),sequential)
        self.assertEqual(original['a'],{'x':1})

    def test_empty_patch_and_conflicts_keep_original_state(self):
        """Empty patches share revisions while overlapping concurrent writes still fail."""
        context=MappingProxyType({'a':1});revisions=MappingProxyType({('a',):10})
        result=_apply_context_operations(context,revisions,(),0,20)
        self.assertIs(result[0],context);self.assertIs(result[1],revisions)
        with self.assertRaisesRegex(Exception,'changed after Node start'):
            _apply_context_operations(context,revisions,(ContextOperation.set('b',2),ContextOperation.set('a',3)),0,20)
        self.assertEqual(context,{'a':1});self.assertEqual(revisions,{('a',):10})

class ContractTests(unittest.TestCase):
    def test_plain_internal_contract_keeps_strict_canonical_semantics(self):
        """Simple fast restoration agrees with JSON restoration on valid and invalid records."""
        contract=ValueContract.create(Document,location='test')
        self.assertTrue(contract._python_record_equivalent)
        for value in ({'items':[1,2]}, {'items':[True]}, {'items':[1.2]}, {'items':['1']},
            {'items':[1],'extra':1},{'items':(1,)},{'items':[2**100]}):
            try:
                expected=contract.restore(value)
            except TypeError:
                with self.assertRaises(TypeError):contract._restore_internal(value)
            else:self.assertEqual(contract._restore_internal(value),expected)

    def test_mode_sensitive_model_uses_original_json_restore(self):
        """Custom validation retains JSON-mode behavior and never uses the fast path."""
        contract=ValueContract.create(ModeSensitive,location='test')
        self.assertFalse(contract._python_record_equivalent)
        self.assertEqual(contract._restore_internal({'value':1}).value,1)

class DispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_workers_reuse_and_shutdown_releases_threads(self):
        """Sequential calls reuse a daemon worker and shutdown joins it."""
        pool=_BurstThreadPool(1)
        try:
            ids=[await await_concurrent_future(pool.submit(threading.get_ident)) for _ in range(20)]
            self.assertEqual(len(set(ids)),1)
            threads=tuple(pool._threads)
            self.assertTrue(all(thread.daemon for thread in threads))
        finally:pool.shutdown()
        self.assertTrue(all(not thread.is_alive() for thread in threads))

    async def test_completed_future_has_cancellation_boundary_without_pipe(self):
        """Completed results avoid pipe allocation while retaining asynchronous cancellation."""
        from concurrent.futures import Future
        future=Future();future.set_result(1)
        with patch('autoagent.core.executor.future.os.pipe',side_effect=AssertionError('pipe created')):
            self.assertEqual(await await_concurrent_future(future),1)
            task=asyncio.create_task(await_concurrent_future(future))
            asyncio.get_running_loop().call_soon(task.cancel)
            with self.assertRaises(asyncio.CancelledError):await task

    def test_runtime_loop_releases_pinned_notifier(self):
        """Loop-owned notification resources disappear when the App closes."""
        from autoagent import AutoAgentApp
        from tests.benchmarks.benchmark_core_audit import Value, identity
        from autoagent import Node, Workflow
        before=set(_notifiers)
        app=AutoAgentApp()
        app.invoke(Workflow('notify-life',nodes=[Node('a',identity)]),{'value':1})
        self.assertEqual(len(set(_notifiers)-before),1)
        app.close()
        self.assertEqual(set(_notifiers),before)
