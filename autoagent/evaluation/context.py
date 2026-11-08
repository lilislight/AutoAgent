"""Workflow scenarios and business rules consume only Host ExecutionView."""

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any
from uuid import uuid4

from autoagent.host.execution_view import ExecutionView, Interaction

from .models import CheckResult, EvalCase, EvaluationError, json_copy


@dataclass(frozen=True)
class EvaluatorContext:
    case: EvalCase
    execution: ExecutionView | None
    observations: tuple[ExecutionView, ...]
    resources: Mapping[str, Any]


class ScenarioContext:
    def __init__(self, environment, options):
        self.environment, self.resources = environment, environment.resources
        self._options, self._host = options, None
        self._sessions, self._roots = {}, {}
        self._submissions, self._observations, self._checks = set(), [], []
        self._read_scope = None

    @property
    def observations(self): return tuple(self._observations)

    @property
    def case(self): return self.environment.case

    def _iid(self, execution):
        if isinstance(execution, Interaction): execution = execution.execution
        return execution.invocation_id if isinstance(execution, ExecutionView) else execution

    async def _owned(self, execution):
        iid = self._iid(execution)
        state = await self._host.get_execution_state(iid)
        if state['invocation_id'] not in self._roots:
            raise ValueError('Scenario controls are limited to this Trial\'s executions.')
        return iid, state

    async def _capture(self, execution):
        iid, _ = await self._owned(execution)
        return await self._host.get_execution_view(iid, read_limit=self._options.read_limit)

    async def invoke(self, workflow_id, value, *, entry_node_id, revision_id=None,
                     session='default', until='terminal', timeout=None):
        if not isinstance(session, str) or not session: raise ValueError('Session alias must be nonempty.')
        sid = self._sessions.setdefault(session, str(uuid4()))
        async def submit():
            ref = await self._host.execute_json(workflow_id, json_copy(value), revision_id=revision_id,
                entry_node_id=entry_node_id, session_id=sid)
            self._roots[ref.invocation_id] = ref
            return ref
        async with asyncio.timeout(timeout):
            task = asyncio.create_task(submit())
            self._submissions.add(task)
            task.add_done_callback(self._submissions.discard)
            ref = await asyncio.shield(task)
            return await self.observe(ref.invocation_id, until=until)

    async def observe(self, execution, *, until='terminal', timeout=None):
        if until not in ('none', 'boundary', 'terminal'): raise ValueError('until must be none, boundary or terminal.')
        iid, _ = await self._owned(execution)
        async with asyncio.timeout(timeout):
            while True:
                state = await self._host.wait_execution(iid, until=until)
                if state['observation'] == 'interrupted':
                    raise EvaluationError('Execution was interrupted; recover explicitly in the Scenario.')
                view = await self._capture(iid)
                if until == 'none' or view.settled or until == 'boundary' and view.to_record()['at_boundary']:
                    self._observations.append(view)
                    return view
                await asyncio.sleep(.01)

    async def wait_for(self, execution, *, node_id=None, state=None, interaction_node=None,
                       child_workflow_id=None, message_kind=None, timeout=None):
        """Await one business condition; polling never retains intermediate observations."""
        if sum((node_id is not None, interaction_node is not None, child_workflow_id is not None, message_kind is not None)) != 1:
            raise ValueError('Choose one Node state, pending approval, Child Workflow or message condition.')
        if node_id is not None and state is None: raise ValueError('Node condition requires state.')
        if node_id is not None and state not in ('ready','running','waiting','completed','failed','skipped','cancelled'):
            raise ValueError('Unknown business Node state.')
        async with asyncio.timeout(timeout):
            while True:
                view = await self._capture(execution)
                match = False
                if node_id is not None:
                    match = any(item.status == state for item in await view.node(node_id).all())
                elif interaction_node is not None:
                    pending = [item for item in await view.interactions(node_id=interaction_node, family=True) if item.kind == 'external']
                    if len(pending) > 1: raise EvaluationError('Approval is ambiguous; select a specific interaction in the business view.')
                    match = bool(pending)
                elif child_workflow_id is not None:
                    children = view.children(workflow_id=child_workflow_id, recursive=True)
                    match = bool(children) and all(item.status in ('completed', 'failed', 'cancelled') for item in children)
                else:
                    match = bool(await view.messages(kind=message_kind))
                if match:
                    self._observations.append(view)
                    return view
                if view.settled: raise EvaluationError('Execution finished without satisfying the requested business condition.')
                await asyncio.sleep(.05)

    async def respond(self, execution, response, *, wait_id=None, node_id=None, until='terminal', timeout=None):
        async with asyncio.timeout(timeout):
            while True:
                view = await self._capture(execution)
                candidates = [item for item in await view.interactions(node_id=node_id, family=True)
                    if item.kind == 'external' and (wait_id is None or item.id == wait_id)]
                if len(candidates) > 1: raise EvaluationError('Approval is ambiguous; select wait_id or node_id.')
                if candidates: break
                if view.settled: raise EvaluationError('Execution has no matching pending approval.')
                await asyncio.sleep(.05)
            target = candidates[0]
            await self._host.perform_execution_action(view.root_invocation_id, 'respond',
                target_invocation_id=target.execution.invocation_id, wait_id=target.id, payload=json_copy(response))
            return await self.observe(execution, until=until)

    async def signal(self, execution, endpoint, payload, *, source_id, sequence, until='terminal', timeout=None):
        async with asyncio.timeout(timeout):
            _, state = await self._owned(execution)
            await self._host.perform_execution_action(state['invocation_id'], 'signal', endpoint=endpoint,
                payload=json_copy(payload), source_id=source_id, sequence=sequence)
            return await self.observe(execution, until=until)

    async def pause(self, execution, *, timeout=None):
        _, state = await self._owned(execution)
        await self._host.pause(state['invocation_id'], timeout=timeout)
        return await self.observe(execution, until='boundary', timeout=timeout)

    async def recover(self, execution, *, until='terminal', timeout=None):
        async with asyncio.timeout(timeout):
            _, state = await self._owned(execution)
            await self._host.recover(state['invocation_id'])
            return await self.observe(execution, until=until)

    async def cancel(self, execution, *, reason='Evaluation Scenario cancellation', timeout=None):
        async with asyncio.timeout(timeout):
            _, state = await self._owned(execution)
            await self._host.cancel(state['invocation_id'], reason=reason)
            return await self.observe(execution)

    def check(self, result):
        if not isinstance(result, CheckResult): raise TypeError('Business rules must return CheckResult.')
        if any(item['name'] == result.name for item in self._checks):
            raise EvaluationError(f'Duplicate business rule name: {result.name}. Use stable, distinct phase names.')
        record = result.to_record()
        if self._read_scope is not None:
            record['evidence'] += json_copy(self._read_scope)
            self._read_scope.clear()
        self._checks.append(record)
        return result.passed

    def evaluator_context(self, execution):
        return EvaluatorContext(self.case, execution, self.observations, MappingProxyType(self.resources))

    async def _cleanup(self):
        if self._submissions: await asyncio.gather(*tuple(self._submissions), return_exceptions=True)
        errors = []
        for ref in tuple(self._roots.values()):
            try:
                state = await self._host.get_execution_state(ref.invocation_id)
                if not state['settled']:
                    await self._host.cancel(ref, reason='Evaluation cleanup')
                    await self._host.wait_execution(ref.invocation_id, until='terminal')
            except Exception as error:
                errors.append(dict(invocation_id=ref.invocation_id, type=type(error).__name__, message=str(error)))
        return errors
