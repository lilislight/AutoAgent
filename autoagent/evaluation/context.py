"""Scenario controls and a separate read-only Evaluator context."""

import asyncio
from dataclasses import dataclass
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any
from uuid import uuid4

from .evidence import Evidence
from .models import CheckResult, EvalCase, EvaluationError, json_copy


@dataclass(frozen=True)
class EvaluatorContext:
    case: EvalCase
    evidence: Evidence | None
    observations: tuple[Evidence, ...]
    resources: Mapping[str, Any]


class ScenarioContext:
    def __init__(self, environment, options):
        self.environment = environment
        self.case = environment.case
        self.resources = environment.resources
        self._options = options
        self._host = None
        self._sessions = {}
        self._roots = {}
        self._submissions = set()
        self._observations = []
        self._checks = []

    @property
    def observations(self):
        return tuple(self._observations)

    async def invoke(self, workflow_id, value, *, entry_node_id, revision_id=None,
                     session='default', until='terminal', timeout=None):
        """Session aliases belong to this Trial, never to another Case or Run."""
        if not isinstance(session, str) or not session:
            raise ValueError('Session alias must be a nonempty string.')
        sid = self._sessions.setdefault(session, str(uuid4()))

        async def submit():
            ref = await self._host.execute(workflow_id, json_copy(value), revision_id=revision_id,
                entry_node_id=entry_node_id, session_id=sid)
            self._roots[ref.invocation_id] = ref
            return ref

        task = asyncio.create_task(submit())
        self._submissions.add(task)
        task.add_done_callback(self._submissions.discard)
        ref = await asyncio.shield(task)
        return await self.observe(ref, until=until, timeout=timeout)

    def _iid(self, execution):
        return execution.invocation_id if hasattr(execution, 'invocation_id') else execution

    async def observe(self, execution, *, until='terminal', timeout=None):
        iid = self._iid(execution)
        if until not in ('none', 'boundary', 'terminal'):
            raise ValueError('until must be none, boundary or terminal.')
        async with asyncio.timeout(timeout):
            while True:
                state = await self._host.wait_execution(iid, until=until)
                if state['observation'] == 'interrupted':
                    raise EvaluationError('Execution was interrupted; recover explicitly in the Scenario.')
                snapshot = await self._host.capture_execution_evidence(iid)
                if until == 'none' or snapshot['settled'] or until == 'boundary' and snapshot['at_boundary']:
                    evidence = Evidence(self._host, snapshot, self._options.evidence_event_limit)
                    self._observations.append(evidence)
                    return evidence
                # A Timer may awaken between observation and durable capture.
                await asyncio.sleep(.01)

    async def respond(self, execution, response, *, wait_id=None, node_id=None, until='terminal', timeout=None):
        """Select exactly one external Wait, including one owned by a child."""
        observation = await self.observe(execution, until='boundary', timeout=timeout)
        candidates = [wait for member in observation.snapshot['members'] for wait in member['waits']
            if wait['kind'] == 'external' and (wait_id is None or wait['wait_id'] == wait_id)
            and (node_id is None or wait.get('node_id') == node_id)]
        if len(candidates) != 1:
            raise EvaluationError(f'Expected one matching external Wait; found {len(candidates)}. Select wait_id or node_id.')
        wait = candidates[0]
        await self._host.perform_execution_action(observation.snapshot['root_invocation_id'], 'respond',
            target_invocation_id=wait['invocation_id'], wait_id=wait['wait_id'], payload=json_copy(response))
        return await self.observe(execution, until=until, timeout=timeout)

    async def signal(self, execution, endpoint, payload, *, source_id, sequence, until='terminal', timeout=None):
        await self._host.signal(self._iid(execution), endpoint, json_copy(payload), source_id=source_id, sequence=sequence)
        return await self.observe(execution, until=until, timeout=timeout)

    async def pause(self, execution, *, timeout=None):
        await self._host.pause(self._iid(execution), timeout=timeout)
        return await self.observe(execution, until='boundary', timeout=timeout)

    async def recover(self, execution, *, until='terminal', timeout=None):
        await self._host.recover(self._iid(execution))
        return await self.observe(execution, until=until, timeout=timeout)

    async def cancel(self, execution, *, reason='Evaluation Scenario cancellation', timeout=None):
        await self._host.cancel(self._iid(execution), reason=reason)
        return await self.observe(execution, timeout=timeout)

    def check(self, result):
        """Retain a phase-specific business check without hiding independent failures."""
        if not isinstance(result, CheckResult):
            raise TypeError('check requires CheckResult.')
        self._checks.append(result.to_record())
        return result.passed

    def evaluator_context(self, evidence):
        return EvaluatorContext(self.case, evidence, self.observations, MappingProxyType(self.resources))

    async def _cleanup(self):
        if self._submissions:
            await asyncio.gather(*tuple(self._submissions), return_exceptions=True)
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
