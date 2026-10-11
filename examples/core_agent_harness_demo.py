"""Core-only, deterministic Agent collaboration; no providers or storage needed.

Run: .venv/bin/python -m examples.core_agent_harness_demo
     .venv/bin/python -m examples.core_agent_harness_demo --scenario approved --verbose

Parent: mock planner -> Map/Spawn two Children -> Select(message, Child status)
        -> feedback or external Wait/Resume -> SendSignal -> supervise again.
Child:  mock LLM -> SendSignal(Owner) -> AwaitSignal -> validate decision
        -> revise through a graph back edge, or invoke an approved mock tool.

Python handlers finish normally; all waits and revision loops are explicit Nodes.
Recovery rules are declared, but this example does not capture/reload Checkpoints.
The two ChildCase slots match this example's fixed two-task plan. They are not a
general supervisor for an arbitrary number of dynamically spawned agents.
"""

import argparse
import asyncio
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
import json
import threading
import time
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from autoagent import (
    AutoAgentApp, AwaitSignal, ChildCase, ConditionContext, ContextOperation,
    ContextPatch, Edge, InputMappingContext, Map, Node, OutputBindingContext,
    OwnerHandle, RuntimeHandle, RuntimeObservation, Select,
    SelectResult, SendSignal, SignalBatch, SignalCase, SignalEndpoint, Spawn,
    Wait, Workflow,
)


Scenario = Literal['approved', 'rejected', 'llm_failure', 'tool_failure']


def plain(value):
    """Convert Core's immutable hook views to plain records for nested models."""
    if isinstance(value, Mapping):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value


class Model(BaseModel):
    model_config = ConfigDict(extra='forbid')

    @model_validator(mode='before')
    @classmethod
    def from_hook_view(cls, value):
        return plain(value)


class RunSpec(Model):
    run_id: str
    scenario: Scenario = 'approved'


class Task(Model):
    run_id: str
    task_id: str
    instruction: str
    tool: Literal['search', 'publish']
    fail_llm: bool = False
    fail_tool: bool = False


class Plan(Model):
    tasks: list[Task]


class Children(Model):
    handles: list[RuntimeHandle]


class Draft(Model):
    task_id: str
    attempt: int
    proposal: str
    operation_key: str


class DraftInput(Model):
    task: Task
    attempt: int
    feedback: str


class ReviewRequest(Model):
    child: RuntimeHandle
    task: Task
    draft: Draft


class Decision(Model):
    operation_key: str
    action: Literal['revise', 'approve', 'reject']
    feedback: str


class DecisionInput(Model):
    draft: Draft
    batch: SignalBatch


class ToolCall(Model):
    task: Task
    draft: Draft
    decision: Decision


class TaskResult(Model):
    task_id: str
    outcome: Literal['executed', 'rejected']
    text: str
    operation_key: str


class ApprovalRequest(Model):
    task_id: str
    proposal: str
    operation_key: str


class ApprovalResponse(Model):
    operation_key: str
    approved: bool
    reason: str


class Instruction(Model):
    text: str


class ParentState(Model):
    handles: list[RuntimeHandle]
    versions: list[str]
    results: dict[str, RuntimeObservation] = Field(default_factory=dict)


class SupervisorInput(Model):
    state: ParentState
    choice: SelectResult


class Action(Model):
    kind: Literal['feedback', 'approval', 'continue', 'finish']
    state: ParentState
    request: ReviewRequest | None = None
    decision: Decision | None = None


class Outcome(Model):
    task_id: str
    runtime_status: str
    outcome: str
    text: str
    error: str | None = None


class Summary(Model):
    status: Literal['completed', 'degraded']
    tasks: list[Outcome]


class MockServices:
    """Fake providers, tools and remote idempotency storage, scoped to one run.

    The cache models a remote service's atomic key/result contract. It is only
    in memory here; it does not prove crash recovery or exactly-once effects.
    A lock also protects snapshots shared with the external approval driver.
    """

    def __init__(self):
        self.lock = threading.RLock()
        self.timeline = []
        self.approved = set()
        self.effects = {}
        self.runtime_counts = Counter()
        self.llm_active = 0
        self.llm_max_parallel = 0

    def record(self, kind: str, **details):
        with self.lock:
            self.timeline.append({'sequence': len(self.timeline) + 1,
                                  'kind': kind, **details})

    async def append(self, event):
        # Observe Runtime Events without adding Host or Persistence.
        with self.lock:
            self.runtime_counts[event.payload.kind] += 1

    def snapshot(self):
        with self.lock:
            return {'timeline': list(self.timeline),
                    'effects': dict(self.effects),
                    'runtime_events': dict(self.runtime_counts),
                    'llm_max_parallel': self.llm_max_parallel}

    async def plan(self, spec: RunSpec) -> Plan:
        self.record('parent.plan', scenario=spec.scenario)
        return Plan(tasks=[
            Task(run_id=spec.run_id, task_id='research', instruction='Find supporting evidence',
                 tool='search', fail_llm=spec.scenario == 'llm_failure'),
            Task(run_id=spec.run_id, task_id='publish', instruction='Publish the reviewed report',
                 tool='publish', fail_tool=spec.scenario == 'tool_failure'),
        ])

    async def llm(self, value: DraftInput) -> Draft:
        task = value.task
        with self.lock:
            self.llm_active += 1
            self.llm_max_parallel = max(self.llm_max_parallel, self.llm_active)
        self.record('child.llm.started', task_id=task.task_id, attempt=value.attempt,
                    feedback=value.feedback)
        try:
            await asyncio.sleep(0.015)  # Fake provider latency; permits real overlap.
            if task.fail_llm:
                raise RuntimeError('simulated LLM failure before sending a request')
            draft = Draft(task_id=task.task_id, attempt=value.attempt,
                          proposal=f'{task.instruction}; feedback: {value.feedback}',
                          operation_key=f'{task.run_id}/{task.task_id}/tool/{value.attempt}')
            self.record('child.llm.completed', **draft.model_dump())
            return draft
        finally:
            with self.lock:
                self.llm_active -= 1

    async def tool(self, call: ToolCall) -> TaskResult:
        if call.decision.action != 'approve' or call.decision.operation_key != call.draft.operation_key:
            raise ValueError('Tool requires approval for this exact operation')
        key = call.draft.operation_key
        with self.lock:
            if key not in self.approved:
                raise ValueError('External approver has not authorized this operation')
            self.record('child.tool.called', task_id=call.task.task_id, operation_key=key)
            if key in self.effects:
                return TaskResult.model_validate(self.effects[key])
        await asyncio.sleep(0.01)
        if call.task.fail_tool:
            raise RuntimeError('simulated tool failure after approval')
        result = TaskResult(task_id=call.task.task_id, outcome='executed',
                            text=f'mock {call.task.tool} completed', operation_key=key)
        with self.lock:
            # The fake remote operation and its result are one atomic commit.
            existing = self.effects.setdefault(key, result.model_dump())
            self.record('child.tool.result', task_id=call.task.task_id, operation_key=key)
            return TaskResult.model_validate(existing)


def patch(**values) -> ContextPatch:
    return ContextPatch(invocation=tuple(ContextOperation.set(k, v) for k, v in values.items()))


def incoming(context: InputMappingContext):
    # Each routed join in this example receives exactly one selected input.
    return next(iter(context.incoming.values()))


def no_input(context: InputMappingContext) -> None:
    return None


def draft_input(context: InputMappingContext) -> DraftInput:
    previous = context.invocation_context.get('decision')
    return DraftInput(task=Task.model_validate(context.invocation_input),
                      attempt=context.invocation_context.get('attempt', 0) + 1,
                      feedback=previous['feedback'] if previous else 'initial draft')


def save_draft(context: OutputBindingContext) -> ContextPatch:
    return patch(draft=context.output, attempt=context.output['attempt'])


def review_request(context: InputMappingContext) -> ReviewRequest:
    return ReviewRequest(child=context.self_handle,
                         task=Task.model_validate(context.invocation_input),
                         draft=Draft.model_validate(context.invocation_context['draft']))


def decision_input(context: InputMappingContext) -> DecisionInput:
    return DecisionInput(draft=Draft.model_validate(context.invocation_context['draft']),
                         batch=SignalBatch.model_validate(plain(incoming(context))))


def save_decision(context: OutputBindingContext) -> ContextPatch:
    return patch(decision=context.output)


def decide_is(action):
    def condition(context: ConditionContext) -> bool:
        return context.output['action'] == action
    return condition


def tool_input(context: InputMappingContext) -> ToolCall:
    return ToolCall(task=Task.model_validate(context.invocation_input),
                    draft=Draft.model_validate(context.invocation_context['draft']),
                    decision=Decision.model_validate(context.invocation_context['decision']))


def rejected(call: ToolCall) -> TaskResult:
    return TaskResult(task_id=call.task.task_id, outcome='rejected',
                      text=call.decision.feedback, operation_key=call.draft.operation_key)


def result_input(context: InputMappingContext) -> TaskResult:
    return TaskResult.model_validate(incoming(context))


def result_identity(value: TaskResult) -> TaskResult:
    return value


def task_identity(value: Task) -> Task:
    return value


def child_workflow(services: MockServices) -> Workflow:
    def validate_decision(value: DecisionInput) -> Decision:
        decision = Decision.model_validate(value.batch.messages[0].payload)
        if decision.operation_key != value.draft.operation_key:
            raise ValueError('Stale or unrelated decision')
        services.record('child.decision', task_id=value.draft.task_id, **decision.model_dump())
        return decision

    return Workflow('mock_worker', signal_endpoints=[SignalEndpoint('decisions', Decision)], nodes=[
        Node('start', task_identity),
        Node('llm', services.llm, input_mapping=draft_input, output_binding=save_draft),
        Node('ask_parent', SendSignal(OwnerHandle(), 'reviews'), input_mapping=review_request),
        Node('wait_for_parent', AwaitSignal('decisions')),
        Node('decide', validate_decision, input_mapping=decision_input, output_binding=save_decision),
        Node('tool', services.tool, input_mapping=tool_input),
        Node('reject', rejected, input_mapping=tool_input),
        Node('finish', result_identity, input_mapping=result_input),
    ], edges=[
        Edge('start', 'llm'),
        Edge('llm', 'ask_parent'), Edge('ask_parent', 'wait_for_parent'),
        Edge('wait_for_parent', 'decide'),
        Edge('decide', 'llm', decide_is('revise'), id='revise_back'),
        Edge('decide', 'tool', decide_is('approve')),
        Edge('decide', 'reject', decide_is('reject')),
        Edge('tool', 'finish'), Edge('reject', 'finish'),
    ])


def state_from(context: InputMappingContext) -> ParentState:
    return ParentState.model_validate(context.invocation_context['state'])


def child_slot(index):
    def resolve(context: InputMappingContext) -> RuntimeHandle:
        return state_from(context).handles[index]
    return resolve


def child_version(index):
    def resolve(context: InputMappingContext) -> str:
        return state_from(context).versions[index]
    return resolve


def spawn_input(context: InputMappingContext) -> list[Task]:
    return Plan.model_validate(incoming(context)).tasks


def children_input(context: InputMappingContext) -> Children:
    return Children(handles=[RuntimeHandle.model_validate(h) for h in incoming(context)])


def initial_state(children: Children) -> ParentState:
    return ParentState(handles=children.handles, versions=[''] * len(children.handles))


def save_state(context: OutputBindingContext) -> ContextPatch:
    return patch(state=context.output)


def supervisor_input(context: InputMappingContext) -> SupervisorInput:
    return SupervisorInput(state=state_from(context), choice=SelectResult.model_validate(plain(incoming(context))))


def save_action(context: OutputBindingContext) -> ContextPatch:
    return patch(state=context.output['state'], action=context.output)


def action_is(kind):
    def condition(context: ConditionContext) -> bool:
        return context.output['kind'] == kind
    return condition


def current_action(context: InputMappingContext) -> Action:
    return Action.model_validate(context.invocation_context['action'])


def decision_target(context: InputMappingContext) -> RuntimeHandle:
    return current_action(context).request.child


def feedback_input(context: InputMappingContext) -> Decision:
    return current_action(context).decision


def approval_input(context: InputMappingContext) -> ApprovalRequest:
    request = current_action(context).request
    return ApprovalRequest(task_id=request.task.task_id, proposal=request.draft.proposal,
                           operation_key=request.draft.operation_key)


def approved_decision(context: InputMappingContext) -> Decision:
    response = ApprovalResponse.model_validate(incoming(context))
    request = current_action(context).request
    if response.operation_key != request.draft.operation_key:
        raise ValueError('Approval does not match the requested tool call')
    return Decision(operation_key=response.operation_key,
                    action='approve' if response.approved else 'reject', feedback=response.reason)


def tick() -> None:
    return None


def summary(state: ParentState) -> Summary:
    tasks = []
    for task_id, observation in sorted(state.results.items()):
        result = TaskResult.model_validate(observation.output) if observation.status == 'completed' else None
        tasks.append(Outcome(task_id=task_id, runtime_status=observation.status,
                             outcome=result.outcome if result else observation.status,
                             text=result.text if result else 'Child did not complete successfully',
                             error=f"{observation.error['type']}: {observation.error['message']}"
                             if observation.error else None))
    return Summary(status='degraded' if any(t.runtime_status != 'completed' for t in tasks) else 'completed', tasks=tasks)


def parent_workflow(services: MockServices) -> Workflow:
    def supervise(value: SupervisorInput) -> Action:
        state, choice = value.state, value.choice
        if choice.case == 'review':
            batch = SignalBatch.model_validate(choice.value)
            request = ReviewRequest.model_validate(batch.messages[0].payload)
            if request.child not in state.handles:
                raise ValueError('Review sender is not an owned Child')
            services.record('parent.review', task_id=request.task.task_id, attempt=request.draft.attempt)
            if request.draft.attempt == 1:
                decision = Decision(operation_key=request.draft.operation_key, action='revise',
                                    feedback='Add evidence and describe the side effects before executing')
                return Action(kind='feedback', state=state, request=request, decision=decision)
            return Action(kind='approval', state=state, request=request)
        if choice.case == 'instruction':
            batch = SignalBatch.model_validate(choice.value)
            services.record('parent.external_message', text=batch.messages[0].payload['text'])
        else:
            index = int(choice.case.removeprefix('child_'))
            observation = RuntimeObservation.model_validate(choice.value)
            state.versions[index] = observation.version
            services.record('parent.child_status', child=index, status=observation.status)
            if observation.status in {'completed', 'failed', 'cancelled'}:
                # This plan has two fixed, ordered task identities.
                state.results[('research', 'publish')[index]] = observation
        return Action(kind='finish' if len(state.results) == len(state.handles) else 'continue', state=state)

    return Workflow('mock_supervisor', signal_endpoints=[
        SignalEndpoint('reviews', ReviewRequest), SignalEndpoint('instructions', Instruction),
    ], nodes=[
        Node('plan', services.plan),
        Node('spawn_workers', Spawn(child_workflow(services), 'start'),
                  input_mapping=spawn_input, map=Map(max_parallelism=2)),
        Node('initialize', initial_state, input_mapping=children_input, output_binding=save_state),
        Node('select', Select({
            'review': SignalCase('reviews'), 'instruction': SignalCase('instructions'),
            'child_0': ChildCase(child_slot(0), after=child_version(0)),
            'child_1': ChildCase(child_slot(1), after=child_version(1)),
        })),
        Node('supervise', supervise, input_mapping=supervisor_input, output_binding=save_action),
        Node('feedback', SendSignal(decision_target, 'decisions'), input_mapping=feedback_input),
        Node('approval', Wait(ApprovalRequest, ApprovalResponse), input_mapping=approval_input),
        Node('send_decision', SendSignal(decision_target, 'decisions'), input_mapping=approved_decision),
        Node('loop_tail', tick, input_mapping=no_input),
        Node('summary', summary, input_mapping=state_from),
    ], edges=[
        Edge('plan', 'spawn_workers'), Edge('spawn_workers', 'initialize'), Edge('initialize', 'select'),
        Edge('select', 'supervise'),
        Edge('supervise', 'feedback', action_is('feedback')),
        Edge('supervise', 'approval', action_is('approval')),
        Edge('supervise', 'loop_tail', action_is('continue')),
        Edge('supervise', 'summary', action_is('finish')),
        Edge('feedback', 'loop_tail'), Edge('approval', 'send_decision'), Edge('send_decision', 'loop_tail'),
        Edge('loop_tail', 'select', id='supervision_back'),
    ])


@dataclass
class DemoResult:
    scenario: Scenario
    status: str
    summary: dict
    observations: dict


async def run_scenario(scenario: Scenario = 'approved', *, timeout: float = 10) -> DemoResult:
    """The outer driver simulates a human client; coordination stays in Core.

    Join/invoke can return an internal stable boundary before a Child finishes.
    Drive to terminal or a human Wait by inspecting public status, with a bound.
    """
    services = MockServices()
    app = AutoAgentApp(runtime_event_sink=services)
    try:
        submitted = await app.asubmit_invoke(parent_workflow(services),
                                           RunSpec(run_id=f'demo-{scenario}', scenario=scenario).model_dump())
        deadline = time.monotonic() + timeout
        external_message_sent = False
        handled_waits = set()
        while time.monotonic() < deadline:
            status = await app.astatus(submitted.ref)
            if status.status in {'completed', 'failed', 'cancelled'}:
                if status.status != 'completed':
                    raise AssertionError(f'Parent unexpectedly terminated: {status}')
                return DemoResult(scenario, status.status, status.output, services.snapshot())
            for wait in status.waits:
                if wait.kind != 'external' or wait.id in handled_waits:
                    continue
                request = ApprovalRequest.model_validate(wait.request)
                services.record('external.approval_requested', **request.model_dump())
                with services.lock:
                    assert request.operation_key not in services.effects, 'Tool ran before approval'
                if not external_message_sent:
                    receipt = await app.asignal(submitted.ref, 'instructions',
                                               {'text': 'Keep an audit trail of approved operations'},
                                               source_id='human', sequence=1)
                    retry = await app.asignal(submitted.ref, 'instructions',
                                             {'text': 'Keep an audit trail of approved operations'},
                                             source_id='human', sequence=1)
                    assert receipt == retry
                    still_waiting = await app.astatus(submitted.ref)
                    assert any(w.id == wait.id and w.kind == 'external' for w in still_waiting.waits)
                    services.record('external.signal_did_not_resume_approval', duplicate_receipt=True)
                    external_message_sent = True
                approved = not (scenario == 'rejected' and request.task_id == 'publish')
                with services.lock:
                    if approved:
                        services.approved.add(request.operation_key)
                response = ApprovalResponse(operation_key=request.operation_key, approved=approved,
                                            reason='Human approved' if approved else 'Human denied publishing')
                services.record('external.approval_decision', task_id=request.task_id, **response.model_dump())
                await app.asubmit_resume(submitted.ref, wait.id, response.model_dump())
                handled_waits.add(wait.id)
            await asyncio.sleep(0.005)
        raise TimeoutError(f'Scenario {scenario} did not settle within {timeout}s')
    finally:
        await app.aclose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=['all', 'approved', 'rejected', 'llm_failure', 'tool_failure'], default='all')
    parser.add_argument('--verbose', action='store_true', help='Include ordered protocol and mock-provider observations')
    args = parser.parse_args()
    scenarios = ['approved', 'rejected', 'llm_failure', 'tool_failure'] if args.scenario == 'all' else [args.scenario]
    for scenario in scenarios:
        result = asyncio.run(run_scenario(scenario))
        report = {'scenario': result.scenario, 'parent_status': result.status,
                  'summary': result.summary, 'tool_effects': len(result.observations['effects']),
                  'llm_max_parallel': result.observations['llm_max_parallel']}
        if args.verbose:
            report['observations'] = result.observations
        print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
