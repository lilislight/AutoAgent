"""A complete order business scenario: calculation, approval, routing and idempotent payment.

The ledger is a per-Trial external-service fixture. Every assessment enters
through the Workflow and observes business facts through ExecutionView.
"""

from contextlib import asynccontextmanager
from pathlib import Path
from typing_extensions import TypedDict

from autoagent import ConditionContext, Edge, InputMappingContext, Node, Recovery, UserEventContext, UserEventSpec, Wait, Workflow
from autoagent.evaluation import CheckResult, EvalCase, EvalSuite, OutputEquals, StatusIs
from autoagent.host import AutoAgentHost, HostSettings


class Order(TypedDict):
    key: str
    prices: list[int]


class Total(TypedDict):
    key: str
    total: int


class Decision(Total):
    approved: bool


class Receipt(Total):
    status: str


class Ledger:
    def __init__(self):
        self.payments = {}
        self.requests = []

    def pay(self, key, total):
        self.requests.append((key, total))
        if key in self.payments and self.payments[key] != total:
            raise ValueError('The same business key cannot charge a different amount.')
        self.payments.setdefault(key, total)


def calculate(value: Order) -> Total:
    return dict(key=value['key'], total=sum(value['prices']))


def approved(context: ConditionContext) -> bool:
    return context.output['approved']


def denied(context: ConditionContext) -> bool:
    return not context.output['approved']


def receipt_message(context: UserEventContext) -> UserEventSpec:
    return UserEventSpec(kind='order.receipt', payload=context.value)


def order_workflow(ledger=None, *, wrong_approval_total=False, normalize_receipt=False):
    ledger = ledger or Ledger()

    def approval_input(context: InputMappingContext) -> Total:
        total = next(iter(context.incoming.values()))
        return dict(key=total['key'], total=total['total'] + int(wrong_approval_total))

    def charge(value: Decision) -> Receipt:
        ledger.pay(value['key'], value['total'])
        return dict(key=value['key'], total=value['total'], status='paid')

    def refuse(value: Decision) -> Receipt:
        return dict(key=value['key'], total=value['total'], status='denied')

    def receipt_input(context: InputMappingContext) -> Receipt:
        receipt = next(iter(context.incoming.values()))
        return dict(receipt, total=sum(context.invocation_input['prices'])) if normalize_receipt else receipt

    def finish(value: Receipt) -> Receipt:
        return value

    return Workflow('order', nodes=[Node('calculate', calculate),
        Node('approval', Wait(Total, Decision), input_mapping=approval_input),
        Node('charge', charge, recovery_mode=Recovery('replay_safe')),
        Node('deny', refuse), Node('finish', finish, input_mapping=receipt_input, user_events=receipt_message)], edges=[
            Edge('calculate', 'approval', id='request-approval'),
            Edge('approval', 'charge', id='approved', condition=approved),
            Edge('approval', 'deny', id='denied', condition=denied),
            Edge('charge', 'finish', id='paid-receipt'), Edge('deny', 'finish', id='denied-receipt')])


@asynccontextmanager
async def resources(context):
    context.resources['ledger'] = Ledger()
    yield


def host_factory(environment):
    return AutoAgentHost(workflows=[order_workflow(environment.resources['ledger'])],
        settings=HostSettings(database_url=environment.database_url))


async def order_scenario(context):
    waiting = await context.invoke('order', context.case.input, entry_node_id='calculate', until='boundary')
    calculation = await waiting.node('calculate').one()
    approval = await waiting.node('approval').one()
    context.check(CheckResult('approval-amount', await calculation.output(pointer='/total') == await approval.input(pointer='/total'),
        'Approval must receive the calculated amount.'))
    before = await waiting.context(scope='session')
    context.check(CheckResult('no-payment-before-approval', not context.resources['ledger'].payments))
    request = await (await waiting.interactions(node_id='approval'))[0].request()
    response = dict(request, approved=context.case.expected['status'] == 'paid')
    final = await context.respond(waiting, response, node_id='approval')
    context.check(CheckResult('old-observation-stays-waiting', waiting.status == 'waiting'))
    context.check(CheckResult('history-remains-readable', await waiting.context(scope='session') == before))
    return final


async def business_rules(context):
    flow, expected = context.execution, context.case.expected
    charged = await flow.node('charge').was_executed()
    allowed = expected['status'] == 'paid'
    payments = context.resources['ledger'].payments
    path_correct = await flow.route('approved').was_taken() == allowed
    messages = await flow.messages(kind='order.receipt')
    public_receipt = await messages[0].body() if len(messages) == 1 else None
    return CheckResult('payment-business-rules', charged == allowed and path_correct and public_receipt == expected and
        payments == ({expected['key']: expected['total']} if allowed else {}),
        'Routing, public receipt and actual ledger state must agree with the approval.')


orders = EvalSuite('orders', cases=(
    EvalCase('approved', {'key': 'order-a', 'prices': [100, 250]}, {'key': 'order-a', 'total': 350, 'status': 'paid'}),
    EvalCase('denied', {'key': 'order-b', 'prices': [100, 250]}, {'key': 'order-b', 'total': 350, 'status': 'denied'})),
    scenario=order_scenario, evaluators=(StatusIs(), OutputEquals(), business_rules),
    fixture=resources, host_factory=host_factory, conditions={'ledger': 'isolated-idempotent-v1'})
