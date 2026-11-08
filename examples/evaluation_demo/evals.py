"""Run with: autoagent --project examples/evaluation_demo eval run normalization.

Reports and each Trial's real execution database remain under
.autoagent/evaluations. No model credentials or external services are needed.
"""

from autoagent.evaluation import CheckResult, EvalCase, EvalSuite, NodeExecuted, OutputEquals, StatusIs


async def normalize(context):
    return await context.invoke('normalize', context.case.input, entry_node_id='normalize')


normalization = EvalSuite(
    id='normalization',
    conditions={},
    cases=(EvalCase('trim-and-lower', {'text': '  HELLO  '}, {'text': 'hello'}),
           EvalCase('empty', {'text': '   '}, {'text': ''})),
    scenario=normalize,
    evaluators=(StatusIs(), OutputEquals()),
)


async def approve(context):
    waiting = await context.invoke('approval', context.case.input, entry_node_id='approve', until='boundary')
    wait = (await waiting.interactions())[0]
    request = await wait.request()
    context.check(CheckResult('approval-request', request == context.case.input,
        'Approval receives the original request.'))
    return await context.respond(waiting, context.case.expected, node_id='approve')


approval = EvalSuite(
    id='approval',
    conditions={},
    cases=(EvalCase('deny', {'text': 'Refund order 123'}, {'text': 'denied'}),
           EvalCase('allow', {'text': 'Refund order 456'}, {'text': 'approved'})),
    scenario=approve,
    evaluators=(StatusIs(), OutputEquals(), NodeExecuted('approve')),
)
