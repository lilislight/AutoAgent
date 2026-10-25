"""Run with: autoagent --project examples/evaluation_demo eval run normalization.

Reports and each Trial's real execution database remain under
.autoagent/evaluations. No model credentials or external services are needed.
"""

from autoagent.evaluation import CheckResult, EvalCase, EvalSuite, EventCount, OutputEquals, StatusIs


async def normalize(context):
    return await context.invoke('normalize', context.case.input, entry_node_id='normalize')


normalization = EvalSuite(
    id='normalization',
    cases=(EvalCase('trim-and-lower', {'text': '  HELLO  '}, {'text': 'hello'}),
           EvalCase('empty', {'text': '   '}, {'text': ''})),
    scenario=normalize,
    evaluators=(StatusIs(), OutputEquals()),
)


async def approve(context):
    waiting = await context.invoke('approval', context.case.input, entry_node_id='approve', until='boundary')
    wait = waiting.member()['waits'][0]
    request = await waiting.wait_request(wait)
    context.check(CheckResult('approval-request', request == context.case.input,
        'Approval receives the original request.', (waiting.reference(),)))
    return await context.respond(waiting, context.case.expected, node_id='approve')


approval = EvalSuite(
    id='approval',
    cases=(EvalCase('deny', {'text': 'Refund order 123'}, {'text': 'denied'}),
           EvalCase('allow', {'text': 'Refund order 456'}, {'text': 'approved'})),
    scenario=approve,
    evaluators=(StatusIs(), OutputEquals(), EventCount('invocation.completed', minimum=1, maximum=1)),
)
