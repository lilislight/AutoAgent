"""Offline token queries and Node/Invocation/Session/Workflow/Project limits.

Run: .venv/bin/python -m examples.agent_usage_guide.token_usage
The mock API reports 14 tokens per request. Two calls exhaust a Session budget.
"""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

from autoagent import (
    InputMappingContext, Node, ResourcePolicy, UsagePolicy, ValueContract,
    Workflow, WorkflowResourcePolicy,
)
from autoagent.agent import (
    LLM_CALL, LLMRequest, LocalTokenPricing, TokenPrice, OpenAIResponsesProvider, ProjectUsagePolicy,
    TimeRange, UsageWindow, WindowUsagePolicy, llm_call_operator,
)
from autoagent.host import AutoAgentHost, HostSettings
from .support import connection, offline_http, request, show


def model_input(context: InputMappingContext) -> LLMRequest:
    return ValueContract.create(LLMRequest, location="input").restore(context.invocation_input)


async def main():
    @asynccontextmanager
    async def resources(setup):
        # Disabling optional recording cannot disable required budget metering.
        async with OpenAIResponsesProvider(connection(), record_calls=False) as provider:
            setup.register_operator(llm_call_operator(provider), capability_id=LLM_CALL.id, default=True)
            yield

    workflow = Workflow("token-guide", resource=WorkflowResourcePolicy(
        invocation=UsagePolicy(max_total_tokens=20), session=UsagePolicy(max_total_tokens=28, max_cost_usd="0.000052")),
        nodes=[Node("model", LLM_CALL, input_mapping=model_input,
                    resource=ResourcePolicy(usage=UsagePolicy(max_total_tokens=20)))])
    policy = ProjectUsagePolicy(
        project=WindowUsagePolicy(UsageWindow("month", "Asia/Hong_Kong"), UsagePolicy(max_total_tokens=100)),
        workflows={workflow.id: WindowUsagePolicy(UsageWindow("day", "Asia/Hong_Kong"), UsagePolicy(max_total_tokens=50))},
    )
    # Real catalog models resolve automatically when LiteLLM is installed.
    # This simulated model uses explicit USD rates per million tokens.
    pricing = LocalTokenPricing(overrides={"demo-model": TokenPrice("1", "3")})
    with TemporaryDirectory() as directory:
        settings = HostSettings(database_url="sqlite+aiosqlite:///" + str(Path(directory) / "runtime.sqlite"))
        async with offline_http() as api:
            async with AutoAgentHost(workflows=[workflow], settings=settings, lifespan=resources,
                                     usage_policy=policy, pricing=pricing, root=directory) as host:
                results = []
                for _ in range(3):
                    result = await host.join(await host.execute(workflow.id, request().model_dump(mode="json"),
                                                               session_id="example-session"))
                    results.append(result)
                assert [result.status for result in results] == ["completed", "completed", "failed"]
                assert "ResourceBudgetExceeded" in str(results[-1].error)
                assert len(api.requests) == 2
                first = results[0]
                window = TimeRange.day(timezone="Asia/Hong_Kong")
                for name, filters, expected in (
                    ("Node / Invocation", dict(invocation_id=first.invocation_id, node_id="model"), 14),
                    ("Invocation", dict(invocation_id=first.invocation_id), 14),
                    ("Session", dict(session_id="example-session"), 28),
                    ("Workflow / day", dict(workflow_id=workflow.id, start=window.start, end=window.end), 28),
                    ("Project / day", dict(start=window.start, end=window.end), 28),
                ):
                    summary = await host.token_usage(**filters)
                    assert summary.total_tokens == expected
                    assert summary.cost_usd == ("0.000026" if expected == 14 else "0.000052")
                    show(name, summary)
        # Reopening reads the same facts, without importing/executing a Workflow.
        async with AutoAgentHost(settings=settings, root=directory, read_only=True) as host:
            assert (await host.token_usage()).total_tokens == 28
            assert (await host.token_usage()).cost_usd == "0.000052"


if __name__ == "__main__":
    asyncio.run(main())
