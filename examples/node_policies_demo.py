"""Retry, per-Node loop limits, Host resolvers and explicit error branches.

Run: .venv/bin/python -m examples.node_policies_demo
All operations are local; no model API credentials are needed.
"""

import asyncio
import json
import tempfile
from contextlib import asynccontextmanager, closing
from pathlib import Path

from typing_extensions import TypedDict

from autoagent import (
    AutoAgentApp, BackoffPolicy, Capability, ConditionContext, Edge,
    InputMappingContext, Node, Operator, ResourcePolicy, RetryPolicy, Workflow,
)
from autoagent.host import AutoAgentHost, HostSettings


class Text(TypedDict):
    text: str


def identity(value: Text) -> Text:
    return value


def prepare_request(context: InputMappingContext) -> Text:
    return {"text": "prepared:" + context.invocation_input["text"]}


def backup_input(context: InputMappingContext) -> Text:
    failure = context.incoming["primary_failed"]
    print("Error Edge input:", json.dumps(failure, ensure_ascii=False))
    # The original value is from before prepare_request(), not its result.
    return failure["input"]["invocation_input"]


def again(context: ConditionContext) -> bool:
    return True


def stop(context: ConditionContext) -> bool:
    return False


async def main():
    contract = Operator(identity).contract
    primary = Capability("text.primary", contract)
    backup = Capability("text.backup", contract)
    calls = []

    @asynccontextmanager
    async def resources(setup):
        # A real integration can create Providers here. The selected Operator
        # constructs its LLMRequest after the resolver has selected it.
        backend = setup.environment["DEMO_BACKEND"]
        async def unavailable(value: Text) -> Text:
            calls.append((backend, value["text"]))
            raise ConnectionError("demonstration service unavailable")
        selected = setup.register_operator(Operator(unavailable, id=backend), capability_id=primary.id)
        setup.register_operator(Operator(identity, id="other"), capability_id=primary.id, default=True)
        setup.register_operator(Operator(identity, id="backup"), capability_id=backup.id, default=True)
        async def resolve(capability, value):
            return selected
        setup.register_capability_resolver(primary.id, resolve)
        yield

    workflow = Workflow("explicit-backup", nodes=[
        Node("primary", primary, input_mapping=prepare_request,
            retry=RetryPolicy(max_attempts=1, backoff=BackoffPolicy(initial_seconds=.01, max_seconds=.1)),
            resource=ResourcePolicy(max_executions_per_invocation=1)),
        Node("backup", backup, input_mapping=backup_input),
    ], edges=[Edge("primary", "backup", on="error", id="primary_failed")])
    with tempfile.TemporaryDirectory() as directory:
        settings = HostSettings(database_url="sqlite+aiosqlite:///" + str(Path(directory) / "runtime.db"))
        async with AutoAgentHost(workflows=[workflow], settings=settings, lifespan=resources,
                                 environment={"DEMO_BACKEND": "configured-service"}) as host:
            result = await host.join(await host.execute(workflow.id, {"text": "original input"}))
            assert result.status == "completed", result.error
            assert result.output == {"text": "original input"}
            assert len(calls) == 2  # Initial call plus one Retry; one primary Node start.
            print("Host result:", result.output)
            print("Primary Calls:", calls)

    loop = Workflow("bounded-loop", nodes=[Node("entry", identity),
        Node("repeat", identity, resource=ResourcePolicy(max_executions_per_invocation=3)),
        Node("exit", identity)], edges=[Edge("entry", "repeat"),
        Edge("repeat", "repeat", again, id="back"), Edge("repeat", "exit", stop, id="exit")])
    with closing(AutoAgentApp()) as app:
        result = await app.ainvoke(loop, {"text": "loop input"})
        assert result.error.type == "NodeExecutionLimitExceeded"
        print("Loop guard:", result.error.message)


if __name__ == "__main__":
    asyncio.run(main())
