"""Application extensions and per-Node policies for the general Agent.

Run .venv/bin/python -m examples.agent_workflow.configuration to check the graph
without model credentials. Register build_workflow() with AutoAgentHost to run
it with your Providers, Tools and Session settings. Built-in Providers record
and enforce the configured usage policies through the existing Host ledger.
"""

from autoagent.agent import (
    AgentNodePolicy, AgentWorkflowOptions, AgentWorkflowPolicies,
    ContextPolicy, CompactionInput, CompactionResult, LLMHistoryCompactor, agent_workflow,
)
from autoagent.core import (
    ResourcePolicy, RetryPolicy, TimeoutPolicy,
    UsagePolicy, Workflow, WorkflowCompiler, WorkflowResourcePolicy,
)


OPTIONS = AgentWorkflowOptions(
    instructions="You are a helpful assistant. Use tools when needed.",
    llm_options={"streaming": True, "max_output_tokens": 2048},
    max_rounds=10,
)


async def application_compaction(value: CompactionInput) -> CompactionResult:
    """A custom method may trim observations before falling back to LLM summarization."""
    return await LLMHistoryCompactor(max_output_tokens=512)(value)


def build_workflow() -> Workflow:
    return agent_workflow(
        options=OPTIONS,
        context_policy=ContextPolicy(id="application.context", version="1", context_window=32_768,
            compact_at_ratio=0.8, target_ratio=0.5, compact=application_compaction),
        policies=AgentWorkflowPolicies(
            resource=WorkflowResourcePolicy(
                invocation=UsagePolicy(max_total_tokens=100_000),
                session=UsagePolicy(max_total_tokens=1_000_000)),
            nodes={
                "llm_call": AgentNodePolicy(
                    timeout=TimeoutPolicy(120),
                    retry=RetryPolicy(max_attempts=1, retry_on=("LLMProviderError",)),
                    resource=ResourcePolicy(usage=UsagePolicy(max_total_tokens=80_000))),
                "compact_context": AgentNodePolicy(
                    timeout=TimeoutPolicy(60),
                    resource=ResourcePolicy(usage=UsagePolicy(max_total_tokens=20_000))),
                # Both Maps retain never Recovery and no automatic Retry.
                "tool_execute": AgentNodePolicy(timeout=TimeoutPolicy(30)),
                "tool_execute_approved": AgentNodePolicy(timeout=TimeoutPolicy(30)),
            },
        ),
    )


if __name__ == "__main__":
    graph = WorkflowCompiler().compile_or_raise(build_workflow())
    print(f"Agent graph: {len(graph.nodes)} Nodes, {len(graph.edges)} Edges")
    print("Workflow Revision:", graph.workflow_revision_id)
