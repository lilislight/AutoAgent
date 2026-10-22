"""Ordinary Nodes plus LLM_CALL: prepare → llm → tools → llm, or llm → finish."""

from autoagent import Edge, Map, Node, ResourcePolicy, RetryPolicy, TimeoutPolicy, Workflow
from autoagent.agent import LLM_CALL
from autoagent.host import workflow
from . import assistant as dialogue
from .tools import weather_tools


@workflow
def weather_workflow():
    return Workflow("weather-assistant", version="2", nodes=[
        Node("prepare", dialogue.prepare, output_binding=dialogue.prepared),
        Node("llm", LLM_CALL, input_mapping=dialogue.model_input,
             output_binding=dialogue.model_output, timeout=TimeoutPolicy(seconds=60),
             retry=RetryPolicy(max_attempts=1),
             resource=ResourcePolicy(max_executions_per_invocation=dialogue.MAX_MODEL_CALLS)),
        Node("tools", weather_tools().as_operator(id="weather.tools"),
             input_mapping=dialogue.tool_inputs, output_binding=dialogue.tool_outputs,
             map=Map(max_parallelism=4), timeout=TimeoutPolicy(seconds=10)),
        Node("finish", dialogue.finish, input_mapping=dialogue.final_input),
    ], edges=[
        Edge("prepare", "llm"),
        Edge("llm", "tools", condition=dialogue.needs_tools),
        Edge("tools", "llm"),
        Edge("llm", "finish", condition=dialogue.answered),
    ])
