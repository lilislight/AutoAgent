import json
from contextlib import asynccontextmanager

from autoagent import Workflow
from autoagent.agent import (
    AgentAccess,
    AgentHostSettings,
    AgentModelSettings,
    AgentResources,
    AgentWorkflowOptions,
    LLMMessage,
    LLMRequest,
    LLMResponse,
    LLMStreamChunk,
    LLMToolCall,
    LLMToolResult,
    TextContent,
    agent_workflow,
)
from autoagent.host import HostSetup


class OfflineProvider:
    provider_id = "offline"

    async def ainvoke(self, request: LLMRequest) -> LLMResponse:
        model = request.model or "offline-notes"
        for item in reversed(request.input):
            if isinstance(item, LLMToolResult) and item.call_id == "record-note":
                text = "Release note declined." if item.is_error else "Release note recorded."
                return LLMResponse(
                    model=model,
                    finish_reason="stop",
                    output=(LLMMessage(role="assistant", content=(TextContent(text=text),)),),
                )
            if isinstance(item, LLMMessage) and item.role == "user":
                break
        return LLMResponse(
            model=model,
            finish_reason="tool_calls",
            output=(
                LLMToolCall(
                    name="record_release_note",
                    call_id="record-note",
                    arguments=json.dumps({"text": "Add Workflow authoring guidance."}),
                ),
            ),
        )

    async def astream(self, request: LLMRequest):
        yield LLMStreamChunk(type="completed", response=await self.ainvoke(request))


@asynccontextmanager
async def resources(setup: HostSetup):
    setup.providers.register("offline", OfflineProvider(), version="1")
    yield


def build_workflow(notes: list[str]) -> Workflow:
    def record_release_note(text: str) -> str:
        """Record a release note in this application's in-memory fixture."""
        notes.append(text)
        return text

    return agent_workflow(
        options=AgentWorkflowOptions(
            workflow_id="release-note-assistant",
            instructions="Record the requested release note after tool approval.",
            max_rounds=3,
            runtime=AgentHostSettings(
                model=AgentModelSettings(provider="offline", model="offline-notes"),
                access=AgentAccess(rules={"record_release_note": "ask"}),
            ),
        ),
        resources=AgentResources(tools=[record_release_note]),
    )


def release_note_workflow() -> Workflow:
    return build_workflow([])
