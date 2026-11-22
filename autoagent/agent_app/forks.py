"""Agent conversation branches with shared history and independent execution."""

from autoagent.agent import Conversation
from autoagent.agent.context import restore
from autoagent.host.models import HostRunConflict


class ConversationForks:
    def __init__(self, application):
        self.application = application
        self.host = application.host

    async def storage(self, method, *args, **kwargs):
        return await self.host._perform(lambda: self.application.storage.forks(method, *args, **kwargs))

    async def info(self, session_id):
        return await self.storage("info", session_id)

    async def target(self, session_id, invocation_id):
        return await self.storage("target", session_id, invocation_id)

    async def _context(self, invocation_id, sequence):
        result = await self.host.load_execution_context(invocation_id, sequence=sequence,
            scope="session", pointer="/agent_conversation")
        if not result["available"]:
            raise HostRunConflict("fork_context_unavailable", "Context at this reply is no longer available.")
        conversation = restore(Conversation, result["value"])
        if conversation.pending is not None:
            raise HostRunConflict("fork_context_incomplete", "This reply still has unfinished Tool results.")
        return {"agent_conversation": conversation.model_dump(mode="json")}

    async def create(self, session_id, new_session_id, invocation_id):
        await self.application.workspace_session(session_id)
        if session_id == new_session_id:
            raise ValueError("A Fork requires a new conversation ID.")
        target = await self.target(session_id, invocation_id)
        await self._context(invocation_id, target["sequence"])
        record = await self.application.context.get_settings(session_id)
        await self.host._perform(lambda: self.application.storage.forks("create",
            session_id, new_session_id, target, record), control=True)
        saved = await self.application.context.get_settings(new_session_id)
        return saved

    async def initial_context(self, session_id):
        row = await self.info(session_id)
        if row and row["fork_source_session_id"]:
            return await self._context(row["fork_source_invocation_id"], row["fork_source_sequence"])
        return None

    async def image(self, session_id, image_id):
        from .images import UploadedImage
        # Media resolution runs on an Operator loop; storage marshals that call
        # to its own worker without entering the Host's Server-loop interface.
        row = await self.application.storage.forks("image", session_id, image_id)
        return UploadedImage(row["id"], row["mime_type"], bytes(row["data"])) if row else None
