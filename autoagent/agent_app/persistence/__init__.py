"""Agent product storage; the Host only executes generic storage extensions."""

from .context import ApplicationContextStorage
from .conversation import ConversationStorage
from .forks import ForkStorage
from .queue import ApplicationQueueStorage


class AgentApplicationStorage:
    def __init__(self, persistence):
        self.persistence = persistence

    def _sink(self):
        return self.persistence() if callable(self.persistence) else self.persistence

    async def context(self, method, *args, **kwargs):
        return await self._sink().extension_operation(ApplicationContextStorage, method, *args, **kwargs)

    async def queue(self, method, *args, **kwargs):
        return await self._sink().extension_operation(ApplicationQueueStorage, method, *args, **kwargs)

    async def conversation(self, method, *args, **kwargs):
        return await self._sink().extension_operation(ConversationStorage, method, *args, **kwargs)

    async def forks(self, method, *args, **kwargs):
        return await self._sink().extension_operation(ForkStorage, method, *args, **kwargs)


__all__ = ["AgentApplicationStorage"]
