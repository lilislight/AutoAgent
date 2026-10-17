"""Standalone transport shutdown, before ASGI lifespan teardown."""

import asyncio

import uvicorn


class _Server(uvicorn.Server):
    async def shutdown(self, sockets=None):
        # Uvicorn drains HTTP responses before sending lifespan.shutdown.
        # Close our unbounded responses first, while Host is still available.
        hubs = tuple(getattr(self.config.app.state, '_autoagent_trace_hubs', ()))
        await asyncio.gather(*(hub.close() for hub in hubs))
        await super().shutdown(sockets=sockets)
