"""Application-owned SDK callbacks, configured before negotiation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mcp.client.session import (
        ElicitationFnT, ListRootsFnT, LoggingFnT, MessageHandlerFnT, SamplingFnT,
    )
    from mcp_types import SamplingCapability, LoggingLevel


@dataclass(frozen=True, slots=True)
class MCPCallbacks:
    """Delegate callback contracts and capability negotiation to the SDK.

    Legacy reverse requests need mode='legacy'. Modern input-required results
    are handled by the SDK using the same sampling and elicitation callbacks.
    Functions are runtime resources, never checkpoint data.
    """

    sampling_callback: SamplingFnT | None = field(default=None, repr=False)
    sampling_capabilities: SamplingCapability | None = field(default=None, repr=False)
    elicitation_callback: ElicitationFnT | None = field(default=None, repr=False)
    list_roots_callback: ListRootsFnT | None = field(default=None, repr=False)
    logging_callback: LoggingFnT | None = field(default=None, repr=False)
    message_handler: MessageHandlerFnT | None = field(default=None, repr=False)
    log_level: LoggingLevel | None = None

    def sdk_options(self) -> dict:
        return {
            name: getattr(self, name) for name in self.__dataclass_fields__
            if getattr(self, name) is not None
        }


__all__ = ["MCPCallbacks"]
