from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


RuntimeRetentionMode = Literal[
    "retain_all",
    "evict_durable_terminal",
    "lru_durable_terminal",
]


@dataclass(frozen=True)
class RuntimeRetentionPolicy:
    """Controls process-local runtime history after durable persistence.

    Active, waiting, and not-yet-durable Invocations are always retained.
    Durable events remain queryable through the configured backend after their
    in-memory aggregate is evicted.
    """

    mode: RuntimeRetentionMode = "retain_all"
    max_terminal_invocations: int = 128
    max_replay_checkpoints_per_invocation: int = 8

    def __post_init__(self) -> None:
        if self.mode not in {
            "retain_all",
            "evict_durable_terminal",
            "lru_durable_terminal",
        }:
            raise ValueError(f"Unknown Runtime retention mode: {self.mode}")
        if self.max_terminal_invocations < 0:
            raise ValueError("max_terminal_invocations cannot be negative.")
        if self.max_replay_checkpoints_per_invocation < 1:
            raise ValueError(
                "max_replay_checkpoints_per_invocation must be positive."
            )
        if (
            self.mode == "lru_durable_terminal"
            and self.max_terminal_invocations < 1
        ):
            raise ValueError(
                "lru_durable_terminal requires max_terminal_invocations."
            )
