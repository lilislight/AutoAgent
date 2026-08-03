from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any

from autoagent.evaluation.evaluator import Evaluator
from autoagent.evaluation.result import EvalStepResult


class EvalCase(ABC):
    """Session-scoped controller supplied to one ``eval_*`` method."""

    @property
    @abstractmethod
    def session_id(self) -> str:
        """Stable Session key shared by every Step in this Case."""

    @abstractmethod
    async def invoke(
        self,
        input: dict[str, Any] | None = None,
        *,
        evaluators: Iterable[Evaluator] = (),
    ) -> EvalStepResult:
        """Run one Full-mode Invocation in the Case Session."""

    @abstractmethod
    async def resume(
        self,
        *,
        wait_key: str,
        response: Any,
        evaluators: Iterable[Evaluator] = (),
    ) -> EvalStepResult:
        """Resume the current waiting Invocation in this Case Session."""
