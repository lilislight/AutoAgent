"""Completed-call provenance retained independently of later execution settings."""

from ..models import LLMResponse
from .history import HistoryRoute


_KEY = "_autoagent_history_origin"


def stamp_response(response: LLMResponse, route: HistoryRoute | None, snapshot_id: str) -> LLMResponse:
    return response.model_copy(update={"provider_metadata": {
        **response.provider_metadata,
        _KEY: {"route": route.model_dump(mode="json") if route else None, "snapshot_id": snapshot_id},
    }})


def restore_response(response: LLMResponse) -> tuple[LLMResponse, HistoryRoute | None, str | None]:
    metadata = dict(response.provider_metadata)
    stamp = metadata.pop(_KEY, None)
    clean = response.model_copy(update={"provider_metadata": metadata})
    if stamp is None:
        return clean, None, None
    route = HistoryRoute.model_validate(stamp["route"]) if stamp.get("route") else None
    return clean, route, stamp["snapshot_id"]
