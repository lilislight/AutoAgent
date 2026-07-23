from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from autoagent.core.operators.contract import OperatorContract


class OperatorManifest(BaseModel):
    """Serializable compatibility contract for one Operator.

    The manifest deliberately does not hash Python source code. A developer must
    change ``version`` when implementation behavior changes in a way that affects
    persisted work. Input/output hashes protect the named-argument protocol, and
    crash recovery is owned by NodePolicy because the whole Node phase, not an
    individual OperatorExecution, is the unit that may be replayed.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    operator_id: str = Field(description="Stable Operator identity.")
    version: str | int = Field(description="Developer-managed compatibility version.")
    capability_id: str | None = Field(
        default=None,
        description="Capability implemented by this Operator, when any.",
    )
    input_schema_hash: str = Field(
        description="Hash of the canonical generated Operator input schema."
    )
    output_schema_hash: str = Field(
        description="Hash of the canonical generated Operator output schema."
    )
    manifest_hash: str = Field(
        description="Hash of all compatibility fields in this manifest."
    )

    @classmethod
    def from_contract(
        cls,
        *,
        operator_id: str,
        version: str | int,
        capability_id: str | None,
        contract: OperatorContract,
    ) -> OperatorManifest:
        """Build a stable manifest from the contract inferred at registration."""

        input_schema_hash = _hash_json(contract.input.describe())
        output_schema_hash = _hash_json(contract.output.describe())
        fields = {
            "operator_id": operator_id,
            "version": version,
            "capability_id": capability_id,
            "input_schema_hash": input_schema_hash,
            "output_schema_hash": output_schema_hash,
        }
        return cls(**fields, manifest_hash=_hash_json(fields))


def callable_operator_id(handler: Callable[..., Any]) -> str:
    """Return the stable virtual Operator id used for a directly bound callable."""

    module = getattr(handler, "__module__", handler.__class__.__module__)
    qualname = getattr(handler, "__qualname__", handler.__class__.__qualname__)
    return f"python:{module}:{qualname}"


def _hash_json(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
