from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from autoagent.operators.contract import OperatorContract


RecoveryMode = Literal["never", "replay_safe", "idempotent"]


class OperatorManifest(BaseModel):
    """Serializable compatibility and crash-recovery contract for one Operator.

    The manifest deliberately does not hash Python source code. A developer must
    change ``version`` when implementation behavior changes in a way that affects
    persisted work. Input/output hashes protect the named-argument protocol, and
    ``recovery_mode`` states whether an interrupted NodeExecution may be replayed.

    Recovery modes:
      - ``never``: process loss makes the owning Invocation terminal interrupted.
      - ``replay_safe``: repeating the whole NodeExecution has no unsafe effects.
      - ``idempotent``: the implementation declares repeated equivalent work
        safe and can use the persisted NodeExecution idempotency key. V1 keeps
        that key stable but does not inject it into an ordinary callable.
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
    recovery_mode: RecoveryMode = Field(
        description="Automatic recovery behavior after process loss."
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
        recovery_mode: RecoveryMode,
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
            "recovery_mode": recovery_mode,
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
