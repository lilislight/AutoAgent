"""Strict callable and value contracts for durable Workflow execution."""

from __future__ import annotations

import inspect
import json
import math
import types
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from typing import (
    Annotated,
    Any,
    Literal,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    is_typeddict,
)
from uuid import UUID

try:
    from pydantic import TypeAdapter, ValidationError
except ImportError:  # pragma: no cover - a minimal Core installation
    TypeAdapter = None  # type: ignore[assignment]

    class ValidationError(Exception):
        pass


_SCALARS = {type(None), bool, int, float, str, UUID}
_CONTAINERS = {list, tuple, set, frozenset, dict, Mapping}
_STREAMS = {Iterator, AsyncIterator}


@dataclass(frozen=True, slots=True)
class ValueContract:
    """One explicit, safely reconstructable business-value contract."""

    annotation: object
    schema: str

    @classmethod
    def create(cls, annotation: object, *, location: str = "value") -> "ValueContract":
        validate_safe_annotation(annotation, location=location)
        schema: object = {"type": annotation_name(annotation)}
        if TypeAdapter is not None:
            try:
                schema = TypeAdapter(annotation).json_schema()
            except Exception:
                # The strict analyzer above remains authoritative. Some valid
                # runtime types, such as arbitrary Enum subclasses, do not
                # require Pydantic schema support to round-trip safely.
                pass
        return cls(
            annotation=annotation,
            schema=json.dumps(schema, sort_keys=True, separators=(",", ":")),
        )

    def validate(self, value: object) -> object:
        validated = value
        if TypeAdapter is not None:
            try:
                validated = TypeAdapter(self.annotation).validate_python(value)
            except ValidationError as error:
                raise TypeError(str(error)) from error
            except Exception:
                validated = value
        _validate_runtime_value(validated, self.annotation, path="value")
        return validated


@dataclass(frozen=True, slots=True)
class ParameterContract:
    name: str
    kind: inspect._ParameterKind
    value: ValueContract


@dataclass(frozen=True, slots=True)
class OperatorContract:
    signature: inspect.Signature
    parameters: tuple[ParameterContract, ...]
    output: ValueContract | None
    stream_chunk: ValueContract | None
    stream_kind: str | None

    @classmethod
    def from_callable(cls, handler: Callable[..., object]) -> "OperatorContract":
        signature = inspect.signature(handler)
        hints = _resolved_hints(handler)
        parameters: list[ParameterContract] = []
        for parameter in signature.parameters.values():
            if parameter.kind in {
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            }:
                raise TypeError("Operator variadic parameters are not supported.")
            annotation = hints.get(parameter.name, parameter.annotation)
            parameters.append(
                ParameterContract(
                    name=parameter.name,
                    kind=parameter.kind,
                    value=ValueContract.create(
                        annotation,
                        location=f"Operator parameter {parameter.name!r}",
                    ),
                )
            )
        output_annotation = hints.get("return", signature.return_annotation)
        stream = stream_annotation(output_annotation)
        if stream is None:
            output = ValueContract.create(
                output_annotation, location="Operator return annotation"
            )
            chunk = None
            stream_kind = None
        else:
            stream_kind, chunk_annotation = stream
            output = None
            chunk = ValueContract.create(
                chunk_annotation, location="Operator stream chunk annotation"
            )
        return cls(
            signature=signature,
            parameters=tuple(parameters),
            output=output,
            stream_chunk=chunk,
            stream_kind=stream_kind,
        )

    @property
    def input_schema(self) -> str:
        return json.dumps(
            [
                {
                    "name": parameter.name,
                    "kind": parameter.kind.name,
                    "schema": json.loads(parameter.value.schema),
                }
                for parameter in self.parameters
            ],
            sort_keys=True,
            separators=(",", ":"),
        )

    @property
    def declared_output_schema(self) -> str:
        contract = self.stream_chunk if self.stream_chunk is not None else self.output
        assert contract is not None
        return contract.schema

    def prepare_call(self, value: object) -> tuple[tuple[object, ...], dict[str, object]]:
        if not self.parameters:
            self.signature.bind()
            return (), {}
        if isinstance(value, Mapping):
            try:
                bound = self.signature.bind(**dict(value))
            except TypeError:
                pass
            else:
                return (), self._validate_bound(bound)
        try:
            bound = self.signature.bind(value)
        except TypeError as error:
            raise TypeError(
                "Operator input must be one positional value or a mapping whose "
                "keys match the callable parameters."
            ) from error
        validated = self._validate_bound(bound)
        return tuple(validated[name] for name in bound.arguments), {}

    def _validate_bound(self, bound: inspect.BoundArguments) -> dict[str, object]:
        contracts = {parameter.name: parameter.value for parameter in self.parameters}
        return {
            name: contracts[name].validate(value)
            for name, value in bound.arguments.items()
        }


def validate_safe_annotation(annotation: object, *, location: str) -> None:
    """Reject annotations that cannot be deterministically checkpointed."""

    if annotation is inspect.Signature.empty:
        raise TypeError(f"{location} must have an explicit type annotation.")
    # ``-> None`` is represented as ``None`` before get_type_hints() resolves
    # it, while ``None | T`` contains ``NoneType``. Treat both spellings as the
    # same durable scalar contract.
    if annotation is None:
        annotation = type(None)
    if annotation in {Any, object}:
        raise TypeError(f"{location} cannot use Any or object.")
    if isinstance(annotation, str):
        raise TypeError(f"{location} contains an unresolved forward reference.")
    if annotation in _CONTAINERS:
        raise TypeError(f"{location} cannot use a bare container type.")
    if annotation in _SCALARS:
        return

    origin = get_origin(annotation)
    arguments = get_args(annotation)
    if origin is Annotated:
        validate_safe_annotation(arguments[0], location=location)
        return
    if origin in {Union, types.UnionType}:
        if not arguments:
            raise TypeError(f"{location} has an empty Union.")
        for item in arguments:
            validate_safe_annotation(item, location=location)
        return
    if origin is Literal:
        if not arguments:
            raise TypeError(f"{location} has an empty Literal.")
        for item in arguments:
            _validate_runtime_value(item, type(item), path=location)
        return
    if origin in _CONTAINERS:
        _validate_container_annotation(origin, arguments, location)
        return
    if is_typeddict(annotation):
        _require_importable_type(annotation, location)
        for name, item in _resolved_hints(annotation).items():
            validate_safe_annotation(item, location=f"{location}.{name}")
        return
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        _require_importable_type(annotation, location)
        for member in annotation:
            _validate_runtime_supported(member.value, path=f"{location}.{member.name}")
        return
    if isinstance(annotation, type) and _is_pydantic_model(annotation):
        _require_importable_type(annotation, location)
        for name, item in _model_annotations(annotation).items():
            validate_safe_annotation(item, location=f"{location}.{name}")
        return
    if isinstance(annotation, type) and is_dataclass(annotation):
        _require_importable_type(annotation, location)
        hints = _resolved_hints(annotation)
        for field in fields(annotation):
            validate_safe_annotation(
                hints.get(field.name, field.type), location=f"{location}.{field.name}"
            )
        return
    raise TypeError(
        f"{location} uses unsupported durable type {annotation_name(annotation)!r}."
    )


def stream_annotation(annotation: object) -> tuple[str, object] | None:
    """Return the stream kind and Chunk annotation for an explicit stream."""

    origin = get_origin(annotation)
    arguments = get_args(annotation)
    if origin in _STREAMS:
        if len(arguments) != 1:
            raise TypeError("Stream return annotation must declare one Chunk type.")
        return ("async" if origin is AsyncIterator else "sync", arguments[0])
    # Generator and AsyncGenerator have additional send/return parameters, but
    # only yielded values are part of the Workflow contract.
    from collections.abc import AsyncGenerator, Generator

    if origin is Generator:
        if len(arguments) != 3 or arguments[1:] != (type(None), type(None)):
            raise TypeError(
                "Generator return annotation must be Generator[Chunk, None, None]."
            )
        return "sync", arguments[0]
    if origin is AsyncGenerator:
        if len(arguments) != 2 or arguments[1] is not type(None):
            raise TypeError(
                "AsyncGenerator return annotation must be AsyncGenerator[Chunk, None]."
            )
        return "async", arguments[0]
    return None


def annotation_name(annotation: object) -> str:
    if isinstance(annotation, type):
        return f"{annotation.__module__}:{annotation.__qualname__}"
    return str(annotation)


def _validate_container_annotation(
    origin: object, arguments: tuple[object, ...], location: str
) -> None:
    if not arguments:
        raise TypeError(f"{location} cannot use a bare container type.")
    if origin in {dict, Mapping}:
        if len(arguments) != 2 or arguments[0] is not str:
            raise TypeError(f"{location} mapping keys must be str.")
        validate_safe_annotation(arguments[1], location=f"{location} value")
        return
    if origin is tuple:
        if len(arguments) == 2 and arguments[1] is Ellipsis:
            validate_safe_annotation(arguments[0], location=f"{location} item")
            return
        for index, item in enumerate(arguments):
            validate_safe_annotation(item, location=f"{location}[{index}]")
        return
    if len(arguments) != 1:
        raise TypeError(f"{location} must declare exactly one item type.")
    validate_safe_annotation(arguments[0], location=f"{location} item")


def _validate_runtime_value(value: object, annotation: object, *, path: str) -> None:
    if annotation is None:
        annotation = type(None)
    origin = get_origin(annotation)
    arguments = get_args(annotation)
    if origin is Annotated:
        _validate_runtime_value(value, arguments[0], path=path)
        return
    if origin in {Union, types.UnionType}:
        errors: list[TypeError] = []
        for item in arguments:
            try:
                _validate_runtime_value(value, item, path=path)
                return
            except TypeError as error:
                errors.append(error)
        raise TypeError(f"{path} does not match any declared Union member.") from errors[-1]
    if origin is Literal:
        if value not in arguments:
            raise TypeError(f"{path} is not one of the declared Literal values.")
        _validate_runtime_supported(value, path=path)
        return
    if origin in _CONTAINERS:
        _validate_runtime_container(value, origin, arguments, path)
        return
    if is_typeddict(annotation):
        if not isinstance(value, Mapping):
            raise TypeError(f"{path} must be a mapping.")
        hints = _resolved_hints(annotation)
        required = getattr(annotation, "__required_keys__", frozenset(hints))
        missing = required.difference(value)
        unexpected = set(value).difference(hints)
        if missing or unexpected:
            raise TypeError(
                f"{path} has invalid keys; missing={sorted(missing)!r}, "
                f"unexpected={sorted(unexpected)!r}."
            )
        for name, item in value.items():
            _validate_runtime_value(item, hints[name], path=f"{path}.{name}")
        return
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        if not isinstance(value, annotation):
            raise TypeError(f"{path} must be {annotation_name(annotation)}.")
        _validate_runtime_supported(value, path=path)
        return
    if isinstance(annotation, type) and _is_pydantic_model(annotation):
        if not isinstance(value, annotation):
            raise TypeError(f"{path} must be {annotation_name(annotation)}.")
        for name, item in _model_annotations(annotation).items():
            _validate_runtime_value(getattr(value, name), item, path=f"{path}.{name}")
        return
    if isinstance(annotation, type) and is_dataclass(annotation):
        if not isinstance(value, annotation):
            raise TypeError(f"{path} must be {annotation_name(annotation)}.")
        hints = _resolved_hints(annotation)
        for field in fields(annotation):
            _validate_runtime_value(
                getattr(value, field.name),
                hints.get(field.name, field.type),
                path=f"{path}.{field.name}",
            )
        return
    if annotation in _SCALARS:
        if annotation is float:
            valid = isinstance(value, (int, float)) and not isinstance(value, bool)
        else:
            valid = isinstance(value, annotation)
        if not valid:
            raise TypeError(f"{path} must be {annotation_name(annotation)}.")
        _validate_runtime_supported(value, path=path)
        return
    # Compilation rejects every other annotation. Keep this guard so runtime
    # validation remains sound even if a ValueContract is constructed wrongly.
    raise TypeError(f"{path} has an unsupported runtime annotation.")


def _validate_runtime_container(
    value: object,
    origin: object,
    arguments: tuple[object, ...],
    path: str,
) -> None:
    if origin in {dict, Mapping}:
        if not isinstance(value, Mapping):
            raise TypeError(f"{path} must be a mapping.")
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} mapping key must be str.")
            _validate_runtime_value(item, arguments[1], path=f"{path}.{key}")
        return
    expected = {
        list: list,
        tuple: tuple,
        set: set,
        frozenset: frozenset,
    }[origin]
    if not isinstance(value, expected):
        raise TypeError(f"{path} must be {expected.__name__}.")
    if origin is tuple and not (len(arguments) == 2 and arguments[1] is Ellipsis):
        if len(value) != len(arguments):
            raise TypeError(f"{path} tuple length does not match its contract.")
        for index, (item, item_annotation) in enumerate(zip(value, arguments)):
            _validate_runtime_value(item, item_annotation, path=f"{path}[{index}]")
        return
    item_annotation = arguments[0]
    for index, item in enumerate(value):
        _validate_runtime_value(item, item_annotation, path=f"{path}[{index}]")


def _validate_runtime_supported(value: object, *, path: str) -> None:
    if value is None or isinstance(value, (bool, int, str, UUID)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise TypeError(f"{path} contains a non-finite float.")
        return
    if isinstance(value, Enum):
        _validate_runtime_supported(value.value, path=path)
        return
    if _is_pydantic_instance(value):
        _validate_runtime_supported(value.model_dump(mode="python"), path=path)
        return
    if is_dataclass(value) and not isinstance(value, type):
        for field in fields(value):
            _validate_runtime_supported(getattr(value, field.name), path=f"{path}.{field.name}")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} mapping key must be str.")
            _validate_runtime_supported(item, path=f"{path}.{key}")
        return
    if isinstance(value, (list, tuple, set, frozenset)):
        for index, item in enumerate(value):
            _validate_runtime_supported(item, path=f"{path}[{index}]")
        return
    raise TypeError(
        f"{path} contains unsupported durable value {type(value).__module__}:"
        f"{type(value).__qualname__}."
    )


def _resolved_hints(value: object) -> dict[str, object]:
    try:
        return get_type_hints(value, include_extras=True)
    except Exception as error:
        raise TypeError(f"Type annotations cannot be resolved: {error}") from error


def _require_importable_type(value: type[object], location: str) -> None:
    if "<locals>" in value.__qualname__ or value.__module__ == "__main__":
        raise TypeError(
            f"{location} type must be defined at module scope and importable."
        )


def _is_pydantic_model(value: type[object]) -> bool:
    return hasattr(value, "model_fields") and callable(getattr(value, "model_validate", None))


def _is_pydantic_instance(value: object) -> bool:
    return callable(getattr(value, "model_dump", None)) and callable(
        getattr(type(value), "model_validate", None)
    )


def _model_annotations(value: type[object]) -> dict[str, object]:
    return {
        name: field.annotation
        for name, field in value.model_fields.items()
    }
