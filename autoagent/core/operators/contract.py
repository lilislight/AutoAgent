"""Strict durable value contracts used at every Core boundary."""

from __future__ import annotations

import inspect
import json
import math
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from enum import Enum
from types import UnionType
from typing import (
    Annotated,
    Any,
    Literal,
    NotRequired,
    Required,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)
from typing_extensions import is_typeddict

from pydantic import BaseModel, TypeAdapter, ValidationError


def _is_model_type(value: object) -> bool:
    return isinstance(value, type) and issubclass(value, BaseModel)


def _type_name(value: object) -> str:
    if value is None or value is type(None):
        return "None"
    return f"{value.__module__}:{value.__qualname__}"  # type: ignore[attr-defined]


def _validate_annotation(annotation: object, location: str) -> object | None:
    if annotation in {None, type(None)}:
        return None
    if annotation is inspect.Signature.empty:
        raise TypeError(f"{location} must declare an explicit contract.")
    if not is_typeddict(annotation) and not _is_model_type(annotation):
        raise TypeError(
            f"{location} must be a TypedDict, Pydantic BaseModel, or None."
        )
    if "<locals>" in annotation.__qualname__:  # type: ignore[attr-defined]
        raise TypeError(f"{location} must be declared at module scope.")
    _validate_fields(annotation, location, set())
    try:
        json.dumps(TypeAdapter(annotation).json_schema(), sort_keys=True)
    except Exception as error:
        raise TypeError(f"{location} cannot produce a durable schema: {error}") from error
    return annotation


def _validate_fields(annotation: object, location: str, seen: set[object]) -> None:
    if annotation in seen:
        return
    seen.add(annotation)
    if annotation in {str, int, float, bool, type(None)}:
        return
    if annotation in {Any, object}:
        raise TypeError(f"{location} contains an unconstrained field type.")
    if is_typeddict(annotation):
        for name, field_type in get_type_hints(annotation, include_extras=True).items():
            _validate_fields(field_type, f"{location}.{name}", seen)
        return
    if _is_model_type(annotation):
        config = annotation.model_config  # type: ignore[union-attr]
        if bool(config.get("arbitrary_types_allowed", False)):
            raise TypeError(f"{location} cannot enable arbitrary_types_allowed.")
        if config.get("extra") != "forbid":
            raise TypeError(f"{location} must configure extra='forbid'.")
        for name, field_info in annotation.model_fields.items():  # type: ignore[union-attr]
            _validate_fields(field_info.annotation, f"{location}.{name}", seen)
        return
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return
    origin = get_origin(annotation)
    arguments = get_args(annotation)
    if origin is Annotated:
        _validate_fields(arguments[0], location, seen)
        return
    if origin is Literal:
        if not all(value is None or isinstance(value, (str, int, float, bool)) for value in arguments):
            raise TypeError(f"{location} contains a non-durable Literal value.")
        return
    if origin in {Union, UnionType}:
        for item in arguments:
            _validate_fields(item, location, seen)
        return
    if origin is list:
        if len(arguments) != 1:
            raise TypeError(f"{location} contains an incomplete list type.")
        _validate_fields(arguments[0], f"{location}[]", seen)
        return
    if origin is dict:
        if len(arguments) != 2 or arguments[0] is not str:
            raise TypeError(f"{location} dictionaries require str keys and a value type.")
        _validate_fields(arguments[1], f"{location}[]", seen)
        return
    if origin is tuple:
        values = arguments[:-1] if arguments and arguments[-1] is Ellipsis else arguments
        if not values:
            raise TypeError(f"{location} contains an incomplete tuple type.")
        for item in values:
            _validate_fields(item, f"{location}[]", seen)
        return
    # Required/NotRequired wrappers used by TypedDict expose one argument.
    if origin in {Required, NotRequired}:
        _validate_fields(arguments[0], location, seen)
        return
    raise TypeError(f"{location} contains unsupported field type {annotation!r}.")


@dataclass(frozen=True, slots=True)
class ValueContract:
    """One nominal, runtime-validated Operator value contract."""

    annotation: object | None
    name: str
    schema: str
    _adapter: TypeAdapter[object] | None = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_adapter",
            None if self.annotation is None else TypeAdapter(self.annotation),
        )

    @classmethod
    def create(cls, annotation: object, *, location: str) -> "ValueContract":
        normalized = _validate_annotation(annotation, location)
        if normalized is None:
            return cls(None, "None", "null")
        schema = TypeAdapter(normalized).json_schema()
        return cls(
            normalized,
            _type_name(normalized),
            json.dumps(schema, sort_keys=True, separators=(",", ":")),
        )

    def same_as(self, other: "ValueContract") -> bool:
        return self.annotation is other.annotation

    def validate(self, value: object) -> object:
        if self.annotation is None:
            if value is not None:
                raise TypeError("Value must be None.")
            return None
        assert self._adapter is not None
        try:
            validated = self._adapter.validate_python(value, strict=True)
        except ValidationError as error:
            raise TypeError(str(error)) from error
        if is_typeddict(self.annotation):
            return dict(validated)
        return validated

    def to_record(self, value: object) -> object:
        validated = self.validate(value)
        if validated is None:
            return None
        assert self._adapter is not None
        record = self._adapter.dump_python(
            validated,
            mode="json",
            round_trip=True,
            by_alias=True,
        )
        _validate_json_record(record)
        return record

    def restore(self, value: object) -> object:
        if self.annotation is None:
            if value is not None:
                raise TypeError("Value record must be None.")
            return None
        assert self._adapter is not None
        _validate_json_record(value)
        try:
            encoded = json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            validated = self._adapter.validate_json(encoded, strict=True)
        except (TypeError, ValueError, ValidationError) as error:
            raise TypeError(str(error)) from error
        canonical = self._adapter.dump_python(
            validated,
            mode="json",
            round_trip=True,
            by_alias=True,
        )
        if canonical != value:
            raise TypeError("Value record is not in canonical contract form.")
        if is_typeddict(self.annotation):
            return dict(validated)
        return validated


def _validate_json_record(value: object) -> None:
    """Reject Python-domain values that are not canonical JSON records."""

    if value is None or type(value) in {str, bool, int}:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise TypeError("Value record floats must be finite.")
        return
    if type(value) is list:
        for item in value:
            _validate_json_record(item)
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError("Value record mappings require string keys.")
            _validate_json_record(item)
        return
    raise TypeError("Value record must contain only canonical JSON values.")


@dataclass(frozen=True, slots=True)
class OperatorContract:
    input: ValueContract
    output: ValueContract | None
    stream_chunk: ValueContract | None = None

    @property
    def accepts_input(self) -> bool:
        """Whether the handler declares one business input argument."""

        return self.input.annotation is not None

    @classmethod
    def from_callable(cls, handler: object) -> "OperatorContract":
        signature = inspect.signature(handler)
        annotation_source = (
            handler
            if inspect.isfunction(handler) or inspect.ismethod(handler)
            else handler.__call__  # type: ignore[attr-defined]
        )
        try:
            hints = get_type_hints(annotation_source, include_extras=True)
        except Exception as error:
            raise TypeError(f"Operator annotations cannot be resolved: {error}") from error
        parameters = tuple(signature.parameters.values())
        if len(parameters) > 1:
            raise TypeError("Operator accepts zero or one business input object.")
        if parameters and parameters[0].kind in {
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        }:
            raise TypeError("Operator input must be one positional object.")
        input_annotation = (
            hints.get(parameters[0].name, parameters[0].annotation)
            if parameters
            else None
        )
        output_annotation = hints.get("return", signature.return_annotation)
        stream = _stream_chunk(output_annotation)
        return cls(
            input=ValueContract.create(input_annotation, location="Operator input"),
            output=(
                None
                if stream is not None
                else ValueContract.create(output_annotation, location="Operator output")
            ),
            stream_chunk=(
                ValueContract.create(stream, location="Operator stream chunk")
                if stream is not None
                else None
            ),
        )


def _stream_chunk(annotation: object) -> object | None:
    origin = get_origin(annotation)
    if origin not in {Iterator, AsyncIterator}:
        return None
    arguments = get_args(annotation)
    if len(arguments) != 1:
        raise TypeError("Stream Operator must declare Iterator[Chunk].")
    return arguments[0]
