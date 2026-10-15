"""Compile ordinary typed Python functions into Tool metadata and validators."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field
import inspect
from typing import Annotated, Any, Literal, get_args, get_origin, get_type_hints

from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, create_model
from typing_extensions import is_typeddict

from ..models import LLMToolDefinition
from .context import ToolContext


TOOL_DEFINITION_ATTRIBUTE = "__autoagent_tool_definition__"


def _require_annotation(annotation: object, seen: set[int] | None = None) -> None:
    if annotation is inspect.Signature.empty or annotation is Any or annotation is object:
        raise TypeError("Tool parameters and return value require concrete type annotations.")
    if annotation is JsonValue:
        return
    seen = set() if seen is None else seen
    if id(annotation) in seen:
        return
    seen.add(id(annotation))
    origin = get_origin(annotation)
    if annotation in (dict, list, tuple, set) or (origin in (dict, list, tuple, set) and not get_args(annotation)):
        raise TypeError("Tool containers require explicit element types.")
    if origin is Annotated:
        _require_annotation(get_args(annotation)[0], seen)
    elif origin is not Literal:
        if origin is dict and get_args(annotation)[0] is not str:
            raise TypeError("Tool dictionaries must have string keys.")
        for item in get_args(annotation):
            if item is not Ellipsis:
                _require_annotation(item, seen)
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        for info in annotation.model_fields.values():
            _require_annotation(info.annotation, seen)
    elif is_typeddict(annotation):
        for item in get_type_hints(annotation, include_extras=True).values():
            _require_annotation(item, seen)


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    """Runtime metadata; only llm_definition() goes into an LLMRequest."""

    _llm: LLMToolDefinition = field(repr=False)
    _input_model: type[BaseModel] = field(repr=False, compare=False)
    _output_adapter: TypeAdapter = field(repr=False, compare=False)
    _bindings: tuple[tuple[str, str], ...] = field(repr=False)
    is_async: bool
    context_parameter: str | None = None

    @property
    def name(self) -> str:
        return self._llm.name

    @property
    def description(self) -> str | None:
        return self._llm.description

    @property
    def output_schema(self) -> dict:
        return deepcopy(self._output_adapter.json_schema())

    def llm_definition(self) -> LLMToolDefinition:
        """Detach mutable schema dictionaries from executable Tool metadata."""
        return LLMToolDefinition.model_validate(self._llm.model_dump(mode="python"))

    @classmethod
    def from_callable(
        cls, handler: Callable[..., object], *, name: str | None = None,
        description: str | None = None, strict: bool | None = None,
    ) -> ToolDefinition:
        if not callable(handler):
            raise TypeError("Tool handler must be callable.")
        source = handler if inspect.isfunction(handler) or inspect.ismethod(handler) else handler.__call__
        if inspect.isgeneratorfunction(source) or inspect.isasyncgenfunction(source):
            raise TypeError("Tools return one observation; generator handlers are unsupported.")
        signature = inspect.signature(handler)
        try:
            hints = get_type_hints(source, include_extras=True)
        except Exception as error:
            raise TypeError(f"Tool annotations cannot be resolved: {error}") from error
        fields = {}
        bindings = []
        context_parameter = None
        for index, parameter in enumerate(signature.parameters.values()):
            if parameter.kind not in {
                inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY,
            }:
                raise TypeError("Tool parameters must be named; positional-only and variadic parameters are unsupported.")
            annotation = hints.get(parameter.name, parameter.annotation)
            base_annotation = get_args(annotation)[0] if get_origin(annotation) is Annotated else annotation
            if base_annotation is ToolContext:
                if context_parameter is not None or parameter.default is not inspect.Signature.empty:
                    raise TypeError("Tools accept one required ToolContext parameter.")
                context_parameter = parameter.name
                continue
            _require_annotation(annotation)
            if parameter.default is not inspect.Signature.empty:
                try:
                    TypeAdapter(annotation).validate_python(parameter.default, strict=True)
                except Exception as error:
                    raise TypeError(f"Tool parameter {parameter.name!r} has an invalid default.") from error
            default = ... if parameter.default is inspect.Signature.empty else parameter.default
            # Internal field names allow Python parameters beginning with '_'.
            # JSON properties and handler kwargs always use the real names.
            internal = f"argument_{index}"
            fields[internal] = (annotation, Field(default=default, alias=parameter.name))
            bindings.append((internal, parameter.name))
        output_annotation = hints.get("return", signature.return_annotation)
        _require_annotation(output_annotation)
        input_model = create_model(
            "ToolArguments", __module__=getattr(source, "__module__", __name__),
            __config__=ConfigDict(
                extra="forbid", strict=True, validate_default=True, allow_inf_nan=False,
            ), **fields,
        )
        output_adapter = TypeAdapter(output_annotation)
        try:
            parameters = input_model.model_json_schema(by_alias=True)
            output_adapter.json_schema()
            definition = LLMToolDefinition(
                name=name if name is not None else getattr(handler, "__name__", ""),
                description=description if description is not None else inspect.getdoc(handler),
                parameters=parameters, strict=strict,
            )
        except Exception as error:
            raise TypeError(f"Tool cannot produce a serializable definition: {error}") from error
        return cls(definition, input_model, output_adapter, tuple(bindings), inspect.iscoroutinefunction(source), context_parameter)


def get_tool_definition(handler: Callable[..., object]) -> ToolDefinition:
    definition = getattr(handler, TOOL_DEFINITION_ATTRIBUTE, None)
    if not isinstance(definition, ToolDefinition):
        raise TypeError("Tool functions must be decorated with @tool.")
    return definition


__all__ = ["ToolDefinition", "get_tool_definition"]
