from __future__ import annotations

import inspect
import types
import warnings
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Annotated, Any, Literal, Union, get_args, get_origin, is_typeddict
from uuid import UUID

from pydantic import BaseModel, ConfigDict, TypeAdapter, create_model

from autoagent.core.operators.streaming import StreamingResult


class OperatorContractWarning(UserWarning):
    """A callable contract is usable but cannot be verified completely."""


@dataclass(frozen=True)
class ContractIssue:
    """One structural error or non-blocking contract warning."""

    message: str
    severity: Literal["error", "warning"]


_MISSING = object()


@dataclass(frozen=True)
class ParameterContract:
    """Canonical description of one named Operator input parameter."""

    name: str
    annotation: Any
    required: bool
    default: Any = field(default=_MISSING, repr=False)
    keyword_only: bool = False


@dataclass(frozen=True)
class SchemaContract:
    """Canonical runtime validator plus a generated JSON Schema descriptor.

    ``arguments`` contracts validate the Mapping produced by input mapping and
    bind its keys to a Python handler's named parameters. ``value`` contracts
    validate one Operator or aggregated node output. Workflow authors never
    construct these contracts: registration and compilation derive them from
    Python callable annotations.

    The generated JSON Schema is portable metadata for tool declarations, UI
    inspection, adapters, and diagnostics. Runtime retains private Pydantic
    adapters only to validate live values and restore persisted JSON through
    the exact Workflow revision's contract.
    """

    kind: Literal["arguments", "value"]
    parameters: tuple[ParameterContract, ...] = ()
    annotation: Any = Any
    allow_extra: bool = False
    extra_annotation: Any = Any
    inspectable: bool = True
    portable: bool = True
    restoration_mode: Literal["typed", "json"] = "typed"
    _json_schema: Mapping[str, Any] = field(
        default_factory=lambda: MappingProxyType({}),
        repr=False,
        compare=False,
    )
    _signature: inspect.Signature | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    _parameter_adapters: Mapping[str, TypeAdapter[Any]] = field(
        default_factory=lambda: MappingProxyType({}),
        repr=False,
        compare=False,
    )
    _extra_adapter: TypeAdapter[Any] | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    _value_adapter: TypeAdapter[Any] | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    @property
    def json_schema(self) -> dict[str, Any]:
        """Return a detached JSON Schema so callers cannot mutate the contract."""

        return deepcopy(dict(self._json_schema))

    def describe(self) -> dict[str, Any]:
        """Return the serializable contract view exposed by Workflow IR tooling."""

        return {
            "kind": self.kind,
            "json_schema": self.json_schema,
            "portable": self.portable,
            "known": self.known,
            "restoration_mode": self.restoration_mode,
        }

    @property
    def known(self) -> bool:
        """Whether this contract has enough Python information to validate."""

        if self.kind == "arguments":
            return self.inspectable
        return not _is_missing(self.annotation)

    def validate(self, value: Any) -> Any:
        """Validate a named argument Mapping or one output value strictly."""

        if self.kind == "arguments":
            return self._validate_arguments(value)
        if self._value_adapter is not None:
            validated = self._value_adapter.validate_python(value, strict=True)
            return (
                _normalize_dynamic_json(validated)
                if self.restoration_mode == "json"
                else _ensure_serializable(validated)
            )
        return value

    def restore(self, value: Any) -> Any:
        """Materialize persisted JSON using this executable contract."""

        if self.kind == "arguments":
            return self._restore_arguments(value)
        if self.restoration_mode == "json" or self._value_adapter is None:
            return _normalize_dynamic_json(value)
        return _ensure_serializable(
            self._value_adapter.validate_python(value, strict=False)
        )

    def _validate_arguments(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise TypeError(
                "Operator input must be a mapping whose keys match handler parameters."
            )
        arguments = dict(value)
        if not self.inspectable or self._signature is None:
            return arguments

        bound = self._signature.bind(**arguments)
        validated = dict(arguments)
        parameters_by_name = {
            parameter.name: parameter for parameter in self.parameters
        }
        for name, argument in bound.arguments.items():
            adapter = self._parameter_adapters.get(name)
            if adapter is not None:
                validated_value = adapter.validate_python(argument, strict=True)
                parameter = parameters_by_name[name]
                validated[name] = (
                    _normalize_dynamic_json(validated_value)
                    if _is_dynamic_json(parameter.annotation)
                    else validated_value
                )
                continue
            if name in parameters_by_name or self._extra_adapter is None:
                continue
            for extra_name, extra_value in argument.items():
                validated[extra_name] = self._extra_adapter.validate_python(
                    extra_value,
                    strict=True,
                )
                if _is_dynamic_json(self.extra_annotation):
                    validated[extra_name] = _normalize_dynamic_json(
                        validated[extra_name]
                    )
        return _ensure_serializable(validated)

    def _restore_arguments(self, value: Any) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise TypeError("Operator input must be a mapping.")
        arguments = dict(value)
        if not self.inspectable or self._signature is None:
            return arguments

        bound = self._signature.bind(**arguments)
        restored = dict(arguments)
        parameters_by_name = {
            parameter.name: parameter for parameter in self.parameters
        }
        for name, argument in bound.arguments.items():
            adapter = self._parameter_adapters.get(name)
            if adapter is not None:
                restored_value = adapter.validate_python(argument, strict=False)
                parameter = parameters_by_name[name]
                restored[name] = (
                    _normalize_dynamic_json(restored_value)
                    if _is_dynamic_json(parameter.annotation)
                    else restored_value
                )
                continue
            if name in parameters_by_name or self._extra_adapter is None:
                continue
            for extra_name, extra_value in argument.items():
                restored[extra_name] = self._extra_adapter.validate_python(
                    extra_value,
                    strict=False,
                )
                if _is_dynamic_json(self.extra_annotation):
                    restored[extra_name] = _normalize_dynamic_json(
                        restored[extra_name]
                    )
        return _ensure_serializable(restored)


@dataclass(frozen=True)
class OperatorContract:
    """Input and output contracts derived from one Python Operator handler."""

    input: SchemaContract
    output: SchemaContract


def callable_contract(
    handler: Any,
) -> tuple[OperatorContract, tuple[ContractIssue, ...]]:
    """Derive an Operator contract using AutoAgent's named-argument protocol.

    Node input is always a Mapping whose keys bind to named parameters.
    Positional-only and variadic positional parameters cannot be represented by
    that protocol and are rejected. Every parameter and return value must be
    annotated. Explicit ``Any`` is a dynamic JSON contract; omission is an error.
    """

    signature = _signature(handler)
    if signature is None:
        return (
            OperatorContract(
                input=_arguments_contract(None, (), allow_extra=True),
                output=value_contract(Any),
            ),
            (
                ContractIssue(
                    "Callable signature cannot be inspected; Workflow callables "
                    "must declare serializable input and output contracts.",
                    "error",
                ),
            ),
        )

    issues: list[ContractIssue] = []
    parameters: list[ParameterContract] = []
    allow_extra = False
    extra_annotation: Any = Any
    for parameter in signature.parameters.values():
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            issues.append(
                ContractIssue(
                    f"Variadic positional parameter '*{parameter.name}' is not "
                    "supported; use one explicit collection parameter instead.",
                    "error",
                )
            )
            continue
        if parameter.kind is inspect.Parameter.POSITIONAL_ONLY:
            issues.append(
                ContractIssue(
                    f"Positional-only parameter '{parameter.name}' cannot be bound "
                    "from AutoAgent's named input mapping.",
                    "error",
                )
            )
            continue
        if parameter.kind is inspect.Parameter.VAR_KEYWORD:
            allow_extra = True
            extra_annotation = _normalize_annotation(parameter.annotation)
            if _is_missing(extra_annotation):
                extra_annotation = Any
                issues.append(
                    ContractIssue(
                        f"Variadic keyword parameter '**{parameter.name}' has no "
                        "type annotation.",
                        "error",
                    )
                )
            elif not _annotation_is_serializable(extra_annotation):
                issues.append(
                    ContractIssue(
                        f"Variadic keyword parameter '**{parameter.name}' uses a "
                        "non-serializable Workflow type: "
                        f"{_annotation_name(extra_annotation)}.",
                        "error",
                    )
                )
            continue

        annotation = _normalize_annotation(parameter.annotation)
        if _is_missing(annotation):
            annotation = Any
            issues.append(
                ContractIssue(
                    f"Parameter '{parameter.name}' has no type annotation.",
                    "error",
                )
            )
        elif not _annotation_is_serializable(annotation):
            issues.append(
                ContractIssue(
                    f"Parameter '{parameter.name}' uses a non-serializable "
                    f"Workflow type: {_annotation_name(annotation)}.",
                    "error",
                )
            )
        required = parameter.default is inspect.Parameter.empty
        parameters.append(
            ParameterContract(
                name=parameter.name,
                annotation=annotation,
                required=required,
                default=_MISSING if required else parameter.default,
                keyword_only=parameter.kind is inspect.Parameter.KEYWORD_ONLY,
            )
        )

    output_annotation = _normalize_annotation(
        _effective_output_annotation(signature.return_annotation)
    )
    if _is_missing(output_annotation):
        output_annotation = Any
        issues.append(
            ContractIssue(
                "Callable has no return type annotation.",
                "error",
            )
        )
    elif not _annotation_is_serializable(output_annotation):
        issues.append(
            ContractIssue(
                "Callable return annotation uses a non-serializable Workflow "
                f"type: {_annotation_name(output_annotation)}.",
                "error",
            )
        )

    return (
        OperatorContract(
            input=_arguments_contract(
                signature.replace(return_annotation=inspect.Signature.empty),
                tuple(parameters),
                allow_extra=allow_extra,
                extra_annotation=extra_annotation,
            ),
            output=value_contract(output_annotation),
        ),
        tuple(issues),
    )


def callable_output_contract(
    handler: Any,
) -> tuple[SchemaContract, tuple[ContractIssue, ...]]:
    """Derive only the persisted output contract of a framework hook."""

    signature = _signature(handler)
    if signature is None:
        return value_contract(Any), (
            ContractIssue("Callable signature cannot be inspected.", "error"),
        )
    annotation = _normalize_annotation(
        _effective_output_annotation(signature.return_annotation)
    )
    if _is_missing(annotation):
        return value_contract(Any), (
            ContractIssue("Callable has no return type annotation.", "error"),
        )
    if not _annotation_is_serializable(annotation):
        return value_contract(annotation), (
            ContractIssue(
                "Callable return annotation uses a non-serializable Workflow "
                f"type: {_annotation_name(annotation)}.",
                "error",
            ),
        )
    return value_contract(annotation), ()


def ensure_callable_contract(handler: Any) -> OperatorContract:
    """Reject unsupported callables and emit non-blocking annotation warnings."""

    contract, issues = callable_contract(handler)
    errors = [issue.message for issue in issues if issue.severity == "error"]
    if errors:
        raise ValueError(" ".join(errors))
    for issue in issues:
        if issue.severity == "warning":
            warnings.warn(issue.message, OperatorContractWarning, stacklevel=3)
    return contract


def _effective_output_annotation(annotation: Any) -> Any:
    """Resolve a StreamingResult annotation to its final business output.

    ``Output | StreamingResult[Chunk, Output]`` is intentionally equivalent to
    ``Output`` so one Operator can select invoke or stream mode at runtime
    without changing the Capability contract.
    """

    origin = get_origin(annotation)
    if origin is StreamingResult:
        arguments = get_args(annotation)
        return arguments[1] if len(arguments) == 2 else Any
    if origin not in {Union, types.UnionType}:
        return annotation

    resolved = tuple(
        _effective_output_annotation(value)
        for value in get_args(annotation)
    )
    unique: list[Any] = []
    for value in resolved:
        if value not in unique:
            unique.append(value)
    if len(unique) == 1:
        return unique[0]
    return Union[tuple(unique)]


def value_contract(annotation: Any) -> SchemaContract:
    """Build the canonical contract for one Python output annotation."""

    normalized_annotation = _normalize_annotation(annotation)
    resolved_annotation = (
        Any if _is_missing(normalized_annotation) else normalized_annotation
    )
    adapter = _type_adapter(resolved_annotation)
    json_schema, portable = _annotation_json_schema(resolved_annotation)
    return SchemaContract(
        kind="value",
        annotation=resolved_annotation,
        portable=portable,
        restoration_mode=(
            "json" if _is_dynamic_json(resolved_annotation) else "typed"
        ),
        _json_schema=MappingProxyType(json_schema),
        _value_adapter=adapter,
    )


def compare_contracts(
    capability: OperatorContract,
    operator: OperatorContract,
) -> tuple[ContractIssue, ...]:
    """Check whether an Operator can serve every valid Capability invocation.

    Only mismatches that make named invocation definitely impossible are
    errors. Annotation differences and unknown types remain warnings because
    Python may still execute them correctly.
    """

    issues = _compare_arguments(capability.input, operator.input)
    expected_output = capability.output.annotation
    actual_output = operator.output.annotation
    if _is_unknown(actual_output) and not _is_unknown(expected_output):
        issues.append(
            ContractIssue(
                "Operator output contract is unknown; Capability output "
                "compatibility cannot be proven statically.",
                "warning",
            )
        )
    elif (
        not _is_unknown(expected_output)
        and not _is_unknown(actual_output)
        and expected_output != actual_output
    ):
        issues.append(
            ContractIssue(
                "Capability and Operator return annotations differ; runtime "
                "validation will enforce the effective node contract.",
                "warning",
            )
        )
    return tuple(issues)


def _arguments_contract(
    signature: inspect.Signature | None,
    parameters: tuple[ParameterContract, ...],
    *,
    allow_extra: bool,
    extra_annotation: Any = Any,
) -> SchemaContract:
    adapters = {
        parameter.name: adapter
        for parameter in parameters
        if (adapter := _type_adapter(parameter.annotation)) is not None
    }
    extra_adapter = _type_adapter(extra_annotation) if allow_extra else None
    json_schema, portable = _arguments_json_schema(
        parameters,
        allow_extra=allow_extra,
        extra_annotation=extra_annotation,
    )
    return SchemaContract(
        kind="arguments",
        parameters=parameters,
        allow_extra=allow_extra,
        extra_annotation=extra_annotation,
        inspectable=signature is not None,
        portable=portable,
        _json_schema=MappingProxyType(json_schema),
        _signature=signature,
        _parameter_adapters=MappingProxyType(adapters),
        _extra_adapter=extra_adapter,
    )


def _arguments_json_schema(
    parameters: tuple[ParameterContract, ...],
    *,
    allow_extra: bool,
    extra_annotation: Any,
) -> tuple[dict[str, Any], bool]:
    fields: dict[str, tuple[Any, Any]] = {}
    portable = True
    for parameter in parameters:
        _, annotation_portable = _annotation_json_schema(parameter.annotation)
        safe_annotation = parameter.annotation if annotation_portable else Any
        portable = portable and annotation_portable
        default = ... if parameter.required else parameter.default
        fields[parameter.name] = (safe_annotation, default)

    model = create_model(
        "AutoAgentArguments",
        __config__=ConfigDict(extra="allow" if allow_extra else "forbid"),
        **fields,
    )
    schema = model.model_json_schema()
    schema.pop("title", None)
    if allow_extra:
        extra_schema, extra_portable = _annotation_json_schema(extra_annotation)
        schema["additionalProperties"] = extra_schema if extra_schema else True
        portable = portable and extra_portable
    else:
        schema["additionalProperties"] = False
    return schema, portable


def _annotation_json_schema(annotation: Any) -> tuple[dict[str, Any], bool]:
    if annotation is Any:
        return {}, True
    if _is_missing(annotation) or annotation is object:
        return {}, False
    adapter = _type_adapter(annotation)
    if adapter is None:
        return {}, False
    try:
        return adapter.json_schema(), True
    except Exception:
        return {}, False


def _type_adapter(annotation: Any) -> TypeAdapter[Any] | None:
    if _is_missing(annotation) or annotation is object:
        return None
    try:
        return TypeAdapter(annotation)
    except Exception:
        return None


def _compare_arguments(
    capability: SchemaContract,
    operator: SchemaContract,
) -> list[ContractIssue]:
    issues: list[ContractIssue] = []
    if not capability.inspectable or not operator.inspectable:
        issues.append(
            ContractIssue(
                "Capability or Operator input contract is not inspectable; "
                "compatibility will be checked when invoked.",
                "warning",
            )
        )
        return issues

    capability_parameters = {item.name: item for item in capability.parameters}
    operator_parameters = {item.name: item for item in operator.parameters}
    for name, expected in capability_parameters.items():
        actual = operator_parameters.get(name)
        if actual is None:
            if operator.allow_extra:
                issues.append(
                    ContractIssue(
                        f"Capability parameter '{name}' is accepted only through "
                        "Operator **kwargs; handling cannot be verified statically.",
                        "warning",
                    )
                )
            else:
                issues.append(
                    ContractIssue(
                        f"Operator cannot accept Capability parameter '{name}'.",
                        "error",
                    )
                )
            continue

        if not expected.required and actual.required:
            issues.append(
                ContractIssue(
                    f"Operator parameter '{name}' is required but the Capability "
                    "allows callers to omit it.",
                    "error",
                )
            )
        if not _is_unknown(expected.annotation) and not _is_unknown(actual.annotation):
            if expected.annotation != actual.annotation:
                issues.append(
                    ContractIssue(
                        f"Parameter '{name}' annotations differ between Capability "
                        "and Operator.",
                        "warning",
                    )
                )
        elif expected.annotation != actual.annotation:
            issues.append(
                ContractIssue(
                    f"Parameter '{name}' cannot be compared completely because an "
                    "annotation is missing or Any.",
                    "warning",
                )
            )
        if not expected.required and not actual.required:
            if expected.default != actual.default:
                issues.append(
                    ContractIssue(
                        f"Parameter '{name}' has different default values in the "
                        "Capability and Operator.",
                        "warning",
                    )
                )

    for name, parameter in operator_parameters.items():
        if name not in capability_parameters and parameter.required:
            issues.append(
                ContractIssue(
                    f"Operator requires parameter '{name}', but the Capability "
                    "contract cannot provide it.",
                    "error",
                )
            )
    return issues


def _is_unknown(annotation: Any) -> bool:
    return (
        annotation is inspect.Parameter.empty
        or annotation is inspect.Signature.empty
        or annotation is Any
    )


def _is_missing(annotation: Any) -> bool:
    return annotation is inspect.Parameter.empty or annotation is inspect.Signature.empty


def _normalize_annotation(annotation: Any) -> Any:
    return type(None) if annotation is None else annotation


def _is_dynamic_json(annotation: Any) -> bool:
    if annotation is Any:
        return True
    origin = get_origin(annotation)
    if annotation in {dict, list}:
        return True
    return origin in {dict, list} and any(
        argument is Any for argument in get_args(annotation)
    )


def _annotation_is_serializable(
    annotation: Any,
    *,
    seen: frozenset[Any] = frozenset(),
) -> bool:
    if annotation is Any:
        return True
    if annotation in {
        str,
        int,
        float,
        bool,
        type(None),
        UUID,
        datetime,
        date,
        time,
        Decimal,
    }:
        return True
    if annotation in {bytes, bytearray, memoryview, object} or _is_missing(annotation):
        return False
    if annotation in seen:
        return True
    nested_seen = seen | {annotation}
    if inspect.isclass(annotation) and issubclass(annotation, Enum):
        return all(
            _runtime_value_is_serializable(member.value)
            for member in annotation
        )
    if inspect.isclass(annotation) and issubclass(annotation, BaseModel):
        return all(
            _annotation_is_serializable(
                field_info.annotation,
                seen=nested_seen,
            )
            for field_info in annotation.model_fields.values()
        )
    if is_typeddict(annotation):
        return all(
            _annotation_is_serializable(value, seen=nested_seen)
            for value in annotation.__annotations__.values()
        )

    origin = get_origin(annotation)
    arguments = get_args(annotation)
    if origin is type:
        return False
    if origin in {Union, types.UnionType}:
        return all(
            _annotation_is_serializable(value, seen=nested_seen)
            for value in arguments
        )
    if origin is Literal:
        return all(_runtime_value_is_serializable(value) for value in arguments)
    if origin is Annotated:
        return bool(arguments) and _annotation_is_serializable(
            arguments[0],
            seen=nested_seen,
        )
    if annotation in {dict, list, tuple, set, frozenset}:
        return True
    if origin is dict:
        return (
            len(arguments) == 2
            and arguments[0] is str
            and _annotation_is_serializable(arguments[1], seen=nested_seen)
        )
    if origin in {list, set, frozenset}:
        return len(arguments) == 1 and _annotation_is_serializable(
            arguments[0],
            seen=nested_seen,
        )
    if origin is tuple:
        values = arguments[:-1] if arguments[-1:] == (Ellipsis,) else arguments
        return all(
            _annotation_is_serializable(value, seen=nested_seen)
            for value in values
        )
    return False


def _runtime_value_is_serializable(value: Any) -> bool:
    from autoagent.core.runtime.serialization import RuntimeSerializationError
    from autoagent.core.runtime.serialization import normalize_json_value

    try:
        normalize_json_value(value)
    except RuntimeSerializationError:
        return False
    return True


def _normalize_dynamic_json(value: Any) -> Any:
    from autoagent.core.runtime.serialization import normalize_json_value

    return normalize_json_value(value)


def _ensure_serializable(value: Any) -> Any:
    from autoagent.core.runtime.serialization import ensure_serializable_value

    return ensure_serializable_value(value)


def _annotation_name(annotation: Any) -> str:
    return getattr(annotation, "__name__", repr(annotation))


def _signature(handler: Any) -> inspect.Signature | None:
    try:
        return inspect.signature(handler, eval_str=True)
    except (NameError, TypeError, ValueError):
        try:
            return inspect.signature(handler)
        except (TypeError, ValueError):
            return None
