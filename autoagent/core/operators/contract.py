from __future__ import annotations

import inspect
import warnings
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal

from pydantic import ConfigDict, TypeAdapter, create_model


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

    The generated JSON Schema is descriptive and portable metadata for tool
    declarations, UI inspection, MCP adapters, and diagnostics. Runtime uses
    the private Pydantic adapters and signature retained inside this object, so
    an annotation may remain executable even when it cannot be represented
    completely as JSON Schema.
    """

    kind: Literal["arguments", "value"]
    parameters: tuple[ParameterContract, ...] = ()
    annotation: Any = Any
    allow_extra: bool = False
    extra_annotation: Any = Any
    inspectable: bool = True
    portable: bool = True
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
        }

    @property
    def known(self) -> bool:
        """Whether this contract has enough Python information to validate."""

        if self.kind == "arguments":
            return self.inspectable
        return not _is_unknown(self.annotation)

    def validate(self, value: Any) -> Any:
        """Validate a named argument Mapping or one output value strictly."""

        if self.kind == "arguments":
            return self._validate_arguments(value)
        if self._value_adapter is not None:
            self._value_adapter.validate_python(value, strict=True)
        return value

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
        named_parameters = {parameter.name for parameter in self.parameters}
        for name, argument in bound.arguments.items():
            adapter = self._parameter_adapters.get(name)
            if adapter is not None:
                validated[name] = adapter.validate_python(argument, strict=True)
                continue
            if name in named_parameters or self._extra_adapter is None:
                continue
            for extra_name, extra_value in argument.items():
                validated[extra_name] = self._extra_adapter.validate_python(
                    extra_value,
                    strict=True,
                )
        return validated


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
    that protocol and are rejected. Missing annotations and ``Any`` reduce
    validation quality but do not prevent execution, so they produce warnings.
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
                    "Callable signature cannot be inspected; input and output "
                    "compatibility will be checked only when invoked.",
                    "warning",
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
            extra_annotation = parameter.annotation
            continue

        annotation = parameter.annotation
        if _is_unknown(annotation):
            annotation = Any
            issues.append(
                ContractIssue(
                    f"Parameter '{parameter.name}' has no concrete type annotation; "
                    "runtime type validation is unavailable for this field.",
                    "warning",
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

    output_annotation = signature.return_annotation
    if _is_unknown(output_annotation):
        output_annotation = Any
        issues.append(
            ContractIssue(
                "Callable has no concrete return annotation; runtime output "
                "validation is unavailable.",
                "warning",
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


def value_contract(annotation: Any) -> SchemaContract:
    """Build the canonical contract for one Python output annotation."""

    resolved_annotation = Any if _is_unknown(annotation) else annotation
    adapter = _type_adapter(resolved_annotation)
    json_schema, portable = _annotation_json_schema(resolved_annotation)
    return SchemaContract(
        kind="value",
        annotation=resolved_annotation,
        portable=portable,
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
    if _is_unknown(annotation):
        return {}, False
    adapter = _type_adapter(annotation)
    if adapter is None:
        return {}, False
    try:
        return adapter.json_schema(), True
    except Exception:
        return {}, False


def _type_adapter(annotation: Any) -> TypeAdapter[Any] | None:
    if _is_unknown(annotation):
        return None
    try:
        return TypeAdapter(annotation)
    except Exception:
        try:
            return TypeAdapter(
                annotation,
                config=ConfigDict(arbitrary_types_allowed=True),
            )
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


def _signature(handler: Any) -> inspect.Signature | None:
    try:
        return inspect.signature(handler, eval_str=True)
    except (NameError, TypeError, ValueError):
        try:
            return inspect.signature(handler)
        except (TypeError, ValueError):
            return None
