"""Project environment snapshots with process-over-file precedence."""

from __future__ import annotations

import ast
import os
import re
from collections.abc import Mapping
from pathlib import Path

from .errors import HostDiagnostic, HostSettingsError


_ENVIRONMENT_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_project_environment(
    project_root: str | Path,
    *,
    env_file: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Load ``.env`` then overlay the real process environment.

    The returned snapshot may contain application-specific values.  Only the
    known AutoAgent keys are selected into :class:`HostSettings`.
    """

    root = Path(project_root).expanduser().resolve()
    selected = root / ".env" if env_file is None else Path(env_file).expanduser()
    if not selected.is_absolute():
        selected = root / selected

    values: dict[str, str] = {}
    if selected.is_file():
        values.update(_read_dotenv(selected))
    elif env_file is not None:
        raise HostSettingsError(
            [
                HostDiagnostic(
                    code="ENV_FILE_NOT_FOUND",
                    message=f"Environment file does not exist: {selected}",
                    path=str(selected),
                )
            ]
        )

    process = os.environ if environ is None else environ
    invalid = next(
        (
            key
            for key, value in process.items()
            if not isinstance(key, str) or not isinstance(value, str)
        ),
        None,
    )
    if invalid is not None:
        raise HostSettingsError(
            [
                HostDiagnostic(
                    code="ENVIRONMENT_VALUE_INVALID",
                    message="Environment keys and values must be strings.",
                    field=str(invalid),
                )
            ]
        )
    values.update(process)
    return values


def _read_dotenv(path: Path) -> dict[str, str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise HostSettingsError(
            [
                HostDiagnostic(
                    code="ENV_FILE_READ_FAILED",
                    message=f"Cannot read environment file: {error}",
                    path=str(path),
                )
            ]
        ) from error

    values: dict[str, str] = {}
    diagnostics: list[HostDiagnostic] = []
    for line_number, source in enumerate(lines, start=1):
        line = source.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, raw_value = line.partition("=")
        key = key.strip()
        if not separator or not _ENVIRONMENT_KEY.fullmatch(key):
            diagnostics.append(
                HostDiagnostic(
                    code="ENV_FILE_LINE_INVALID",
                    message=f"Invalid environment assignment on line {line_number}.",
                    path=str(path),
                    field=str(line_number),
                )
            )
            continue
        try:
            values[key] = _dotenv_value(raw_value.strip())
        except ValueError as error:
            diagnostics.append(
                HostDiagnostic(
                    code="ENV_FILE_VALUE_INVALID",
                    message=f"Invalid value for {key} on line {line_number}: {error}",
                    path=str(path),
                    field=key,
                )
            )
    if diagnostics:
        raise HostSettingsError(diagnostics)
    return values


def _dotenv_value(value: str) -> str:
    if not value:
        return ""
    if value[0] in {"'", '"'}:
        quote = value[0]
        closing = _closing_quote(value, quote)
        if closing is None:
            raise ValueError("unterminated quoted value")
        tail = value[closing + 1 :].strip()
        if tail and not tail.startswith("#"):
            raise ValueError("unexpected text after quoted value")
        try:
            parsed = ast.literal_eval(value[: closing + 1])
        except (SyntaxError, ValueError) as error:
            raise ValueError("invalid quoted value") from error
        if not isinstance(parsed, str):  # pragma: no cover - literal is quoted
            raise ValueError("quoted value must be a string")
        return parsed
    comment = value.find(" #")
    return (value[:comment] if comment >= 0 else value).rstrip()


def _closing_quote(value: str, quote: str) -> int | None:
    escaped = False
    for index, character in enumerate(value[1:], start=1):
        if quote == '"' and character == "\\" and not escaped:
            escaped = True
            continue
        if character == quote and not escaped:
            return index
        escaped = False
    return None
