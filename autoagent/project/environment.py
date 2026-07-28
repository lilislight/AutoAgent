from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

from dotenv import dotenv_values


def load_project_environment(
    project_root: Path,
    *,
    env_file: str | Path | None = None,
    use_env_file: bool = True,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Resolve one environment snapshot for App, Provider, and Server settings."""

    values: dict[str, str] = {}
    if use_env_file:
        selected = (
            project_root / ".env"
            if env_file is None
            else Path(env_file).expanduser()
        )
        if not selected.is_absolute():
            selected = project_root / selected
        values.update(
            {
                key: value
                for key, value in dotenv_values(selected).items()
                if value is not None
            }
        )
    values.update(os.environ if environ is None else environ)
    return values
