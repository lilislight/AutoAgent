#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if [[ -n "${PYTHON:-}" ]]; then
    python_bin="${PYTHON}"
elif [[ -x "${repository_root}/.venv/bin/python" ]]; then
    python_bin="${repository_root}/.venv/bin/python"
else
    python_bin="python3"
fi

if ! command -v npm >/dev/null 2>&1; then
    echo "error: npm is required to build the tracing UI." >&2
    exit 1
fi

if ! "${python_bin}" -c "import build" >/dev/null 2>&1; then
    echo "error: Python package 'build' is required." >&2
    echo "install it with: ${python_bin} -m pip install build" >&2
    exit 1
fi

exec "${python_bin}" "${repository_root}/scripts/build_wheel.py"
