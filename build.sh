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
    echo "error: Node.js and npm are required to build the web UI." >&2
    exit 1
fi

if ! "${python_bin}" -c 'import build, setuptools, wheel' >/dev/null 2>&1; then
    echo "error: Install requirements-dev.txt in the selected Python environment first." >&2
    exit 1
fi

staging="$(mktemp -d "${TMPDIR:-/tmp}/autoagent-wheel.XXXXXXXX")"
trap 'rm -rf -- "${staging}"' EXIT

cp -R "${repository_root}/autoagent" "${staging}/autoagent"
rm -rf -- "${staging}/autoagent/server/static"
find "${staging}/autoagent" -type d -name __pycache__ -prune -exec rm -rf -- {} +
cp "${repository_root}/pyproject.toml" "${staging}/pyproject.toml"
for license_file in "${repository_root}"/LICENSE*; do
    if [[ -f "${license_file}" ]]; then
        cp "${license_file}" "${staging}/"
    fi
done

echo "Building web UI..."
(
    cd "${repository_root}/ui"
    npm ci
    npm run build -- --outDir "${staging}/autoagent/server/static"
)

echo "Building wheel..."
"${python_bin}" -m build --wheel --no-isolation --outdir "${repository_root}/dist" "${staging}"
echo "Wheel saved to ${repository_root}/dist"
