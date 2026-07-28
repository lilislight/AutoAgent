# AutoAgent Project Contract

This reference owns project discovery, Manifest, dependency, and environment
boundaries. It does not define Workflow topology, public APIs, or CLI behavior.

## Contents

- Project discovery and Manifest schema
- Entrypoint format
- Workflow module boundary
- Dependencies and environment
- Minimum project
- Package availability and version

## Project discovery

An AutoAgent project has one `auto-agent.toml`. The CLI accepts either the
project directory or the explicit Manifest path through the global
`--project` option. Workflow source may use any importable directory layout;
the Manifest is the discovery contract.

Do not force a user project to copy the normative example layout.

## Manifest schema

V1 accepts only this strict shape:

```toml
schema_version = 1

[project]
name = "release-automation"
version = "0.1.0"
description = "Optional human-readable project description."

[[workflows]]
entrypoint = "workflows.release:workflow"

[[workflows]]
entrypoint = "workflows.approval:workflow"
```

Rules:

- `schema_version` must be `1`.
- `project.name` and `project.version` must be non-empty strings.
- `project.description` is optional.
- At least one `[[workflows]]` entry is required.
- Each entrypoint must be unique.
- Unknown fields are rejected.

## Entrypoint format

Use `<python-module>:<object-path>`.

For:

```toml
entrypoint = "workflows.weather:workflow"
```

the loader imports `workflows.weather` from the project root and reads its
`workflow` attribute. Dotted attributes are allowed:

```toml
entrypoint = "package.module:exports.weather"
```

The selected object must be a `Workflow`. The Workflow object's `id`, not the
entrypoint string, is the ID passed to CLI commands. Workflow IDs must be
unique across the project.

## Workflow module boundary

A Workflow module may define:

- input, output, and intermediate models;
- typed callable Operators;
- Conditions, mappings, bindings, selectors, and aggregators;
- Tools and child Workflows;
- one or more exported Workflow objects.

It must not choose:

- App construction or lifecycle;
- RuntimeStore or database backend;
- database URL and persistence settings;
- Server host, port, authentication, or UI directory;
- hosting, tenancy, secrets, or resource allocation.

The CLI, an embedding host, or a future platform owns those decisions.

## Dependencies

Declare Python dependencies in the project's normal packaging file, such as
`pyproject.toml`. Include `autoagent` and direct business dependencies. Do not
add a package manager-specific lock or command requirement to Workflow source.

Keep imports usable by ordinary Python tooling. A project may choose its own
environment and package manager.

## Environment

Runtime and Provider settings belong in process environment variables. The CLI
loads `<project-root>/.env` by default, then lets process environment variables
override equal keys.

Use:

```text
--env-file <path>   load another environment file
--no-env-file       do not load a file
```

Commit `.env.example` with safe placeholders and explanations. Do not commit a
real `.env` containing secrets.

Only add Provider variables needed by the exported Workflows. The repository
root `.env.example` is the authoritative list of framework settings.

## Minimum project

```text
project/
├── auto-agent.toml
├── pyproject.toml
├── .env.example
└── workflows.py
```

`workflows.py` may export `workflow`, and the Manifest may use:

```toml
[[workflows]]
entrypoint = "workflows:workflow"
```

Additional folders for models, tools, fixtures, or tests are optional.

## Package availability and version

Before reading examples or authoring code, select the project's Python
environment and check whether it already contains AutoAgent:

```bash
python -c "import autoagent, importlib.metadata as m; print(m.version('autoagent')); print(autoagent.__file__)"
autoagent --version
```

Use the `autoagent` executable from the selected environment. If Python can
import the intended version but the CLI is missing or reports another version,
fix environment activation or executable resolution before installing another
copy.

If the import is missing or its version does not satisfy the project:

1. search the target project directory for `autoagent-*.whl`, excluding virtual
   environments and build caches;
2. install a supplied or locally found compatible Wheel into the selected
   environment:

   ```bash
   python -m pip install <path-to-autoagent-wheel>
   ```

3. if no compatible local Wheel exists, install the required package version
   from the configured package index:

   ```bash
   python -m pip install autoagent
   ```

   Add the project's declared version constraint to that command when one
   exists.

If several local Wheels could match and the intended version is unclear, ask
which one to use. After installation, repeat both version commands. The import
and CLI must resolve from the same selected environment and report the intended
version.

Declare the matching `autoagent` constraint in project dependencies. Do not
commit a Wheel or absolute local Wheel path as a permanent dependency unless
the requester explicitly wants a vendored artifact. Report the exact package
check and installation command used.
