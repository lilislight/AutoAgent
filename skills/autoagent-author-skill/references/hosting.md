# Run and embed a Workflow project

Use the same registered Workflow definition from the CLI, an embedded
Python application, or FastAPI. Choose one owner for the Host lifecycle
and its database writer.

| Entry point | Lifecycle owner |
| --- | --- |
| `autoagent server` or `create_app(project=...)` | The Server opens and closes the project Host. |
| `async with AutoAgentHost.from_project(...)` | The calling Python application owns the Host. |
| `create_router(host)` or `create_app(host=host)` | The enclosing application enters and closes the supplied Host. |

## Configure the project and start the Server

Use `autoagent[cli,server]` in the selected project environment. Set
`AUTOAGENT_DATABASE_URL` in the project `.env` or process environment.
For example, the default URL
`sqlite+aiosqlite:///.autoagent/runtime.db` resolves relative to the
project root. Use `postgresql+asyncpg://` for PostgreSQL. Keep credentials
in environment configuration.

```bash
autoagent --project . server --host 127.0.0.1 --port 8765
```

The standalone Server serves the bundled UI when the installed build
contains it. Add `--no-ui` for API-only operation. Use the reported
API base URL, including the API prefix, when running remote CLI commands.
See [CLI source selection](validation.md#choose-local-or-server-cli).

From Python, `autoagent.server.create_app(project=".", ui=False)` gives an
ASGI app whose lifespan owns the Host. Pass `ui=True` to serve the UI too.

## Run a project from Python

Use `AutoAgentHost.from_project(path)` for a project using
[manifest or decorator registration](project.md). If the embedding
application already has the Workflow objects, construct
`AutoAgentHost(workflows=[...], settings=..., lifespan=...)`. Enter it
once on the application's event loop.

```python
from autoagent.host import AutoAgentHost


async def run_normalization():
    async with AutoAgentHost.from_project(".") as host:
        execution = await host.execute(
            "normalize", {"text": "  Hello  "}, entry_node_id="normalize"
        )
        result = await host.join(execution)
        return result
```

`execute` submits a Root Invocation with the Python input contract.
`join` observes a stable boundary. Inspect `result.status` and
`result.waits` before treating an observed result as terminal. For a
pending external Wait, use `await host.respond(execution, wait_id, response)`
and observe the Invocation again. To use an existing Session, pass its
`session_id` explicitly after checking that the Session is ready to reuse.

For shared project resources, declare `lifespan = "module:resources"` in
the manifest's `[project]` table. The asynchronous context manager receives
`HostSetup`. Bind Operator implementations with
`setup.register_operator(..., capability_id=..., default=True)` and use
`setup.providers` for named Providers. Create loop-bound clients inside
the scope that uses them and release them on exit.

## Add AutoAgent to an existing FastAPI application

Include the router during app assembly and enter its Host during the
application lifespan. The sample uses the supplied Host for the entire
ASGI lifespan.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from autoagent.host import AutoAgentHost
from autoagent.server import ServerSettings, create_router

host = AutoAgentHost.from_project(".")
settings = ServerSettings.from_environment(host.environment, root=host.root)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with host:
        yield


app = FastAPI(lifespan=lifespan)
app.include_router(create_router(host, settings=settings))
```

When the application also needs the AutoAgent UI, call `mount_ui` after
including the API router and supply the same settings. Choose an unused
UI mount path in an application with its own routes.

Use CLI or Host commands for development runs and ordinary execution
control. Use the same running Server for remote execution and control when
it already owns the database writer.
