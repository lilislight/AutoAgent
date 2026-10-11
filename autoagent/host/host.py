"""High-level application operations used by CLI and Server implementations."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from contextlib import asynccontextmanager
from uuid import uuid4
from pathlib import Path
from types import MappingProxyType
from dataclasses import replace

from sqlalchemy.engine import make_url
from pydantic import TypeAdapter

from autoagent import InvocationResult, InvocationWait, RuntimeErrorInfo, RuntimeTransitionError, WorkflowCompiler

from .models import HostError, HostSettings, HostCapacityError, ProjectCheck
from .persistence import DatabaseSink
from .persistence.models import Page
from .residency import Residency
from autoagent.core.app._residency import retained_size
from .project import Project, environment_scope, import_scope, load_project_environment
from .runtime import Runtime

_json_output = TypeAdapter(object)


def _result_view(result):
    """CLI/Server outputs use JSON values regardless of graph residency."""
    return replace(result, output=_json_output.dump_python(result.output, mode="json"))


class AutoAgentHost:
    """Own runtime and persistence behind business operations, in one caller loop.

    CLI/Server use ``async with host``. Checkpoint, loading, residency and durable
    flushing are internal policies, not user-facing controls.
    """

    def __init__(
        self,
        *,
        workflows=(),
        settings=None,
        lifespan=None,
        operators=(),
        environment=None,
        root=None,
    ):
        self.root = Path(root or Path.cwd()).expanduser().resolve()
        self.environment = MappingProxyType(
            dict(os.environ if environment is None else environment)
        )
        self.settings = settings or HostSettings.from_environment(self.environment)
        self._workflows, self._operators = tuple(workflows), tuple(operators)
        self._lifespan = lifespan
        self._project = None
        self._state = "new"
        self._loop = None
        self._opening_task = None
        self._closing_task = None
        self._operations = set()
        self._request_bytes = 0
        self._control_requests = 0
        self._runtime = None
        self._persistence = None
        self._environment_context = None
        self._import_context = None

    @classmethod
    def from_project(
        cls,
        path=".",
        *,
        settings=None,
        env_file=None,
        use_env_file=True,
        environment=None,
        lifespan=None,
    ):
        project = Project.find(path)
        env = load_project_environment(
            project.root,
            env_file=env_file,
            use_env_file=use_env_file,
            environ=environment,
        )
        host = cls(
            settings=settings, environment=env, root=project.root, lifespan=lifespan
        )
        host._project = project
        return host

    @classmethod
    def check_project(
        cls, path=".", *, env_file=None, use_env_file=True, environment=None
    ):
        project = Project.find(path)
        env = load_project_environment(
            project.root,
            env_file=env_file,
            use_env_file=use_env_file,
            environ=environment,
        )
        return project.check(env)

    def _check_loop(self):
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise HostError("A Host belongs to one CLI/Server event loop.")

    async def __aenter__(self):
        self._check_loop()
        if self._state != "new":
            raise HostError("A Host context can be entered only once.")
        if self._opening_task is None:
            self._state = "opening"
            self._opening_task = asyncio.create_task(self._open())
        try:
            await asyncio.shield(self._opening_task)
        except asyncio.CancelledError:
            self._begin_exit()
            raise
        return self

    async def __aexit__(self, *_):
        self._check_loop()
        await asyncio.shield(self._begin_exit())

    def _begin_exit(self):
        if self._closing_task is None:
            self._state = "closing"
            self._closing_task = asyncio.create_task(self._exit())
            self._closing_task.add_done_callback(_consume)
        return self._closing_task

    async def _open(self):
        try:
            self._environment_context = environment_scope(self.environment)
            self._environment_context.__enter__()
            workflows, lifespan = self._workflows, self._lifespan
            if self._project is not None:
                self._import_context = import_scope(self.root)
                self._import_context.__enter__()
                workflows, declared_lifespan = self._project.load()
                lifespan = lifespan or declared_lifespan
            # Static checks precede creating databases or project resources.
            from autoagent import WorkflowCompiler

            for workflow in workflows:
                WorkflowCompiler().compile_or_raise(workflow)
            url = self.settings.resolved_database_url(self.root)
            parsed = make_url(url)
            if parsed.drivername == "sqlite+aiosqlite" and parsed.database not in (
                None,
                "",
                ":memory:",
            ):
                Path(parsed.database).parent.mkdir(parents=True, exist_ok=True)
            self._persistence = await DatabaseSink.open(
                url, config=self.settings.persistence
            )
            self._runtime = Runtime(
                self._persistence,
                self.settings,
                workflows,
                self._operators,
                lifespan,
                self.environment,
            )
            await self._runtime.prepare()
            self._runtime.residency = Residency(self._runtime, self.settings)
            self._runtime.residency.start()
            if self._state != "closing":
                self._state = "ready"
        except BaseException:
            try:
                if self._runtime is not None:
                    await self._runtime.app.aclose(timeout=None)
            finally:
                try:
                    if self._persistence is not None:
                        await self._persistence.close()
                finally:
                    self._release_environment()
                    self._state = "failed"
            raise

    async def _exit(self):
        completed = False
        try:
            if self._opening_task is not None:
                try:
                    await asyncio.shield(self._opening_task)
                except BaseException:  # noqa: BLE001 -- opening already released every resource
                    return
            if self._runtime is not None:
                await self._runtime.residency.stop()
                try:
                    await self._runtime.shutdown()
                finally:
                    await self._runtime.residency.finish()
            completed = True
        finally:
            for task in tuple(self._operations):
                task.cancel()
            await asyncio.gather(*tuple(self._operations), return_exceptions=True)
            try:
                if self._persistence is not None:
                    await self._persistence.close()
            finally:
                self._release_environment()
                self._state = "closed"
                if completed:
                    self._runtime = None
                    self._persistence = None

    def _release_environment(self):
        if self._import_context is not None:
            self._import_context.__exit__(None, None, None)
            self._import_context = None
        if self._environment_context is not None:
            self._environment_context.__exit__(None, None, None)
            self._environment_context = None

    async def _perform(self, operation: Callable, *, payload=None, control=False):
        self._check_loop()
        if self._state != "ready":
            raise HostError("Host is not accepting operations.")
        size = retained_size((payload,)) if payload is not None else 0
        count = self._control_requests if control else len(self._operations) - self._control_requests
        limit = self.settings.control_request_reserve if control else self.settings.max_requests
        if count >= limit or self._request_bytes + size > self.settings.max_request_bytes:
            raise HostCapacityError("Host request capacity is exhausted.")
        self._request_bytes += size
        if control:
            self._control_requests += 1
        async def admitted():
            try:
                return await operation()
            finally:
                self._request_bytes -= size
                if control:
                    self._control_requests -= 1
        task = asyncio.create_task(admitted())
        self._operations.add(task)
        task.add_done_callback(self._operations.discard)
        task.add_done_callback(_consume)
        # A request disconnect does not cancel an admitted Core operation.
        return await asyncio.shield(task)

    async def check_workflows(self, workflow_ids=None):
        """Compile project definitions without opening Core or Persistence."""
        if self._state not in ("new", "ready"):
            raise HostError("Host cannot check Workflows in its current lifecycle state.")
        with environment_scope(self.environment), import_scope(self.root):
            workflows = self._project.load()[0] if self._project else self._workflows
            if self._state == "ready":
                workflows = tuple(self._runtime.setup.workflows)
            if workflow_ids is not None:
                requested = set(workflow_ids)
                selected = tuple(w for w in workflows if w.id in requested)
                if requested - {w.id for w in selected}:
                    raise HostError("Unknown Workflow requested for compilation.")
                workflows = selected
            return ProjectCheck(
                self._project.name if self._project else self.root.name,
                tuple(WorkflowCompiler().compile(w) for w in workflows),
            )

    async def list_workflows(self, *, workflow_id=None, include_history=False, limit=100, cursor=None):
        async def query():
            if type(limit) is not int or not 1 <= limit <= 1000:
                raise ValueError("limit must be between 1 and 1000.")
            if include_history:
                page = await self._persistence.list_workflows(workflow_id=workflow_id, limit=limit, cursor=cursor)
                rows = tuple(dict(row, runnable=row["revision_id"] in self._runtime.revisions) for row in page.items)
                return Page(rows, page.next_cursor, page.frontier)
            rows = sorted(({
                "workflow_id": wid, "revision_id": revision,
                "workflow_version": self._runtime.revisions[revision].workflow_version,
                "runnable": True,
            } for wid, revision in self._runtime.workflow_ids.items() if workflow_id is None or wid == workflow_id), key=lambda row: (row["workflow_id"], row["revision_id"]))
            if cursor is not None:
                if not isinstance(cursor, (tuple, list)) or len(cursor) != 2:
                    raise ValueError("Invalid Workflow cursor.")
                rows = [r for r in rows if (r["workflow_id"], r["revision_id"]) > tuple(cursor)]
            chosen = rows[:limit]
            return Page(tuple(chosen), (chosen[-1]["workflow_id"], chosen[-1]["revision_id"]) if len(rows) > limit else None)
        return await self._perform(query)

    async def get_workflow(self, workflow_id, *, revision_id=None):
        async def query():
            revision = revision_id or self._runtime.workflow_ids.get(workflow_id)
            if revision is None:
                raise HostError(f"Unknown registered Workflow: {workflow_id}")
            record = await self._persistence.get_workflow(revision)
            if record is None or record["workflow_id"] != workflow_id:
                raise HostError("Unknown Workflow Revision.")
            return dict(record, runnable=revision in self._runtime.revisions)
        return await self._perform(query)

    async def execute(self, workflow_id, value, *, revision_id=None, session_id=None, session_context=None, entry_node_id=None):
        async def execute():
            if self._runtime.residency.fault is not None:
                raise HostCapacityError("Host residency persistence failed; admission is stopped.") from self._runtime.residency.fault
            revision = revision_id or self._runtime.workflow_ids.get(workflow_id)
            snapshot = self._runtime.revisions.get(revision)
            if snapshot is None or snapshot.workflow_id != workflow_id:
                raise HostError("Workflow Revision is not registered for execution.")
            sid = session_id or str(uuid4())
            item = self._runtime.residency.touch(sid)
            item.users += 1
            try:
                async with self._runtime.coordinated(sid):
                    if sid in self._runtime.resident:
                        await self._runtime.residency.admit_resident(sid)
                    await self._runtime.prepare_session(sid)
                    previous = self._runtime.resident.get(sid)
                    old_children = tuple(child for child, owner in self._runtime.owners.items() if owner == sid)
                    submission = await self._runtime.app.asubmit_invoke(
                        revision, value, session_id=sid,
                        session_context=session_context, entry_node_id=entry_node_id,
                    )
                    for child in old_children:
                        self._runtime.owners.pop(child, None)
                    if previous is not None:
                        self._runtime.needs_recovery.discard(previous.invocation_id)
                    self._runtime.resident[sid] = submission.ref
                    self._runtime.residency.installed(sid)
                    return submission.ref
            finally:
                item.users -= 1
                self._runtime.residency.loading.discard(sid)
        return await self._perform(execute, payload=(value, session_context))

    @asynccontextmanager
    async def _execution(self, execution, *, load=False, touch=False):
        ref = await self._runtime.reference(execution)
        item = self._runtime.residency.touch(ref.session_id) if touch else self._runtime.residency.entry(ref.session_id)
        item.users += 1
        try:
            async with self._runtime.coordinated(ref.session_id):
                if load:
                    await self._runtime.ensure_resident(ref)
                yield_ref = ref
            yield yield_ref
        finally:
            item.users -= 1
            self._runtime.residency.trim()

    async def _stored_result(self, ref):
        row = await self._persistence.execution_snapshot(ref.invocation_id, view="accepted")
        if row is None:
            raise HostError("Invocation history is unavailable.")
        waits = tuple(InvocationWait(w["id"], w["request"] if w["kind"] == "external" else None, w["kind"], w["request"] if w["kind"] != "external" else None) for w in row["waits"])
        return InvocationResult(
            ref, row["status"], row["output"],
            RuntimeErrorInfo(**row["error"]) if row["error"] else None,
            waits, row.get("pending_outcome"), row.get("control_state") or "active",
        )

    async def get_execution(self, execution):
        async def query():
            async with self._execution(execution) as ref:
                if self._runtime.resident.get(ref.session_id) == ref:
                    return _result_view(await self._runtime.app.astatus(ref))
                return await self._stored_result(ref)
        return await self._perform(query)

    async def join(self, execution, *, timeout=None):
        async def wait():
            if timeout is not None:
                import math
                if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout < 0:
                    raise ValueError("timeout must be finite and nonnegative.")
            async with self._execution(execution) as ref:
                if self._runtime.resident.get(ref.session_id) == ref:
                    return _result_view(await self._runtime.app.ajoin(ref, timeout=timeout))
                result = await self._stored_result(ref)
                if result.status in ("completed", "failed", "cancelled") or result.control_state == "paused" or result.waits:
                    return result
                raise HostError("Invocation is not active in this Host; use continue_execution.")
        return await self._perform(wait)

    async def respond(self, execution, wait_id, response):
        async def deliver():
            async with self._execution(execution, load=True, touch=True) as ref:
                await self._runtime.activate(ref)
                await self._runtime.app.asubmit_resume(ref, wait_id, response)
                return ref
        return await self._perform(deliver, payload=response, control=True)

    async def signal(self, execution, endpoint, payload, *, source_id, sequence):
        async def deliver():
            async with self._execution(execution, load=True, touch=True) as ref:
                await self._runtime.activate(ref)
                return await self._runtime.app.asignal(ref, endpoint, payload, source_id=source_id, sequence=sequence)
        return await self._perform(deliver, payload=payload, control=True)

    async def pause(self, execution, *, timeout=None):
        async def pause():
            async with self._execution(execution, load=True, touch=True) as ref:
                await self._runtime.activate(ref)
                return _result_view(await self._runtime.app.apause(ref, timeout=timeout))
        return await self._perform(pause, control=True)

    async def continue_execution(self, execution):
        async def recover():
            async with self._execution(execution, load=True, touch=True) as ref:
                return _result_view(await self._runtime.activate(ref, force=True))
        return await self._perform(recover, control=True)

    async def cancel(self, execution, *, reason=None):
        async def cancel():
            async with self._execution(execution, load=True, touch=True) as ref:
                return _result_view(await self._runtime.app.acancel(ref, reason))
        return await self._perform(cancel, control=True)


def _consume(task):
    if not task.cancelled():
        task.exception()
