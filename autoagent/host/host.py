"""High-level application operations used by CLI and Server implementations."""

from __future__ import annotations

import asyncio
import json
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

from .models import HostError, HostSettings, HostCapacityError, HostNotFoundError, HostRunConflict, ProjectCheck
from .persistence import DatabaseSink
from .persistence.models import Page
from .residency import Residency
from autoagent.core.app._residency import retained_size
from .project import Project, environment_scope, import_scope, load_project_environment
from .runtime import Runtime
from .controls import ExecutionControls
from .checkpoints import CheckpointControls
from .exploration import Exploration
from .observation import ExecutionObservation
from .history import HistoryQueries
from .forks import ForkControls
from .operations import ExecutionOperations, OperationControls

_json_output = TypeAdapter(object)


def _result_view(result):
    """CLI/Server outputs use JSON values regardless of graph residency."""
    return replace(result, output=_json_output.dump_python(result.output, mode="json"))


class AutoAgentHost(OperationControls, ExecutionControls, CheckpointControls, Exploration, ExecutionObservation, HistoryQueries, ForkControls):
    """Own runtime and persistence behind business operations, in one caller loop.

    CLI/Server use ``async with host``. Loading and residency are automatic;
    explicit Checkpoint metadata controls support debugging and retention.
    ``read_only=True`` opens existing history without importing project code,
    starting Core, executing lifespan, or acquiring writer ownership.
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
        read_only=False,
    ):
        self.root = Path(root or Path.cwd()).expanduser().resolve()
        self.environment = MappingProxyType(
            dict(os.environ if environment is None else environment)
        )
        self.settings = settings or HostSettings.from_environment(self.environment)
        self._workflows, self._operators = tuple(workflows), tuple(operators)
        self._lifespan = lifespan
        self._project = None
        if type(read_only) is not bool:
            raise ValueError("read_only must be a boolean.")
        self._read_only = read_only
        self._state = "new"
        self._loop = None
        self._opening_task = None
        self._closing_task = None
        self._operations = set()
        self._request_bytes = 0
        self._control_requests = 0
        self._runtime = None
        self._persistence = None
        self._execution_operations = None
        self._environment_context = None
        self._import_context = None
        self._memory_monitor = None

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
        read_only=False,
    ):
        project = Project.find(path)
        env = load_project_environment(
            project.root,
            env_file=env_file,
            use_env_file=use_env_file,
            environ=environment,
        )
        host = cls(
            settings=settings, environment=env, root=project.root, lifespan=lifespan, read_only=read_only
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
            from .memory import MemoryMonitor
            self._memory_monitor = MemoryMonitor(self)
            await self._memory_monitor.start()
            if self._read_only:
                self._persistence = await DatabaseSink.open(self.settings.resolved_database_url(self.root),
                    config=self.settings.persistence, read_only=True)
                await self._memory_monitor.ready()
                if self._state != "closing":
                    self._state = "ready"
                return
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
            self._execution_operations = ExecutionOperations(self)
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
            await self._memory_monitor.ready()
            if self._state != "closing":
                self._state = "ready"
        except BaseException:
            try:
                if self._memory_monitor is not None:
                    await self._memory_monitor.stop()
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
                await self._execution_operations.close()
                await self._runtime.residency.stop()
                try:
                    await self._runtime.shutdown()
                finally:
                    await self._runtime.residency.finish()
            completed = True
        finally:
            if self._memory_monitor is not None:
                await self._memory_monitor.stop()
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
        if self._read_only and (control or payload is not None):
            raise HostRunConflict("read_only", "This Host is open for read-only inspection.")
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

    async def memory_snapshot(self, *, history=0):
        """Read bounded telemetry without loading or walking Runtime State."""
        self._check_loop()
        if self._state != 'ready':
            raise HostError('Host is not ready.')
        return self._memory_monitor.snapshot(history=history)

    async def check_workflows(self, workflow_ids=None):
        """Compile project definitions without opening Core or Persistence."""
        if self._read_only:
            raise HostError("Use check_project for offline source compilation.")
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
                rows = tuple(dict(row, runnable=self._runtime is not None and row["revision_id"] in self._runtime.revisions) for row in page.items)
                return Page(rows, page.next_cursor, page.frontier)
            rows = sorted(({
                "workflow_id": wid, "revision_id": revision,
                "workflow_version": self._runtime.revisions[revision].workflow_version,
                "runnable": True,
            } for wid, revision in (self._runtime.workflow_ids.items() if self._runtime else ()) if workflow_id is None or wid == workflow_id), key=lambda row: (row["workflow_id"], row["revision_id"]))
            if cursor is not None:
                if not isinstance(cursor, (tuple, list)) or len(cursor) != 2:
                    raise ValueError("Invalid Workflow cursor.")
                rows = [r for r in rows if (r["workflow_id"], r["revision_id"]) > tuple(cursor)]
            chosen = rows[:limit]
            return Page(tuple(chosen), (chosen[-1]["workflow_id"], chosen[-1]["revision_id"]) if len(rows) > limit else None)
        return await self._perform(query)

    async def browse_workflows(self, *, search="", limit=100, cursor=None, registered_only=False):
        """Browse grouped saved Workflows with current registration metadata."""
        async def query():
            if not isinstance(search, str) or len(search) > 512:
                raise ValueError("search must be a string of at most 512 characters.")
            current = self._runtime.workflow_ids if self._runtime else {}
            page = await self._persistence.workflow_catalog(registered_ids=tuple(current), search=search.strip().lower(), limit=limit, cursor=cursor, registered_only=registered_only)
            rows = []
            for row in page.items:
                revision = current.get(row["workflow_id"])
                result = dict(row, current_revision_id=revision)
                if revision is not None:
                    result["workflow_version"] = self._runtime.revisions[revision].workflow_version
                rows.append(result)
            return Page(tuple(rows), page.next_cursor, page.frontier)
        return await self._perform(query)

    async def list_workflow_revisions(self, workflow_id, *, prefix=None, limit=100, cursor=None):
        """Read bounded newest-first history for one Workflow."""
        async def query():
            if prefix is not None and (not isinstance(prefix, str) or len(prefix) > 512):
                raise ValueError("prefix must be a string of at most 512 characters.")
            page = await self._persistence.workflow_revisions(workflow_id, prefix=prefix.strip().lower() if prefix else None, limit=limit, cursor=cursor)
            return Page(tuple(dict(row, runnable=self._runtime is not None and row["revision_id"] in self._runtime.revisions) for row in page.items), page.next_cursor, page.frontier)
        return await self._perform(query)

    async def get_workflow(self, workflow_id, *, revision_id=None):
        async def query():
            revision = revision_id or (self._runtime.workflow_ids.get(workflow_id) if self._runtime else None)
            if revision is None:
                raise HostNotFoundError(f"Unknown registered Workflow: {workflow_id}")
            record = await self._persistence.get_workflow(revision)
            if record is None or record["workflow_id"] != workflow_id:
                raise HostNotFoundError("Unknown Workflow Revision.")
            return dict(record, runnable=self._runtime is not None and revision in self._runtime.revisions)
        return await self._perform(query)

    async def get_workflow_graph(self, revision_id):
        """Read only drawing metadata for one exact saved Revision."""
        async def query():
            record = await self._persistence.workflow_graph(revision_id)
            if record is None:
                raise HostNotFoundError("Unknown Workflow Revision.")
            return dict(record, runnable=self._runtime is not None and revision_id in self._runtime.revisions)
        return await self._perform(query)

    async def get_workflow_object(self, revision_id, kind, identity):
        """Read an individual saved Node or Edge definition on demand."""
        async def query():
            record = await self._persistence.workflow_object(revision_id, kind, identity)
            if record is None:
                raise HostNotFoundError("Unknown Workflow object.")
            return record
        return await self._perform(query)

    async def list_executions(self, *, search="", status=None, workflow_ids=(), revision_ids=(), statuses=(), session_id=None,
                              started_after_us=None, started_before_us=None, limit=50, cursor=None):
        """Browse recent persisted Root executions without activating Sessions."""
        if not isinstance(search, str) or len(search) > 512:
            raise ValueError("search must have at most 512 characters.")
        if status not in (None, "created", "running", "waiting", "settling", "completed", "failed", "cancelled", "paused"):
            raise ValueError("Unknown execution status.")
        return await self._perform(lambda: self._persistence.trace_page("root", search=search.strip(),
            workflow_ids=workflow_ids, revision_ids=revision_ids, statuses=tuple(statuses) + ((status,) if status else ()),
            session_id=session_id, started_after_us=started_after_us, started_before_us=started_before_us, limit=limit, cursor=cursor))

    async def list_execution_sessions(self, *, search="", workflow_ids=(), revision_ids=(), statuses=(), started_after_us=None, started_before_us=None, session_id=None, limit=50, cursor=None):
        """Browse persisted Session history without loading Core State."""
        if not isinstance(search, str) or len(search) > 512:
            raise ValueError("search must have at most 512 characters.")
        return await self._perform(lambda: self._persistence.execution_sessions(
            search=search.strip().lower(), workflow_ids=workflow_ids, revision_ids=revision_ids, statuses=statuses,
            session_id=session_id, started_after_us=started_after_us, started_before_us=started_before_us, limit=limit, cursor=cursor,
        ))

    async def list_session_invocations(self, session_id, *, search="", workflow_ids=(), revision_ids=(), statuses=(), started_after_us=None, started_before_us=None, limit=50, cursor=None):
        """Read one Session's cross-Workflow history with optional identity matching."""
        if not isinstance(search, str) or len(search) > 512:
            raise ValueError("search must have at most 512 characters.")
        async def query():
            page = await self._persistence.trace_page(
                "session", session_id=session_id, search=search.strip().lower(),
                workflow_ids=workflow_ids, revision_ids=revision_ids, statuses=statuses, started_after_us=started_after_us, started_before_us=started_before_us, limit=limit, cursor=cursor,
            )
            if page is None:
                raise HostNotFoundError("Unknown persisted Session.")
            return page
        return await self._perform(query)

    async def get_execution_trace(self, invocation_id):
        """Read a durable status snapshot and compact graph state."""
        async def query():
            record = await self._persistence.execution_trace(invocation_id)
            if record is None:
                raise HostNotFoundError("Unknown persisted execution.")
            return record
        return await self._perform(query)

    async def _trace_page(self, kind, invocation_id, **filters):
        async def query():
            page = await self._persistence.trace_page(kind, invocation_id, **filters)
            if page is None:
                raise HostNotFoundError("Unknown persisted execution.")
            return page
        return await self._perform(query)

    async def get_trace_dataset(self, invocation_id, **options):
        """Capture one durable, immutable metadata or Replay export."""
        async def query():
            result = await self._persistence.trace_dataset(invocation_id, **options)
            if result is None:
                raise HostNotFoundError('Unknown persisted execution.')
            return result
        return await self._perform(query)

    async def get_trace_dataset_page(self, snapshot_id, kind, page):
        return await self._perform(lambda: self._persistence.trace_dataset_page(snapshot_id, kind, page))

    async def release_trace_dataset(self, snapshot_id):
        return await self._perform(lambda: self._persistence.release_trace_dataset(snapshot_id))

    async def get_trace_head(self, invocation_id):
        async def query():
            result = await self._persistence.trace_head(invocation_id)
            if result is None:
                raise HostNotFoundError('Unknown persisted execution.')
            return result
        return await self._perform(query)

    async def wait_trace_commit(self, revision, *, timeout=10):
        """Wait without consuming a normal operation slot or loading Core State."""
        self._check_loop()
        if self._state != 'ready':
            raise HostError('Host is not open.')
        if type(revision) is not int or revision < 0 or isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 30:
            raise ValueError('Invalid trace wait boundary.')
        return await self._persistence.wait_trace_commit(revision, timeout)

    async def get_execution_timeline(self, invocation_id):
        """Read a timing overview without loading Core State or Event values."""
        async def query():
            record = await self._persistence.execution_timeline(invocation_id)
            if record is None:
                raise HostNotFoundError('Unknown persisted execution.')
            return record
        return await self._perform(query)

    async def list_timeline_items(self, invocation_id, *, kind='node', start_us, end_us, occurrence_id=None, node_id=None, limit=50, cursor=None):
        """Page timing metadata inside a half-open time window."""
        async def query():
            page = await self._persistence.timeline_page(invocation_id, kind=kind, start_us=start_us,
                end_us=end_us, occurrence_id=occurrence_id, node_id=node_id, limit=limit, cursor=cursor)
            if page is None:
                raise HostNotFoundError('Unknown execution or Node Occurrence.')
            return page
        return await self._perform(query)

    async def get_timeline_position(self, invocation_id, kind, identity, **filters):
        """Locate an interval page through persisted metadata only."""
        async def query():
            result = await self._persistence.timeline_position(invocation_id, kind, identity, **filters)
            if result is None:
                raise HostNotFoundError('Interval is absent from this execution, filter or time window.')
            return result
        return await self._perform(query)

    async def get_timeline_event_groups(self, invocation_id, *, start_us, end_us, buckets=100, through_sequence=None):
        """Read complete, bounded Event rail metadata without loading values."""
        async def query():
            result = await self._persistence.timeline_event_groups(invocation_id, start_us=start_us, end_us=end_us,
                buckets=buckets, through_sequence=through_sequence)
            if result is None:
                raise HostNotFoundError('Unknown persisted execution.')
            return result
        return await self._perform(query)

    async def list_timeline_markers(self, invocation_id, *, start_us, end_us, through_sequence=None, limit=100, cursor=None):
        """Page lightweight Event boundaries without loading Event bodies."""
        async def query():
            page = await self._persistence.timeline_markers(invocation_id, start_us=start_us,
                end_us=end_us, through_sequence=through_sequence, limit=limit, cursor=cursor)
            if page is None:
                raise HostNotFoundError('Unknown persisted execution.')
            return page
        return await self._perform(query)

    async def get_timeline_frame(self, invocation_id, *, sequence=None, at_us=None, direction=0):
        """Read a historical Graph frame without changing the running execution."""
        async def query():
            frame = await self._persistence.timeline_frame(invocation_id, sequence=sequence,
                at_us=at_us, direction=direction)
            if frame is None:
                raise HostNotFoundError('Unknown persisted execution.')
            return frame
        return await self._perform(query)

    async def get_timeline_replay(self, invocation_id, *, sequence=None, at_us=None, through_sequence=None, limit=200):
        """Read a bounded window of complete Events and compact Graph changes."""
        async def query():
            result = await self._persistence.timeline_replay(invocation_id, sequence=sequence, at_us=at_us,
                through_sequence=through_sequence, limit=limit)
            if result is None:
                raise HostNotFoundError('Unknown persisted execution.')
            return result
        return await self._perform(query)

    async def get_execution_record(self, invocation_id, kind, identity):
        """Read one selected Node, Call or Wait's detail metadata."""
        async def query():
            record = await self._persistence.execution_record(invocation_id, kind, identity)
            if record is None:
                raise HostNotFoundError('Unknown execution object.')
            return record
        return await self._perform(query)

    async def list_node_executions(self, invocation_id, node_id=None, *, status=None, limit=50, cursor=None):
        return await self._trace_page("node", invocation_id, node_id=node_id, status=status, limit=limit, cursor=cursor)

    async def list_operator_calls(self, invocation_id, *, occurrence_id=None, status=None, limit=50, cursor=None):
        return await self._trace_page("call", invocation_id, occurrence_id=occurrence_id, status=status, limit=limit, cursor=cursor)

    async def list_edge_evaluations(self, invocation_id, edge_id=None, *, occurrence_id=None, limit=50, cursor=None):
        return await self._trace_page("edge", invocation_id, edge_id=edge_id, occurrence_id=occurrence_id, limit=limit, cursor=cursor)

    async def list_execution_waits(self, invocation_id, *, occurrence_id=None, status=None, limit=50, cursor=None):
        return await self._trace_page("wait", invocation_id, occurrence_id=occurrence_id, status=status, limit=limit, cursor=cursor)

    async def list_child_executions(self, invocation_id, *, occurrence_id=None, call_id=None, order="desc", limit=50, cursor=None):
        return await self._trace_page("child", invocation_id, occurrence_id=occurrence_id, call_id=call_id, order=order, limit=limit, cursor=cursor)

    async def get_execution_family(self, invocation_id):
        async def query():
            family = await self._persistence.execution_family(invocation_id)
            if family is None:
                raise HostNotFoundError('Unknown invocation.')
            return family
        return await self._perform(query)

    async def get_execution_value(self, invocation_id, kind, identity, field, *, pointer=None):
        """Resolve one scoped Input, Output, Error, Metrics or Call definition."""
        async def query():
            record = await self._persistence.trace_value(invocation_id, kind, identity, field, pointer=pointer)
            if record is None:
                raise HostNotFoundError("Unknown execution object.")
            return record
        return await self._perform(query)

    async def list_execution_stages(self, invocation_id, occurrence_id, *, limit=20, cursor=None):
        async def query():
            page = await self._persistence.execution_stages(invocation_id, occurrence_id, limit=limit, cursor=cursor)
            if page is None:
                raise HostNotFoundError('Unknown Node Occurrence.')
            return page
        return await self._perform(query)

    async def get_execution_stage_value(self, invocation_id, occurrence_id, event_id, field, *, pointer=None):
        async def query():
            value = await self._persistence.execution_stage_value(invocation_id, occurrence_id, event_id, field, pointer=pointer)
            if value is None:
                raise HostNotFoundError('Unknown execution stage.')
            return value
        return await self._perform(query)

    async def _run_session(self, sid):
        """Read admission metadata without restoring Core or reading business values."""
        session = await self._persistence.get_session(sid, view="accepted")
        if sid in self._runtime.owners:
            return dict(exists=True, available=False, code="child_session", message="A Child Session cannot be used for a Root execution.")
        page = await self._persistence.list_invocations(session_id=sid, include_planned=True, limit=1, view="accepted")
        if page.items and page.items[0]["parent_invocation_id"] is not None:
            return dict(exists=True, available=False, code="child_session", message="A Child Session cannot be used for a Root execution.")
        if session is None:
            return dict(exists=False, available=False, code="session_not_found", message="This Session does not exist in this Host's store.")
        latest = session.get("latest_invocation_id")
        inv = await self._persistence.get_invocation(latest, view="accepted") if latest else None
        if inv is not None and inv["status"] not in ("completed", "failed", "cancelled"):
            return dict(exists=True, available=False, code="session_busy", message="This Session still has an unfinished Invocation.")
        return dict(exists=True, available=True, code=None, message=None)

    async def get_workflow_run_spec(self, workflow_id, *, revision_id, session_id=None, entry_node_id=None):
        """Describe exact registered Entry inputs and optional Session admission eligibility."""
        async def query():
            ir = self._runtime.workflow_irs.get(revision_id) if self._runtime else None
            if ir is None or ir.workflow_id != workflow_id:
                graph = await self._persistence.workflow_graph(revision_id)
                if graph is None or graph["workflow_id"] != workflow_id:
                    raise HostNotFoundError("Unknown Workflow Revision.")
                entries, registered = [], False
            else:
                entries, registered = [], True
                chosen = entry_node_id or (ir.entry_node_ids[0] if len(ir.entry_node_ids) == 1 else None)
                if chosen is not None and chosen not in ir.entry_node_ids:
                    raise ValueError("Choose a declared Entry Node for this Revision.")
                for identity in ir.entry_node_ids:
                    node = ir.node(identity)
                    mapped = node.input_mapping is not None
                    entries.append(dict(node_id=identity, input_mode="raw_json" if mapped else "contract",
                        input_schema=json.loads(node.input_contract.schema) if identity == chosen and not mapped and node.input_contract is not None else None))
            session = await self._run_session(session_id) if session_id else None
            return dict(workflow_id=workflow_id, revision_id=revision_id, registered=registered,
                current_revision_id=self._runtime.workflow_ids.get(workflow_id), entries=entries,
                can_run=registered and (session is None or session["available"]),
                code="workflow_unregistered" if not registered else session["code"] if session else None,
                message="This exact Revision is not registered in this Host." if not registered else session["message"] if session else None,
                session=session)
        return await self._perform(query)

    async def execute(self, workflow_id, value, *, revision_id=None, session_id=None, session_context=None, entry_node_id=None):
        return await self._execute(workflow_id, value, revision_id=revision_id, session_id=session_id,
            session_context=session_context, entry_node_id=entry_node_id)

    async def execute_json(self, workflow_id, value, *, revision_id, entry_node_id, session_id=None, session_context=None, require_existing_session=False, require_new_session=False):
        """Decode JSON against the registered Entry contract before normal Root admission."""
        return await self._execute(workflow_id, value, revision_id=revision_id, session_id=session_id,
            session_context=session_context, entry_node_id=entry_node_id, json_input=True,
            require_existing_session=require_existing_session, require_new_session=require_new_session)

    async def _execute(self, workflow_id, value, *, revision_id=None, session_id=None, session_context=None, entry_node_id=None, json_input=False, require_existing_session=False, require_new_session=False):
        async def execute():
            if require_new_session and require_existing_session:
                raise ValueError("Choose either a new or an existing Session.")
            if self._runtime.residency.fault is not None:
                raise HostCapacityError("Host residency persistence failed; admission is stopped.") from self._runtime.residency.fault
            revision = revision_id or self._runtime.workflow_ids.get(workflow_id)
            snapshot = self._runtime.revisions.get(revision)
            if snapshot is None or snapshot.workflow_id != workflow_id:
                raise HostRunConflict("workflow_unregistered", "Workflow Revision is not registered for execution.")
            incoming = value
            if json_input:
                ir = self._runtime.workflow_irs[revision]
                if entry_node_id not in ir.entry_node_ids:
                    raise ValueError("Choose a declared Entry Node for this Revision.")
                node = ir.node(entry_node_id)
                if node.input_mapping is None and node.input_contract is not None:
                    contract = node.input_contract
                    if contract.annotation is None:
                        incoming = contract.validate(value)
                    else:
                        incoming = TypeAdapter(contract.annotation).validate_json(json.dumps(value, allow_nan=False), strict=True)
            sid = session_id or str(uuid4())
            item = self._runtime.residency.touch(sid)
            item.users += 1
            try:
                async with self._runtime.coordinated(sid):
                    # Generated UUIDs take the ordinary admission path. Explicit
                    # IDs must never silently reuse a warm or buffered Session.
                    if require_new_session and session_id:
                        exists = self._runtime.repository.state(sid).session is not None or sid in self._runtime.resident or sid in self._runtime.owners
                        if not exists:
                            exists = await self._persistence.get_session(sid, view="accepted") is not None
                        if not exists:
                            planned = await self._persistence.list_invocations(session_id=sid, include_planned=True, limit=1, view="accepted")
                            exists = bool(planned.items)
                        if exists:
                            raise HostRunConflict("session_exists", "Session ID already exists. Choose another ID or select Existing session.")
                    if require_existing_session:
                        eligibility = await self._run_session(sid)
                        if not eligibility["available"]:
                            raise HostRunConflict(eligibility["code"], eligibility["message"])
                        if session_context is not None:
                            raise HostRunConflict("session_context_existing", "Session context can only be set for a new Session.")
                    if sid in self._runtime.resident:
                        await self._runtime.residency.admit_resident(sid)
                    if require_new_session:
                        # A fresh generated UUID needs no existence preflight;
                        # explicit IDs were checked under the Session lock above.
                        await self._runtime.residency.reserve(sid)
                    else:
                        await self._runtime.prepare_session(sid)
                    previous = self._runtime.resident.get(sid)
                    old_children = tuple(child for child, owner in self._runtime.owners.items() if owner == sid)
                    submission = await self._runtime.app.asubmit_invoke(
                        revision, incoming, session_id=sid,
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
        if self._runtime is None:
            raise HostRunConflict("read_only", "Runtime operations require a writable Host.")
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
                pending_fork = self._runtime.fork_tasks.get(ref.invocation_id)
                if pending_fork is not None:
                    return _result_view(await asyncio.wait_for(asyncio.shield(pending_fork), timeout))
                if self._runtime.resident.get(ref.session_id) == ref:
                    return _result_view(await self._runtime.app.ajoin(ref, timeout=timeout))
                result = await self._stored_result(ref)
                if result.status in ("completed", "failed", "cancelled") or result.control_state == "paused" or result.waits:
                    return result
                raise HostError("Invocation is not active in this Host; use recover.")
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

    async def recover(self, execution, *, request_id: str | None = None, expected_version: int | None = None):
        """Recover the same Root graph, hydrating released values from Persistence as needed."""
        async def recover():
            async with self._execution(execution, load=True, touch=True) as ref:
                return _result_view(await self._runtime.activate(ref, force=True, request_id=request_id, expected_version=expected_version))
        return await self._perform(recover, control=True)

    async def cancel(self, execution, *, reason=None):
        async def cancel():
            async with self._execution(execution, load=True, touch=True) as ref:
                return _result_view(await self._runtime.app.acancel(ref, reason))
        return await self._perform(cancel, control=True)


def _consume(task):
    if not task.cancelled():
        task.exception()
