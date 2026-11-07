"""Local Host execution and shared command input/result contracts."""

import asyncio
import json
import logging
import math
import re
from pathlib import Path
import sys
from uuid import uuid4

from . import CliError


def command_name(options):
    return f"{options.group}.{options.command}" if getattr(options, "command", None) else options.group


def validate_options(options):
    remote = getattr(options, "server_url", None)
    if remote is not None and not remote.strip():
        raise CliError("USAGE_ERROR", "--server cannot be empty.")
    if hasattr(options, "request_timeout") and not remote:
        raise CliError("USAGE_ERROR", "--request-timeout requires --server; it controls remote HTTP requests.")
    if remote and (hasattr(options, "project") or hasattr(options, "env_file") or getattr(options, "no_env_file", False)):
        raise CliError("USAGE_ERROR", "--server cannot be combined with --project or project environment-file options.")
    if remote and options.group not in ("invoke", "invocation", "memory") and not (getattr(options, "history_query", False) or getattr(options, 'management_query', False)):
        raise CliError("USAGE_ERROR", "--server is supported by execution and persisted-history commands.")
    if options.group == "server" and hasattr(options, "output"):
        raise CliError("USAGE_ERROR", "server writes its ready report to stdout; --output is not supported.")
    if options.group == 'memory':
        if not 0 <= options.history <= 604800:
            raise CliError('USAGE_ERROR', '--history must be between 0 and 604800.')
        if not 2 <= options.points <= 2000:
            raise CliError('USAGE_ERROR', '--points must be between 2 and 2000.')
        if options.window_seconds is not None and (not math.isfinite(options.window_seconds) or not 0 < options.window_seconds <= 604800):
            raise CliError('USAGE_ERROR', '--window-seconds must be positive and at most 604800.')
    for key in ("request_timeout", "wait_timeout"):
        value = getattr(options, key, None)
        if value is not None and (not math.isfinite(value) or value < 0 or key == "request_timeout" and value == 0):
            raise CliError("USAGE_ERROR", f"--{key.replace('_', '-')} must be finite and {'positive' if key == 'request_timeout' else 'nonnegative'}.")
    if getattr(options, "wait", None) == "none" and not remote:
        raise CliError("USAGE_ERROR", "--wait none requires --server; a local Host stops when the CLI exits.")
    if options.group == "invoke" and options.session_mode == "existing" and not options.session_id:
        raise CliError("USAGE_ERROR", "--session-mode existing requires --session-id.")
    if getattr(options, "request_id", None) is not None and not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", options.request_id):
        raise CliError("USAGE_ERROR", "--request-id must contain 1–128 letters, digits, underscores or hyphens.")
    if getattr(options, "expected_version", None) is not None and not 0 <= options.expected_version <= 9007199254740991:
        raise CliError("USAGE_ERROR", "--expected-version must be a nonnegative safe integer.")
    if options.group in ("invoke", "invocation"):
        for key, limit in (("workflow_id", 512), ("session_id", 512), ("revision_id", 4096),
                           ("entry_node", 4096), ("invocation_id", 4096), ("wait_id", 4096), ("target_invocation_id", 4096)):
            value = getattr(options, key, None)
            if isinstance(value, list):
                continue
            if value is not None and (not value.strip() or len(value) > limit):
                raise CliError("USAGE_ERROR", f"{key} must be nonempty and at most {limit} characters.")
        if getattr(options, "reason", None) is not None and len(options.reason) > 4096:
            raise CliError("USAGE_ERROR", "--reason must be at most 4096 characters.")


def read_input(options):
    try:
        if getattr(options, "input_file", None) is not None:
            text = sys.stdin.read() if options.input_file == "-" else Path(options.input_file).expanduser().read_text(encoding="utf-8")
        else:
            text = options.input_json
        def invalid(value):
            raise ValueError(f"Non-finite JSON number: {value}")
        return json.loads(text, parse_constant=invalid)
    except (OSError, UnicodeError, ValueError) as error:
        raise CliError("INVALID_JSON", str(error)) from error


def document(options, source, state):
    status = 4 if state.get("observation") in ("timeout", "interrupted", "outcome_unknown") else 1 if state.get("status") in ("failed", "cancelled") else 0
    return {"schema_version": 1, "command": command_name(options), "ok": status == 0,
            "source": source, "data": state}, status


async def failure_details(state, read_error, diagnose):
    """Best-effort bounded diagnostics never change an already observed outcome."""
    if state.get("status") != "failed":
        return
    try:
        error = await read_error()
        if error.get("available") and isinstance(error.get("value"), str):
            state["error_message"] = error["value"][:1200]
        evidence = await diagnose()
        state["failed_nodes"] = [{key: row[key] for key in ("node_id", "occurrence_id", "error_type", "error_code") if key in row}
                                 for row in evidence["failed_nodes"]["items"]]
    except Exception:
        logging.getLogger(__name__).debug("Could not read failure details; execution outcome is preserved.", exc_info=True)


def action_options(options, controls, value=None):
    action = options.command
    request = dict(request_id=getattr(options, "request_id", None) or uuid4().hex)
    options._request_id = request["request_id"]
    if action == "recover":
        request["expected_version"] = options.expected_version if options.expected_version is not None else controls["expected_version"]
    elif action == "cancel":
        request["reason"] = options.reason
    elif action == "respond":
        request.update(wait_id=options.wait_id, target_invocation_id=options.target_invocation_id or options.invocation_id, payload=value)
    return request


def local_error(error):
    from pydantic import ValidationError
    from autoagent import RuntimeTransitionError, WorkflowCompileError
    from autoagent.host import HostError, HostCapacityError, HostNotFoundError, HostRunConflict, ProjectError
    from autoagent.host.persistence.models import ConflictError

    if isinstance(error, ImportError):
        return CliError("DEPENDENCY_MISSING", str(error), exit_code=3)
    if isinstance(error, ValidationError):
        return CliError("INVALID_INPUT", "Input validation failed.", details={"errors": [
            dict(path=list(item["loc"]), code=item["type"], message=item["msg"])
            for item in error.errors(include_url=False, include_input=False, include_context=False)]})
    if isinstance(error, ConflictError) and "writer owns" in str(error):
        return CliError("STORE_BUSY", "Another Host owns this database. Use --server to submit to the running Server.", exit_code=3)
    if isinstance(error, WorkflowCompileError):
        return CliError("COMPILE_FAILED", str(error), exit_code=1)
    if isinstance(error, HostNotFoundError):
        return CliError('NOT_FOUND', str(error))
    if isinstance(error, ProjectError):
        return CliError('PROJECT_INVALID', str(error))
    if isinstance(error, (HostRunConflict, RuntimeTransitionError)):
        return CliError(getattr(error, "code", "ACTION_UNAVAILABLE"), str(error))
    if isinstance(error, (ValueError, TypeError)):
        return CliError("INVALID_REQUEST", str(error))
    if isinstance(error, HostCapacityError):
        return CliError("HOST_BUSY", str(error), exit_code=3)
    if isinstance(error, HostError):
        return CliError('HOST_UNAVAILABLE', str(error), exit_code=3)
    return error


async def run_local(options, value):
    try:
        from autoagent.host import AutoAgentHost
        from autoagent.host.project import Project, import_scope
    except ModuleNotFoundError as error:
        raise CliError("DEPENDENCY_MISSING", "Install autoagent[cli] to run local Workflows.", exit_code=3) from error
    try:
        with import_scope(Path.cwd()):
            project = Project.find(getattr(options, "project", "."))
            host = AutoAgentHost.from_project(getattr(options, "project", "."),
                env_file=getattr(options, "env_file", None), use_env_file=not getattr(options, "no_env_file", False))
            options._root = host.root
            source = dict(kind="project", root=str(host.root), name=project.name)
            async with host:
                if options.group == "invoke":
                    revision = options.revision_id
                    if revision is None:
                        workflows = await host.list_workflows(workflow_id=options.workflow_id, limit=1)
                        if not workflows.items:
                            raise CliError("WORKFLOW_NOT_FOUND", f"Workflow is not registered: {options.workflow_id}")
                        revision = workflows.items[0]["revision_id"]
                    ref = await host.execute_json(options.workflow_id, value, revision_id=revision,
                        entry_node_id=options.entry_node, session_id=options.session_id,
                        require_new_session=options.session_mode == "new", require_existing_session=options.session_mode == "existing")
                    iid = ref.invocation_id
                    options._execution = dict(invocation_id=iid, session_id=ref.session_id, revision_id=ref.workflow_revision_id)
                    state = await host.wait_execution(iid, until=options.wait, timeout=options.wait_timeout)
                else:
                    controls = await host.get_execution_controls(options.invocation_id)
                    iid = controls["root_invocation_id"]
                    options._execution = dict(invocation_id=iid, session_id=controls["root_session_id"])
                    kwargs = action_options(options, controls, value)
                    if "expected_version" in kwargs:
                        options._execution["submitted_expected_version"] = kwargs["expected_version"]
                    if options.command != "recover":
                        kwargs.pop("request_id")
                    try:
                        async with asyncio.timeout(options.wait_timeout):
                            await host.perform_execution_action(options.invocation_id, options.command, **kwargs)
                            state = await host.wait_execution(iid, until=options.wait,
                                timeout=None)  # the one command deadline includes the action itself
                    except TimeoutError:
                        state = dict(await host.get_execution_state(iid), observation="timeout")
                    state["request_id"] = options._request_id
                    if "expected_version" in kwargs:
                        state["submitted_expected_version"] = kwargs["expected_version"]
                await failure_details(state,
                    lambda: host.get_execution_value(iid, "invocation", iid, "error", pointer="/message"),
                    lambda: host.diagnose_execution(iid, limit=3))
            # Report the observed boundary, not a fictitious background local task.
            state.update(host_state="closed", execution_continues=False)
            return host.root, *document(options, source, state)
    except Exception as error:
        mapped = local_error(error)
        if mapped is error:
            raise
        raise mapped from error
