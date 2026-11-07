"""Bounded, source-scoped history queries for local read-only Host and HTTP."""

import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from . import CliError
from .execution import command_name, local_error

STATUSES = ("created", "running", "waiting", "paused", "settling", "completed", "failed", "cancelled")


def _page_arguments(parser):
    parser.add_argument("--limit", type=int, default=50, help="Page size, 1–200; default: 50. No implicit download of later pages.")
    parser.add_argument("--cursor", help="Opaque next_cursor from the same query and evidence source.")
    parser.add_argument("--all", action="store_true", help="Export every page to --output FILE, using bounded memory; live results are not a frozen snapshot.")


def _filters(parser, *, session=False):
    parser.add_argument("--search", default="", help="Match Session/Invocation IDs, fuzzy Workflow IDs, or Revision prefixes.")
    parser.add_argument("--workflow-id", action="append", default=[], help="Exact Workflow ID; repeat for multiple values.")
    parser.add_argument("--revision-id", action="append", default=[], help="Exact Revision ID; repeat for multiple values.")
    parser.add_argument("--status", action="append", choices=STATUSES, default=[])
    parser.add_argument("--started-after", type=_timestamp, metavar="ISO8601", help="Invocation start time, inclusive; timezone required.")
    parser.add_argument("--started-before", type=_timestamp, metavar="ISO8601", help="Invocation start time, inclusive; timezone required.")
    if session:
        parser.add_argument("--session-id")


def _timestamp(value):
    import argparse
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            raise ValueError("timezone required")
        delta = stamp.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
        result = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
        if not 0 <= result <= 9007199254740991:
            raise ValueError("timestamp is out of range")
        return result
    except (ValueError, OverflowError) as error:
        raise argparse.ArgumentTypeError("Use an ISO 8601 timestamp with timezone, e.g. 2026-10-18T09:00:00Z.") from error


def add_parsers(groups, workflow_commands, invocation_commands, common):
    def leaf(parent, name, help):
        result = parent.add_parser(name, parents=[common], allow_abbrev=False, help=help, description=help)
        result.set_defaults(history_query=True)
        return result
    catalog = leaf(workflow_commands, 'list', 'Browse the Host catalog; local mode opens project resources. Use --saved for read-only database inspection.')
    catalog.add_argument('--search', default='', help='Fuzzy Workflow ID or Revision prefix.')
    catalog.add_argument('--registered-only', action='store_true', help='Only Workflows registered in this Host.')
    catalog.add_argument('--saved', action='store_true', help='Local only: read saved Workflows without importing code or checking registration.')
    _page_arguments(catalog)
    session = groups.add_parser("session", parents=[common], allow_abbrev=False, help="Read persisted Session history without executing project code.")
    commands = session.add_subparsers(dest="command", required=True)
    listing = leaf(commands, "list", "List Sessions with matching Invocation history.")
    _filters(listing, session=True)
    _page_arguments(listing)
    descriptions = {"show": "Summarize Session activity and Invocation/Workflow counts without reading Context.",
        "invocations": "Page a Session's Invocations in ascending Event order; ordinals remain unchanged by filtering.",
        "workflows": "Page Workflow Revisions used by this Session.",
        "checkpoints": "Page Checkpoint positions, Pin state and retention metadata."}
    for name in ("show", "invocations", "workflows", "checkpoints"):
        selected = leaf(commands, name, descriptions[name])
        selected.add_argument("session_id")
        if name != "show":
            _page_arguments(selected)
        if name == "invocations":
            _filters(selected)
    listing = leaf(invocation_commands, "list", "List Root Invocations from persisted history.")
    _filters(listing, session=True)
    _page_arguments(listing)
    descriptions = {"show": "Summarize one Invocation, status counts and Event sequence range.",
        "family": "Read Root, ancestor and child navigation metadata.",
        "nodes": "Page Node occurrences; repeated Node runs have separate Occurrence IDs.",
        "calls": "Page Operator Calls, optionally within a Node occurrence.",
        "edges": "Page conditional Edge evaluations with EVENT_ID:RESULT_INDEX identities.",
        "waits": "Page durable Waits and their response availability.",
        "children": "Page child Invocations, optionally by Occurrence or Call.",
        "events": "Page Event headers in ascending Session sequence; bodies are read separately.",
        "stages": "Page a Node occurrence's mapping, aggregation, binding and routing stages."}
    for name in ("show", "family", "nodes", "calls", "edges", "waits", "children", "events", "stages"):
        selected = leaf(invocation_commands, name, descriptions[name])
        selected.add_argument("invocation_id")
        if name == 'show':
            selected.add_argument('--graph', action='store_true', help='Read one consistent persisted graph snapshot: Node/Edge states, counts and active routes; no Input/Output or replay.')
        if name not in ("show", "family"):
            _page_arguments(selected)
        if name == "nodes":
            selected.add_argument("--node-id")
            selected.add_argument("--status", choices=("ready", "running", "waiting", "completed", "failed", "skipped", "cancelled"))
        if name in ("calls", "edges", "waits", "children"):
            selected.add_argument("--occurrence-id")
        if name in ("calls", "waits"):
            selected.add_argument("--status", choices=("running", "completed", "failed", "lost", "cancelled") if name == "calls" else ("waiting", "resumed", "cancelled"))
        if name == "edges":
            selected.add_argument("--edge-id")
        if name == "children":
            selected.add_argument("--call-id")
            selected.add_argument("--order", choices=("asc", "desc"), default="desc")
        if name == "events":
            selected.add_argument("--after", type=int, default=0, help="Read after this Session Event sequence; sequences can have gaps within one Invocation.")
            selected.add_argument("--event-name", action="append", default=[])
        if name == "stages":
            selected.add_argument("occurrence_id")
    for name in ("node", "call", "edge", "wait", "event"):
        selected = leaf(invocation_commands, name, "Read one scoped " + name + " metadata record.")
        selected.add_argument("invocation_id")
        selected.add_argument("identity", help="Occurrence/Call/Wait/Event ID; Edge evaluation uses EVENT_ID:RESULT_INDEX.")
        if name == "event":
            selected.add_argument("--field", choices=("payload", "delta"), help="Explicitly read the Event body field; omitted means metadata only.")
            selected.add_argument("--path", help="RFC 6901 JSON Pointer within the selected field.")
    value = leaf(invocation_commands, "value", "Explicitly read one Input, Output, Error, Metrics, definition or Wait value.")
    value.add_argument("invocation_id")
    value.add_argument("--kind", choices=("invocation", "node", "call", "wait"), default="invocation")
    value.add_argument("--identity", help="Defaults to Invocation ID for kind=invocation; otherwise required.")
    value.add_argument("--field", required=True, choices=("input", "output", "error", "metrics", "definition", "request", "response", "cancel_reason"))
    value.add_argument("--path", help="Read only this JSON Pointer within the field, e.g. /messages/0/content.")
    state = leaf(invocation_commands, "state", "Inspect state after an exact persisted Event; local read-only inspection, without running project code.")
    state.add_argument("invocation_id")
    state.add_argument("--sequence", type=int, required=True, help="Exact Session Event sequence belonging to this Invocation; state is read after this Event.")
    state.add_argument("--context", choices=("session", "invocation"), help="Explicitly read historical Context instead of the small state summary.")
    state.add_argument("--path", help="RFC 6901 JSON Pointer within Context. Omitted reads the whole Context, subject to configured budgets.")
    stage = leaf(invocation_commands, "stage-value", "Read a selected mapping, aggregation, binding, routing or fault value.")
    for argument in ("invocation_id", "occurrence_id", "event_id"):
        stage.add_argument(argument)
    stage.add_argument("--field", required=True, choices=("mapped_input", "output", "patch", "conditions", "error"))
    stage.add_argument("--path")
    diagnose = leaf(invocation_commands, "diagnose", "Summarize saved failures, waits and child evidence without loading Input/Output.")
    diagnose.add_argument("invocation_id")
    diagnose.add_argument("--limit", type=int, default=20, help="Evidence records per category, 1–100; counts cover the whole selected subtree.")
    diff = leaf(workflow_commands, "diff", "Compare two saved Revisions of the same Workflow.")
    diff.add_argument("base_revision_id")
    diff.add_argument("target_revision_id")
    detail = diff.add_mutually_exclusive_group()
    detail.add_argument("--node-id", help="Read changed fields for this Node.")
    detail.add_argument("--edge-id", help="Read changed fields for this Edge.")
    detail.add_argument("--workflow", action="store_true", help="Read changed Workflow settings, excluding Node/Edge definitions.")
    _page_arguments(diff)
    revisions = leaf(workflow_commands, "revisions", "List persisted Workflow Revisions; does not import project code.")
    revisions.add_argument("workflow_id")
    revisions.add_argument("--prefix")
    _page_arguments(revisions)
    show = leaf(workflow_commands, "show", "Read a saved graph, or explicitly request its definition.")
    show.add_argument("workflow_id")
    show.add_argument("--revision-id", required=True)
    selection = show.add_mutually_exclusive_group()
    selection.add_argument("--node-id")
    selection.add_argument("--edge-id")
    selection.add_argument("--definition", action="store_true", help="Read the full saved definition, including contracts and hooks.")
    checkpoints = groups.add_parser("checkpoint", parents=[common], allow_abbrev=False, help="Inspect and manage Checkpoints; list them with session checkpoints.")
    checkpoint_commands = checkpoints.add_subparsers(dest="command", required=True)
    selected = leaf(checkpoint_commands, "show", "Read positions, pin state and deletion blockers.")
    selected.add_argument("checkpoint_id")
    from .management import add_parsers as add_management
    add_management(groups, workflow_commands, invocation_commands, checkpoint_commands, common)


def validate_query(options):
    if options.group == "invocation" and options.command == "state":
        if getattr(options, "server_url", None):
            raise CliError("USAGE_ERROR", "invocation state currently supports local read-only inspection only; omit --server and use --project to select its database.")
        if not 1 <= options.sequence <= 9007199254740991:
            raise CliError("USAGE_ERROR", "--sequence must be a positive Session Event sequence within the supported integer range.")
        if options.path is not None and options.context is None:
            raise CliError("USAGE_ERROR", "--path requires --context session or --context invocation.")
    if options.group == 'workflow' and options.command == 'list':
        if options.saved and (getattr(options, 'server_url', None) or options.registered_only):
            raise CliError('USAGE_ERROR', '--saved is local read-only inspection; omit --server and --registered-only.')
        if len(options.search) > 512:
            raise CliError('USAGE_ERROR', '--search must have at most 512 characters.')
    maximum = 100 if options.command == 'diagnose' else 200
    if hasattr(options, "limit") and not 1 <= options.limit <= maximum:
        raise CliError("USAGE_ERROR", f"--limit must be between 1 and {maximum}.")
    if getattr(options, 'all', False):
        if not getattr(options, 'output', None) or options.output == '-':
            raise CliError('USAGE_ERROR', '--all requires --output FILE; use --limit/--cursor for interactive inspection.')
        if getattr(options, 'cursor', None):
            raise CliError('USAGE_ERROR', '--all starts at the first page and cannot be combined with --cursor.')
    if options.group == 'workflow' and options.command == 'diff' and (options.node_id or options.edge_id or options.workflow):
        if getattr(options, 'all', False) or options.cursor:
            raise CliError('USAGE_ERROR', 'Object comparison has no pages; omit --all and --cursor.')
    for key in ("workflow_id", "revision_id", "event_name"):
        value = getattr(options, key, None)
        if isinstance(value, list):
            if len(value) > 64 or any(not item.strip() for item in value):
                raise CliError("USAGE_ERROR", f"Invalid {key} filter.")
            setattr(options, key, sorted(set(value)))
    if hasattr(options, "started_after") and options.started_after is not None and options.started_before is not None and options.started_after > options.started_before:
        raise CliError("USAGE_ERROR", "--started-after cannot be later than --started-before.")
    if not 0 <= getattr(options, "after", 0) <= 9007199254740991:
        raise CliError("USAGE_ERROR", "--after must be a nonnegative Session sequence within the supported integer range.")
    if getattr(options, "path", None) is not None:
        from autoagent.host.persistence.reading import pointer_tokens
        try:
            pointer_tokens(options.path)
        except ValueError as error:
            raise CliError("USAGE_ERROR", str(error)) from error
    if options.group == "invocation" and options.command == "value":
        if options.kind == "invocation":
            options.identity = options.identity or options.invocation_id
        elif options.identity is None:
            raise CliError("USAGE_ERROR", "--identity is required for Node, Call and Wait values.")
    if options.group == "invocation" and options.command == "event" and options.path is not None and options.field is None:
        raise CliError("USAGE_ERROR", "--path requires --field when reading an Event.")


def _scope(options):
    ignored = {"project", "server_url", "env_file", "no_env_file", "format", "output", "debug", "log_level", "request_timeout", "history_query", "cursor", "limit", "all"}
    return {key: value for key, value in vars(options).items() if key not in ignored and not key.startswith("_")}


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def _cursor(token, source, scope):
    if token is None:
        return None
    try:
        if len(token) > 32000:
            raise ValueError
        raw = json.loads(base64.b64decode(token + "=" * (-len(token) % 4), altchars=b"-_", validate=True))
        if raw.get("version") != 1 or raw.get("source") != _digest(source) or raw.get("scope") != _digest(scope):
            raise ValueError
        return raw["position"]
    except (ValueError, KeyError, TypeError, AttributeError) as error:
        raise CliError("INVALID_CURSOR", "Cursor is invalid or belongs to another query/database/Server.") from error


def _pack_cursor(position, source, scope):
    if position is None:
        return None
    return base64.urlsafe_b64encode(json.dumps(dict(version=1, source=_digest(source), scope=_digest(scope), position=position), separators=(",", ":")).encode()).decode().rstrip("=")


def _filters_values(options):
    return dict(search=options.search.strip().lower(), workflow_ids=options.workflow_id, revision_ids=options.revision_id,
        statuses=options.status, started_after_us=options.started_after, started_before_us=options.started_before)


async def _local_query(host, options, cursor):
    group, name = options.group, options.command
    page = dict(limit=options.limit, cursor=cursor) if hasattr(options, "cursor") and hasattr(options, "limit") else {}
    if group == "session":
        if name == "list":
            return await host.list_execution_sessions(**_filters_values(options), session_id=options.session_id, **page)
        if name == "show":
            return await host.get_session_information(options.session_id)
        if name == "invocations":
            return await host.list_session_history(options.session_id, **_filters_values(options), **page)
        if name == "workflows":
            return await host.list_session_workflows(options.session_id, **page)
        return await host.list_checkpoints(options.session_id, **page)
    if group == "checkpoint":
        return await host.get_checkpoint(options.checkpoint_id)
    if group == "workflow":
        if name == 'list':
            return await host.browse_workflows(search=options.search, registered_only=options.registered_only, **page)
        if name == 'diff':
            if options.node_id or options.edge_id or options.workflow:
                graph = await host.get_workflow_graph(options.base_revision_id)
                kind = 'node' if options.node_id else 'edge' if options.edge_id else 'workflow'
                return await host.get_revision_comparison_object(options.base_revision_id, options.target_revision_id,
                    kind, options.node_id or options.edge_id or graph['workflow_id'])
            return await host.compare_workflow_revisions(options.base_revision_id, options.target_revision_id, **page)
        if name == "revisions":
            return await host.list_workflow_revisions(options.workflow_id, prefix=options.prefix, **page)
        graph = await host.get_workflow_graph(options.revision_id)
        if graph["workflow_id"] != options.workflow_id:
            raise CliError("NOT_FOUND", "Revision does not belong to the selected Workflow.")
        if options.node_id or options.edge_id:
            return await host.get_workflow_object(options.revision_id, "node" if options.node_id else "edge", options.node_id or options.edge_id)
        return await host.get_workflow(options.workflow_id, revision_id=options.revision_id) if options.definition else graph
    if name == "list":
        return await host.list_executions(**_filters_values(options), session_id=options.session_id, **page)
    iid = options.invocation_id
    if name == "state":
        if options.context:
            return await host.read_execution_context(iid, sequence=options.sequence, scope=options.context, pointer=options.path)
        return await host.inspect_execution_state(iid, sequence=options.sequence)
    if name == 'diagnose':
        return await host.diagnose_execution(iid, limit=options.limit)
    if name == "show":
        return await host.get_execution_trace(iid) if options.graph else await host.get_invocation_summary(iid)
    if name == "family":
        return await host.get_execution_family(iid)
    if name == "nodes":
        return await host.list_node_executions(iid, options.node_id, status=options.status, **page)
    if name == "calls":
        return await host.list_operator_calls(iid, occurrence_id=options.occurrence_id, status=options.status, **page)
    if name == "edges":
        return await host.list_edge_evaluations(iid, options.edge_id, occurrence_id=options.occurrence_id, **page)
    if name == "waits":
        return await host.list_execution_waits(iid, occurrence_id=options.occurrence_id, status=options.status, **page)
    if name == "children":
        return await host.list_child_executions(iid, occurrence_id=options.occurrence_id, call_id=options.call_id, order=options.order, **page)
    if name == "events":
        return await host.list_execution_events(iid, after=options.after, names=options.event_name, **page)
    if name == "stages":
        return await host.list_execution_stages(iid, options.occurrence_id, **page)
    if name == "stage-value":
        return await host.get_execution_stage_value(iid, options.occurrence_id, options.event_id, options.field, pointer=options.path)
    if name == "value":
        return await host.get_execution_value(iid, options.kind, options.identity, options.field, pointer=options.path)
    if name == "event":
        return await host.get_execution_event(iid, options.identity, field=options.field, pointer=options.path)
    return await host.get_execution_record(iid, name, options.identity)


def _http_filters(options):
    values = _filters_values(options)
    return {"q": values["search"], "workflow_id": values["workflow_ids"], "revision_id": values["revision_ids"],
        "status": values["statuses"], "started_after_us": values["started_after_us"], "started_before_us": values["started_before_us"]}


async def _remote_query(client, options, cursor):
    from urllib.parse import quote
    group, name = options.group, options.command
    params = dict(limit=options.limit, cursor=cursor) if hasattr(options, "limit") else {}
    if group == "session":
        route = {"list": "/execution-sessions", "show": "/session-information", "invocations": "/session-history",
            "workflows": "/session-workflows", "checkpoints": "/session-checkpoints"}[name]
        if name in ("list", "invocations"):
            params.update(_http_filters(options))
        params["session_id"] = options.session_id
    elif group == "checkpoint":
        route = "/checkpoints/" + quote(options.checkpoint_id, safe="")
    elif group == "workflow":
        if name == 'list':
            route = '/workflow-catalog'
            params.update(q=options.search, registered_only=options.registered_only)
        elif name == 'diff':
            route = '/workflow-comparison'
            params.update(base_revision_id=options.base_revision_id, target_revision_id=options.target_revision_id)
            if options.node_id or options.edge_id or options.workflow:
                route = '/workflow-comparison-object'
                graph = await client.request('GET', '/workflow-graph', params={'revision_id': options.base_revision_id})
                params = dict(base_revision_id=options.base_revision_id, target_revision_id=options.target_revision_id,
                    kind='node' if options.node_id else 'edge' if options.edge_id else 'workflow',
                    identity=options.node_id or options.edge_id or graph['workflow_id'])
        elif name == "revisions":
            route = "/workflow-revisions"
            params.update(workflow_id=options.workflow_id, prefix=options.prefix)
        else:
            graph = await client.request("GET", "/workflow-graph", params={"revision_id": options.revision_id})
            if graph["workflow_id"] != options.workflow_id:
                raise CliError("NOT_FOUND", "Revision does not belong to the selected Workflow.")
            if options.node_id or options.edge_id:
                route = "/workflow-object"
                params.update(revision_id=options.revision_id, kind="node" if options.node_id else "edge", identity=options.node_id or options.edge_id)
            elif options.definition:
                route = "/workflow-revisions/" + quote(options.revision_id, safe="")
            else:
                return graph
    elif name == "list":
        route = "/executions"
        params.update(_http_filters(options), session_id=options.session_id)
    else:
        params["invocation_id"] = options.invocation_id
        if name == 'diagnose':
            route = '/invocation-diagnosis'
            params = dict(invocation_id=options.invocation_id, limit=options.limit)
        elif name in ("show", "family"):
            route = ('/execution-trace' if options.graph else '/invocation-summary') if name == 'show' else '/execution-family'
        elif name in ("nodes", "calls", "edges", "waits", "children"):
            route = "/execution-items"
            params["kind"] = {"nodes": "node", "calls": "call", "edges": "edge", "waits": "wait", "children": "child"}[name]
            for key in ("node_id", "edge_id", "occurrence_id", "call_id", "order", "status"):
                if hasattr(options, key):
                    params[key] = getattr(options, key)
        elif name in ("node", "call", "edge", "wait"):
            route = "/execution-record"
            params.update(kind=name, identity=options.identity)
        elif name == "events":
            route = "/invocation-events"
            params.update(after=options.after, name=options.event_name)
        elif name == "event":
            route = "/invocation-event"
            params.update(event_id=options.identity, field=options.field, pointer=options.path)
        elif name == "stages":
            route = "/execution-stages"
            params["occurrence_id"] = options.occurrence_id
        elif name == "stage-value":
            route = "/execution-stage-value"
            params.update(occurrence_id=options.occurrence_id, event_id=options.event_id, field=options.field, pointer=options.path)
        else:
            route = "/execution-value"
            params.update(kind=options.kind, identity=options.identity, field=options.field, pointer=options.path)
    result = await client.request("GET", route, params={key: value for key, value in params.items() if value is not None})
    return result


def _result(options, source, data):
    scope = _scope(options)
    if hasattr(data, "items") and not isinstance(data, dict):
        data = dict(items=list(data.items), next_cursor=data.next_cursor, frontier=data.frontier, total_count=data.total_count)
    if "items" in data and hasattr(options, "limit"):
        omitted = {'value_keys'}
        if options.group == 'session' and options.command == 'invocations':
            # Session order metadata does not check business-value availability.
            omitted.add('values')
        data["items"] = [{key: value for key, value in row.items() if key not in omitted} for row in data["items"]]
        data["next_cursor"] = _pack_cursor(data.get("next_cursor"), source, scope)
        data["returned_count"] = len(data["items"])
    else:
        data = {key: value for key, value in data.items() if key != "value_keys"}
    if options.group == 'invocation' and options.command == 'show' and options.graph:
        # The graph's route decision includes unconditional Edges; it is not
        # itself a condition evaluation. Keep that distinction explicit.
        data['edges'] = [dict(edge_id=row['edge_id'], selected=row['condition_result'],
            condition_evaluation_count=row['evaluations']) for row in data['edges']]
        data['invocation'] = {key:value for key,value in data['invocation'].items() if key != 'value_keys'}
    if source.get('registration_checked') is False:
        # Restrict normalization to metadata; never change user JSON values.
        if options.group == 'workflow' and options.command in ('list', 'revisions', 'show') or options.group == 'session' and options.command == 'workflows':
            for row in data.get('items', (data,)):
                if 'runnable' in row:
                    row['runnable'] = None
                row['registration_checked'] = False
    if options.group == 'workflow' and options.command == 'list':
        data['items'] = [dict(
            workflow_id=row['workflow_id'],
            workflow_version=row['workflow_version'],
            revision_id=row['current_revision_id'] or row['revision_id'],
            registration='unknown' if source['registration_checked'] is False else
                'registered' if row['current_revision_id'] else 'unregistered',
            matched_revision_id=row['matched_revision_id'],
        ) for row in data['items']]
    if options.command == 'diagnose':
        prefix = ['autoagent', '--server', source['url']] if source['kind'] == 'server' else ['autoagent', '--project', source['root']]
        suggestions = []
        for row in data['failed_nodes']['items']:
            suggestions.append(prefix + ['invocation', 'stages', row['invocation_id'], row['occurrence_id']])
        for row in data['failed_calls']['items']:
            suggestions.append(prefix + ['invocation', 'value', row['invocation_id'], '--kind', 'call',
                '--identity', row['call_id'], '--field', 'error'])
        for row in data['active_nodes']['items']:
            suggestions.append(prefix + ['invocation', 'node', row['invocation_id'], row['occurrence_id']])
        for row in data['pending_waits']['items']:
            suggestions.append(prefix + ['invocation', 'value', row['invocation_id'], '--kind', 'wait',
                '--identity', row['wait_id'], '--field', 'request'])
        if data['assessment'] == 'cancelled':
            suggestions.insert(0, prefix + ['invocation', 'value', data['invocation']['invocation_id'], '--field', 'cancel_reason'])
        data['next_queries'] = suggestions[:options.limit]
    live_catalog = options.group == 'workflow' and options.command == 'list' and source['registration_checked']
    return dict(schema_version=1, command=command_name(options), ok=True, source=source, view='live' if live_catalog else 'durable', data=data), 0


async def _read_query(options, source, query, root, stderr):
    cursor = _cursor(getattr(options, 'cursor', None), source, _scope(options))
    if not getattr(options, 'all', False):
        return root, *_result(options, source, await query(cursor))
    from .output import export_pages
    async def pages():
        position = None
        while True:
            document, _ = _result(options, source, await query(position))
            yield document
            token = document['data'].get('next_cursor')
            if token is None:
                break
            next_position = _cursor(token, source, _scope(options))
            if next_position == position:
                raise CliError('INVALID_CURSOR', 'The query did not advance its page cursor.', exit_code=3)
            position = next_position
    options._export_started = True
    await export_pages(pages(), options.output, root, options.format, stderr)
    return root, None, 0


async def run_query(options, *, stderr=None):
    validate_query(options)
    if getattr(options, "server_url", None):
        from .remote import ServerClient
        async with ServerClient(options.server_url, timeout=options.request_timeout) as client:
            identity = await client.request("GET", "/history-store")
            source = dict(kind="server", url=client.url, store_id=identity["store_id"],
                registration_checked=identity['registration_checked'])
            options._source = source
            return await _read_query(options, source, lambda cursor: _remote_query(client, options, cursor), Path.cwd(), stderr)
    try:
        from autoagent.host import AutoAgentHost
        from autoagent.host.project import Project
        project = Project.find(getattr(options, "project", "."))
        live_catalog = options.group == 'workflow' and options.command == 'list' and not options.saved
        host = AutoAgentHost.from_project(getattr(options, "project", "."),
            env_file=getattr(options, "env_file", None), use_env_file=not getattr(options, "no_env_file", False), read_only=not live_catalog)
        options._root = host.root
        async with host:
            identity = await host.get_history_identity()
            source = dict(kind="project", root=str(host.root), name=project.name, store_id=identity["store_id"],
                registration_checked=identity['registration_checked'])
            options._source = source
            return await _read_query(options, source, lambda cursor: _local_query(host, options, cursor), host.root, stderr)
    except FileNotFoundError as error:
        raise CliError("DATABASE_NOT_FOUND", str(error)) from error
    except Exception as error:
        mapped = local_error(error)
        if mapped is error:
            raise
        raise mapped from error


def render_query(data):
    if "items" not in data:
        return json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True)
    lines = [f"ITEMS {data['returned_count']}" + (f" / {data['total_count']}" if data.get("total_count") is not None else "")]
    identities = ("checkpoint_id", "wait_id", "call_id", "occurrence_id", "invocation_id", "revision_id", "workflow_revision_id", "session_id", "workflow_id")
    for row in data["items"]:
        if 'registration' in row and 'workflow_id' in row:
            lines.append(f"WORKFLOW {row['workflow_id']}  version={row['workflow_version']}  revision={row['revision_id']}  registration={row['registration']}")
            if row['matched_revision_id'] is not None and row['matched_revision_id'] != row['revision_id']:
                lines.append(f"  MATCHED_REVISION {row['matched_revision_id']}")
        elif 'kind' in row and 'fields' in row and 'id' in row:
            lines.append(f"{row['status'].upper()} {row['kind']} {row['id']}  fields={', '.join(row['fields']) or '-'}")
        elif "event_name" in row:
            lines.append(f"#{row['sequence']} {row['event_name']}  {row['event_id']}")
        else:
            key = next((key for key in identities if key in row), None)
            if "result_index" in row:
                label, identity = "EDGE_EVALUATION", f"{row['event_id']}:{row['result_index']}"
            else:
                label, identity = key.upper() if key else "RECORD", row[key] if key else ""
            detail = "  ".join(f"{field}={row[field]}" for field in ("workflow_id", "workflow_version", "workflow_revision_id", "node_id", "operator_id", "unit_index", "edge_id", "status", "control_state", "condition_result", "error_type", "error_code") if field != key and row.get(field) is not None)
            ordinal = f"#{row['ordinal']} " if 'opening_sequence' in row else ''
            lines.append(f"{ordinal}{label} {identity}  {detail}".rstrip())
        if 'opening_sequence' in row:
            lines.append(f"  OPENING_SEQUENCE {row['opening_sequence'] if row['opening_sequence'] is not None else 'unknown (imported history)'}  RECOVERY_COUNT {row['recovery_count']}")
        if row.get('registration_checked') is False and 'current_revision_id' not in row:
            lines.append('  REGISTRATION unknown (not checked in read-only mode)')
        timing = "  ".join(f"{field}={row[field]}" for field in ("sequence", "occurred_at_us", "started_at_us", "completed_at_us", "duration_ns", "execution_duration_ns", "queue_duration_ns", "last_activity_at_us") if row.get(field) is not None)
        if timing:
            lines.append("  " + timing)
        if row.get("values"):
            lines.append("  VALUES " + ", ".join(row["values"]))
    if data.get("next_cursor"):
        lines += ["", "NEXT_CURSOR " + data["next_cursor"]]
    return "\n".join(lines)
