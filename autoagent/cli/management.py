"""Small adapters for live queries and explicit Checkpoint mutations."""

from pathlib import Path
from argparse import BooleanOptionalAction
import re
from urllib.parse import quote

from . import CliError
from .execution import command_name, local_error


def add_parsers(groups, workflows, invocations, checkpoints, common):
    input_help = 'Describe registered Entry input contracts and Session eligibility. Local mode opens project resources; does not submit an Invocation.'
    inputs = workflows.add_parser('inputs', parents=[common], allow_abbrev=False,
        help=input_help, description=input_help)
    inputs.add_argument('workflow_id')
    inputs.add_argument('--revision-id', help='Exact registered Revision; omit for the current Revision.')
    inputs.add_argument('--entry-node', help='Select an Entry to read its Schema; omit to list Entries.')
    inputs.add_argument('--session-id', help='Check whether this existing Session can accept another Invocation.')
    inputs.set_defaults(management_query=True)
    operations = groups.add_parser('operation', parents=[common], allow_abbrev=False,
        help='Query an existing Host operation receipt; never resubmit a command.')
    show = operations.add_subparsers(dest='command', required=True).add_parser('show', parents=[common],
        allow_abbrev=False, help='Read a temporary operation receipt from --server.')
    show.add_argument('operation_id')
    show.set_defaults(management_query=True)
    actions = invocations.add_parser('actions', parents=[common], allow_abbrev=False,
        help='Read currently allowed Host actions and reasons; performs no action.')
    actions.add_argument('invocation_id')
    actions.add_argument('--cursor', help='Read the next page of pending responses.')
    actions.add_argument('--target-invocation-id')
    actions.add_argument('--wait-id', help='Select a pending external Wait and include its response schema.')
    actions.set_defaults(management_query=True)
    fork = invocations.add_parser('fork', parents=[common], allow_abbrev=False,
        help='Branch the whole Root graph immediately after an exact Session Event; never alter the source.',
        description='Fork from a Root or Child Event. --plan checks replay requirements without creating a branch.')
    fork.add_argument('invocation_id')
    fork.add_argument('--sequence', required=True, type=int, help='Exact Session Event Sequence belonging to this Invocation.')
    fork.add_argument('--session-id', help='New Root Session ID; omit to generate one. Existing IDs are rejected.')
    fork.add_argument('--plan', action='store_true', help='Only validate and describe the cut and Calls requiring replay.')
    from .main import _wait_arguments
    _wait_arguments(fork)
    fork.set_defaults(management_query=True)
    for name in ('create', 'pin', 'unpin', 'delete'):
        parser = checkpoints.add_parser(name, parents=[common], allow_abbrev=False,
            help={'create': 'Capture the current graph or a historical Session Event position without executing it.',
                'pin': 'Protect a Checkpoint from automatic cleanup.',
                'unpin': 'Allow automatic cleanup of a Checkpoint.',
                'delete': 'Delete one Checkpoint; required recovery anchors remain protected.'}[name])
        parser.set_defaults(management_query=True)
        parser.add_argument('session_id' if name == 'create' else 'checkpoint_id')
        if name == 'create':
            parser.add_argument('--sequence', type=int, help='Capture immediately after this Session Event Sequence; omit for current state.')
            parser.add_argument('--pin', action=BooleanOptionalAction, default=True,
                help='Protect manual captures by default; --no-pin allows automatic retention and leaves existing Pins unchanged.')


async def _remote(client, options):
    if options.group == 'invocation' and options.command == 'fork':
        if options.plan:
            return await client.request('GET','/execution-fork-plan',params=dict(invocation_id=options.invocation_id,sequence=options.sequence))
        from uuid import uuid4
        session = options.session_id or str(uuid4())
        options._execution = dict(source_invocation_id=options.invocation_id,session_id=session)
        data = await client.request('POST','/execution-forks',body=dict(invocation_id=options.invocation_id,
            sequence=options.sequence,session_id=session))
        if data.get('accepted') is not True or not all(isinstance(data.get(k),str) and data[k]
                for k in ('invocation_id','session_id','selected_invocation_id','selected_session_id')):
            raise CliError('OUTCOME_UNKNOWN','Server did not confirm the Fork identity. Check the requested new Session before retrying.',exit_code=4)
        options._execution = dict(invocation_id=data['invocation_id'],session_id=data['session_id'],mutation_applied=True)
        if options.wait != 'none':
            import asyncio
            deadline = None if options.wait_timeout is None else asyncio.get_running_loop().time()+options.wait_timeout
            data['execution'] = await client.observe(data['invocation_id'],until=options.wait,deadline=deadline)
        return data
    if options.group == 'workflow':
        revision = options.revision_id
        if revision is None:
            listing = await client.request('GET', '/workflows', params={'workflow_id':options.workflow_id, 'limit':1})
            if not listing['items']:
                raise CliError('WORKFLOW_NOT_FOUND', 'Workflow is not registered: ' + options.workflow_id)
            revision = listing['items'][0]['revision_id']
        data = await client.request('GET', '/workflow-run-spec', params={key:value for key,value in dict(
            workflow_id=options.workflow_id, revision_id=revision, entry_node_id=options.entry_node,
            session_id=options.session_id).items() if value is not None})
        return _input_description(data, options)
    if options.group == 'operation':
        return await client.request('GET', '/execution-operations/' + quote(options.operation_id, safe=''))
    if options.group == 'invocation':
        from .history import _cursor, _pack_cursor, _scope
        scope = _scope(options)
        store = await client.request('GET', '/history-store')
        options._source['store_id'] = store['store_id']
        cursor = _cursor(options.cursor, options._source, scope)
        data = await client.request('GET', '/execution-controls', params={key: value for key, value in dict(
            invocation_id=options.invocation_id, cursor=cursor, target_invocation_id=options.target_invocation_id,
            wait_id=options.wait_id).items() if value is not None})
        data['next_cursor'] = _pack_cursor(data.get('next_cursor'), options._source, scope)
        data['actions'] = {name: entry for name, entry in data['actions'].items()
            if name in ('respond', 'pause', 'recover', 'cancel')}
        data.pop('endpoints', None)
        if options.wait_id and data['actions']['respond']['available']:
            spec = await client.request('GET', '/execution-action-spec', params={key: value for key, value in dict(
                invocation_id=options.invocation_id, action='respond', wait_id=options.wait_id,
                target_invocation_id=options.target_invocation_id).items() if value is not None})
            data['response_schema'] = spec['input_schema']
        return data
    if options.command == 'create':
        return await client.request('POST', '/session-checkpoints', body=dict(session_id=options.session_id, sequence=options.sequence, pinned=options.pin))
    path = '/checkpoints/' + quote(options.checkpoint_id, safe='')
    if options.command == 'delete':
        await client.request('DELETE', path)
        return dict(checkpoint_id=options.checkpoint_id, deleted=True)
    return await client.request('PUT', path + '/pin', body=dict(pinned=options.command == 'pin'))


async def run(options):
    if options.group == 'invocation' and options.command == 'fork' and not 1 <= options.sequence <= 9007199254740991:
        raise CliError('USAGE_ERROR','--sequence must be a positive safe integer.')
    if options.group == 'invocation' and options.command == 'fork' and options.plan and options.session_id is not None:
        raise CliError('USAGE_ERROR','--session-id is for creation; omit it when checking --plan.')
    for key in ('session_id', 'checkpoint_id', 'invocation_id', 'target_invocation_id', 'wait_id', 'workflow_id', 'revision_id', 'entry_node'):
        value = getattr(options, key, None)
        if value is not None and (not value.strip() or len(value) > 4096):
            raise CliError('USAGE_ERROR', f'{key} must be nonempty and at most 4096 characters.')
    if options.group == 'operation' and not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', options.operation_id):
        raise CliError('USAGE_ERROR', 'operation_id must contain 1–128 letters, digits, underscores or hyphens.')
    if options.group == 'operation' and not getattr(options, 'server_url', None):
        raise CliError('USAGE_ERROR', 'Operation receipts exist only in the running Host memory; use --server to query that Host. Local commands return their results directly.')
    if options.group == 'checkpoint':
        options._execution = {key: getattr(options, key) for key in ('session_id', 'checkpoint_id') if hasattr(options, key)}
    elif options.group == 'operation':
        options._execution = dict(operation_id=options.operation_id)
    elif options.group == 'invocation':
        options._execution = dict(invocation_id=options.invocation_id)
    source = None
    try:
        if getattr(options, 'server_url', None):
            from .remote import ServerClient
            async with ServerClient(options.server_url, timeout=options.request_timeout) as client:
                source = dict(kind='server', url=client.url)
                options._source = source
                data = await _remote(client, options)
            root = Path.cwd()
        else:
            from autoagent.host import AutoAgentHost
            from autoagent.host.project import Project
            project = Project.find(getattr(options, 'project', '.'))
            host = AutoAgentHost.from_project(project.root, env_file=getattr(options, 'env_file', None),
                use_env_file=not getattr(options, 'no_env_file', False))
            root = options._root = host.root
            source = options._source = dict(kind='project', root=str(root), name=project.name)
            async with host:
                if options.group == 'workflow':
                    revision = options.revision_id
                    if revision is None:
                        listing = await host.list_workflows(workflow_id=options.workflow_id, limit=1)
                        if not listing.items:
                            raise CliError('WORKFLOW_NOT_FOUND', 'Workflow is not registered: ' + options.workflow_id)
                        revision = listing.items[0]['revision_id']
                    data = _input_description(await host.get_workflow_run_spec(options.workflow_id,
                        revision_id=revision, entry_node_id=options.entry_node, session_id=options.session_id), options)
                elif options.group == 'invocation' and options.command == 'fork':
                    if options.plan:
                        data = await host.plan_fork(options.invocation_id, sequence=options.sequence)
                    else:
                        from uuid import uuid4
                        session = options.session_id or str(uuid4())
                        options._execution = dict(source_invocation_id=options.invocation_id,session_id=session)
                        data = await host.fork(options.invocation_id, sequence=options.sequence,session_id=session)
                        options._execution = dict(invocation_id=data['invocation_id'],session_id=data['session_id'],mutation_applied=True)
                        data['execution'] = await host.wait_execution(data['invocation_id'], until=options.wait,timeout=options.wait_timeout)
                elif options.group == 'invocation':
                    from .history import _cursor, _pack_cursor, _scope
                    source['store_id'] = (await host.get_history_identity())['store_id']
                    scope = _scope(options)
                    data = await host.get_execution_controls(options.invocation_id,
                        cursor=_cursor(options.cursor, source, scope),
                        target_invocation_id=options.target_invocation_id, wait_id=options.wait_id)
                    data['next_cursor'] = _pack_cursor(data.get('next_cursor'), source, scope)
                    data['actions'] = {name: action for name, action in data['actions'].items()
                        if name in ('respond', 'pause', 'recover', 'cancel')}
                    data.pop('endpoints', None)
                    if options.wait_id and data['actions']['respond']['available']:
                        spec = await host.get_execution_action_spec(options.invocation_id, 'respond',
                            target_invocation_id=options.target_invocation_id, wait_id=options.wait_id)
                        data['response_schema'] = spec['input_schema']
                elif options.command == 'create':
                    data = await host.create_checkpoint(options.session_id, sequence=options.sequence, pinned=options.pin)
                elif options.command == 'delete':
                    await host.delete_checkpoint(options.checkpoint_id)
                    data = dict(checkpoint_id=options.checkpoint_id, deleted=True)
                else:
                    data = await host.set_checkpoint_pinned(options.checkpoint_id, options.command == 'pin')
    except Exception as error:
        mapped = local_error(error)
        if mapped is error:
            raise
        raise mapped from error
    if options.group == 'checkpoint':
        options._execution.update(checkpoint_id=data['checkpoint_id'], mutation_applied=True)
    status = 0
    if options.group == 'invocation' and options.command == 'fork' and not options.plan:
        state = data.get('execution',{})
        status = 4 if state.get('observation') == 'timeout' else 1 if state.get('status') in ('failed','cancelled') else 0
    return root, dict(schema_version=1, command=command_name(options), ok=status == 0, source=source,
        view='live' if options.group in ('operation', 'invocation', 'workflow') else 'durable', data=data), status


def _input_description(data, options):
    selected = options.entry_node or (data['entries'][0]['node_id'] if len(data['entries']) == 1 else None)
    return dict(data, selected_entry_node_id=selected, entry_node_required=selected is None,
        input_note='Use the original Workflow JSON input when input_mode is raw_json; its mapped Operator schema is not the caller input schema.')
