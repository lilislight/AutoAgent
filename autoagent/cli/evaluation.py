"""Local Evaluation CLI with existing output and error conventions."""

from pathlib import Path
from uuid import uuid4

from . import CliError


def add_parsers(groups, common):
    evaluation = groups.add_parser('eval', parents=[common], allow_abbrev=False,
        help='Check business behavior and compare Workflow revisions using project Evaluation Suites.')
    commands = evaluation.add_subparsers(dest='command')
    for name in ('list', 'check', 'run', 'show', 'compare'):
        command = commands.add_parser(name, parents=[common], allow_abbrev=False)
        command.set_defaults(evaluation_query=True)
        if name == 'check':
            command.add_argument('suite_id', nargs='?', help='Check all Suites when omitted; never execute business code.')
        elif name == 'run':
            command.add_argument('suite_id')
            command.add_argument('--case', dest='case_ids', action='append', default=[], help='Exact Case id; repeat to select more Cases.')
            command.add_argument('--repeats', type=int, default=1)
            command.add_argument('--concurrency', type=int, default=1)
            command.add_argument('--timeout', type=float, default=60, help='Per-Trial setup, Scenario and assessment timeout in seconds.')
            command.add_argument('--cleanup-timeout', type=float, default=10)
            command.add_argument('--run-dir', help='New directory for reports and Trial databases; default: .autoagent/evaluations/RUN_ID.')
        elif name == 'show':
            command.add_argument('run', help='Run id, Run directory or report.json path.')
        elif name == 'compare':
            command.add_argument('baseline')
            command.add_argument('candidate')


def resolve_run(value, root):
    selected = Path(value).expanduser()
    if selected.is_absolute():
        return selected
    direct = root / selected
    return direct if direct.exists() or len(selected.parts) > 1 else root / '.autoagent' / 'evaluations' / selected


async def run(options):
    from autoagent.evaluation import RunOptions, compare_runs, load_report, run_suite
    from autoagent.evaluation.models import EvaluationError
    from autoagent.evaluation.project import ProjectHostFactory, load_suites
    from autoagent.host.project import Project, import_scope, load_project_environment

    try:
        project = Project.find(getattr(options, 'project', '.'))
        source = dict(kind='project', name=project.name, root=str(project.root))
        if options.command == 'show':
            data = load_report(resolve_run(options.run, project.root))
            ok = data.get('ok', False)
        elif options.command == 'compare':
            data = compare_runs(resolve_run(options.baseline, project.root), resolve_run(options.candidate, project.root))
            ok = data['ok']
        else:
            environment = load_project_environment(project.root, env_file=getattr(options, 'env_file', None),
                use_env_file=not getattr(options, 'no_env_file', False))
            with import_scope(Path.cwd()):
                suites = load_suites(project, environment)
            chosen = [suite for suite in suites if getattr(options, 'suite_id', None) in (None, suite.id)]
            if getattr(options, 'suite_id', None) is not None and not chosen:
                raise CliError('SUITE_NOT_FOUND', f'Unknown Evaluation Suite: {options.suite_id}')
            if options.command == 'list':
                data = dict(suites=[dict(id=suite.id, version=suite.version,
                    cases=[dict(id=case.id, tags=list(case.tags)) for case in suite.cases]) for suite in suites])
                ok = True
            elif options.command == 'check':
                report = project.check(environment)
                data = dict(suites=[dict(suite.identity(), case_count=len(suite.cases)) for suite in chosen],
                    workflows=report.to_record())
                ok = bool(chosen) and report.ok
            else:
                config = RunOptions(concurrency=options.concurrency, repeats=options.repeats,
                    timeout_seconds=options.timeout, cleanup_timeout_seconds=options.cleanup_timeout)
                run_id = str(uuid4())
                directory = Path(options.run_dir).expanduser() if options.run_dir else Path('.autoagent/evaluations') / run_id
                if not directory.is_absolute():
                    directory = project.root / directory
                with import_scope(Path.cwd()):
                    data = await run_suite(chosen[0], directory=directory,
                        host_factory=ProjectHostFactory(str(project.root), environment),
                        options=config, case_ids=options.case_ids, run_id=run_id)
                data['report_path'] = str(directory.resolve() / 'report.json')
                ok = data['ok']
        document = dict(schema_version=1, command='eval.' + options.command, ok=bool(ok), source=source, data=data)
        return project.root, document, 0 if ok else 1
    except CliError:
        raise
    except (ValueError, EvaluationError) as error:
        raise CliError('EVALUATION_INVALID', str(error)) from error
    except FileExistsError as error:
        raise CliError('RUN_EXISTS', 'Run directory already exists; choose a new --run-dir.') from error


def render(data, command):
    if command == 'eval.list':
        lines = []
        for suite in data['suites']:
            lines.append(f"SUITE {suite['id']}  version={suite['version']}  cases={len(suite['cases'])}")
            lines.extend(f"  CASE {case['id']}" for case in suite['cases'])
        return '\n'.join(lines) if lines else 'No Evaluation Suites declared. Add [[evaluations]] entrypoints to auto-agent.toml.'
    if command == 'eval.check':
        return '\n'.join([f"SUITE {suite['id']}  cases={suite['case_count']}" for suite in data['suites']] +
            [f"WORKFLOWS {'passed' if data['workflows']['ok'] else 'failed'}"])
    if command == 'eval.compare':
        lines = [f"COMPARE {data['baseline_run_id']} -> {data['candidate_run_id']}"]
        lines.extend(f"CASE {case['case_id']}  {case['outcome']}  baseline={case['baseline']}  candidate={case['candidate']}" for case in data['cases'])
        if data['environment_changed'] or data['metadata_changed'] or data['host_factory_changed']:
            lines.append('CONTEXT CHANGED: inspect environment, fixture bindings and Suite metadata before attributing differences to Workflow code.')
        return '\n'.join(lines)
    lines = [f"RUN {data['run_id']}  {data['status']}",
        'RESULTS ' + '  '.join(f'{key}={value}' for key, value in data['summary'].items())]
    if data.get('report_path'):
        lines.append('REPORT ' + data['report_path'])
    for result in data['results']:
        lines.append(f"CASE {result['case_id']}  trial={result['trial']}  {result['verdict']}")
        for check in result['checks']:
            if check['passed'] is False:
                lines.append(f"  FAILED {check['name']}: {check['reason']}")
        for error in result['errors'] + result['cleanup']['errors']:
            lines.append(f"  ERROR {error.get('phase', 'cleanup')} {error['type']}: {error['message']}")
        if result['verdict'] != 'passed':
            for root in result['roots']:
                lines.append('  INVOCATION ' + root['invocation_id'])
            lines.append('  HISTORY ' + str(result.get('history', {}).get('database_url', result['database'])))
    return '\n'.join(lines)
