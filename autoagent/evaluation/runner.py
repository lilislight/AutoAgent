"""Bounded Case workers using real Hosts; Scenario functions themselves are not durable."""

import asyncio
from contextlib import AsyncExitStack
from dataclasses import asdict, replace
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
import inspect
from pathlib import Path
import platform
import subprocess
from uuid import uuid4

from .context import ScenarioContext
from autoagent.host.execution_view import ExecutionView, observe_reads
from .models import CheckResult, EvaluationEnvironment, EvaluationError, RunOptions, digest, json_copy
from .report import save_run, summary, write_json


def now():
    return datetime.now(timezone.utc).isoformat()


def error_record(error, phase):
    return dict(phase=phase, type=type(error).__name__, message=str(error))


def environment_identity(root=None):
    try:
        release = version('autoagent')
    except PackageNotFoundError:
        release = 'source'
    result = dict(python=platform.python_version(), platform=platform.platform(), autoagent=release)
    if root is not None:
        from autoagent.host.project import source_files
        paths = list(source_files(Path(root)))
        manifest = Path(root) / 'auto-agent.toml'
        if manifest.is_file():
            paths.append(manifest)
        result['project_source_hash'] = digest({str(path.relative_to(root)): digest(path.read_text(encoding='utf-8'))
            for path in sorted(paths)})
        try:
            def git(*arguments):
                return subprocess.run(['git', '-C', str(root), *arguments], capture_output=True,
                    check=True, timeout=5).stdout
            result.update(git_commit=git('rev-parse', 'HEAD').decode().strip(),
                working_tree_diff_hash=digest(git('diff', 'HEAD', '--', '.').decode(errors='replace')))
        except (OSError, subprocess.SubprocessError):
            result['git_commit'] = None
    return result


async def run_trial(suite, case, trial, factory, directory, options, contract):
    directory.mkdir(parents=True, exist_ok=False)
    # Each Trial owns a fresh Host/database, even when workers run concurrently.
    environment = EvaluationEnvironment(replace(case), trial, str(directory),
        'sqlite+aiosqlite:///' + str(directory / 'runtime.db'))
    context = ScenarioContext(environment, options)
    result = dict(case_id=case.id, case_fingerprint=case.fingerprint, trial=trial,
        execution='not_started', verdict='ungraded', checks=[], errors=[], observations=[],
        roots=[], cleanup=dict(ok=True, errors=[]), database=str(directory / 'runtime.db'))
    stack = AsyncExitStack()
    evidence = None
    phase = 'setup'
    case_fingerprint = case.fingerprint
    def verify_contract():
        if suite.identity() != contract or context.case.fingerprint != case_fingerprint:
            raise EvaluationError('Evaluation rules or configuration changed during the Run; create a new Suite contract.')
    try:
        async with asyncio.timeout(options.timeout_seconds):
            verify_contract()
            if suite.fixture is not None:
                await stack.enter_async_context(suite.fixture(context))
            context._host = await stack.enter_async_context(factory(environment))
            from sqlalchemy.engine import make_url
            url = make_url(context._host.settings.resolved_database_url(context._host.root))
            result['history'] = dict(database_url=url.render_as_string(hide_password=True))
            result['database'] = url.database if url.drivername == 'sqlite+aiosqlite' else None
            phase = 'scenario'
            with observe_reads() as reads:
                context._read_scope = reads
                evidence = await suite.scenario(context)
            context._read_scope = None
            verify_contract()
            if evidence is None and context.observations:
                evidence = context.observations[-1]
            if evidence is not None and (not isinstance(evidence, ExecutionView) or evidence not in context.observations):
                raise TypeError('Scenario must return its ExecutionView observation, or None.')
            if not context._roots:
                raise ValueError('A business Scenario must execute a Workflow.')
            result['execution'] = 'completed'
            phase = 'evaluation'
            view = context.evaluator_context(evidence)
            for evaluator in suite.evaluators:
                try:
                    with observe_reads() as reads:
                        context._read_scope = reads
                        check = evaluator(view)
                        if inspect.isawaitable(check):
                            check = await check
                        verify_contract()
                        if not isinstance(check, CheckResult):
                            raise TypeError('Evaluator must return CheckResult.')
                        context.check(check)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    result['errors'].append(dict(error_record(error, 'evaluation'), evidence=json_copy(reads)))
    except TimeoutError as error:
        result['execution'] = 'timeout' if phase != 'evaluation' else 'completed'
        result['errors'].append(error_record(error, phase + '_timeout'))
    except asyncio.CancelledError:
        result['execution'] = 'interrupted'
        result['verdict'] = 'interrupted'
    except Exception as error:
        result['execution'] = 'error' if phase != 'evaluation' else 'completed'
        result['errors'].append(error_record(error, phase))
    finally:
        # Grade before cleanup: cancelling an expected Wait must not alter its verdict/evidence.
        try:
            async with asyncio.timeout(options.cleanup_timeout_seconds):
                if context._host is not None:
                    result['cleanup']['errors'].extend(await context._cleanup())
        except (Exception, asyncio.CancelledError) as error:
            result['cleanup']['errors'].append(error_record(error, 'cleanup'))
        # Fixture/Host closure still runs when cancellation settlement fails.
        try:
            async with asyncio.timeout(options.cleanup_timeout_seconds):
                await stack.aclose()
        except (Exception, asyncio.CancelledError) as error:
            result['cleanup']['errors'].append(error_record(error, 'close'))
        result['cleanup']['ok'] = not result['cleanup']['errors']
    result['checks'] = context._checks
    result['observations'] = [item.to_record() for item in context.observations]
    result['roots'] = [dict(invocation_id=ref.invocation_id, session_id=ref.session_id) for ref in context._roots.values()]
    if result['verdict'] != 'interrupted':
        result['verdict'] = 'error' if result['errors'] or not result['cleanup']['ok'] else (
            'failed' if any(check['passed'] is False for check in result['checks']) else
            'passed' if result['checks'] and all(check['passed'] is True for check in result['checks']) else 'ungraded')
    result['result_file'] = str(directory.relative_to(directory.parent.parent) / 'result.json')
    write_json(directory / 'result.json', result)
    return result


async def run_suite(suite, *, directory, host_factory=None, options=None, case_ids=(), run_id=None):
    """Execute a Suite and retain reports plus real Host history in a new Run directory."""
    options = options or RunOptions()
    factory = suite.host_factory or host_factory
    if factory is None:
        raise ValueError('Provide host_factory, or configure Suite.host_factory.')
    if isinstance(case_ids, str) or len(set(case_ids)) != len(case_ids) or set(case_ids) - {case.id for case in suite.cases}:
        raise ValueError('Case selection contains duplicates or unknown ids.')
    selected = [case for case in suite.cases if not case_ids or case.id in case_ids]
    directory = Path(directory).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=False)
    root = getattr(factory, 'project', None)
    report = dict(schema_version=2, kind='autoagent.evaluation.run', run_id=run_id or str(uuid4()),
        status='running', started_at=now(), completed_at=None, suite=suite.identity(),
        environment=environment_identity(root), options=asdict(options),
        conditions=dict(declared=json_copy(suite.conditions), metadata=json_copy(suite.metadata),
            runtime={key: environment_identity().get(key) for key in ('python', 'autoagent')},
            factory=json_copy(factory.evaluation_conditions()) if hasattr(factory, 'evaluation_conditions') else {}),
        cases={case.id: case.fingerprint for case in selected}, results=[])
    planned = len(selected) * options.repeats
    report['summary'] = summary([], planned)
    save_run(directory, report)
    jobs = iter((case, trial) for case in selected for trial in range(1, options.repeats + 1))

    async def worker():
        for case, trial in jobs:
            path = directory / 'trials' / f'{digest(case.id)[:24]}-{trial}'
            result = await run_trial(suite, case, trial, factory, path, options, report['suite'])
            report['results'].append(result)
            report['summary'] = summary(report['results'], planned)
            save_run(directory, report)
            if result['execution'] == 'interrupted':
                raise asyncio.CancelledError

    workers = [asyncio.create_task(worker()) for _ in range(min(options.concurrency, planned))]
    try:
        await asyncio.gather(*workers)
        report['status'] = 'completed'
    except BaseException:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        report['status'] = 'interrupted'
        raise
    finally:
        report['completed_at'] = now()
        report['summary'] = summary(report['results'], planned)
        report['ok'] = report['status'] == 'completed' and report['summary']['passed'] == planned
        save_run(directory, report)
    return report
