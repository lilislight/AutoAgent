"""Evaluation CLI discovery, real execution, incremental reports and concise comparisons."""

from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from uuid import uuid4

from autoagent.cli.main import main


class EvaluationCliTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.module = 'evaluation_fixture_' + uuid4().hex
        self.evals = self.module + '_evals'
        (self.root / (self.module + '.py')).write_text('''
from typing_extensions import TypedDict
from autoagent import Node, Workflow
class Value(TypedDict):
    text: str
def echo(value: Value) -> Value:
    return value
def workflow():
    return Workflow('echo', nodes=[Node('echo', echo)])
''')
        (self.root / (self.evals + '.py')).write_text('''
from autoagent.evaluation import EvalSuite, EvalCase, StatusIs, OutputEquals
async def scenario(context):
    return await context.invoke('echo', context.case.input, entry_node_id='echo')
suite = EvalSuite('echo-suite', (EvalCase('hello', {'text':'hello'}, {'text':'hello'}),),
                  scenario, (StatusIs(), OutputEquals()), conditions={})
''')
        (self.root / 'auto-agent.toml').write_text(f'''schema_version=1
[project]
name='evaluation-cli'
[[workflows]]
entrypoint='{self.module}:workflow'
[[evaluations]]
entrypoint='{self.evals}:suite'
''')

    def tearDown(self):
        for name in (self.module, self.evals):
            sys.modules.pop(name, None)
        self.temporary.cleanup()

    def invoke(self, *arguments):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = main(['--project', str(self.root), 'eval', *arguments, '--format', 'json'])
        return code, json.loads(stdout.getvalue()), stderr.getvalue()

    def test_list_check_and_regular_project_check_are_lazy(self):
        """Suite discovery does not start Hosts and ordinary project commands never import Eval modules."""
        (self.root / (self.evals + '.py')).write_text('raise RuntimeError("Do not load evals")')
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = main(['--project', str(self.root), 'project', 'check', '--format', 'json'])
        self.assertEqual(code, 0, stdout.getvalue())
        self.assertNotIn(self.evals, sys.modules)

    def test_list_check_run_show_compare_and_output_file(self):
        """The complete CLI round trip retains independently queryable history and comparable Cases."""
        for action in (('list',), ('check',), ('check', 'echo-suite')):
            code, document, stderr = self.invoke(*action)
            self.assertEqual(code, 0, (document, stderr))
        code, baseline, stderr = self.invoke('run', 'echo-suite', '--run-dir', 'baseline', '--repeats', '2', '--concurrency', '2')
        self.assertEqual(code, 0, (baseline, stderr))
        self.assertEqual(baseline['data']['summary']['passed'], 2)
        code, candidate, _ = self.invoke('run', 'echo-suite', '--run-dir', 'candidate')
        self.assertEqual(code, 0, candidate)
        code, shown, _ = self.invoke('show', str(self.root / 'baseline'))
        self.assertEqual(shown['data']['results'], baseline['data']['results'])
        code, comparison, _ = self.invoke('compare', str(self.root / 'baseline'), str(self.root / 'candidate'))
        self.assertEqual(code, 0, comparison)
        self.assertEqual(comparison['data']['cases'][0]['outcome'], 'unchanged_passed')
        self.assertTrue(Path(baseline['data']['results'][0]['database']).exists())
        output = self.root / 'export.json'
        self.assertEqual(main(['--project', str(self.root), 'eval', 'show', str(self.root / 'baseline'), '--output', str(output)]), 0)
        self.assertEqual(json.loads(output.read_text())['command'], 'eval.show')
        # Expanded CLI exports remain usable without the source manifest or Trial files.
        moved = self.root / 'moved' / 'portable.json'
        moved.parent.mkdir()
        output.replace(moved)
        code, portable, _ = self.invoke('show', str(moved))
        self.assertEqual(code, 0, portable)
        self.assertEqual(portable['data']['results'], shown['data']['results'])
        code, compared, _ = self.invoke('compare', str(moved), str(self.root / 'candidate'))
        self.assertEqual(code, 0, compared)
        self.assertEqual(compared['data']['cases'][0]['outcome'], 'unchanged_passed')

    def test_case_selection_usage_errors_and_no_directory_overwrite(self):
        """Unknown Cases, invalid concurrency and existing Run directories fail without overwriting data."""
        code, document, _ = self.invoke('run', 'echo-suite', '--case', 'absent')
        self.assertEqual(code, 2, document)
        code, document, _ = self.invoke('run', 'echo-suite', '--concurrency', '0')
        self.assertEqual(code, 2, document)
        code, document, _ = self.invoke('check', 'absent')
        self.assertEqual(document['error']['code'], 'SUITE_NOT_FOUND')
        directory = self.root / 'existing'
        directory.mkdir()
        (directory / 'sentinel').write_text('keep')
        code, document, _ = self.invoke('run', 'echo-suite', '--run-dir', str(directory))
        self.assertEqual(document['error']['code'], 'RUN_EXISTS')
        self.assertEqual((directory / 'sentinel').read_text(), 'keep')

    def test_subprocess_help_and_machine_output(self):
        """Installed-style CLI imports the new command and returns parseable JSON without logs on stdout."""
        completed = subprocess.run([sys.executable, '-m', 'autoagent', '--project', str(self.root),
            'eval', 'list', '--format', 'json'], capture_output=True, text=True, timeout=15)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(json.loads(completed.stdout)['data']['suites'][0]['id'], 'echo-suite')
