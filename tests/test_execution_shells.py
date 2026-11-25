"""Default Shell discovery and explicit interpreter selection for command Tools."""

import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from autoagent.agent import (LLMToolCall, LocalExecutionService, ToolBusinessError,
    ToolContext, ToolPolicy, execution_tools)
from autoagent.agent.execution.backend import ExecutionCapabilities
from autoagent.agent.execution.local import _windows_process_options


class ShellSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.bin = self.root / 'program files'
        self.bin.mkdir()

    def executable(self, name):
        path = self.bin / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b'fixture, never executed')
        path.chmod(0o755)
        return path

    def service(self, *, windows=True, environment=None, backend=None):
        with patch('autoagent.agent.execution.service._is_windows', return_value=windows):
            service = LocalExecutionService(self.root,
                environment={'PATH': str(self.bin), 'SYSTEMROOT': '', 'WINDIR': '', **(environment or {})},
                backend=backend)
        self.addAsyncCleanup(service.aclose)
        return service

    def call(self, **arguments):
        return LLMToolCall(name='execute_command', call_id='shell', arguments=json.dumps(
            {'mode': 'shell', 'script': "Write-Output 'hello'", **arguments}))

    async def test_windows_prefers_powershell_seven_and_advertises_its_path(self):
        """When both interpreters exist, the default and advertised contract select PowerShell 7."""
        preferred = self.executable('pwsh.exe')
        self.executable('powershell.exe')
        service = self.service()
        self.assertEqual(service.default_shell, 'pwsh')
        tools = execution_tools(service, context=ToolContext(session_id='shell'))
        definition = next(tool for tool in tools.definitions if tool.name == 'execute_command')
        self.assertIn('PowerShell 7+', definition.parameters['properties']['shell']['description'])
        self.assertIn(str(preferred), definition.parameters['properties']['shell']['description'])
        spec, _, _ = service._plan({'mode': 'shell', 'script': 'Write-Output hello'})
        self.assertEqual(spec.argv[0], str(preferred))

    async def test_windows_falls_back_and_policy_sees_the_actual_shell(self):
        """The legacy fallback is advertised and checked against rules as powershell before any process starts."""
        legacy = self.executable('powershell.exe')
        service = self.service()
        policy = ToolPolicy(rules=[{'when': {'field': 'args.shell', 'op': 'equals', 'value': 'powershell'},
            'decision': 'deny'}])
        tools = execution_tools(service, context=ToolContext(session_id='shell'), policy=policy)
        self.assertEqual(service.default_shell, 'powershell')
        self.assertIn('Windows PowerShell 5.1', service.shell_description)
        self.assertIn(str(legacy), service.shell_description)
        check = await tools.apreflight(self.call())
        self.assertIsNone(check.result)
        self.assertEqual(check.decision.action, 'deny')
        self.assertEqual(service.manager.records(), ())

    async def test_legacy_shell_is_found_in_systemroot_without_a_path_entry(self):
        """Windows' built-in interpreter remains usable when its installation directory is absent from PATH."""
        system = self.root / 'Windows'
        legacy = system / 'System32' / 'WindowsPowerShell' / 'v1.0' / 'powershell.exe'
        legacy.parent.mkdir(parents=True)
        legacy.write_bytes(b'fixture, never executed')
        legacy.chmod(0o755)
        service = self.service(environment={'SystemRoot': str(system), 'Path': str(self.bin)})
        self.assertNotIn('Path', service.environment)
        self.assertEqual(service.environment['SYSTEMROOT'], str(system))
        self.assertEqual(service.default_shell, 'powershell')
        spec, _, _ = service._plan({'mode': 'shell', 'script': 'Write-Output hello'})
        self.assertEqual(spec.argv[0], str(legacy))

    async def test_explicit_selection_never_silently_changes_interpreter(self):
        """A request for pwsh fails when missing; a request for legacy PowerShell does not upgrade to pwsh."""
        legacy = self.executable('powershell.exe')
        service = self.service()
        with self.assertRaises(ToolBusinessError) as failure:
            service._plan({'mode': 'shell', 'shell': 'pwsh', 'script': 'Write-Output hello'})
        self.assertEqual(failure.exception.code, 'program_unavailable')
        self.executable('pwsh.exe')
        self.assertEqual(service.default_shell, 'pwsh')
        spec, _, _ = service._plan({'mode': 'shell', 'shell': 'powershell', 'script': 'Write-Output hello'})
        self.assertEqual(spec.argv[0], str(legacy))

    async def test_missing_interpreters_fail_without_creating_logs_or_processes(self):
        """An unavailable default returns the normal program error without launching or creating requested artifacts."""
        service = self.service()
        tools = execution_tools(service, context=ToolContext(session_id='shell'))
        result = await tools.ainvoke(self.call(log_path='logs/run.txt'))
        self.assertEqual(result.structured_content['error']['kind'], 'program_unavailable')
        self.assertEqual(service.manager.records(), ())
        self.assertFalse((self.root / 'logs').exists())
        self.assertIn('No default shell is available', service.shell_description)

    async def test_execution_reports_the_resolved_executable_and_preserves_script(self):
        """The backend receives a UTF-8-initialized legacy command, and the Tool result identifies its actual executable."""
        legacy = self.executable('powershell.exe')
        handle = SimpleNamespace(
            state='exited', wait=AsyncMock(), terminate=AsyncMock(), read=AsyncMock(return_value=dict(
                stdout='你好\n', stderr='', state='exited', exit_code=0, wall_time_seconds=.01,
                total_output_bytes=7, next_cursor=7, retained_start=0, truncated=False, has_more=False,
                reason=None, error=None)))
        backend = SimpleNamespace(capabilities=ExecutionCapabilities(output_logs=True), spawn=AsyncMock(return_value=handle))
        service = self.service(backend=backend)
        tools = execution_tools(service, context=ToolContext(session_id='shell'))
        script = "param()\nWrite-Output '你好'"
        result = await tools.ainvoke(self.call(script=script))
        self.assertFalse(result.is_error, result)
        self.assertEqual(result.structured_content['program'], str(legacy))
        self.assertEqual(result.structured_content['stdout'], '你好\n')
        spec = backend.spawn.call_args.args[0]
        self.assertEqual(spec.argv[0], str(legacy))
        self.assertEqual(spec.argv[1:-1], ('-NoLogo', '-NoProfile', '-NonInteractive', '-Command'))
        self.assertIn('[Console]::OutputEncoding = $OutputEncoding', spec.argv[-1])
        self.assertTrue(spec.argv[-1].endswith('& {\n' + script + '\n}'))

    async def test_posix_keeps_bash_and_does_not_use_windows_fallback(self):
        """POSIX Shell selection and scripts retain the existing Bash invocation contract."""
        bash = self.executable('bash')
        service = self.service(windows=False)
        script = "printf '%s' 'hello'"
        self.assertEqual(service.default_shell, 'bash')
        spec, _, _ = service._plan({'mode': 'shell', 'script': script})
        self.assertEqual(spec.argv, (str(bash), '--noprofile', '--norc', '-c', script))

    async def test_legacy_console_is_hidden_while_standard_streams_remain_pipes(self):
        """Legacy PowerShell gets the console required by .NET encoding APIs without showing a window."""
        startup = SimpleNamespace(dwFlags=0, wShowWindow=5)
        windows = SimpleNamespace(STARTUPINFO=lambda: startup, STARTF_USESHOWWINDOW=1,
            SW_HIDE=0, CREATE_NEW_CONSOLE=16, CREATE_NO_WINDOW=0x08000000)
        with patch('autoagent.agent.execution.local.subprocess', windows):
            options = _windows_process_options((str(self.bin / 'powershell.exe'),))
            self.assertEqual(options['creationflags'], windows.CREATE_NEW_CONSOLE)
            self.assertIs(options['startupinfo'], startup)
            self.assertEqual(startup.dwFlags, windows.STARTF_USESHOWWINDOW)
            self.assertEqual(startup.wShowWindow, windows.SW_HIDE)
            self.assertEqual(_windows_process_options((str(self.bin / 'pwsh.exe'),)),
                {'creationflags': windows.CREATE_NO_WINDOW})


@unittest.skipUnless(os.name == 'nt', 'Requires native Windows PowerShell')
class NativeWindowsShellTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_shell_utf8_output_param_and_explicit_exit(self):
        """A real legacy PowerShell process produces readable Unicode and preserves param() and explicit exit behavior."""
        with tempfile.TemporaryDirectory() as directory:
            service = LocalExecutionService(directory)
            try:
                try:
                    service._shell_binary('powershell', service.cwd)
                except ToolBusinessError:
                    self.skipTest('Windows PowerShell is not installed')
                tools = execution_tools(service, context=ToolContext(session_id='windows'))
                result = await tools.ainvoke(LLMToolCall(name='execute_command', call_id='unicode', arguments=json.dumps(
                    {'mode': 'shell', 'shell': 'powershell', 'script': "param()\nWrite-Output '你好'; exit 7"})))
                self.assertIn('你好', result.structured_content['stdout'])
                self.assertEqual(result.structured_content['exit_code'], 7)
                self.assertTrue(result.is_error)
            finally:
                await service.aclose()
