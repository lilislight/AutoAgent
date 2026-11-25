"""Portable writer ownership, memory sampling and local Agent operations."""

from contextlib import contextmanager
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from sqlalchemy.engine import URL

from autoagent.agent import LLMToolCall, LocalFileService, Skill, ToolContext, file_tools
from autoagent.agent_app.skills import editable, enabled, set_enabled
from autoagent.host.memory import process_sample
from autoagent.host.registration import _atomic_write
from autoagent.host.residency import process_memory_bytes
from autoagent.host.persistence import ConflictError
from autoagent.host.persistence.storage import Storage


@contextmanager
def portable_filesystem():
    """Exercise the same paths used when descriptor-relative POSIX APIs are absent."""
    with patch.dict(os.__dict__), patch.object(os, 'supports_dir_fd', set()), patch.object(os, 'supports_fd', set()):
        for name in ('fchmod', 'O_NOFOLLOW', 'O_DIRECTORY', 'O_NONBLOCK'):
            os.__dict__.pop(name, None)
        yield


class PortableFilesystemTests(unittest.TestCase):
    def test_file_tools_read_create_overwrite_edit_transfer_search_and_delete(self):
        """File Tools retain binary data and mutation semantics without POSIX-only flags or directory descriptors."""
        with tempfile.TemporaryDirectory() as directory, portable_filesystem():
            root = Path(directory).resolve()
            tools = file_tools(LocalFileService(root), context=ToolContext(session_id='portable'))

            def invoke(name, **arguments):
                result = tools.invoke(LLMToolCall(name=name, call_id=name, arguments=json.dumps(arguments)))
                self.assertFalse(result.is_error, str(result.content))
                return result

            invoke('write_file', path='nested/file.txt', content='first\r\n', create_parents=True)
            invoke('write_file', path='nested/file.txt', content='second\r\n', overwrite=True)
            invoke('edit_file', path='nested/file.txt', edits=[dict(old_text='second', new_text='third')])
            self.assertEqual((root / 'nested/file.txt').read_bytes(), b'third\r\n')
            invoke('read_file', path='nested/file.txt')
            invoke('list_files', path='nested')
            invoke('search_files', path='nested', query='third')
            invoke('manage_files', request=dict(operation='copy', path='nested/file.txt', destination='copy.txt'))
            invoke('manage_files', request=dict(operation='move', path='copy.txt', destination='moved.txt'))
            self.assertEqual((root / 'moved.txt').read_bytes(), b'third\r\n')
            invoke('manage_files', request=dict(operation='delete', path='moved.txt'))
            self.assertFalse((root / 'moved.txt').exists())

    def test_skill_switch_is_editable_without_posix_apis_and_retains_document(self):
        """A writable local Skill can be toggled on Windows without losing Markdown, BOM or CRLF content."""
        with tempfile.TemporaryDirectory() as directory, portable_filesystem():
            root = Path(directory).resolve()
            skill_root = root / 'sample'
            skill_root.mkdir()
            document = skill_root / 'SKILL.md'
            document.write_bytes(b'\xef\xbb\xbf---\r\nname: sample\r\ndescription: Example\r\n---\r\n# Keep body\r\n')
            skill = Skill(skill_root).freeze()
            self.assertTrue(editable(skill, (root,)))
            set_enabled(skill, False, roots=(root,))
            self.assertFalse(enabled(Skill(skill_root)))
            self.assertTrue(document.read_bytes().startswith(b'\xef\xbb\xbf'))
            self.assertTrue(document.read_bytes().endswith(b'# Keep body\r\n'))
            set_enabled(Skill(skill_root).freeze(), True, roots=(root,))
            self.assertTrue(enabled(Skill(skill_root)))

    def test_registration_atomic_write_without_fchmod(self):
        """Creating and updating an existing manifest works without Unix descriptor chmod."""
        with tempfile.TemporaryDirectory() as directory, portable_filesystem():
            path = Path(directory) / 'auto-agent.toml'
            _atomic_write(path, b'first\n')
            _atomic_write(path, b'second\n')
            self.assertEqual(path.read_bytes(), b'second\n')
            self.assertEqual(list(path.parent.iterdir()), [path])

    def test_current_memory_falls_back_without_resource_or_procfs(self):
        """Residency uses current RSS while telemetry preserves Windows peak working-set data separately."""
        sample = SimpleNamespace(rss=123456, peak_wset=987654)
        with patch.object(Path, 'read_text', side_effect=OSError), patch('psutil.Process') as process:
            process.return_value.memory_info.return_value = sample
            self.assertEqual(process_memory_bytes(), sample.rss)
            result = process_sample()
            self.assertEqual(result['rss_bytes'], sample.rss)
            self.assertEqual(result['peak_rss_bytes'], sample.peak_wset)


class PortableSQLiteTests(unittest.IsolatedAsyncioTestCase):
    async def test_host_memory_api_starts_without_resource_or_procfs(self):
        """An enabled Agent Server can start, sample and serve memory telemetry without importing Unix resource."""
        import httpx
        import psutil  # Preload this machine's native adapter before hiding Unix-only modules.
        from autoagent.agent_app import AgentApplication
        from autoagent.host import AutoAgentHost, HostSettings
        from autoagent.server import create_app

        read_text = Path.read_text
        def without_proc(path, *args, **kwargs):
            if path.as_posix().startswith('/proc/'):
                raise FileNotFoundError('procfs is unavailable')
            return read_text(path, *args, **kwargs)

        with tempfile.TemporaryDirectory() as directory, patch.dict(sys.modules, {'resource': None}), \
                patch.object(Path, 'read_text', without_proc):
            database = Path(directory) / 'memory.sqlite'
            settings = HostSettings(database_url=URL.create('sqlite+aiosqlite', database=str(database)).render_as_string(),
                memory_monitor_enabled=True, memory_monitor_interval_seconds=.05, memory_budget_bytes=2 * 1024**3)
            async with AutoAgentHost(root=directory, settings=settings, applications=(AgentApplication,), environment={}) as host:
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(host, ui=False)),
                        base_url='http://test') as client:
                    for _ in range(2):
                        response = await client.get('/api/v2/memory')
                        self.assertEqual(response.status_code, 200, response.text)
                        data = response.json()
                        self.assertTrue(data['enabled'])
                        self.assertGreater(data['latest']['process']['rss_bytes'], 0)
                        self.assertGreater(data['latest']['resources']['system_total_bytes'], 0)
                        self.assertIsNone(data['latest']['cgroup'])
                        await asyncio.sleep(.1)

    async def test_writer_excludes_another_process_and_readers_use_escaped_file_uri(self):
        """The platform-neutral lock excludes writers across processes while readers coexist on special-character paths."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / 'data # 100% résumé.sqlite'
            url = URL.create('sqlite+aiosqlite', database=str(database))
            writer, reader, contender = Storage(url), Storage(url, read_only=True), Storage(url)
            try:
                await writer.open()
                await reader.open()
                with self.assertRaises(ConflictError):
                    await contender.open()
                script = (
                    'import sys\nfrom filelock import FileLock, Timeout\n'
                    'try:\n    FileLock(sys.argv[1]).acquire(timeout=0)\n'
                    'except Timeout:\n    sys.exit(0)\n'
                    'sys.exit(1)\n'
                )
                result = await asyncio.to_thread(subprocess.run,
                    [sys.executable, '-c', script, str(database.resolve()) + '.writer.lock'],
                    capture_output=True, text=True, timeout=15)
                self.assertEqual(result.returncode, 0, result.stderr)
                await writer.close()
                await contender.open()
            finally:
                await reader.close()
                await contender.close()
                await writer.close()

    async def test_crashed_process_releases_writer_ownership(self):
        """Abrupt exit leaves no stale ownership that prevents the next writer from opening its database."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / 'data.sqlite'
            script = 'import os,sys\nfrom filelock import FileLock\nlock=FileLock(sys.argv[1])\nlock.acquire(timeout=0)\nos._exit(0)\n'
            result = await asyncio.to_thread(subprocess.run,
                [sys.executable, '-c', script, str(database.resolve()) + '.writer.lock'],
                capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            storage = Storage(URL.create('sqlite+aiosqlite', database=str(database)))
            try:
                await storage.open()
            finally:
                await storage.close()
