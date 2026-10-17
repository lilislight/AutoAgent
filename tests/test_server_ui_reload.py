"""Fresh UI entry documents after local frontend rebuilds."""
import tempfile
import unittest
from pathlib import Path

import httpx
from fastapi import FastAPI
from autoagent.server import ServerSettings, mount_ui


class UIRebuildTests(unittest.IsolatedAsyncioTestCase):
    async def test_frontend_rebuild_does_not_keep_deleted_script_references(self):
        """An already-mounted UI serves the current HTML/assets after a local rebuild."""
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            def build(name):
                (directory / 'index.html').write_text(
                    f'<html><head><!--AUTOAGENT_CONFIG--></head><body><script src="{name}.js"></script></body></html>')
                (directory / f'{name}.js').write_text(f'console.log("{name}");')
            build('old')
            app = FastAPI()
            mount_ui(app, settings=ServerSettings(ui_directory=directory))
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
                first = await client.get('/')
                self.assertIn('old.js', first.text)
                build('new')
                (directory / 'old.js').unlink()
                for path in ('/', '/workflows/example'):
                    fresh = await client.get(path)
                    self.assertEqual(fresh.status_code, 200)
                    self.assertEqual(fresh.headers['cache-control'], 'no-store')
                    self.assertIn('new.js', fresh.text)
                    self.assertNotIn('old.js', fresh.text)
                    self.assertIn('autoagent-config', fresh.text)
                self.assertEqual((await client.get('/new.js')).status_code, 200)
