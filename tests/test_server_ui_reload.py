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
                for path in ('/', '/workflows/example', '/agent', '/agent/c/saved-session'):
                    fresh = await client.get(path)
                    self.assertEqual(fresh.status_code, 200)
                    self.assertEqual(fresh.headers['cache-control'], 'no-store')
                    self.assertIn('new.js', fresh.text)
                    self.assertNotIn('old.js', fresh.text)
                    self.assertIn('autoagent-config', fresh.text)
                self.assertEqual((await client.get('/new.js')).status_code, 200)

    async def test_agent_routes_respect_mounts_proxy_roots_and_static_errors(self):
        """Agent deep links load the SPA under a proxy prefix without masking missing assets or APIs."""
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            (directory / 'index.html').write_text('<html><head><!--AUTOAGENT_CONFIG--></head><body>Agent</body></html>')
            app = FastAPI()
            mount_ui(app, settings=ServerSettings(ui_path='/tools/autoagent', ui_directory=directory))
            transport = httpx.ASGITransport(app=app, root_path='/proxy')
            async with httpx.AsyncClient(transport=transport, base_url='http://test') as client:
                for path in ('/agent', '/agent/', '/agent/c/saved', '/agent/c/chat%2Fwith%20spaces'):
                    response = await client.get('/proxy/tools/autoagent' + path)
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertIn('<base href="/proxy/tools/autoagent/">', response.text)
                    self.assertIn('"apiPrefix": "/proxy/api/v2"', response.text)
                for path in ('/assets/missing.js', '/agent/assets/missing.js', '/agent/c/', '/agent/c/chat/missing.js'):
                    self.assertEqual((await client.get('/proxy/tools/autoagent' + path)).status_code, 404, path)
