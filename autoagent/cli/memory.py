"""Read memory telemetry from the running Server that owns the process."""

from pathlib import Path

from . import CliError
from .remote import ServerClient


async def run(options):
    if not getattr(options, 'server_url', None):
        raise CliError('USAGE_ERROR', 'memory requires --server to inspect the running Server process.')
    async with ServerClient(options.server_url, timeout=options.request_timeout) as client:
        params = {'history': options.history, 'points': options.points}
        if options.window_seconds is not None:
            params['window_seconds'] = options.window_seconds
        data = await client.request('GET', '/memory', params=params)
    return Path.cwd(), dict(schema_version=1, command='memory', ok=True,
                            source=dict(kind='server', url=options.server_url.rstrip('/')),
                            data=data), 0
