"""Run the real live-review example and its local UI in one Host.

.venv/bin/python -m examples.local_ui_demo.run_live --rounds 30 --children 20
Use --database to select a fresh SQLite file for the current schema.
Approval is supplied by this example after --approval-seconds; UI observes only.
"""
import argparse
import asyncio
import contextlib
import json
from pathlib import Path
import time

import uvicorn
from autoagent.host import AutoAgentHost
from autoagent.server import ServerSettings, create_app


async def run_demo(host, args):
    root=await host.execute('live-document-review',{'text':'Live tracing review','rounds':args.rounds,'delay_ms':round(args.step_seconds*1000),'children':args.children},entry_node_id='prepare')
    url=f'http://127.0.0.1:{args.port}/?view=executions&execution={root.invocation_id}&session={root.session_id}&pane=split'
    print('Live tracing:',url,flush=True)
    if args.state_file:
        Path(args.state_file).write_text(json.dumps({'url':url,'invocation_id':root.invocation_id,'session_id':root.session_id}))
    approval_at=None
    while True:
        result=await host.join(root)
        if result.status in ('completed','failed','cancelled'):
            print('Root:',result.status,flush=True)
            break
        for wait in result.waits:
            if wait.kind=='external':
                approval_at=approval_at or time.monotonic()+args.approval_seconds
                if time.monotonic()>=approval_at:
                    await host.respond(root,wait.id,wait.request)
                    print('Example approval supplied.',flush=True)
        await asyncio.sleep(.2)
    return root


async def main(args):
    environment={'AUTOAGENT_DATABASE_URL':'sqlite+aiosqlite:///'+str(Path(args.database).resolve())}
    async with AutoAgentHost.from_project(Path(__file__).parent,environment=environment) as host:
        task=asyncio.create_task(run_demo(host,args))
        app=create_app(host,settings=ServerSettings(port=args.port))
        try:
            await uvicorn.Server(uvicorn.Config(app,host='127.0.0.1',port=args.port,log_level='info')).serve()
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',default='/tmp/autoagent-live-ui-demo.db')
    parser.add_argument('--port',type=int,default=8765)
    parser.add_argument('--rounds',type=int,default=12)
    parser.add_argument('--step-seconds',type=float,default=1.5)
    parser.add_argument('--approval-seconds',type=float,default=20)
    parser.add_argument('--children',type=int,default=4)
    parser.add_argument('--state-file')
    args=parser.parse_args()
    if not 1<=args.rounds<=200 or not .01<=args.step_seconds<=10 or args.approval_seconds<0 or not 1<=args.children<=20:
        parser.error('Expected 1–200 rounds, .01–10 seconds per step, nonnegative approval delay and 1–20 children')
    asyncio.run(main(args))
