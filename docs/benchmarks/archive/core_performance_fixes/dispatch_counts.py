import json, os, threading
from unittest.mock import patch
from autoagent import AutoAgentApp
from autoagent.core.executor.node_executor import _run_sync
counts={'operator_threads':0,'pipes':0}
start=threading.Thread.start;pipe=os.pipe
def counted_start(thread):
 if thread.name.startswith('autoagent-operator-'):counts['operator_threads']+=1
 return start(thread)
def counted_pipe():
 counts['pipes']+=1
 return pipe()
with patch.object(threading.Thread,'start',counted_start),patch.object(os,'pipe',counted_pipe):
 app=AutoAgentApp()
 async def run():
  for _ in range(500):assert await _run_sync(app._node_executor._pool,lambda:1)==1
 try:app._runtime_loop.run(run())
 finally:app.close()
print(json.dumps(counts))
