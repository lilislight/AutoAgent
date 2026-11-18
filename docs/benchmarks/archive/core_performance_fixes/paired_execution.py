import subprocess,json
from pathlib import Path
script=Path(__file__).resolve()
root=script.parents[4];python=str(root/'.venv/bin/python')
out=script.parent/'paired';out.mkdir(exist_ok=True)
for i in range(3):
 order=('before','after') if i%2==0 else ('after','before')
 for side in order:
  cwd=root if side=='after' else Path('/tmp/autoagent-audit-fix-before')
  result=subprocess.run([python,'-m','tests.benchmarks.benchmark_core_execution'],cwd=cwd,capture_output=True,text=True,check=True)
  (out/f'{side}_{i+1}.json').write_text(result.stdout)
 print(f'paired execution round {i+1} complete',flush=True)
