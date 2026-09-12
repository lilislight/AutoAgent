"""Run interleaved comparisons against a pre-change source snapshot.

Usage: .venv/bin/python artifacts/core_execution_refactor/run_benchmarks.py /path/to/baseline
"""
import sys
import json
import subprocess
from pathlib import Path
root=Path(__file__).resolve().parents[2]
before=Path(sys.argv[1]).resolve()
python=sys.executable
out=root/'artifacts/core_execution_refactor/paired'
out.mkdir(exist_ok=True)
for iteration in range(3):
    for side, cwd in ([('before',before),('after',root)] if iteration%2==0 else [('after',root),('before',before)]):
        for name, module in [('execution','benchmark_core_execution'),('full','benchmark_full_core'),('memory','benchmark_in_memory_refactor')]:
            result=subprocess.run([python,'-m','tests.benchmarks.'+module],cwd=cwd,capture_output=True,text=True,check=True)
            data=json.loads(result.stdout)
            (out/f'{side}_{name}_{iteration+1}.json').write_text(json.dumps(data,indent=2)+'\n')
        print(f'Completed pair {iteration+1}: {side}',flush=True)
