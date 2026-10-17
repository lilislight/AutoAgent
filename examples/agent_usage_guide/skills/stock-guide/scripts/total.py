"""Sum explicitly supplied quantities; invoked only through the example runner."""

import json
from pathlib import Path
import sys


if __name__ == "__main__":
    quantities = json.load(sys.stdin)
    rule = Path("references/rules.md").read_text(encoding="utf-8").splitlines()[0]
    print(json.dumps({"total": sum(quantities), "rule": rule}))
