"""Run offline guide examples in separate processes and fail on any error.

Run: .venv/bin/python -m examples.agent_usage_guide.run_all [--litellm]
Use --http to also exercise HTTP/SSE/Bearer and local OAuth callback sockets.
"""

import argparse
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--litellm", action="store_true")
    parser.add_argument("--http", action="store_true")
    parser.add_argument("--recording", action="store_true", help="Include Host/SQLite recording and Capability examples; requires autoagent[host]")
    options = parser.parse_args()
    commands = [(name, []) for name in ("llm", "media", "embeddings", "tools", "skills", "mcp", "nodes")]
    commands.extend(("providers", ["--backend", backend, "--protocol", protocol])
        for backend in ("openai", "deepseek") for protocol in ("chat", "responses"))
    if options.litellm:
        commands.extend(("providers", ["--backend", "litellm", "--protocol", protocol]) for protocol in ("chat", "responses"))
        commands.append(("embeddings", ["--litellm"]))
    if options.http:
        commands.extend((("mcp_http", []), ("mcp_auth", [])))
    if options.recording:
        commands.extend((("recording", []), ("capabilities", [])))
    for name, arguments in commands:
        module = "examples.agent_usage_guide." + name
        print(f"\n>>> {module} {' '.join(arguments)}", flush=True)
        subprocess.run([sys.executable, "-m", module, *arguments],
                       cwd=Path(__file__).resolve().parents[2], check=True, timeout=90)
    print(f"\nPASS: {len(commands)} example commands.", flush=True)


if __name__ == "__main__":
    main()
