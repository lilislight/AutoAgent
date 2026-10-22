"""Start the same Server as `autoagent --project examples/deepseek_weather_assistant server`."""

from pathlib import Path
import sys
from autoagent.cli.main import main as cli_main


def main(argv=None):
    return cli_main(["--project", str(Path(__file__).resolve().parent), "server",
                     *(sys.argv[1:] if argv is None else argv)])


if __name__ == "__main__":
    raise SystemExit(main())
