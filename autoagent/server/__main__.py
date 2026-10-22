"""Serve through the shared CLI so help, logging and errors have one policy."""

import sys
from autoagent.cli.main import main as cli_main


def main(argv=None):
    return cli_main(["server", *(sys.argv[1:] if argv is None else argv)])


if __name__ == "__main__":
    raise SystemExit(main())
