"""Project inspection, Server, execution and read-only history commands."""

import argparse
import asyncio
from contextlib import redirect_stdout
from importlib.metadata import PackageNotFoundError, version
import sys
import time
import traceback

from . import CliError
from .commands import inspect_project, run
from .output import failure_root, render, write_report
from .execution import command_name, read_input, validate_options


class Parser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_defaults(_selected_parser=self)

    def add_subparsers(self, **kwargs):
        kwargs["required"] = False
        return super().add_subparsers(**kwargs)

    def error(self, message):
        message = message.replace("argument group:", "command:").replace("argument command:", "subcommand:")
        raise CliError("USAGE_ERROR", message, details={
            "usage": self.format_usage().strip(), "hint": f"Run '{self.prog} --help' for available commands and options."})


def build_parser():
    common = Parser(add_help=False, argument_default=argparse.SUPPRESS, allow_abbrev=False)
    common.add_argument("--project", metavar="PATH", help="Project directory or explicit manifest; default: current directory.")
    common.add_argument("--server", dest="server_url", metavar="API_URL", help="Use an existing Server API base URL; mutually exclusive with --project.")
    common.add_argument("--request-timeout", type=float, default=argparse.SUPPRESS, help="HTTP request timeout in seconds; default: 30.")
    env = common.add_mutually_exclusive_group()
    env.add_argument("--env-file", metavar="PATH", help="Use this environment file, relative to the project root.")
    env.add_argument("--no-env-file", action="store_true", help="Do not load the project's .env file.")
    common.add_argument("--format", choices=("text", "json"), help="Output format; default: text. Logs go to stderr.")
    common.add_argument("--output", metavar="PATH", help="Save a report relative to the project root (cwd with --server); .json implies JSON; '-' means stdout.")
    common.add_argument("--debug", action="store_true", help="Include exception tracebacks on stderr.")
    common.add_argument("--log-level", choices=("debug", "info", "warning", "error", "critical"),
                        help="Log verbosity; default: AUTOAGENT_LOG_LEVEL, otherwise info for Server and warning for other commands. SDKs stay at warning unless debug is selected.")
    parser = Parser(prog="autoagent", parents=[common], allow_abbrev=False,
        description="Inspect projects, serve API/UI, execute Workflows and query persisted history locally or through a Server.",
        epilog="Exit codes: 0 success, 1 check/execution failed or cancelled, 2 invalid request, 3 service/I/O failure, 4 wait timeout or unknown outcome, 130 interrupted.")
    try:
        release = version("autoagent")
    except PackageNotFoundError:
        release = "unknown (source checkout)"
    parser.add_argument("--version", action="version", version=f"AutoAgent {release}")
    groups = parser.add_subparsers(dest="group", required=True, parser_class=Parser)
    project = groups.add_parser("project", parents=[common], allow_abbrev=False, help="Inspect the current project.")
    commands = project.add_subparsers(dest="command", required=True, parser_class=Parser)
    check = commands.add_parser("check", parents=[common], allow_abbrev=False, help="Discover and compile all Workflows.")
    check.add_argument("--warnings-as-errors", action="store_true")
    workflow = groups.add_parser("workflow", parents=[common], allow_abbrev=False, help="Inspect local Workflow plans and saved Revisions.")
    commands = workflow.add_subparsers(dest="command", required=True, parser_class=Parser)
    check = commands.add_parser("check", parents=[common], allow_abbrev=False, help="Check one Workflow and report compiler diagnostics.")
    check.add_argument("workflow_id")
    check.add_argument("--warnings-as-errors", action="store_true")
    compile = commands.add_parser("compile", parents=[common], allow_abbrev=False, help="Inspect the compiled execution plan without running it.")
    compile.add_argument("workflow_id")
    compile.add_argument("--warnings-as-errors", action="store_true")
    selected = compile.add_mutually_exclusive_group()
    selected.add_argument("--node-id", help="Show one Node, its incoming/outgoing Edges and enclosing Loops.")
    selected.add_argument("--edge-id", help="Show one Edge and its Loop boundary roles.")
    workflow_commands = commands
    server = groups.add_parser("server", parents=[common], allow_abbrev=False, help="Run the project Server and UI in the foreground.")
    server.add_argument("--host", help="Listening address; default: project environment configuration.")
    server.add_argument("--port", type=int, help="Listening port; default: project environment configuration.")
    server.add_argument("--no-ui", action="store_true")
    server.add_argument("--access-log", action=argparse.BooleanOptionalAction, default=None,
                        help="Log HTTP requests; default: AUTOAGENT_SERVER_ACCESS_LOG or true.")
    invoke = groups.add_parser("invoke", parents=[common], allow_abbrev=False, help="Start a registered Workflow in a new or existing Session.")
    invoke.add_argument("workflow_id")
    invoke.add_argument("--revision-id")
    invoke.add_argument("--entry-node", required=True)
    invoke.add_argument("--session-mode", choices=("new", "existing"), default="new")
    invoke.add_argument("--session-id")
    _input_arguments(invoke)
    _wait_arguments(invoke)
    invocation = groups.add_parser("invocation", parents=[common], allow_abbrev=False, help="Control Root executions or inspect persisted Invocation evidence.")
    controls = invocation.add_subparsers(dest="command", required=True, parser_class=Parser)
    for action in ("respond", "pause", "recover", "cancel"):
        control = controls.add_parser(action, parents=[common], allow_abbrev=False)
        control.add_argument("invocation_id")
        control.add_argument("--request-id", help="Stable operation identity; generated if omitted.")
        _wait_arguments(control)
        if action == "respond":
            control.add_argument("--wait-id", required=True)
            control.add_argument("--target-invocation-id", help="Invocation owning the Wait; defaults to the selected Invocation.")
            _input_arguments(control)
        elif action == "recover":
            control.add_argument("--expected-version", type=int, help="Expected Root Event sequence; read from Host when omitted.")
        elif action == "cancel":
            control.add_argument("--reason")
    from .history import add_parsers
    add_parsers(groups, workflow_commands, controls, common)
    _prepare_help(parser)
    return parser


def _prepare_help(parser):
    """Keep global options usable before commands, but show only relevant leaf flags."""
    local_only = parser.prog.endswith((" server", "project check", "workflow check", "workflow compile"))
    hidden = {"server_url", "request_timeout"} if local_only else set()
    if parser.prog.endswith(" server"):
        hidden.add("output")
    for action in list(parser._actions):
        if action.dest in hidden:
            parser._actions.remove(action)
            for group in parser._action_groups:
                if action in group._group_actions:
                    group._group_actions.remove(action)
            for option in action.option_strings:
                parser._option_string_actions.pop(option, None)
        if isinstance(action, argparse._SubParsersAction):
            for child in action.choices.values():
                _prepare_help(child)
    examples = {
        "autoagent": "autoagent project check\nautoagent --project examples/deepseek_weather_assistant server",
        "autoagent server": "autoagent --project examples/deepseek_weather_assistant server --port 8765",
        "autoagent workflow": "autoagent workflow list\nautoagent workflow compile WORKFLOW_ID",
        "autoagent invocation": "autoagent invocation show INVOCATION_ID\nautoagent invocation value INVOCATION_ID --field output",
        "autoagent invoke": "autoagent --server http://127.0.0.1:8765/api/v2 invoke weather-assistant --entry-node prepare --input-json '{\"question\":\"北京天气？\"}' --wait terminal",
    }
    if parser.prog in examples:
        parser.formatter_class = argparse.RawDescriptionHelpFormatter
        parser.epilog = (parser.epilog + "\n\n" if parser.epilog else "") + "Examples:\n  " + examples[parser.prog].replace("\n", "\n  ")


def _input_arguments(parser):
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--input-json", help="Input JSON, including explicit null when appropriate.")
    inputs.add_argument("--input-file", help="UTF-8 JSON file relative to cwd; '-' reads stdin.")


def _wait_arguments(parser):
    parser.add_argument("--wait", choices=("boundary", "terminal", "none"), default="boundary",
        help="Wait for a stable boundary (includes Timer/Wait), terminal outcome, or only submission (Server only).")
    parser.add_argument("--wait-timeout", type=float, help="Maximum observation time in seconds; does not cancel the execution.")


def _requested_format(argv):
    """Keep argument errors machine-readable even if parsing stops early."""
    result, output = None, None
    for index, token in enumerate(argv):
        if token == "--":
            break
        if token == "--format" and index + 1 < len(argv):
            result = argv[index + 1]
        elif token.startswith("--format="):
            result = token.split("=", 1)[1]
        elif token == '--output' and index + 1 < len(argv):
            output = argv[index + 1]
        elif token.startswith('--output='):
            output = token.split('=', 1)[1]
    return 'json' if result == 'json' or result is None and output and output.lower().endswith('.json') else 'text'


def main(argv=None):
    started = time.monotonic()
    argv = list(sys.argv[1:] if argv is None else argv)
    format = _requested_format(argv)
    options, root = None, None
    stdout, stderr = sys.stdout, sys.stderr
    try:
        parser = build_parser()
        if not argv:
            parser.print_help(file=stdout)
            return 0
        try:
            options = parser.parse_args(argv)
        except SystemExit as error:
            # Only argparse help/version may terminate the CLI successfully.
            return int(error.code or 0)
        if options._selected_parser._subparsers is not None:
            options._selected_parser.print_help(file=stdout)
            return 0
        format = options.format = getattr(options, "format", format)
        if getattr(options, "env_file", None) is not None and getattr(options, "no_env_file", False):
            raise CliError("USAGE_ERROR", "--env-file and --no-env-file cannot be combined.")
        validate_options(options)
        options.request_timeout = getattr(options, "request_timeout", 30)
        from .logging import command_logging
        # Third-party import/factory print() calls must not corrupt JSON stdout.
        with command_logging(options, stderr), redirect_stdout(stderr):
            if options.group == "server":
                from .server import launch
                launch(options, stdout, stderr)
                return 0
            if getattr(options, 'management_query', False):
                from .management import run as run_management
                root, document, status = asyncio.run(run_management(options))
            elif getattr(options, "history_query", False):
                from .history import run_query
                root, document, status = asyncio.run(run_query(options, stderr=stderr))
            elif options.group in ("invoke", "invocation"):
                value = read_input(options) if options.group == "invoke" or options.command == "respond" else None
                if getattr(options, "server_url", None):
                    from .remote import run_remote
                    root, document, status = asyncio.run(run_remote(options, value))
                else:
                    from .execution import run_local
                    root, document, status = asyncio.run(run_local(options, value))
            else:
                root, report = inspect_project(options)
                document, status = run(options, root, report)
        try:
            if document is None:
                return status
            if options.group == "invoke" or options.group == "invocation" and not getattr(options, "history_query", False):
                document["data"]["elapsed_seconds"] = round(time.monotonic() - started, 3)
            write_report(render(document, format), getattr(options, "output", None), root, stdout, stderr)
        except BrokenPipeError:
            raise
        except OSError as error:
            raise CliError("OUTPUT_WRITE_FAILED", str(error), exit_code=3) from error
        return status
    except BrokenPipeError:
        return 0
    except (Exception, KeyboardInterrupt, SystemExit) as error:
        if isinstance(error, KeyboardInterrupt):
            failure = CliError("INTERRUPTED", "Command interrupted.", exit_code=130)
        elif isinstance(error, SystemExit):
            failure = CliError("COMMAND_EXITED", f"Project code exited before the command completed: {error.code}", exit_code=3)
        elif isinstance(error, CliError):
            failure = error
        else:
            failure = CliError("COMMAND_FAILED", str(error) or type(error).__name__, exit_code=3)
        command = command_name(options) if options else None
        details = dict(failure.details or {})
        details.update(getattr(options, "_execution", {}))
        if getattr(options, "_request_id", None):
            details["request_id"] = options._request_id
        document = {"schema_version": 1, "command": command, "ok": False,
            "error": {"code": failure.code, "message": str(failure), **({"details": details} if details else {})}}
        if getattr(options, "_source", None):
            document["source"] = options._source
        elif root is not None and not getattr(options, 'server_url', None):
            document["source"] = {"kind": "project", "root": str(root)}
        if getattr(options, "debug", False):
            traceback.print_exc(file=stderr)
        output = getattr(options, 'output', None)
        if output not in (None, '-') and not getattr(options, '_export_started', False) and failure.code not in ('OUTPUT_WRITE_FAILED', 'USAGE_ERROR'):
            try:
                write_report(render(document, format), output, failure_root(options, root), stdout, stderr)
                return failure.exit_code
            except OSError as write_error:
                stderr.write(f'Cannot save error report: {write_error}\n')
                document['error'] = dict(code='OUTPUT_WRITE_FAILED', message=str(write_error),
                    details=dict(command_error=document['error'], **getattr(options, '_execution', {})))
                failure.exit_code = 3
        stream = stdout if format == "json" else stderr
        stream.write(render(document, format))
        stream.flush()
        return failure.exit_code
