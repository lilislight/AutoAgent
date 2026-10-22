"""Foreground command logging; importing the SDK never configures global logs."""

from contextlib import contextmanager
import logging
import os

from . import CliError


@contextmanager
def command_logging(options, stderr):
    environment = os.environ
    if not getattr(options, "server_url", None):
        try:
            from autoagent.host.project import Project, load_project_environment
            from autoagent.host import ProjectError
        except ModuleNotFoundError as error:
            raise CliError("DEPENDENCY_MISSING", "Install autoagent[cli] to load project configuration.", exit_code=3) from error

        try:
            project = Project.find(getattr(options, "project", "."))
            environment = load_project_environment(project.root, env_file=getattr(options, "env_file", None),
                                                   use_env_file=not getattr(options, "no_env_file", False))
        except ProjectError as error:
            raise CliError("PROJECT_INVALID", str(error)) from error
    default_level = "info" if options.group == "server" else "warning"
    level = "debug" if getattr(options, "debug", False) else getattr(options, "log_level", None) or environment.get("AUTOAGENT_LOG_LEVEL", default_level)
    level = level.strip().lower()
    if level not in {"debug", "info", "warning", "error", "critical"}:
        raise CliError("USAGE_ERROR", "AUTOAGENT_LOG_LEVEL must be debug, info, warning, error or critical.")
    options.log_level = level
    root = logging.getLogger()
    previous = (root.level, root.handlers[:])
    handler = logging.StreamHandler(stderr)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %z",
    ))
    root.handlers = [handler]
    root.setLevel(level.upper())
    sdk_names = ("httpx", "httpcore", "openai", "LiteLLM", "LiteLLM Router", "LiteLLM Proxy")
    names = (*sdk_names, "uvicorn", "uvicorn.error", "uvicorn.access")
    loggers = [(logging.getLogger(name), logging.getLogger(name).level,
                logging.getLogger(name).handlers[:], logging.getLogger(name).propagate) for name in names]
    for logger, *_ in loggers:
        logger.handlers = []
        logger.propagate = True
        threshold = getattr(logging, level.upper())
        if logger.name in sdk_names and level != "debug":
            threshold = max(logging.WARNING, threshold)
        logger.setLevel(threshold)
    try:
        yield
    finally:
        root.setLevel(previous[0])
        root.handlers = previous[1]
        for logger, saved_level, handlers, propagate in loggers:
            logger.setLevel(saved_level)
            logger.handlers = handlers
            logger.propagate = propagate
