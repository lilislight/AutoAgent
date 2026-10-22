"""Command-line adapters for AutoAgent project development."""


def error_code(code):
    """Normalize transport error names while preserving domain-specific codes."""
    return {
        'invalid_input':'INVALID_INPUT', 'invalid_request':'INVALID_REQUEST',
        'not_found':'NOT_FOUND', 'host_busy':'HOST_BUSY',
        'host_unavailable':'HOST_UNAVAILABLE', 'query_timeout':'QUERY_TIMEOUT',
    }.get(code, code)


class CliError(Exception):
    """A command failure with a stable code and process exit status."""

    def __init__(self, code, message, *, exit_code=2, details=None):
        super().__init__(message)
        self.code = error_code(code)
        self.exit_code = exit_code
        self.details = details or {}
