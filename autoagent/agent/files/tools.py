"""Six ordinary Tools; all authorization remains on the ToolSet boundary."""

from typing import Annotated, Literal
from pydantic import Field
from ..models import ToolOutput
from ..tools import Tool, ToolContext, ToolSet, ToolBusinessError
from ..tools.operations import ToolOperationPolicy
from .models import FileEdit, FileManagement
from .service import FileService
from .operations import FILE_OPERATION_SCHEMA

Positive = Annotated[int, Field(gt=0)]
PageSize = Annotated[int, Field(gt=0, le=1000)]


class _FileTool(Tool):
    def __init__(self, handler, service, **options):
        super().__init__(handler, **options)
        self.service = service

    @property
    def definition(self):
        value = super().definition
        config = self.service.config
        props = value.parameters["properties"]
        descriptions = {
            "path": "Path relative to this Workspace's working directory, or an absolute path subject to access policy. Permission previews never authorize content access.",
            "pattern": "Case-sensitive glob matched against returned relative paths (for example '*.py' or 'src/**/*.py'), not a regex. Uses the same glob matcher as search include/exclude.",
            "include": "Case-sensitive glob patterns for candidate relative paths, not content regexes. Uses the same matcher as list_files pattern.",
            "exclude": "Glob patterns excluded from traversal; matching directories are pruned. Explicit path selection is better than scanning a large tree.",
            "cursor": "Opaque continuation from this tool. Keep path, filters and search options unchanged; limit may change. On a changed query or directory, restart without cursor. Never synthesize a cursor.",
            "limit": "Maximum result rows on this page, not total entries scanned or total matches. May change while continuing with cursor.",
            "expected_version": "Optional version precondition from a read or mutation. Omit for ordinary writes and edits. If supplied, the current file must match it; stale versions are rejected.",
            "overwrite": "False creates only and rejects an existing path. True replaces the entire file. Neither a prior read nor expected_version is required; access permissions still apply.",
            "max_bytes": "Maximum returned UTF-8 text bytes including line labels, not the maximum file size. Continue long lines using next_line and next_column.",
            "case_sensitive": "True by default. Set false explicitly for case-insensitive content search; path glob matching stays case-sensitive.",
            "regex": "False by default: query is literal text. Set true for a regular expression; do not escape literal searches as regexes.",
            "mode": "matches returns match locations (path, line, column); files returns matching paths; count counts matching lines. Choose the needed mode directly; count then files then matches is not required. match_count is page-local, not a total.",
            "edits": "Exact replacements against the same original file, committed atomically. Do not include read_file line labels in old_text. Replacements must be unique unless replace_all=true, and must not overlap.",
        }
        for name, description in descriptions.items():
            if name in props:
                props[name]["description"] = description
        if value.name in {"list_files", "search_files"}:
            props["max_depth"]["anyOf"][0]["maximum"] = config.max_depth
            details = (f" Scan budget: {config.max_entries} eligible entries per call; maximum depth {config.max_depth}."
                       f" Generated directories {', '.join(config.generated_directories) or '(none)'} are listed but not descended unless include_generated=true; an explicit path into them is allowed."
                       f" Entry/text payloads have a {config.max_output_bytes}-byte budget (pagination metadata is additional). Limits return partial success with truncated/reason and cursor; continue with unchanged filters and the returned cursor (limit may change). has_more means a continuation cursor exists, not that depth-limited or skipped paths were searched."
                       " Depth-limited paths must be opened explicitly. Excludes and gitignore prune directories.")
            if value.name == "search_files":
                details += (f" Content scan: {config.max_scan_bytes} bytes and {config.search_timeout_seconds:g}s per call;"
                            f" individual files over {config.max_file_bytes} bytes are skipped, not fatal."
                            " match_count is for this page; total_count appears only for an exact, unpaginated search without protected/skipped files. A final page's count is not the sum of earlier pages.")
        else:
            details = f" Individual files/content are capped at {config.max_file_bytes} bytes; returned text at {config.max_output_bytes} bytes."
            if value.name == "read_file":
                props["max_bytes"]["maximum"] = config.max_output_bytes
                details += " Long lines are paged with next_line/next_column; pass both as start_line/start_column to continue."
            if value.name == "write_file":
                props["content"]["maxLength"] = config.max_file_bytes
        return value.model_copy(update={"description": value.description + details})


def file_tools(service: FileService, *, context: ToolContext | None = None) -> ToolSet:
    """Bind a backend explicitly for standalone Tools or a custom Workflow."""
    if not isinstance(service, FileService):
        raise TypeError("File Tools require a FileService.")

    def execute(name):
        current = ToolContext.current()
        return service.execute(name, current.arguments, current)

    def list_files(path: str = ".", pattern: str = "*", recursive: bool = False,
                   include_hidden: bool = False, respect_gitignore: bool = True,
                   exclude: list[str] = [], limit: PageSize = 100, cursor: str | None = None,
                   include_generated: bool = False, max_depth: Annotated[int | None, Field(ge=0)] = None) -> ToolOutput:
        """List one page of directory entries with names, types and sizes. Recursive listings skip restricted subdirectories. Use stat for full metadata."""
        return execute("list_files")

    def search_files(query: str, path: str = ".", include: list[str] = ["*"], exclude: list[str] = [],
                     regex: bool = False, case_sensitive: bool = True,
                     include_hidden: bool = False, respect_gitignore: bool = True,
                     mode: Literal["matches", "files", "count"] = "matches",
                     limit: PageSize = 100, max_matches: Annotated[int, Field(gt=0, le=10000)] = 1000,
                     cursor: str | None = None, include_generated: bool = False,
                     max_depth: Annotated[int | None, Field(ge=0)] = None) -> ToolOutput:
        """Locate text matches in readable files. Directory searches skip files requiring approval or denied by policy. Search a specific path to request access to that file. Join the result's base path with each row's path; use read_file at the returned line. Modes return locations, file paths, or counts."""
        return execute("search_files")

    def read_file(path: str, start_line: Positive = 1, start_column: Positive = 1,
                  max_lines: Annotated[int, Field(gt=0, le=2000)] = 200,
                  max_bytes: Positive = min(32768, service.config.max_output_bytes)) -> ToolOutput:
        """Read numbered text lines and return an optional version precondition for later mutations. Continue with next_line/next_column as start_line/start_column (columns count Unicode characters). Supported images return image content; other binary files are rejected."""
        return execute("read_file")

    def write_file(path: str, content: str, overwrite: bool = False,
                   expected_version: str | None = None, create_parents: bool = False) -> ToolOutput:
        """Create a UTF-8 file, or replace its entire contents with overwrite=true. No prior read is required. Use edit_file for partial changes. Returns confirmation and a version; diff stays in execution metadata."""
        return execute("write_file")

    def edit_file(path: str, edits: Annotated[list[FileEdit], Field(min_length=1, max_length=100)],
                  expected_version: str | None = None) -> ToolOutput:
        """Edit an existing file using exact text replacements. No prior read or version is required. All replacements use the same original content and commit together. Matches must be unique unless replace_all=true."""
        return execute("edit_file")

    def manage_files(request: FileManagement) -> ToolOutput:
        """Stat, mkdir, copy/move a regular file, or delete an explicit path. Transfers never overwrite. expected_version is optional for move/delete. Delete removes only files or empty directories."""
        return execute("manage_files")

    handlers = (list_files, search_files, read_file, write_file, edit_file, manage_files)
    tools = []
    for handler in handlers:
        name = handler.__name__
        def resolve(arguments, current, name=name):
            try:
                return service.operations(name, arguments, current)
            except OSError as error:
                raise ToolBusinessError("File target is unavailable: " + str(error), code="file_target_unavailable") from error
        tools.append(_FileTool(handler, service, operations=resolve, operation_schema=FILE_OPERATION_SCHEMA))
    return ToolSet(tools, context=context, policy=ToolOperationPolicy(service.evaluate))
