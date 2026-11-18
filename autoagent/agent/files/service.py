"""Local bounded file I/O. OS permissions remain the deployment boundary."""

from contextlib import contextmanager
import base64
import codecs
import difflib
import errno
from fnmatch import fnmatchcase
import hashlib
import heapq
import json
import mimetypes
import os
from pathlib import Path
import stat
import threading
import time
from typing import Protocol, runtime_checkable
from uuid import uuid4

from ..models import DataSource, ImageContent, TextContent, ToolOutput
from ..tools import ToolBusinessError, ToolContext, ToolDecision
from ..tools.operations import ToolOperation, combine_decisions
from .models import FileToolsConfig


def fail(code, message):
    raise ToolBusinessError(message, code=code)


def version(data):
    return hashlib.sha256(data).hexdigest()


def fingerprint(path):
    try:
        value = path.stat()
        return f"{value.st_dev}:{value.st_ino}"
    except FileNotFoundError:
        return "missing"


def glob_match(path, pattern):
    return fnmatchcase(path, pattern) or (pattern.startswith("**/") and fnmatchcase(path, pattern[3:]))


_FILE_ACTIONS = {
    "list_files": "inspect", "read_file": "read", "search_files": "read",
    "write_file": "write", "edit_file": "write",
}
_MANAGEMENT_ACTIONS = {
    "stat": "inspect", "mkdir": "write", "copy": "read", "move": "delete", "delete": "delete",
}


@runtime_checkable
class FileService(Protocol):
    """Backend resolves operations before authorization, then executes that plan."""

    config: FileToolsConfig
    version: str
    def operations(self, name, arguments, context) -> tuple[ToolOperation, ...]: ...
    def evaluate(self, operation, context) -> ToolDecision: ...
    def execute(self, name, arguments, context) -> ToolOutput: ...


_locks_guard = threading.Lock()
_locks = {}


@contextmanager
def resource_locks(paths):
    """Coordinate all local services, Sessions and aliases for canonical targets."""
    keys = sorted({os.path.normcase(str(path)) for path in paths})
    with _locks_guard:
        selected = []
        for key in keys:
            lock, users = _locks.get(key, (threading.RLock(), 0))
            _locks[key] = lock, users + 1
            selected.append((key, lock))
    try:
        for _, lock in selected:
            lock.acquire()
        try:
            yield
        finally:
            for _, lock in reversed(selected):
                lock.release()
    finally:
        with _locks_guard:
            for key, _ in selected:
                lock, users = _locks[key]
                if users == 1:
                    del _locks[key]
                else:
                    _locks[key] = lock, users - 1


class LocalFileService:
    """One Workspace binding with explicit optional version preconditions."""

    def __init__(self, root, *, working_directory=".", config=None):
        self.root = Path(root).expanduser().resolve(strict=True)
        self.cwd = (self.root / working_directory).resolve(strict=True)
        if not self.root.is_dir() or not self.cwd.is_dir() or not self.cwd.is_relative_to(self.root):
            raise ValueError("File service working directory must be inside its Workspace.")
        self.config = FileToolsConfig.model_validate(config or FileToolsConfig())
        self._root_identity = fingerprint(self.root)
        self.version = version(json.dumps(dict(config=self.config.model_dump(mode="json"),
            root=str(self.root), cwd=str(self.cwd), identity=self._root_identity), sort_keys=True).encode())

    def _resolve(self, requested):
        if not requested or "\x00" in requested:
            fail("invalid_path", "A nonempty file path is required.")
        if fingerprint(self.root) != self._root_identity:
            fail("workspace_changed", "The Workspace directory changed; reopen the Workspace.")
        raw = Path(requested).expanduser()
        try:
            return (raw if raw.is_absolute() else self.cwd / raw).resolve()
        except (OSError, RuntimeError, ValueError):
            fail("invalid_path", "The file path cannot be resolved.")

    def _target(self, action, path):
        inside = path.is_relative_to(self.root)
        attributes = dict(scope="workspace" if inside else "external",
            path=path.relative_to(self.root).as_posix() if inside else path.as_posix())
        return ToolOperation(action="file." + action, resource=str(path), attributes=attributes)

    def _operation(self, action, requested, pinned):
        path = self._resolve(requested)
        operation = self._target(action, path)
        parent = pinned.get((operation.action, operation.resource))
        if parent is None:
            parent = path.parent
            while not parent.exists() and parent != parent.parent:
                parent = parent.parent
        elif not path.is_relative_to(parent) or parent == path:
            fail("file_target_changed", "The authorized directory anchor no longer contains this target.")
        return operation.model_copy(update={"attributes": {
            **operation.attributes, "_binding": {"requested": requested, "parent": str(parent),
                "parent_identity": fingerprint(parent), "root_identity": self._root_identity},
        }})

    def operations(self, name, arguments, context):
        values = arguments.get("request", arguments)
        command = values["operation"] if name == "manage_files" else None
        action = _FILE_ACTIONS[name] if command is None else _MANAGEMENT_ACTIONS[command]
        path = values["path"]
        targets = [(action, path)]
        if command in {"copy", "move"}:
            targets.append(("write", values["destination"]))
        # Diffs and moves can expose old contents through a result or another path.
        if name == "edit_file" or (name == "write_file" and values.get("overwrite")) or command == "move":
            targets.append(("read", path))
        # Reuse the authorized ancestor if a sibling call has since created a parent.
        pinned = {(item["action"], item["resource"]): Path(item["attributes"]["_binding"]["parent"])
            for item in context.authorization.get("tool_binding", {}).get("operations", ())
            if "_binding" in item["attributes"]}
        return tuple(self._operation(action, requested, pinned) for action, requested in targets)

    def evaluate(self, operation, context):
        policies = (self.config.policy, *context.services.get("tool_operation_policies", ()))
        decisions = [policy.evaluate_operation(None, context, operation) for policy in policies]
        evaluate = context.services.get("operation_policy")
        if evaluate is not None and (selected := evaluate(operation, context)) is not None:
            decisions.append(selected)
        return combine_decisions(decisions)

    def _verify(self, plan):
        binding = plan.attributes["_binding"]
        if (str(self._resolve(binding["requested"])) != plan.resource
                or fingerprint(Path(binding["parent"])) != binding["parent_identity"]):
            fail("file_target_changed", "The resolved file target changed after authorization; retry for a fresh permission check.")

    @contextmanager
    def _parent(self, path, *, create=False):
        """No-follow descriptor traversal on POSIX; bounded portable fallback."""
        descriptors = []
        context = ToolContext.current()
        anchors = {Path(binding["parent"]): binding["parent_identity"]
            for plan in context.operations if (binding := plan.attributes.get("_binding")) is not None
            and plan.action.startswith("file.") and path.is_relative_to(Path(binding["parent"]))} if context else {}
        try:
            if path == path.parent:
                yield None, str(path)
            elif os.open in os.supports_dir_fd and hasattr(os, "O_NOFOLLOW"):
                flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                current = os.open(path.anchor, flags)
                descriptors.append(current)
                walked = Path(path.anchor)
                for part in path.parts[1:-1]:
                    if create:
                        try:
                            os.mkdir(part, dir_fd=current)
                        except FileExistsError:
                            pass
                    current = os.open(part, flags, dir_fd=current)
                    descriptors.append(current)
                    walked /= part
                    if walked == self.root:
                        info = os.fstat(current)
                        if f"{info.st_dev}:{info.st_ino}" != self._root_identity:
                            fail("workspace_changed", "Workspace identity changed during file access.")
                    if walked in anchors:
                        info = os.fstat(current)
                        if f"{info.st_dev}:{info.st_ino}" != anchors[walked]:
                            fail("file_target_changed", "The authorized directory was replaced during file access.")
                yield current, path.name
            else:
                if create:
                    path.parent.mkdir(parents=True, exist_ok=True)
                if self._resolve(str(path)) != path:
                    fail("file_target_changed", "File target changed during access.")
                if any(anchor.is_symlink() or fingerprint(anchor) != stamp for anchor, stamp in anchors.items()):
                    fail("file_target_changed", "The authorized directory changed during access.")
                yield None, str(path)
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)

    def create_directory(self, path):
        """Create an authorized runtime directory through no-follow traversal."""
        with self._parent(path / ".autoagent-directory-check", create=True):
            pass

    def open_output(self, path):
        """Open a new authorized output file without clobbering or following links."""
        with self._parent(path, create=True) as (directory, name):
            descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600, dir_fd=directory)
        return os.fdopen(descriptor, "wb")

    def _bytes(self, path):
        with self._parent(path) as (directory, name):
            descriptor = os.open(name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
                dir_fd=directory)
            with os.fdopen(descriptor, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode):
                    fail("unsupported_file", "Only regular files can be read.")
                if info.st_size > self.config.max_file_bytes:
                    fail("limit_exceeded", f"File size {info.st_size} bytes exceeds max_file_bytes={self.config.max_file_bytes}. "
                        "Reducing read_file max_bytes does not bypass this file-size cap; use stat for metadata or a targeted execution command.")
                data = stream.read(self.config.max_file_bytes + 1)
                if len(data) > self.config.max_file_bytes:
                    fail("limit_exceeded", f"File grew beyond max_file_bytes={self.config.max_file_bytes} during reading; "
                        "use stat for metadata or a targeted execution command.")
                return data, info

    @staticmethod
    def _decode(data):
        encoding = "utf-8"
        if data.startswith(codecs.BOM_UTF8):
            encoding = "utf-8-sig"
        elif data.startswith(codecs.BOM_UTF16_LE):
            encoding, data = "utf-16-le", data[2:]
        elif data.startswith(codecs.BOM_UTF16_BE):
            encoding, data = "utf-16-be", data[2:]
        try:
            text = data.decode(encoding)
        except UnicodeError:
            fail("unsupported_encoding", "File is not UTF-8 or BOM-marked UTF-16 text. Use manage_files stat for metadata "
                "or an appropriate execution command; retrying read_file with a smaller page will not decode it.")
        if "\x00" in text:
            fail("unsupported_file", "Binary file cannot be read as text. Use manage_files stat for metadata "
                "or a format-specific execution command rather than retrying read_file.")
        return text, encoding

    def _expected(self, expected, data):
        if expected is not None and version(data) != expected:
            fail("version_conflict", "File does not match expected_version; inspect the latest contents before retrying.")

    def _output(self, record, text=None, **metadata):
        return ToolOutput(structured_content=record, content=() if text is None else (TextContent(text=text),),
            tool_metadata={"file": metadata} if metadata else {})

    def _read(self, path, args, context):
        data, info = self._bytes(path)
        mime = mimetypes.guess_type(path.name)[0] or ""
        if mime in {"image/png", "image/jpeg", "image/gif", "image/webp"}:
            return ToolOutput(content=(ImageContent(source=DataSource(data=base64.b64encode(data).decode(),
                mime_type=mime)),), structured_content={"path": str(path), "version": version(data), "mime_type": mime,
                    "size_bytes": len(data), "modified_at_ns": info.st_mtime_ns})
        text, encoding = self._decode(data)
        lines = text.splitlines(keepends=True)
        start, column = args["start_line"], args.get("start_column", 1)
        maximum = min(args["max_bytes"], self.config.max_output_bytes)
        selected, size, next_line, next_column = [], 0, None, None
        end = start - 1
        for index in range(start - 1, min(len(lines), start - 1 + args["max_lines"])):
            line = lines[index][column - 1:]
            prefix = f"{index + 1}: " if column == 1 else f"{index + 1}:{column}: "
            remaining = maximum - size - len(prefix.encode())
            encoded = line.encode("utf-8")
            if len(encoded) > remaining:
                fragment = encoded[:max(0, remaining)].decode("utf-8", errors="ignore")
                if not fragment:
                    if not selected:
                        fail("invalid_limit", f"max_bytes must fit a numbered character; current output cap is {self.config.max_output_bytes} bytes.")
                    next_line, next_column = index + 1, column
                    break
                selected.append(prefix + fragment)
                end = index + 1
                next_line, next_column = index + 1, column + len(fragment)
                break
            selected.append(prefix + line)
            size += len(prefix.encode()) + len(encoded)
            end = index + 1
            column = 1
        else:
            if end < len(lines):
                next_line, next_column = end + 1, 1
        record = dict(path=str(path), version=version(data), start_line=start, end_line=end,
            total_lines=len(lines), next_line=next_line, truncated=next_line is not None,
            size_bytes=len(data), modified_at_ns=info.st_mtime_ns)
        if next_column is not None:
            record["next_column"] = next_column
        return self._output(record, "".join(selected), encoding=encoding)

    def _change(self, path, before, after, *, create_parents=False):
        if len(after) > self.config.max_file_bytes:
            fail("limit_exceeded", "New content exceeds the configured byte limit.")
        try:
            old_text = self._decode(before)[0] if before is not None else ""
        except ToolBusinessError as error:
            if error.code not in {"unsupported_file", "unsupported_encoding"}:
                raise
            old_text = None
        new_text = self._decode(after)[0]
        with self._parent(path, create=create_parents) as (directory, name):
            temporary = ".autoagent-" + uuid4().hex
            temp_path = temporary if directory is not None else str(path.parent / temporary)
            descriptor = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=directory)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    if before is not None:
                        info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                        if not stat.S_ISREG(info.st_mode):
                            fail("unsupported_file", "Only regular files can be overwritten.")
                        if hasattr(os, "fchmod"):
                            os.fchmod(stream.fileno(), stat.S_IMODE(info.st_mode))
                    stream.write(after)
                    stream.flush()
                    os.fsync(stream.fileno())
                # Recheck after writing the temp file; external writers need not obey our lock.
                if before is None:
                    # link is an atomic no-clobber create on the same filesystem.
                    os.link(temp_path, name, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
                else:
                    latest, _ = self._bytes(path)
                    if latest != before:
                        fail("version_conflict", "File changed during edit; no replacement was committed.")
                    os.replace(temp_path, name, src_dir_fd=directory, dst_dir_fd=directory)
            finally:
                try:
                    os.unlink(temp_path, dir_fd=directory)
                except FileNotFoundError:
                    pass
        diff = "".join(difflib.unified_diff(old_text.splitlines(True), new_text.splitlines(True),
            fromfile=str(path), tofile=str(path))) if old_text is not None else None
        limit = self.config.max_output_bytes
        rendered = diff.encode()[:limit].decode("utf-8", errors="ignore") if diff is not None else None
        return self._output(dict(path=str(path), version=version(after), change="created" if before is None else "updated"),
            diff=rendered, diff_truncated=diff is not None and len(diff.encode()) > limit,
            path=str(path), operation="create" if before is None else "edit")

    def _write(self, path, args):
        before = None
        expected = args["expected_version"]
        if not path.exists() and expected is not None:
            fail("version_conflict", "expected_version requires an existing file, but the file is missing.")
        if path.exists():
            if not args["overwrite"]:
                fail("already_exists", "File already exists; use overwrite=true to replace all content, or edit_file for exact changes.")
            before, _ = self._bytes(path)
            self._expected(expected, before)
        return self._change(path, before, args["content"].encode(), create_parents=args["create_parents"])

    def _edit(self, path, args):
        before, _ = self._bytes(path)
        self._expected(args["expected_version"], before)
        text, encoding = self._decode(before)
        changes = []
        for edit in args["edits"]:
            positions, offset = [], 0
            while (index := text.find(edit["old_text"], offset)) >= 0:
                positions.append(index)
                offset = index + len(edit["old_text"])
            if not positions:
                fail("no_match", "old_text does not occur in the file. Read the relevant lines and use exact text, "
                    "including whitespace but without read_file line labels. No edits were applied.")
            if len(positions) != 1 and not edit["replace_all"]:
                fail("ambiguous_match", f"old_text matches {len(positions)} times; include surrounding lines to make it unique, "
                    "or explicitly set replace_all=true. No edits were applied.")
            changes.extend((index, index + len(edit["old_text"]), edit["new_text"]) for index in positions)
        changes.sort()
        if any(left[1] > right[0] for left, right in zip(changes, changes[1:])):
            fail("overlapping_edits", "Edits overlap; combine them into a single replacement.")
        for start, end, replacement in reversed(changes):
            text = text[:start] + replacement + text[end:]
        encoded = text.encode(encoding)
        if encoding in {"utf-16-le", "utf-16-be"}:
            encoded = before[:2] + encoded
        return self._change(path, before, encoded)

    def _directory_stamp(self, path):
        # Filesystems can coalesce timestamps. Include a streaming, order-free
        # membership checksum so a same-tick add/remove still invalidates cursors.
        count = checksum = 0
        with self._parent(path / ".") as (parent, name):
            descriptor = os.open(name, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent) if os.scandir in os.supports_fd else None
            try:
                info = os.fstat(descriptor) if descriptor is not None else path.stat(follow_symlinks=False)
                if not stat.S_ISDIR(info.st_mode):
                    fail("listing_changed", "A listed directory changed; restart without cursor.")
                with os.scandir(descriptor if descriptor is not None else str(path)) as entries:
                    for entry in entries:
                        count += 1
                        checksum = (checksum + int.from_bytes(hashlib.blake2b(os.fsencode(entry.name), digest_size=16).digest())) % (1 << 128)
            finally:
                if descriptor is not None:
                    os.close(descriptor)
        return [info.st_dev, info.st_ino, info.st_mtime_ns, count, f"{checksum:032x}"]

    def _scope(self, path, args):
        return version(json.dumps(dict(path=str(path), binding=self.version,
            arguments={key: value for key, value in args.items() if key not in {"cursor", "limit"}}), sort_keys=True).encode())

    def _resume(self, path, args):
        if not args.get("cursor"):
            return (), None, None
        try:
            if len(args["cursor"]) > 65536:
                raise ValueError
            value = json.loads(base64.urlsafe_b64decode(args["cursor"].encode()))
            if value["scope"] != self._scope(path, args):
                fail("listing_changed", "Cursor belongs to different path, filters, search options or service configuration. "
                    "Restart without cursor; only limit may change between pages.")
            after = tuple(value["after"])
            if not after or any(not isinstance(part, str) or part in ("", ".", "..") or "/" in part or "\\" in part for part in after):
                raise ValueError
            if len(value["directories"]) > self.config.max_depth + 2:
                raise ValueError
            for relative, stamp in value["directories"].items():
                parts = Path(relative).parts
                if Path(relative).is_absolute() or ".." in parts or "\\" in relative or after[:len(parts)] != parts:
                    raise ValueError
                directory = path / relative
                if self._resolve(str(directory)) != directory or self._directory_stamp(directory) != stamp:
                    fail("listing_changed", "Directory contents changed since this cursor was issued; restart without cursor.")
            line = value.get("line")
            if line is not None and (type(line) is not int or line < 0):
                raise ValueError
            return after, line, value.get("file_version")
        except (ValueError, TypeError, KeyError, AttributeError, OSError):
            fail("listing_changed", "Cursor is invalid or expired; restart without cursor and use the cursor returned by this tool.")

    def _ignore_spec(self, directory, context):
        from pathspec import GitIgnoreSpec

        ignore = directory / ".gitignore"
        if not ignore.is_file():
            return None
        target = self._resolve(str(ignore))
        if self.evaluate(self._target("read", target), context).action == "allow":
            body, _ = self._bytes(target)
            return GitIgnoreSpec.from_lines(self._decode(body)[0].splitlines())
        return None

    def _cursor(self, path, args, after, state, *, line=None):
        if not after:
            return None
        directories = {relative: stamp for relative, stamp in state.get("directories", {}).items()
            if not Path(relative).parts or after[:len(Path(relative).parts)] == Path(relative).parts}
        value = dict(scope=self._scope(path, args), after=after, directories=directories)
        if line is not None:
            value["line"] = line
            if state.get("file_path") == after:
                value["file_version"] = state["file_version"]
        return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode()

    def _entries(self, path, args, context, *, after=(), include_after=False, state=None, deadline=None):
        """Lazy depth-first pages, with bounded directory batches and no symlink descent."""
        state = state if state is not None else {}
        state.setdefault("directories", {})
        state.setdefault("scanned", 0)
        maximum_depth = min(args.get("max_depth") if args.get("max_depth") is not None else self.config.max_depth,
                            self.config.max_depth)

        def visit(directory, relative, inherited, depth):
            if state.get("stop"):
                return
            state["directories"][relative.as_posix()] = self._directory_stamp(directory)
            rules = list(inherited)
            if args["respect_gitignore"] and (spec := self._ignore_spec(directory, context)) is not None:
                rules.append((directory, spec))
            prefix = relative.parts
            start = after[len(prefix)] if len(after) > len(prefix) and after[:len(prefix)] == prefix else None
            inclusive = start is not None
            # Only a bounded batch of DirEntry objects is retained at each depth.
            while not state.get("stop"):
                with self._parent(directory / ".") as (parent, name):
                    descriptor = os.open(name, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                        dir_fd=parent) if os.scandir in os.supports_fd else None
                    try:
                        with os.scandir(descriptor if descriptor is not None else str(directory)) as scan:
                            children = heapq.nsmallest(257, (item for item in scan if start is None or item.name > start or inclusive and item.name == start), key=lambda item: item.name)
                        for child in children[:256]:
                            name = child.name
                            if name == ".git" and args["respect_gitignore"] or not args["include_hidden"] and name.startswith("."):
                                continue
                            item, rel = directory / name, relative / name
                            key = rel.parts
                            ancestor = after[:len(key)] == key
                            if key < after and not ancestor or key == after and not include_after and not ancestor:
                                continue
                            is_directory = child.is_dir(follow_symlinks=False)
                            ignored = False
                            for base, spec in rules:
                                check = spec.check_file(item.relative_to(base).as_posix() + ("/" if is_directory else ""))
                                if check.include is not None:
                                    ignored = check.include
                            if ignored or any(glob_match(rel.as_posix(), pattern) for pattern in args["exclude"]):
                                continue
                            if deadline is not None and time.monotonic() >= deadline:
                                state.update(stop=True, reason="timeout")
                                return
                            if key > after or include_after and key == after:
                                if state["scanned"] >= self.config.max_entries:
                                    state.update(stop=True, reason="max_entries")
                                    return
                                state["scanned"] += 1
                                try:
                                    info = child.stat(follow_symlinks=False)
                                except FileNotFoundError:
                                    continue
                                yield item, rel.as_posix(), "symlink" if child.is_symlink() else "directory" if is_directory else "file", info
                            if args["recursive"] and is_directory:
                                if not args.get("include_generated", False) and name in self.config.generated_directories:
                                    continue
                                access = self.evaluate(self._target("inspect", self._resolve(str(item))), context)
                                if access.action == "allow":
                                    if depth >= maximum_depth:
                                        state.setdefault("depth_limited_paths", [])
                                        if len(state["depth_limited_paths"]) < 8:
                                            state["depth_limited_paths"].append(rel.as_posix())
                                    else:
                                        yield from visit(item, rel, rules, depth + 1)
                                else:
                                    blocked = state.setdefault("protected_directories", [])
                                    if len(blocked) < 8:
                                        blocked.append(dict(path=rel.as_posix(), list=access.action, content_searched=False))
                            if state.get("stop"):
                                return
                    finally:
                        if descriptor is not None:
                            os.close(descriptor)
                if len(children) <= 256:
                    break
                start, inclusive = children[255].name, False
        inherited = []
        if args["respect_gitignore"] and path.is_relative_to(self.root):
            for directory in reversed(path.parents):
                if not directory.is_relative_to(self.root):
                    continue
                if (spec := self._ignore_spec(directory, context)) is not None:
                    inherited.append((directory, spec))
        yield from visit(path, Path(), inherited, 0)

    def _list(self, path, args, context):
        after, _, _ = self._resume(path, args)
        state, selected, used = {}, [], 0
        position, reason = after, None
        iterator = self._entries(path, args, context, after=after, state=state)
        try:
            for _, relative, kind, info in iterator:
                if not glob_match(relative, args["pattern"]):
                    position = Path(relative).parts
                    continue
                row = dict(path=relative, type=kind, size_bytes=info.st_size)
                size = len(json.dumps(row).encode())
                if len(selected) >= args["limit"] or used + size > self.config.max_output_bytes:
                    reason = "page_limit" if len(selected) >= args["limit"] else "output_bytes"
                    break
                selected.append(row)
                used += size
                position = Path(relative).parts
        finally:
            iterator.close()
        reason = reason or state.get("reason")
        cursor = self._cursor(path, args, position, state) if reason else None
        record = dict(path=str(path), entries=selected, cursor=cursor, has_more=bool(cursor),
            truncated=bool(reason or state.get("depth_limited_paths") or state.get("protected_directories")))
        if state.get("protected_directories"):
            record["protected_directories"] = state["protected_directories"]
        if reason or state.get("depth_limited_paths"):
            record.update(truncated=True, reason=reason or "max_depth")
            if state.get("depth_limited_paths"):
                record["depth_limited_paths"] = state["depth_limited_paths"]
            record["next_step"] = "Continue with cursor and unchanged filters; limit may change." if cursor else "Open a narrower path to inspect deeper entries."
        return self._output(record)

    def _search(self, path, args, context):
        deadline = time.monotonic() + self.config.search_timeout_seconds
        after, resume_line, expected_file_version = self._resume(path, args)
        search = None
        if args["regex"] or not args["case_sensitive"]:
            import regex
            try:
                search = regex.compile(args["query"] if args["regex"] else regex.escape(args["query"]),
                    0 if args["case_sensitive"] else regex.IGNORECASE | regex.FULLCASE)
            except regex.error as error:
                fail("invalid_regex", str(error))
        rows, skipped = [], []
        matches, total_bytes, position, next_line, reason = 0, 0, after, None, None
        state = {}
        traversal = dict(args, recursive=True)
        explicit_file = path.is_file()
        entries = iter([] if after and resume_line is None else [(path, path.name, "file", path.stat())]) if explicit_file else self._entries(
            path, traversal, context, after=after, include_after=resume_line is not None, state=state, deadline=deadline)

        def record():
            value = dict(path=str(path.parent if explicit_file else path), results=rows, match_count=matches)
            if state.get("protected_directories"):
                value["protected_directories"] = state["protected_directories"]
            if skipped:
                value["skipped"] = skipped
            return value

        def fits(row):
            return len(json.dumps(record() | {"results": [*rows, row]}).encode()) <= self.config.max_output_bytes

        try:
            for item, relative, kind, info in entries:
                key = Path(relative).parts
                begin = resume_line if key == after and resume_line is not None else 0
                if time.monotonic() >= deadline:
                    position, next_line, reason = key, begin if kind == "file" else None, "timeout"
                    break
                if kind != "file" or not any(glob_match(relative, pattern) for pattern in args["include"]):
                    position = key
                    continue
                # Directory authorization covers the search root. Each file
                # must also allow content access before testing the query.
                if not explicit_file and self.evaluate(self._target("read", item), context).action != "allow":
                    position = key
                    continue
                if info.st_size > self.config.max_file_bytes or info.st_size > self.config.max_scan_bytes:
                    if len(skipped) < 10:
                        skipped.append(dict(path=relative, reason="file_too_large"))
                    position = key
                    continue
                if total_bytes + info.st_size > self.config.max_scan_bytes:
                    position, next_line, reason = key, begin, "max_scan_bytes"
                    break
                try:
                    data, _ = self._bytes(item)
                    current_version = version(data)
                    if key == after and resume_line is not None and expected_file_version is not None and current_version != expected_file_version:
                        fail("listing_changed", "Search file changed between pages; restart without cursor.")
                    state.update(file_path=key, file_version=current_version)
                    total_bytes += len(data)
                    lines = self._decode(data)[0].splitlines()
                except ToolBusinessError as error:
                    if error.code == "listing_changed":
                        raise
                    if len(skipped) < 10:
                        skipped.append(dict(path=relative, reason=error.code))
                    position = key
                    continue
                file_matches = 0
                for index in range(begin, len(lines)):
                    line = lines[index]
                    if time.monotonic() >= deadline:
                        reason, next_line = "timeout", index
                        break
                    try:
                        match = search.search(line, timeout=min(self.config.regex_timeout_seconds,
                            max(0.001, deadline - time.monotonic()))) if search else None
                    except TimeoutError:
                        fail("regex_timeout", "Regex exceeded its execution limit; simplify the pattern.")
                    column = match.start() if search and match else -1 if search else line.find(args["query"])
                    if column < 0:
                        continue
                    if args["mode"] == "matches":
                        row = dict(path=relative, line=index + 1, column=column + 1)
                        if not fits(row):
                            reason, next_line = "output_bytes", index
                            break
                        rows.append(row)
                    elif args["mode"] == "files":
                        row = dict(path=relative)
                        if not fits(row):
                            reason, next_line = "output_bytes", index
                            break
                        rows.append(row)
                    matches += 1
                    file_matches += 1
                    if args["mode"] == "files":
                        if matches >= args["max_matches"]:
                            reason = "max_matches"
                        break
                    if matches >= args["max_matches"] or args["mode"] == "matches" and len(rows) >= args["limit"]:
                        reason, next_line = "max_matches" if matches >= args["max_matches"] else "page_limit", index + 1
                        break
                if args["mode"] == "count" and file_matches:
                    row = dict(path=relative, count=file_matches)
                    if not fits(row):
                        matches -= file_matches
                        reason, next_line = "output_bytes", begin
                    else:
                        rows.append(row)
                position = key
                if next_line is not None and next_line >= len(lines):
                    next_line = None
                if reason:
                    break
                if len(rows) >= args["limit"]:
                    reason = "page_limit"
                    break
        finally:
            if hasattr(entries, "close"):
                entries.close()
        if explicit_file and next_line is None and reason in {"page_limit", "max_matches"}:
            reason = None
        reason = reason or state.get("reason")
        value = record()
        value["count_scope"] = "page"
        value["truncated"] = bool(reason or state.get("depth_limited_paths"))
        value["has_more"] = False
        if reason or state.get("depth_limited_paths"):
            value.update(truncated=True, reason=reason or "max_depth")
            cursor = self._cursor(path, args, position, state, line=next_line) if reason else None
            if cursor:
                value["cursor"] = cursor
                value["has_more"] = True
            if state.get("depth_limited_paths"):
                value["depth_limited_paths"] = state["depth_limited_paths"]
            value["next_step"] = "Continue with cursor and unchanged filters (limit may change); counts are per page." if cursor else "Narrow the path or include patterns to continue."
        elif not args.get("cursor") and not skipped and not state.get("protected_directories"):
            value["total_count"] = matches
        return self._output(value)

    def _manage(self, paths, args):
        operation, path = args["operation"], paths[0]
        if path == self.root and operation in {"move", "delete"}:
            fail("invalid_path", "The Workspace root cannot be moved or deleted.")
        if operation == "stat":
            info = path.stat()
            return self._output(dict(path=str(path), type="directory" if path.is_dir() else "file",
                size_bytes=info.st_size, modified_at_ns=info.st_mtime_ns))
        if operation == "mkdir":
            with self._parent(path, create=args["parents"]) as (directory, name):
                os.mkdir(name, dir_fd=directory)
        elif operation in {"move", "copy"}:
            destination = paths[1]
            if path == destination:
                fail("invalid_path", "Source and destination must differ.")
            if path.is_dir():
                fail("unsupported_operation", "Directory transfers are not supported; move or copy explicit files.")
            data, _ = self._bytes(path)
            if operation == "move":
                self._expected(args["expected_version"], data)
            # Atomic no-clobber destination creation; never overwrite another Agent's file.
            with self._parent(path) as (source_dir, source_name), self._parent(destination) as (target_dir, target_name):
                if operation == "move":
                    os.link(source_name, target_name, src_dir_fd=source_dir, dst_dir_fd=target_dir, follow_symlinks=False)
                    os.unlink(source_name, dir_fd=source_dir)
                else:
                    descriptor = os.open(target_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=target_dir)
                    try:
                        with os.fdopen(descriptor, "wb") as stream:
                            stream.write(data)
                    except BaseException:
                        os.unlink(target_name, dir_fd=target_dir)
                        raise
            return self._output(dict(operation=operation, path=str(path), destination=str(destination),
                version=version(data)))
        elif operation == "delete":
            if path.is_dir():
                with self._parent(path) as (directory, name):
                    os.rmdir(name, dir_fd=directory)
            else:
                data, _ = self._bytes(path)
                self._expected(args["expected_version"], data)
                with self._parent(path) as (directory, name):
                    os.unlink(name, dir_fd=directory)
        return self._output(dict(operation=operation, path=str(path)))

    def execute(self, name, arguments, context):
        plans = context.operations
        if not plans:
            fail("missing_plan", "File Tools require ToolSet authorization.")
        args = arguments.get("request", arguments)
        paths = [Path(value.resource) for value in plans]
        try:
            with resource_locks(paths):
                for plan in plans:
                    self._verify(plan)
                if name == "read_file":
                    return self._read(paths[0], args, context)
                if name == "write_file":
                    return self._write(paths[0], args)
                if name == "edit_file":
                    return self._edit(paths[0], args)
                if name == "list_files":
                    return self._list(paths[0], args, context)
                if name == "search_files":
                    return self._search(paths[0], args, context)
                return self._manage(paths, args)
        except FileNotFoundError:
            fail("not_found", "The requested file or directory does not exist.")
        except FileExistsError:
            fail("already_exists", "The destination already exists.")
        except PermissionError:
            fail("permission_denied", "The operating system denied this file operation.")
        except (OSError, UnicodeError) as error:
            if isinstance(error, OSError) and error.errno in {errno.ENOTEMPTY, errno.EEXIST} and args.get("operation") == "delete":
                fail("directory_not_empty", "Directory is not empty. List its contents and explicitly remove the intended files first; "
                    "manage_files never recursively deletes a directory.")
            fail("file_operation_failed", str(error))
