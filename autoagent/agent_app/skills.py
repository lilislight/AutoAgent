"""Product-only Skill switches stored in SKILL.md, never per-Skill database rows."""

import hashlib
import json
import os
from pathlib import Path
import stat
from threading import RLock

from autoagent.agent.files import FileToolsConfig, LocalFileService
from autoagent.agent.files.service import resource_locks
from autoagent.agent.tools import ToolBusinessError

KEY = 'autoagent-app-enabled'
_lock = RLock()


def enabled(skill):
    return skill.metadata.metadata.get(KEY, 'true').strip().lower() != 'false'


def location(skill):
    source = skill.source
    value = getattr(getattr(source, 'snapshot', None), 'source_location', None) or getattr(source, 'location', '')
    path = Path(value)
    return path if path.is_absolute() and path.name == 'SKILL.md' else None


def _managed_path(skill, roots):
    path = location(skill)
    if path is None or '..' in path.parts:
        return None
    for root in roots:
        try:
            if (path.is_relative_to(Path(root).resolve(strict=True))
                    and not any(part.is_symlink() or part.is_junction() for part in (path, *path.parents))):
                return path
        except (OSError, RuntimeError):
            continue
    return None


def _writable(path, info):
    bits = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
    return (stat.S_ISREG(info.st_mode) and bool(info.st_mode & bits)
            and bool(path.parent.stat().st_mode & bits) and os.access(path.parent, os.W_OK)
            and os.access(path, os.W_OK))


def editable(skill, roots):
    path = _managed_path(skill, roots)
    if path is not None:
        try:
            return _writable(path, path.stat(follow_symlinks=False))
        except OSError:
            pass
    return False


def _read_document(files, path):
    try:
        data, info = files._bytes(path)
    except ToolBusinessError as error:
        raise ValueError(str(error)) from error
    if not _writable(path, info):
        raise ValueError('This Skill document is read-only or has no managed local source.')
    return data, info


def _identity(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def set_enabled(skill, value, *, roots):
    """Change only the extension value; preserve other frontmatter and Markdown."""
    if type(value) is not bool:
        raise ValueError('Skill enabled must be a boolean.')
    path = _managed_path(skill, roots)
    if path is None:
        raise ValueError('This Skill document is read-only or has no managed local source.')
    import yaml
    from autoagent.agent.skills.parser import parse_skill
    with _lock, resource_locks((path,)):
        maximum = skill.limits.max_document_bytes
        files = LocalFileService(path.parent, config=FileToolsConfig(max_file_bytes=maximum))
        data, original = _read_document(files, path)
        if hashlib.sha256(data).hexdigest() != skill.load().revision:
            raise ValueError('Skill document changed; refresh its configuration and try again.')
        text = data.decode('utf-8-sig')
        lines = text.splitlines(keepends=True)
        end = next(index for index in range(1, len(lines)) if lines[index].strip() == '---')
        front = ''.join(lines[1:end])
        node = yaml.compose(front)
        metadata = next((v for k, v in node.value if k.value == 'metadata'), None)
        quoted = json.dumps('true' if value else 'false')
        newline = '\r\n' if '\r\n' in text else '\n'
        if metadata is None:
            front = front.rstrip('\r\n') + newline + 'metadata:' + newline + f'  {KEY}: {quoted}' + newline
        else:
            flag = next((v for k, v in metadata.value if k.value == KEY), None)
            if flag is not None:
                front = front[:flag.start_mark.index] + quoted + front[flag.end_mark.index:]
            elif metadata.flow_style:
                index = metadata.end_mark.index - 1
                prefix = ', ' if metadata.value else ''
                front = front[:index] + prefix + KEY + ': ' + quoted + front[index:]
            else:
                index = metadata.end_mark.index
                indent = metadata.value[0][0].start_mark.column if metadata.value else 2
                front = front[:index] + ' ' * indent + KEY + ': ' + quoted + newline + front[index:]
        updated = (lines[0] + front + ''.join(lines[end:])).encode('utf-8')
        if data.startswith(b'\xef\xbb\xbf'):
            updated = b'\xef\xbb\xbf' + updated
        parse_skill(updated, directory_name=skill.metadata.name)
        try:
            current, info = _read_document(files, path)
            if current != data or _identity(original) != _identity(info):
                raise ValueError('Skill document changed; refresh its configuration and try again.')
            files._change(path, data, updated)
        except ToolBusinessError as error:
            raise ValueError(str(error)) from error
