"""Product-only Skill switches stored in SKILL.md, never per-Skill database rows."""

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import stat
from threading import RLock
from uuid import uuid4

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
    if (path is None or '..' in path.parts or os.open not in os.supports_dir_fd
            or not hasattr(os, 'O_NOFOLLOW')):
        return None
    for root in roots:
        try:
            if path.is_relative_to(Path(root).resolve(strict=True)):
                return path
        except (OSError, RuntimeError):
            continue
    return None


@contextmanager
def _parent_descriptor(path):
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    parent = os.open(path.anchor, flags)
    try:
        for part in path.parent.parts[1:]:
            child = os.open(part, flags, dir_fd=parent)
            os.close(parent)
            parent = child
        yield parent
    finally:
        os.close(parent)


def _writable(parent, name, info):
    bits = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
    return (stat.S_ISREG(info.st_mode) and bool(info.st_mode & bits)
            and bool(os.fstat(parent).st_mode & bits) and os.access('.', os.W_OK, dir_fd=parent)
            and os.access(name, os.W_OK, dir_fd=parent))


def editable(skill, roots):
    path = _managed_path(skill, roots)
    if path is not None:
        try:
            with _parent_descriptor(path) as parent:
                return _writable(parent, path.name, os.stat(path.name, dir_fd=parent, follow_symlinks=False))
        except OSError:
            pass
    return False


def _read_document(parent, name, maximum):
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    with os.fdopen(descriptor, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not _writable(parent, name, info):
            raise ValueError('This Skill document is read-only or has no managed local source.')
        data = stream.read(maximum + 1)
        if len(data) > maximum:
            raise ValueError('Skill document exceeds its size limit.')
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
    with _lock, _parent_descriptor(path) as parent:
        maximum = skill.limits.max_document_bytes
        data, original = _read_document(parent, path.name, maximum)
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
        temporary = '.SKILL.' + uuid4().hex
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=parent)
        try:
            with os.fdopen(descriptor, 'wb') as stream:
                stream.write(updated)
                os.fchmod(stream.fileno(), stat.S_IMODE(original.st_mode))
                stream.flush()
                os.fsync(stream.fileno())
            with _parent_descriptor(path) as current_parent:
                if not os.path.samestat(os.fstat(parent), os.fstat(current_parent)):
                    raise ValueError('Skill directory changed; refresh its configuration and try again.')
            current, info = _read_document(parent, path.name, maximum)
            if current != data or _identity(original) != _identity(info):
                raise ValueError('Skill document changed; refresh its configuration and try again.')
            os.replace(temporary, path.name, src_dir_fd=parent, dst_dir_fd=parent)
        finally:
            try:
                os.unlink(temporary, dir_fd=parent)
            except FileNotFoundError:
                pass
