"""Product-only Skill switches stored in SKILL.md, never per-Skill database rows."""

import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
from threading import RLock

KEY = 'autoagent-app-enabled'
_lock = RLock()


def enabled(skill):
    return skill.metadata.metadata.get(KEY, 'true').strip().lower() != 'false'


def location(skill):
    source = skill.source
    value = getattr(getattr(source, 'snapshot', None), 'source_location', None) or getattr(source, 'location', '')
    path = Path(value)
    return path if path.is_absolute() and path.name == 'SKILL.md' else None


def editable(skill, roots):
    path = location(skill)
    if path is None:
        return False
    try:
        resolved = path.resolve(strict=True)
        return any(resolved.is_relative_to(Path(root).resolve(strict=True)) for root in roots) and bool(
            resolved.stat().st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)) and bool(
                resolved.parent.stat().st_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)) and os.access(resolved, os.W_OK) and os.access(resolved.parent, os.W_OK)
    except OSError:
        return False


def set_enabled(skill, value, *, roots):
    """Change only the extension value; preserve other frontmatter and Markdown."""
    if type(value) is not bool:
        raise ValueError('Skill enabled must be a boolean.')
    if not editable(skill, roots):
        raise ValueError('This Skill document is read-only or has no managed local source.')
    import yaml
    from autoagent.agent.skills.parser import parse_skill
    with _lock:
        path = location(skill).resolve(strict=True)
        data = path.read_bytes()
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
        mode = stat.S_IMODE(path.stat().st_mode)
        descriptor, temporary = tempfile.mkstemp(prefix='.SKILL.', dir=path.parent)
        try:
            with os.fdopen(descriptor, 'wb') as stream:
                stream.write(updated)
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(temporary, mode)
            if path.read_bytes() != data:
                raise ValueError('Skill document changed; refresh and try again.')
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
