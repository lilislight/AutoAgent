"""Bounded line previews whose continuation positions remain raw byte offsets."""

from collections import deque


def prefix(data, size):
    return data[:max(0, size)].decode("utf-8", errors="ignore").encode("utf-8")


def preview(chunks, start, end, *, max_bytes, line_bytes, tail):
    output = deque()
    size = omitted = line_omitted = 0
    position = start
    lines = {"stdout": (0, False), "stderr": (0, False)}
    for offset, name, chunk in chunks:
        left, right = max(start, offset), min(end, offset + len(chunk))
        if left >= right:
            continue
        raw_position = left
        for part in chunk[left - offset:right - offset].splitlines(keepends=True):
            body = part.rstrip(b"\r\n")
            newline = part[len(body):]
            seen, marked = lines[name]
            kept = prefix(body, max(0, line_bytes - 4 - seen))
            dropped = len(body) - len(kept)
            shown = kept + (b"..." if dropped and not marked else b"") + newline
            if not tail and size + len(shown) > max_bytes:
                piece = prefix(kept, max_bytes - size)
                if piece:
                    output.append((name, piece))
                    position = raw_position + len(piece)
                return _result(output, position, omitted, line_omitted)
            if shown:
                output.append((name, shown))
            size += len(shown)
            line_omitted += dropped
            position = raw_position + len(part)
            raw_position = position
            lines[name] = (0, False) if newline else (seen + len(body), marked or bool(dropped))
            while tail and size > max_bytes:
                stream, data = output.popleft()
                count = min(len(data), size - max_bytes)
                rest = data[count:].decode("utf-8", errors="ignore").encode("utf-8")
                removed = len(data) - len(rest)
                size -= removed
                omitted += removed
                if rest:
                    output.appendleft((stream, rest))
    return _result(output, position, omitted, line_omitted)


def _result(output, position, omitted, line_omitted):
    streams = {"stdout": bytearray(), "stderr": bytearray()}
    for name, data in output:
        streams[name].extend(data)
    return {name: bytes(data).decode("utf-8") for name, data in streams.items()} | dict(
        next_cursor=position, preview_omitted_bytes=omitted, omitted_line_bytes=line_omitted)
