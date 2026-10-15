"""Durable trace positions, scoped to one store generation and Invocation."""
import base64
import binascii
import json


class TraceResetRequired(ValueError):
    """A cached trace belongs to a replaced or rebuilt store."""


def encode_trace_cursor(store_id, iid, frontier):
    value = json.dumps([1, store_id, iid, frontier], separators=(',', ':'))
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip('=')


def decode_trace_cursor(value, store_id, iid, frontier):
    try:
        if not isinstance(value, str) or len(value) > 16384:
            raise ValueError
        row = json.loads(base64.b64decode(value + '=' * (-len(value) % 4), altchars=b'-_', validate=True))
        if not isinstance(row, list) or len(row) != 4 or type(row[0]) is not int or row[0] != 1 or not isinstance(row[1], str) or row[2] != iid or type(row[3]) is not int or not 0 <= row[3] <= 9007199254740991:
            raise ValueError
    except (ValueError, TypeError, UnicodeDecodeError, binascii.Error):
        raise ValueError('Invalid trace cursor or cursor belongs to another execution.') from None
    if row[1] != store_id or row[3] > frontier:
        raise TraceResetRequired('Saved trace data changed. Reload this execution.')
    return row[3]
