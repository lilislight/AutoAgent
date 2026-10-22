"""Identical deterministic business work for both Server versions."""
import asyncio
import hashlib
from pydantic import BaseModel, ConfigDict, Field


class Payload(BaseModel):
    model_config = ConfigDict(extra='forbid')
    step: int = 0
    blob: str = ''
    rows: list[dict[str, int]] = Field(default_factory=list)


class Batch(BaseModel):
    model_config = ConfigDict(extra='forbid')
    items: list[Payload]


class Summary(BaseModel):
    model_config = ConfigDict(extra='forbid')
    step: int
    count: int
    bytes: int
    digest: str


def summarize(values):
    digest = hashlib.sha256()
    total = step = 0
    for value in values:
        if not isinstance(value, Payload):
            value = Payload.model_validate(value)
        raw = value.blob.encode()
        digest.update(raw)
        total += len(raw)
        for row in value.rows:
            digest.update(f"{row['id']}:{row['value']};".encode())
        step += value.step
    return Summary(step=step, count=len(values), bytes=total, digest=digest.hexdigest())


def operations(case):
    size, shape = case.get('payload_bytes', 0), case.get('shape', 'text')

    async def entry(value: Payload) -> Payload:
        return value

    async def advance(value: Payload) -> Payload:
        await asyncio.sleep(case.get('delay_ms', 0) / 1000)
        step = value.step + 1
        if shape == 'rows':
            return Payload(step=step, rows=[{'id': i, 'value': i + step} for i in range(max(1, size // 30))])
        blob = (f'{step:012d}:' + 'x' * max(0, size - 13)) if size else ''
        return Payload(step=step, blob=blob)

    async def finish(value: Payload) -> Summary:
        return summarize([value])

    async def batch(value: Payload) -> Batch:
        return Batch(items=[Payload(step=i) for i in range(case.get('width', 1))])

    return entry, advance, finish, batch
