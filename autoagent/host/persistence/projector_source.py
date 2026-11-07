"""Read projector metadata without decoding input/output/context Artifacts."""
from .reading import MetadataReader
from .projections import BUSINESS_FIELDS
from .storage import SOURCES


class MetadataRecordReader:
    """Recursion uses methods, so no closure cycle retains an Event reader."""
    def __init__(self, reader, availability):
        self.reader = reader
        self.availability = availability

    async def error(self, path):
        if not await self.reader.present(path):
            return None
        result = {}
        for key in ('type', 'code', 'phase'):
            try:
                result[key] = await self.reader.read((*path, key), deep=False)
            except KeyError:
                result[key] = None
        return result

    async def request(self, path, kind):
        if not await self.reader.present(path):
            return None, kind not in ('child', 'any')
        if kind == 'timer':
            return await self.reader.read((*path, 'deadline_at_us'), deep=False), True
        if kind == 'any':
            cases = await self.reader.read((*path, 'cases'), deep=False)
            deadline, independent = None, False
            for index in range(len(cases)):
                case_path = (*path, 'cases', index)
                case_kind = await self.reader.read((*case_path, 'kind'), deep=False)
                due, external = await self.request((*case_path, 'condition'), case_kind)
                independent |= external
                if due is not None:
                    deadline = due if deadline is None else min(deadline, due)
            return deadline, independent
        return None, kind != 'child'

    async def walk(self, item, path):
        if isinstance(item, list):
            return [await self.walk(value, (*path, index)) for index, value in enumerate(item)]
        if not isinstance(item, dict):
            return item
        result = {}
        if 'request' in item:
            deadline, independent = await self.request((*path, 'request'), item.get('kind', item.get('wait_kind')))
            deadline = min((d for d in (deadline, item.get('deadline_at_us')) if d is not None), default=None)
            result.update(deadline_at_us=deadline, has_independent_case=independent)
        for key, value in item.items():
            if key == 'deadline_at_us' and 'request' in item:
                continue
            target = (*path, key)
            operation = key == 'value' and isinstance(item.get('op'), str) and isinstance(item.get('path'), list) and item['path']
            business = item['path'][-1] if operation else key
            if business in BUSINESS_FIELDS:
                available = await self.reader.present(target) if self.availability or business == 'pending_recovery' else False
                result[key] = available if business == 'pending_recovery' else True if available else None
            elif business in ('error', 'fault'):
                result[key] = await self.error(target)
            else:
                result[key] = await self.walk(value, target)
        return result


async def metadata_record(storage, conn, kind, identity, *, availability=False, row=None, refs=None):
    """Preserve light_record semantics while business bodies remain external."""
    if kind == 'runtime_event' and row is None:
        name, key, _ = SOURCES[kind]
        row = await storage.row(conn, name, **{key: identity})
    reader = await MetadataReader.open(storage, conn, kind, identity, row=row, refs=refs)
    metadata = MetadataRecordReader(reader, availability)
    if kind == 'runtime_event':
        record = {key: row[key] for key in ('session_id', 'invocation_id', 'sequence', 'event_name', 'occurred_at_us', 'schema_version')}
        record.update(id=identity,
                      payload=await metadata.walk(reader.record['payload'], ('payload',)),
                      delta=await metadata.walk(reader.record.get('delta'), ('delta',)))
        return record
    return await metadata.walk(reader.record, ())
