"""Contract recovery and real workflow measurements, with profiling kept separate."""
import gc
import json
import statistics
import time
import tracemalloc
from unittest.mock import patch
from typing import Literal
from typing_extensions import TypedDict, NotRequired
from pydantic import BaseModel, field_validator
from autoagent import AutoAgentApp, Node, Workflow
from autoagent.core.operators.contract import ValueContract


class Row(TypedDict):
    value: int | None
    label: Literal['a', 'b']
    note: NotRequired[str | None]


class NullableDocument(TypedDict):
    rows: list[Row]


class NullableBatch(TypedDict):
    rows: list[int] | None
    status: Literal["ready", "empty"]


class UnionDocument(TypedDict):
    rows: list[int | str]


class ModelRow(BaseModel):
    model_config = {'extra': 'forbid'}
    value: int | None
    label: Literal['a', 'b']


class ModelDocument(BaseModel):
    model_config = {'extra': 'forbid'}
    rows: list[ModelRow]


class CustomDocument(ModelDocument):
    @field_validator('rows')
    @classmethod
    def check_rows(cls, rows):
        return rows


async def nullable_identity(value: NullableDocument) -> NullableDocument:
    return value


async def batch_identity(value: NullableBatch) -> NullableBatch:
    return value


async def model_identity(value: ModelDocument) -> ModelDocument:
    return value


def measure(action):
    action()
    samples = []
    for _ in range(7):
        start = time.perf_counter_ns()
        action()
        samples.append((time.perf_counter_ns() - start) / 1e6)
    gc.collect()
    tracemalloc.start()
    try:
        action()
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    return {'samples_ms': samples, 'median_ms': statistics.median(samples), 'peak_bytes': peak}


def workflow_case(annotation, handler, record):
    contract = ValueContract.create(annotation, location='benchmark')
    value = contract.restore(record)
    app = AutoAgentApp()
    workflow = Workflow('contract-bench', nodes=[Node('entry', handler)])
    app.register_workflow(workflow)
    def run():
        result = app.invoke(workflow.id, value, session_id='bench')
        assert result.status == 'completed', result.error
    try:
        result = measure(run)
        original = ValueContract._restore_internal
        elapsed = 0
        calls = 0
        def timed(self, value):
            nonlocal elapsed, calls
            start = time.perf_counter_ns()
            try:
                return original(self, value)
            finally:
                elapsed += time.perf_counter_ns() - start
                calls += 1
        with patch.object(ValueContract, '_restore_internal', timed):
            start = time.perf_counter_ns()
            run()
            total = time.perf_counter_ns() - start
        result['profile'] = {'restore_calls': calls, 'restore_ms': elapsed / 1e6,
                             'workflow_ms': total / 1e6, 'restore_fraction': elapsed / total}
        return result
    finally:
        app.close()


def main():
    report = {'contracts': {}, 'workflows': {}}
    for size in (10, 10000):
        rows = [{'value': i if i % 2 else None, 'label': 'a'} for i in range(size)]
        batch = {'rows': list(range(size)), 'status': 'ready'}
        for annotation, record in ((NullableDocument, {'rows': rows}), (NullableBatch, batch),
                                   (UnionDocument, {'rows': [i if i % 2 else 'a' for i in range(size)]}),
                                   (ModelDocument, {'rows': rows}), (CustomDocument, {'rows': rows})):
            contract = ValueContract.create(annotation, location='benchmark')
            name = f'{annotation.__name__}_{size}'
            report['contracts'][name] = measure(lambda: contract._restore_internal(record))
            report['contracts'][name]['python_fast_path'] = contract._python_record_equivalent
        for annotation, handler in ((NullableDocument, nullable_identity), (ModelDocument, model_identity)):
            report['workflows'][f'{annotation.__name__}_{size}'] = workflow_case(annotation, handler, {'rows': rows})
        report['workflows'][f'NullableBatch_{size}'] = workflow_case(NullableBatch, batch_identity, batch)
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
