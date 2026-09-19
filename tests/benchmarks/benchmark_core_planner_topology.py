"""Planner retained-history queries and static topology lookup/construction costs."""
import gc
import inspect
import json
import statistics
import time
import tracemalloc
from dataclasses import replace
from types import MappingProxyType
from autoagent import AutoAgentApp, Node, Workflow
from autoagent.core.compiler.compiler import WorkflowCompiler
from autoagent.core.runtime import TransitionPlanner, SchedulerDelta
from autoagent.core.runtime._execution_index import ExecutionIndex
from autoagent.core.runtime.events import WaitRequested, NodeCompleted, ChildAwaitSuspended
from autoagent.core.runtime.state import ChildInvocationPlan, ChildUnitState, NodeExecutionState
from tests.benchmarks.benchmark_core_execution import identity, loop_workflow


def measure(action, repeats=1):
    action()
    samples = []
    for _ in range(7):
        start = time.perf_counter_ns()
        for _ in range(repeats):
            result = action()
        samples.append((time.perf_counter_ns() - start) / repeats)
    gc.collect()
    tracemalloc.start()
    try:
        result = action()
        retained, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return {'median_ns': statistics.median(samples), 'samples_ns': samples,
            'retained_bytes': retained, 'peak_bytes': peak}


def seed_state():
    app = AutoAgentApp()
    try:
        result = app.invoke(Workflow('seed', nodes=[Node('entry', identity)]), {'value': 1})
        return app._repository.state(result.session_id)
    finally:
        app.close()


def retained_state(seed, count):
    old = next(iter(seed.invocation.scheduler.occurrences.values()))
    occurrences = {f'{i}@root': replace(old, id=f'{i}@root', status='completed') for i in range(count)}
    occurrences['0@root'] = replace(occurrences['0@root'], status='running', execution=NodeExecutionState())
    occurrences[f'{count-1}@root'] = replace(occurrences[f'{count-1}@root'], status='waiting')
    plan = ChildInvocationPlan('p', '0@root', 'await', 'w', 'r',
                               (ChildUnitState(0, 'child', 'child-i', None, phase='accepted'),))
    return replace(seed, invocation=replace(seed.invocation, status='running',
        child_plans=MappingProxyType({'p': plan}), scheduler=replace(seed.invocation.scheduler,
        occurrences=MappingProxyType(occurrences), ready=())))


def topology(count):
    """Compose disjoint copies of a compiler-produced Loop IR for lookup scaling."""
    seed = WorkflowCompiler().compile_or_raise(loop_workflow())
    nodes, edges, loops = [], [], []
    for i in range(count):
        prefix = f'{i}:'
        nodes.extend(replace(n, id=prefix+n.id) for n in seed.nodes)
        edges.extend(replace(e, id=prefix+e.id, source=prefix+e.source, target=prefix+e.target) for e in seed.edges)
        loops.extend(replace(r, id=prefix+r.id, header_node_id=prefix+r.header_node_id,
            node_ids=tuple(prefix+n for n in r.node_ids),
            back_edge_ids=tuple(prefix+e for e in r.back_edge_ids),
            entry_edge_ids=tuple(prefix+e for e in r.entry_edge_ids),
            exit_edge_ids=tuple(prefix+e for e in r.exit_edge_ids)) for r in seed.loop_regions)
    return replace(seed, nodes=tuple(nodes), edges=tuple(edges), loop_regions=tuple(loops),
                   entry_node_ids=tuple(f'{i}:entry' for i in range(count)),
                   exit_node_ids=tuple(f'{i}:exit' for i in range(count)))


def nested_topology(depth):
    """Compile a real nested-loop graph; compilation is outside IR/query timing."""
    from autoagent import Edge
    from tests.benchmarks.benchmark_core_execution import again, finished
    names = ['entry', 'exit'] + [f'{side}{i}' for side in ('h', 't') for i in range(depth)]
    edges = [Edge('entry', 'h0'), Edge(f'h{depth-1}', f't{depth-1}')]
    for i in range(depth):
        if i + 1 < depth:
            edges.append(Edge(f'h{i}', f'h{i+1}'))
        edges.append(Edge(f't{i}', f'h{i}', again, id=f'back{i}'))
        edges.append(Edge(f't{i}', f't{i-1}' if i else 'exit', finished, id=f'exit{i}'))
    return WorkflowCompiler().compile_or_raise(Workflow('nested-bench',
        nodes=[Node(n, identity) for n in names], edges=edges))


def nested_measurements():
    report = {}
    for depth in (10, 50):
        workflow = nested_topology(depth)
        def queries():
            workflow.containing_loops(f'h{depth-1}')
            workflow.back_loop(f'back{depth-1}')
            workflow.entry_loops('missing')
            workflow.exit_loops(f'exit{depth-1}')
        report[str(depth)] = {'four_queries': measure(queries, 100),
                              'construct_ir': measure(lambda: replace(workflow))}
    return report


def main():
    seed = seed_state()
    planner = TransitionPlanner()
    indexed = '_execution_index' in inspect.signature(planner.plan).parameters
    report = {'indexed': indexed, 'planner': {}, 'topology': {}}
    for count in (100, 1000, 10000, 50000):
        state = retained_state(seed, count)
        index = ExecutionIndex(state)
        for payload in (WaitRequested('0@root', 'wait', {}), ChildAwaitSuspended('p', '0@root'), NodeCompleted('0@root', {})):
            kwargs = {'_execution_index': index} if indexed else {}
            action = lambda: planner.plan(state, payload, occurred_at_us=999,
                invocation_id=state.invocation.id, session_id=state.session.id,
                scheduler_delta=SchedulerDelta(), **kwargs)
            report['planner'][f'{payload.kind}_{count}'] = measure(action, 10)
    for count in (0, 10, 100, 1000):
        workflow = topology(count)
        def queries():
            workflow.containing_loops(f'{count-1}:step')
            workflow.back_loop(f'{count-1}:back')
            workflow.entry_loops('missing')
            workflow.exit_loops(f'{count-1}:exit')
        report['topology'][str(count)] = {'four_queries': measure(queries, 100),
                                         'construct_ir': measure(lambda: replace(workflow))}
    report['nested'] = nested_measurements()
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
