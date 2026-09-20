"""Construct complete graph bundles from recorded per-Session test prefixes."""
from autoagent import AppCheckpoint, RuntimeGraphCheckpoint


def graph_bundle(sessions):
    sessions = tuple(sessions)
    by_id = {s.session_id: s for s in sessions}
    children = {u.session_id for s in sessions if s.state.invocation is not None for p in s.state.invocation.child_plans.values() for u in p.units}
    graphs = []
    for root in sorted(set(by_id) - children):
        pending, found = [root], []
        while pending:
            sid = pending.pop()
            if sid not in by_id or sid in found:
                continue
            found.append(sid)
            if by_id[sid].state.invocation is not None:
                pending.extend(u.session_id for p in by_id[sid].state.invocation.child_plans.values() for u in p.units)
        graphs.append(RuntimeGraphCheckpoint(root, tuple(by_id[sid] for sid in found)))
    if sum(len(g.sessions) for g in graphs) != len(sessions):
        raise ValueError('Unreachable Sessions in test prefix.')
    return AppCheckpoint(tuple(graphs))


def root_snapshot(checkpoint):
    if isinstance(checkpoint, RuntimeGraphCheckpoint):
        return next(s for s in checkpoint.sessions if s.session_id == checkpoint.root_session_id)
    return checkpoint


def session_checkpoints(checkpoint):
    return checkpoint.sessions if isinstance(checkpoint, RuntimeGraphCheckpoint) else tuple(s for g in checkpoint.graphs for s in g.sessions)


def load_graph(app, checkpoint):
    """Wrap a single-Root recorded prefix for tests of non-graph behavior."""
    from autoagent import SessionCheckpoint
    if isinstance(checkpoint, SessionCheckpoint):
        checkpoint = RuntimeGraphCheckpoint(checkpoint.session_id, (checkpoint,))
    return app.load_checkpoint(checkpoint)


def child_refs(app, parent):
    """Inspect durable ownership in tests; these references confer no App control."""
    def inspect():
        inv = app._repository.state(parent.session_id).invocation
        return tuple(app._ref_for_invocation(u.session_id, child)
            for p in inv.child_plans.values() for u in p.units
            if (child := app._repository.state(u.session_id).invocation) is not None)
    async def run(): return inspect()
    return app._runtime_loop.run(run())


def inspect_result(app, ref):
    """Read a Child's immutable State for assertions without a public Child API."""
    from autoagent import ChildHandle, InvocationResult, InvocationWait
    from autoagent.core.runtime import thaw
    sid = ref.child_session_id if isinstance(ref, ChildHandle) else ref.session_id
    async def run():
        inv = app._repository.state(sid).invocation
        return InvocationResult(app._ref_for_invocation(sid, inv), inv.status, thaw(inv.output), inv.error,
            tuple(InvocationWait(w.id, thaw(w.request)) for w in inv.scheduler.waits.values() if w.status == 'waiting'))
    return app._runtime_loop.run(run())


def join_observed(app, ref, timeout=None):
    """Root uses its public join; Child assertions wait only on its internal Task."""
    import asyncio
    from autoagent import ChildHandle
    sid = ref.child_session_id if isinstance(ref, ChildHandle) else ref.session_id
    if sid not in app._child_owners:
        return app.join(ref, timeout=timeout)
    async def wait():
        task = app._task_runtime.task(sid)
        if task is not None:
            await asyncio.wait_for(asyncio.shield(task), timeout)
    app._runtime_loop.run(wait())
    return inspect_result(app, ref)


def resume_graph_wait(app, ref, wait_id, response):
    """Old per-Child fixtures now deliver external responses through Root control."""
    root = app._root_session_id(ref.session_id)
    if root == ref.session_id:
        return app.resume(ref, wait_id, response)
    root_ref = app._ref_for_invocation(root, app._repository.state(root).invocation)
    app.submit_resume(root_ref, wait_id, response)
    return join_observed(app, ref)


def status_observed(app, ref):
    from autoagent import ChildHandle
    sid = ref.child_session_id if isinstance(ref, ChildHandle) else ref.session_id
    return inspect_result(app, ref) if sid in app._child_owners else app.status(ref)


async def async_children(app, ref):
    async def inspect():
        inv = app._repository.state(ref.session_id).invocation
        return tuple(app._ref_for_invocation(u.session_id, child)
                     for p in inv.child_plans.values() for u in p.units
                     if (child := app._repository.state(u.session_id).invocation) is not None)
    return await app._await(app._submit(inspect()))


async def async_join_observed(app, ref, timeout=None):
    import asyncio
    from autoagent import ChildHandle, InvocationResult, InvocationWait
    from autoagent.core.runtime import thaw
    sid = ref.child_session_id if isinstance(ref, ChildHandle) else ref.session_id
    if sid not in app._child_owners:
        return await app.ajoin(ref, timeout=timeout)
    async def inspect():
        task = app._task_runtime.task(sid)
        if task is not None:
            await asyncio.wait_for(asyncio.shield(task), timeout)
        inv = app._repository.state(sid).invocation
        return InvocationResult(app._ref_for_invocation(sid, inv), inv.status, thaw(inv.output), inv.error,
             tuple(InvocationWait(w.id, thaw(w.request)) for w in inv.scheduler.waits.values() if w.status == 'waiting'))
    return await app._await(app._submit(inspect()))


async def async_resume_graph_wait(app, ref, wait_id, response):
    root = app._root_session_id(ref.session_id)
    if root == ref.session_id:
        return await app.aresume(ref, wait_id, response)
    root_ref = app._ref_for_invocation(root, app._repository.state(root).invocation)
    await app.asubmit_resume(root_ref, wait_id, response)
    return await async_join_observed(app, ref)


async def resume_graph_wait_internal(app, ref, wait_id, response, *, wait_for_boundary=True):
    from autoagent import InvocationResult, InvocationWait
    from autoagent.core.runtime import thaw
    root = app._root_session_id(ref.session_id)
    root_ref = app._ref_for_invocation(root, app._repository.state(root).invocation)
    result = await app._resume(root_ref, wait_id, response, wait_for_boundary=wait_for_boundary and root == ref.session_id)
    if root == ref.session_id or not wait_for_boundary:
        return result
    task = app._task_runtime.task(ref.session_id)
    if task is not None:
        import asyncio
        await asyncio.shield(task)
    inv = app._repository.state(ref.session_id).invocation
    return InvocationResult(ref, inv.status, thaw(inv.output), inv.error,
        tuple(InvocationWait(w.id, thaw(w.request)) for w in inv.scheduler.waits.values() if w.status == 'waiting'))
