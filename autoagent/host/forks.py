"""Root-owned historical branching shared by Server and CLI."""
import asyncio

from autoagent.core.runtime import RecoverySource, RuntimeEvent, RuntimeGraphCheckpoint
from autoagent.core.errors import RuntimeTransitionError
from .models import HostNotFoundError, HostRunConflict


class ForkControls:
    async def _prepare_fork(self, invocation_id, sequence, session_id=None):
        if self._runtime is None:
            raise HostRunConflict('FORK_RUNTIME_UNAVAILABLE','Fork requires a running Host with compatible registered Workflows.')
        if not isinstance(invocation_id,str) or not invocation_id.strip() or len(invocation_id)>4096:
            raise ValueError('invocation_id must be nonempty and at most 4096 characters.')
        if session_id is not None and (not isinstance(session_id,str) or not session_id.strip() or len(session_id)>4096):
            raise ValueError('session_id must be nonempty and at most 4096 characters.')
        if type(sequence) is not int or not 1 <= sequence <= 9007199254740991:
            raise ValueError('sequence must be a positive Session Event Sequence.')
        await self._persistence.flush()
        row = await self._persistence.get_invocation(invocation_id, view='durable')
        if row is None:
            raise HostNotFoundError('Source Invocation is unavailable.')
        event = await self._persistence.fork_position(row['session_id'], sequence)
        if event is None or event['invocation_id'] != invocation_id:
            raise HostRunConflict('FORK_POSITION_INVALID', 'The Event must belong to the selected Invocation.')
        if event['event_name'] == 'signal.inherited' or event['event_name'] == 'invocation.forked' and event['initialization_events'] > 1:
            raise HostRunConflict('FORK_TRANSITION_IN_PROGRESS','Choose a position after Fork initialization completes.')
        bundle = await self._persistence.read_checkpoint_position(row['session_id'], sequence, full_history=True)
        def rebuild():
            graph = self._runtime._restore_graph(bundle)
            anchor = RuntimeGraphCheckpoint.from_record(bundle['checkpoint']) if bundle['checkpoint'] else None
            source = RecoverySource.from_events((RuntimeEvent.from_record(e)
                for records in bundle['events'].values() for e in records),
                initial_states={s.session_id:s.state for s in anchor.sessions} if anchor else None)
            return graph, source
        try:
            graph, source = await asyncio.to_thread(rebuild)
            plan = await self._runtime.app.aprepare_fork(graph, source=source, session_id=session_id)
        except (RuntimeTransitionError, ValueError, KeyError) as error:
            raise HostRunConflict(getattr(error,'code','FORK_POSITION_INVALID'), str(error)) from error
        return plan, row, event

    async def plan_fork(self, invocation_id, *, sequence):
        """Describe the exact cut and replay work without admitting a new execution."""
        async def query():
            try:
                plan, row, event = await self._prepare_fork(invocation_id, sequence)
            except HostRunConflict as error:
                return dict(available=False, code=error.code, reason=str(error), invocation_id=invocation_id, sequence=sequence)
            return dict(available=True, code=None, reason=None, invocation_id=invocation_id,
                sequence=sequence, event_id=event['event_id'], source_root_invocation_id=row['root_invocation_id'],
                members=[{k:v for k,v in m.items() if k.startswith('source_')} for m in plan.members],
                replay_calls=list(plan.replay_calls), timer_policy='preserve_absolute_deadlines')
        return await self._perform(query)

    async def fork(self, invocation_id, *, sequence, session_id=None):
        """Fork the whole causal Root cut, returning the corresponding selected Child identity."""
        async def submit():
            plan, row, event = await self._prepare_fork(invocation_id, sequence, session_id)
            runtime = self._runtime
            root_sid = plan.graph.root_session_id
            async with self._runtime.coordinated(root_sid):
                if await self._persistence.get_session(root_sid, view='accepted') is not None:
                    raise HostRunConflict('FORK_SESSION_EXISTS', 'A new Fork Session ID must not already exist.')
                await self._runtime.residency.reserve(root_sid)
                lease = self._runtime.residency.entry(root_sid)
                lease.users += 1
                try:
                    await self._persistence.initialize_fork(plan.events)
                    root_id = plan.events[0].invocation_id
                    ref = await self._runtime.reference(root_id)
                    await self._runtime.ensure_resident(ref)
                    task = asyncio.create_task(self._runtime.activate(ref, force=True))
                except BaseException:
                    lease.users -= 1
                    self._runtime.residency.trim()
                    raise
                self._runtime.fork_tasks[root_id] = task
                def finished(t):
                    lease.users -= 1
                    runtime.residency.trim()
                    runtime.fork_tasks.pop(root_id, None)
                    if not t.cancelled():
                        t.exception()
                task.add_done_callback(finished)
                await asyncio.sleep(0)
            selected = next(m for m in plan.members if m['source_invocation_id'] == invocation_id)
            return dict(accepted=True, workflow_id=ref.workflow_id, revision_id=ref.workflow_revision_id,
                session_id=root_sid, invocation_id=root_id, root_invocation_id=root_id,
                selected_session_id=selected['session_id'], selected_invocation_id=selected['invocation_id'],
                source_invocation_id=invocation_id, source_root_invocation_id=row['root_invocation_id'],
                source_sequence=sequence, source_event_id=event['event_id'], members=list(plan.members))
        return await self._perform(submit, control=True)
