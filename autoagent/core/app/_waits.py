"""Resident wake scheduling; durable waits themselves belong to Runtime State."""
import asyncio
from ..commands import RuntimeHandle, SuspensionInfo
from ..runtime.events import WaitRequested, CommandAwakened
from ..runtime.state import child_input_digest
from ..runtime.values import thaw
from ..errors import RuntimeTransitionError
from ..runtime.waits import validate_condition


class WaitRuntime:
    def _init_wait_runtime(self):
        self._wait_sessions = set()
        self._wait_roots = {}
        self._wait_pumps = {}
        self._wait_dirty = set()
        self._wait_timers = {}

    def _refresh_wait_session(self, sid):
        inv = self._repository.state(sid).invocation
        if inv and not inv.terminal and not inv.stopping and any(w.status == 'waiting' and w.kind != 'external' for w in inv.scheduler.waits.values()):
            self._wait_sessions.add(sid)
            root = self._root_session_id(sid)
            self._wait_roots.setdefault(root, set()).add(sid)
        else:
            self._remove_wait_session(sid)

    def _remove_wait_session(self, sid):
        self._wait_sessions.discard(sid)
        root = self._root_session_id(sid)
        sessions = self._wait_roots.get(root)
        if sessions is not None:
            sessions.discard(sid)
            if not sessions:
                self._wait_roots.pop(root, None)
                timer = self._wait_timers.pop(root, None)
                if timer:
                    timer.cancel()

    def _schedule_waits(self, root):
        if self._closing or root not in self._wait_roots:
            return
        self._wait_dirty.add(root)
        if root not in self._wait_pumps:
            task = asyncio.create_task(self._pump_waits(root))
            self._wait_pumps[root] = task

    async def _pump_waits(self, root):
        try:
            while root in self._wait_dirty and not self._closing:
                self._wait_dirty.discard(root)
                timer = self._wait_timers.pop(root, None)
                if timer:
                    timer.cancel()
                deadlines = []
                for sid in sorted(tuple(self._wait_roots.get(root, ()))):
                    async with self._graph_gate(root).shared(), self._session_transition_lock(sid):
                        if sid not in self._wait_sessions:
                            continue
                        if root in self._recovering and sid not in self._recovering[root]:
                            continue
                        await self._settle_runtime_commits((sid,))
                        deadlines.extend(await self._service_waits_locked(sid))
                if deadlines and root in self._wait_roots and root not in self._wait_dirty:
                    delay = min(60_000_000, max(0, min(deadlines) - self._clock_us())) / 1_000_000
                    self._wait_timers[root] = asyncio.get_running_loop().call_later(delay, self._schedule_waits, root)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            self._drive_errors[root] = error
        finally:
            self._wait_pumps.pop(root, None)
            event = self._boundary_events.get(root)
            if event is not None:
                event.set()

    async def _service_waits_locked(self, sid):
        """Resolve older registered consumers before a new nonblocking Receive."""
        inv = self._repository.state(sid).invocation
        if inv is None or inv.stopping or inv.terminal:
            self._remove_wait_session(sid)
            return []
        deadlines = []
        waits = sorted((w for w in inv.scheduler.waits.values() if w.status == 'waiting' and w.kind != 'external'), key=lambda w: w.registered_sequence)
        for wait in waits:
            outcome = self._ready_condition(sid, wait.kind, wait.request)
            if outcome is None:
                deadlines.extend(self._condition_deadlines(wait.kind, wait.request))
                continue
            result, messages = outcome
            await self._emit_locked(sid, inv.id, CommandAwakened(wait.id, result, messages))
            if self._task_runtime.is_live(sid):
                self._task_runtime.wake(sid)
            else:
                self._start_drive(self._workflow_for_state(self._repository.state(sid)), sid, inv.id, None, None)
        self._refresh_wait_session(sid)
        return deadlines

    def _validate_wait_target(self, sid, kind, condition):
        validate_condition(kind, condition)
        if kind == 'child':
            handle = RuntimeHandle.model_validate(thaw(condition['handle']))
            owner = self._parent_plan(handle.session_id, handle.invocation_id)
            if owner is None or owner[0] != sid:
                raise RuntimeTransitionError('AWAIT_NOT_DIRECT_CHILD', 'Child waits require directly owned Children.')
            if self._runtime_identity(handle.session_id)[0] != handle:
                raise RuntimeTransitionError('RUNTIME_HANDLE_MISMATCH', 'Child Handle identity does not match.')
        elif kind == 'signal':
            if condition['endpoint'] not in dict(self._workflow_for_state(self._repository.state(sid)).signal_endpoints):
                raise RuntimeTransitionError('SIGNAL_ENDPOINT_UNKNOWN', 'Workflow does not declare this Endpoint.')
        elif kind == 'any':
            for case in condition['cases']:
                self._validate_wait_target(sid, case['kind'], case['condition'])

    async def _suspend_command(self, sid, iid, call_id, kind, condition):
        root = self._root_session_id(sid)
        async with self._graph_gate(root).shared(), self._session_transition_lock(sid):
            await self._settle_runtime_commits((sid,))
            inv = self._repository.state(sid).invocation
            call = inv.scheduler.operator_calls.get(call_id)
            if inv.id != iid or inv.stopping or call is None or call.status != 'running':
                raise RuntimeTransitionError('COMMAND_CALL_INVALID', 'Suspension requires its running Call.')
            self._validate_wait_target(sid, kind, condition)
            if call_id not in inv.scheduler.waits:
                await self._emit_locked(sid, iid, WaitRequested(call.occurrence_id, call_id, condition, kind))
            self._refresh_wait_session(sid)
            self._schedule_waits(root)

    def _ready_condition(self, sid, kind, condition):
        if kind == 'timer':
            return ({'deadline_at_us': condition['deadline_at_us']}, ()) if self._clock_us() >= condition['deadline_at_us'] else None
        if kind == 'signal':
            inv = self._repository.state(sid).invocation
            selected = tuple(mid for mid, entry in sorted(inv.signals['messages'].items(), key=lambda item: item[1]['message']['accepted_sequence']) if entry['message']['endpoint'] == condition['endpoint'])[:condition['limit']]
            if selected:
                return {'messages': [inv.signals['messages'][mid]['message'] for mid in selected]}, selected
            return None
        if kind == 'child':
            handle = RuntimeHandle.model_validate(thaw(condition['handle']))
            sessions = (handle.session_id, *self._descendant_sessions(handle.session_id))
            for target in sessions:
                if target in self._drive_errors:
                    raise self._drive_errors[target]
            inv = self._repository.state(handle.session_id).invocation
            terminal = inv.terminal and not any(self._task_runtime.is_live(target) for target in sessions)
            waiting = inv.status in {'waiting', 'settling'} and not inv.stopping and not self._task_runtime.is_live(handle.session_id) and bool(self._runtime_waits(sessions))
            if not (terminal or waiting):
                return None
            observation = self._runtime_observation(handle, sessions)
            if condition['after'] == observation.version:
                return None
            return observation.model_dump(mode='python'), ()
        for case in condition['cases']:
            outcome = self._ready_condition(sid, case['kind'], case['condition'])
            if outcome is not None:
                result, messages = outcome
                return ({'case': case['name'], 'value': result} if condition['select'] else result), messages
        return None

    @staticmethod
    def _condition_deadlines(kind, condition):
        if kind == 'timer':
            return [condition['deadline_at_us']]
        if kind == 'any':
            return [c['condition']['deadline_at_us'] for c in condition['cases'] if c['kind'] == 'timer']
        return []

    def _observation_version(self, handle, inv, waits):
        return child_input_digest({'handle': handle.model_dump(), 'status': inv.status, 'pending_outcome': inv.pending_outcome,
            'waits': sorted((w.handle.session_id, w.wait_id, w.kind) for w in waits)})

    def suspension_info(self, ref):
        """Inspect unload eligibility; unloading still rechecks atomically."""
        return self._run(self._suspension_info(ref))

    async def asuspension_info(self, ref):
        return await self._await(self._submit(self._suspension_info(ref)))

    async def _suspension_info(self, ref):
        ref = self._control_ref(ref)
        root = ref.session_id
        self._state_for_ref(ref)
        async with self._graph_gate(root):
            sessions = (root, *self._descendant_sessions(root))
            await self._settle_runtime_commits(sessions)
            waits = self._runtime_waits(sessions)
            deadlines = [d for w in waits if w.kind != 'external' for d in self._condition_deadlines(w.kind, w.condition)]
            can_unload = (not self._graph_controls.get(root) and root not in self._recovering
                and not self._result_leases.get(root) and not any(s in self._attached_streams or self._task_runtime.is_live(s) for s in sessions)
                and all(self._repository.state(s).invocation.status in {'waiting', 'settling', 'completed', 'failed', 'cancelled'} for s in sessions))
            return SuspensionInfo(can_unload=can_unload, next_deadline_at_us=min(deadlines, default=None), waits=waits)

    def _forget_wait_sessions(self, sessions):
        roots = {self._root_session_id(sid) for sid in sessions}
        self._wait_sessions.difference_update(sessions)
        for root in roots:
            remaining = self._wait_roots.get(root)
            if remaining is not None:
                remaining.difference_update(sessions)
                if not remaining:
                    self._wait_roots.pop(root, None)
            timer = self._wait_timers.pop(root, None)
            if timer:
                timer.cancel()
            self._wait_dirty.discard(root)
