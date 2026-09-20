"""Rebuildable execution lookups containing IDs and counters, never Events."""
from ._retention import consumer_sources


class ExecutionIndex:
    def __init__(self, state):
        self.child_remaining = {}
        self.children_remaining = 0
        self.started_count = 0
        self.occurrence_counts = {}
        self.waiting_count = 0
        self.running = {}
        self.occurrences_by_scope = {}
        self.calls_by_occurrence = {}
        self.waits_by_occurrence = {}
        self.output_consumers = {}
        invocation = state.invocation
        if invocation is not None:
            self.child_remaining = {key: sum(unit.phase not in {'terminal', 'abandoned'} for unit in plan.units)
                                    for key, plan in invocation.child_plans.items()}
            self.children_remaining = sum(self.child_remaining.values())
            scheduler = invocation.scheduler
            for collection in ('occurrences', 'operator_calls', 'waits'):
                for item in getattr(scheduler, collection).values():
                    self._change(collection, item, True)
            for collection in ('occurrences', 'resolutions', 'boundary_resolutions'):
                for item in getattr(scheduler, collection).values():
                    self._consumer_change(collection, item, 1)

    def _consumer_change(self, collection, item, direction):
        for source in consumer_sources(collection, item):
            count = self.output_consumers.get(source, 0) + direction
            if count:
                self.output_consumers[source] = count
            else:
                self.output_consumers.pop(source, None)

    def _scope_change(self, item, add):
        for position, frame in enumerate(item.scope):
            key = (frame.loop_region_id, item.scope[:position + 1])
            if add:
                self.occurrences_by_scope.setdefault(key, {})[item.id] = None
            else:
                group = self.occurrences_by_scope[key]
                del group[item.id]
                if not group:
                    del self.occurrences_by_scope[key]

    def _change(self, collection, item, add):
        if collection == 'occurrences':
            self._scope_change(item, add)
            self.occurrence_counts[item.status] = self.occurrence_counts.get(item.status, 0) + (1 if add else -1)
            self.started_count += (1 if add else -1) * (item.started_sequence is not None)
            if item.status == 'running':
                if add:
                    self.running[item.id] = None
                else:
                    self.running.pop(item.id, None)
            return
        if collection == 'waits' and item.status == 'waiting':
            self.waiting_count += 1 if add else -1
        groups = self.calls_by_occurrence if collection == 'operator_calls' else self.waits_by_occurrence
        if add:
            groups.setdefault(item.occurrence_id, {})[item.id] = None
        else:
            group = groups[item.occurrence_id]
            del group[item.id]
            if not group:
                del groups[item.occurrence_id]

    def advance(self, before, after, delta, *, child_unit=None):
        """Update touched entities only; bulk replacement rebuilds from State."""
        touched = {}
        child_plans = None
        for operation in delta.operations:
            path = operation.path
            if path in (('invocation',), ('invocation', 'scheduler'), ('invocation', 'child_plans')):
                return ExecutionIndex(after)
            if len(path) >= 3 and path[:2] == ('invocation', 'child_plans'):
                if child_plans is None:
                    child_plans = set()
                child_plans.add(path[2])
            if len(path) >= 3 and path[:2] == ('invocation', 'scheduler') and path[2] in (
                'occurrences', 'operator_calls', 'waits', 'resolutions', 'boundary_resolutions'
            ):
                if len(path) == 3:
                    return ExecutionIndex(after)
                touched[(path[2], path[3])] = None
        for key in child_plans or ():
            previous_remaining = self.child_remaining.get(key, 0)
            plan = after.invocation.child_plans.get(key)
            if plan is None:
                self.child_remaining.pop(key, None)
            elif child_unit is not None and child_unit[0] == key and key in self.child_remaining:
                unit = child_unit[1]
                old = before.invocation.child_plans[key].units[unit]
                new = plan.units[unit]
                self.child_remaining[key] += (new.phase not in {'terminal', 'abandoned'}) - (old.phase not in {'terminal', 'abandoned'})
            else:
                self.child_remaining[key] = sum(unit.phase not in {'terminal', 'abandoned'} for unit in plan.units)
            self.children_remaining += self.child_remaining.get(key, 0) - previous_remaining
        for collection, key in touched:
            old = getattr(before.invocation.scheduler, collection).get(key)
            new = getattr(after.invocation.scheduler, collection).get(key)
            self._consumer_change(collection, old, -1)
            self._consumer_change(collection, new, 1)
            if collection in ('resolutions', 'boundary_resolutions'):
                continue
            # Preserve insertion order for updates to an existing entity.
            if collection == 'occurrences':
                if old is not None and new is not None:
                    if old.scope != new.scope:
                        self._scope_change(old, False)
                        self._scope_change(new, True)
                    self.occurrence_counts[old.status] -= 1
                    self.occurrence_counts[new.status] = self.occurrence_counts.get(new.status, 0) + 1
                    self.started_count += (new.started_sequence is not None) - (old.started_sequence is not None)
                    if new.status == 'running':
                        self.running[key] = None
                    else:
                        self.running.pop(key, None)
                    continue
            elif old is not None and new is not None and old.occurrence_id == new.occurrence_id:
                if collection == 'waits':
                    self.waiting_count += (new.status == 'waiting') - (old.status == 'waiting')
                continue
            if old is not None:
                self._change(collection, old, False)
            if new is not None:
                self._change(collection, new, True)
        return self
