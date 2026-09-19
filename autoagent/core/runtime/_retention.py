"""Plan payload release; keep scheduling identities and external Events intact."""
from collections import Counter


def consumer_sources(collection, item):
    """Only unresolved scheduling work can consume a source output again."""
    if item is None:
        return ()
    if collection == 'occurrences':
        if item.status in {'ready', 'running', 'waiting'}:
            return tuple(a.source_occurrence_id for a in item.activations)
    elif collection in {'resolutions', 'boundary_resolutions'}:
        if item.activation is not None:
            return (item.activation.source_occurrence_id,)
    return ()


def released_outputs(before, after, delta, occurrence_id, output_node_ids, index):
    """Inspect changed consumers only, including deferred Join/Loop resolutions.

    The index is derived from acknowledged State and holds IDs/counts only.
    Planning never mutates it: failed ACKs must leave the old ownership intact.
    Without Workflow exit metadata, conservatively retain Node outputs.
    """
    if output_node_ids is None:
        return ()
    changes = Counter()
    touched = {
        'occurrences': dict.fromkeys((occurrence_id, *(p.id for p in delta.ready),
                                     *(p.id for p in delta.skipped), *(p.id for p in delta.revived))),
        'resolutions': dict.fromkeys((*(r.id for r in delta.resolutions), *delta.consumed_resolution_ids)),
        'boundary_resolutions': dict.fromkeys(r.id for r in delta.boundary_resolutions),
    }
    if delta.closed_boundaries:
        from .scheduling import boundary_key
        closed = set(delta.closed_boundaries)
        touched['boundary_resolutions'].update(dict.fromkeys(
            key for key, item in before.boundary_resolutions.items()
            if boundary_key(item.loop_region_id, item.loop_scope) in closed))
    for collection, keys in touched.items():
        old, new = getattr(before, collection), getattr(after, collection)
        for key in keys:
            changes.subtract(consumer_sources(collection, old.get(key)))
            changes.update(consumer_sources(collection, new.get(key)))
    # Also consider the newly produced output if no edge selected a consumer.
    changes.setdefault(occurrence_id, 0)
    return tuple(key for key, change in changes.items()
                 if index.output_consumers.get(key, 0) + change == 0
                 and (item := after.occurrences.get(key)) is not None
                 and item.status == 'completed' and item.output is not None
                 and item.node_id not in output_node_ids)
