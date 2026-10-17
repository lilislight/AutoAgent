"""Deterministic lightweight views extracted from facts and explicit StateDelta."""

from __future__ import annotations

from sqlalchemy import func, select

from .schema import meta

PROJECTION_TABLES = (
    "sessions",
    "invocations",
    "node_executions",
    "operator_calls",
    "waits",
    "edge_evaluations",
    "graph_changes",
)


def source(kind, identity, path):
    return {"kind": kind, "id": identity, "path": list(path)}


def timer_deadline(kind, request):
    if not isinstance(request, dict):
        return None
    if kind == "timer":
        return request.get("deadline_at_us")
    if kind == "any":
        deadlines = [timer_deadline(case["kind"], case["condition"]) for case in request.get("cases", ())]
        return min((x for x in deadlines if x is not None), default=None)
    return None


def light_record(item, *, availability=False):
    """Detach metadata without retaining input/output/context in query caches."""
    if isinstance(item, list):
        return [light_record(x, availability=availability) for x in item]
    if not isinstance(item, dict):
        return item
    result = {}
    if "request" in item:
        result["deadline_at_us"] = timer_deadline(item.get("kind", item.get("wait_kind")), item["request"])
    for key, value in item.items():
        if (
            key == "value"
            and isinstance(item.get("op"), str)
            and isinstance(item.get("path"), list)
            and item["path"]
            and item["path"][-1]
            in {
                "input",
                "output",
                "context",
                "mapped_input",
                "aggregate_output",
                "request",
                "response",
                "metrics",
                "origin",
                "patch",
                "pending_context_patch",
                "reason",
                "cancel_reason",
                "error",
                "fault",
                "pending_recovery",
                "reused_calls",
                "command_inputs",
            }
        ):
            result[key] = (
                value is not None if item["path"][-1] == "pending_recovery" else
                {k: value.get(k) for k in ("type", "code", "phase")}
                if isinstance(value, dict) and item["path"][-1] in ("error", "fault")
                else True
                if availability and value is not None
                else None
            )
            continue
        if key in {
            "input",
            "output",
            "context",
            "mapped_input",
            "aggregate_output",
            "request",
            "response",
            "metrics",
            "origin",
            "patch",
            "pending_context_patch",
            "signals",
            "resume_receipts",
            "resume_receipt_frontiers",
            "cancel_origin",
            "pending_recovery",
            "reused_calls",
            "command_inputs",
            "reason",
            "cancel_reason",
        }:
            result[key] = (value is not None) if key == "pending_recovery" else True if availability and value is not None else None
        elif key in {"error", "fault"}:
            result[key] = (
                {k: value.get(k) for k in ("type", "code", "phase")}
                if isinstance(value, dict)
                else None
            )
        else:
            result[key] = light_record(value, availability=availability)
    return result


class ProjectionBatch:
    """Changed rows only; shared by writes, rebuilds and accepted query overlays."""

    def __init__(self, connection, source_loader):
        self.connection = connection
        self.source_loader = source_loader
        self.rows = {name: {} for name in PROJECTION_TABLES}
        self.dirty = {name: set() for name in PROJECTION_TABLES}
        self.order = 0
        self.ordinals = {}

    async def get(self, name, **identity):
        table = meta.tables[name]
        key = tuple(identity[c.name] for c in table.primary_key)
        if key not in self.rows[name]:
            where = [table.c[k] == v for k, v in identity.items()]
            row = (
                (await self.connection.execute(select(table).where(*where)))
                .mappings()
                .first()
            )
            self.rows[name][key] = dict(row) if row else None
        return self.rows[name][key]

    async def put(self, name, identity, values):
        table = meta.tables[name]
        previous = await self.get(name, **identity)
        row = dict(previous or identity)
        row.update(values)
        if name in ("invocations", "node_executions", "operator_calls", "waits"):
            row["updated_store_order"] = self.order
        if name == "node_executions" and previous is None:
            scope = (identity["invocation_id"], row["node_id"])
            if scope not in self.ordinals:
                self.ordinals[scope] = await self.connection.scalar(select(func.max(table.c.ordinal)).where(
                    table.c.invocation_id == scope[0], table.c.node_id == scope[1])) or 0
            self.ordinals[scope] += 1
            row["ordinal"] = self.ordinals[scope]
        key = tuple(identity[c.name] for c in table.primary_key)
        self.rows[name][key] = row
        if previous is None or row != previous:
            self.dirty[name].add(key)
        return row

    def changed(self, name):
        return [
            row
            for key, row in self.rows[name].items()
            if key in self.dirty[name] and row is not None
        ]

    async def runtime(self, event, order, operator_definition_id=None):
        self.order = order
        sid, iid, seq = event["session_id"], event["invocation_id"], event["sequence"]
        payload, name, stamp = (
            event["payload"],
            event["event_name"],
            event["occurred_at_us"],
        )
        ref = lambda *path: source("runtime_event", event["id"], path)
        sess = await self.get("sessions", session_id=sid)
        await self.put(
            "sessions",
            {"session_id": sid},
            {
                "created_at_us": stamp
                if name == "session.opened"
                else (sess or {}).get("created_at_us"),
                "latest_invocation_id": iid
                if name == "invocation.started"
                else (sess or {}).get("latest_invocation_id"),
                "durable_sequence": seq,
                "history_start_sequence": (sess or {}).get("history_start_sequence", 1),
            },
        )
        if iid is None:
            return
        changes = []

        def change(typ, identity, values):
            if values:
                changes.append((typ, identity, values))

        if name == "invocation.started":
            previous = await self.get("invocations", invocation_id=iid)
            await self.put(
                "invocations",
                {"invocation_id": iid},
                {
                    "session_id": sid,
                    "workflow_revision_id": payload["workflow_revision_id"],
                    "entry_node_id": payload["entry_node_id"],
                    "root_invocation_id": (previous or {}).get(
                        "root_invocation_id", iid
                    ),
                    "status": "running",
                    "control_state": "active",
                    "recovery_count": 0,
                    "recovery_pending": False,
                    "started_at_us": stamp,
                    "last_sequence": seq,
                    "input_ref": ref("payload", "input"),
                },
            )

        async def node(oid, value, path):
            previous = await self.get(
                "node_executions", invocation_id=iid, occurrence_id=oid
            )
            defaults = {
                "node_id": value["node_id"],
                "activations": [{"edge_id": a["edge_id"], "source_occurrence_id": a["source_occurrence_id"]} for a in value.get("activations", [])],
                "scope": value["scope"],
                "status": value["status"],
                "recovery_attempts": value.get("recovery_attempts", 0),
            }
            for key in ("ready_at_us", "started_at_us", "completed_at_us"):
                if key in value:
                    defaults[key] = value[key]
            workspace = value.get('execution', {})
            defaults["phase"] = workspace.get("phase", "none")
            if previous is None and 'input_mapped' in workspace.get('completed_stages', ()):
                defaults['input_ref'] = ref(*path, 'execution', 'mapped_input')
            if value.get("error"):
                defaults.update(
                    error_type=value["error"]["type"],
                    error_code=value["error"].get("code"),
                    error_phase=value["error"].get("phase"),
                )
            elif value["status"] == "completed":
                defaults.update(
                    error_ref=None, error_type=None, error_code=None, error_phase=None
                )
            row = await self.put(
                "node_executions",
                {"invocation_id": iid, "occurrence_id": oid},
                defaults,
            )
            diff = {
                k: row[k]
                for k in ("status", "phase", "recovery_attempts")
                if not previous or row[k] != previous.get(k)
            }
            if not previous:
                diff.update(node_id=row["node_id"], scope=row["scope"])
            change("node", oid, diff)
            if not previous and row["status"] == "skipped":
                await outgoing(oid, row, skipped=True)
            # New activations also describe routes already consumed by the Scheduler.
            if not previous:
                for activation in value.get("activations", []):
                    change(
                        "edge",
                        activation["edge_id"]
                        + "@source:"
                        + activation["source_occurrence_id"],
                        {
                            "edge_id": activation["edge_id"],
                            "target_node_id": value["node_id"],
                            "target_scope": value["scope"],
                            "source_occurrence_id": activation["source_occurrence_id"],
                            "selected": True,
                        },
                    )

        async def call(cid, value):
            values = {
                k: value.get(k)
                for k in (
                    "occurrence_id",
                    "operator_id",
                    "unit_index",
                    "status",
                    "started_at_us",
                    "completed_at_us",
                    "queue_duration_ns",
                    "execution_duration_ns",
                )
            }
            if value.get("error"):
                values.update(
                    error_type=value["error"]["type"],
                    error_code=value["error"].get("code"),
                )
            await self.put(
                "operator_calls", {"invocation_id": iid, "call_id": cid}, values
            )

        async def wait(wid, value):
            previous = await self.get("waits", invocation_id=iid, wait_id=wid)
            values = {
                k: value.get(k)
                for k in (
                    "occurrence_id",
                    "kind",
                    "status",
                    "created_at_us",
                    "resumed_at_us",
                    "deadline_at_us",
                )
            }
            await self.put("waits", {"invocation_id": iid, "wait_id": wid}, values)
            if not previous or previous["status"] != values["status"]:
                change(
                    "wait",
                    wid,
                    {k: values[k] for k in ("occurrence_id", "kind", "status")},
                )

        def edge(identity, value):
            change(
                "edge",
                identity,
                {
                    k: value[k]
                    for k in (
                        "edge_id",
                        "target_node_id",
                        "target_scope",
                        "loop_region_id",
                        "loop_scope",
                        "source_scope",
                        "selected",
                    )
                    if k in value
                }
                | (
                    {
                        "source_occurrence_id": value["activation"][
                            "source_occurrence_id"
                        ]
                    }
                    if value.get("activation")
                    else {}
                ),
            )

        async def outgoing(oid, occurrence, *, skipped=False):
            inv = await self.get("invocations", invocation_id=iid)
            if not inv:
                return
            snapshot = await self.source_loader(
                self.connection, "workflow_revision", inv["workflow_revision_id"]
            )
            status = "complete" if occurrence["status"] == "completed" else "error"
            evaluations = meta.tables["edge_evaluations"]
            events = meta.tables["runtime_events"]
            stored = (
                (
                    await self.connection.execute(
                        select(evaluations, events.c.sequence)
                        .join(events, evaluations.c.event_id == events.c.event_id)
                        .where(
                            evaluations.c.invocation_id == iid,
                            evaluations.c.source_occurrence_id == oid,
                            evaluations.c.source_status == status,
                        )
                        .order_by(events.c.sequence, evaluations.c.result_index)
                    )
                )
                .mappings()
                .all()
            )
            condition_values = {r["edge_id"]: r["condition_result"] for r in stored}
            condition_values.update(
                {
                    r["edge_id"]: r["condition_result"]
                    for r in self.rows["edge_evaluations"].values()
                    if r
                    and r["invocation_id"] == iid
                    and r["source_occurrence_id"] == oid
                    and r["source_status"] == status
                }
            )
            for definition in snapshot["definition"]["edges"]:
                if definition["source"] != occurrence["node_id"]:
                    continue
                selected = False
                if not skipped and definition["on"] == status:
                    if definition["condition"] is None:
                        selected = True
                    elif definition["id"] in condition_values:
                        selected = condition_values[definition["id"]]
                    else:
                        continue  # No condition result means no invented decision.
                change(
                    "edge",
                    definition["id"] + "@source:" + oid,
                    {
                        "edge_id": definition["id"],
                        "source_occurrence_id": oid,
                        "source_scope": occurrence["scope"],
                        "target_node_id": definition["target"],
                        "selected": selected,
                    },
                )

        async def scheduler(value, path):
            for oid, item in value.get("occurrences", {}).items():
                await node(oid, item, (*path, "occurrences", oid))
            for cid, item in value.get("operator_calls", {}).items():
                await call(cid, item)
            for wid, item in value.get("waits", {}).items():
                await wait(wid, item)
            for field in ("resolutions", "boundary_resolutions"):
                for rid, item in value.get(field, {}).items():
                    edge(rid, item)

        async def plan(value):
            parent = await self.get("invocations", invocation_id=iid)
            for unit in value["units"]:
                child = await self.get(
                    "invocations", invocation_id=unit["invocation_id"]
                )
                csid = unit["session_id"]
                if await self.get("sessions", session_id=csid) is None:
                    await self.put(
                        "sessions",
                        {"session_id": csid},
                        {
                            "created_at_us": None,
                            "durable_sequence": 0,
                            "history_start_sequence": 1,
                            "latest_invocation_id": None,
                        },
                    )
                values = {
                    "session_id": csid,
                    "workflow_revision_id": value["workflow_revision_id"],
                    "entry_node_id": value["entry_node_id"],
                    "root_invocation_id": parent["root_invocation_id"],
                    "parent_invocation_id": iid,
                    "parent_occurrence_id": value["parent_occurrence_id"],
                    "creation_id": value["creation_id"],
                    "unit_index": unit["unit_index"],
                    "ownership_phase": unit["phase"],
                }
                if not child:
                    values.update(status=None, last_sequence=0)
                await self.put(
                    "invocations", {"invocation_id": unit["invocation_id"]}, values
                )
                if not child or child.get("ownership_phase") != unit["phase"]:
                    change(
                        "invocation",
                        unit["invocation_id"],
                        {"ownership_phase": unit["phase"], "parent_invocation_id": iid},
                    )

        for index, operation in enumerate(
            (event.get("delta") or {}).get("operations", [])
        ):
            path, value = operation["path"], operation.get("value")
            if operation["op"] == "remove":
                if (
                    len(path) == 4
                    and path[:2] == ["invocation", "scheduler"]
                    and path[2] in ("resolutions", "boundary_resolutions")
                ):
                    change("edge", path[3], {"consumed": True})
                continue
            if path == ["invocation"]:
                values = {
                    k: value[k]
                    for k in (
                        "status",
                        "pending_outcome",
                        "control_state",
                        "recovery_count",
                        "last_recovery_id",
                        "started_at_us",
                        "completed_at_us",
                    )
                    if k in value
                }
                values['recovery_pending'] = bool(value.get('pending_recovery'))
                if name == 'invocation.reopened':
                    values.update(error_ref=None, error_type=None, error_code=None, output_ref=None)
                await self.put("invocations", {"invocation_id": iid}, values)
                if "status" in value:
                    change("invocation", iid, {"status": value["status"]})
                await scheduler(
                    value.get("scheduler", {}),
                    ("delta", "operations", index, "value", "scheduler"),
                )
                for item in value.get("child_plans", {}).values():
                    await plan(item)
            elif path == ['invocation', 'pending_recovery']:
                await self.put('invocations', {'invocation_id':iid}, {'recovery_pending':bool(value)})
            elif path[:2] == ["invocation", "child_plans"] and len(path) == 3:
                await plan(value)
            elif path[:2] == ["invocation", "scheduler"] and len(path) >= 4:
                field, identity = path[2:4]
                if field == "occurrences":
                    if len(path) == 4:
                        await node(
                            identity, value, ("delta", "operations", index, "value")
                        )
                    elif path[4] == "execution" and isinstance(value, dict):
                        previous = await self.get(
                            "node_executions", invocation_id=iid, occurrence_id=identity
                        )
                        phase = value.get("phase", "none")
                        await self.put(
                            "node_executions",
                            {"invocation_id": iid, "occurrence_id": identity},
                            {"phase": phase},
                        )
                        if not previous or previous.get("phase") != phase:
                            change("node", identity, {"phase": phase})
                elif field == "operator_calls":
                    if len(path) == 4:
                        await call(identity, value)
                    elif path[4] in (
                        "status",
                        "completed_at_us",
                        "execution_duration_ns",
                    ):
                        await self.put(
                            "operator_calls",
                            {"invocation_id": iid, "call_id": identity},
                            {path[4]: value},
                        )
                elif field == "waits" and len(path) == 4:
                    await wait(identity, value)
                elif (
                    field in ("resolutions", "boundary_resolutions") and len(path) == 4
                ):
                    edge(identity, value)
            elif (
                len(path) == 2
                and path[0] == "invocation"
                and path[1]
                in ("status", "pending_outcome", "control_state", "started_at_us", "completed_at_us")
            ):
                values = {path[1]: value}
                if path[1] == "pending_outcome" and value in ("failed", "cancelled"):
                    values["output_ref"] = None
                await self.put("invocations", {"invocation_id": iid}, values)
                if path[1] in ("status", "pending_outcome", "control_state"):
                    change("invocation", iid, {path[1]: value})

        inv = await self.get("invocations", invocation_id=iid)
        if inv is None:
            raise ValueError(
                "Runtime Event has no InvocationStarted or imported anchor."
            )
        await self.put("invocations", {"invocation_id": iid}, {"last_sequence": seq})
        oid, cid, wid = (
            payload.get("occurrence_id"),
            payload.get("call_id"),
            payload.get("wait_id"),
        )
        if name in ("invocation.completed", "invocation.settling", "invocation.failed"):
            values = {}
            if "output" in payload and (
                name != "invocation.settling" or payload["outcome"] == "completed"
            ):
                values["output_ref"] = ref("payload", "output")
            else:
                values["output_ref"] = None
            if payload.get("error"):
                values.update(
                    error_ref=ref("payload", "error"),
                    error_type=payload["error"]["type"],
                    error_code=payload["error"].get("code"),
                )
            await self.put("invocations", {"invocation_id": iid}, values)
        elif name == "invocation.cancelled":
            await self.put("invocations", {"invocation_id": iid}, {"output_ref": None})
        if name == "operator_call.started":
            await self.put(
                "operator_calls",
                {"invocation_id": iid, "call_id": cid},
                {
                    "operator_definition_id": operator_definition_id,
                    "input_ref": ref("payload", "input"),
                },
            )
        elif name == "operator_call.completed":
            await self.put(
                "operator_calls",
                {"invocation_id": iid, "call_id": cid},
                {"output_ref": ref("payload", "output")},
            )
        elif name == "operator_call.failed":
            await self.put(
                "operator_calls",
                {"invocation_id": iid, "call_id": cid},
                {
                    "error_ref": ref("payload", "error"),
                    "error_type": payload["error"]["type"],
                    "error_code": payload["error"].get("code"),
                },
            )
        elif name == "input.mapped":
            await self.put(
                "node_executions",
                {"invocation_id": iid, "occurrence_id": oid},
                {"input_ref": ref("payload", "mapped_input")},
            )
        elif name == "node_occurrence.completed":
            await self.put(
                "node_executions",
                {"invocation_id": iid, "occurrence_id": oid},
                {
                    "output_ref": ref("payload", "output"),
                    "metrics_ref": ref("payload", "metrics"),
                },
            )
            await outgoing(
                oid,
                await self.get("node_executions", invocation_id=iid, occurrence_id=oid),
            )
        elif name in ("node_occurrence.failed", "node.faulted"):
            await self.put(
                "node_executions",
                {"invocation_id": iid, "occurrence_id": oid},
                {
                    "error_ref": ref("payload", "error"),
                    "error_type": payload["error"]["type"],
                    "error_code": payload["error"].get("code"),
                    "error_phase": payload.get("phase")
                    or payload["error"].get("phase"),
                },
            )
            change(
                "node",
                oid,
                {
                    "error_type": payload["error"]["type"],
                    "error_code": payload["error"].get("code"),
                    "phase": payload.get("phase"),
                },
            )
            if name == "node_occurrence.failed":
                await outgoing(
                    oid,
                    await self.get(
                        "node_executions", invocation_id=iid, occurrence_id=oid
                    ),
                )
        elif name == "node_occurrence.waiting":
            await self.put(
                "waits",
                {"invocation_id": iid, "wait_id": wid},
                {"request_ref": ref("payload", "request"), "deadline_at_us": payload.get("deadline_at_us")},
            )
        elif name in ("wait.resumed", "command.awakened"):
            await self.put(
                "waits",
                {"invocation_id": iid, "wait_id": wid},
                {
                    "response_ref": ref(
                        "payload", "response" if name == "wait.resumed" else "output"
                    )
                },
            )
        elif name == "routing.resolved":
            for index, condition in enumerate(payload["conditions"]):
                await self.put(
                    "edge_evaluations",
                    {"event_id": event["id"], "result_index": index},
                    {
                        "invocation_id": iid,
                        "source_occurrence_id": oid,
                        "edge_id": condition["edge_id"],
                        "source_status": payload["source_status"],
                        "condition_result": condition["selected"],
                        "duration_ns": condition["duration_ns"],
                    },
                )
        if "duration_ns" in payload and oid:
            change("node", oid, {"stage": name})
        for index, (typ, identity, values) in enumerate(changes):
            await self.put(
                "graph_changes",
                {"event_id": event["id"], "change_index": index},
                {
                    "session_id": sid,
                    "invocation_id": iid,
                    "sequence": seq,
                    "store_order": order,
                    "occurred_at_us": stamp,
                    "entity_type": typ,
                    "entity_id": identity,
                    "change": values,
                    "duration_ns": payload.get("duration_ns")
                    if values.get("stage") == name
                    else None,
                    "error_type": values.get("error_type"),
                    "error_code": values.get("error_code"),
                },
            )

    async def checkpoint(self, record, checkpoint_id, *, anchor):
        """Seed only sessions whose pre-anchor history is absent; never regress."""
        kind, identity = "checkpoint", checkpoint_id
        by_id = {
            s["session_id"]: (index, s) for index, s in enumerate(record["sessions"])
        }
        root = by_id[record["root_session_id"]][1]["state"]["invocation"]["id"]
        ownership = {}
        for _, session in by_id.values():
            inv = session["state"]["invocation"]
            if inv:
                for plan in inv.get("child_plans", {}).values():
                    for unit in plan["units"]:
                        ownership[unit["invocation_id"]] = (inv["id"], plan, unit)
        # Parents precede Children, including planned but unopened units.
        pending = [record["root_session_id"]]
        while pending:
            sid = pending.pop(0)
            index, session = by_id[sid]
            state, seq = session["state"], session["state"]["sequence"]
            old = await self.get("sessions", session_id=sid)
            if not anchor or old and old["durable_sequence"] >= seq:
                continue
            inv = state["invocation"]
            await self.put(
                "sessions",
                {"session_id": sid},
                {
                    "created_at_us": state["session"]["created_at_us"],
                    "latest_invocation_id": state["session"]["latest_invocation_id"],
                    "durable_sequence": seq,
                    "history_start_sequence": seq + 1,
                },
            )
            if not inv:
                continue
            iid = inv["id"]
            path = ["sessions", index, "state", "invocation"]
            values = {
                "session_id": sid,
                "workflow_revision_id": inv["workflow_revision_id"],
                "entry_node_id": inv["entry_node_id"],
                "status": inv["status"],
                "pending_outcome": inv.get("pending_outcome"),
                "control_state": inv.get("control_state", "active"),
                "recovery_count": inv.get("recovery_count", 0),
                "last_recovery_id": inv.get("last_recovery_id"),
                "recovery_pending": bool(inv.get("pending_recovery")),
                "started_at_us": inv["started_at_us"],
                "completed_at_us": inv["completed_at_us"],
                "last_sequence": seq,
                "root_invocation_id": root,
            }
            if iid in ownership:
                parent, plan, unit = ownership[iid]
                values.update(
                    parent_invocation_id=parent,
                    parent_occurrence_id=plan["parent_occurrence_id"],
                    creation_id=plan["creation_id"],
                    unit_index=unit["unit_index"],
                    ownership_phase=unit["phase"],
                )
            if inv.get("kind") != "child_result":
                values["input_ref"] = source(kind, identity, [*path, "input"])
            if inv["status"] in ("completed", "settling"):
                values["output_ref"] = source(kind, identity, [*path, "output"])
            if inv.get("error"):
                values.update(
                    error_ref=source(kind, identity, [*path, "error"]),
                    error_type=inv["error"]["type"],
                    error_code=inv["error"].get("code"),
                )
            await self.put("invocations", {"invocation_id": iid}, values)
            sched = inv.get("scheduler", {})
            for oid, occurrence in sched.get("occurrences", {}).items():
                row = {
                    k: occurrence.get(k)
                    for k in (
                        "node_id",
                        "scope",
                        "status",
                        "ready_at_us",
                        "started_at_us",
                        "completed_at_us",
                        "recovery_attempts",
                    )
                }
                row["phase"] = occurrence["execution"]["phase"]
                row["activations"] = [{"edge_id": a["edge_id"], "source_occurrence_id": a["source_occurrence_id"]} for a in occurrence.get("activations", [])]
                np = [*path, "scheduler", "occurrences", oid]
                if occurrence["execution"].get("mapped_input") is not None:
                    row["input_ref"] = source(
                        kind, identity, [*np, "execution", "mapped_input"]
                    )
                if occurrence.get("output") is not None:
                    row["output_ref"] = source(kind, identity, [*np, "output"])
                if occurrence.get("error"):
                    row.update(
                        error_ref=source(kind, identity, [*np, "error"]),
                        error_type=occurrence["error"]["type"],
                        error_code=occurrence["error"].get("code"),
                    )
                await self.put(
                    "node_executions", {"invocation_id": iid, "occurrence_id": oid}, row
                )
            for field, table, id_field in [
                ("operator_calls", "operator_calls", "call_id"),
                ("waits", "waits", "wait_id"),
            ]:
                for key, item in sched.get(field, {}).items():
                    columns = meta.tables[table].columns
                    row = {
                        k: v
                        for k, v in item.items()
                        if k in columns
                        and k
                        not in ("input", "output", "error", "request", "response", "id")
                    }
                    ip = [*path, "scheduler", field, key]
                    for f in ("input", "output", "request", "response", "error"):
                        if item.get(f) is not None:
                            row[f + "_ref"] = source(kind, identity, [*ip, f])
                    await self.put(table, {"invocation_id": iid, id_field: key}, row)
            for plan in inv.get("child_plans", {}).values():
                for unit in plan["units"]:
                    if unit["session_id"] in by_id:
                        pending.append(unit["session_id"])
                    else:
                        csid, ciid = unit["session_id"], unit["invocation_id"]
                        await self.put(
                            "sessions",
                            {"session_id": csid},
                            {
                                "created_at_us": None,
                                "latest_invocation_id": None,
                                "durable_sequence": 0,
                                "history_start_sequence": 1,
                            },
                        )
                        await self.put(
                            "invocations",
                            {"invocation_id": ciid},
                            {
                                "session_id": csid,
                                "workflow_revision_id": plan["workflow_revision_id"],
                                "entry_node_id": plan["entry_node_id"],
                                "status": None,
                                "last_sequence": 0,
                                "root_invocation_id": root,
                                "parent_invocation_id": iid,
                                "parent_occurrence_id": plan["parent_occurrence_id"],
                                "creation_id": plan["creation_id"],
                                "unit_index": unit["unit_index"],
                                "ownership_phase": unit["phase"],
                            },
                        )
