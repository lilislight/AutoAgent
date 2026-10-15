"""SQLAlchemy schema for canonical facts, projections and shared Artifacts."""

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects import postgresql

meta = MetaData()
J = JSON(none_as_null=True).with_variant(
    postgresql.JSONB(none_as_null=True), "postgresql"
)
B = BigInteger().with_variant(Integer(), "sqlite")


def c(name, type_=Text, nullable=True, pk=False, fk=None):
    # Cursor comparisons and accepted overlays must use the same ordering on
    # databases whose default text collations differ.
    if type_ is Text:
        type_ = Text(collation="C").with_variant(Text(collation="BINARY"), "sqlite")
    args = [ForeignKey(fk)] if fk else []
    return Column(name, type_, *args, nullable=nullable, primary_key=pk)


def table(name, *cols):
    return Table(name, meta, *cols)


table(
    "persistence_meta",
    c("id", Integer, False, True),
    c("format_version", Integer, False),
    c("projection_version", Integer, False),
    c("last_store_order", B, False),
    c("store_id", Text, False),
    CheckConstraint("id = 1"),
)
table(
    "artifacts",
    c("hash", Text, False, True),
    c("value_type", Text, False),
    c("logical_bytes", B, False),
    c("body", LargeBinary, False),
    CheckConstraint("value_type IN ('string','array','object')"),
    CheckConstraint("logical_bytes >= 0"),
)
table(
    "artifact_dependencies",
    c("parent_hash", Text, False, True, "artifacts.hash"),
    c("slot", Integer, False, True),
    c("path", Text, False),
    c("child_hash", Text, False, False, "artifacts.hash"),
    CheckConstraint("slot >= 0"),
    CheckConstraint("parent_hash <> child_hash"),
)
table(
    "artifact_refs",
    c("owner_kind", Text, False, True),
    c("owner_id", Text, False, True),
    c("slot", Integer, False, True),
    c("path", Text, False),
    c("artifact_hash", Text, False, False, "artifacts.hash"),
    CheckConstraint(
        "owner_kind IN ('workflow_revision','operator_definition','runtime_event','user_event','checkpoint')"
    ),
    CheckConstraint("slot >= 0"),
)
table(
    "workflow_revisions",
    c("revision_id", Text, False, True),
    c("workflow_id", Text, False),
    c("workflow_version", Text, False),
    c("definition_hash", Text, False),
    c("registered_at_us", B, False),
    c("snapshot_text", Text, False),
)
table(
    "operator_definitions",
    c("definition_id", Text, False, True),
    c("operator_id", Text, False),
    c("version", Text, False),
    c("definition_text", Text, False),
)
table(
    "runtime_events",
    c("event_id", Text, False, True),
    c("session_id", Text, False),
    c("invocation_id"),
    c("sequence", B, False),
    c("event_name", Text, False),
    c("occurred_at_us", B, False),
    c("store_order", B, False),
    c("schema_version", Integer, False),
    c(
        "operator_definition_id",
        Text,
        True,
        False,
        "operator_definitions.definition_id",
    ),
    c("payload_text", Text, False),
    c("delta_text"),
    UniqueConstraint("session_id", "sequence"),
    UniqueConstraint("store_order"),
    CheckConstraint("sequence >= 1"),
    CheckConstraint("store_order >= 1"),
    CheckConstraint(
        "operator_definition_id IS NULL OR event_name IN ('operator_call.started','capability.resolved')"
    ),
)
table(
    "user_events",
    c("event_id", Text, False, True),
    c("session_id", Text, False),
    c("invocation_id", Text, False),
    c("sequence", B, False),
    c("kind", Text, False),
    c("occurrence_id"),
    c("occurred_at_us", B, False),
    c("payload_text", Text, False),
    UniqueConstraint("invocation_id", "sequence"),
    CheckConstraint("sequence >= 1"),
)
table(
    "checkpoints",
    c("checkpoint_id", Text, False, True),
    c("root_session_id", Text, False),
    c("captured_at_us", B, False),
    c("digest", Text, False),
    c("checkpoint_text", Text, False),
    UniqueConstraint("digest"),
)
table(
    "checkpoint_sessions",
    c("checkpoint_id", Text, False, True, "checkpoints.checkpoint_id"),
    c("session_id", Text, False, True),
    c("invocation_id"),
    c("workflow_revision_id", Text, True, False, "workflow_revisions.revision_id"),
    c("sequence", B, False),
    CheckConstraint("sequence >= 1"),
)
table(
    "sessions",
    c("session_id", Text, False, True),
    c("created_at_us", B),
    c("latest_invocation_id"),
    c("durable_sequence", B, False),
    c("history_start_sequence", B, False),
    CheckConstraint("durable_sequence >= 0"),
    CheckConstraint("history_start_sequence >= 1"),
)
table(
    "invocations",
    c("invocation_id", Text, False, True),
    c("updated_store_order", B, False),
    c("session_id", Text, False, False, "sessions.session_id"),
    c("workflow_revision_id", Text, False, False, "workflow_revisions.revision_id"),
    c("entry_node_id", Text, False),
    c("status"),
    c("pending_outcome"),
    c("control_state"),
    c("started_at_us", B),
    c("completed_at_us", B),
    c("last_sequence", B, False),
    c("root_invocation_id", Text, False),
    c("parent_invocation_id"),
    c("parent_occurrence_id"),
    c("creation_id"),
    c("unit_index", Integer),
    c("ownership_phase"),
    c("input_ref", J),
    c("output_ref", J),
    c("error_ref", J),
    c("error_type"),
    c("error_code"),
    CheckConstraint(
        "status IS NULL OR status IN ('created','running','waiting','settling','completed','failed','cancelled')"
    ),
    CheckConstraint(
        "pending_outcome IS NULL OR pending_outcome IN ('completed','failed','cancelled')"
    ),
    CheckConstraint(
        "ownership_phase IS NULL OR ownership_phase IN ('planned','opened','accepted','terminal','abandoned')"
    ),
    CheckConstraint("unit_index IS NULL OR unit_index >= 0"),
    CheckConstraint(
        "(parent_invocation_id IS NULL AND parent_occurrence_id IS NULL AND creation_id IS NULL AND unit_index IS NULL AND ownership_phase IS NULL) OR (parent_invocation_id IS NOT NULL AND parent_occurrence_id IS NOT NULL AND creation_id IS NOT NULL AND unit_index IS NOT NULL AND ownership_phase IS NOT NULL)"
    ),
    CheckConstraint(
        "parent_invocation_id IS NOT NULL OR root_invocation_id = invocation_id"
    ),
    UniqueConstraint("parent_invocation_id", "creation_id", "unit_index"),
    ForeignKeyConstraint(["root_invocation_id"], ["invocations.invocation_id"]),
    ForeignKeyConstraint(
        ["parent_invocation_id", "parent_occurrence_id"],
        ["node_executions.invocation_id", "node_executions.occurrence_id"],
        name="fk_invocation_parent_occurrence",
        use_alter=True,
    ),
)
table(
    "node_executions",
    c("invocation_id", Text, False, True, "invocations.invocation_id"),
    c("occurrence_id", Text, False, True),
    c("updated_store_order", B, False),
    c("ordinal", B, False),
    c("activations", J, False),
    c("node_id", Text, False),
    c("scope", J, False),
    c("status", Text, False),
    c("phase"),
    c("ready_at_us", B),
    c("started_at_us", B),
    c("completed_at_us", B),
    c("recovery_attempts", Integer, False),
    c("input_ref", J),
    c("output_ref", J),
    c("metrics_ref", J),
    c("error_ref", J),
    c("error_type"),
    c("error_code"),
    c("error_phase"),
    CheckConstraint(
        "status IN ('ready','running','waiting','completed','failed','skipped','cancelled')"
    ),
    CheckConstraint("recovery_attempts >= 0"),
)
table(
    "operator_calls",
    c("invocation_id", Text, False, True, "invocations.invocation_id"),
    c("call_id", Text, False, True),
    c("updated_store_order", B, False),
    c("occurrence_id", Text, False),
    c("operator_id", Text, False),
    c(
        "operator_definition_id",
        Text,
        True,
        False,
        "operator_definitions.definition_id",
    ),
    c("unit_index", Integer, False),
    c("status", Text, False),
    c("started_at_us", B, False),
    c("completed_at_us", B),
    c("queue_duration_ns", B, False),
    c("execution_duration_ns", B),
    c("input_ref", J),
    c("output_ref", J),
    c("error_ref", J),
    c("error_type"),
    c("error_code"),
    ForeignKeyConstraint(
        ["invocation_id", "occurrence_id"],
        ["node_executions.invocation_id", "node_executions.occurrence_id"],
    ),
    CheckConstraint("status IN ('running','completed','failed','lost','cancelled')"),
    CheckConstraint("unit_index >= 0"),
    CheckConstraint("queue_duration_ns >= 0"),
    CheckConstraint("execution_duration_ns IS NULL OR execution_duration_ns >= 0"),
)
table(
    "waits",
    c("invocation_id", Text, False, True, "invocations.invocation_id"),
    c("wait_id", Text, False, True),
    c("updated_store_order", B, False),
    c("occurrence_id", Text, False),
    c("kind", Text, False),
    c("status", Text, False),
    c("created_at_us", B, False),
    c("resumed_at_us", B),
    c("deadline_at_us", B),
    c("request_ref", J),
    c("response_ref", J),
    ForeignKeyConstraint(
        ["invocation_id", "occurrence_id"],
        ["node_executions.invocation_id", "node_executions.occurrence_id"],
    ),
    CheckConstraint("status IN ('waiting','resumed','cancelled')"),
)
table(
    "edge_evaluations",
    c("event_id", Text, False, True, "runtime_events.event_id"),
    c("result_index", Integer, False, True),
    c("invocation_id", Text, False, False, "invocations.invocation_id"),
    c("source_occurrence_id", Text, False),
    c("edge_id", Text, False),
    c("source_status", Text, False),
    c("condition_result", Boolean, False),
    c("duration_ns", B, False),
    CheckConstraint("source_status IN ('complete','error')"),
    CheckConstraint("result_index >= 0"),
    CheckConstraint("duration_ns >= 0"),
    ForeignKeyConstraint(
        ["invocation_id", "source_occurrence_id"],
        ["node_executions.invocation_id", "node_executions.occurrence_id"],
    ),
)
table(
    "graph_changes",
    c("event_id", Text, False, True, "runtime_events.event_id"),
    c("change_index", Integer, False, True),
    c("session_id", Text, False),
    c("invocation_id", Text, False, False, "invocations.invocation_id"),
    c("sequence", B, False),
    c("store_order", B, False),
    c("occurred_at_us", B, False),
    c("entity_type", Text, False),
    c("entity_id", Text, False),
    c("change", J, False),
    c("duration_ns", B),
    c("error_type"),
    c("error_code"),
    CheckConstraint("entity_type IN ('invocation','node','edge','wait','scope')"),
    CheckConstraint("change_index >= 0"),
    CheckConstraint("duration_ns IS NULL OR duration_ns >= 0"),
)

indexes = {
    "artifact_dependencies": [("artifact_dep_child", ["child_hash"])],
    "artifact_refs": [("artifact_ref_hash", ["artifact_hash"])],
    "workflow_revisions": [
        ("workflow_history", ["workflow_id", "registered_at_us", "revision_id"])
    ],
    "operator_definitions": [
        ("operator_versions", ["operator_id", "version", "definition_id"])
    ],
    "runtime_events": [("runtime_invocation_seq", ["invocation_id", "sequence"]),
                       ("runtime_invocation_order", ["invocation_id", "store_order"])],
    "user_events": [
        ("user_occurrence_seq", ["invocation_id", "occurrence_id", "sequence"])
    ],
    "checkpoints": [
        ("checkpoint_root_time", ["root_session_id", "captured_at_us", "checkpoint_id"])
    ],
    "invocations": [
        (
            "invocation_workflow_time",
            ["workflow_revision_id", "started_at_us", "invocation_id"],
        ),
        ("invocation_status_time", ["status", "started_at_us", "invocation_id"]),
        ("invocation_session_time", ["session_id", "started_at_us", "invocation_id"]),
        ("invocation_root", ["root_invocation_id", "invocation_id"]),
        ("invocation_parent", ["parent_invocation_id", "unit_index", "invocation_id"]),
        ("invocation_root_update", ["root_invocation_id", "updated_store_order"]),
    ],
    "node_executions": [
        ("node_statistics", ["node_id", "status", "started_at_us", "invocation_id"]),
        ("node_updates", ["invocation_id", "updated_store_order"]),
        ("node_ordinal", ["invocation_id", "node_id", "ordinal"]),
    ],
    "operator_calls": [
        ("call_occurrence", ["invocation_id", "occurrence_id", "unit_index"]),
        ("call_updates", ["invocation_id", "updated_store_order"]),
        (
            "operator_statistics",
            ["operator_id", "status", "started_at_us", "invocation_id"],
        ),
    ],
    "waits": [("invocation_wait_status", ["invocation_id", "status", "wait_id"]),
              ("wait_updates", ["invocation_id", "updated_store_order"]),
              ("wait_deadline", ["status", "deadline_at_us", "invocation_id"])],
    "edge_evaluations": [
        ("edge_statistics", ["edge_id", "source_status", "invocation_id"])
    ],
    "graph_changes": [
        ("graph_updates", ["invocation_id", "store_order", "change_index"]),
        ("graph_replay", ["invocation_id", "sequence", "change_index"]),
        (
            "graph_entity_history",
            ["invocation_id", "entity_type", "entity_id", "sequence", "change_index"],
        ),
    ],
}
for name, items in indexes.items():
    t = meta.tables[name]
    for index_name, cols in items:
        Index(index_name, *[t.c[x] for x in cols])

g = meta.tables['graph_changes']
Index('graph_edge_order', g.c.invocation_id, g.c.change['edge_id'].as_string(), g.c.store_order)
