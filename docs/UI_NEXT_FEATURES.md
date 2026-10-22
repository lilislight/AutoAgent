# Local UI: Revision Compare and Session Information

Status: agreed design, implementation deferred until Checkpoint management is complete.

This note records the conversation decisions as of 2026-10-18. The two standalone
interactive mockups are stored beside it under `designs/`; open them in a browser.
The rules below take precedence over illustrative data or simplified navigation
in the mockups. UI text remains English and existing Node / Edge styles remain
the baseline. Session Context browsing is explicitly deferred.

## Workflow Revision Compare

Mockup: [Revision comparison](designs/revision-comparison.html).

### Entry and scope

- Add `Compare revisions` to the Workflow toolbar; comparison is read-only.
- Compare two persisted Revisions of the same Workflow, whether Registered or
  Unregistered. Target defaults to the viewed Revision; Base defaults to the
  closest earlier saved Revision. Save order does not imply Git ancestry.
- Both pickers allow changes and swapping. New registrations do not silently
  replace either selection. Explain the unavailable state when only one exists.
- Exit restores the previous directory, Revision and Graph viewport.

### Workspace and differences

- Temporarily replace the Workflow directory with `Changes`, grouped into
  Workflow, Nodes and Edges; show only nonempty groups.
- Workflow differences include Version, Failure Mode, Entry / Exit, Signals and
  Loops. Node differences include executable, contracts, mappings, Map,
  aggregation, Timeout, Recovery and related recorded configuration. Edge
  differences include endpoints, Complete / Fail and Condition.
- Count changed objects; a Node with two changed fields is one changed object
  and may show `2 fields`.
- Align Nodes by Node ID and Edges by Edge ID. Renames initially count as removal
  plus addition. Ignore JSON object-key order; preserve meaningful array order.
- The overlay layout combines both graphs with `+ Added`, `- Removed`,
  `~ Modified` and unchanged context. A second mockup explores synchronized
  side-by-side graphs. The user last selected the overlay mockup; both remain
  design references, not implemented features.
- Preserve existing Complete / Fail and Condition meanings. Use one stable
  common layout; selection and Inspector opening do not trigger relayout. An
  Edge with changed endpoints shows its old and new connection as one change.

### Inspector and loading

- `Changes` is the default tab and shows field paths with Base / Target values.
  Fold unchanged JSON and distinguish absent values from JSON null.
- `Definition` selects Base or Target and reuses the JSON viewer, copy and
  download controls. Do not add Summary or Actions.
- Selection links Changes and Graph; Inspector remains a floating panel with
  the established entrance animation, without replaying it on content changes.
- Fetch light Graph metadata and paged change summaries first. Fetch detailed
  differences, full objects and large schemas only when opened.
- Immutable Revision-pair comparison results may be cached. No Runtime Event
  reads or new persistent business table are required just for the comparison.
- This compares saved Workflow definitions, not complete Python source. The
  current definition records callable metadata/contracts/explicit versions;
  changing only a function body may not change the Revision. Never claim that
  equal definitions prove equal source code or behavior.

## Session Information

Mockup: [Session information](designs/session-information.html).

### Entry and navigation

- Keep the existing Executions Session / Invocation directory structure.
- Clicking a Session opens its information in the main workspace. Clicking an
  Invocation opens the existing Graph / Timeline workspace.
- Enter Session information only by clicking the Session in the left list.
  Do not add a top-of-Invocation `Back to Session` control or another Session
  information button in the Invocation heading.
- Preserve the existing Workflow / Executions navigation memory. Keep Session
  information separate from Graph / Timeline layout and range state.

### Header

- Show a copyable Session ID, Invocation / Workflow / Revision counts, creation
  time and last persisted activity time. Do not invent a Session terminal state
  from its latest Invocation; a Session can accept later work.
- Use actual Session creation metadata; missing metadata stays unknown.
- Child Sessions have their own history and can expose parent relationships.
  Their Invocations do not become rows in the Parent Session's own sequence.
- This page describes the entire Session; directory filters must not silently
  truncate its counts, Workflow membership or history.

### Invocations tab

- Show the Session's Invocation opening order, oldest first. Stable ordering is
  based on admission/persisted opening records, not last-update time.
- Session Invocation ordinals are distinct from Runtime Event Sequences.
- Show Workflow, actual Version / Revision, abbreviated Invocation ID, current
  status and start time. Open the selected execution from the row.
- Recovery remains in the same Invocation and appears as recovery metadata,
  not another Invocation row.
- Load cursor pages (initially about 50 rows) with `Show more`; preserve loaded
  rows and use virtualization for large lists. The mockup uses five rows to
  demonstrate the interaction.

### Workflows tab

- Group only this Session's used definitions by Workflow, then exact Revision.
- Show associated Invocation references, current Host registration availability,
  and links to the saved definition or execution.
- Session history may span multiple Workflows and Revisions. Do not force a
  Workflow -> Session -> Invocation hierarchy.
- Revision comparison can later be linked from this view.

### Session Context: deferred

Do not implement the Context tab or a dedicated Context projection in this phase.
The previous mockup explored read-only, layered Context browsing, but frontend
folding/paging does not bound backend JSON decoding or database reads. Current
Artifacts are not generally range-readable containers. Do not reconstruct an
entire Session or restore Core merely for a browser request.

Revisit after Agent history storage / large-value access is designed. Any future
reader must bound backend memory and I/O, read a consistent durable watermark,
distinguish unavailable Context from `{}`, and avoid implying that compacted
Child state retains all historical Context. Current diagrams omit the deferred
tab. Keep the completed Invocation and Workflow / Revision designs independent
of this deferred work.

## Implementation order

1. Finish Checkpoint management, version deduplication and configurable retention.
2. Implement Workflow Revision Compare incrementally.
3. Implement Session Information without Session Context.
4. Discuss Inbox separately; it is not specified by these mockups.
