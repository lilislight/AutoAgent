# Project and Compiler Diagnostics

This reference owns diagnosis by stable code. It does not define CLI syntax,
Workflow APIs, or runtime Trace debugging.

## Read the document

A Compiler Diagnostic may contain:

- `code`: stable machine-readable identity;
- `severity`: `error`, `warning`, or `info`;
- `workflow_id`;
- `object_type`: Workflow, Node, or Edge;
- `object_id`;
- `field`;
- `message`;
- `hint`;
- `source_index`;
- additional metadata.

Use `object_type`, `object_id`, and `field` to locate authored source. Follow the
hint when it is compatible with the user requirement. Never parse only the
human message when a stable code is available.

Fix errors, review warnings, and re-run the exact failed check. Use
`--warnings-as-errors` in final validation when the project intends a clean
authoring contract.

## Project diagnostics

### Manifest

- `PROJECT_MANIFEST_NOT_FOUND`: pass the correct project directory or create
  `auto-agent.toml`.
- `PROJECT_MANIFEST_INVALID_TOML`: repair TOML syntax.
- `PROJECT_MANIFEST_INVALID`: use schema version 1, required project fields,
  at least one unique entrypoint, and no unknown fields.

### Workflow loading

- `WORKFLOW_MODULE_IMPORT_FAILED`: make the module importable from project root
  and fix import-time exceptions.
- `WORKFLOW_OBJECT_NOT_FOUND`: correct the object path after `:`.
- `WORKFLOW_OBJECT_INVALID`: export a `Workflow` object.
- `WORKFLOW_ID_DUPLICATE`: give every exported Workflow a unique stable ID.

Do not add path mutation code to Workflow modules to hide an invalid
entrypoint.

## Structural diagnostics

### Nodes and Edges

- `NODE_ID_REQUIRED`: provide a non-empty Node ID.
- `NODE_DUPLICATE_ID`: rename one Node and update its Edges.
- `EDGE_UNKNOWN_NODE`: correct the source or target Node reference.
- `EDGE_DUPLICATE_ID`: provide unique explicit Edge IDs.
- `WF_NO_ENTRY`: add a structurally valid entry or mark the intended entry.
- `WF_ENTRY_HAS_INCOMING_EDGE`: remove the incoming Edge or stop marking that
  Node as an explicit entry.

### Conditions and mappings

- `STRING_CONDITION_UNSUPPORTED`: use a supported callable Condition.
- `CONDITION_UNSUPPORTED`: provide a supported callable returning bool.
- `MAPPING_UNSUPPORTED`: use a supported callable Input Mapping or Output
  Binding.
- `OPERATOR_CONTRACT_UNAVAILABLE`: add concrete callable annotations.
- `OPERATOR_CONTRACT_INVALID`: make mapped arguments and outputs compatible
  with the Operator contract.

### Capability and Operator binding

- `CAPABILITY_NOT_REGISTERED`: use a known abstract capability or provide the
  required host integration.
- `CAPABILITY_HAS_NO_OPERATOR`: ensure runtime hosting registers an
  implementation; static Project Check may intentionally defer binding.
- `OPERATOR_NOT_REGISTERED`: an existing project uses a specific nonrecommended
  Operator reference that the host does not register; prefer a direct typed
  callable or a public abstract `CapabilityRef`.
- `CAPABILITY_UNSUPPORTED`: use a typed callable, public `CapabilityRef`,
  SystemCommand, or child Workflow.
- `SYSTEM_COMMAND_UNSUPPORTED`: V1 supports only `SystemCommand(id="wait")`.
- `SYSTEM_COMMAND_CONFIG_UNSUPPORTED`: remove unsupported command payload.

Do not import host registries into Workflow source to silence a static error.

## Loop diagnostics

- `LOOP_ENTRY_INVALID`: make the Loop reducible with one valid header and
  distinguish external entry Edges from back Edges.
- `LOOP_IRREDUCIBLE`: restructure cross-jumping cyclic paths into natural
  nested or separate Loops.
- `LOOP_OVERLAP_INVALID`: remove partially overlapping Loop regions; nesting
  or disjoint regions are valid.

After a Loop compiles, add a resource execution bound and test both continuation
and exit paths.

## Child Workflow diagnostics

- `SUBWORKFLOW_RECURSION`: remove recursive child expansion.
- `SUBWORKFLOW_SELECTOR_INVALID`: select a real child entry/exit when multiple
  boundaries exist.
- `SUBWORKFLOW_MAP_UNSUPPORTED`: move Map to a supported non-child boundary.
- `SUBWORKFLOW_NODE_BEHAVIOR_UNSUPPORTED`: remove parent Node behavior that
  cannot be applied to an expanded child Workflow.

## Policy diagnostics

### Selection, Retry, and recovery

- `POLICY_SELECTION_INVALID`
- `POLICY_SELECTION_OPERATOR_UNKNOWN`
- `POLICY_SELECTION_OPERATOR_MISMATCH`
- `POLICY_RETRY_INVALID`
- `POLICY_BACKOFF_INVALID`
- `POLICY_RECOVERY_IDEMPOTENCY_KEY_REQUIRED`
- `POLICY_TIMEOUT_INVALID`
- `POLICY_CONCURRENCY_INVALID`
- `POLICY_RESOURCE_INVALID`

Check positive limits, supported enum values, capability compatibility, and
idempotency requirements. Do not weaken safety constraints only to compile.

### Map and Replication

- `POLICY_REPLICATION_INVALID`
- `POLICY_MAP_INVALID`
- `SYSTEM_COMMAND_MAP_UNSUPPORTED`
- `POLICY_MAP_FAN_IN_UNSUPPORTED`
- `POLICY_MAP_INPUT_MAPPING_CONFLICT`
- `POLICY_MAP_REPLICATION_CONFLICT`

Choose one data source and one fan-out model for the target Node. Move
aggregation to the Map/Replication aggregator rather than combining
unsupported policies.

### Aggregator warning

- `POLICY_AGGREGATOR_OUTPUT_UNVERIFIED`: add a concrete aggregator return
  annotation compatible with the target Node output contract.

## Repair loop

1. Record the stable code and object ID.
2. Read only the owning reference for that category.
3. Make the smallest semantic correction.
4. Run `workflow check` again.
5. Run the affected Invocation path and tests after compilation succeeds.

If source appears correct but the same Diagnostic persists, report a possible
framework defect with the minimal Workflow reproducer instead of reaching into
Compiler internals.
