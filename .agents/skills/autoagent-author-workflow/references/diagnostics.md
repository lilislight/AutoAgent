# Project and Compiler Diagnostics

Use this reference after `project check` or `workflow check` fails. The
Diagnostic itself is authoritative; this file supplies the repair process and
category boundaries rather than duplicating every Compiler code.

## Read the Diagnostic

Prefer its stable fields over parsing the message:

- `code` and `severity` identify the rule;
- `workflow_id`, `object_type`, and `object_id` locate the authored object;
- `field` locates the invalid setting;
- `message`, `hint`, and metadata explain the concrete conflict.

Fix errors, review warnings, and rerun the exact failed command. Use
`--warnings-as-errors` for final validation when the project expects no
warnings.

## Repair by category

### Project loading

For `PROJECT_MANIFEST_*` codes, repair `auto-agent.toml` against
[project-contract.md](project-contract.md). For `WORKFLOW_MODULE_*` or
`WORKFLOW_OBJECT_*`, make the entrypoint importable from the project root and
export an actual `Workflow`. Do not mutate `sys.path` in Workflow modules.

### Graph structure

For Node, Edge, entry, or duplicate-ID codes, use explicit stable IDs and fix
the referenced topology. Entry Nodes cannot have incoming Edges; exits are
defined structurally by having no outgoing Edge.

For `LOOP_*` codes, rebuild the cycle as a natural Loop with one valid header,
an external entry, and clear back Edges. Nested or disjoint Loops are valid;
partially overlapping regions are not. Add a resource bound after it compiles.

For `SUBWORKFLOW_*` codes, remove recursive expansion, select real child
boundaries when required, and move unsupported parent behavior to an ordinary
Node or Edge outside the child boundary.

### Callable contracts and Hooks

For Condition, mapping, or Operator contract codes, use a supported named
callable with concrete parameter and return annotations. Make Input Mapping
keys match the target callable and keep Conditions boolean.

For capability binding codes, prefer a direct typed callable for project-owned
logic or a public `CapabilityRef` for a host-provided implementation. Do not
import host registries into Workflow source to silence a static error.

### Policies

For `POLICY_*` codes, inspect the exact policy field, supported enum, positive
limit, callable contract, and target Node combination. Common conflicts are:

- capability selection on a direct callable;
- Map combined with target Input Mapping, fan-in, Replication, a child
  Workflow, or a SystemCommand;
- an unbounded Loop;
- recovery marked idempotent without the required idempotency contract;
- an aggregator whose return annotation cannot be checked.

Do not weaken safety constraints merely to compile. Read
[policies.md](policies.md) or [workflow-design.md](workflow-design.md) for the
owning behavior.

### Evaluation loading and execution

For `EVAL_SUITE_*`, `EVAL_MODULE_*`, or `EVAL_OBJECT_*` diagnostics, repair the
Manifest ID, Workflow target, importable `module:EvaluationClass` entrypoint,
or `Evaluation` subclass. Eval modules are deliberately loaded only by
`autoagent eval`.

`autoagent eval run` exit status `1` means a business Evaluator failed; inspect
the Case, Step, Evaluator value, and comment before changing Workflow code.
Exit status `2` means loading, configuration, Provider, or Evaluator
infrastructure failed and must not be presented as a business mismatch.

## Suspected framework defects

Reading framework source is allowed only when reproducible evidence indicates
that the installed public API, Compiler, or Runtime is defective:

1. record the selected AutoAgent version and module path;
2. reduce the failure to the smallest public-API Workflow and deterministic
   input;
3. reproduce against the selected installed package;
4. inspect its implementation only far enough to identify the likely defect;
5. do not copy internal implementation or imports into the authored project;
6. do not modify AutoAgent framework source unless the requester explicitly
   changes the task to framework maintenance.

Report the version, path, reproducer, expected behavior, actual result, and
suspected cause. Do not hide the defect with an internal API workaround.

## Repair loop

1. Record the Diagnostic code and authored object ID.
2. Read only the reference that owns that category.
3. Make the smallest semantic correction.
4. Rerun the same check.
5. After compilation succeeds, run `autoagent eval check` and the affected Eval
   Case.
6. Before handoff, run the complete registered Suite.
