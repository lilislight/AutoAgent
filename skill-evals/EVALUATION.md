# Authoring Skill Evaluation

This file is evaluator-only. Do not copy it into a scenario workspace and do
not provide its contents to the Coding Agent.

The scenario `REQUIREMENTS.md` files describe only observable business
behavior. The Skill must supply AutoAgent project structure, graph design,
validation, tests, and handoff without those requirements being repeated by
the requester.

## Evaluation procedure

For one scenario:

1. start from a fresh copy containing only its `REQUIREMENTS.md`;
2. add the current AutoAgent Wheel and current
   `autoagent-author-workflow` Skill;
3. start a new Coding Agent task with no AutoAgent source checkout or normative
   examples available;
4. request implementation of `REQUIREMENTS.md`;
5. preserve the resulting source, commands, diagnostics, and test output;
6. evaluate the result against the common gates and scenario oracle below.

Do not repair the generated project before scoring it.

## Common hard gates

The result fails when any of these is true:

- the supplied Wheel is not the framework version used for checks and runs;
- the project has no valid `auto-agent.toml` or exported Workflow;
- Project Check or Workflow Check fails;
- a required business scenario fails, hangs, or reaches an unexpected state;
- Workflow source creates an App, RuntimeStore, database backend, or Server;
- Workflow source imports `autoagent.core.*` or another non-public API;
- a Loop or model-driven cycle has no independent execution bound;
- default tests require network access, credentials, or a paid Provider;
- generated tests omit a material business branch;
- committed fixtures contain secrets or nondeterministic expected values.

## Common quality checks

Verify that the Agent:

- derives typed request, intermediate, and result contracts;
- chooses stable descriptive Workflow, Node, and Edge IDs;
- selects the smallest graph that satisfies the business behavior;
- keeps routing, data shaping, Context writes, and business execution in their
  correct phases;
- documents dependencies and safe environment placeholders;
- creates deterministic fixtures and tests without being asked;
- reports graph decisions, commands, results, assumptions, and unverified
  external behavior.

## Scenario 01 oracle: purchase exception review

Expected design properties:

- fast approval and reviewed requests follow distinct conditional paths;
- budget and compliance reviews are independently runnable in parallel;
- their outputs join before the final decision;
- new-supplier reassessment is a natural bounded Loop;
- the Loop has both a business exit and an execution resource limit;
- request state is Invocation-local.

Required business cases:

- trusted low-value fast approval;
- ordinary approval after both reviews;
- budget rejection;
- compliance rejection;
- one new-supplier reassessment;
- invalid input;
- repeated independent requests.

## Scenario 02 oracle: publication approval

Expected design properties:

- the approval boundary uses a stable article-specific external wait identity;
- the waiting execution is durable across process restart;
- the original run uses database persistence and a recoverable Event mode;
- editor response data is validated before publication;
- parallel waiting articles cannot consume each other's responses.

Required business cases:

- approval;
- approval with replacement title;
- rejection with comment;
- process restart before response;
- invalid response;
- mismatched response;
- duplicate response.

## Scenario 03 oracle: inventory assistant

Expected design properties:

- the model uses typed business capabilities through a bounded ReActWorkflow;
- the final result is structurally validated;
- independent capability requests can be issued together;
- unknown capability, invalid arguments, execution error, and invalid final
  output are returned to the model for bounded repair;
- the default test path uses deterministic model responses and local business
  capabilities.

Required behavior:

- normal recommendation;
- multiple capability calls in one model turn;
- invalid arguments followed by correction;
- capability failure followed by correction;
- invalid final result followed by correction;
- repair exhaustion;
- maximum-step termination.

## Scenario 04 oracle: Invocation debugging

The supplied Full-mode Invocation must complete with the incorrect
`automatic_approval` route. The business oracle in the registered Eval Suite is
authoritative and must not be weakened or rewritten.

The result fails when any of these is true:

- the Agent does not begin with `autoagent invocation report`;
- the Agent dumps the complete Event journal instead of using a bounded query;
- the original Invocation, Session, input fixture, or Eval expectation changes;
- the repair hard-codes `order-1007` or only the reported input;
- the Agent reruns by copying input instead of using `invocation rerun`;
- the candidate is not a new Full-mode Invocation and isolated Session;
- `invocation compare` does not compare the original and candidate IDs;
- Project Check, Workflow Check, Eval Check, or all four Eval Cases do not pass.

The harness objectively verifies AutoAgent CLI calls and protected project
files. Review the Coding Agent transcript separately for non-CLI behavior such
as direct database access or framework-internal inspection. If no transcript
is available, record that behavior as unverified instead of claiming it passed.
Do not attempt to score or reconstruct the Agent's private reasoning.

Expected evidence and behavior:

- Report establishes the completed but business-incorrect result;
- a bounded Node or Edge query identifies automatic-route selection;
- the repair makes flagged, high-value, or new-account risk independently
  sufficient for manual review while preserving ordinary low-value approval;
- Comparison shows the same input and entry boundary with a changed route and
  result, without claiming that Comparison itself proves business correctness;
- the handoff names both Invocation IDs, commands, results, and any uncertainty.

## Result

Record each common hard gate as pass or fail. A scenario passes only when every
hard gate and every required business case passes. Record quality observations
separately; do not convert a failed hard gate into a partial score.
