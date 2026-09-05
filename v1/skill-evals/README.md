# AutoAgent Skill Evaluations

Scenarios 01–03 evaluate whether a Coding Agent can translate a framework-free
business request into a working AutoAgent project. Each of those scenario
directories intentionally contains only its `REQUIREMENTS.md` before a clean
test begins.

Before an evaluation, copy the current AutoAgent Wheel and the current
`autoagent-author-workflow` Skill into the selected directory. Then copy that
directory outside the AutoAgent repository and run a fresh Coding Agent task
with it as the workspace. Give the Agent only this instruction:

```text
Implement the project described in REQUIREMENTS.md. Use the provided
autoagent-author-workflow Skill and AutoAgent Wheel. Do not inspect the
AutoAgent framework source or repository examples.
```

The Agent should install the supplied Wheel, create the project, run compiler
checks and tests, and report the commands and results. Do not copy generated
solutions between evaluation directories.

Do not copy or show `EVALUATION.md` to the Coding Agent. It is the evaluator's
hidden acceptance contract, not part of the user request or Skill.

## Debugging scenario

Scenario 04 evaluates `autoagent-debug-invocation`. Unlike an authoring test,
it intentionally starts with an existing project, Eval Suite, input, and
incorrect Workflow implementation because the Agent must investigate recorded
evidence rather than generate a project from scratch.

Prepare a fresh external copy of `04-debug-invocation`, copy in the current
Wheel and Debug Skill, and install the Wheel. From the AutoAgent repository,
prepare the incident and transparent CLI audit proxy:

```bash
python skill-evals/harness/prepare_debug_eval.py \
  --workspace /path/to/external/04-debug-invocation \
  --real-cli /path/to/installed/autoagent
```

The command creates the incorrect Full-mode incident, substitutes its ID into
`REQUIREMENTS.md`, records an immutable evaluation baseline, and prints an
`ACTIVATE` command. Apply that command in the shell used to launch the Coding
Agent so every `autoagent` CLI call is recorded without changing its output or
exit code. Then give the Agent only this instruction:

```text
Investigate and fix the issue described in REQUIREMENTS.md. Use the provided
autoagent-debug-invocation Skill and AutoAgent Wheel. Do not inspect the
AutoAgent framework repository or EVALUATION.md.
```

Do not repair the starter project, run its Eval for the Agent, or reveal the
expected source change before the evaluation.

After the Agent finishes, review the observable process and final result:

```bash
python skill-evals/harness/review_debug_eval.py \
  --workspace /path/to/external/04-debug-invocation
```

The reviewer checks the CLI evidence order, protected fixtures, Rerun and
Comparison boundaries, and final Eval result. It does not capture or infer the
Agent's private reasoning. AutoAgent CLI calls are objectively audited;
non-AutoAgent shell activity can only be reviewed from the Agent transcript.
